//! Opt-in evaluation extension. Its fixed destination is a loopback fixture.
use std::sync::Arc;

use pi_rs_core::{
    Error, Extension, ExtensionResult, ExtensionState, Outcome, Result, Tool, ToolContext, ToolSpec,
};
use reqwest::{Client, Url};
use serde_json::{Value, json};

pub struct Fixtures {
    client: Client,
    endpoint: Url,
}

impl Fixtures {
    pub fn new(endpoint: &str) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid fixture endpoint".into()))?;
        if endpoint.scheme() != "http"
            || endpoint.host_str() != Some("127.0.0.1")
            || endpoint.path() != "/fixture/"
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Config(
                "fixture endpoint must be a loopback /fixture/ URL".into(),
            ));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Config("could not build fixture client".into()))?;
        Ok(Self { client, endpoint })
    }
}

impl Extension for Fixtures {
    fn id(&self) -> &str {
        "evaluation-fixtures"
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        [
            ("patch_fixture", "Replace the return expression of the discount function in the synthetic buggy.js fixture.", "expression"),
            ("submit_mock_order", "Submit an order to the synthetic evaluation store only.", "id"),
        ].into_iter().map(|(name, description, argument)| {
            Arc::new(FixtureTool {
                client: self.client.clone(),
                endpoint: self.endpoint.join(name).expect("fixed relative path"),
                name, description, argument,
            }) as Arc<dyn Tool>
        }).collect()
    }

    fn migrate(
        &self,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        match previous {
            None => Ok(Some(counter(0))),
            Some(previous) if previous.version == 1 && previous.data.as_u64().is_some() => Ok(None),
            Some(_) => Err("unsupported fixture state".into()),
        }
    }

    fn after_tool(
        &self,
        name: &str,
        outcome: &Outcome,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        if matches!(name, "patch_fixture" | "submit_mock_order")
            && matches!(outcome, Outcome::Succeeded(_))
        {
            let count = previous
                .and_then(|state| state.data.as_u64())
                .ok_or("missing fixture counter")?;
            return Ok(Some(counter(count.saturating_add(1))));
        }
        Ok(None)
    }
}

fn counter(count: u64) -> ExtensionState {
    ExtensionState {
        version: 1,
        data: json!(count),
    }
}

struct FixtureTool {
    client: Client,
    endpoint: Url,
    name: &'static str,
    description: &'static str,
    argument: &'static str,
}

#[async_trait::async_trait]
impl Tool for FixtureTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: self.description.into(),
            parameters: json!({
                "type":"object", "properties":{(self.argument):{"type":"string","minLength":1}},
                "required":[self.argument], "additionalProperties":false,
            }),
        }
    }

    async fn invoke(&self, arguments: Value, context: ToolContext) -> Outcome {
        let response = self
            .client
            .post(self.endpoint.clone())
            .header("idempotency-key", context.operation_id.to_string())
            .json(&arguments)
            .send()
            .await;
        let Ok(mut response) = response else {
            return Outcome::Unknown("fixture response missing".into());
        };
        if response.status().is_client_error() {
            return Outcome::Failed("fixture rejected the request".into());
        }
        if !response.status().is_success() {
            return Outcome::Unknown("fixture did not confirm the request".into());
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len() + chunk.len() <= 4096 => {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(None) => break,
                _ => return Outcome::Unknown("fixture response was incomplete".into()),
            }
        }
        match String::from_utf8(bytes) {
            Ok(result) => Outcome::Succeeded(result),
            Err(_) => Outcome::Unknown("fixture response could not be decoded".into()),
        }
    }
}
