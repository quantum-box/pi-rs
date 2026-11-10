//! Account-scoped discovery for Radius gateway models.
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use pi_rs::{Error, Result, ollama::ModelDetails};
use reqwest::{Client, Url};
use serde::Deserialize;

pub(super) struct Catalog {
    pub endpoint: Url,
    pub models: Vec<Model>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Model {
    pub id: String,
    pub name: String,
    reasoning: bool,
    input: Vec<String>,
    context_window: u32,
    max_tokens: u32,
    #[serde(default)]
    thinking_level_map: BTreeMap<String, Option<String>>,
}

impl Model {
    pub fn details(&self) -> ModelDetails {
        let thinking_levels = super::thinking::LEVELS
            .iter()
            .copied()
            .filter(|level| {
                *level == "off"
                    || self.reasoning
                        && self
                            .thinking_level_map
                            .get(*level)
                            .map_or(!matches!(*level, "xhigh" | "max"), Option::is_some)
            })
            .map(str::to_owned)
            .collect();
        ModelDetails {
            context_window: Some(self.context_window),
            max_output_tokens: Some(self.max_tokens),
            reasoning: Some(self.reasoning),
            images: Some(self.input.iter().any(|input| input == "image")),
            thinking_levels: Some(thinking_levels),
        }
    }
}

pub(super) async fn load(client: &Client, gateway: Url, token: &str) -> Result<Catalog> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Config {
        base_url: String,
        models: Vec<serde_json::Value>,
    }

    let url = gateway
        .join("/v1/config")
        .expect("valid Radius config endpoint");
    let mut response = client
        .get(url)
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/json")
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|_| Error::Config("Radius model discovery failed".into()))?;
    if !response.status().is_success() {
        return Err(Error::Config(format!(
            "Radius model discovery failed ({})",
            response.status()
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::Config("Could not read Radius model catalog".into()))?
    {
        if body.len() + chunk.len() > 2 * 1024 * 1024 {
            return Err(Error::Config("Radius model catalog exceeds 2 MiB".into()));
        }
        body.extend_from_slice(&chunk);
    }
    let config: Config = serde_json::from_slice(&body)
        .map_err(|_| Error::Config("Radius returned an invalid model catalog".into()))?;
    let mut endpoint = Url::parse(&config.base_url)
        .map_err(|_| Error::Config("Radius returned an invalid model endpoint".into()))?;
    if endpoint.origin() != gateway.origin()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(Error::Config(
            "Radius model endpoint must match the authenticated gateway origin".into(),
        ));
    }
    let path = endpoint.path().trim_end_matches('/');
    let path = if path.ends_with("/messages") {
        path.to_owned()
    } else {
        format!("{path}/messages")
    };
    endpoint.set_path(&path);
    let mut seen = HashSet::new();
    let models = config
        .models
        .into_iter()
        .filter_map(|model| serde_json::from_value::<Model>(model).ok())
        .filter(|model| {
            !model.id.is_empty()
                && !model.name.is_empty()
                && model.context_window > 0
                && model.max_tokens > 0
                && seen.insert(model.id.clone())
        })
        .collect();
    Ok(Catalog { endpoint, models })
}
