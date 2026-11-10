//! Configured classifier and image models used by the native codemode host.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use pi_rs::{Error, Extension, Outcome, Result, Tool, ToolContext, ToolExecution, ToolSpec};
use reqwest::Url;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{
    Cli,
    model_auth::{Credential, Keys},
};

mod wire;
use wire::{classifier_answers, classifier_payload, image_output, image_payload, usage};

pub(super) const TOOL: &str = "pi_model_operation";
const RESPONSE_LIMIT: usize = 7 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Chat,
    Classifier,
    Image,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Api {
    #[serde(rename = "typesafe-system-one")]
    SystemOne,
    #[serde(rename = "openrouter-images")]
    OpenRouterImages,
}

impl Api {
    fn kind(self) -> Kind {
        match self {
            Self::SystemOne => Kind::Classifier,
            Self::OpenRouterImages => Kind::Image,
        }
    }

    fn route(self) -> &'static str {
        match self {
            Self::SystemOne => "systemone",
            Self::OpenRouterImages => "chat/completions",
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ImageInput {
    Assets,
    StateImageUrls,
    Images,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Compat {
    image_input: Option<ImageInput>,
    send_image_opt_in_header: Option<bool>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
struct Cost {
    input: f64,
    output: f64,
    cache_read: f64,
    cache_write: f64,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Configured {
    id: String,
    name: Option<String>,
    api: Option<Api>,
    base_url: Option<String>,
    input: Option<Vec<String>>,
    output: Option<Vec<String>>,
    context_window: Option<u32>,
    #[serde(default)]
    cost: Cost,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    compat: Compat,
}

#[derive(Clone)]
struct Entry {
    provider: String,
    model: Configured,
    api: Api,
    base: Url,
    endpoint: Url,
    api_key: Option<String>,
}

#[derive(Clone, Default)]
pub(super) struct Catalogue(Vec<Entry>);

impl Catalogue {
    pub fn parse(providers: &Map<String, Value>) -> Result<Self> {
        let mut combined: Map<String, Value> =
            serde_json::from_str(include_str!("../assets/operation-models-v1.5.0.json"))
                .expect("pinned Pi model operation catalogue");
        for (provider, configured) in providers {
            let name = provider.to_ascii_lowercase();
            let Some(configured) = configured.as_object() else {
                continue;
            };
            let mut entry = combined
                .remove(&name)
                .and_then(|entry| entry.as_object().cloned())
                .unwrap_or_default();
            let mut models = entry
                .remove("models")
                .and_then(|models| models.as_array().cloned())
                .unwrap_or_default();
            for model in configured
                .get("models")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if matches!(model["type"].as_str(), Some("image" | "classifier"))
                    && let Some(index) = models.iter().position(|built_in| {
                        built_in["type"] == model["type"] && built_in["id"] == model["id"]
                    })
                {
                    models.remove(index);
                }
            }
            models.extend(
                configured
                    .get("models")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .cloned(),
            );
            entry.extend(configured.clone());
            entry.insert("models".into(), json!(models));
            combined.insert(name, json!(entry));
        }
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        for (name, provider) in &combined {
            let name = name.to_ascii_lowercase();
            for model in provider["models"].as_array().into_iter().flatten() {
                let kind = match model["type"].as_str() {
                    Some("classifier") => Kind::Classifier,
                    Some("image") => Kind::Image,
                    _ => continue,
                };
                if model["api"].as_str().is_some()
                    && serde_json::from_value::<Api>(model["api"].clone()).is_err()
                {
                    continue;
                }
                let mut configured: Configured = serde_json::from_value(model.clone())
                    .map_err(|_| Error::Config(format!("invalid model operation for {name}")))?;
                let Some(api) = configured
                    .api
                    .or_else(|| serde_json::from_value(provider["api"].clone()).ok())
                    .or(match (name.as_str(), kind) {
                        ("typesafe" | "openrouter", Kind::Classifier) => Some(Api::SystemOne),
                        ("openrouter", Kind::Image) => Some(Api::OpenRouterImages),
                        _ => None,
                    })
                    .filter(|api| api.kind() == kind)
                else {
                    continue;
                };
                if name.is_empty()
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
                    || configured.id.trim().is_empty()
                    || configured.id.len() > 256
                    || !seen.insert((name.clone(), kind, configured.id.clone()))
                {
                    return Err(Error::Config(format!(
                        "invalid or duplicate operation model {name}/{}",
                        configured.id
                    )));
                }
                if [
                    configured.cost.input,
                    configured.cost.output,
                    configured.cost.cache_read,
                    configured.cost.cache_write,
                ]
                .iter()
                .any(|cost| !cost.is_finite() || *cost < 0.0 || *cost > 1_000_000.0)
                {
                    return Err(Error::Config(format!(
                        "invalid operation model cost for {name}/{}",
                        configured.id
                    )));
                }
                let base = configured
                    .base_url
                    .as_deref()
                    .or_else(|| provider["baseUrl"].as_str())
                    .or_else(|| official_base(&name))
                    .ok_or_else(|| {
                        Error::Config(format!("operation provider {name} requires baseUrl"))
                    })?;
                let (base, endpoint) = endpoints(base, api.route())?;
                let inherited: BTreeMap<String, String> = if provider["headers"].is_null() {
                    BTreeMap::new()
                } else {
                    serde_json::from_value(provider["headers"].clone()).map_err(|_| {
                        Error::Config(format!("invalid operation provider headers for {name}"))
                    })?
                };
                let mut headers = inherited;
                headers.append(&mut configured.headers);
                for header in headers.keys() {
                    HeaderName::from_bytes(header.as_bytes()).map_err(|_| {
                        Error::Config(format!("invalid operation provider header name for {name}"))
                    })?;
                }
                configured.headers = headers;
                let inherited: Compat = if provider["compat"].is_null() {
                    Compat::default()
                } else {
                    serde_json::from_value(provider["compat"].clone()).map_err(|_| {
                        Error::Config(format!("invalid operation compatibility for {name}"))
                    })?
                };
                configured.compat.image_input =
                    configured.compat.image_input.or(inherited.image_input);
                configured.compat.send_image_opt_in_header = configured
                    .compat
                    .send_image_opt_in_header
                    .or(inherited.send_image_opt_in_header);
                result.push(Entry {
                    provider: name.clone(),
                    model: configured,
                    api,
                    base,
                    endpoint,
                    api_key: provider["apiKey"].as_str().map(str::to_owned),
                });
            }
        }
        Ok(Self(result))
    }
    async fn list(
        &self,
        kind: Kind,
        provider: Option<&str>,
        id: Option<&str>,
        available: bool,
        keys: &Keys,
    ) -> Vec<Value> {
        let mut entries = Vec::new();
        let mut resolved = BTreeMap::new();
        for model in &self.0 {
            if model.api.kind() != kind
                || provider.is_some_and(|provider| model.provider != provider)
                || id.is_some_and(|id| model.model.id != id)
            {
                continue;
            }
            if available {
                let key = (model.provider.as_str(), model.endpoint.as_str());
                let usable = match resolved.get(&key) {
                    Some(usable) => *usable,
                    None => {
                        let usable = match model.credential(keys) {
                            Some(credential) => credential.resolve().await.is_ok(),
                            None => false,
                        };
                        resolved.insert(key, usable);
                        usable
                    }
                };
                if !usable {
                    continue;
                }
            }
            entries.push(model.descriptor());
        }
        entries
    }
}

pub(super) fn official_base(provider: &str) -> Option<&'static str> {
    match provider {
        "typesafe" => Some("https://api.typesafe.ai/v1"),
        "openrouter" => Some("https://openrouter.ai/api/v1"),
        _ => None,
    }
}

fn endpoints(value: &str, route: &str) -> Result<(Url, Url)> {
    let base =
        Url::parse(value).map_err(|_| Error::Config("invalid operation model baseUrl".into()))?;
    let loopback = matches!(base.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
        || base.host_str().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err(Error::Config("invalid operation model baseUrl".into()));
    }
    let endpoint = if base
        .path()
        .trim_end_matches('/')
        .ends_with(&format!("/{route}"))
    {
        base.clone()
    } else {
        Url::parse(&format!("{}/{route}", base.as_str().trim_end_matches('/')))
            .map_err(|_| Error::Config("invalid operation model endpoint".into()))?
    };
    Ok((base, endpoint))
}

impl Entry {
    fn descriptor(&self) -> Value {
        let mut value = json!({
            "type": self.api.kind(),
            "provider": self.provider,
            "id": self.model.id,
            "name": self.model.name.as_deref().unwrap_or(&self.model.id),
            "api": self.api,
            "input": self.model.input.clone().unwrap_or_else(|| vec!["text".into()]),
            "cost": self.model.cost,
        });
        if let Some(window) = self.model.context_window {
            value["contextWindow"] = json!(window);
        }
        if self.api.kind() == Kind::Image {
            value["output"] = json!(
                self.model
                    .output
                    .clone()
                    .unwrap_or_else(|| vec!["image".into()])
            );
        }
        value
    }

    fn credential(&self, keys: &Keys) -> Option<Credential> {
        let official = official_base(&self.provider)
            .and_then(|base| endpoints(base, self.api.route()).ok())
            .is_some_and(|(_, endpoint)| endpoint == self.endpoint);
        keys.operation_credential(&self.provider, self.endpoint.as_str(), official)
            .cloned()
            .or_else(|| {
                let key = match (self.provider.as_str(), official) {
                    ("typesafe", true) => "TYPESAFE_API_KEY",
                    ("openrouter", true) => "OPENROUTER_API_KEY",
                    _ => return None,
                };
                std::env::var(key)
                    .ok()
                    .filter(|key| super::model_auth::valid_key(key))
                    .map(Credential::Literal)
            })
            .or_else(|| self.api_key.as_deref().and_then(Credential::configured))
    }

    fn accepts_images(&self) -> bool {
        self.model
            .input
            .as_ref()
            .is_some_and(|input| input.iter().any(|kind| kind == "image"))
    }

    fn image_input(&self) -> ImageInput {
        self.model.compat.image_input.unwrap_or_else(|| {
            if self.provider == "openrouter"
                || self
                    .base
                    .host_str()
                    .is_some_and(|host| host == "openrouter.ai" || host.ends_with(".openrouter.ai"))
            {
                ImageInput::StateImageUrls
            } else {
                ImageInput::Assets
            }
        })
    }
}

#[derive(Clone)]
pub(super) struct Operations {
    args: Cli,
    client: reqwest::Client,
}

impl Operations {
    pub fn new(args: &Cli) -> Result<Self> {
        Ok(Self {
            args: args.clone(),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(args.model_timeout_secs))
                .build()
                .map_err(|_| {
                    Error::Config("could not initialize model operation HTTP client".into())
                })?,
        })
    }
}

impl Extension for Operations {
    fn id(&self) -> &str {
        "native-model-operations"
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![Arc::new(self.clone())]
    }
}

#[async_trait::async_trait]
impl Tool for Operations {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: TOOL.into(),
            description:
                "Private native model catalogue and classifier or image invocation for codemode"
                    .into(),
            parameters: json!({"type":"object","properties":{
                "action":{"enum":["list","classify","generateImages"]},
                "type":{"enum":["chat","classifier","image"]},
                "provider":{"type":"string"},"id":{"type":"string"},
                "available":{"type":"boolean"},"context":{"type":"object"}
            },"required":["action"],"additionalProperties":false}),
        }
    }

    fn execution(&self) -> ToolExecution {
        ToolExecution::ParallelRead
    }

    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        let keys = match Keys::load() {
            Ok(keys) => keys,
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        let catalogue = self.args.model_profiles.operations();
        let provider = args["provider"].as_str();
        if args["action"] == "list" {
            let kind = serde_json::from_value::<Kind>(args["type"].clone()).unwrap_or(Kind::Chat);
            let entries = if kind == Kind::Chat {
                self.chat_models(provider, args["id"].as_str(), args["available"] == true)
            } else {
                catalogue
                    .list(
                        kind,
                        provider,
                        args["id"].as_str(),
                        args["available"] == true,
                        &keys,
                    )
                    .await
            };
            return Outcome::Structured {
                data: json!(entries),
                attachments: vec![],
            };
        }
        let kind = if args["action"] == "classify" {
            Kind::Classifier
        } else {
            Kind::Image
        };
        let mut data = json!({
            "provider": provider.unwrap_or_default(),
            "model": args["id"].as_str().unwrap_or_default(),
            "stopReason": "stop",
        });
        data[if kind == Kind::Classifier {
            "answers"
        } else {
            "output"
        }] = if kind == Kind::Classifier {
            json!({})
        } else {
            json!([])
        };
        let result = if let Some(model) = catalogue.0.iter().find(|model| {
            model.api.kind() == kind
                && Some(model.provider.as_str()) == provider
                && Some(model.model.id.as_str()) == args["id"].as_str()
        }) {
            let operation = self.perform(model, &args["context"], &keys, &mut data);
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(Error::Cancelled),
                result = operation => result,
            }
        } else {
            Err(Error::Config("unknown model for this operation".into()))
        };
        if let Err(error) = result {
            data["stopReason"] = json!(if matches!(error, Error::Cancelled) {
                "aborted"
            } else {
                "error"
            });
            data["errorMessage"] = json!(error.to_string());
        }
        Outcome::Structured {
            data,
            attachments: vec![],
        }
    }
}

impl Operations {
    fn chat_models(
        &self,
        provider: Option<&str>,
        selected: Option<&str>,
        available: bool,
    ) -> Vec<Value> {
        let profiles = &self.args.model_profiles;
        let mut entries = Vec::new();
        for candidate in profiles
            .providers()
            .filter(|candidate| provider.is_none_or(|name| name == candidate.id()))
        {
            let mut ids = profiles.model_ids(&candidate);
            if candidate == self.args.provider
                && !self.args.model.is_empty()
                && !ids.contains(&self.args.model)
            {
                ids.push(self.args.model.clone());
            }
            for id in ids
                .into_iter()
                .filter(|id| selected.is_none_or(|selected| selected == id))
            {
                if available && self.args.connect_provider(&candidate, Some(&id)).is_err() {
                    continue;
                }
                let metadata = profiles.model(&candidate, &id);
                let mut detail = pi_rs::ollama::ModelDetails::default();
                if let Some(metadata) = &metadata {
                    metadata.apply(&candidate, &mut detail);
                }
                let mut model = json!({
                    "type":"chat",
                    "provider":candidate.id(),
                    "id":id,
                    "name":metadata.as_ref().and_then(|metadata| metadata.name.as_deref()).unwrap_or(&id),
                    "api":candidate.api(),
                    "input":if detail.images == Some(true) { vec!["text", "image"] } else { vec!["text"] },
                });
                if let Some(window) = detail.context_window {
                    model["contextWindow"] = json!(window);
                }
                entries.push(model);
            }
        }
        entries
    }

    async fn perform(
        &self,
        entry: &Entry,
        context: &Value,
        keys: &Keys,
        data: &mut Value,
    ) -> Result<()> {
        let (payload, image_opt_in) = match entry.api {
            Api::SystemOne => classifier_payload(entry, context)?,
            Api::OpenRouterImages => (image_payload(entry, context)?, false),
        };
        let credential = entry
            .credential(keys)
            .ok_or_else(|| Error::Config(format!("No API key for provider: {}", entry.provider)))?;
        let key = credential.resolve().await?;
        let mut headers = HeaderMap::new();
        let mut authorization = HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| Error::Config("invalid model operation credential".into()))?;
        authorization.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, authorization);
        if image_opt_in {
            headers.insert(
                "typesafe-danger-zone-images",
                HeaderValue::from_static("true"),
            );
        }
        for (name, value) in &entry.model.headers {
            let value = if value.starts_with('!') {
                pi_rs_workbench::mcp::resolve_value(value).await.ok()
            } else {
                super::model_profiles::expand_value(value)
            }
            .ok_or_else(|| {
                Error::Config(format!("could not resolve operation model header {name}"))
            })?;
            let mut value = HeaderValue::from_str(&value)
                .map_err(|_| Error::Config(format!("invalid operation model header {name}")))?;
            value.set_sensitive(true);
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("validated header name"),
                value,
            );
        }
        let mut response = self
            .client
            .post(entry.endpoint.clone())
            .headers(headers)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&payload)?)
            .send()
            .await
            .map_err(|error| {
                Error::Model(
                    if error.is_timeout() {
                        "model operation timed out"
                    } else {
                        "model operation could not connect"
                    }
                    .into(),
                )
            })?;
        if !response.status().is_success() {
            return Err(Error::Model(format!(
                "model operation returned HTTP {}",
                response.status().as_u16()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > RESPONSE_LIMIT as u64)
        {
            return Err(Error::Model(
                "model operation response exceeds the size limit".into(),
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::Model("could not read model operation response".into()))?
        {
            if body.len() + chunk.len() > RESPONSE_LIMIT {
                return Err(Error::Model(
                    "model operation response exceeds the size limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&body)
            .map_err(|_| Error::Model("model operation response is not valid JSON".into()))?;
        data["api"] = json!(entry.api);
        data["timestamp"] = json!(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        );
        if let Some(usage) = usage(entry, &body["usage"]) {
            data["usage"] = usage;
        }
        match entry.api {
            Api::SystemOne => {
                data["answers"] = classifier_answers(&context["questions"], &body["answers"])?
            }
            Api::OpenRouterImages => {
                data["output"] = json!(image_output(&body)?);
                if let Some(id) = body["id"].as_str() {
                    data["responseId"] = json!(id);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_operations_use_distinct_routes_for_equal_model_ids() {
        let config = json!({"proxy": {
            "baseUrl":"http://127.0.0.1:5000/v1",
            "models":[
                {"type":"classifier","api":"typesafe-system-one","id":"shared"},
                {"type":"image","api":"openrouter-images","id":"shared"},
            ]
        }});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let routes: Vec<_> = catalogue
            .0
            .iter()
            .filter(|model| model.provider == "proxy")
            .map(|model| model.endpoint.path())
            .collect();
        assert_eq!(routes, ["/v1/systemone", "/v1/chat/completions"]);
        assert!(endpoints("http://example.com/v1", "systemone").is_err());
        assert!(endpoints("https://token@example.com/v1", "systemone").is_err());
    }
}
