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
use wire::{
    classifier_answers, classifier_payload, classifier_response, image_output, image_payload, usage,
};

pub(super) const TOOL: &str = "pi_model_operation";
const RESPONSE_LIMIT: usize = 7 * 1024 * 1024;
const CLASSIFIER_MAX_RETRIES: usize = 2;
const MAX_SERVER_RETRY_DELAY: Duration = Duration::from_secs(60);

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
    #[serde(rename = "cloudflare-workers-ai-system-one")]
    WorkersAiSystemOne,
    #[serde(rename = "openrouter-images")]
    OpenRouterImages,
}

impl Api {
    fn kind(self) -> Kind {
        match self {
            Self::SystemOne | Self::WorkersAiSystemOne => Kind::Classifier,
            Self::OpenRouterImages => Kind::Image,
        }
    }

    fn route(self) -> &'static str {
        match self {
            Self::SystemOne => "systemone",
            Self::WorkersAiSystemOne => "run",
            Self::OpenRouterImages => "chat/completions",
        }
    }
}

fn retryable_classifier_response(response: &reqwest::Response) -> bool {
    match response
        .headers()
        .get("x-should-retry")
        .and_then(|value| value.to_str().ok())
    {
        Some("true") => return true,
        Some("false") => return false,
        _ => {}
    }
    matches!(response.status().as_u16(), 408 | 409 | 429 | 500..=599)
}

fn classifier_retry_delay(
    headers: &HeaderMap,
    retry_index: usize,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Duration> {
    let numeric = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite())
    };
    let delay_ms = numeric("retry-after-ms").or_else(|| {
        numeric("retry-after")
            .map(|seconds| seconds * 1000.0)
            .or_else(|| {
                headers
                    .get("retry-after")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
                    .map(|date| date.signed_duration_since(now).num_milliseconds() as f64)
            })
    });
    if let Some(delay_ms) = delay_ms {
        if delay_ms > MAX_SERVER_RETRY_DELAY.as_millis() as f64 {
            return Err(Error::Model(
                "model operation requested a retry delay over 60 seconds".into(),
            ));
        }
        return Ok(Duration::from_secs_f64(delay_ms.max(0.0) / 1000.0));
    }
    let base_ms = 500 * (1u64 << retry_index.min(4));
    let jitter = (uuid::Uuid::new_v4().as_u128() % u128::from(base_ms / 4 + 1)) as u64;
    Ok(Duration::from_millis(base_ms - jitter))
}

async fn send_model_request(
    client: &reqwest::Client,
    request: reqwest::Request,
    api: Api,
) -> Result<Vec<u8>> {
    let retries = if api.kind() == Kind::Classifier {
        CLASSIFIER_MAX_RETRIES
    } else {
        0
    };
    'attempt: for attempt in 0..=retries {
        let request = request
            .try_clone()
            .ok_or_else(|| Error::Config("model operation request cannot be retried".into()))?;
        let mut response = match client.execute(request).await {
            Ok(response) => response,
            Err(error) if error.is_timeout() && attempt < retries => {
                tokio::time::sleep(classifier_retry_delay(
                    &HeaderMap::new(),
                    attempt,
                    chrono::Utc::now(),
                )?)
                .await;
                continue;
            }
            Err(error) => {
                return Err(Error::Model(
                    if error.is_timeout() {
                        "model operation timed out"
                    } else {
                        "model operation could not connect"
                    }
                    .into(),
                ));
            }
        };
        if response.status().is_success() {
            if response
                .content_length()
                .is_some_and(|length| length > RESPONSE_LIMIT as u64)
            {
                return Err(Error::Model(
                    "model operation response exceeds the size limit".into(),
                ));
            }
            let mut body = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        if body.len() + chunk.len() > RESPONSE_LIMIT {
                            return Err(Error::Model(
                                "model operation response exceeds the size limit".into(),
                            ));
                        }
                        body.extend_from_slice(&chunk);
                    }
                    Ok(None) => return Ok(body),
                    Err(error) if error.is_timeout() && attempt < retries => {
                        drop(response);
                        tokio::time::sleep(classifier_retry_delay(
                            &HeaderMap::new(),
                            attempt,
                            chrono::Utc::now(),
                        )?)
                        .await;
                        continue 'attempt;
                    }
                    Err(error) => {
                        return Err(Error::Model(
                            if error.is_timeout() {
                                "model operation timed out"
                            } else {
                                "could not read model operation response"
                            }
                            .into(),
                        ));
                    }
                }
            }
        }
        if attempt == retries || !retryable_classifier_response(&response) {
            return Err(Error::Model(format!(
                "model operation returned HTTP {}",
                response.status().as_u16()
            )));
        }
        let delay = classifier_retry_delay(response.headers(), attempt, chrono::Utc::now())?;
        drop(response);
        // Dropping this future on operation cancellation also drops the sleep.
        tokio::time::sleep(delay).await;
    }
    unreachable!("retry loop returns on its last attempt")
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
            let inferred_base =
                if provider["baseUrl"].as_str().is_none() && official_base(&name).is_none() {
                    unambiguous_model_base(provider)
                } else {
                    None
                };
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
                let key_endpoint = provider["baseUrl"]
                    .as_str()
                    .or_else(|| official_base(&name))
                    .or(inferred_base.as_deref())
                    .map(|base| endpoints(base, api.route()))
                    .transpose()?
                    .map(|(_, endpoint)| endpoint);
                let inherited: BTreeMap<String, String> = if provider["headers"].is_null() {
                    BTreeMap::new()
                } else {
                    serde_json::from_value(provider["headers"].clone()).map_err(|_| {
                        Error::Config(format!("invalid operation provider headers for {name}"))
                    })?
                };
                let mut headers = BTreeMap::new();
                // Provider headers can carry credentials under arbitrary names. Bind
                // all of them to the provider's resolved endpoint, just like its key.
                let inherit_headers = key_endpoint.as_ref() == Some(&endpoint);
                for (inherited, header, value) in inherited
                    .into_iter()
                    .map(|(header, value)| (true, header, value))
                    .chain(
                        std::mem::take(&mut configured.headers)
                            .into_iter()
                            .map(|(header, value)| (false, header, value)),
                    )
                {
                    let header = HeaderName::from_bytes(header.as_bytes()).map_err(|_| {
                        Error::Config(format!("invalid operation provider header name for {name}"))
                    })?;
                    if !inherited || inherit_headers {
                        headers.insert(header.as_str().to_owned(), value);
                    }
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
                let api_key = (key_endpoint.as_ref() == Some(&endpoint))
                    .then(|| provider["apiKey"].as_str().map(str::to_owned))
                    .flatten();
                result.push(Entry {
                    provider: name.clone(),
                    model: configured,
                    api,
                    base,
                    endpoint,
                    api_key,
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

fn unambiguous_model_base(provider: &Value) -> Option<String> {
    let models = provider["models"].as_array()?;
    let mut bases = BTreeSet::new();
    for model in models {
        let raw = model["baseUrl"].as_str()?;
        let mut base = Url::parse(raw).ok()?;
        let path = base.path().trim_end_matches('/').to_owned();
        base.set_path(&path);
        bases.insert(base.to_string());
    }
    (bases.len() == 1)
        .then(|| bases.into_iter().next())
        .flatten()
}

pub(super) fn official_base(provider: &str) -> Option<&'static str> {
    match provider {
        "typesafe" => Some("https://api.typesafe.ai/v1"),
        "openrouter" => Some("https://openrouter.ai/api/v1"),
        _ => None,
    }
}

fn endpoints(value: &str, route: &str) -> Result<(Url, Url)> {
    let mut base =
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
    let path = base.path().trim_end_matches('/').to_owned();
    base.set_path(&path);
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

async fn resolve_header(value: &str, env: Option<&Map<String, Value>>) -> Option<String> {
    if value.starts_with('!') {
        pi_rs_workbench::mcp::resolve_value(value).await.ok()
    } else {
        super::model_profiles::expand_value_with_env(value, env)
    }
}

impl Entry {
    fn is_official(&self) -> bool {
        official_base(&self.provider)
            .and_then(|base| endpoints(base, self.api.route()).ok())
            .is_some_and(|(_, endpoint)| endpoint == self.endpoint)
    }

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
        self.credential_with_env(keys, |name| std::env::var(name).ok())
    }

    fn credential_with_env(
        &self,
        keys: &Keys,
        env: impl Fn(&str) -> Option<String>,
    ) -> Option<Credential> {
        let official = self.is_official();
        if let Some(key) =
            keys.operation_credential(&self.provider, self.endpoint.as_str(), official)
        {
            return Some(key.clone());
        }
        if let Some(key) = self.api_key.as_deref() {
            return Some(match Credential::configured(key) {
                Some(Credential::Literal(value)) if super::model_auth::valid_key(&value) => {
                    Credential::Literal(value)
                }
                Some(credential @ Credential::Command(_)) => credential,
                _ => Credential::Invalid,
            });
        }
        {
            let key = match (self.provider.as_str(), official) {
                ("typesafe", true) => "TYPESAFE_API_KEY",
                ("openrouter", true) => "OPENROUTER_API_KEY",
                _ => return None,
            };
            env(key)
                .filter(|key| super::model_auth::valid_key(key))
                .map(Credential::Literal)
        }
    }

    fn accepts_images(&self) -> bool {
        self.model
            .input
            .as_ref()
            .is_some_and(|input| input.iter().any(|kind| kind == "image"))
    }

    fn openrouter_compat(&self) -> bool {
        self.provider == "openrouter"
            || self
                .base
                .host_str()
                .is_some_and(|host| host == "openrouter.ai" || host.ends_with(".openrouter.ai"))
    }

    fn image_input(&self) -> ImageInput {
        self.model.compat.image_input.unwrap_or_else(|| {
            if matches!(self.api, Api::WorkersAiSystemOne) {
                if self.model.id.starts_with("@cf/cloudflare/clef") {
                    ImageInput::Images
                } else {
                    ImageInput::Assets
                }
            } else if self.openrouter_compat() {
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
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(args.model_timeout_secs));
        for proxy in super::http_proxy::configured(&args.settings)? {
            builder = builder.proxy(proxy);
        }
        if let Some(timeout) = args.settings.http_idle_timeout() {
            builder = builder.read_timeout(timeout);
        }
        Ok(Self {
            args: args.clone(),
            client: builder.build().map_err(|_| {
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
                "action":{"enum":["list","resolve","classify","generateImages"]},
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
        let catalogue = self.args.model_profiles.operations();
        let provider = args["provider"].as_str();
        if args["action"] == "resolve" {
            let kind = serde_json::from_value::<Kind>(args["type"].clone()).ok();
            let found = catalogue.0.iter().any(|model| {
                Some(model.api.kind()) == kind
                    && Some(model.provider.as_str()) == provider
                    && Some(model.model.id.as_str()) == args["id"].as_str()
            });
            return if found {
                Outcome::Structured {
                    data: json!(true),
                    attachments: vec![],
                }
            } else {
                Outcome::Failed("unknown model for this operation".into())
            };
        }
        let keys = match Keys::load_async().await {
            Ok(keys) => keys,
            Err(error) => return Outcome::Failed(error.to_string()),
        };
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
        let Some(model) = catalogue.0.iter().find(|model| {
            model.api.kind() == kind
                && Some(model.provider.as_str()) == provider
                && Some(model.model.id.as_str()) == args["id"].as_str()
        }) else {
            return Outcome::Failed("unknown model for this operation".into());
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
        let inputs = &args["context"];
        let has_images = if kind == Kind::Classifier {
            inputs["images"]
                .as_array()
                .is_some_and(|images| !images.is_empty())
        } else {
            inputs["input"]
                .as_array()
                .is_some_and(|blocks| blocks.iter().any(|block| block["type"] == "image"))
        };
        initialize_image_result(model, &mut data);
        let result = if has_images && self.args.settings.block_model_images() {
            Err(Error::Config(super::image_policy::BLOCKED_IMAGE.into()))
        } else {
            let operation = self.perform(model, &args["context"], &keys, &mut data);
            tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => Err(Error::Cancelled),
                result = operation => result,
            }
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

fn initialize_image_result(entry: &Entry, data: &mut Value) {
    if matches!(entry.api, Api::OpenRouterImages) && data.get("timestamp").is_none() {
        data["api"] = json!(entry.api);
        data["timestamp"] = json!(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        );
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
                let adapter = profiles.model_adapter(&candidate, &id);
                let metadata = profiles.model(&candidate, &id);
                let mut detail = pi_rs::ollama::ModelDetails::default();
                if let Some(metadata) = &metadata {
                    metadata.apply(adapter, &mut detail);
                }
                let mut model = json!({
                    "type":"chat",
                    "provider":candidate.id(),
                    "id":id,
                    "name":metadata.as_ref().and_then(|metadata| metadata.name.as_deref()).unwrap_or(&id),
                    "api":adapter.api(),
                    "input":if detail.images == Some(true) { vec!["text", "image"] } else { vec!["text"] },
                });
                if let Some(metadata) = metadata {
                    metadata.apply_catalog_metadata(&mut model);
                }
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
        initialize_image_result(entry, data);
        let (payload, image_opt_in) = match entry.api {
            Api::SystemOne | Api::WorkersAiSystemOne => classifier_payload(entry, context)?,
            Api::OpenRouterImages => (image_payload(entry, context)?, false),
        };
        let credential = entry
            .credential(keys)
            .ok_or_else(|| Error::Config(format!("No API key for provider: {}", entry.provider)))?;
        let key = credential.resolve().await?;
        let credential_env = keys.operation_environment(
            &entry.provider,
            entry.endpoint.as_str(),
            entry.is_official(),
        );
        let mut headers = HeaderMap::new();
        let mut authorization = HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| Error::Config("invalid model operation credential".into()))?;
        authorization.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, authorization);
        for (name, value) in &entry.model.headers {
            let value = resolve_header(value, credential_env).await.ok_or_else(|| {
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
        // Pi adds the automatic classifier image opt-in as request options,
        // after provider and model headers have been combined.
        if image_opt_in {
            headers.insert(
                "typesafe-danger-zone-images",
                HeaderValue::from_static("true"),
            );
        }
        let request = self
            .client
            .post(entry.endpoint.clone())
            .headers(headers)
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&payload)?)
            .build()
            .map_err(|_| Error::Config("invalid model operation request".into()))?;
        let body = send_model_request(&self.client, request, entry.api).await?;
        let body: Value = serde_json::from_slice(&body)
            .map_err(|_| Error::Model("model operation response is not valid JSON".into()))?;
        if !matches!(entry.api, Api::OpenRouterImages) {
            data["api"] = json!(entry.api);
            data["timestamp"] = json!(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
            );
        }
        let result_body = if matches!(entry.api, Api::WorkersAiSystemOne) {
            classifier_response(&body)?
        } else {
            &body
        };
        if let Some(usage) = usage(entry, &result_body["usage"]) {
            data["usage"] = usage;
        }
        match entry.api {
            Api::SystemOne | Api::WorkersAiSystemOne => {
                data["answers"] =
                    classifier_answers(&context["questions"], &result_body["answers"])?
            }
            Api::OpenRouterImages => {
                if let Some(id) = body["id"].as_str() {
                    data["responseId"] = json!(id);
                }
                // Keep accepted blocks if a later image fails native validation.
                let output = data["output"]
                    .as_array_mut()
                    .expect("image operation output is initialized as an array");
                image_output(&body, output)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolve_uses_catalogue_without_provider_request() {
        use clap::Parser;
        let config = json!({"relay":{"baseUrl":"http://127.0.0.1:1/v1", "models":[
            {"type":"image", "id":"fixture", "api":"openrouter-images", "input":["text"], "output":["image"]}
        ]}});
        let mut args = Cli::parse_from(["pi"]);
        args.model_profiles =
            super::super::model_profiles::Profiles::parse(&json!({"providers":config})).unwrap();
        let operation = Operations {
            args,
            client: reqwest::Client::new(),
        };
        let context = || ToolContext {
            operation_id: uuid::Uuid::new_v4(),
            cancellation: Default::default(),
        };
        let resolve =
            |kind, id| json!({"action":"resolve", "type":kind, "provider":"relay", "id":id});
        assert!(matches!(
            operation
                .invoke(resolve("image", "fixture"), context())
                .await,
            Outcome::Structured { .. }
        ));
        assert!(matches!(
            operation
                .invoke(resolve("classifier", "fixture"), context())
                .await,
            Outcome::Failed(_)
        ));
        assert!(matches!(
            operation
                .invoke(resolve("image", "missing"), context())
                .await,
            Outcome::Failed(_)
        ));
    }

    #[tokio::test]
    async fn openrouter_image_empty_choice_preserves_result_and_usage() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let (end, length) = loop {
                let mut chunk = [0; 2048];
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
                if let Some(pos) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    let end = pos + 4;
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end, length);
                }
            };
            while request.len() < end + length {
                let mut chunk = [0; 2048];
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }
            let body = json!({"id":"fixture-response", "choices":[], "usage":{
                "prompt_tokens":100, "completion_tokens":10,
                "prompt_tokens_details":{"cached_tokens":120,"cache_write_tokens":20}
            }})
            .to_string();
            stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            request
        });
        let config = json!({"relay":{"baseUrl":format!("http://{address}/v1"),"apiKey":"fixture-key", "models":[{
            "type":"image", "id":"fixture", "api":"openrouter-images", "input":["text"], "output":["image"],
            "cost":{"input":1.0,"output":2.0,"cacheRead":3.0,"cacheWrite":4.0}
        }]}});
        let profiles =
            super::super::model_profiles::Profiles::parse(&json!({"providers":config})).unwrap();
        let entry = profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.provider == "relay" && entry.model.id == "fixture")
            .unwrap();
        let operation = Operations {
            args: Cli::parse_from(["pi"]),
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
        };
        let mut data = json!({"stopReason":"stop", "output":[]});
        let context = json!({"input":[{"type":"text","text":"draw"}]});
        tokio::time::timeout(
            Duration::from_secs(10),
            operation.perform(entry, &context, &Keys::parse(&json!({})), &mut data),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(data["stopReason"], "stop");
        assert_eq!(data["responseId"], "fixture-response");
        assert_eq!(data["output"], json!([]));
        assert_eq!(data["usage"]["input"], 0);
        assert_eq!(data["usage"]["output"], 10);
        assert_eq!(data["usage"]["cacheRead"], 100);
        assert_eq!(data["usage"]["cacheWrite"], 20);
        assert_eq!(data["usage"]["totalTokens"], 130);
        assert!((data["usage"]["cost"]["total"].as_f64().unwrap() - 0.0004).abs() < 1e-12);
        let request = server.await.unwrap();
        assert!(request.starts_with(b"POST /v1/chat/completions "));
    }

    #[tokio::test]
    async fn openrouter_image_partial_results_keep_response_metadata() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const CHILD: &str = "PI_RS_IMAGE_RESULT_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let home = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "operation_models::tests::openrouter_image_partial_results_keep_response_metadata"])
                .env(CHILD, "1")
                .env("PI_RS_HOME", home.path())
                .output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        let valid_image = json!({"type":"image","mimeType":"image/png","data":"aW1hZ2U="});
        let cases = [
            (json!({"choices":[{}]}), vec![], true),
            (
                json!({"choices":[{"message":{"content":"caption","images":{"image_url":"data:image/png;base64,YQ=="}}}]}),
                vec![json!({"type":"text","text":"caption"})],
                true,
            ),
            (
                json!({"choices":[{"message":{"content":"caption","images":[{"image_url":"data:image/png;base64,aW1hZ2U="},null]}}]}),
                vec![json!({"type":"text","text":"caption"}), valid_image.clone()],
                true,
            ),
            (
                json!({"choices":[{"message":{"content":"caption","images":null}}]}),
                vec![json!({"type":"text","text":"caption"})],
                false,
            ),
            (
                json!({"choices":[{"message":{"content":"caption","images":"invalid"}}]}),
                vec![json!({"type":"text","text":"caption"})],
                false,
            ),
            (
                json!({"choices":[{"message":{"content":"caption","images":[
                    {"image_url":{"url":"data:image/png;base64,aW1hZ2U="}},
                    {"image_url":"data:image/png;base64,%%%"}
                ]}}]}),
                vec![json!({"type":"text","text":"caption"}), valid_image.clone()],
                true,
            ),
            (
                json!({"choices":[{"message":{"content":"caption","images":[
                    {"image_url":"https://example.org/image.png"},
                    {"image_url":"data:;base64,YQ=="},
                    {"image_url":"data:image/png;base64,"},
                    {"image_url":"data:image/png;base64,aW1hZ2U="},
                    {"image_url":{"url":"data:image/png;base64,aW1hZ2U="}}
                ]}}]}),
                vec![
                    json!({"type":"text","text":"caption"}),
                    valid_image.clone(),
                    valid_image,
                ],
                false,
            ),
        ];
        for (mut body, expected_output, fails) in cases {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            body["id"] = json!("image-response");
            body["usage"] = json!({"prompt_tokens":12, "completion_tokens":34});
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = [0; 4096];
                assert_ne!(stream.read(&mut bytes).await.unwrap(), 0);
                let body = body.to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            });
            let config = json!({"relay":{"baseUrl":format!("http://{address}/v1"),"apiKey":"fixture-key", "models":[{
                "type":"image", "id":"fixture", "api":"openrouter-images", "input":["text"], "output":["image"]
            }]}});
            let profiles =
                super::super::model_profiles::Profiles::parse(&json!({"providers":config}))
                    .unwrap();
            let mut args = Cli::parse_from(["pi"]);
            args.model_profiles = profiles;
            let operation = Operations {
                args,
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            };
            let outcome = tokio::time::timeout(
                Duration::from_secs(10),
                operation.invoke(json!({"action":"generateImages", "provider":"relay", "id":"fixture", "context":{"input":[{"type":"text","text":"draw"}]}}), ToolContext {
                    operation_id: uuid::Uuid::new_v4(),
                    cancellation: pi_rs::CancellationToken::new(),
                }),
            ).await.unwrap();
            let Outcome::Structured { data, .. } = outcome else {
                panic!("expected structured result")
            };
            assert_eq!(data["stopReason"], if fails { "error" } else { "stop" });
            assert_eq!(data.get("errorMessage").is_some(), fails);
            assert_eq!(data["api"], "openrouter-images");
            assert!(data["timestamp"].as_u64().unwrap() > 0);
            assert_eq!(data["responseId"], "image-response");
            assert_eq!(data["usage"]["totalTokens"], 46);
            assert_eq!(data["usage"]["cost"]["totalFromProvider"], false);
            assert_eq!(data["output"], json!(expected_output));
            server.await.unwrap();
        }
        let config = json!({"relay":{"baseUrl":"http://127.0.0.1:1/v1","apiKey":"fixture", "models":[{
            "type":"image", "id":"fixture", "api":"openrouter-images", "input":["text"], "output":["image"]
        }]}});
        let mut args = Cli::parse_from(["pi"]);
        args.model_profiles =
            super::super::model_profiles::Profiles::parse(&json!({"providers":config})).unwrap();
        let operation = Operations {
            args,
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
        };
        let cancellation = pi_rs::CancellationToken::new();
        cancellation.cancel();
        let outcome = operation.invoke(json!({"action":"generateImages", "provider":"relay", "id":"fixture", "context":{"input":[{"type":"text","text":"draw"}]}}), ToolContext {
            operation_id: uuid::Uuid::new_v4(), cancellation,
        }).await;
        let Outcome::Structured { data, .. } = outcome else {
            panic!("expected structured result")
        };
        assert_eq!(data["stopReason"], "aborted");
        assert_eq!(data["api"], "openrouter-images");
        assert!(data["timestamp"].as_u64().unwrap() > 0);
        assert_eq!(data["output"], json!([]));
    }

    #[tokio::test]
    async fn openrouter_image_early_error_retains_result_identity() {
        use clap::Parser;
        let config = json!({"relay":{"baseUrl":"http://127.0.0.1:1/v1", "models":[{
            "type":"image", "id":"fixture", "api":"openrouter-images", "input":["text"], "output":["image"]
        }]}});
        let profiles =
            super::super::model_profiles::Profiles::parse(&json!({"providers":config})).unwrap();
        let entry = profiles
            .operations()
            .0
            .iter()
            .find(|entry| entry.provider == "relay")
            .unwrap();
        let operation = Operations {
            args: Cli::parse_from(["pi"]),
            client: reqwest::Client::builder().no_proxy().build().unwrap(),
        };
        let mut data = json!({"stopReason":"stop", "output":[]});
        assert!(
            operation
                .perform(
                    entry,
                    &json!({"input":[]}),
                    &Keys::parse(&json!({})),
                    &mut data
                )
                .await
                .is_err()
        );
        assert_eq!(data["api"], "openrouter-images");
        assert!(data["timestamp"].as_u64().unwrap() > 0);
        assert_eq!(data["output"], json!([]));
    }

    #[test]
    fn openrouter_image_choices_validate_nonempty_responses() {
        let mut output = Vec::new();
        image_output(&json!({"choices":[]}), &mut output).unwrap();
        assert!(output.is_empty());
        assert!(image_output(&json!({}), &mut output).is_err());
        assert!(image_output(&json!({"choices":[{}]}), &mut output).is_err());
        image_output(
            &json!({"choices":[{"message":{"content":"description"}}]}),
            &mut output,
        )
        .unwrap();
        assert_eq!(output, vec![json!({"type":"text","text":"description"})]);
    }
    #[test]
    fn openrouter_image_usage_keeps_reported_cache_counts() {
        let config = json!({"relay":{"baseUrl":"https://relay.example/v1", "models":[{
            "type":"image", "id":"fixture", "api":"openrouter-images"
        }]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay")
            .unwrap();
        for (
            prompt,
            cached,
            written,
            expected_input,
            expected_read,
            expected_write,
            expected_total,
        ) in [
            (100, 30, 10, 70, 20, 10, 110),
            (100, 120, 0, 0, 120, 0, 130),
            (100, 30, 120, 0, 0, 120, 130),
        ] {
            let result = usage(
                entry,
                &json!({"prompt_tokens":prompt, "completion_tokens":10,
                "prompt_tokens_details":{"cached_tokens":cached, "cache_write_tokens":written}}),
            )
            .unwrap();
            assert_eq!(result["input"], expected_input);
            assert_eq!(result["cacheRead"], expected_read);
            assert_eq!(result["cacheWrite"], expected_write);
            assert_eq!(result["totalTokens"], expected_total);
        }
    }

    #[tokio::test]
    async fn configured_workers_ai_classifiers_use_cloudflare_protocol() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let direct = json!({"success":true,"result":{"answers":{"ok":{"type":"noul","noul":0.8}},"usage":{"input_tokens":8,"output_tokens":3}}});
        let nested = json!({"success":true,"result":{"state":"Completed","result":{"answers":{"ok":{"type":"noul","noul":0.7}},"usage":{"input_tokens":4,"output_tokens":2}}}});
        for (id, body, opt_in, expected_probability, expected_error) in [
            ("@cf/cloudflare/clef-1", direct, false, Some(0.8), None),
            ("@cf/typesafe/jev", nested.clone(), false, Some(0.7), None),
            ("@cf/typesafe/jev", nested, true, Some(0.7), None),
            (
                "@cf/typesafe/jev",
                json!({"success":true,"result":{"state":"Running"}}),
                false,
                None,
                Some("did not complete"),
            ),
            (
                "@cf/typesafe/jev",
                json!({"success":false,"errors":[{"message":"private provider detail"}]}),
                false,
                None,
                Some("request failed"),
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let (end, length) = loop {
                    let mut chunk = [0; 2048];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(pos) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        let end = pos + 4;
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end, length);
                    }
                };
                while request.len() < end + length {
                    let mut chunk = [0; 2048];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&chunk[..read]);
                }
                let body = body.to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                request
            });
            let compat = if opt_in {
                json!({"sendImageOptInHeader":true})
            } else {
                json!({})
            };
            let config = json!({"cloudflare-workers-ai":{"baseUrl":format!("http://{address}/ai"),"apiKey":"test-key","models":[{
                "type":"classifier","id":id,"api":"cloudflare-workers-ai-system-one", "input":["text","image"],
                "compat":compat, "cost":{"input":1.0,"output":2.0}
            }]}});
            let profiles =
                super::super::model_profiles::Profiles::parse(&json!({"providers":config}))
                    .unwrap();
            let entry = profiles
                .operations()
                .0
                .iter()
                .find(|entry| entry.provider == "cloudflare-workers-ai" && entry.model.id == id)
                .unwrap();
            assert_eq!(entry.endpoint.path(), "/ai/run");
            assert_eq!(
                entry.descriptor()["api"],
                "cloudflare-workers-ai-system-one"
            );
            let context = json!({"state":{"text":"test"}, "questions":{"ok":{"type":"bool", "instructions":"decide", "criteria":{"true":"yes", "false":"no"}}},
                "images":[{"type":"image", "mimeType":"image/png", "data":"aGVsbG8="}]});
            let operation = Operations {
                args: Cli::parse_from(["pi"]),
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            };
            let mut data = json!({});
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                operation.perform(entry, &context, &Keys::parse(&json!({})), &mut data),
            )
            .await
            .unwrap();
            if let Some(message) = expected_error {
                let error = result.unwrap_err().to_string();
                assert!(error.contains(message), "{error}");
                assert!(!error.contains("private provider detail"));
            } else {
                result.unwrap();
                assert_eq!(
                    data["answers"]["ok"]["probability"],
                    expected_probability.unwrap()
                );
                assert!(data["usage"]["totalTokens"].as_u64().unwrap() > 0);
            }
            let request = server.await.unwrap();
            let end = request
                .windows(4)
                .position(|part| part == b"\r\n\r\n")
                .unwrap()
                + 4;
            let headers = std::str::from_utf8(&request[..end])
                .unwrap()
                .to_ascii_lowercase();
            assert!(headers.starts_with("post /ai/run "));
            assert!(headers.contains("authorization: bearer test-key"));
            assert_eq!(
                headers.contains("typesafe-danger-zone-images: true"),
                opt_in
            );
            let request: Value = serde_json::from_slice(&request[end..]).unwrap();
            assert_eq!(request["model"], id);
            assert_eq!(request["input"]["questions"]["ok"]["type"], "noul");
            if id.starts_with("@cf/cloudflare/clef") {
                assert!(
                    request["input"]["images"][0]
                        .as_str()
                        .unwrap()
                        .starts_with("data:image/png;base64,")
                );
            } else {
                assert!(request["input"]["assets"]["<image-1>"].is_object());
                assert_eq!(request["input"]["state"][0], "<image-1>");
            }
        }
    }

    #[test]
    fn workers_ai_classifier_credentials_remain_endpoint_bound() {
        let config = json!({"cloudflare-workers-ai":{
            "baseUrl":"http://127.0.0.1:1111/ai", "apiKey":"provider-secret", "models":[{
                "type":"classifier", "id":"fixture", "api":"cloudflare-workers-ai-system-one", "baseUrl":"http://127.0.0.1:2222/ai"
            }]
        }});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "cloudflare-workers-ai")
            .unwrap();
        assert!(entry.api_key.is_none());
        let keys = Keys::parse(
            &json!({"cloudflare-workers-ai":{"type":"api_key", "key":"saved-secret", "endpoint":"http://127.0.0.1:1111/ai"}}),
        );
        assert!(
            entry
                .credential_with_env(&keys, |_| Some("ambient-secret".into()))
                .is_none()
        );
    }

    #[tokio::test]
    async fn workers_ai_model_endpoint_override_does_not_receive_provider_headers() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for overridden in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let (end, length) = loop {
                    let mut chunk = [0; 2048];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(pos) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        let end = pos + 4;
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end, length);
                    }
                };
                while request.len() < end + length {
                    let mut chunk = [0; 2048];
                    let read = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(read, 0);
                    request.extend_from_slice(&chunk[..read]);
                }
                let body =
                    r#"{"success":true,"result":{"answers":{"ok":{"type":"noul","noul":0.8}}}}"#;
                stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                request
            });
            let provider_base = format!("http://{address}/provider");
            let model_base = if overridden {
                format!("http://{address}/model")
            } else {
                format!("{provider_base}/")
            };
            let config = json!({"cloudflare-workers-ai":{
                "baseUrl":provider_base, "apiKey":"provider-key",
                "headers":{"X-Provider-Secret":"provider-value", "Authorization":"Bearer provider-secret", "x-shared":"provider"},
                "models":[{"type":"classifier", "id":"fixture", "api":"cloudflare-workers-ai-system-one", "baseUrl":model_base,
                    "headers":{"X-Model-Secret":"model-value", "X-Shared":"model"}}]
            }});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let entry = catalogue
                .0
                .iter()
                .find(|entry| entry.provider == "cloudflare-workers-ai")
                .unwrap();
            let keys = Keys::parse(
                &json!({"cloudflare-workers-ai":{"type":"api_key", "key":"destination-key", "endpoint":entry.endpoint.as_str()}}),
            );
            let context = json!({"state":{"text":"test"}, "questions":{"ok":{"type":"bool", "instructions":"decide", "criteria":{"true":"yes", "false":"no"}}}});
            let operation = Operations {
                args: Cli::parse_from(["pi"]),
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            };
            tokio::time::timeout(
                Duration::from_secs(10),
                operation.perform(entry, &context, &keys, &mut json!({})),
            )
            .await
            .unwrap()
            .unwrap();
            let request = tokio::time::timeout(Duration::from_secs(10), server)
                .await
                .unwrap()
                .unwrap();
            let end = request
                .windows(4)
                .position(|part| part == b"\r\n\r\n")
                .unwrap()
                + 4;
            let headers = std::str::from_utf8(&request[..end])
                .unwrap()
                .to_ascii_lowercase();
            assert!(headers.contains("x-model-secret: model-value"));
            assert!(headers.contains("x-shared: model"));
            assert!(!headers.contains("x-shared: provider"));
            assert_eq!(
                headers.contains("x-provider-secret: provider-value"),
                !overridden
            );
            if overridden {
                assert!(headers.contains("authorization: bearer destination-key"));
                assert!(!headers.contains("provider-secret"));
                assert!(!headers.contains("provider-key"));
            } else {
                assert!(headers.contains("authorization: bearer provider-secret"));
            }
        }
    }

    #[tokio::test]
    async fn operation_provider_headers_never_cross_model_endpoints() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn fixture(listener: tokio::net::TcpListener, response: &'static str) -> String {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let (end, length) = loop {
                let mut chunk = [0; 2048];
                let count = stream.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
                if let Some(pos) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    let end = pos + 4;
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (end, length);
                }
            };
            while request.len() < end + length {
                let mut chunk = [0; 2048];
                let count = stream.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
            }
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
            std::str::from_utf8(&request[..end])
                .unwrap()
                .to_ascii_lowercase()
        }

        for (kind, api, route, response, context) in [
            (
                "image",
                "openrouter-images",
                "chat/completions",
                r#"{"choices":[]}"#,
                json!({"input":[{"type":"text","text":"draw"}]}),
            ),
            (
                "classifier",
                "typesafe-system-one",
                "systemone",
                r#"{"answers":{"ok":{"type":"noul","noul":0.8}}}"#,
                json!({"state":{"text":"test"},"questions":{"ok":{"type":"bool","instructions":"decide","criteria":{"true":"yes","false":"no"}}}}),
            ),
        ] {
            let primary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let alternate = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let primary_base = format!("http://{}/v1", primary.local_addr().unwrap());
            let alternate_base = format!("http://{}/v1", alternate.local_addr().unwrap());
            let primary_server = tokio::spawn(fixture(primary, response));
            let alternate_server = tokio::spawn(fixture(alternate, response));
            let config = json!({"relay":{"baseUrl":primary_base, "apiKey":"provider-key", "headers":{"Authorization":"Bearer provider-secret", "X-Provider-Secret":"secret-value", "X-Shared":"provider-value"}, "models":[
                {"type":kind,"id":"primary","api":api,"baseUrl":format!("{primary_base}/"),"input":["text"]},
                {"type":kind,"id":"alternate","api":api,"baseUrl":alternate_base,"input":["text"],"headers":{"X-Model-Secret":"model-value","X-Shared":"model-value"}}
            ]}});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let operation = Operations {
                args: Cli::parse_from(["pi"]),
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            };
            for id in ["primary", "alternate"] {
                let entry = catalogue
                    .0
                    .iter()
                    .find(|entry| entry.provider == "relay" && entry.model.id == id)
                    .unwrap();
                assert!(entry.endpoint.path().ends_with(route));
                let keys = if id == "alternate" {
                    Keys::parse(
                        &json!({"relay":{"type":"api_key","key":"destination-key","endpoint":entry.endpoint.as_str()}}),
                    )
                } else {
                    Keys::parse(&json!({}))
                };
                let mut data = json!({"output":[]});
                tokio::time::timeout(
                    Duration::from_secs(10),
                    operation.perform(entry, &context, &keys, &mut data),
                )
                .await
                .unwrap()
                .unwrap();
            }
            let primary = tokio::time::timeout(Duration::from_secs(5), primary_server)
                .await
                .unwrap()
                .unwrap();
            let alternate = tokio::time::timeout(Duration::from_secs(5), alternate_server)
                .await
                .unwrap()
                .unwrap();
            assert!(primary.contains("authorization: bearer provider-secret"));
            assert!(primary.contains("x-provider-secret: secret-value"));
            assert!(primary.contains("x-shared: provider-value"));
            assert!(alternate.contains("authorization: bearer destination-key"));
            assert!(alternate.contains("x-model-secret: model-value"));
            assert!(alternate.contains("x-shared: model-value"));
            assert!(!alternate.contains("provider-secret"));
            assert!(!alternate.contains("secret-value"));
            assert!(!alternate.contains("provider-value"));
            assert!(!alternate.contains("provider-key"));
        }
    }

    #[test]
    fn operation_provider_headers_are_scoped_for_every_api() {
        let config = json!({"relay":{"baseUrl":"https://provider.example/ai", "headers":{"X-Provider":"value"}, "models":[
            {"type":"classifier", "id":"workers", "api":"cloudflare-workers-ai-system-one", "baseUrl":"https://other.example/ai", "headers":{"X-Model":"kept"}},
            {"type":"classifier", "id":"system", "api":"typesafe-system-one", "baseUrl":"https://other.example/ai"}
        ]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let workers = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay" && entry.model.id == "workers")
            .unwrap();
        assert!(!workers.model.headers.contains_key("x-provider"));
        assert_eq!(workers.model.headers["x-model"], "kept");
        let system = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay" && entry.model.id == "system")
            .unwrap();
        assert!(!system.model.headers.contains_key("x-provider"));
    }
    #[test]
    fn workers_ai_classifier_honors_image_format_override() {
        let config = json!({"relay": {"baseUrl":"http://127.0.0.1:1234/ai", "models":[{
            "type":"classifier", "id":"@cf/typesafe/jev", "api":"cloudflare-workers-ai-system-one", "input":["text","image"],
            "compat":{"imageInput":"state-image-urls"}
        }]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay")
            .unwrap();
        let context = json!({"state":{"text":"test"}, "questions":{"ok":{"type":"bool", "instructions":"test", "criteria":{"true":"yes", "false":"no"}}},
            "images":[{"type":"image","mimeType":"image/png","data":"aGVsbG8="}]});
        let (body, opt_in) = classifier_payload(entry, &context).unwrap();
        assert_eq!(body["input"]["state"][0]["type"], "image_url");
        assert!(!opt_in);
        let mut without_image = context;
        without_image["images"] = json!([]);
        let (body, opt_in) = classifier_payload(entry, &without_image).unwrap();
        assert_eq!(body["input"]["state"], json!({"text":"test"}));
        assert!(!opt_in);
    }

    #[test]
    fn classifier_rejects_non_object_state_before_request() {
        let config = json!({"relay":{"baseUrl":"http://127.0.0.1:1234", "models":[{
            "type":"classifier", "id":"fixture", "api":"typesafe-system-one"
        }]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay")
            .unwrap();
        let questions = json!({"ok":{"type":"bool","instructions":"test","criteria":{"true":"yes","false":"no"}}});
        for state in [json!(null), json!([]), json!("text"), json!(42)] {
            let error = classifier_payload(entry, &json!({"state":state,"questions":questions}))
                .unwrap_err();
            assert!(error.to_string().contains("state must be an object"));
        }
        assert!(classifier_payload(entry, &json!({"state":{},"questions":questions})).is_ok());
    }

    #[tokio::test]
    async fn classifier_retries_transient_http_failures_with_server_controls() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (api, responses, expected_success, expected_error) in [
            (
                Api::SystemOne,
                vec![(429, "retry-after-ms: 0\r\n"), (200, "")],
                true,
                "",
            ),
            (
                Api::SystemOne,
                vec![
                    (400, "x-should-retry: true\r\nretry-after-ms: 0\r\n"),
                    (200, ""),
                ],
                true,
                "",
            ),
            (
                Api::SystemOne,
                vec![(503, "x-should-retry: false\r\n")],
                false,
                "HTTP 503",
            ),
            (Api::SystemOne, vec![(400, "")], false, "HTTP 400"),
            (
                Api::SystemOne,
                vec![
                    (429, "retry-after-ms: 0\r\n"),
                    (429, "retry-after-ms: 0\r\n"),
                    (429, "retry-after-ms: 0\r\n"),
                ],
                false,
                "HTTP 429",
            ),
            (
                Api::SystemOne,
                vec![(429, "retry-after-ms: 60001\r\n")],
                false,
                "over 60 seconds",
            ),
            (
                Api::OpenRouterImages,
                vec![(429, "retry-after-ms: 0\r\n")],
                false,
                "HTTP 429",
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let mut requests = Vec::new();
                for (status, headers) in responses {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let (end, length) = loop {
                        let mut chunk = [0u8; 1024];
                        let read = stream.read(&mut chunk).await.unwrap();
                        assert_ne!(read, 0);
                        request.extend_from_slice(&chunk[..read]);
                        if let Some(position) =
                            request.windows(4).position(|part| part == b"\r\n\r\n")
                        {
                            let end = position + 4;
                            let header = std::str::from_utf8(&request[..end]).unwrap();
                            let length = header
                                .lines()
                                .find_map(|line| {
                                    let (name, value) = line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap();
                            break (end, length);
                        }
                    };
                    while request.len() < end + length {
                        let mut chunk = [0u8; 1024];
                        let read = stream.read(&mut chunk).await.unwrap();
                        assert_ne!(read, 0);
                        request.extend_from_slice(&chunk[..read]);
                    }
                    requests.push(request[end..end + length].to_vec());
                    stream.write_all(format!("HTTP/1.1 {status} Fixture\r\n{headers}content-length: 0\r\nconnection: close\r\n\r\n").as_bytes()).await.unwrap();
                }
                requests
            });
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let request = client
                .post(format!("http://{address}/test"))
                .header("authorization", "Bearer fixture")
                .body("fixture-body")
                .build()
                .unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                send_model_request(&client, request, api),
            )
            .await
            .unwrap();
            assert_eq!(result.is_ok(), expected_success);
            if let Err(error) = result {
                assert!(error.to_string().contains(expected_error), "{error}");
            }
            let requests = tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap();
            assert!(requests.iter().all(|request| request == b"fixture-body"));
        }
    }

    #[test]
    fn classifier_retry_delays_honor_server_limit_and_dates() {
        let now = chrono::DateTime::parse_from_rfc2822("Wed, 21 Oct 2015 07:28:00 GMT")
            .unwrap()
            .to_utc();
        let mut headers = HeaderMap::new();
        assert!(
            (Duration::from_millis(375)..=Duration::from_millis(500))
                .contains(&classifier_retry_delay(&headers, 0, now).unwrap())
        );
        assert!(
            (Duration::from_millis(750)..=Duration::from_secs(1))
                .contains(&classifier_retry_delay(&headers, 1, now).unwrap())
        );
        headers.insert(
            "retry-after",
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:05 GMT"),
        );
        assert_eq!(
            classifier_retry_delay(&headers, 0, now).unwrap(),
            Duration::from_secs(5)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("12.5"));
        assert_eq!(
            classifier_retry_delay(&headers, 0, now).unwrap(),
            Duration::from_micros(12500)
        );
        headers.insert("retry-after-ms", HeaderValue::from_static("60001"));
        assert!(classifier_retry_delay(&headers, 0, now).is_err());
    }

    #[tokio::test]
    async fn cancellation_interrupts_classifier_retry_wait() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sent, received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&chunk[..read]);
            }
            stream.write_all(b"HTTP/1.1 429 Fixture\r\nretry-after-ms: 5000\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await.unwrap();
            sent.send(()).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(250), listener.accept())
                    .await
                    .is_err()
            );
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let request = client
            .post(format!("http://{address}/test"))
            .body("")
            .build()
            .unwrap();
        let cancellation = pi_rs::CancellationToken::new();
        let operation = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => true,
                    _ = send_model_request(&client, request, Api::SystemOne) => false,
                }
            }
        });
        received.await.unwrap();
        cancellation.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), operation)
                .await
                .unwrap()
                .unwrap()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn classifier_retries_timeouts_before_headers_and_during_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (body_stall, cancel) in [(false, false), (true, false), (false, true), (true, true)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (ready, received) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut first, _) = listener.accept().await.unwrap();
                let mut bytes = [0u8; 2048];
                assert_ne!(first.read(&mut bytes).await.unwrap(), 0);
                if body_stall {
                    first
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 12\r\n\r\n{")
                        .await
                        .unwrap();
                }
                ready.send(()).unwrap();
                if cancel {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(1500), listener.accept())
                            .await
                            .is_err(),
                        "cancelled request was retried"
                    );
                } else {
                    let (mut second, _) =
                        tokio::time::timeout(Duration::from_secs(5), listener.accept())
                            .await
                            .expect("timeout was not retried")
                            .unwrap();
                    assert_ne!(second.read(&mut bytes).await.unwrap(), 0);
                    second
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                        )
                        .await
                        .unwrap();
                }
                drop(first);
            });
            let client = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_millis(500))
                .build()
                .unwrap();
            let request = client
                .post(format!("http://{address}/test"))
                .body("fixture")
                .build()
                .unwrap();
            let cancellation = pi_rs::CancellationToken::new();
            let operation = tokio::spawn({
                let cancellation = cancellation.clone();
                async move {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => None,
                        result = send_model_request(&client, request, Api::SystemOne) => Some(result),
                    }
                }
            });
            received.await.unwrap();
            if cancel {
                cancellation.cancel();
            }
            let result = tokio::time::timeout(Duration::from_secs(5), operation)
                .await
                .unwrap()
                .unwrap();
            if cancel {
                assert!(result.is_none());
            } else {
                assert_eq!(result.unwrap().unwrap(), b"ok");
            }
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn classifier_image_header_defaults_are_independent_of_wire_format() {
        use clap::Parser;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (
            provider,
            mode,
            override_header,
            with_image,
            expected,
            provider_header,
            model_header,
            expected_custom,
        ) in [
            (
                "typesafe",
                "state-image-urls",
                None,
                true,
                Some("true"),
                None,
                None,
                None,
            ),
            ("openrouter", "assets", None, true, None, None, None, None),
            (
                "openrouter",
                "assets",
                Some(true),
                true,
                Some("true"),
                None,
                None,
                None,
            ),
            (
                "typesafe",
                "images",
                Some(false),
                true,
                None,
                None,
                None,
                None,
            ),
            (
                "typesafe",
                "state-image-urls",
                None,
                false,
                None,
                None,
                None,
                None,
            ),
            (
                "openrouter",
                "assets",
                Some(true),
                false,
                None,
                None,
                None,
                None,
            ),
            (
                "typesafe",
                "assets",
                None,
                true,
                Some("true"),
                Some(("TypeSafe-Danger-Zone-Images", "false")),
                Some(("typesafe-danger-zone-images", "false")),
                None,
            ),
            (
                "typesafe",
                "assets",
                Some(false),
                true,
                Some("false"),
                Some(("typesafe-danger-zone-images", "false")),
                None,
                None,
            ),
            (
                "typesafe",
                "assets",
                None,
                false,
                Some("false"),
                None,
                Some(("TypeSafe-Danger-Zone-Images", "false")),
                None,
            ),
            (
                "typesafe",
                "assets",
                None,
                true,
                Some("true"),
                Some(("x-fixture", "provider")),
                Some(("X-Fixture", "model")),
                Some("model"),
            ),
            (
                "typesafe",
                "assets",
                None,
                true,
                Some("true"),
                Some(("X-Fixture", "provider")),
                Some(("x-fixture", "model")),
                Some("model"),
            ),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let (header_end, content_length) = loop {
                    let mut chunk = [0u8; 2048];
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&chunk[..count]);
                    if let Some(pos) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                        let end = pos + 4;
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (end, length);
                    }
                };
                while request.len() < header_end + content_length {
                    let mut chunk = [0u8; 2048];
                    let count = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&chunk[..count]);
                }
                let response = b"{\"answers\":{\"ok\":{\"type\":\"noul\",\"noul\":0.6}}}";
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).as_bytes()).await.unwrap();
                stream.write_all(response).await.unwrap();
                request
            });
            let mut compat = json!({"imageInput":mode});
            if let Some(value) = override_header {
                compat["sendImageOptInHeader"] = json!(value);
            }
            let provider_headers: BTreeMap<_, _> = provider_header.into_iter().collect();
            let model_headers: BTreeMap<_, _> = model_header.into_iter().collect();
            let config = json!({provider:{"baseUrl":format!("http://{address}/v1"), "apiKey":"test-key", "headers":provider_headers, "models":[{
                "type":"classifier", "id":"parity-fixture", "api":"typesafe-system-one", "input":["text","image"], "compat":compat, "headers":model_headers
            }]}});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let entry = catalogue
                .0
                .iter()
                .find(|entry| entry.provider == provider && entry.model.id == "parity-fixture")
                .unwrap();
            let context = json!({"state":{"text":"test"}, "questions":{"ok":{"type":"bool", "instructions":"decide", "criteria":{"true":"yes", "false":"no"}}},
                "images":if with_image { json!([{"type":"image", "mimeType":"image/png", "data":"aGVsbG8="}]) } else { json!([]) }});
            let operation = Operations {
                args: Cli::parse_from(["pi"]),
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
            };
            let mut data = json!({});
            tokio::time::timeout(
                Duration::from_secs(10),
                operation.perform(entry, &context, &Keys::parse(&json!({})), &mut data),
            )
            .await
            .unwrap()
            .unwrap();
            let request = tokio::time::timeout(Duration::from_secs(10), server)
                .await
                .unwrap()
                .unwrap();
            let end = request
                .windows(4)
                .position(|part| part == b"\r\n\r\n")
                .unwrap()
                + 4;
            let headers = std::str::from_utf8(&request[..end]).unwrap();
            let header_values = |wanted: &str| {
                headers
                    .lines()
                    .filter_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case(wanted).then(|| value.trim())
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                header_values("typesafe-danger-zone-images"),
                expected.into_iter().collect::<Vec<_>>(),
                "{provider} {mode} {override_header:?}"
            );
            assert_eq!(
                header_values("x-fixture"),
                expected_custom.into_iter().collect::<Vec<_>>()
            );
            let body: Value = serde_json::from_slice(&request[end..]).unwrap();
            assert_eq!(body["questions"]["ok"]["type"], "noul");
            if with_image {
                match mode {
                    "assets" => assert!(body["assets"]["<image-1>"].is_object()),
                    "state-image-urls" => assert_eq!(body["state"][0]["type"], "image_url"),
                    "images" => assert!(
                        body["images"][0]
                            .as_str()
                            .unwrap()
                            .starts_with("data:image/png;base64,")
                    ),
                    _ => unreachable!(),
                }
            }
            assert_eq!(data["answers"]["ok"]["probability"], 0.6);
        }
    }

    #[test]
    fn classifier_header_default_detects_openrouter_host_independently() {
        let config = json!({"relay":{"baseUrl":"https://eu.openrouter.ai/api/v1", "models":[{
            "type":"classifier", "id":"fixture", "api":"typesafe-system-one", "input":["text","image"], "compat":{"imageInput":"assets"}
        }]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "relay")
            .unwrap();
        let context = json!({"state":{"text":"test"}, "questions":{"ok":{"type":"bool","instructions":"test","criteria":{"true":"yes","false":"no"}}}, "images":[{"type":"image","mimeType":"image/png","data":"aGVsbG8="}]});
        let (body, opt_in) = classifier_payload(entry, &context).unwrap();
        assert!(body["assets"]["<image-1>"].is_object());
        assert!(!opt_in);
    }

    #[tokio::test]
    async fn operation_credentials_prefer_stored_then_configured_then_environment() {
        for (provider, variable) in [
            ("typesafe", "TYPESAFE_API_KEY"),
            ("openrouter", "OPENROUTER_API_KEY"),
        ] {
            let config = json!({provider: {"apiKey":"configured-key"}});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let entry = catalogue
                .0
                .iter()
                .find(|entry| entry.provider == provider)
                .unwrap();
            let env = |name: &str| (name == variable).then(|| "environment-key".to_owned());
            let empty = Keys::parse(&json!({}));
            assert_eq!(
                entry
                    .credential_with_env(&empty, env)
                    .and_then(|key| key.literal().map(str::to_owned))
                    .as_deref(),
                Some("configured-key")
            );
            let saved = Keys::parse(&json!({provider: {"type":"api_key", "key":"stored-key"}}));
            assert_eq!(
                entry
                    .credential_with_env(&saved, env)
                    .and_then(|key| key.literal().map(str::to_owned))
                    .as_deref(),
                Some("stored-key")
            );
            let mut no_config = entry.clone();
            no_config.api_key = None;
            assert_eq!(
                no_config
                    .credential_with_env(&empty, env)
                    .and_then(|key| key.literal().map(str::to_owned))
                    .as_deref(),
                Some("environment-key")
            );
            no_config.endpoint = Url::parse("https://proxy.example/v1/systemone").unwrap();
            assert!(no_config.credential_with_env(&empty, env).is_none());
            let mut invalid = entry.clone();
            invalid.api_key = Some("${PI_RS_MISSING_OPERATION_KEY_TEST_8E43B}".into());
            assert!(
                invalid
                    .credential_with_env(&empty, env)
                    .unwrap()
                    .resolve()
                    .await
                    .is_err()
            );
            invalid.api_key = Some(String::new());
            assert!(
                invalid
                    .credential_with_env(&empty, env)
                    .unwrap()
                    .resolve()
                    .await
                    .is_err()
            );
            for key in ["", "${PI_RS_MISSING_OPERATION_KEY_TEST_8E43B}"] {
                let saved = Keys::parse(&json!({provider: {"type":"api_key", "key":key}}));
                assert!(
                    entry
                        .credential_with_env(&saved, env)
                        .unwrap()
                        .resolve()
                        .await
                        .is_err()
                );
            }
        }
    }

    #[tokio::test]
    async fn missing_stored_key_falls_back_and_stored_env_resolves_headers() {
        let catalogue = Catalogue::parse(json!({}).as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "typesafe")
            .unwrap();
        let keys = Keys::parse(
            &json!({"typesafe":{"type":"api_key", "env":{"HEADER_SECRET":"local-header"}}}),
        );
        let env = |name: &str| (name == "TYPESAFE_API_KEY").then(|| "environment-key".to_owned());
        assert_eq!(
            entry
                .credential_with_env(&keys, env)
                .unwrap()
                .resolve()
                .await
                .unwrap(),
            "environment-key"
        );
        let stored_env =
            keys.operation_environment("typesafe", entry.endpoint.as_str(), entry.is_official());
        assert_eq!(
            resolve_header("$HEADER_SECRET", stored_env)
                .await
                .as_deref(),
            Some("local-header")
        );
        assert!(
            keys.operation_environment("typesafe", "https://other.example/v1/systemone", false)
                .is_none()
        );
        let invalid = Keys::parse(
            &json!({"typesafe":{"type":"api_key", "key":"$PI_RS_MISSING_KEY_TEST_3F64"}}),
        );
        assert!(
            entry
                .credential_with_env(&invalid, env)
                .unwrap()
                .resolve()
                .await
                .is_err()
        );
    }

    #[test]
    fn operation_endpoints_normalize_complete_route_trailing_slashes() {
        let (_, plain) = endpoints("https://proxy.example/v1/systemone", "systemone").unwrap();
        let (_, slash) = endpoints("https://proxy.example/v1/systemone/", "systemone").unwrap();
        assert_eq!(plain, slash);
        let config = json!({"relay":{"apiKey":"secret", "models":[
            {"type":"classifier", "id":"one", "api":"typesafe-system-one", "baseUrl":"https://proxy.example/v1/systemone/"},
            {"type":"classifier", "id":"two", "api":"typesafe-system-one", "baseUrl":"https://proxy.example/v1/systemone"}
        ]}});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        for entry in catalogue.0.iter().filter(|entry| entry.provider == "relay") {
            assert_eq!(entry.endpoint, plain);
            assert_eq!(entry.api_key.as_deref(), Some("secret"));
        }
    }

    #[tokio::test]
    async fn operation_uses_slash_terminated_saved_endpoint_before_configured_key() {
        let config = json!({"proxy":{
            "baseUrl":"https://proxy.example/v1", "apiKey":"configured-key",
            "models":[{"type":"classifier", "id":"classifier", "api":"typesafe-system-one"}]
        }});
        let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
        let entry = catalogue
            .0
            .iter()
            .find(|entry| entry.provider == "proxy")
            .unwrap();
        let keys = Keys::parse(&json!({"proxy":{
            "type":"api_key", "key":"stored-key", "endpoint":"https://proxy.example/v1/systemone/",
            "env":{"HEADER":"saved-header"}
        }}));
        assert_eq!(
            entry.credential(&keys).unwrap().resolve().await.unwrap(),
            "stored-key"
        );
        let env = keys.operation_environment("proxy", entry.endpoint.as_str(), entry.is_official());
        assert_eq!(
            resolve_header("$HEADER", env).await.as_deref(),
            Some("saved-header")
        );
    }

    #[tokio::test]
    async fn slash_terminated_official_saved_bases_keep_key_precedence() {
        for (provider, base, variable) in [
            ("typesafe", "https://api.typesafe.ai/v1", "TYPESAFE_API_KEY"),
            (
                "openrouter",
                "https://openrouter.ai/api/v1",
                "OPENROUTER_API_KEY",
            ),
        ] {
            let config = json!({provider:{"apiKey":"configured-key"}});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let entries: Vec<_> = catalogue
                .0
                .iter()
                .filter(|entry| entry.provider == provider && entry.is_official())
                .collect();
            assert!(!entries.is_empty());
            let env = |name: &str| (name == variable).then(|| "ambient-key".to_owned());
            for saved_base in [base.to_owned(), format!("{base}/")] {
                let keys = Keys::parse(
                    &json!({provider:{"type":"api_key", "key":"stored-key", "endpoint":saved_base, "env":{"HEADER":"stored-header"}}}),
                );
                for entry in &entries {
                    assert_eq!(
                        entry
                            .credential_with_env(&keys, env)
                            .unwrap()
                            .resolve()
                            .await
                            .unwrap(),
                        "stored-key"
                    );
                    let header_env =
                        keys.operation_environment(provider, entry.endpoint.as_str(), true);
                    assert_eq!(
                        resolve_header("$HEADER", header_env).await.as_deref(),
                        Some("stored-header")
                    );
                    assert!(
                        keys.operation_credential(
                            provider,
                            "https://other.example/v1/systemone",
                            false
                        )
                        .is_none()
                    );
                }
            }
            let other = Keys::parse(
                &json!({provider:{"type":"api_key", "key":"wrong-key", "endpoint":"https://other.example/v1/"}}),
            );
            for entry in entries {
                assert_eq!(
                    entry
                        .credential_with_env(&other, env)
                        .unwrap()
                        .resolve()
                        .await
                        .unwrap(),
                    "configured-key"
                );
                assert!(
                    other
                        .operation_environment(provider, entry.endpoint.as_str(), true)
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn configured_key_is_bound_to_provider_endpoint_not_model_override() {
        for (base, override_base) in [
            ("https://api.typesafe.ai/v1", "https://proxy.example/v1"),
            ("https://proxy.example/v1", "https://api.typesafe.ai/v1"),
        ] {
            let config = json!({"typesafe": {
                "baseUrl": base, "apiKey": "provider-key",
                "models": [
                    {"type":"classifier", "id":"same", "api":"typesafe-system-one"},
                    {"type":"classifier", "id":"override", "api":"typesafe-system-one", "baseUrl":override_base}
                ]
            }});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let find = |id| {
                catalogue
                    .0
                    .iter()
                    .find(|entry| entry.provider == "typesafe" && entry.model.id == id)
                    .unwrap()
            };
            assert_eq!(find("same").api_key.as_deref(), Some("provider-key"));
            assert!(find("override").api_key.is_none());
            let keys = Keys::parse(&json!({}));
            let env = |name: &str| (name == "TYPESAFE_API_KEY").then(|| "ambient-key".to_owned());
            assert_eq!(
                find("same")
                    .credential_with_env(&keys, env)
                    .and_then(|key| key.literal().map(str::to_owned))
                    .as_deref(),
                Some("provider-key")
            );
            assert_eq!(
                find("override")
                    .credential_with_env(&keys, env)
                    .and_then(|key| key.literal().map(str::to_owned))
                    .as_deref(),
                (override_base == "https://api.typesafe.ai/v1").then_some("ambient-key")
            );
        }
    }

    #[test]
    fn configured_key_uses_unambiguous_model_base_when_provider_has_none() {
        let model = |id, base| json!({"type":"classifier", "id":id, "api":"typesafe-system-one", "baseUrl":base});
        for (models, expected) in [
            (vec![model("one", "https://proxy.example/v1")], true),
            (
                vec![
                    model("one", "https://proxy.example/v1"),
                    model("two", "https://proxy.example/v1/"),
                ],
                true,
            ),
            (
                vec![
                    model("one", "https://proxy.example/v1"),
                    model("two", "https://other.example/v1"),
                ],
                false,
            ),
        ] {
            let config = json!({"relay":{"apiKey":"provider-key", "models":models}});
            let catalogue = Catalogue::parse(config.as_object().unwrap()).unwrap();
            let entries: Vec<_> = catalogue
                .0
                .iter()
                .filter(|entry| entry.provider == "relay")
                .collect();
            assert!(!entries.is_empty());
            for entry in entries {
                assert_eq!(entry.api_key.as_deref(), expected.then_some("provider-key"));
            }
        }
    }

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
