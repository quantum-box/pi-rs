//! Native Google Gemini Generate Content and model catalog support.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, Model, ModelApi, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    ProviderReplay, ResolvedModelIdentity, Result, ToolCall, Usage, extract_tool_images,
};
use reqwest::{
    Client, Response, Url,
    header::{HeaderMap, HeaderValue},
};
use serde_json::{Value, json};

pub const DEFAULT_ENDPOINT: &str = "https://generativelanguage.googleapis.com/v1beta";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    pub id: String,
    pub context_window: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub reasoning: bool,
    pub thinking_levels: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Progress {
    WaitingForModel,
    Thinking,
    Generating,
    PreparingTool,
}

type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct Google {
    client: Client,
    http_proxies: Vec<reqwest::Proxy>,
    endpoint: Url,
    api_key: HeaderValue,
    provider: Arc<str>,
    replay_scope: Arc<str>,
    headers: HeaderMap,
    catalog: Arc<RwLock<BTreeMap<String, CatalogEntry>>>,
    probe_context: bool,
    thinking: Option<String>,
    progress: Option<Arc<dyn Fn(Progress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
}

impl Google {
    pub fn new(endpoint: &str, key: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .map_err(|_| Error::Config("invalid Google Gemini API base URL".into()))?;
        let loopback = matches!(
            endpoint.host_str(),
            Some("localhost" | "127.0.0.1" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Config("Google Gemini base URL must use HTTPS or loopback HTTP with no credentials, query, or fragment".into()));
        }
        let mut api_key = HeaderValue::from_str(key)
            .ok()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| Error::Config("missing or invalid Google Gemini API key".into()))?;
        api_key.set_sensitive(true);
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::Config("could not build Google Gemini HTTP client".into()))?;
        Ok(Self {
            client,
            http_proxies: Vec::new(),
            endpoint,
            api_key,
            provider: Arc::from("google"),
            replay_scope: Arc::from("google"),
            headers: HeaderMap::new(),
            catalog: Arc::default(),
            probe_context: true,
            thinking: None,
            progress: None,
            reasoning: None,
        })
    }

    /// Set the per-read HTTP idle timeout; `None` disables it.
    pub fn with_http_idle_timeout(self, timeout: Option<Duration>) -> Result<Self> {
        let proxies = self.http_proxies.clone();
        self.with_http_options(timeout, &proxies)
    }

    /// Configure the per-read deadline and explicit HTTP proxies.
    pub fn with_http_options(
        mut self,
        timeout: Option<Duration>,
        proxies: &[reqwest::Proxy],
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10));
        for proxy in proxies {
            builder = builder.proxy(proxy.clone());
        }
        if let Some(timeout) = timeout {
            builder = builder.read_timeout(timeout);
        }
        self.client = builder
            .build()
            .map_err(|_| Error::Config("could not build Google Gemini HTTP client".into()))?;
        self.http_proxies = proxies.to_vec();
        Ok(self)
    }

    /// Bind signed Gemini parts to a named provider and its model route.
    pub fn with_provider(mut self, provider: &str, replay_scope: impl Into<Arc<str>>) -> Self {
        self.provider = Arc::from(provider);
        self.replay_scope = replay_scope.into();
        self
    }

    /// Return the configured request headers for protected context identity.
    pub fn context_headers(&self) -> reqwest::header::HeaderMap {
        self.headers.clone()
    }

    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_context_probe(mut self, enabled: bool) -> Self {
        self.probe_context = enabled;
        self
    }
    pub fn with_thinking(mut self, thinking: Option<&str>) -> Self {
        self.thinking = thinking.map(str::to_owned);
        self
    }
    pub fn with_progress(mut self, observer: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(observer));
        self
    }
    pub fn with_reasoning(mut self, observer: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.reasoning = Some(Arc::new(observer));
        self
    }

    fn url(&self, resource: Option<&str>) -> Url {
        let mut url = self.endpoint.clone();
        let mut segments = url.path_segments_mut().expect("HTTP base URL");
        segments.pop_if_empty().push("models");
        if let Some(resource) = resource {
            segments.push(resource);
        }
        drop(segments);
        url
    }

    async fn read_model(&self, url: Url) -> Result<Value> {
        let response = self
            .client
            .get(url)
            .header("x-goog-api-key", self.api_key.clone())
            .headers(self.headers.clone())
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|_| Error::Model("Google model catalog request failed".into()))?;
        let mut response = check_status(response)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::Model("Google model catalog response interrupted".into()))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                return Err(Error::Model(
                    "Google model catalog exceeds size limit".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn cached_model(&self, id: &str) -> Option<CatalogEntry> {
        self.catalog
            .read()
            .expect("Google catalog")
            .get(model_id(id))
            .cloned()
    }

    pub async fn model(&self, id: &str) -> Option<CatalogEntry> {
        let id = model_id(id);
        if !valid_model_id(id) {
            return None;
        }
        if let Some(entry) = self.cached_model(id) {
            return Some(entry);
        }
        let raw = self.read_model(self.url(Some(id))).await.ok()?;
        let entry = catalog_entry(&raw)?;
        if entry.id != id {
            return None;
        }
        self.catalog
            .write()
            .expect("Google catalog")
            .insert(entry.id.clone(), entry.clone());
        Some(entry)
    }

    pub async fn models(&self) -> Result<Vec<CatalogEntry>> {
        let mut entries = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let mut page = None::<String>;
        for _ in 0..32 {
            let mut url = self.url(None);
            if let Some(cursor) = &page {
                url.query_pairs_mut().append_pair("pageToken", cursor);
            }
            let value = self.read_model(url).await?;
            let models = value["models"]
                .as_array()
                .ok_or_else(|| Error::Model("invalid Google model catalog".into()))?;
            for raw in models {
                if let Some(entry) = catalog_entry(raw) {
                    entries.insert(entry.id.clone(), entry);
                }
            }
            page = value["nextPageToken"]
                .as_str()
                .filter(|token| !token.is_empty())
                .map(str::to_owned);
            let Some(cursor) = &page else {
                self.catalog
                    .write()
                    .expect("Google catalog")
                    .extend(entries.clone());
                return Ok(entries.into_values().collect());
            };
            if !seen.insert(cursor.clone()) {
                return Err(Error::Model(
                    "Google model catalog repeated a page token".into(),
                ));
            }
        }
        Err(Error::Model(
            "Google model catalog exceeded pagination limit".into(),
        ))
    }
}

fn model_id(model: &str) -> &str {
    model.strip_prefix("models/").unwrap_or(model)
}

fn valid_model_id(id: &str) -> bool {
    !id.is_empty()
        && !matches!(id, "." | "..")
        && id.len() <= 160
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

fn catalog_entry(raw: &Value) -> Option<CatalogEntry> {
    let id = model_id(raw["name"].as_str()?);
    if !valid_model_id(id)
        || !raw["supportedGenerationMethods"]
            .as_array()
            .is_some_and(|methods| methods.iter().any(|method| method == "generateContent"))
    {
        return None;
    }
    let enabled = raw["thinking"]
        .as_bool()
        .unwrap_or_else(|| family(id).is_some());
    let limit = |field: &str| {
        raw[field]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n != 0)
    };
    Some(CatalogEntry {
        id: id.into(),
        context_window: limit("inputTokenLimit"),
        max_output_tokens: limit("outputTokenLimit"),
        reasoning: enabled,
        thinking_levels: available_thinking(id, enabled)
            .into_iter()
            .map(str::to_owned)
            .collect(),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ThinkingFamily {
    LevelPro,
    LevelFlash,
    BudgetPro,
    BudgetFlash,
}

fn family(id: &str) -> Option<ThinkingFamily> {
    let normalized = id.to_ascii_lowercase();
    let id = normalized.as_str();
    if gemini_major(id).is_some_and(|version| version >= 3)
        || matches!(id, "gemini-flash-latest" | "gemini-flash-lite-latest")
        || id.starts_with("gemma-4")
        || id.starts_with("gemma4-")
    {
        Some(if id.contains("-pro") {
            ThinkingFamily::LevelPro
        } else {
            ThinkingFamily::LevelFlash
        })
    } else if id.starts_with("gemini-2.5") {
        Some(if id.contains("-pro") {
            ThinkingFamily::BudgetPro
        } else {
            ThinkingFamily::BudgetFlash
        })
    } else {
        None
    }
}

fn gemini_major(id: &str) -> Option<u32> {
    let id = id.to_ascii_lowercase();
    let version = id.strip_prefix("gemini-")?;
    let version = version.strip_prefix("live-").unwrap_or(version);
    version
        .bytes()
        .take_while(u8::is_ascii_digit)
        .fold(None, |value: Option<u32>, digit| {
            Some(
                value
                    .unwrap_or(0)
                    .saturating_mul(10)
                    .saturating_add((digit - b'0') as u32),
            )
        })
}

/// Whether the model expects call IDs in both function calls and their responses.
pub fn uses_tool_call_ids(model: &str) -> bool {
    let id = model_id(model);
    gemini_major(id).is_some_and(|version| version >= 3)
        || id.starts_with("claude-")
        || id.starts_with("gpt-oss-")
}

pub fn model_thinking_levels(id: &str) -> Vec<String> {
    let id = model_id(id);
    available_thinking(id, family(id).is_some())
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn available_thinking(id: &str, enabled: bool) -> Vec<&'static str> {
    if !enabled {
        return vec!["off"];
    }
    match family(id) {
        Some(ThinkingFamily::LevelPro) => vec!["low", "high"],
        Some(ThinkingFamily::LevelFlash) => vec!["minimal", "low", "medium", "high"],
        Some(ThinkingFamily::BudgetPro) => vec!["minimal", "low", "medium", "high"],
        Some(ThinkingFamily::BudgetFlash) => {
            vec!["off", "minimal", "low", "medium", "high"]
        }
        None => vec!["low"],
    }
}

fn thinking_config(id: &str, level: Option<&str>, max_tokens: u32) -> Option<Value> {
    let level = level?;
    let normalized = id.to_ascii_lowercase();
    let id = normalized.as_str();
    let family = family(id)?;
    match family {
        ThinkingFamily::LevelPro | ThinkingFamily::LevelFlash => {
            let high = matches!(level, "high" | "xhigh" | "max");
            let native = if high {
                "HIGH"
            } else if family == ThinkingFamily::LevelPro {
                "LOW"
            } else {
                match level {
                    "off" | "minimal" => "MINIMAL",
                    "medium" => "MEDIUM",
                    _ => "LOW",
                }
            };
            Some(json!({"thinkingLevel":native,"includeThoughts":level!="off"}))
        }
        ThinkingFamily::BudgetPro | ThinkingFamily::BudgetFlash => {
            let minimum = if family == ThinkingFamily::BudgetPro {
                128
            } else {
                0
            };
            let requested = match level {
                "off" => 0,
                "minimal" if id.contains("flash-lite") => 512,
                "minimal" => 128,
                "low" => 2_048,
                "medium" => 8_192,
                _ if family == ThinkingFamily::BudgetPro => 32_768,
                _ => 24_576,
            };
            let budget = requested.min(max_tokens.saturating_sub(1)).max(minimum);
            Some(json!({"thinkingBudget":budget,"includeThoughts":level!="off"}))
        }
    }
}

fn check_status(response: Response) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let message = format!("Google Gemini returned HTTP {status}");
    if status.as_u16() == 429 || status.is_server_error() {
        Err(Error::RetryableModel(message))
    } else {
        Err(Error::Model(message))
    }
}

fn identity(provider: &str, model: &str) -> ResolvedModelIdentity {
    ResolvedModelIdentity {
        provider: provider.into(),
        model: model.into(),
    }
}

fn identity_for_scope(provider: &str, model: &str) -> ResolvedModelIdentity {
    identity(provider, model)
}

fn push_content(contents: &mut Vec<Value>, role: &str, parts: Vec<Value>, merge: bool) {
    if parts.is_empty() {
        return;
    }
    let tool_result = parts
        .first()
        .is_some_and(|part| part.get("functionResponse").is_some());
    if merge
        && let Some(last) = contents.last_mut().filter(|last| {
            last["role"] == role
                && last["parts"][0].get("functionResponse").is_some() == tool_result
        })
    {
        last["parts"]
            .as_array_mut()
            .expect("content parts")
            .extend(parts);
    } else {
        contents.push(json!({"role":role,"parts":parts}));
    }
}

fn wire_request(
    request: &ModelRequest,
    thinking: Option<&str>,
    replay_scope: &str,
    edited_scope: &str,
) -> Result<Value> {
    let identity = identity(replay_scope, model_id(&request.model));
    let gemini_v3 = gemini_major(&identity.model).is_some_and(|version| version >= 3);
    let multimodal_tool_response = gemini_major(&identity.model).is_none_or(|version| version >= 3);
    let explicit_tool_ids = uses_tool_call_ids(&identity.model);
    let mut contents = Vec::new();
    let mut system = Vec::new();
    let mut calls = BTreeMap::new();
    let mut tool_images = Vec::new();
    for message in &request.messages {
        if !matches!(message, Message::Tool { .. }) && !tool_images.is_empty() {
            push_content(
                &mut contents,
                "user",
                std::mem::take(&mut tool_images),
                false,
            );
        }
        match message {
            Message::System { content } => system.push(content.as_str()),
            Message::User { content } => {
                push_content(&mut contents, "user", vec![json!({"text":content})], true)
            }
            Message::UserWithImages { content, images } => {
                let mut parts = Vec::new();
                if !content.is_empty() {
                    parts.push(json!({"text":content}));
                }
                for image in images {
                    image.validate()?;
                    parts
                        .push(json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}}));
                }
                push_content(&mut contents, "user", parts, true);
            }
            Message::Assistant {
                content,
                calls: invocations,
                replay,
            } => {
                let parts = if let Some(replay) = replay.as_ref().filter(|replay| {
                    replay.identity
                        == if replay.is_context_edit() {
                            identity_for_scope(edited_scope, &identity.model)
                        } else {
                            identity.clone()
                        }
                        && replay.supports_api(ModelApi::GoogleGenerativeAi)
                }) {
                    replay
                        .blocks
                        .iter()
                        .filter(|part| part.get("_pi_context_edit_api").is_none())
                        .cloned()
                        .map(|mut part| {
                            if gemini_v3
                                && replay.is_context_edit()
                                && part.get("functionCall").is_some()
                                && part.get("thoughtSignature").is_none()
                            {
                                part["thoughtSignature"] =
                                    json!("skip_thought_signature_validator");
                            }
                            if !explicit_tool_ids
                                && let Some(call) =
                                    part.get_mut("functionCall").and_then(Value::as_object_mut)
                            {
                                call.remove("id");
                            }
                            part
                        })
                        .collect()
                } else {
                    let mut parts = Vec::new();
                    if !content.is_empty() {
                        parts.push(json!({"text":content}));
                    }
                    for call in invocations {
                        let arguments = serde_json::from_str::<Value>(&call.arguments)
                            .ok()
                            .filter(Value::is_object)
                            .unwrap_or_else(|| json!({}));
                        let mut part = json!({"functionCall":{"name":call.name,"args":arguments}});
                        if explicit_tool_ids {
                            part["functionCall"]["id"] = json!(call.id);
                        }
                        // Google accepts this sentinel when a turn originated on another provider.
                        if gemini_v3 {
                            part["thoughtSignature"] = json!("skip_thought_signature_validator");
                        }
                        parts.push(part);
                    }
                    parts
                };
                for (call, part) in invocations
                    .iter()
                    .zip(parts.iter().filter(|part| part["functionCall"].is_object()))
                {
                    let id = part["functionCall"]["id"].as_str();
                    calls.insert(
                        call.id.as_str(),
                        (call.name.as_str(), id.map(str::to_owned)),
                    );
                }
                push_content(&mut contents, "model", parts, false);
            }
            Message::Tool {
                call_id,
                content,
                is_structured,
                ..
            } => {
                let Some((name, id)) = calls.get(call_id.as_str()) else {
                    return Err(Error::Model(
                        "Google tool result has no preceding call".into(),
                    ));
                };
                let (text, images) =
                    if pi_rs_core::tool_content_is_structured(content, *is_structured) {
                        extract_tool_images(content.clone())
                    } else {
                        (content.clone(), Vec::new())
                    };
                let mut response = json!({"name":name,"response":{"output":text}});
                if let Some(id) = id {
                    response["id"] = json!(id);
                }
                if multimodal_tool_response {
                    if !images.is_empty() {
                        response["parts"] = json!(images.iter().map(|image| json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}})).collect::<Vec<_>>());
                    }
                } else if !images.is_empty() {
                    tool_images.push(json!({"text":"Tool result image:"}));
                    tool_images.extend(images.iter().map(|image| json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}})));
                }
                push_content(
                    &mut contents,
                    "user",
                    vec![json!({"functionResponse":response})],
                    true,
                );
            }
        }
    }
    push_content(&mut contents, "user", tool_images, false);
    let mut body = json!({"contents":contents,"generationConfig":{"maxOutputTokens":request.max_output_tokens,"candidateCount":1}});
    if !system.is_empty() {
        body["systemInstruction"] = json!({"parts":[{"text":system.join("\n\n")}]});
    }
    if !request.tools.is_empty() {
        let declarations = request.tools.iter().map(|tool| json!({
            "name":tool.name,"description":tool.description,"parametersJsonSchema":tool.parameters
        })).collect::<Vec<_>>();
        body["tools"] = json!([{"functionDeclarations":declarations}]);
        body["toolConfig"] = json!({"functionCallingConfig":{"mode":"AUTO"}});
    }
    if let Some(config) = thinking_config(&identity.model, thinking, request.max_output_tokens) {
        body["generationConfig"]["thinkingConfig"] = config;
    }
    Ok(body)
}

#[async_trait::async_trait]
impl Model for Google {
    fn context_identity(&self) -> Option<String> {
        if !self.http_proxies.is_empty() {
            return None;
        }
        let mut headers: Vec<_> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort();
        pi_rs_core::protected_model_fingerprint(&(
            headers,
            "google-v1",
            self.endpoint.as_str(),
            self.provider.as_ref(),
            self.replay_scope.as_ref(),
            self.api_key.as_bytes(),
            &self.thinking,
        ))
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let edited_scope =
            pi_rs_core::endpoint_replay_scope(&self.replay_scope, self.endpoint.as_str());
        match message {
            Message::Assistant {
                replay: Some(replay),
                ..
            } if replay.identity
                == identity(
                    if replay.is_context_edit() {
                        &edited_scope
                    } else {
                        &self.replay_scope
                    },
                    model_id(&request.model),
                ) =>
            {
                pi_rs_core::estimate_replay_tokens(&replay.blocks)
                    .max(pi_rs_core::estimate_message_tokens(message))
            }
            _ => pi_rs_core::estimate_message_tokens(message),
        }
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        if self.probe_context {
            self.model(model).await?.context_window
        } else {
            self.cached_model(model)?.context_window
        }
    }

    async fn stream(&self, mut request: ModelRequest) -> Result<ModelStream> {
        let model = model_id(&request.model).to_owned();
        if !valid_model_id(&model) {
            return Err(Error::Config("invalid Google Gemini model ID".into()));
        }
        let catalog = if self.probe_context {
            self.model(&model).await
        } else {
            self.cached_model(&model)
        };
        if let Some(limit) = catalog.as_ref().and_then(|entry| entry.max_output_tokens) {
            request.max_output_tokens = request.max_output_tokens.min(limit);
        }
        let thinking = if catalog.as_ref().is_some_and(|entry| !entry.reasoning) {
            None
        } else {
            self.thinking.as_deref()
        };
        let edited_scope =
            pi_rs_core::endpoint_replay_scope(&self.replay_scope, self.endpoint.as_str());
        let body = wire_request(&request, thinking, &self.replay_scope, &edited_scope)?;
        let mut url = self.url(Some(&format!("{model}:streamGenerateContent")));
        url.query_pairs_mut().append_pair("alt", "sse");
        let mut last_progress = None;
        report_progress(
            &self.progress,
            &mut last_progress,
            Progress::WaitingForModel,
        );
        let response = self
            .client
            .post(url)
            .header("x-goog-api-key", self.api_key.clone())
            .headers(self.headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|error| {
                Error::RetryableModel(
                    if error.is_timeout() && !error.is_connect() {
                        "HTTP idle timeout"
                    } else {
                        "Google Gemini request failed"
                    }
                    .into(),
                )
            })?;
        let response = check_status(response)?;
        let progress = self.progress.clone();
        let observe_reasoning = self.reasoning.clone();
        let provider = self.provider.clone();
        let replay_scope = self.replay_scope.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let mut stream = response.bytes_stream().eventsource();
            let mut metadata = ModelMetadata {
                provider_api: Some(ModelApi::GoogleGenerativeAi),
                replay_identity: Some(identity(&edited_scope, &model)),
                ..ModelMetadata::default()
            };
            let mut calls = Vec::new();
            let mut call_ids = BTreeSet::new();
            let mut blocks = Vec::new();
            let mut reasoning = String::new();
            let mut total_bytes = 0usize;
            let mut usage = None;
            let mut finish = None;
            while let Some(event) = stream.next().await {
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) if error.is_timeout() => Error::RetryableModel("HTTP idle timeout".into()),
                    _ => Error::RetryableModel("Google Gemini stream interrupted".into()),
                })?;
                total_bytes = total_bytes.saturating_add(event.data.len());
                if total_bytes > MAX_RESPONSE_BYTES {
                    Err(Error::Model("Google Gemini response exceeds size limit".into()))?;
                }
                if event.data == "[DONE]" {
                    break;
                }
                let value: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("invalid Google Gemini stream chunk".into()))?;
                if let Some(error) = value.get("error") {
                    let code = error["code"].as_u64().unwrap_or_default();
                    Err(if code == 429 || (500..600).contains(&code) {
                        Error::RetryableModel("Google Gemini returned a transient stream error".into())
                    } else {
                        Error::Model("Google Gemini returned a stream error".into())
                    })?;
                }
                if let Some(id) = value["responseId"].as_str().filter(|id| id.len() <= 160) {
                    metadata.provider_request_id = Some(id.into());
                }
                if let Some(version) = value["modelVersion"].as_str().filter(|version| valid_model_id(version)) {
                    metadata.resolved_model = Some(identity(&provider, version));
                }
                if let Some(stats) = value.get("usageMetadata") {
                    let count = |key: &str| stats[key].as_u64().unwrap_or_default();
                    usage = Some(Usage {
                        input_tokens: count("promptTokenCount"),
                        output_tokens: count("candidatesTokenCount").saturating_add(count("thoughtsTokenCount")),
                    });
                    metadata.cached_input_tokens = stats["cachedContentTokenCount"].as_u64();
                }
                if value["promptFeedback"]["blockReason"].as_str().is_some_and(|reason| !reason.is_empty() && reason != "BLOCK_REASON_UNSPECIFIED") {
                    Err(Error::Model("Google Gemini blocked the prompt".into()))?;
                }
                let Some(candidates) = value["candidates"].as_array() else {
                    continue;
                };
                if candidates.len() > 1 || candidates.first().is_some_and(|candidate| candidate["index"].as_u64().unwrap_or_default() != 0) {
                    Err(Error::Model("unexpected Google Gemini candidate".into()))?;
                }
                let Some(candidate) = candidates.first() else {
                    continue;
                };
                if let Some(parts) = candidate["content"]["parts"].as_array() {
                    for part in parts {
                        let thought = part["thought"].as_bool().unwrap_or(false);
                        if thought {
                            report_progress(&progress, &mut last_progress, Progress::Thinking);
                        }
                        if let Some(text) = part["text"].as_str().filter(|text| !text.is_empty()) {
                            if thought {
                                reasoning.push_str(text);
                                if let Some(observer) = &observe_reasoning {
                                    observer(text);
                                }
                                yield ModelEvent::Reasoning(text.into());
                            } else {
                                report_progress(&progress, &mut last_progress, Progress::Generating);
                                yield ModelEvent::Text(text.into());
                            }
                        }
                        if let Some(call) = part.get("functionCall") {
                            report_progress(&progress, &mut last_progress, Progress::PreparingTool);
                            let name = call["name"].as_str().filter(|name| !name.is_empty()).ok_or_else(|| Error::Model("invalid Google Gemini function name".into()))?;
                            let arguments = match call.get("args") {
                                None | Some(Value::Null) => "{}".to_owned(),
                                Some(args) if args.is_object() => args.to_string(),
                                _ => Err(Error::Model("Google Gemini function arguments are not an object".into()))?,
                            };
                            let provided = call["id"].as_str().filter(|id| !id.is_empty() && id.len() <= 160);
                            let mut id = provided.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
                            while !call_ids.insert(id.clone()) {
                                id = uuid::Uuid::new_v4().to_string();
                            }
                            if calls.len() == 64 {
                                Err(Error::Model("too many Google Gemini function calls".into()))?;
                            }
                            let index = calls.len() as u64;
                            calls.push(ToolCall { id: id.clone(), name: name.into(), arguments: arguments.clone() });
                            yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta { index, id, name: name.into(), arguments });
                        }
                        // Preserve every part exactly: signatures belong to their original part.
                        blocks.push(part.clone());
                    }
                }
                if let Some(reason) = candidate["finishReason"].as_str().filter(|reason| *reason != "FINISH_REASON_UNSPECIFIED") {
                    finish = Some(reason.to_owned());
                }
            }
            metadata.finish_reason = Some(match finish.as_deref() {
                Some("STOP") if calls.is_empty() => "stop",
                Some("STOP") => "tool_calls",
                Some("MAX_TOKENS") => {
                    calls.clear();
                    blocks.retain(|part| part.get("functionCall").is_none());
                    "length"
                }
                None => Err(Error::RetryableModel("Google Gemini stream ended without a finish reason".into()))?,
                Some(_) => Err(Error::Model("Google Gemini did not complete the response".into()))?,
            }.into());
            if !blocks.is_empty() {
                metadata.replay = Some(Box::new(ProviderReplay { identity: identity(&replay_scope, &model), blocks }));
            }
            metadata.reasoning = (!reasoning.is_empty()).then_some(reasoning);
            yield ModelEvent::Metadata(metadata);
            yield ModelEvent::Complete { calls, usage };
        }))
    }
}

fn report_progress(
    observer: &Option<Arc<dyn Fn(Progress) + Send + Sync>>,
    last: &mut Option<Progress>,
    stage: Progress,
) {
    if *last != Some(stage) {
        *last = Some(stage);
        if let Some(observer) = observer {
            observer(stage);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(unix, windows))]
    #[test]
    fn static_custom_header_has_protected_stable_identity() {
        let make = |secret: &'static str| {
            let mut headers = HeaderMap::new();
            let mut value = reqwest::header::HeaderValue::from_static(secret);
            value.set_sensitive(true);
            headers.insert("x-route-secret", value);
            Google::new("https://example.com/v1", "key")
                .unwrap()
                .with_headers(headers)
        };
        let first = make("pin-one");
        let same = make("pin-one");
        let changed = make("pin-two");
        assert!(first.context_identity().is_some());
        assert_eq!(first.context_identity(), same.context_identity());
        assert_ne!(first.context_identity(), changed.context_identity());
        assert!(!first.context_identity().unwrap().contains("pin-one"));
    }

    #[test]
    fn edited_replay_is_endpoint_bound_and_gemini_calls_keep_or_gain_signature() {
        let scope_a = pi_rs_core::endpoint_replay_scope("google", "https://a.example/v1");
        let scope_b = pi_rs_core::endpoint_replay_scope("google", "https://b.example/v1");
        let mut request = ModelRequest {
            model: "gemini-3-pro".into(),
            messages: vec![Message::Assistant {
                content: "answer".into(),
                calls: vec![ToolCall {
                    id: "c".into(),
                    name: "read".into(),
                    arguments: "{}".into(),
                }],
                replay: Some(Box::new(ProviderReplay {
                    identity: identity(&scope_a, "gemini-3-pro"),
                    blocks: vec![
                        json!({"text":"answer","thoughtSignature":"text-sig"}),
                        json!({"thought":true,"text":"secret","thoughtSignature":"thinking-sig"}),
                        json!({"functionCall":{"id":"c","name":"read","args":{}},"thoughtSignature":"call-sig"}),
                        json!({"_pi_context_edit_api":"google-generative-ai"}),
                    ],
                })),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let same = wire_request(&request, None, "google", &scope_a).unwrap();
        assert_eq!(
            same["contents"][0]["parts"][0]["thoughtSignature"],
            "text-sig"
        );
        assert_eq!(
            same["contents"][0]["parts"][2]["thoughtSignature"],
            "call-sig"
        );
        let other = wire_request(&request, None, "google", &scope_b).unwrap();
        assert!(!other.to_string().contains("secret"));
        assert_eq!(
            other["contents"][0]["parts"][1]["thoughtSignature"],
            "skip_thought_signature_validator"
        );
        if let Message::Assistant {
            replay: Some(replay),
            ..
        } = &mut request.messages[0]
        {
            replay.blocks[2]
                .as_object_mut()
                .unwrap()
                .remove("thoughtSignature");
        }
        let unsigned = wire_request(&request, None, "google", &scope_a).unwrap();
        assert_eq!(
            unsigned["contents"][0]["parts"][2]["thoughtSignature"],
            "skip_thought_signature_validator"
        );
        if let Message::Assistant { content, calls, .. } = &mut request.messages[0] {
            content.clear();
            calls.clear();
        }
        assert!(
            wire_request(&request, None, "google", &scope_b).unwrap()["contents"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn edited_replay_keeps_thought_order_only_for_original_model() {
        let replay = ProviderReplay {
            identity: identity("google", "gemini"),
            blocks: vec![
                json!({"text":"first"}),
                json!({"thought":true,"text":"private","thoughtSignature":"sig"}),
                json!({"text":"last"}),
                json!({"_pi_context_edit_api":"google-generative-ai"}),
            ],
        };
        let mut request = ModelRequest {
            model: "gemini".into(),
            messages: vec![Message::Assistant {
                content: "firstlast".into(),
                calls: vec![],
                replay: Some(Box::new(replay)),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        assert_eq!(
            wire_request(&request, None, "google", "google").unwrap()["contents"][0]["parts"],
            json!([{"text":"first"},{"thought":true,"text":"private","thoughtSignature":"sig"},{"text":"last"}])
        );
        request.model = "other".into();
        assert_eq!(
            wire_request(&request, None, "google", "google").unwrap()["contents"][0]["parts"],
            json!([{"text":"firstlast"}])
        );
    }

    #[test]
    fn capacity_estimate_matches_image_and_replay_serialization() {
        let model = Google::new("https://example.com", "key").unwrap();
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 16,
        };
        let tool = |n| {
            Message::Tool { call_id: "c".into(), content: json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(n)}]}}).to_string(), is_error: false, is_structured: Some(true) }
        };
        let small = model.estimate_message_tokens(&request, &tool(4));
        let large = model.estimate_message_tokens(&request, &tool(20000));
        assert_eq!(small, large);
        let replay = ProviderReplay {
            identity: identity("google", "fixture"),
            blocks: vec![
                json!({"text":"answer"}),
                json!({"text":"x".repeat(5000),"thought":true}),
            ],
        };
        let assistant = Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: Some(Box::new(replay)),
        };
        assert!(model.estimate_message_tokens(&request, &assistant) > 1000);
        let mismatch = if let Message::Assistant {
            replay: Some(mut replay),
            ..
        } = assistant.clone()
        {
            replay.identity.model = "different".into();
            Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(replay),
            }
        } else {
            unreachable!()
        };
        assert_eq!(
            model.estimate_message_tokens(&request, &mismatch),
            pi_rs_core::estimate_message_tokens(&mismatch)
        );
    }

    #[test]
    fn plain_json_tool_output_is_not_extracted_as_an_image() {
        let text = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        let mut request = ModelRequest {
            model: "gemini-3-pro-preview".into(),
            messages: vec![
                Message::Assistant {
                    content: String::new(),
                    calls: vec![ToolCall {
                        id: "call".into(),
                        name: "read".into(),
                        arguments: "{}".into(),
                    }],
                    replay: None,
                },
                Message::Tool {
                    call_id: "call".into(),
                    content: text.into(),
                    is_error: false,
                    is_structured: Some(false),
                },
            ],
            tools: vec![],
            max_output_tokens: 32,
        };
        let body = wire_request(&request, None, "google", "google").unwrap();
        let response = &body["contents"][1]["parts"][0]["functionResponse"];
        assert_eq!(response["response"]["output"], text);
        assert!(response.get("parts").is_none());
        request.messages[1] =
            serde_json::from_value(json!({"role":"tool","call_id":"call","content":text})).unwrap();
        let body = wire_request(&request, None, "google", "google").unwrap();
        assert_eq!(
            body["contents"][1]["parts"][0]["functionResponse"]["parts"][0]["inlineData"]["data"],
            "aW1hZ2U="
        );
    }

    #[test]
    fn tool_images_follow_model_multimodal_response_support() {
        let image_result = json!({"data":{"content":[
            {"type":"text","text":"picture"},
            {"type":"image","mimeType":"image/png","data":"YWJj"},
            {"type":"image","mimeType":"image/jpeg","data":"ZGVm"}
        ]}})
        .to_string();
        for (model, nested) in [
            ("gemini-2.5-flash", false),
            ("Gemini-2.5-flash", false),
            ("GeMiNi-LiVe-2.5-flash", false),
            ("gemini-2_flash", false),
            ("GEMINI-LIVE-2extra", false),
            ("gemini-3-pro-preview", true),
            ("GeMiNi-3-pro-preview", true),
            ("gemini-3_flash", true),
            ("claude-sonnet-4", true),
            ("other-model", true),
        ] {
            let request = ModelRequest {
                model: model.into(),
                messages: vec![
                    Message::Assistant {
                        content: String::new(),
                        calls: ["a", "b"]
                            .into_iter()
                            .map(|id| ToolCall {
                                id: id.into(),
                                name: "read".into(),
                                arguments: "{}".into(),
                            })
                            .collect(),
                        replay: None,
                    },
                    Message::Tool {
                        is_structured: Some(true),
                        is_error: false,
                        call_id: "a".into(),
                        content: image_result.clone(),
                    },
                    Message::Tool {
                        is_structured: Some(false),
                        is_error: false,
                        call_id: "b".into(),
                        content: "second result".into(),
                    },
                    Message::User {
                        content: "follow up".into(),
                    },
                ],
                tools: vec![],
                max_output_tokens: 32,
            };
            let body = wire_request(&request, None, "google", "google").unwrap();
            let contents = body["contents"].as_array().unwrap();
            if model.to_ascii_lowercase().starts_with("gemini") {
                let call = &contents[0]["parts"][0];
                assert_eq!(call["functionCall"].get("id").is_some(), nested, "{model}");
                assert_eq!(call.get("thoughtSignature").is_some(), nested, "{model}");
            }
            let responses = contents[1]["parts"].as_array().unwrap();
            assert_eq!(responses.len(), 2, "{model}");
            assert_eq!(
                responses[1]["functionResponse"]["response"]["output"],
                "second result"
            );
            let images = json!([
                {"inlineData":{"mimeType":"image/png","data":"YWJj"}},
                {"inlineData":{"mimeType":"image/jpeg","data":"ZGVm"}}
            ]);
            if nested {
                assert_eq!(contents.len(), 3, "{model}");
                assert_eq!(responses[0]["functionResponse"]["parts"], images, "{model}");
            } else {
                assert_eq!(contents.len(), 3, "{model}");
                assert!(responses[0]["functionResponse"].get("parts").is_none());
                assert_eq!(contents[2]["parts"][0]["text"], "Tool result image:");
                assert_eq!(
                    &contents[2]["parts"].as_array().unwrap()[1..3],
                    images.as_array().unwrap()
                );
            }
            assert_eq!(
                contents.last().unwrap()["parts"]
                    .as_array()
                    .unwrap()
                    .last()
                    .unwrap()["text"],
                "follow up"
            );
            let output = responses[0]["functionResponse"]["response"]["output"]
                .as_str()
                .unwrap();
            assert!(!output.contains("YWJj"));
            assert!(!output.contains("ZGVm"));
        }
    }

    #[test]
    fn gemini_major_accepts_leading_digits_before_any_suffix() {
        for (id, expected) in [
            ("gemini-2_flash", Some(2)),
            ("GEMINI-LIVE-03preview", Some(3)),
            ("gemini-12abc", Some(12)),
            ("gemini-0000000000000000000002x", Some(2)),
            ("gemini-9999999999999999999999x", Some(u32::MAX)),
            ("gemini-_2", None),
            ("gemini-live-", None),
            ("gemini-live2", None),
        ] {
            assert_eq!(gemini_major(id), expected, "{id}");
        }
        assert_eq!(
            model_thinking_levels("gemini-3_flash"),
            model_thinking_levels("gemini-3-flash")
        );
        assert_eq!(
            thinking_config("gemini-3_flash", Some("low"), 1024),
            thinking_config("gemini-3-flash", Some("low"), 1024)
        );
        assert_eq!(model_thinking_levels("gemini-2_flash"), vec!["off"]);
        assert_eq!(thinking_config("gemini-2_flash", Some("low"), 1024), None);
    }

    #[test]
    fn mixed_case_gemini_ids_use_the_same_model_controls() {
        for (lower, mixed) in [
            ("gemini-2.5-flash-lite", "Gemini-2.5-Flash-Lite"),
            ("gemini-2.5-pro", "Gemini-2.5-Pro"),
            ("gemini-3-pro-preview", "GeMiNi-3-Pro-Preview"),
            ("gemini-live-3-flash", "GeMiNi-LiVe-3-Flash"),
        ] {
            assert_eq!(gemini_major(lower), gemini_major(mixed));
            assert_eq!(uses_tool_call_ids(lower), uses_tool_call_ids(mixed));
            assert_eq!(model_thinking_levels(lower), model_thinking_levels(mixed));
            assert_eq!(
                thinking_config(lower, Some("minimal"), 1024),
                thinking_config(mixed, Some("minimal"), 1024)
            );
        }
        assert_eq!(gemini_major("not-gemini-2"), None);
        assert_eq!(gemini_major("gemini-unknown"), None);
    }

    #[tokio::test]
    async fn streamed_parts_replay_with_original_signature_boundaries() {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", server.local_addr().unwrap());
        let serve = std::thread::spawn(move || {
            let (mut socket, _) = server.accept().unwrap();
            let mut reader = BufReader::new(&socket);
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let parts = [
                json!({"thought":true,"text":"think ","thoughtSignature":"first"}),
                json!({"thought":true,"text":"more"}),
                json!({"text":"answer ","thoughtSignature":"old"}),
                json!({"text":"done","thoughtSignature":"new"}),
                json!({"text":" unsigned"}),
                json!({"text":"","thoughtSignature":"empty-text"}),
                json!({"thoughtSignature":"signature-only"}),
                json!({"functionCall":{"name":"read","args":{}},"thoughtSignature":"call"}),
                json!({"text":"after"}),
            ];
            let mut body = String::new();
            for part in parts {
                body.push_str(&format!(
                    "data: {}\n\n",
                    json!({"candidates":[{"content":{"parts":[part]}}]})
                ));
            }
            body.push_str("data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n");
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
        });
        let model = Google::new(&endpoint, "test")
            .unwrap()
            .with_context_probe(false);
        let mut stream = model
            .stream(ModelRequest {
                model: "gemini-3.5-flash".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 32,
            })
            .await
            .unwrap();
        let mut events = Vec::new();
        let mut replay = None;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                ModelEvent::Reasoning(s) => events.push(format!("thinking:{s}")),
                ModelEvent::Text(s) => events.push(format!("text:{s}")),
                ModelEvent::Metadata(metadata) => replay = metadata.replay,
                _ => {}
            }
        }
        serve.join().unwrap();
        assert_eq!(
            events,
            [
                "thinking:think ",
                "thinking:more",
                "text:answer ",
                "text:done",
                "text: unsigned",
                "text:after"
            ]
        );
        let replay = replay.unwrap();
        assert_eq!(
            replay.blocks,
            vec![
                json!({"thought":true,"text":"think ","thoughtSignature":"first"}),
                json!({"thought":true,"text":"more"}),
                json!({"text":"answer ","thoughtSignature":"old"}),
                json!({"text":"done","thoughtSignature":"new"}),
                json!({"text":" unsigned"}),
                json!({"text":"","thoughtSignature":"empty-text"}),
                json!({"thoughtSignature":"signature-only"}),
                json!({"functionCall":{"name":"read","args":{}},"thoughtSignature":"call"}),
                json!({"text":"after"}),
            ]
        );
        let expected = json!(replay.blocks);
        let request = ModelRequest {
            model: "gemini-3.5-flash".into(),
            messages: vec![Message::Assistant {
                content: "answer done unsignedafter".into(),
                calls: vec![],
                replay: Some(replay),
            }],
            tools: vec![],
            max_output_tokens: 32,
        };
        assert_eq!(
            wire_request(&request, None, "google", "google").unwrap()["contents"][0]["parts"],
            expected
        );
    }

    #[test]
    fn signed_tool_parts_and_wire_ids_stay_with_the_exact_model() {
        let original = json!({"functionCall":{"name":"read","id":"original-call","args":{"path":"a"}},"thoughtSignature":"opaque"});
        let mut request = ModelRequest {
            model: "gemini-3.5-flash".into(),
            messages: vec![
                Message::Assistant {
                    content: String::new(),
                    calls: vec![ToolCall {
                        id: "logical-call".into(),
                        name: "read".into(),
                        arguments: r#"{"path":"a"}"#.into(),
                    }],
                    replay: Some(Box::new(ProviderReplay {
                        identity: identity("google", "gemini-3.5-flash"),
                        blocks: vec![original.clone()],
                    })),
                },
                Message::Tool {
                    is_structured: Some(false),
                    is_error: false,
                    call_id: "logical-call".into(),
                    content: "tool output".into(),
                },
                Message::User {
                    content: "Follow-up after the tool result".into(),
                },
            ],
            tools: Vec::new(),
            max_output_tokens: 1024,
        };
        let wire = wire_request(&request, Some("low"), "google", "google").unwrap();
        assert_eq!(wire["contents"][0]["parts"][0], original);
        assert_eq!(
            wire["contents"][1]["parts"][0]["functionResponse"]["id"],
            "original-call"
        );
        assert_eq!(wire["contents"][1]["parts"].as_array().unwrap().len(), 1);
        assert_eq!(
            wire["contents"][2]["parts"][0]["text"],
            "Follow-up after the tool result"
        );
        request.model = "gemini-3.1-pro".into();
        let other = wire_request(&request, Some("low"), "google", "google").unwrap();
        let call = &other["contents"][0]["parts"][0];
        assert_eq!(call["functionCall"]["id"], "logical-call");
        assert_ne!(call["thoughtSignature"], original["thoughtSignature"]);
        assert_eq!(
            other["contents"][1]["parts"][0]["functionResponse"]["id"],
            "logical-call"
        );
    }

    #[test]
    fn thinking_config_uses_the_models_native_control() {
        assert_eq!(
            thinking_config("gemini-3.1-pro", Some("low"), 1024),
            Some(json!({"thinkingLevel":"LOW","includeThoughts":true}))
        );
        assert_eq!(
            thinking_config("gemini-2.5-flash-lite", Some("off"), 1024),
            Some(json!({"thinkingBudget":0,"includeThoughts":false}))
        );
        assert_eq!(
            thinking_config("gemini-2.5-pro", Some("high"), 1024),
            Some(json!({"thinkingBudget":1023,"includeThoughts":true}))
        );
    }
}
