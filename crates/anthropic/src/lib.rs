//! Native Anthropic Messages and Models API integration.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    AnthropicInputTransformations, AssistantDiagnostic, Error, Message, Model, ModelApi,
    ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay, ReportedUsageCost,
    ResolvedModelIdentity, Result, ToolCall, Usage,
};
use reqwest::{
    Client, Response, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub const DEFAULT_ENDPOINT: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Default)]
pub struct CatalogEntry {
    pub id: String,
    pub context_window: Option<u32>,
    pub max_output_tokens: Option<u32>,
    pub reasoning: Option<bool>,
    pub images: Option<bool>,
    pub thinking_levels: Vec<String>,
    thinking: Option<ThinkingMode>,
    can_disable_thinking: bool,
    effort_levels: bool,
}

#[derive(Clone, Copy, Debug)]
enum ThinkingMode {
    Adaptive,
    Budgeted,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Progress {
    WaitingForModel,
    Thinking,
    Generating,
    PreparingTool,
}

pub use pi_rs_core::CacheRetention;

type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct Anthropic {
    client: Client,
    http_proxies: Vec<reqwest::Proxy>,
    endpoint: Url,
    auth: (HeaderName, HeaderValue),
    provider: Arc<str>,
    replay_scope: Arc<str>,
    headers: HeaderMap,
    catalog: Arc<RwLock<BTreeMap<String, CatalogEntry>>>,
    probe_context: bool,
    thinking_level: Option<String>,
    thinking_budgets: Option<Value>,
    configured_thinking: Option<CatalogEntry>,
    configured_thinking_is_fallback: bool,
    allow_empty_signature: bool,
    cache_retention: CacheRetention,
    cache_tools: bool,
    progress: Option<Arc<dyn Fn(Progress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
}

impl Anthropic {
    pub fn new(endpoint: &str, api_key: &str) -> Result<Self> {
        Self::connect(endpoint, HeaderName::from_static("x-api-key"), api_key)
    }

    /// Connect to a Messages-compatible service using bearer authentication.
    pub fn with_bearer_token(endpoint: &str, token: &str) -> Result<Self> {
        if token.trim().is_empty() {
            return Err(Error::Config(
                "missing Anthropic-compatible bearer token".into(),
            ));
        }
        Self::connect(endpoint, AUTHORIZATION, &format!("Bearer {token}"))
    }

    fn connect(endpoint: &str, name: HeaderName, credential: &str) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid Anthropic endpoint".into()))?;
        let loopback = matches!(
            endpoint.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]")
        );
        if !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.path().ends_with("/messages")
        {
            return Err(Error::Config(
                "Anthropic endpoint must be HTTPS or loopback HTTP with a /messages path and no embedded credentials, query, or fragment".into(),
            ));
        }
        let mut value = HeaderValue::from_str(credential)
            .ok()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| Error::Config("missing or invalid Anthropic API key".into()))?;
        value.set_sensitive(true);
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::Config("could not build Anthropic HTTP client".into()))?;
        Ok(Self {
            client,
            http_proxies: Vec::new(),
            endpoint,
            auth: (name, value),
            provider: Arc::from("anthropic"),
            replay_scope: Arc::from("anthropic"),
            headers: HeaderMap::new(),
            catalog: Default::default(),
            probe_context: true,
            thinking_level: None,
            thinking_budgets: None,
            configured_thinking: None,
            configured_thinking_is_fallback: false,
            allow_empty_signature: false,
            cache_retention: CacheRetention::None,
            cache_tools: true,
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
            .map_err(|_| Error::Config("could not build Anthropic HTTP client".into()))?;
        self.http_proxies = proxies.to_vec();
        Ok(self)
    }

    /// Isolate opaque reasoning blocks when another service uses the Messages protocol.
    pub fn with_provider(mut self, provider: &str, replay_scope: impl Into<Arc<str>>) -> Self {
        self.provider = Arc::from(provider);
        self.replay_scope = replay_scope.into();
        self
    }

    /// Add request defaults while preserving explicitly configured headers.
    pub fn with_default_headers(mut self, headers: &HeaderMap) -> Self {
        for (name, value) in headers {
            self.headers
                .entry(name.clone())
                .or_insert_with(|| value.clone());
        }
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

    pub fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking_level = level.map(str::to_owned);
        self
    }

    pub fn with_thinking_budget_settings(mut self, budgets: Option<Value>) -> Self {
        self.thinking_budgets = budgets;
        self
    }

    /// Use known thinking capabilities for a gateway that lacks a Models API.
    pub fn with_configured_thinking(mut self, level: &str, adaptive: bool, disable: bool) -> Self {
        self.thinking_level = Some(level.into());
        self.configured_thinking_is_fallback = false;
        self.configured_thinking = Some(CatalogEntry {
            thinking_levels: vec![level.into()],
            thinking: Some(if adaptive {
                ThinkingMode::Adaptive
            } else {
                ThinkingMode::Budgeted
            }),
            can_disable_thinking: disable,
            effort_levels: adaptive,
            ..CatalogEntry::default()
        });
        self
    }

    /// Use declared capabilities only when model discovery is unavailable.
    pub fn with_fallback_thinking(mut self, level: &str, adaptive: bool, disable: bool) -> Self {
        self = self.with_configured_thinking(level, adaptive, disable);
        self.configured_thinking_is_fallback = true;
        self
    }

    /// Adaptive thinking can be supported without an output effort selector.
    pub fn without_thinking_effort(mut self) -> Self {
        if let Some(details) = &mut self.configured_thinking {
            details.effort_levels = false;
        }
        self
    }

    pub fn with_empty_thinking_signature(mut self, enabled: bool) -> Self {
        self.allow_empty_signature = enabled;
        self
    }

    pub fn with_cache_retention(mut self, retention: CacheRetention, cache_tools: bool) -> Self {
        self.cache_retention = retention;
        self.cache_tools = cache_tools;
        self
    }

    pub fn without_cache(mut self) -> Self {
        self.cache_retention = CacheRetention::None;
        self
    }

    pub async fn model(&self, model: &str) -> Option<CatalogEntry> {
        if let Some(details) = self.cached_model(model) {
            return Some(details);
        }
        if !self.probe_context {
            return None;
        }
        let mut url = self.models_url();
        url.path_segments_mut().ok()?.push(model);
        let details = tokio::time::timeout(Duration::from_secs(2), async {
            let response = send(self.authenticate(self.client.get(url))).await.ok()?;
            decode_model(&read_json(response).await.ok()?)
        })
        .await
        .ok()
        .flatten()?;
        self.catalog
            .write()
            .ok()?
            .insert(model.into(), details.clone());
        Some(details)
    }

    pub fn with_progress(mut self, observer: impl Fn(Progress) + Send + Sync + 'static) -> Self {
        self.progress = Some(Arc::new(observer));
        self
    }

    pub fn with_reasoning(mut self, observer: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.reasoning = Some(Arc::new(observer));
        self
    }

    pub fn cached_model(&self, model: &str) -> Option<CatalogEntry> {
        self.catalog.read().ok()?.get(model).cloned()
    }

    fn models_url(&self) -> Url {
        let mut url = self.endpoint.clone();
        let base = url
            .path()
            .strip_suffix("/messages")
            .expect("validated endpoint");
        url.set_path(&format!("{base}/models"));
        url
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header("anthropic-version", API_VERSION)
            .header(self.auth.0.clone(), self.auth.1.clone())
            .headers(self.headers.clone())
    }

    /// Return paginated models in server order and retain their metadata for this client.
    pub async fn models(&self) -> Result<Vec<CatalogEntry>> {
        tokio::time::timeout(Duration::from_secs(10), self.load_models())
            .await
            .map_err(|_| Error::RetryableModel("Anthropic model listing timed out".into()))?
    }

    async fn load_models(&self) -> Result<Vec<CatalogEntry>> {
        let mut result = Vec::new();
        let mut seen = BTreeSet::new();
        let mut cursors = BTreeSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            let mut url = self.models_url();
            url.query_pairs_mut().append_pair("limit", "100");
            if let Some(cursor) = &cursor {
                url.query_pairs_mut().append_pair("after_id", cursor);
            }
            let response = send(self.authenticate(self.client.get(url))).await?;
            let page = read_json(response).await?;
            let models = page["data"]
                .as_array()
                .ok_or_else(|| Error::Model("invalid Anthropic model list".into()))?;
            for model in models {
                if let Some(model) = decode_model(model)
                    && seen.insert(model.id.clone())
                {
                    result.push(model);
                }
            }
            if page["has_more"] != true {
                self.catalog
                    .write()
                    .expect("model catalog")
                    .extend(result.iter().map(|entry| (entry.id.clone(), entry.clone())));
                return Ok(result);
            }
            let next = page["last_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| Error::Model("missing Anthropic model cursor".into()))?;
            if !cursors.insert(next.to_owned()) {
                return Err(Error::Model("repeated Anthropic model cursor".into()));
            }
            cursor = Some(next.to_owned());
        }
        Err(Error::Model(
            "Anthropic model listing exceeds page limit".into(),
        ))
    }
}

fn decode_model(model: &Value) -> Option<CatalogEntry> {
    let tokens = |key: &str| {
        model[key]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .filter(|count| *count > 0)
    };
    let capabilities = &model["capabilities"];
    let types = &capabilities["thinking"]["types"];
    let thinking = if types["adaptive"]["supported"] == true {
        Some(ThinkingMode::Adaptive)
    } else if types["enabled"]["supported"] == true {
        Some(ThinkingMode::Budgeted)
    } else {
        None
    };
    let can_disable_thinking = types["disabled"]["supported"].as_bool().unwrap_or(true);
    let mut thinking_levels = Vec::new();
    let mut effort_levels = false;
    if can_disable_thinking || thinking.is_none() {
        thinking_levels.push("off".into());
    }
    match thinking {
        Some(ThinkingMode::Adaptive) => {
            thinking_levels.extend(
                ["low", "medium", "high", "xhigh", "max"]
                    .into_iter()
                    .filter(|level| capabilities["effort"][*level]["supported"] == true)
                    .map(str::to_owned),
            );
            effort_levels = thinking_levels.iter().any(|level| level != "off");
            if !effort_levels {
                thinking_levels.push("high".into());
            }
        }
        Some(ThinkingMode::Budgeted) => thinking_levels.extend(
            ["minimal", "low", "medium", "high", "xhigh", "max"]
                .into_iter()
                .map(str::to_owned),
        ),
        None => {}
    }
    Some(CatalogEntry {
        id: model["id"].as_str().filter(|id| !id.is_empty())?.into(),
        context_window: tokens("max_input_tokens"),
        max_output_tokens: tokens("max_tokens"),
        reasoning: model["capabilities"]["thinking"]["supported"].as_bool(),
        images: capabilities["image_input"]["supported"].as_bool(),
        thinking_levels,
        thinking,
        can_disable_thinking,
        effort_levels,
    })
}

async fn send(request: reqwest::RequestBuilder) -> Result<Response> {
    let response = request.send().await.map_err(|error| {
        Error::RetryableModel(
            if error.is_timeout() && !error.is_connect() {
                "HTTP idle timeout"
            } else {
                "Anthropic HTTP connection failed"
            }
            .into(),
        )
    })?;
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        let message = format!("Anthropic returned HTTP {status}");
        if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
            Err(Error::RetryableModel(message))
        } else {
            Err(Error::Model(message))
        }
    }
}

async fn read_json(mut response: Response) -> Result<Value> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::RetryableModel("broken Anthropic HTTP response".into()))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(Error::Model("Anthropic response exceeds size limit".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Model("invalid Anthropic response".into()))
}

fn encode_request(
    request: ModelRequest,
    replay_scope: &str,
    edited_scope: &str,
    allow_empty_signature: bool,
) -> Result<Value> {
    let mut system = Vec::new();
    let mut turns: Vec<(&str, Vec<Value>)> = Vec::new();
    for message in request.messages {
        let (role, blocks) = match message {
            Message::System { content } => {
                system.push(content);
                continue;
            }
            Message::User { content } => ("user", text_block(content).into_iter().collect()),
            Message::UserWithImages { content, images } => {
                let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                for image in images {
                    image.validate()?;
                    blocks.push(json!({"type":"image","source":{
                        "type":"base64","media_type":image.mime_type,"data":image.data
                    }}));
                }
                ("user", blocks)
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                let blocks = match replay {
                    Some(replay)
                        if replay_matches(
                            &replay,
                            if replay.is_context_edit() { edited_scope } else { replay_scope },
                            &request.model,
                            &content,
                            &calls,
                        ) =>
                    {
                        replay
                            .blocks
                            .into_iter()
                            .filter(|block| block.get("_pi_context_edit_api").is_none() && block["_pi_omitted_text"] != true)
                            .map(|mut block| {
                                if block["_pi_thinking"] == true && allow_empty_signature {
                                    return json!({"type":"thinking","thinking":block["text"],"signature":""});
                                }
                                if let Some(object) = block.as_object_mut() {
                                    object.remove("_pi_thinking");
                                }
                                block
                            })
                            .collect()
                    }
                    _ => {
                        let mut blocks: Vec<_> = text_block(content).into_iter().collect();
                        blocks.extend(calls.into_iter().map(|call| tool_block(&call)));
                        blocks
                    }
                };
                ("assistant", blocks)
            }
            Message::Tool {
                call_id,
                content,
                is_error,
                ..
            } => {
                let mut block =
                    json!({"type":"tool_result","tool_use_id":call_id,"content":content});
                if is_error {
                    block["is_error"] = json!(true);
                }
                ("user", vec![block])
            }
        };
        if blocks.is_empty() {
            continue;
        }
        if let Some((previous, content)) = turns.last_mut()
            && *previous == role
        {
            content.extend(blocks);
        } else {
            turns.push((role, blocks));
        }
    }
    if turns.is_empty() {
        return Err(Error::Model(
            "Anthropic requires at least one conversation message".into(),
        ));
    }
    let messages: Vec<_> = turns
        .into_iter()
        .map(|(role, content)| json!({"role":role,"content":content}))
        .collect();
    let mut body = json!({"model":request.model,"messages":messages,"max_tokens":request.max_output_tokens,"stream":true});
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(request.tools.into_iter().map(|tool| {
            json!({"name":tool.name,"description":tool.description,"input_schema":tool.parameters})
        }).collect::<Vec<_>>());
    }
    Ok(body)
}

fn apply_cache_control(body: &mut Value, retention: CacheRetention, cache_tools: bool) {
    let Some(control) = retention.anthropic_control() else {
        return;
    };
    if let Some(system) = body.get("system").and_then(Value::as_str) {
        body["system"] = json!([{"type":"text","text":system,"cache_control":control}]);
    }
    if cache_tools
        && let Some(tool) = body
            .get_mut("tools")
            .and_then(Value::as_array_mut)
            .and_then(|tools| tools.last_mut())
    {
        tool["cache_control"] = control.clone();
    }
    if let Some(message) = body
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .and_then(|messages| messages.last_mut())
        && message["role"] == "user"
        && let Some(block) = message
            .get_mut("content")
            .and_then(Value::as_array_mut)
            .and_then(|blocks| blocks.last_mut())
        && matches!(
            block["type"].as_str(),
            Some("text" | "image" | "tool_result")
        )
    {
        block["cache_control"] = control;
    }
}

fn text_block(content: String) -> Option<Value> {
    (!content.is_empty()).then(|| json!({"type":"text","text":content}))
}

fn configure_thinking(
    body: &mut Value,
    details: Option<&CatalogEntry>,
    level: &str,
    max_output: u32,
    budgets: Option<&Value>,
) -> Result<()> {
    if level == "off" {
        if let Some(model) = details.filter(|model| model.thinking.is_some()) {
            if !model.can_disable_thinking {
                return Err(Error::Config(
                    "selected Anthropic model cannot disable thinking".into(),
                ));
            }
            body["thinking"] = json!({"type":"disabled"});
        }
        return Ok(());
    }
    let details = details
        .filter(|model| model.thinking_levels.iter().any(|value| value == level))
        .ok_or_else(|| {
            Error::Config("selected Anthropic model does not advertise that thinking level".into())
        })?;
    match details.thinking {
        Some(ThinkingMode::Adaptive) => {
            body["thinking"] = json!({"type":"adaptive"});
            // A model can advertise adaptive thinking without effort controls.
            if details.effort_levels {
                body["output_config"] = json!({"effort":level});
            }
        }
        Some(ThinkingMode::Budgeted) => {
            let key = match level {
                "minimal" => "minimal",
                "low" => "low",
                "medium" => "medium",
                _ => "high",
            };
            let default = match level {
                "minimal" => 1024,
                "low" => 2048,
                "medium" => 8192,
                _ => 16_384,
            };
            let preferred = if let Some(budgets) = budgets {
                let object = budgets
                    .as_object()
                    .ok_or_else(|| Error::Config("thinkingBudgets must be an object".into()))?;
                match object.get(key) {
                    None => default,
                    Some(value) => value
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .filter(|n| *n >= 1024)
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "thinkingBudgets.{key} must be an integer of at least 1024"
                            ))
                        })?,
                }
            } else {
                default
            };
            let budget = preferred.min(max_output.saturating_sub(1024).max(1024));
            if budget >= max_output {
                return Err(Error::Config(
                    "Anthropic budgeted thinking needs at least 1025 output tokens".into(),
                ));
            }
            body["thinking"] =
                json!({"type":"enabled","budget_tokens":budget,"display":"summarized"});
        }
        None => {}
    }
    Ok(())
}

fn tool_block(call: &ToolCall) -> Value {
    let input = serde_json::from_str::<Value>(&call.arguments)
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    json!({"type":"tool_use","id":call.id,"name":call.name,"input":input})
}

fn replay_matches(
    replay: &ProviderReplay,
    scope: &str,
    model: &str,
    text: &str,
    calls: &[ToolCall],
) -> bool {
    if !replay.supports_api(ModelApi::AnthropicMessages)
        || replay.identity.provider != scope
        || replay.identity.model != model
    {
        return false;
    }
    let rendered = replay
        .blocks
        .iter()
        .filter(|block| block["type"] == "text" && block["_pi_thinking"] != true)
        .filter_map(|block| block["text"].as_str())
        .collect::<String>();
    if rendered != text {
        return false;
    }
    let mut replay_calls = replay
        .blocks
        .iter()
        .filter(|block| block["type"] == "tool_use");
    for call in calls {
        let Some(block) = replay_calls.next() else {
            return false;
        };
        let expected = tool_block(call);
        if ["id", "name", "input"]
            .iter()
            .any(|key| block[*key] != expected[*key])
        {
            return false;
        }
    }
    replay_calls.next().is_none()
}

struct ResponseBlock {
    raw: Value,
    text: String,
    signature: String,
    complete: bool,
}

impl ResponseBlock {
    fn new(raw: &Value) -> Self {
        let key = if raw["type"] == "thinking" {
            "thinking"
        } else {
            "text"
        };
        Self {
            raw: raw.clone(),
            text: raw[key].as_str().unwrap_or_default().into(),
            signature: raw["signature"].as_str().unwrap_or_default().into(),
            complete: false,
        }
    }

    fn signed(&self) -> bool {
        matches!(
            self.raw["type"].as_str(),
            Some("thinking" | "redacted_thinking")
        )
    }

    fn finish(mut self, allow_empty_signature: bool) -> Result<Value> {
        if !self.complete {
            return Err(Error::Model(
                "Anthropic content block ended prematurely".into(),
            ));
        }
        match self.raw["type"].as_str() {
            Some("text") => self.raw["text"] = json!(self.text),
            Some("thinking") if allow_empty_signature || !self.signature.is_empty() => {
                self.raw["thinking"] = json!(self.text);
                self.raw["signature"] = json!(self.signature);
            }
            Some("thinking") => {
                return Err(Error::Model(
                    "Anthropic thinking block is missing a signature".into(),
                ));
            }
            Some("redacted_thinking") if self.raw["data"].as_str().is_none_or(str::is_empty) => {
                return Err(Error::Model(
                    "Anthropic redacted thinking block is empty".into(),
                ));
            }
            _ => {}
        }
        Ok(self.raw)
    }
}

struct Call {
    id: String,
    name: String,
    raw: Value,
    fragments: String,
    complete: bool,
}

impl Call {
    fn finish(mut self) -> Result<(ToolCall, Value)> {
        if !self.complete {
            return Err(Error::Model(
                "Anthropic tool block ended prematurely".into(),
            ));
        }
        let input = if self.fragments.is_empty() {
            self.raw["input"].take()
        } else {
            serde_json::from_str(&self.fragments)
                .map_err(|_| Error::Model("invalid Anthropic tool input JSON".into()))?
        };
        if !input.is_object() {
            return Err(Error::Model(
                "Anthropic tool input must be an object".into(),
            ));
        }
        let call = ToolCall {
            id: self.id,
            name: self.name,
            arguments: input.to_string(),
        };
        self.raw["input"] = input;
        Ok((call, self.raw))
    }
}

#[derive(Default)]
struct StreamUsage {
    input: Option<u64>,
    output: Option<u64>,
    cached: Option<u64>,
    cache_written: Option<u64>,
    cache_written_1h: Option<u64>,
}
impl StreamUsage {
    fn update(&mut self, value: &Value, metadata: &mut ModelMetadata) {
        self.input = value["input_tokens"].as_u64().or(self.input);
        self.output = value["output_tokens"].as_u64().or(self.output);
        self.cached = value["cache_read_input_tokens"].as_u64().or(self.cached);
        self.cache_written = value["cache_creation_input_tokens"]
            .as_u64()
            .or(self.cache_written);
        self.cache_written_1h = value["cache_creation"]["ephemeral_1h_input_tokens"]
            .as_u64()
            .or(self.cache_written_1h);
        metadata.cache_write_1h_tokens = self.cache_written_1h;
        metadata.cached_input_tokens = self.cached;
        metadata.cache_creation_input_tokens = self.cache_written;
        metadata.provider_prompt_tokens = self.total_input();
    }
    fn total_input(&self) -> Option<u64> {
        Some(
            self.input?
                .saturating_add(self.cached.unwrap_or(0))
                .saturating_add(self.cache_written.unwrap_or(0)),
        )
    }
    fn finish(&self) -> Option<Usage> {
        Some(Usage {
            input_tokens: self.total_input()?,
            output_tokens: self.output?,
        })
    }
}

#[async_trait::async_trait]
impl Model for Anthropic {
    fn context_identity(&self) -> Option<String> {
        // Thinking and discovered output limits can depend on a changing catalog.
        if !self.http_proxies.is_empty()
            || (self.thinking_level.is_some()
                && (self.configured_thinking.is_none() || self.probe_context))
        {
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
            "anthropic-v1",
            self.endpoint.as_str(),
            self.provider.as_ref(),
            self.replay_scope.as_ref(),
            self.auth.0.as_str(),
            self.auth.1.as_bytes(),
            &self.thinking_level,
            &self.thinking_budgets,
            format!("{:?}", self.configured_thinking),
            self.configured_thinking_is_fallback,
            self.allow_empty_signature,
            self.cache_retention.anthropic_control(),
            self.cache_tools,
        ))
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let edited_scope =
            pi_rs_core::endpoint_replay_scope(&self.replay_scope, self.endpoint.as_str());
        match message {
            Message::Tool { content, .. } => content.encode_utf16().count().div_ceil(4) as u64,
            Message::Assistant {
                content,
                calls,
                replay: Some(replay),
            } if replay_matches(
                replay,
                if replay.is_context_edit() {
                    &edited_scope
                } else {
                    &self.replay_scope
                },
                &request.model,
                content,
                calls,
            ) =>
            {
                pi_rs_core::estimate_replay_tokens(&replay.blocks)
                    .max(pi_rs_core::estimate_message_tokens(message))
            }
            _ => pi_rs_core::estimate_message_tokens(message),
        }
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        self.model(model).await?.context_window
    }

    async fn stream(&self, mut request: ModelRequest) -> Result<ModelStream> {
        let discovered = if self.thinking_level.is_some() {
            self.model(&request.model).await
        } else {
            self.cached_model(&request.model)
        };
        let details = if self.configured_thinking_is_fallback && discovered.is_some() {
            discovered
        } else if let Some(mut configured) = self.configured_thinking.clone() {
            if let Some(discovered) = discovered {
                configured.id = discovered.id;
                configured.context_window = discovered.context_window;
                configured.max_output_tokens = discovered.max_output_tokens;
                configured.reasoning = discovered.reasoning;
                configured.images = discovered.images;
            }
            Some(configured)
        } else {
            discovered
        };
        if let Some(limit) = details.as_ref().and_then(|model| model.max_output_tokens) {
            request.max_output_tokens = request.max_output_tokens.min(limit);
        }
        let requested_model = request.model.clone();
        let max_output_tokens = request.max_output_tokens;
        let provider = self.provider.clone();
        let openrouter =
            provider.as_ref() == "openrouter" || self.endpoint.host_str() == Some("openrouter.ai");
        let replay_scope = self.replay_scope.clone();
        let edited_scope = pi_rs_core::endpoint_replay_scope(&replay_scope, self.endpoint.as_str());
        let mut body = encode_request(
            request,
            &replay_scope,
            &edited_scope,
            self.allow_empty_signature,
        )?;
        apply_cache_control(&mut body, self.cache_retention, self.cache_tools);
        if let Some(level) = self.thinking_level.as_deref() {
            configure_thinking(
                &mut body,
                details.as_ref(),
                level,
                max_output_tokens,
                self.thinking_budgets.as_ref(),
            )?;
        }
        if let Some(progress) = &self.progress {
            progress(Progress::WaitingForModel);
        }
        let response =
            send(self.authenticate(self.client.post(self.endpoint.clone()).json(&body))).await?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected an Anthropic SSE response".into()));
        }
        let progress = self.progress.clone();
        let allow_empty_signature = self.allow_empty_signature;
        let reasoning = self.reasoning.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0_usize;
            let mut events = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|error| Error::RetryableModel(if error.is_timeout() && !error.is_connect() { "HTTP idle timeout" } else { "broken Anthropic HTTP stream" }.into()))?;
                size = size.saturating_add(chunk.len());
                if size > MAX_RESPONSE_BYTES {
                    return Err(Error::Model("Anthropic stream exceeds size limit".into()));
                }
                Ok(chunk)
            }).eventsource();
            let mut last_progress = Some(Progress::WaitingForModel);
            let mut report = |stage| {
                if last_progress != Some(stage) {
                    last_progress = Some(stage);
                    if let Some(progress) = &progress { progress(stage); }
                }
            };
            let mut calls = BTreeMap::<u64, Call>::new();
            let mut blocks = BTreeMap::<u64, ResponseBlock>::new();
            let mut usage = StreamUsage::default();
            let mut metadata = ModelMetadata {
                provider_api: Some(ModelApi::AnthropicMessages),
                replay_identity: Some(ResolvedModelIdentity { provider: edited_scope, model: requested_model.clone() }),
                ..ModelMetadata::default()
            };
            let mut reasoning_text = String::new();
            let mut input_transformations = None;
            let mut started = false;
            while let Some(event) = events.next().await {
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) => error,
                    _ => Error::RetryableModel("broken Anthropic SSE stream".into()),
                })?;
                let value: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("malformed Anthropic SSE event".into()))?;
                let previous = metadata.clone();
                match value["type"].as_str() {
                    Some("message_start") if !started => {
                        started = true;
                        let message = &value["message"];
                        metadata.provider_request_id = message["id"].as_str().filter(|id|
                            id.len() <= 128 && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                        ).map(str::to_owned);
                        metadata.resolved_model = Some(ResolvedModelIdentity {
                            provider: provider.to_string(),
                            model: message["model"].as_str().unwrap_or(&requested_model).into(),
                        });
                        usage.update(&message["usage"], &mut metadata);
                        input_transformations = AnthropicInputTransformations::from_value(&message["input_transformations"]);
                    }
                    Some("content_block_start") if started => {
                        let index = block_index(&value)?;
                        let block = &value["content_block"];
                        if blocks.contains_key(&index) || calls.contains_key(&index)
                            || blocks.len().saturating_add(calls.len()) >= 256
                        {
                            Err(Error::Model("invalid Anthropic content block index".into()))?;
                        }
                        if block["type"] == "tool_use" {
                            let id = required(&block["id"], "tool ID")?;
                            let name = required(&block["name"], "tool name")?;
                            if calls.len() >= 64 {
                                Err(Error::Model("too many Anthropic tool calls".into()))?;
                            }
                            calls.insert(index, Call {
                                id: id.into(), name: name.into(), raw: block.clone(),
                                fragments: String::new(), complete: false,
                            });
                            report(Progress::PreparingTool);
                            yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                                index,
                                id: id.into(),
                                name: name.into(),
                                arguments: block["input"].as_object().filter(|input| !input.is_empty())
                                    .map_or_else(String::new, |input| Value::Object(input.clone()).to_string()),
                            });
                        } else {
                            let captured = ResponseBlock::new(block);
                            match block["type"].as_str() {
                                Some("text") if !captured.text.is_empty() => {
                                    report(Progress::Generating);
                                    yield ModelEvent::Text(captured.text.clone());
                                }
                                Some("thinking" | "redacted_thinking") => {
                                    report(Progress::Thinking);
                                    if !captured.text.is_empty() && block["type"] == "thinking" {
                                        reasoning_text.push_str(&captured.text);
                                        if let Some(observer) = &reasoning { observer(&captured.text); }
                                        yield ModelEvent::Reasoning(captured.text.clone());
                                    }
                                }
                                _ => {}
                            }
                            blocks.insert(index, captured);
                        }
                    }
                    Some("content_block_delta") if started => {
                        let index = block_index(&value)?;
                        let delta = &value["delta"];
                        let captured = blocks.get_mut(&index).filter(|block| !block.complete);
                        match delta["type"].as_str() {
                            Some("text_delta") => {
                                let captured = captured.filter(|block| block.raw["type"] == "text")
                                    .ok_or_else(|| Error::Model("Anthropic text delta has no open block".into()))?;
                                if let Some(text) = delta["text"].as_str().filter(|text| !text.is_empty()) {
                                    captured.text.push_str(text);
                                    report(Progress::Generating);
                                    yield ModelEvent::Text(text.into());
                                }
                            }
                            Some("thinking_delta") => {
                                let captured = captured.filter(|block| block.raw["type"] == "thinking")
                                    .ok_or_else(|| Error::Model("Anthropic thinking delta has no open block".into()))?;
                                if let Some(text) = delta["thinking"].as_str().filter(|text| !text.is_empty()) {
                                    captured.text.push_str(text);
                                    report(Progress::Thinking);
                                    reasoning_text.push_str(text);
                                    if let Some(observer) = &reasoning { observer(text); }
                                    yield ModelEvent::Reasoning(text.into());
                                }
                            }
                            Some("signature_delta") => {
                                let captured = captured.filter(|block| block.raw["type"] == "thinking")
                                    .ok_or_else(|| Error::Model("Anthropic signature delta has no open block".into()))?;
                                captured.signature.push_str(delta["signature"].as_str().unwrap_or_default());
                            }
                            Some("citations_delta") => {
                                if let Some(captured) = captured.filter(|block| block.raw["type"] == "text") {
                                    if !captured.raw["citations"].is_array() {
                                        captured.raw["citations"] = json!([]);
                                    }
                                    captured.raw["citations"].as_array_mut().expect("citations array")
                                        .push(delta["citation"].clone());
                                }
                            }
                            Some("input_json_delta") => {
                                let call = calls.get_mut(&index).filter(|call| !call.complete)
                                    .ok_or_else(|| Error::Model("Anthropic tool input has no open block".into()))?;
                                if let Some(arguments) = delta["partial_json"].as_str().filter(|text| !text.is_empty()) {
                                    call.fragments.push_str(arguments);
                                    yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                                        index, id: String::new(), name: String::new(), arguments: arguments.into(),
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                    Some("content_block_stop") if started => {
                        let index = block_index(&value)?;
                        if let Some(call) = calls.get_mut(&index) {
                            call.complete = true;
                        }
                        if let Some(block) = blocks.get_mut(&index) {
                            block.complete = true;
                        }
                    }
                    Some("message_delta") if started => {
                        usage.update(&value["usage"], &mut metadata);
                        if value["input_transformations"].is_array() {
                            input_transformations = AnthropicInputTransformations::from_value(&value["input_transformations"]);
                        }
                        if openrouter
                            && let Some(cost) = ReportedUsageCost::deserialize(&value["usage"])
                                .ok().and_then(|accounting| accounting.total())
                        {
                            metadata.cost_usd = Some(cost);
                        metadata.cost_from_provider = Some(true);
                        }
                        if let Some(reason) = value["delta"]["stop_reason"].as_str() {
                            metadata.finish_reason = Some(match reason {
                                "end_turn" | "stop_sequence" => "stop",
                                "max_tokens" => "length",
                                "tool_use" => "tool_calls",
                                "refusal" => "content_filter",
                                _ => "other",
                            }.into());
                        }
                    }
                    Some("message_stop") if started => {
                        match metadata.finish_reason.as_deref() {
                            Some("stop" | "length") if calls.is_empty() => {}
                            Some("tool_calls") if !calls.is_empty() => {}
                            _ => Err(Error::Model("Anthropic response did not finish successfully".into()))?,
                        }
                        let signed = blocks.values().any(ResponseBlock::signed);
                        let mut replay = BTreeMap::new();
                        if signed {
                            for (index, block) in blocks {
                                replay.insert(index, block.finish(allow_empty_signature)?);
                            }
                        }
                        let mut completed_calls = Vec::new();
                        for (index, call) in calls {
                            let (call, raw) = call.finish()?;
                            if signed {
                                replay.insert(index, raw);
                            }
                            completed_calls.push(call);
                        }
                        if !reasoning_text.is_empty() {
                            metadata.reasoning = Some(reasoning_text);
                        }
                        if let Some(details) = input_transformations {
                            let millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
                            let timestamp = i64::try_from(millis).unwrap_or(i64::MAX);
                            metadata.diagnostics.push(AssistantDiagnostic::AnthropicInputTransformations { timestamp, details });
                        }
                        if signed {
                            metadata.replay = Some(Box::new(ProviderReplay {
                                identity: ResolvedModelIdentity { provider: replay_scope.to_string(), model: requested_model },
                                blocks: replay.into_values().collect(),
                            }));
                        }
                        if metadata != previous {
                            yield ModelEvent::Metadata(metadata);
                        }
                        yield ModelEvent::Complete { calls: completed_calls, usage: usage.finish() };
                        return;
                    }
                    Some("error") => {
                        let kind = value["error"]["type"].as_str().unwrap_or_default();
                        Err(if matches!(kind, "overloaded_error" | "api_error" | "rate_limit_error") {
                            Error::RetryableModel("Anthropic returned a transient stream error".into())
                        } else {
                            Error::Model("Anthropic returned a stream error".into())
                        })?;
                    }
                    _ => {}
                }
                if metadata != previous {
                    yield ModelEvent::Metadata(metadata.clone());
                }
            }
            Err(Error::RetryableModel("Anthropic stream ended before message_stop".into()))?;
        }))
    }
}

fn block_index(value: &Value) -> Result<u64> {
    value["index"]
        .as_u64()
        .ok_or_else(|| Error::Model("missing Anthropic block index".into()))
}

fn required<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::Model(format!("missing Anthropic {field}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(unix, windows))]
    #[test]
    fn context_identity_tracks_static_route_and_configuration() {
        let make = |endpoint: &str, key: &str| Anthropic::new(endpoint, key).unwrap();
        let first = make(DEFAULT_ENDPOINT, "key-a");
        assert!(first.context_identity().is_some());
        assert_eq!(first.context_identity(), first.clone().context_identity());
        assert_ne!(
            first.context_identity(),
            make(DEFAULT_ENDPOINT, "key-b").context_identity()
        );
        assert_ne!(
            first.context_identity(),
            make("https://example.com/v1/messages", "key-a").context_identity()
        );
        assert_ne!(
            first.context_identity(),
            first
                .clone()
                .with_configured_thinking("high", true, true)
                .context_identity()
        );
        let configured = first.clone().with_configured_thinking("high", true, true);
        assert!(configured.context_identity().is_none());
        assert!(
            configured
                .with_context_probe(false)
                .context_identity()
                .is_some()
        );
        assert!(
            first
                .with_thinking(Some("high"))
                .context_identity()
                .is_none()
        );
    }

    #[test]
    fn capacity_estimate_matches_image_and_replay_serialization() {
        let model = Anthropic::new(DEFAULT_ENDPOINT, "key").unwrap();
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
        assert!(large > small + 4000);
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "anthropic".into(),
                model: "fixture".into(),
            },
            blocks: vec![
                json!({"type":"text","text":"answer"}),
                json!({"type":"thinking","thinking":"x".repeat(5000),"signature":"sig"}),
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
    fn edited_replay_preserves_whitespace_and_thinking() {
        let scope = pi_rs_core::endpoint_replay_scope("anthropic", "https://one.example/messages");
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![Message::Assistant {
                content: "first \nsecond".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: ResolvedModelIdentity {
                        provider: scope.clone(),
                        model: "m".into(),
                    },
                    blocks: vec![
                        json!({"type":"text","text":"first \n"}),
                        json!({"type":"text","text":"second"}),
                        json!({"type":"thinking","thinking":"secret","signature":"sig"}),
                        json!({"_pi_context_edit_api":"anthropic-messages"}),
                    ],
                })),
            }],
            tools: vec![],
            max_output_tokens: 16,
        };
        let body = encode_request(request, "anthropic", &scope, false).unwrap();
        assert_eq!(
            body["messages"][0]["content"],
            json!([{"type":"text","text":"first \n"},{"type":"text","text":"second"},{"type":"thinking","thinking":"secret","signature":"sig"}])
        );
    }

    #[test]
    fn edited_thinking_is_bound_to_the_original_endpoint() {
        let scope_a = pi_rs_core::endpoint_replay_scope("anthropic", "https://a.example/messages");
        let scope_b = pi_rs_core::endpoint_replay_scope("anthropic", "https://b.example/messages");
        let request = ModelRequest {
            model: "claude".into(),
            messages: vec![
                Message::User {
                    content: "question".into(),
                },
                Message::Assistant {
                    content: String::new(),
                    calls: vec![],
                    replay: Some(Box::new(ProviderReplay {
                        identity: ResolvedModelIdentity {
                            provider: scope_a.clone(),
                            model: "claude".into(),
                        },
                        blocks: vec![
                            json!({"type":"thinking","thinking":"secret","signature":"sig"}),
                            json!({"_pi_context_edit_api":"anthropic-messages"}),
                        ],
                    })),
                },
            ],
            tools: vec![],
            max_output_tokens: 16,
        };
        let same = encode_request(request.clone(), "anthropic", &scope_a, false).unwrap();
        assert_eq!(same["messages"][1]["content"][0]["thinking"], "secret");
        let other = encode_request(request, "anthropic", &scope_b, false).unwrap();
        assert_eq!(other["messages"].as_array().unwrap().len(), 1);
        assert!(!other.to_string().contains("secret"));
    }

    #[test]
    fn edited_replay_is_scoped_and_private_marker_is_not_sent() {
        let make = |model: &str| ModelRequest {
            model: model.into(),
            tools: vec![],
            max_output_tokens: 16,
            messages: vec![Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: ResolvedModelIdentity {
                        provider: "anthropic".into(),
                        model: "claude".into(),
                    },
                    blocks: vec![
                        json!({"type":"thinking","thinking":"secret","signature":"signed"}),
                        json!({"type":"text","text":"answer"}),
                        json!({"_pi_context_edit_api":"anthropic-messages"}),
                    ],
                })),
            }],
        };
        let same = encode_request(make("claude"), "anthropic", "anthropic", false).unwrap();
        assert_eq!(
            same["messages"][0]["content"],
            json!([{"type":"thinking","thinking":"secret","signature":"signed"},{"type":"text","text":"answer"}])
        );
        let other = encode_request(make("other"), "anthropic", "anthropic", false).unwrap();
        assert_eq!(
            other["messages"][0]["content"],
            json!([{"type":"text","text":"answer"}])
        );
    }

    #[test]
    fn tool_result_error_status_is_encoded_independently_of_text() {
        let body = encode_request(
            ModelRequest {
                model: "fixture".into(),
                messages: vec![
                    Message::User {
                        content: "go".into(),
                    },
                    Message::Tool {
                        call_id: "failed".into(),
                        content: "normal text".into(),
                        is_error: true,
                        is_structured: Some(false),
                    },
                    Message::Tool {
                        call_id: "ok".into(),
                        content: "Tool failed: but succeeded".into(),
                        is_error: false,
                        is_structured: Some(false),
                    },
                ],
                tools: vec![],
                max_output_tokens: 128,
            },
            "fixture",
            "fixture",
            false,
        )
        .unwrap();
        let blocks = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|turn| turn["content"].as_array().unwrap())
            .filter(|block| block["type"] == "tool_result")
            .collect::<Vec<_>>();
        assert_eq!(blocks[0]["is_error"], true);
        assert_eq!(blocks[0]["content"], "normal text");
        assert!(blocks[1].get("is_error").is_none());
        assert_eq!(blocks[1]["content"], "Tool failed: but succeeded");
    }

    #[test]
    fn input_transformations_keep_only_diagnostic_fields() {
        let details = AnthropicInputTransformations::from_value(&json!([
            {"type":"thinking_dropped","path":"messages[1].content[0]","reason":"invalid_signature","thinking":"private"},
            null,
            {"type":"other","reason":3}
        ]))
        .unwrap();
        let diagnostic = AssistantDiagnostic::AnthropicInputTransformations {
            timestamp: 123,
            details,
        };
        assert_eq!(diagnostic.dropped_thinking_blocks(), 1);
        assert_eq!(
            serde_json::to_value(diagnostic).unwrap(),
            json!({
                "type":"anthropic_input_transformations",
                "timestamp":123,
                "details":{"transformations":[
                    {"type":"thinking_dropped","path":"messages[1].content[0]","reason":"invalid_signature"},
                    {"type":"other"}
                ]}
            })
        );
        assert!(AnthropicInputTransformations::from_value(&json!([])).is_none());
    }

    #[test]
    fn thinking_controls_respect_advertised_effort_and_output_budget() {
        let adaptive = decode_model(&json!({
            "id":"adaptive",
            "capabilities":{
                "thinking":{"types":{"adaptive":{"supported":true},"disabled":{"supported":false}}},
                "effort":{"low":{"supported":true},"high":{"supported":true}}
            }
        }))
        .unwrap();
        assert_eq!(adaptive.thinking_levels, ["low", "high"]);
        let mut body = json!({});
        assert!(configure_thinking(&mut body, Some(&adaptive), "off", 2048, None).is_err());
        configure_thinking(&mut body, Some(&adaptive), "high", 2048, None).unwrap();
        assert_eq!(body["thinking"]["type"], "adaptive");
        assert_eq!(body["output_config"]["effort"], "high");

        let legacy = decode_model(&json!({
            "id":"legacy","capabilities":{"thinking":{"types":{"enabled":{"supported":true}}}}
        }))
        .unwrap();
        let mut body = json!({});
        assert!(configure_thinking(&mut body, Some(&legacy), "high", 1024, None).is_err());
        configure_thinking(&mut body, Some(&legacy), "high", 2048, None).unwrap();
        assert_eq!(
            body["thinking"],
            json!({"type":"enabled","budget_tokens":1024,"display":"summarized"})
        );
        let budgets = json!({"high": 3456, "low": "invalid"});
        configure_thinking(&mut body, Some(&legacy), "xhigh", 10000, Some(&budgets)).unwrap();
        assert_eq!(body["thinking"]["budget_tokens"], 3456);
        assert!(
            configure_thinking(&mut body, Some(&legacy), "low", 10000, Some(&budgets)).is_err()
        );
        configure_thinking(&mut body, Some(&legacy), "high", 2048, Some(&budgets)).unwrap();
        assert_eq!(body["thinking"]["budget_tokens"], 1024);
        for (level, expected) in [
            ("minimal", 1024),
            ("low", 2048),
            ("medium", 8192),
            ("high", 16384),
            ("xhigh", 16384),
        ] {
            configure_thinking(&mut body, Some(&legacy), level, 20000, None).unwrap();
            assert_eq!(body["thinking"]["budget_tokens"], expected);
        }
    }

    #[test]
    fn tool_blocks_must_close_with_an_object_before_dispatch() {
        let call = |complete, fragments: &str| Call {
            id: "toolu_fixture".into(),
            name: "read".into(),
            raw: json!({"type":"tool_use","id":"toolu_fixture","name":"read","input":{}}),
            fragments: fragments.into(),
            complete,
        };
        assert!(call(false, r#"{"path":"file.txt"}"#).finish().is_err());
        assert!(call(true, r#"{"path":"file.txt""#).finish().is_err());
        assert!(call(true, "[]").finish().is_err());
        let (result, _) = call(true, r#"{"path":"file.txt"}"#).finish().unwrap();
        assert_eq!(result.arguments, r#"{"path":"file.txt"}"#);
    }
}
