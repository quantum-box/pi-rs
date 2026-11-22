//! Streaming adapter for Ollama's /v1/chat/completions interface.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::header::HeaderMap;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod compatibility;
mod mistral;
mod reasoning_details;
pub use compatibility::ChatCompatibility;

use pi_rs_core::{
    CacheRetention, Error, Message, Model, ModelApi, ModelEvent, ModelMetadata, ModelRequest,
    ModelStream, ProviderReplay, ReportedUsageCost, ResolvedModelIdentity, Result, ToolCall, Usage,
};

type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct Ollama {
    client: Client,
    provider_retry: pi_rs_core::ProviderRetry,
    http_proxies: Vec<reqwest::Proxy>,
    endpoint: Url,
    provider: String,
    token: Option<String>,
    headers: HeaderMap,
    cache_retention: CacheRetention,
    prompt_cache_key_enabled: bool,
    prompt_cache_long: bool,
    session_cache_key: Option<String>,
    reasoning_effort: Option<String>,
    template_reasoning_effort: Option<String>,
    thinking_budget_level: Option<String>,
    thinking_budgets: Option<Value>,
    compatibility: Arc<ChatCompatibility>,
    supports_reasoning: Option<bool>,
    reasoning_enabled: bool,
    allow_reasoning_off: bool,
    sampling: BTreeMap<String, Value>,
    deployments: Arc<BTreeMap<String, String>>,
    native_probes: bool,
    mistral: Option<mistral::Options>,
    progress: Option<Arc<dyn Fn(ModelProgress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
}

/// A prepared Chat Completions request with immutable wire tool decisions.
pub struct PreparedChatRequest {
    client: Ollama,
    request: ModelRequest,
    reservation_request: ModelRequest,
    wire_tools: ChatWireTools,
    effective_tools: Vec<pi_rs_core::EffectiveToolDeclaration>,
}

#[derive(Default)]
struct ChatWireTools {
    declarations: Vec<Value>,
    grammar_inputs: BTreeMap<String, String>,
}

impl PreparedChatRequest {
    /// Decisions for the exact tool schemas that will be sent.
    pub fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
        &self.effective_tools
    }
}

struct PreparedChatDispatch {
    request: PreparedChatRequest,
    snapshot: pi_rs_core::ToolDecisionSnapshot,
}

#[async_trait::async_trait]
impl pi_rs_core::PreparedToolDispatch for PreparedChatDispatch {
    fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
        &self.request.effective_tools
    }
    fn decision_snapshot(&self) -> &pi_rs_core::ToolDecisionSnapshot {
        &self.snapshot
    }
    fn dispatch_request(&self) -> &ModelRequest {
        &self.request.request
    }
    fn reservation_request(&self) -> &ModelRequest {
        &self.request.reservation_request
    }
    async fn dispatch(self: Box<Self>) -> Result<pi_rs_core::DetailedModelStream> {
        let stream = self
            .request
            .client
            .stream_with_wire_tools(self.request.request, Some(self.request.wire_tools))
            .await?;
        Ok(Box::pin(stream.map(|event| {
            event.map(|event| pi_rs_core::EnrichedModelEvent::Legacy(event).into())
        })))
    }
}

/// Capabilities reported by the native Ollama service.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
    #[serde(rename = "maxTokens", skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking_levels: Option<Vec<String>>,
}

/// Transient presentation signals, separate from the durable model result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProgress {
    WaitingForModel,
    LoadingModel,
    Thinking,
    Generating,
    PreparingTool,
}

fn report_progress(
    observer: &Option<Arc<dyn Fn(ModelProgress) + Send + Sync>>,
    previous: &mut Option<ModelProgress>,
    progress: ModelProgress,
) {
    if *previous != Some(progress) {
        *previous = Some(progress);
        if let Some(observer) = observer {
            observer(progress);
        }
    }
}

impl Ollama {
    /// Resolve constraints once, using the configured compatibility capability.
    pub fn prepare_tool_request(
        &self,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<PreparedChatRequest> {
        let (wire_tools, effective_tools) = resolved_constrained_tools(
            &request,
            declarations,
            self.compatibility.supports_strict_mode == Some(true),
            self.mistral.is_none()
                && self.compatibility.supports_openai_grammar_tools() == Some(true),
        )?;
        let mut reservation_request = request.clone();
        for (spec, wire) in reservation_request
            .tools
            .iter_mut()
            .zip(&wire_tools.declarations)
        {
            spec.parameters = if wire["type"] == "custom" {
                wire["custom"]["format"].clone()
            } else {
                wire["function"]["parameters"].clone()
            };
        }
        Ok(PreparedChatRequest {
            client: self.clone(),
            request,
            reservation_request,
            wire_tools,
            effective_tools,
        })
    }

    pub fn prepare_bound_tool_dispatch(
        &self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Box<dyn pi_rs_core::PreparedToolDispatch>> {
        let client = if context.summary {
            self.clone().without_cache()
        } else {
            self.clone().with_session_cache_key(&context.external_id)
        };
        let prepared = client.prepare_tool_request(request.clone(), declarations)?;
        let snapshot = pi_rs_core::ToolDecisionSnapshot::capture(
            context,
            request,
            declarations,
            &prepared.effective_tools,
        )
        .map_err(|error| Error::Config(error.to_string()))?;
        Ok(Box::new(PreparedChatDispatch {
            request: prepared,
            snapshot,
        }))
    }

    /// Accept a loopback HTTP service or a TLS-protected relay, without redirects or proxy discovery.
    pub fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
        let openai_endpoint = endpoint.contains("api.openai.com");
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid model endpoint".into()))?;
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
            return Err(Error::Config("model endpoint must use HTTPS or loopback HTTP and contain no credentials, query, or fragment".into()));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::Config("could not build model HTTP client".into()))?;
        Ok(Self {
            client,
            http_proxies: Vec::new(),
            provider_retry: pi_rs_core::ProviderRetry::default(),
            endpoint,
            provider: "ollama".into(),
            token,
            headers: HeaderMap::new(),
            cache_retention: CacheRetention::None,
            prompt_cache_key_enabled: openai_endpoint,
            prompt_cache_long: openai_endpoint
                && std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long"),
            session_cache_key: None,
            reasoning_effort: None,
            template_reasoning_effort: None,
            thinking_budget_level: None,
            thinking_budgets: None,
            compatibility: Arc::default(),
            supports_reasoning: None,
            reasoning_enabled: false,
            allow_reasoning_off: true,
            sampling: BTreeMap::new(),
            deployments: Arc::default(),
            native_probes: true,
            mistral: None,
            progress: None,
            reasoning: None,
        })
    }

    /// Enable the Mistral wire protocol and its model-specific capabilities.
    pub fn with_mistral(
        mut self,
        supports_images: bool,
        prompt_mode: bool,
        effort: Option<&str>,
    ) -> Self {
        self.mistral = Some(mistral::Options {
            images: supports_images,
            prompt_mode,
            effort: effort.map(str::to_owned),
        });
        Arc::make_mut(&mut self.compatibility)
            .supports_strict_mode
            .get_or_insert(true);
        self.native_probes = false;
        self
    }

    /// Map catalog model IDs to their wire deployment names.
    pub fn with_deployments(mut self, deployments: &BTreeMap<String, String>) -> Self {
        self.deployments = Arc::new(deployments.clone());
        self
    }

    /// Set the per-read HTTP idle timeout; `None` disables it.
    pub fn with_http_idle_timeout(self, timeout: Option<Duration>) -> Result<Self> {
        let proxies = self.http_proxies.clone();
        self.with_http_options(timeout, &proxies)
    }

    /// Configure the per-read deadline and explicit HTTP proxies.
    /// Configure provider-level retries before a response stream starts.
    pub fn with_provider_retry(mut self, policy: pi_rs_core::ProviderRetry) -> Self {
        self.provider_retry = policy;
        self
    }

    pub fn with_http_options(
        mut self,
        timeout: Option<Duration>,
        proxies: &[reqwest::Proxy],
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(10));
        for proxy in proxies {
            builder = builder.proxy(proxy.clone());
        }
        if let Some(timeout) = timeout {
            builder = builder.read_timeout(timeout);
        }
        self.client = builder
            .build()
            .map_err(|_| Error::Config("could not build model HTTP client".into()))?;
        self.http_proxies = proxies.to_vec();
        Ok(self)
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

    fn authenticate(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let request = match (&self.token, self.provider.as_ref()) {
            (Some(token), "cloudflare-ai-gateway") => {
                request.header("cf-aig-authorization", format!("Bearer {token}"))
            }
            (Some(token), _) => request.bearer_auth(token),
            (None, _) => request,
        };
        request.headers(self.headers.clone())
    }

    /// Enable or suppress auxiliary native endpoint probes.
    pub fn with_native_probes(mut self, enabled: bool) -> Self {
        self.native_probes = enabled;
        self
    }

    /// Override provider thinking effort. None preserves the model default.
    pub fn with_reasoning_effort(mut self, effort: Option<&str>) -> Self {
        let local_ollama = self.endpoint.port_or_known_default() == Some(11434);
        self.template_reasoning_effort = effort.filter(|level| *level != "off").map(str::to_owned);
        self.reasoning_effort = effort.map(|effort| {
            match effort {
                "off" => "none",
                "minimal" if local_ollama => "low",
                "xhigh" | "max" if local_ollama => "high",
                effort => effort,
            }
            .to_owned()
        });
        self
    }

    /// Set an effort already resolved against the model's advertised controls.
    pub fn with_reported_reasoning_effort(mut self, effort: Option<&str>) -> Self {
        self.template_reasoning_effort = effort.filter(|level| *level != "off").map(str::to_owned);
        self.reasoning_effort =
            effort.map(|level| if level == "off" { "none" } else { level }.to_owned());
        self
    }

    /// Apply the provider value from an explicit thinking-level map.
    pub fn with_mapped_reasoning_effort(mut self, effort: Option<&str>) -> Self {
        self.template_reasoning_effort = effort.map(str::to_owned);
        self.reasoning_effort = effort.map(str::to_owned);
        self
    }

    fn chat_reasoning_effort(&self) -> Option<&str> {
        if self.compatibility.uses_chat_template() {
            self.template_reasoning_effort.as_deref()
        } else {
            self.reasoning_effort.as_deref()
        }
    }

    /// Set the selected Pi thinking level and optional budget overrides.
    pub fn with_thinking_budgets(mut self, level: Option<&str>, budgets: Option<Value>) -> Self {
        self.thinking_budget_level = level.map(str::to_owned);
        self.thinking_budgets = budgets;
        self
    }

    /// Use Anthropic-style cache markers on a compatible chat endpoint.
    pub fn with_cache_retention(mut self, retention: CacheRetention) -> Self {
        self.cache_retention = retention;
        self
    }

    /// Configure session cache keys and the OpenAI-compatible long retention field.
    pub fn with_prompt_cache_policy(mut self, key_enabled: bool, long: bool) -> Self {
        self.prompt_cache_key_enabled = key_enabled;
        self.prompt_cache_long = long;
        self
    }

    /// Attach the current external session identity for prompt cache affinity.
    pub fn with_session_cache_key(mut self, session_id: &str) -> Self {
        self.session_cache_key =
            (!session_id.is_empty()).then(|| session_id.chars().take(64).collect());
        self
    }

    /// Disable automatic prompt caching for summary requests.
    pub fn without_cache(mut self) -> Self {
        self.cache_retention = CacheRetention::None;
        self.prompt_cache_key_enabled = false;
        self.prompt_cache_long = false;
        self.session_cache_key = None;
        self
    }

    pub fn with_chat_compatibility(
        mut self,
        compatibility: ChatCompatibility,
        reasoning: Option<bool>,
        enabled: bool,
        allow_off: bool,
    ) -> Self {
        self.compatibility = Arc::new(compatibility);
        self.supports_reasoning = reasoning;
        self.reasoning_enabled = enabled;
        self.allow_reasoning_off = allow_off;
        self
    }

    /// Set endpoint-wide strict tool support; callers must ensure it applies to
    /// every model dispatched through this client.
    pub fn with_strict_tool_support(mut self, supported: bool) -> Self {
        Arc::make_mut(&mut self.compatibility).supports_strict_mode = Some(supported);
        self
    }

    /// Set custom grammar tool support for the routed model.
    pub fn with_grammar_tool_support(mut self, supported: bool) -> Self {
        Arc::make_mut(&mut self.compatibility).set_openai_grammar_tool_support(supported);
        self
    }

    /// Scope replay to the configured provider, even on shared gateways.
    pub fn with_provider(mut self, provider: &str) -> Self {
        self.provider = provider.to_owned();
        self
    }

    fn replay_identity(&self, model: &str) -> ResolvedModelIdentity {
        let provider = if let Some(deployment) = self.deployments.get(model) {
            pi_rs_core::endpoint_replay_scope(
                &self.provider,
                &format!("{}\n{deployment}", self.endpoint),
            )
        } else {
            pi_rs_core::endpoint_replay_scope(&self.provider, self.endpoint.as_str())
        };
        ResolvedModelIdentity {
            provider,
            model: model.to_owned(),
        }
    }

    fn has_ambiguous_legacy_replay(&self, request: &ModelRequest) -> bool {
        let scope = format!(
            "chat-completions+sha256:{:x}",
            Sha256::digest(self.endpoint.as_str().as_bytes())
        );
        request.messages.iter().any(|message| matches!(message, Message::Assistant { replay: Some(replay), .. } if replay.identity.model == request.model && replay.identity.provider == scope))
    }

    fn compatible_message(&self, message: Message, identity: &ResolvedModelIdentity) -> Vec<Value> {
        let mut reasoning = None;
        let mut details = Vec::new();
        let mut edited_reasoning = Vec::new();
        let mut edited_order = Vec::new();
        if let Message::Assistant {
            replay: Some(replay),
            ..
        } = &message
            && replay.identity == *identity
            && replay.supports_api(ModelApi::OpenaiCompletions)
        {
            for block in &replay.blocks {
                if block["type"] == "context_edit_reasoning" {
                    edited_reasoning.push(block.clone());
                    edited_order.push(block.clone());
                    if self.compatibility.preserves_reasoning_details()
                        && let Some(items) = block["details"].as_array()
                    {
                        details.extend(
                            items
                                .iter()
                                .filter(|item| reasoning_details::is_valid(item))
                                .cloned(),
                        );
                    }
                } else if block["type"] == "context_edit_text" {
                    edited_order.push(block.clone());
                } else if block["type"] == "reasoning_content" {
                    reasoning = block["text"].as_str().map(str::to_owned);
                } else if self.compatibility.preserves_reasoning_details()
                    && reasoning_details::is_valid(block)
                {
                    details.push(block.clone());
                }
            }
        }
        let mut messages = encode_message(message);
        let Some(first) = messages.first_mut() else {
            return messages;
        };
        if first["role"] == "system"
            && self.supports_reasoning == Some(true)
            && self.compatibility.supports_developer_role == Some(true)
        {
            first["role"] = json!("developer");
        }
        if first["role"] == "assistant"
            && self.supports_reasoning != Some(false)
            && self
                .compatibility
                .requires_reasoning_content_on_assistant_messages
                == Some(true)
        {
            first["reasoning_content"] = json!(if self.compatibility.requires_thinking_as_text
                == Some(true)
            {
                ""
            } else {
                reasoning.as_deref().unwrap_or_default()
            });
        }
        if !details.is_empty() {
            first["reasoning_details"] = Value::Array(details);
        }
        if edited_reasoning.is_empty()
            && self.supports_reasoning != Some(false)
            && self.compatibility.requires_thinking_as_text == Some(true)
            && let Some(text) = reasoning.as_deref().filter(|text| !text.trim().is_empty())
        {
            let mut parts = vec![json!({"type":"text","text":text})];
            if let Some(content) = first["content"]
                .as_str()
                .filter(|content| !content.is_empty())
            {
                parts.push(json!({"type":"text","text":content}));
            }
            first["content"] = json!(parts);
        }
        if !edited_reasoning.is_empty() && self.supports_reasoning != Some(false) {
            let text = edited_reasoning
                .iter()
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n");
            if self.compatibility.requires_thinking_as_text == Some(true) {
                let ordered: String = edited_order
                    .iter()
                    .filter_map(|b| b["text"].as_str())
                    .collect();
                first["content"] = json!(if edited_order
                    .iter()
                    .any(|b| b["type"] == "context_edit_text")
                {
                    ordered
                } else if first["content"].as_str().unwrap_or_default().is_empty() {
                    text
                } else {
                    format!(
                        "{text}\n\n{}",
                        first["content"].as_str().unwrap_or_default()
                    )
                });
            } else {
                let ordinary: Vec<_> = edited_reasoning
                    .iter()
                    .filter(|b| {
                        !self.compatibility.preserves_reasoning_details()
                            || !b["details"]
                                .as_array()
                                .is_some_and(|items| items.iter().any(reasoning_details::is_valid))
                    })
                    .collect();
                let text = ordinary
                    .iter()
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                let field = ordinary
                    .first()
                    .and_then(|b| b["field"].as_str())
                    .or(self.compatibility.reasoning_field.as_deref())
                    .unwrap_or("reasoning_content");
                if !text.is_empty()
                    && matches!(field, "reasoning_content" | "reasoning" | "reasoning_text")
                {
                    first[field] = json!(text);
                }
            }
        }
        if first["role"] == "assistant"
            && first["content"].as_str().is_some_and(str::is_empty)
            && first.get("tool_calls").is_none()
            && first
                .get("reasoning_content")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            && first
                .get("reasoning")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            && first
                .get("reasoning_text")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            && first.get("reasoning_details").is_none()
        {
            return Vec::new();
        }
        messages
    }

    pub fn with_sampling(mut self, sampling: BTreeMap<String, Value>) -> Result<Self> {
        validate_sampling(&sampling)?;
        self.sampling = sampling;
        Ok(self)
    }

    pub fn with_progress(
        mut self,
        observer: impl Fn(ModelProgress) + Send + Sync + 'static,
    ) -> Self {
        self.progress = Some(Arc::new(observer));
        self
    }

    /// Observe provider-supplied reasoning deltas separately from the answer.
    pub fn with_reasoning(mut self, observer: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.reasoning = Some(Arc::new(observer));
        self
    }

    /// Discover models from the configured OpenAI-compatible endpoint.
    pub async fn models(&self) -> Result<Vec<String>> {
        let mut endpoint = self.endpoint.clone();
        let path = endpoint
            .path()
            .strip_suffix("chat/completions")
            .ok_or_else(|| {
                Error::Config("model discovery requires a chat/completions endpoint".into())
            })?;
        let path = format!("{path}models");
        endpoint.set_path(&path);
        let request = self
            .authenticate(self.client.get(endpoint))
            .timeout(Duration::from_secs(10));
        let mut response = request
            .send()
            .await
            .map_err(|_| Error::Model("model discovery request failed".into()))?;
        if !response.status().is_success() {
            return Err(Error::Model(format!(
                "model discovery returned HTTP {}",
                response.status()
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::Model("model discovery response interrupted".into()))?
        {
            if bytes.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
                return Err(Error::Model("model list exceeds size limit".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        let response: Value = serde_json::from_slice(&bytes)?;
        let models = response["data"]
            .as_array()
            .ok_or_else(|| Error::Model("invalid model list".into()))?;
        let mut models: Vec<_> = models
            .iter()
            .filter_map(|model| model["id"].as_str())
            .map(str::to_owned)
            .collect();
        models.sort();
        models.dedup();
        Ok(models)
    }

    /// Inspect selected catalog entries on a native service within a shared time bound.
    pub async fn model_details(&self, models: &[String]) -> BTreeMap<String, ModelDetails> {
        let mut details = BTreeMap::new();
        if models.is_empty() {
            return details;
        }
        let Some(running) = self.running_models().await else {
            return details;
        };
        if !running["models"].is_array() {
            return details;
        }
        let Some(endpoint) = self.native_endpoint("show") else {
            return details;
        };
        let mut requests = futures_util::stream::iter(models.iter().map(|model| {
            let endpoint = endpoint.clone();
            async move { (model, self.show_model(endpoint, model).await) }
        }))
        .buffer_unordered(4);
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some((model, info)) = requests.next().await {
                let mut info = info.unwrap_or_default();
                info.context_window = info
                    .context_window
                    .or_else(|| running_context_window(&running, model));
                if info != ModelDetails::default() {
                    details.insert(model.clone(), info);
                }
            }
        })
        .await;
        details
    }

    fn native_endpoint(&self, operation: &str) -> Option<Url> {
        if !self.native_probes {
            return None;
        }
        let prefix = self.endpoint.path().strip_suffix("/v1/chat/completions")?;
        let mut endpoint = self.endpoint.clone();
        endpoint.set_path(&format!("{prefix}/api/{operation}"));
        Some(endpoint)
    }

    async fn show_model(&self, endpoint: Url, model: &str) -> Option<ModelDetails> {
        let request = self
            .authenticate(self.client.post(endpoint))
            .timeout(Duration::from_millis(750))
            .json(&json!({"model": model}));
        let value = read_native_response(request).await?;
        Some(decode_model_details(&value))
    }

    async fn running_models(&self) -> Option<Value> {
        let request = self
            .authenticate(self.client.get(self.native_endpoint("ps")?))
            .timeout(Duration::from_millis(250));
        read_native_response(request).await
    }

    async fn model_loaded(&self, model: &str) -> Option<bool> {
        let value = self.running_models().await?;
        Some(
            value
                .get("models")?
                .as_array()?
                .iter()
                .any(|item| running_model_matches(item, model)),
        )
    }
}

/// Sampling parameters can extend a request without replacing its managed fields.
pub fn validate_sampling(sampling: &BTreeMap<String, Value>) -> Result<()> {
    if let Some(key) = sampling.keys().find(|key| {
        matches!(
            key.as_str(),
            "model"
                | "messages"
                | "tools"
                | "tool_choice"
                | "functions"
                | "function_call"
                | "stream"
                | "stream_options"
                | "max_tokens"
                | "max_completion_tokens"
                | "max_output_tokens"
                | "reasoning_effort"
                | "n"
        )
    }) {
        return Err(Error::Config(format!(
            "sampling parameter {key:?} is managed by the agent"
        )));
    }
    Ok(())
}

async fn read_native_response(request: reqwest::RequestBuilder) -> Option<Value> {
    let mut response = request.send().await.ok()?.error_for_status().ok()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
            return None;
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).ok()
}

fn decode_model_details(value: &Value) -> ModelDetails {
    let info = &value["model_info"];
    let context_window = info["general.architecture"]
        .as_str()
        .and_then(|arch| info.get(format!("{arch}.context_length")))
        .and_then(Value::as_u64)
        .and_then(|length| u32::try_from(length).ok())
        .filter(|length| *length > 0);
    let capability = |name: &str| {
        value["capabilities"]
            .as_array()
            .map(|items| items.iter().any(|item| item.as_str() == Some(name)))
    };
    let mut levels = Vec::new();
    if let Some(values) = value["thinking"]["values"].as_array() {
        for value in values {
            let level = match value {
                Value::Bool(false) => Some("off"),
                Value::Bool(true) => Some("high"),
                Value::String(level) if !level.trim().is_empty() => Some(level.as_str()),
                _ => None,
            };
            if let Some(level) = level
                && levels.iter().all(|existing| existing != level)
            {
                levels.push(level.to_owned());
            }
        }
    }
    let reasoning = if levels.is_empty() {
        capability("thinking")
    } else {
        Some(levels.iter().any(|level| level != "off"))
    };
    if levels.is_empty() && reasoning == Some(false) {
        levels.push("off".into());
    }
    ModelDetails {
        context_window,
        max_output_tokens: None,
        reasoning,
        images: capability("vision"),
        thinking_levels: (!levels.is_empty()).then_some(levels),
    }
}

fn running_model_matches(item: &Value, model: &str) -> bool {
    item.get("name")
        .or_else(|| item.get("model"))
        .and_then(Value::as_str)
        .is_some_and(|name| name.trim_end_matches(":latest") == model.trim_end_matches(":latest"))
}

fn running_context_window(value: &Value, model: &str) -> Option<u32> {
    value
        .get("models")?
        .as_array()?
        .iter()
        .filter(|item| running_model_matches(item, model))
        .filter_map(|item| item.get("context_length")?.as_u64()?.try_into().ok())
        .filter(|window| *window > 0)
        .min()
}

#[async_trait::async_trait]
impl Model for Ollama {
    fn supports_mid_conversation_system_messages(&self, _model: &str) -> bool {
        self.mistral.is_none()
            && self
                .compatibility
                .supports_mid_conversation_system_messages()
    }

    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Option<Box<dyn pi_rs_core::PreparedToolDispatch + 'a>>> {
        Ok(Some(self.prepare_bound_tool_dispatch(
            request,
            declarations,
            context,
        )?))
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let prepared = self
            .clone()
            .with_session_cache_key(external_id)
            .prepare_tool_request(request, declarations)?;
        reject_stream_only_strict(&prepared.effective_tools)?;
        let stream = prepared
            .client
            .stream_with_wire_tools(prepared.request, Some(prepared.wire_tools))
            .await?;
        Ok(Box::pin(stream.map(|event| {
            event.map(|event| pi_rs_core::EnrichedModelEvent::Legacy(event).into())
        })))
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let prepared = self
            .clone()
            .without_cache()
            .prepare_tool_request(request, declarations)?;
        reject_stream_only_strict(&prepared.effective_tools)?;
        let stream = prepared
            .client
            .stream_with_wire_tools(prepared.request, Some(prepared.wire_tools))
            .await?;
        Ok(Box::pin(stream.map(|event| {
            event.map(|event| pi_rs_core::EnrichedModelEvent::Legacy(event).into())
        })))
    }

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
        let base = pi_rs_core::protected_model_fingerprint(&(
            headers,
            "chat-completions-v1",
            self.endpoint.as_str(),
            &self.provider,
            &self.token,
            format!("{:?}", self.compatibility),
            self.supports_reasoning,
            self.reasoning_enabled,
            self.allow_reasoning_off,
            self.chat_reasoning_effort(),
            &self.thinking_budget_level,
            &self.thinking_budgets,
            &self.sampling,
            self.cache_retention.anthropic_control(),
        ));
        let base = if self.mistral.is_some() {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(base, format!("{:?}", self.mistral)))
            })
        } else {
            base
        };
        let base = if self.deployments.is_empty() {
            base
        } else {
            pi_rs_core::protected_model_fingerprint(&(base, self.deployments.as_ref()))
        };
        if self.prompt_cache_key_enabled || self.prompt_cache_long {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(
                    base,
                    self.prompt_cache_key_enabled,
                    self.prompt_cache_long,
                ))
            })
        } else {
            base
        }
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let base = pi_rs_core::estimate_message_tokens(message);
        if self.mistral.is_some() {
            return match message {
                Message::Assistant {
                    calls,
                    replay: Some(replay),
                    ..
                } if replay.identity == self.replay_identity(&request.model)
                    && replay.supports_api(ModelApi::MistralConversations) =>
                {
                    mistral::replay_parts(&replay.blocks).map_or(base, |parts| {
                        let calls = calls
                            .iter()
                            .map(|call| json!({"name":call.name,"arguments":call.arguments}))
                            .collect::<Vec<_>>();
                        pi_rs_core::estimate_replay_tokens(&parts)
                            .saturating_add(pi_rs_core::estimate_replay_tokens(&calls))
                    })
                }
                _ => base,
            };
        }
        if matches!(message, Message::Assistant { .. }) {
            let encoded =
                self.compatible_message(message.clone(), &self.replay_identity(&request.model));
            let extra = encoded.iter().fold(0_u64, |sum, item| {
                [
                    "reasoning_content",
                    "reasoning",
                    "reasoning_text",
                    "reasoning_details",
                ]
                .iter()
                .filter_map(|key| item.get(*key))
                .filter(|value| !value.as_str().is_some_and(str::is_empty))
                .fold(sum, |sum, value| {
                    sum.saturating_add(pi_rs_core::estimate_replay_tokens(std::slice::from_ref(
                        value,
                    )))
                })
            });
            let visible_extra = if self.supports_reasoning != Some(false)
                && self.compatibility.requires_thinking_as_text == Some(true)
            {
                if let Message::Assistant {
                    replay: Some(replay),
                    ..
                } = message
                {
                    if replay.identity == self.replay_identity(&request.model)
                        && replay.supports_api(ModelApi::OpenaiCompletions)
                    {
                        if replay
                            .blocks
                            .iter()
                            .any(|b| b["type"] == "context_edit_reasoning")
                        {
                            replay
                                .blocks
                                .iter()
                                .filter(|b| b["type"] == "context_edit_reasoning")
                                .filter_map(|b| b["text"].as_str())
                                .fold(0u64, |sum, text| {
                                    sum.saturating_add(pi_rs_core::estimate_replay_tokens(&[
                                        json!(text),
                                    ]))
                                })
                        } else {
                            replay
                                .blocks
                                .iter()
                                .rev()
                                .find(|b| b["type"] == "reasoning_content")
                                .and_then(|b| b["text"].as_str())
                                .filter(|text| !text.trim().is_empty())
                                .map_or(0, |text| {
                                    pi_rs_core::estimate_replay_tokens(&[json!(text)])
                                })
                        }
                    } else {
                        0
                    }
                } else {
                    0
                }
            } else {
                0
            };
            base.saturating_add(extra).saturating_add(visible_extra)
        } else {
            base
        }
    }

    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        let head = self
            .mistral
            .is_none()
            .then(|| self.compatibility.leading_system_message(&request.messages))
            .flatten();
        let mut base = request.clone();
        base.messages.clear();
        let messages =
            head.iter()
                .chain(
                    request.messages.iter().filter(|message| {
                        head.is_none() || !matches!(message, Message::System { .. })
                    }),
                )
                .filter(
                    |message| !matches!(message, Message::System { content } if content.is_empty()),
                );
        messages.fold(
            pi_rs_core::estimate_request_tokens(&base),
            |total, message| total.saturating_add(self.estimate_message_tokens(request, message)),
        )
    }

    async fn context_window(&self, model: &str) -> Option<u32> {
        let value = self.running_models().await?;
        running_context_window(&value, model)
    }

    async fn stream_in_session(
        &self,
        _request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.clone()
            .with_session_cache_key(&session_id.to_string())
            .stream(request)
            .await
    }

    async fn stream_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.clone()
            .with_session_cache_key(external_id)
            .stream(request)
            .await
    }

    async fn stream_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.clone().without_cache().stream(request).await
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        self.stream_with_wire_tools(request, None).await
    }
}

impl Ollama {
    fn wire_messages(
        &self,
        messages: Vec<Message>,
        identity: &ResolvedModelIdentity,
    ) -> Vec<Value> {
        let head = self.compatibility.leading_system_message(&messages);
        let collapse_system = head.is_some();
        head.into_iter()
            .chain(
                messages.into_iter().filter(|message| {
                    !collapse_system || !matches!(message, Message::System { .. })
                }),
            )
            .filter(|message| !matches!(message, Message::System { content } if content.is_empty()))
            .flat_map(|message| self.compatible_message(message, identity))
            .collect()
    }

    async fn stream_with_wire_tools(
        &self,
        request: ModelRequest,
        wire_tools: Option<ChatWireTools>,
    ) -> Result<ModelStream> {
        if self.has_ambiguous_legacy_replay(&request) {
            return Err(Error::Model("Legacy chat-completions reasoning replay has no provider identity. Edit or remove the affected assistant context, or start a new session, before continuing.".into()));
        }
        let copilot = (self.provider == "github-copilot")
            .then(|| pi_rs_core::copilot_request_headers(&request.messages).collect::<Vec<_>>());
        let requested_model = request.model.clone();
        let identity = self.replay_identity(&requested_model);
        let infer_finish = self.compatibility.supports_finish_reason == Some(false);
        let preserve_details = self.compatibility.preserves_reasoning_details();
        let preserve_reasoning = self.supports_reasoning != Some(false)
            && (self
                .compatibility
                .requires_reasoning_content_on_assistant_messages
                == Some(true)
                || self.compatibility.requires_thinking_as_text == Some(true));
        let mut last_progress = None;
        report_progress(
            &self.progress,
            &mut last_progress,
            ModelProgress::WaitingForModel,
        );
        let mistral = self.mistral.is_some();
        let mut messages = if let Some(options) = &self.mistral {
            mistral::messages(request.messages, &identity, options.images)?
        } else {
            self.wire_messages(request.messages, &identity)
        };
        let mut wire_tools = wire_tools.unwrap_or_else(|| ChatWireTools {
            declarations: request
                .tools
                .into_iter()
                .map(|function| {
                    let mut function = json!(function);
                    if self.compatibility.supports_strict_mode == Some(true) {
                        function["strict"] = json!(false);
                    }
                    json!({"type": "function", "function": function})
                })
                .collect(),
            ..Default::default()
        });
        project_grammar_calls(&mut messages, &wire_tools.grammar_inputs);
        let tools = std::mem::take(&mut wire_tools.declarations);
        let declared_mistral_tools: Vec<String> = if mistral {
            tools
                .iter()
                .filter_map(|tool| tool["function"]["name"].as_str().map(str::to_owned))
                .collect()
        } else {
            Vec::new()
        };
        let wire_model = self
            .deployments
            .get(&request.model)
            .unwrap_or(&request.model);
        let mut body = json!({"model":wire_model,"messages":messages,"stream":true});
        if mistral {
            body["max_tokens"] = json!(request.max_output_tokens);
        } else {
            self.compatibility.request_options(
                &mut body,
                request.max_output_tokens,
                !tools.is_empty(),
            );
        }
        if self.prompt_cache_key_enabled
            && let Some(key) = &self.session_cache_key
        {
            body["prompt_cache_key"] = json!(key);
        }
        if self.prompt_cache_long {
            body["prompt_cache_retention"] = json!("24h");
        }
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        if let Some(options) = &self.mistral {
            if options.prompt_mode {
                body["prompt_mode"] = json!("reasoning");
            }
            if let Some(effort) = &options.effort {
                body["reasoning_effort"] = json!(effort);
            }
        } else {
            let thinking_budget = self.compatibility.thinking_budget_options(
                &mut body,
                self.thinking_budget_level.as_deref(),
                self.thinking_budgets.as_ref(),
                request.max_output_tokens,
                self.supports_reasoning,
            )?;
            self.compatibility.reasoning_options(
                &mut body,
                self.supports_reasoning,
                self.reasoning_enabled,
                self.chat_reasoning_effort(),
                self.allow_reasoning_off,
                thinking_budget,
            );
        }
        body.as_object_mut().expect("chat request object").extend(
            self.sampling
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        if let Some(control) = self.cache_retention.anthropic_control() {
            apply_anthropic_cache(&mut body, &control);
        }
        let error_secrets = pi_rs_core::provider_error_secrets(
            self.token.as_deref(),
            self.headers.iter().filter_map(|(name, value)| {
                value.to_str().ok().map(|value| (name.as_str(), value))
            }),
        );
        let mut request = self.client.post(self.endpoint.clone());
        if let Some(headers) = copilot {
            for (name, value) in headers {
                if !self.headers.contains_key(name) {
                    request = request.header(name, value);
                }
            }
        }
        let request = if mistral && !self.headers.contains_key(reqwest::header::ACCEPT) {
            request.header(reqwest::header::ACCEPT, "text/event-stream")
        } else {
            request
        };
        let retry = if mistral {
            pi_rs_core::ProviderRetry::default()
        } else {
            self.provider_retry
        };
        let builder = self.authenticate(request).json(&body);
        let monitor_loading = self.progress.is_some()
            && !requested_model.ends_with(":cloud")
            && self.endpoint.port_or_known_default() == Some(11434)
            && matches!(
                self.endpoint.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]")
            );
        let loading =
            tokio::sync::Mutex::new((tokio::time::interval(Duration::from_secs(1)), last_progress));
        let loading_state = &loading;
        let model = &requested_model;
        let response = retry.send(
            || {
                let builder = builder.try_clone().ok_or_else(|| {
                    Error::Config("Chat Completions request cannot be replayed".into())
                });
                async move {
                let response = builder?.send();
                tokio::pin!(response);
                let mut loading = loading_state.lock().await;
                let (poll, last_progress) = &mut *loading;
                loop {
                    tokio::select! {
                        result = &mut response => break result.map_err(|error| Error::RetryableModel(if error.is_timeout() && !error.is_connect() { "HTTP idle timeout" } else { "HTTP connection failed" }.into())),
                        _ = poll.tick(), if monitor_loading => {
                            if let Some(loaded) = self.model_loaded(model).await {
                                report_progress(&self.progress, last_progress, if loaded {
                                    ModelProgress::WaitingForModel
                                } else {
                                    ModelProgress::LoadingModel
                                });
                            }
                        }
                    }
                }
                }
            },
            |response| (response.status().as_u16(), response.headers()),
        ).await?;
        let (mut poll, mut last_progress) = loading.into_inner();
        pi_rs_core::observe_provider_response(
            response.status(),
            response.headers(),
            &error_secrets,
            &self.headers,
        )
        .await;
        if !response.status().is_success() {
            let status = response.status();
            let description = format!("HTTP status {status}");
            if status.as_u16() == 413 {
                return Err(Error::Model("413 request too large".into()));
            }
            if matches!(status.as_u16(), 400 | 422) {
                let overflow = pi_rs_core::provider_context_overflow_stream_or_empty(
                    response.bytes_stream(),
                    &error_secrets,
                    self.provider == "cerebras" && status.as_u16() == 400,
                )
                .await;
                return Err(Error::Model(if overflow {
                    "context window exceeded".into()
                } else {
                    description
                }));
            }
            let permanent_quota = (matches!(status.as_u16(), 408 | 429)
                || status.is_server_error())
                && pi_rs_core::permanent_provider_limit(response.bytes_stream(), &error_secrets)
                    .await;
            return Err(if permanent_quota {
                Error::Model("Chat Completions account quota or billing limit reached".into())
            } else if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
                Error::RetryableModel(description)
            } else {
                Error::Model(description)
            });
        }
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected an SSE response".into()));
        }

        let progress = self.progress.clone();
        let reasoning = self.reasoning.clone();
        let loading_probe = self.clone();
        let observe_stream = pi_rs_core::observes_provider_stream();
        Ok(Box::pin(async_stream::try_stream! {
            let mut bytes = 0usize;
            let mut events = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|error| Error::RetryableModel(if error.is_timeout() && !error.is_connect() { "HTTP idle timeout" } else { "broken HTTP stream" }.into()))?;
                bytes = bytes.saturating_add(chunk.len());
                if bytes > 4 * 1024 * 1024 {
                    return Err(Error::Model("model stream exceeds size limit".into()));
                }
                Ok(chunk)
            }).eventsource();
            let mut calls = BTreeMap::<usize, CallDelta>::new();
            let mut usage = None;
            let mut reported_cached_input = None;
            let mut metadata = ModelMetadata {
                provider_api: Some(if mistral { ModelApi::MistralConversations } else { ModelApi::OpenaiCompletions }),
                replay_identity: Some(identity.clone()),
                ..ModelMetadata::default()
            };
            let mut reasoning_text = String::new();
            let mut replay_blocks = Vec::new();
            let mut mistral_segments = Vec::new();
            let mut mistral_after_tool = false;
            loop {
                let event = tokio::select! {
                    event = events.next() => event,
                    _ = poll.tick(), if monitor_loading && matches!(last_progress, Some(ModelProgress::WaitingForModel | ModelProgress::LoadingModel)) => {
                        if let Some(loaded) = loading_probe.model_loaded(&requested_model).await {
                            report_progress(&progress, &mut last_progress, if loaded {
                                ModelProgress::WaitingForModel
                            } else {
                                ModelProgress::LoadingModel
                            });
                        }
                        continue;
                    }
                };
                let Some(event) = event else {
                    if mistral && metadata.finish_reason.is_some() {
                        break;
                    }
                    Err(Error::RetryableModel("SSE closed before the end marker".into()))?;
                    unreachable!();
                };
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) => error,
                    _ => Error::RetryableModel("broken SSE stream".into()),
                })?;
                if event.data == "[DONE]" {
                    break;
                }
                if observe_stream && let Ok(raw) = serde_json::from_str::<Value>(&event.data) {
                    pi_rs_core::observe_provider_stream(
                        &loading_probe.provider, ModelApi::OpenaiCompletions, &requested_model,
                        &raw, &error_secrets, &loading_probe.headers,
                    ).await;
                }
                let chunk: Chunk = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("malformed SSE chunk".into()))?;
                if let Some(error) = &chunk.error {
                    Err(if pi_rs_core::permanent_provider_error(error, &error_secrets) {
                        Error::Model("Chat Completions account quota or billing limit reached".into())
                    } else if pi_rs_core::retryable_provider_error(error, &error_secrets) {
                        Error::RetryableModel("provider returned a transient stream error".into())
                    } else if pi_rs_core::provider_context_overflow(error, &error_secrets) {
                        Error::Model("context window exceeded".into())
                    } else {
                        Error::Model("provider returned a stream error".into())
                    })?;
                }
                let previous = metadata.clone();
                if let Some(id) = chunk.id
                    && (!mistral || (metadata.provider_request_id.is_none() && !id.is_empty()))
                    && id.len() <= 128 && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                {
                    metadata.provider_request_id = Some(id);
                }
                if let Some(stats) = chunk.usage {
                    let previous: Usage = usage.unwrap_or_default();
                    let input_tokens = stats.prompt_tokens.unwrap_or(previous.input_tokens);
                    usage = Some(Usage { input_tokens, output_tokens: stats.completion_tokens.unwrap_or(previous.output_tokens) });
                    let details = stats.prompt_tokens_details.unwrap_or_default();
                    let cached = details.cached_tokens
                        .or(stats.prompt_cache_hit_tokens)
                        .or(stats.cached_tokens);
                    let cached = if mistral {
                        cached.or(stats.mistral_cached_tokens)
                            .or_else(|| stats.mistral_prompt_details.and_then(|details| details.cached_tokens))
                    } else {
                        cached
                    };
                    reported_cached_input = cached.or(reported_cached_input);
                    metadata.cached_input_tokens = reported_cached_input
                        .map(|cached| if mistral { cached.min(input_tokens) } else { cached });
                    metadata.cache_creation_input_tokens = details.cache_write_tokens
                        .or(metadata.cache_creation_input_tokens);
                    let nested = stats.completion_tokens_details.and_then(|details| details.reasoning_tokens);
                    metadata.reasoning_output_tokens = nested.filter(|count| *count != 0)
                        .or(stats.reasoning_tokens).or(nested).or(metadata.reasoning_output_tokens);
                    if let Some(cost) = stats.accounting.total() {
                        metadata.cost_usd = Some(cost);
                        metadata.cost_from_provider = Some(true);
                    }
                }
                for choice in chunk.choices {
                    if choice.index != 0 || metadata.finish_reason.is_some() {
                        Err(Error::Model("unexpected choice in model stream".into()))?;
                    }
                    if let Some(text) = [
                        choice.delta.reasoning,
                        choice.delta.reasoning_content,
                        choice.delta.reasoning_text,
                    ].into_iter().flatten().find(|text| !text.is_empty()) {
                        report_progress(&progress, &mut last_progress, ModelProgress::Thinking);
                        reasoning_text.push_str(&text);
                        if let Some(observer) = &reasoning { observer(&text); }
                        yield ModelEvent::Reasoning(text);
                    }
                    if preserve_details
                        && let Some(Value::Array(details)) = choice.delta.reasoning_details
                    {
                        for detail in details {
                            reasoning_details::append(&mut replay_blocks, detail);
                        }
                    }
                    if mistral {
                        for segment in mistral::streamed_segments(choice.delta.content) {
                            let event = match &segment {
                                mistral::Segment::Text(text) => {
                                    report_progress(&progress, &mut last_progress, ModelProgress::Generating);
                                    ModelEvent::Text(text.clone())
                                }
                                mistral::Segment::Thinking(text) => {
                                    report_progress(&progress, &mut last_progress, ModelProgress::Thinking);
                                    reasoning_text.push_str(text);
                                    if let Some(observer) = &reasoning { observer(text); }
                                    ModelEvent::Reasoning(text.clone())
                                }
                            };
                            mistral::record(&mut mistral_segments, segment, std::mem::take(&mut mistral_after_tool));
                            yield event;
                        }
                    } else if let Some(content) = choice.delta.content {
                        let Value::String(content) = content else {
                            Err(Error::Model("malformed SSE content delta".into()))?;
                            unreachable!();
                        };
                        if !content.is_empty() {
                            report_progress(&progress, &mut last_progress, ModelProgress::Generating);
                            yield ModelEvent::Text(content);
                        }
                    }
                    let had_calls = !calls.is_empty();
                    for (position, delta) in choice.delta.tool_calls.unwrap_or_default().into_iter().enumerate() {
                        report_progress(&progress, &mut last_progress, ModelProgress::PreparingTool);
                        let index = if let Some(index) = delta.index {
                            index
                        } else if mistral {
                            mistral::stream_call_index(
                                delta.id.as_deref(), delta.function.as_ref(), position, had_calls, &calls,
                                &declared_mistral_tools,
                            )?
                        } else {
                            Err(Error::Model("missing tool call index".into()))?;
                            unreachable!();
                        };
                        if index >= 64 {
                            Err(Error::Model("invalid tool call index".into()))?;
                        }
                        if !mistral {
                            let call = calls.entry(index).or_default();
                            yield ModelEvent::ToolCallDelta(call.append(index, delta, &wire_tools.grammar_inputs)?);
                            continue;
                        }
                        let (mut name, arguments) = delta.function
                            .map(|function| (function.name.unwrap_or_default(), function.arguments))
                            .unwrap_or_default();
                        let arguments = match arguments {
                            Some(Value::String(arguments)) => arguments,
                            Some(value) if mistral => value.to_string(),
                            Some(_) => Err(Error::Model("invalid tool call arguments".into()))?,
                            None => String::new(),
                        };
                        let mut id = delta.id.unwrap_or_default();
                        let call = calls.entry(index).or_default();
                        if mistral {
                            mistral_after_tool = true;
                            if id == "null" {
                                id.clear();
                            }
                            if mistral::repeats_declared_name(&declared_mistral_tools, &call.name, &name) {
                                name.clear();
                            }
                            if !call.id.is_empty() {
                                if id == call.id || delta.index.is_none() {
                                    id.clear();
                                } else if let Some(suffix) = id.strip_prefix(&call.id) {
                                    id = suffix.to_owned();
                                }
                            }
                        }
                        call.id.push_str(&id);
                        call.name.push_str(&name);
                        call.arguments.push_str(&arguments);
                        yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                            index: index as u64, id, name, arguments,
                        });
                    }
                    if mistral && choice.finish_reason.as_deref() == Some("error") {
                        Err(Error::RetryableModel("Mistral reported a server error".into()))?;
                    }
                    metadata.finish_reason = choice.finish_reason.filter(|reason| {
                        !infer_finish || matches!(reason.as_str(), "length" | "content_filter")
                    }).map(|reason| {
                        match reason.as_str() {
                            "stop" | "tool_calls" | "length" | "content_filter" => reason,
                            "end" => "stop".into(),
                            "model_length" if mistral => "length".into(),
                            "function_call" => "tool_calls".into(),
                            _ => "other".into(),
                        }
                    });
                }
                if metadata != previous {
                    yield ModelEvent::Metadata(metadata.clone());
                }
            }
            if infer_finish && !matches!(metadata.finish_reason.as_deref(), Some("length" | "content_filter")) {
                metadata.finish_reason = Some(if calls.is_empty() { "stop" } else { "tool_calls" }.into());
                yield ModelEvent::Metadata(metadata.clone());
            }
            match metadata.finish_reason.as_deref() {
                Some("stop" | "length") if calls.is_empty() => {},
                Some("tool_calls") if !calls.is_empty() => {},
                _ => Err(Error::Model("model response did not finish successfully".into()))?,
            }
            if !mistral_segments.is_empty() {
                replay_blocks.push(mistral::replay_block(mistral_segments));
            }
            if mistral {
                for (index, call) in &mut calls {
                    if call.id.is_empty() {
                        call.id = mistral::call_id(&format!("toolcall:{index}"), 0);
                        yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                            index: *index as u64, id: call.id.clone(), name: String::new(), arguments: String::new(),
                        });
                    }
                }
            }
            if !mistral {
                for (index, call) in &mut calls {
                    let arguments = call.finish(&wire_tools.grammar_inputs)?;
                    if !arguments.is_empty() {
                        yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta {
                            index: *index as u64, id: String::new(), name: String::new(), arguments,
                        });
                    }
                }
            }
            let calls = calls.into_values().map(|call| ToolCall {
                id: call.id, name: call.name, arguments: if mistral && call.arguments.is_empty() { "{}".into() } else { call.arguments },
            }).collect();
            if preserve_reasoning && !reasoning_text.is_empty() {
                replay_blocks.push(json!({"type":"reasoning_content","text":reasoning_text}));
            }
            if !replay_blocks.is_empty() {
                metadata.replay = Some(Box::new(ProviderReplay { identity, blocks: replay_blocks }));
            }
            if !reasoning_text.is_empty() {
                metadata.reasoning = Some(reasoning_text);
            }
            if metadata.reasoning.is_some() || metadata.replay.is_some() || metadata.replay_identity.is_some() {
                yield ModelEvent::Metadata(metadata);
            }
            yield ModelEvent::Complete { calls, usage };
        }))
    }
}

fn reject_stream_only_strict(effective: &[pi_rs_core::EffectiveToolDeclaration]) -> Result<()> {
    if effective.iter().any(|tool| {
        matches!(
            tool.sampling,
            pi_rs_core::EffectiveToolSampling::StrictJsonSchema { .. }
        )
    }) {
        return Err(Error::Config("strict tool sampling requires prepared dispatch so its effective decisions can be retained".into()));
    }
    Ok(())
}

fn resolved_constrained_tools(
    request: &ModelRequest,
    declarations: &[pi_rs_core::ToolDeclaration],
    supports_strict: bool,
    supports_grammar: bool,
) -> Result<(ChatWireTools, Vec<pi_rs_core::EffectiveToolDeclaration>)> {
    let mut names = std::collections::BTreeSet::new();
    if request.tools.len() != declarations.len()
        || request
            .tools
            .iter()
            .zip(declarations)
            .any(|(spec, declaration)| spec != &declaration.spec || !names.insert(&spec.name))
    {
        return Err(Error::Config(
            "tool declarations do not match the effective request".into(),
        ));
    }
    let mut wire = ChatWireTools {
        declarations: Vec::with_capacity(declarations.len()),
        ..Default::default()
    };
    let mut effective = Vec::with_capacity(declarations.len());
    for declaration in declarations {
        let spec = &declaration.spec;
        if supports_grammar
            && let Some((syntax, definition, property)) =
                pi_rs_core::resolve_openai_tool_grammar(declaration)
                    .map_err(|error| Error::Config(error.to_string()))?
        {
            wire.declarations.push(json!({
                "type":"custom", "custom":{
                    "name":spec.name, "description":spec.description,
                    "format":{"type":"grammar","grammar":{"syntax":syntax,"definition":definition}}
                }
            }));
            wire.grammar_inputs
                .insert(spec.name.clone(), property.to_owned());
            effective.push(pi_rs_core::EffectiveToolDeclaration {
                spec: spec.clone(),
                sampling: pi_rs_core::EffectiveToolSampling::OpenAiGrammar {
                    syntax,
                    input_property: property.to_owned(),
                },
            });
            continue;
        }
        let strict = match &declaration.constrained_sampling {
            Some(pi_rs_core::ToolConstraint::JsonSchema { strict }) => {
                pi_rs_core::resolve_strict_tool_schema(
                    &spec.name,
                    &spec.parameters,
                    *strict,
                    supports_strict,
                    |_, _| false,
                )
                .map_err(|error| Error::Config(error.to_string()))?
            }
            _ => None,
        };
        let mut function = json!({"name":spec.name,"description":spec.description,"parameters":strict.as_ref().unwrap_or(&spec.parameters)});
        if supports_strict {
            function["strict"] = json!(strict.is_some());
        }
        let sampling = if strict.is_some() {
            pi_rs_core::EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters.clone(),
                normalization_version: pi_rs_core::STRICT_TOOL_NORMALIZATION_VERSION,
            }
        } else {
            pi_rs_core::EffectiveToolSampling::Unconstrained
        };
        wire.declarations
            .push(json!({"type":"function", "function":function}));
        effective.push(pi_rs_core::EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling,
        });
    }
    pi_rs_core::validate_effective_tool_declarations(request, declarations, &effective)
        .map_err(|error| Error::Config(error.to_string()))?;
    Ok((wire, effective))
}

fn project_grammar_calls(messages: &mut [Value], properties: &BTreeMap<String, String>) {
    if properties.is_empty() {
        return;
    }
    for message in messages {
        if let Some(calls) = message.get_mut("tool_calls").and_then(Value::as_array_mut) {
            for call in calls {
                let function = &call["function"];
                let Some(name) = function["name"].as_str() else {
                    continue;
                };
                let Some(property) = properties.get(name) else {
                    continue;
                };
                let Some(args) = function["arguments"].as_str() else {
                    continue;
                };
                if let Some(input) = pi_rs_core::lossless_openai_grammar_input(args, property) {
                    *call = json!({"id":call["id"],"type":"custom","custom":{"name":name,"input":input}});
                }
            }
        }
    }
}

fn apply_anthropic_cache(body: &mut Value, control: &Value) {
    if let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) {
        if let Some(instructions) = messages
            .iter_mut()
            .find(|message| matches!(message["role"].as_str(), Some("system" | "developer")))
        {
            mark_text_for_cache(instructions, control);
        }
        for message in messages.iter_mut().rev() {
            if matches!(
                message["role"].as_str(),
                Some("user" | "assistant" | "tool")
            ) && mark_text_for_cache(message, control)
            {
                break;
            }
        }
    }
    if let Some(tool) = body
        .get_mut("tools")
        .and_then(Value::as_array_mut)
        .and_then(|tools| tools.last_mut())
    {
        tool["cache_control"] = control.clone();
    }
}

fn mark_text_for_cache(message: &mut Value, control: &Value) -> bool {
    if let Some(text) = message["content"].as_str().filter(|text| !text.is_empty()) {
        message["content"] = json!([{"type":"text","text":text,"cache_control":control}]);
        return true;
    }
    let Some(text) = message
        .get_mut("content")
        .and_then(Value::as_array_mut)
        .and_then(|parts| parts.iter_mut().rev().find(|part| part["type"] == "text"))
    else {
        return false;
    };
    text["cache_control"] = control.clone();
    true
}

fn encode_message(message: Message) -> Vec<Value> {
    match message {
        Message::System { content } => vec![json!({"role": "system", "content": content})],
        Message::User { content } => vec![json!({"role": "user", "content": content})],
        Message::UserWithImages { content, images } => {
            let mut blocks = vec![json!({"type":"text","text":content})];
            blocks.extend(images.into_iter().map(|image| json!({"type":"image_url","image_url":{"url":format!("data:{};base64,{}", image.mime_type, image.data)}})));
            vec![json!({"role":"user","content":blocks})]
        }
        Message::Tool {
            call_id,
            content,
            is_structured,
            ..
        } => encode_tool_result(&call_id, content, is_structured),
        Message::Assistant { content, calls, .. } => {
            let mut message = json!({"role": "assistant", "content": content});
            if !calls.is_empty() {
                message["tool_calls"] = Value::Array(
                    calls
                        .into_iter()
                        .map(|call| {
                            json!({
                                "id": call.id, "type": "function",
                                "function": {"name": call.name, "arguments": call.arguments}
                            })
                        })
                        .collect(),
                );
            }
            vec![message]
        }
    }
}

fn encode_tool_result(call_id: &str, content: String, is_structured: Option<bool>) -> Vec<Value> {
    let mut messages = Vec::new();
    let (content, images) = if pi_rs_core::tool_content_is_structured(&content, is_structured) {
        pi_rs_core::extract_tool_images(content)
    } else {
        (content, Vec::new())
    };
    let mut images: Vec<_> = images
        .into_iter()
        .map(|image| {
            let url = format!("data:{};base64,{}", image.mime_type, image.data);
            json!({"type":"image_url","image_url":{"url":url}})
        })
        .collect();
    messages.push(json!({"role":"tool","tool_call_id":call_id,"content":content}));
    if !images.is_empty() {
        images.insert(
            0,
            json!({"type":"text","text":"Images from the preceding tool result."}),
        );
        messages.push(json!({"role":"user","content":images}));
    }
    messages
}

#[derive(Deserialize)]
struct Chunk {
    id: Option<String>,
    #[serde(default)]
    choices: Vec<Choice>,
    error: Option<Value>,
    usage: Option<WireUsage>,
}

#[derive(Deserialize)]
struct WireUsage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    completion_tokens_details: Option<CompletionTokenDetails>,
    reasoning_tokens: Option<u64>,
    prompt_cache_hit_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    #[serde(alias = "promptTokensDetails")]
    prompt_tokens_details: Option<PromptTokenDetails>,
    #[serde(rename = "num_cached_tokens", alias = "numCachedTokens")]
    mistral_cached_tokens: Option<u64>,
    #[serde(rename = "prompt_token_details", alias = "promptTokenDetails")]
    mistral_prompt_details: Option<PromptTokenDetails>,
    #[serde(flatten)]
    accounting: ReportedUsageCost,
}

#[derive(Deserialize)]
struct CompletionTokenDetails {
    reasoning_tokens: Option<u64>,
}

#[derive(Default, Deserialize)]
struct PromptTokenDetails {
    #[serde(alias = "cachedTokens")]
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct Choice {
    index: u32,
    delta: Delta,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<Value>,
    reasoning: Option<String>,
    reasoning_content: Option<String>,
    reasoning_text: Option<String>,
    reasoning_details: Option<Value>,
    tool_calls: Option<Vec<WireCall>>,
}

#[derive(Deserialize)]
struct WireCall {
    index: Option<usize>,
    id: Option<String>,
    function: Option<WireFunction>,
    custom: Option<WireCustom>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Default, Deserialize)]
struct WireCustom {
    name: Option<String>,
    input: Option<String>,
}

#[derive(Deserialize)]
struct WireFunction {
    name: Option<String>,
    arguments: Option<Value>,
}

#[derive(Default)]
struct CallDelta {
    id: String,
    name: String,
    arguments: String,
    custom: Option<CustomInput>,
    function: bool,
}

#[derive(Default)]
struct CustomInput {
    pending: String,
    started: bool,
}

impl CustomInput {
    fn flush(&mut self, property: &str) -> String {
        let input = json!(std::mem::take(&mut self.pending)).to_string();
        let encoded = &input[1..input.len() - 1];
        if self.started {
            encoded.to_owned()
        } else {
            self.started = true;
            format!("{{{}:\"{encoded}", json!(property))
        }
    }
}

impl CallDelta {
    fn append(
        &mut self,
        index: usize,
        delta: WireCall,
        properties: &BTreeMap<String, String>,
    ) -> Result<pi_rs_core::ToolCallDelta> {
        let id = delta.id.unwrap_or_default();
        self.id.push_str(&id);
        let (name, arguments) = if delta.custom.is_some() || delta.kind.as_deref() == Some("custom")
        {
            if self.function || delta.function.is_some() {
                return Err(Error::Model("tool call changed kind during stream".into()));
            }
            let custom = self.custom.get_or_insert_with(CustomInput::default);
            let delta = delta.custom.unwrap_or_default();
            let name = delta.name.unwrap_or_default();
            if custom.started && !name.is_empty() {
                return Err(Error::Model(
                    "grammar tool name changed during stream".into(),
                ));
            }
            self.name.push_str(&name);
            if let Some(input) = delta.input {
                custom.pending.push_str(&input);
            }
            // A shorter advertised tool name may be the first fragment of a longer name.
            let arguments = if let Some(property) = properties.get(&self.name)
                && !custom.pending.is_empty()
                && (custom.started
                    || !properties.keys().any(|other| {
                        other.len() > self.name.len() && other.starts_with(&self.name)
                    })) {
                custom.flush(property)
            } else {
                String::new()
            };
            (name, arguments)
        } else if let Some(function) = delta.function {
            if self.custom.is_some() {
                return Err(Error::Model("tool call changed kind during stream".into()));
            }
            self.function = true;
            let name = function.name.unwrap_or_default();
            self.name.push_str(&name);
            let arguments = match function.arguments {
                Some(Value::String(arguments)) => arguments,
                Some(_) => return Err(Error::Model("invalid tool call arguments".into())),
                None => String::new(),
            };
            (name, arguments)
        } else {
            (String::new(), String::new())
        };
        self.arguments.push_str(&arguments);
        Ok(pi_rs_core::ToolCallDelta {
            index: index as u64,
            id,
            name,
            arguments,
        })
    }

    fn finish(&mut self, properties: &BTreeMap<String, String>) -> Result<String> {
        match (self.custom.as_mut(), properties.get(&self.name)) {
            (Some(custom), Some(property)) => {
                let mut tail = custom.flush(property);
                tail.push_str("\"}");
                self.arguments.push_str(&tail);
                Ok(tail)
            }
            (Some(_), None) => Err(Error::Model("unadvertised OpenAI grammar tool".into())),
            (None, Some(_)) => Err(Error::Model(
                "OpenAI grammar tool returned as a function".into(),
            )),
            (None, None) => Ok(String::new()),
        }
    }
}

#[cfg(test)]
mod content_tests {
    #[tokio::test]
    async fn thinking_as_text_replays_streamed_reasoning() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 8192];
            let _ = socket.read(&mut request).unwrap();
            let body = format!(
                "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"index":0,"delta":{"reasoning_content":"internal"}}],
                    "usage":{"prompt_tokens":12,"reasoning_tokens":2,
                        "completion_tokens_details":{"reasoning_tokens":4}}}),
                json!({"choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}],
                    "usage":{"completion_tokens":9}})
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        });
        let model = Ollama::new(&format!("http://{address}"), None)
            .unwrap()
            .with_chat_compatibility(
                serde_json::from_value(json!({"requiresThinkingAsText":true})).unwrap(),
                Some(true),
                true,
                true,
            );
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 16,
        };
        let events = model
            .stream(request.clone())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(events.iter().any(|event| matches!(
            event,
            Ok(ModelEvent::Complete {
                usage: Some(Usage {
                    input_tokens: 12,
                    output_tokens: 9
                }),
                ..
            })
        )));
        let replay = events
            .into_iter()
            .rev()
            .find_map(|event| match event.unwrap() {
                ModelEvent::Metadata(metadata) => {
                    assert_eq!(metadata.reasoning_output_tokens, Some(4));
                    metadata.replay
                }
                _ => None,
            })
            .expect("reasoning replay");
        let message = Message::Assistant {
            content: "answer".into(),
            calls: vec![ToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
            replay: Some(replay.clone()),
        };
        let encoded = model.compatible_message(message, &model.replay_identity("m"));
        assert_eq!(
            encoded[0]["content"],
            json!([{"type":"text","text":"internal"},{"type":"text","text":"answer"}])
        );
        assert_eq!(encoded[0]["tool_calls"][0]["id"], "call");
        let thinking_only = Message::Assistant {
            content: String::new(),
            calls: vec![],
            replay: Some(replay),
        };
        assert_eq!(
            model.compatible_message(thinking_only.clone(), &model.replay_identity("m"))[0]["content"],
            json!([{"type":"text","text":"internal"}])
        );
        let mut long = thinking_only.clone();
        if let Message::Assistant {
            replay: Some(replay),
            ..
        } = &mut long
        {
            replay.blocks[0]["text"] = json!("x".repeat(5000));
        }
        assert!(model.estimate_message_tokens(&request, &long) > 1000);
        let no_reasoning = model.clone().with_chat_compatibility(
            serde_json::from_value(json!({"requiresThinkingAsText":true})).unwrap(),
            Some(false),
            true,
            true,
        );
        assert_eq!(
            no_reasoning.estimate_message_tokens(&request, &long),
            pi_rs_core::estimate_message_tokens(&long)
        );
        assert!(
            model
                .compatible_message(thinking_only, &model.replay_identity("other"))
                .is_empty()
        );
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn configured_thinking_budget_is_sent_on_wire() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let (offset, length) = loop {
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0 && bytes.len() + count < 65536);
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..pos]).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    break (pos + 4, length);
                }
            };
            while bytes.len() - offset < length {
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0 && bytes.len() + count < 65536);
                bytes.extend_from_slice(&chunk[..count]);
            }
            let body: serde_json::Value =
                serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
            assert_eq!(body["thinking_budget_tokens"], 2500);
            assert_eq!(body["chat_template_args"]["budget"], 2500);
            assert_eq!(body["reasoning_effort"], "high");
            let reply = "data: [DONE]\n\n";
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", reply.len(), reply).unwrap();
        });
        let compat = serde_json::from_value(json!({
            "thinkingTokenBudgetField":"thinking_budget_tokens",
            "thinkingFormat":"baseten",
            "chatTemplateArgs":{"budget":{"$var":"thinking.budget"}},
        }))
        .unwrap();
        let model = Ollama::new(&format!("http://{address}"), None)
            .unwrap()
            .with_chat_compatibility(compat, Some(true), true, true)
            .with_mapped_reasoning_effort(Some("high"))
            .with_thinking_budgets(Some("low"), Some(json!({"low":2500, "high":7000})));
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 4096,
        };
        let _stream = model.stream(request).await.unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn chat_template_uses_only_explicitly_mapped_off_effort() {
        let compat = serde_json::from_value(json!({"thinkingFormat":"chat-template"})).unwrap();
        let model = Ollama::new("http://127.0.0.1:11434", None)
            .unwrap()
            .with_chat_compatibility(compat, Some(true), false, true);
        let selected = model.clone().with_reasoning_effort(Some("minimal"));
        assert_eq!(selected.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(selected.chat_reasoning_effort(), Some("minimal"));
        assert_eq!(
            model
                .clone()
                .with_reasoning_effort(Some("off"))
                .chat_reasoning_effort(),
            None
        );
        assert_eq!(
            model
                .clone()
                .with_reported_reasoning_effort(Some("off"))
                .chat_reasoning_effort(),
            None
        );
        assert_eq!(
            model
                .with_mapped_reasoning_effort(Some("none"))
                .chat_reasoning_effort(),
            Some("none")
        );
    }

    #[test]
    fn chat_system_messages_follow_model_capability_in_wire_and_estimates() {
        let messages = vec![
            Message::System {
                content: "Initial instructions".into(),
            },
            Message::User {
                content: "First question".into(),
            },
            Message::System {
                content: "Additional instructions".into(),
            },
            Message::System {
                content: String::new(),
            },
            Message::User {
                content: "Second question".into(),
            },
        ];
        let folded = vec![
            Message::System {
                content: "Initial instructions\n\nAdditional instructions".into(),
            },
            messages[1].clone(),
            messages[4].clone(),
        ];
        for preserves in [false, true] {
            let provider: ChatCompatibility =
                serde_json::from_value(json!({"supportsMidConvoSystemMessages":!preserves}))
                    .unwrap();
            let override_value =
                serde_json::from_value(json!({"supportsMidConvoSystemMessages":preserves}))
                    .unwrap();
            let model = Ollama::new("http://127.0.0.1:1/v1/chat/completions", None)
                .unwrap()
                .with_chat_compatibility(provider.overlay(override_value), Some(true), false, true);
            let actual = model.wire_messages(messages.clone(), &model.replay_identity("fixture"));
            let expected = if preserves {
                vec![
                    messages[0].clone(),
                    messages[1].clone(),
                    messages[2].clone(),
                    messages[4].clone(),
                ]
            } else {
                folded.clone()
            };
            let expected_wire: Vec<_> = expected.iter().cloned().flat_map(encode_message).collect();
            assert_eq!(actual, expected_wire);
            let request = ModelRequest {
                model: "fixture".into(),
                messages: messages.clone(),
                tools: vec![],
                max_output_tokens: 1024,
            };
            assert_eq!(
                model.estimate_request_tokens(&request),
                pi_rs_core::estimate_request_tokens(&ModelRequest {
                    messages: expected,
                    ..request
                }),
            );
        }
    }

    #[test]
    fn mistral_system_messages_keep_their_conversation_positions() {
        let model = Ollama::new("http://127.0.0.1:1/v1/chat/completions", None)
            .unwrap()
            .with_mistral(false, false, None);
        let request = ModelRequest {
            model: "mistral".into(),
            messages: vec![
                Message::System {
                    content: "Initial".into(),
                },
                Message::User {
                    content: "Question".into(),
                },
                Message::System {
                    content: "Update".into(),
                },
            ],
            tools: vec![],
            max_output_tokens: 1024,
        };
        let messages = mistral::messages(
            request.messages.clone(),
            &model.replay_identity(&request.model),
            false,
        )
        .unwrap();
        assert_eq!(messages[2], json!({"role":"system","content":"Update"}));
        assert_eq!(
            model.estimate_request_tokens(&request),
            pi_rs_core::estimate_request_tokens(&request)
        );
    }

    #[tokio::test]
    async fn auth_header_error_literals_are_not_classified() {
        use std::io::{Read, Write};
        for (name, value) in [("x-api-key", "quota exceeded"), ("x-auth-token", "timeout")] {
            for streaming in [false, true] {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let event = json!({"error":{"code":"error","message":value}});
                let worker = std::thread::spawn(move || {
                    let (mut socket, _) = listener.accept().unwrap();
                    socket
                        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut chunk = [0; 2048];
                    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        let count = socket.read(&mut chunk).unwrap();
                        assert!(
                            count != 0 && request.len() + count <= 32 * 1024,
                            "incomplete request headers"
                        );
                        request.extend_from_slice(&chunk[..count]);
                    }
                    let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
                    assert!(
                        request.contains(&format!("{name}: {value}")),
                        "header missing from fixture request"
                    );
                    let body = if streaming {
                        format!("data: {event}\n\n")
                    } else {
                        json!({"error":{"message":value}}).to_string()
                    };
                    let status = if streaming { "200 OK" } else { "503 Fixture" };
                    let content_type = if streaming {
                        "text/event-stream"
                    } else {
                        "application/json"
                    };
                    write!(socket, "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                });
                let model = Ollama::new(&format!("http://{address}"), Some("ordinary-key".into()))
                    .unwrap()
                    .with_headers({
                        let mut headers = reqwest::header::HeaderMap::new();
                        headers.insert(name, value.parse().unwrap());
                        headers
                    });
                let request = ModelRequest {
                    model: "m".into(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 16,
                };
                let error = match model.stream(request).await {
                    Err(error) => error,
                    Ok(mut stream) => loop {
                        match stream.next().await {
                            Some(Err(error)) => break error,
                            Some(Ok(_)) => {}
                            None => panic!("missing stream error"),
                        }
                    },
                };
                assert_eq!(
                    matches!(error, Error::RetryableModel(_)),
                    !streaming,
                    "{name} {streaming}"
                );
                assert!(!error.to_string().contains(value));
                worker.join().unwrap();
            }
        }
    }

    #[tokio::test]
    async fn quota_stream_error_ignores_metadata_and_redacts() {
        use std::io::{Read, Write};
        for (message, permanent, retryable) in [
            ("quota exceeded sk-billing-timeout-secret", true, false),
            ("timeout sk-billing-timeout-secret", false, true),
            ("sk-billing-timeout-secret", false, false),
            ("sk-timeout-secret", false, false),
            ("service-is-unavailable", false, true),
            ("sk-header-billing-timeout-secret", false, false),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let event = json!({"error":{"code":"error","message":message,"request_id":"billing-sentinel-secret"}});
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let body = format!("data: {event}\n\n");
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = Ollama::new(
                &format!("http://{address}"),
                Some("sk-billing-timeout-secret".into()),
            )
            .unwrap()
            .with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-routing", "timeout".parse().unwrap());
                headers.insert("x-secret", "sk-timeout-secret".parse().unwrap());
                headers.insert(
                    "x-bearer",
                    "Bearer  sk-header-billing-timeout-secret".parse().unwrap(),
                );
                headers.insert("x-service", "service-is-unavailable".parse().unwrap());
                headers
            });
            let request = ModelRequest {
                model: "m".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 16,
            };
            let mut stream = match model.stream(request).await {
                Ok(stream) => stream,
                Err(error) => panic!("{error}"),
            };
            let error = loop {
                match stream.next().await {
                    Some(Err(error)) => break error,
                    Some(Ok(_)) => {}
                    None => panic!("stream ended without an error"),
                }
            };
            assert_eq!(
                error.to_string().contains("quota or billing limit"),
                permanent
            );
            assert_eq!(matches!(error, Error::RetryableModel(_)), retryable);
            assert!(!error.to_string().contains("sentinel-secret"));
            assert!(!error.to_string().contains("sk-billing-timeout-secret"));
            assert!(!error.to_string().contains("sk-timeout-secret"));
            assert!(
                !error
                    .to_string()
                    .contains("sk-header-billing-timeout-secret")
            );
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn quota_429_is_permanent_and_transient_is_retryable() {
        use std::io::{Read, Write};
        for (status, body, permanent) in [
            (
                503,
                r#"{"error":{"message":"sk-header-billing-timeout-secret"}}"#,
                false,
            ),
            (503, r#"{"error":{"message":"sk-timeout-secret"}}"#, false),
            (
                503,
                r#"{"error":{"message":"sk-billing-timeout-secret"}}"#,
                false,
            ),
            (
                429,
                r#"{"error":{"code":"insufficient_quota","secret":"sentinel-secret"}}"#,
                true,
            ),
            (
                503,
                r#"{"error":{"message":"quota exceeded","secret":"sentinel-secret"}}"#,
                true,
            ),
            (
                503,
                r#"{"error":{"message":"quota exceeded sk-billing-timeout-secret"}}"#,
                true,
            ),
            (
                429,
                r#"{"error":{"message":"rate limit exceeded"},"request_id":"billing-sentinel-secret"}"#,
                false,
            ),
            (
                503,
                r#"{"error":{"message":"temporarily unavailable"},"request_id":"billing-sentinel-secret"}"#,
                false,
            ),
            (408, r#"{"error":{"message":"quota exceeded"}}"#, true),
            (
                429,
                r#"{"error":{"message":"rate limit","request_id":"billing-sentinel-secret"}}"#,
                false,
            ),
            (
                429,
                r#"{"error":{"message":"rate limit sk-billing-timeout-secret"}}"#,
                false,
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let credential = if body.contains("sk-timeout-secret") {
                "sk-timeout-secret"
            } else {
                "sk-billing-timeout-secret"
            };
            let model = Ollama::new(&format!("http://{address}"), Some(credential.into())).unwrap();
            let model = model.with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "x-bearer",
                    "Bearer  sk-header-billing-timeout-secret".parse().unwrap(),
                );
                headers.insert("x-service", "service-is-unavailable".parse().unwrap());
                headers
            });
            let result = model
                .stream(ModelRequest {
                    model: "m".into(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 16,
                })
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("expected error"),
            };
            assert_eq!(matches!(error, Error::Model(_)), permanent);
            assert_eq!(matches!(error, Error::RetryableModel(_)), !permanent);
            if permanent {
                assert!(error.to_string().contains("quota or billing limit"));
            }
            assert!(!error.to_string().contains("sentinel-secret"));
            assert!(
                !error
                    .to_string()
                    .contains("sk-header-billing-timeout-secret")
            );
            assert!(!error.to_string().contains(credential));
            worker.join().unwrap();
        }
    }

    use super::*;

    #[test]
    fn chat_prompt_cache_policy_is_part_of_recovery_identity() {
        let model = Ollama::new("http://localhost:11434/v1/chat/completions", None).unwrap();
        let unchanged = model.clone().with_prompt_cache_policy(false, false);
        assert_eq!(model.context_identity(), unchanged.context_identity());
        assert_ne!(
            model.context_identity(),
            model
                .clone()
                .with_prompt_cache_policy(true, false)
                .context_identity()
        );
        assert_ne!(
            model.context_identity(),
            model
                .clone()
                .with_prompt_cache_policy(false, true)
                .context_identity()
        );
    }

    #[tokio::test]
    async fn chat_prompt_cache_policy_reaches_session_requests_and_skips_summaries() {
        use std::io::{Read, Write};
        for (key_enabled, long, summary, explicit) in [
            (false, false, false, false),
            (true, false, false, false),
            (true, true, false, false),
            (false, true, false, false),
            (true, true, true, false),
            (true, false, false, true),
            (true, true, true, true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut chunk).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + length {
                    let n = socket.read(&mut chunk).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let body: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                body
            });
            let mut model = Ollama::new(&format!("http://{address}/v1/chat/completions"), None)
                .unwrap()
                .with_native_probes(false)
                .with_prompt_cache_policy(key_enabled, long);
            if explicit {
                model = model
                    .with_sampling(BTreeMap::from([(
                        "prompt_cache_key".into(),
                        json!("configured"),
                    )]))
                    .unwrap();
            }
            let request = ModelRequest {
                model: "fixture".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 10,
            };
            if summary {
                let _ = model
                    .stream_for_summary(uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), request)
                    .await;
            } else {
                let _ = model
                    .stream_in_session_with_identity(
                        uuid::Uuid::new_v4(),
                        uuid::Uuid::new_v4(),
                        &format!("{}tail", "猫".repeat(64)),
                        request,
                    )
                    .await;
            }
            let body = worker.join().unwrap();
            let expected = if explicit {
                Some("configured".to_owned())
            } else if key_enabled && !summary {
                Some("猫".repeat(64))
            } else {
                None
            };
            assert_eq!(
                body.get("prompt_cache_key").and_then(Value::as_str),
                expected.as_deref()
            );
            assert_eq!(
                body.get("prompt_cache_retention").and_then(Value::as_str),
                (long && !summary).then_some("24h")
            );
        }
    }

    #[tokio::test]
    async fn cerebras_bodyless_400_is_overflow_without_reclassifying_other_errors() {
        use std::io::{Read, Write};
        for (provider, status, body, overflow) in [
            ("cerebras", 400, "", true),
            ("other", 400, "", false),
            ("cerebras", 422, "", false),
            ("cerebras", 400, "{}", false),
            ("cerebras", 400, "private-error-body", false),
            (
                "cerebras",
                400,
                r#"{"error":{"message":"prompt is too long"}}"#,
                true,
            ),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 1024];
                let header_end = loop {
                    let count = socket.read(&mut chunk).unwrap();
                    assert!(count > 0 && bytes.len() + count <= 32 * 1024);
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(pos) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..header_end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .unwrap();
                while bytes.len() < header_end + length {
                    let count = socket.read(&mut chunk).unwrap();
                    assert!(count > 0 && bytes.len() + count <= 64 * 1024);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                write!(socket, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            });
            let model = Ollama::new(&format!("http://{address}/v1/chat/completions"), None)
                .unwrap()
                .with_provider(provider);
            let error = match model
                .stream(ModelRequest {
                    model: "fixture".into(),
                    messages: vec![],
                    tools: vec![],
                    max_output_tokens: 16,
                })
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("expected HTTP error"),
            };
            assert_eq!(
                error.to_string().contains("context window exceeded"),
                overflow,
                "{provider} {status} {body:?}"
            );
            assert!(!error.to_string().contains("private-error-body"));
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn cerebras_bodyless_overflow_compacts_and_retries_the_turn() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            for index in 0..4 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(10)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 2048];
                let header_end = loop {
                    let count = socket.read(&mut chunk).unwrap();
                    assert!(count > 0 && bytes.len() + count <= 64 * 1024);
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(pos) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let length = String::from_utf8_lossy(&bytes[..header_end])
                    .lines()
                    .find_map(|line| {
                        line.split_once(':').and_then(|(name, value)| {
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                    })
                    .unwrap();
                while bytes.len() < header_end + length {
                    let count = socket.read(&mut chunk).unwrap();
                    assert!(count > 0 && bytes.len() + count <= 128 * 1024);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                if index == 1 {
                    write!(
                        socket,
                        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .unwrap();
                    continue;
                }
                let text = if index == 2 {
                    "saved summary"
                } else {
                    "answer"
                };
                let chunk = json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":1}});
                let body = format!("data: {chunk}\n\ndata: [DONE]\n\n");
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let model = Ollama::new(&format!("http://{address}/v1/chat/completions"), None)
            .unwrap()
            .with_provider("cerebras");
        let registry = pi_rs_core::Registry::default();
        let agent = pi_rs_core::Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: pi_rs_core::Limits {
                context_window: Some(10000),
                output_tokens_per_request: 16,
                model_requests: 3,
                compaction_tokens: Some(pi_rs_core::CompactionTokens {
                    reserve_tokens: Some(0),
                    keep_recent_tokens: None,
                }),
                ..Default::default()
            },
        };
        let mut session =
            pi_rs_core::Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
        let (_sender, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let cancel = pi_rs_core::CancellationToken::new();
        assert_eq!(
            agent
                .run(
                    &mut session,
                    Some("old".into()),
                    &mut inputs,
                    &cancel,
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "answer"
        );
        assert_eq!(
            agent
                .run(
                    &mut session,
                    Some("new".into()),
                    &mut inputs,
                    &cancel,
                    &mut |_| {}
                )
                .await
                .unwrap(),
            "answer"
        );
        assert!(session.records().iter().any(|record| matches!(&record.event, pi_rs_core::Event::Compacted { summary, .. } if summary == "saved summary")));
        worker.join().unwrap();
    }

    #[test]
    fn capacity_estimate_matches_image_and_replay_serialization() {
        let model = Ollama::new("https://example.com", None)
            .unwrap()
            .with_chat_compatibility(
                serde_json::from_value(json!({"requiresReasoningContentOnAssistantMessages":true}))
                    .unwrap(),
                Some(true),
                true,
                false,
            );
        let request = ModelRequest {
            model: "fixture".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 16,
        };
        let tool = |n| {
            Message::Tool { call_id: "c".into(), content: json!({"data":{"content":[{"type":"image","mimeType":"image/png","data":"A".repeat(n)}]}}).to_string(), is_error: false, is_structured: Some(true) }
        };
        assert_eq!(
            model.estimate_message_tokens(&request, &tool(4)),
            model.estimate_message_tokens(&request, &tool(20000))
        );
        let replay = ProviderReplay {
            identity: model.replay_identity("fixture"),
            blocks: vec![json!({"type":"reasoning_content","text":"x".repeat(5000)})],
        };
        let assistant = Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: Some(Box::new(replay)),
        };
        assert!(model.estimate_message_tokens(&request, &assistant) > 1000);
        let mut mismatch = assistant.clone();
        if let Message::Assistant {
            replay: Some(replay),
            ..
        } = &mut mismatch
        {
            replay.identity.model = "other".into();
        }
        assert_eq!(
            model.estimate_message_tokens(&request, &mismatch),
            pi_rs_core::estimate_message_tokens(&mismatch)
        );
    }
    #[test]
    fn context_identity_distinguishes_shared_gateway_providers() {
        let first = Ollama::new("https://gateway.example", None)
            .unwrap()
            .with_provider("first");
        let same = Ollama::new("https://gateway.example", None)
            .unwrap()
            .with_provider("first");
        let other = Ollama::new("https://gateway.example", None)
            .unwrap()
            .with_provider("other");
        if let Some(identity) = first.context_identity() {
            assert_eq!(Some(identity.clone()), same.context_identity());
            assert_ne!(Some(identity), other.context_identity());
        }
    }

    #[test]
    fn context_capacity_uses_matching_running_models_and_rejects_invalid_sizes() {
        let value = json!({"models": [
            {"name":"other:latest", "context_length": 1},
            {"name":"small:latest", "context_length": 8192},
            {"model":"small", "context_length": 4096},
            {"name":"small", "context_length": 0},
            {"name":"small", "context_length": -1},
            {"name":"small", "context_length": "1024"},
            {"name":"small", "context_length": 4294967296_u64}
        ]});
        assert_eq!(running_context_window(&value, "small"), Some(4096));
        assert_eq!(running_context_window(&value, "small:latest"), Some(4096));
        assert_eq!(running_context_window(&value, "missing"), None);
        assert_eq!(running_context_window(&json!({}), "small"), None);
    }

    #[test]
    fn thinking_override_preserves_defaults_and_maps_local_effort() {
        let local = Ollama::new("http://127.0.0.1:11434/v1/chat/completions", None).unwrap();
        assert_eq!(local.reasoning_effort, None);
        assert_eq!(
            local
                .clone()
                .with_reasoning_effort(Some("off"))
                .reasoning_effort
                .as_deref(),
            Some("none")
        );
        assert_eq!(
            local
                .with_reasoning_effort(Some("max"))
                .reasoning_effort
                .as_deref(),
            Some("high")
        );
    }

    #[test]
    fn edited_reasoning_falls_back_when_details_are_unusable() {
        for (config, detail) in [
            (json!({}), json!({"type":"reasoning.text","text":"valid"})),
            (
                json!({"thinkingFormat":"openrouter"}),
                json!({"type":"reasoning.text","text":7}),
            ),
        ] {
            let model = Ollama::new("https://gateway.example", None)
                .unwrap()
                .with_chat_compatibility(
                    serde_json::from_value(config).unwrap(),
                    Some(true),
                    true,
                    true,
                );
            let message = Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: model.replay_identity("m"),
                    blocks: vec![
                        json!({"type":"context_edit_reasoning","text":"preserved","details":[detail],"field":null}),
                        json!({"_pi_context_edit_api":"openai-completions"}),
                    ],
                })),
            };
            let encoded = model.compatible_message(message, &model.replay_identity("m"));
            assert_eq!(encoded[0]["reasoning_content"], "preserved");
            assert!(encoded[0].get("reasoning_details").is_none());
        }
    }

    #[tokio::test]
    async fn legacy_unbound_replay_reports_incompatibility_without_request() {
        let message = Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: Some(Box::new(ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: format!(
                        "chat-completions+sha256:{:x}",
                        Sha256::digest(b"https://gateway.example/")
                    ),
                    model: "m".into(),
                },
                blocks: vec![json!({"type":"reasoning_content","text":"secret"})],
            })),
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![message],
            tools: vec![],
            max_output_tokens: 16,
        };
        for provider in ["one", "two"] {
            let model = Ollama::new("https://gateway.example", None)
                .unwrap()
                .with_provider(provider);
            assert!(model.has_ambiguous_legacy_replay(&request));
            let mut different_model = request.clone();
            different_model.model = "other".into();
            assert!(!model.has_ambiguous_legacy_replay(&different_model));
            let encoded = model.compatible_message(
                different_model.messages[0].clone(),
                &model.replay_identity("other"),
            );
            assert_eq!(encoded[0]["content"], "answer");
            assert!(!encoded[0].to_string().contains("secret"));
            let other_endpoint = Ollama::new("https://other.example", None)
                .unwrap()
                .with_provider(provider);
            assert!(!other_endpoint.has_ambiguous_legacy_replay(&request));
            let error = match model.stream(request.clone()).await {
                Err(error) => error.to_string(),
                Ok(_) => panic!("legacy replay must be rejected"),
            };
            assert!(error.contains("no provider identity"));
        }
    }

    #[test]
    fn shared_gateway_provider_identity_and_mixed_edited_reasoning() {
        let compat = serde_json::from_value(json!({"thinkingFormat":"openrouter"})).unwrap();
        let a = Ollama::new("https://gateway.example/v1/chat/completions", None)
            .unwrap()
            .with_provider("provider-a")
            .with_chat_compatibility(compat, Some(true), true, true);
        let b = Ollama::new("https://gateway.example/v1/chat/completions", None)
            .unwrap()
            .with_provider("provider-b");
        let message = Message::Assistant {
            content: "firstmiddlelast".into(),
            calls: vec![],
            replay: Some(Box::new(ProviderReplay {
                identity: a.replay_identity("m"),
                blocks: vec![
                    json!({"type":"context_edit_text","text":"first"}),
                    json!({"type":"context_edit_reasoning","text":"signed","details":[{"type":"reasoning.text","text":"signed"}]}),
                    json!({"type":"context_edit_text","text":"middle"}),
                    json!({"type":"context_edit_reasoning","text":"ordinary","field":null}),
                    json!({"type":"context_edit_text","text":"last"}),
                    json!({"_pi_context_edit_api":"openai-completions"}),
                ],
            })),
        };
        let encoded = a.compatible_message(message.clone(), &a.replay_identity("m"));
        assert_eq!(encoded[0]["reasoning_details"][0]["text"], "signed");
        assert_eq!(encoded[0]["reasoning_content"], "ordinary");
        let other = b.compatible_message(message.clone(), &b.replay_identity("m"));
        assert!(other[0].get("reasoning_content").is_none());
        assert!(other[0].get("reasoning_details").is_none());
        let text_model = a.with_chat_compatibility(
            serde_json::from_value(json!({"requiresThinkingAsText":true})).unwrap(),
            Some(true),
            true,
            true,
        );
        assert_eq!(
            text_model.compatible_message(message, &text_model.replay_identity("m"))[0]["content"],
            "firstsignedmiddleordinarylast"
        );
    }

    #[test]
    fn edited_reasoning_uses_compatibility_and_counts_wire_fields() {
        for (config, field) in [
            (json!({"reasoningField":"reasoning"}), "reasoning"),
            (json!({"reasoningField":"reasoning_text"}), "reasoning_text"),
            (json!({}), "reasoning_content"),
        ] {
            let model = Ollama::new("https://example.com", None)
                .unwrap()
                .with_chat_compatibility(
                    serde_json::from_value(config).unwrap(),
                    Some(true),
                    true,
                    true,
                );
            let message = Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: Some(Box::new(ProviderReplay {
                    identity: model.replay_identity("model"),
                    blocks: vec![
                        json!({"type":"context_edit_reasoning","text":"x".repeat(5000),"field":null}),
                        json!({"_pi_context_edit_api":"openai-completions"}),
                    ],
                })),
            };
            let request = ModelRequest {
                model: "model".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 16,
            };
            assert_eq!(
                model.compatible_message(message.clone(), &model.replay_identity("model"))[0]
                    [field]
                    .as_str()
                    .unwrap()
                    .len(),
                5000
            );
            assert!(model.estimate_message_tokens(&request, &message) > 1000);
        }
        let model = Ollama::new("https://example.com", None)
            .unwrap()
            .with_chat_compatibility(
                serde_json::from_value(json!({"requiresThinkingAsText":true})).unwrap(),
                Some(true),
                true,
                true,
            );
        let message = Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: Some(Box::new(ProviderReplay {
                identity: model.replay_identity("model"),
                blocks: vec![
                    json!({"type":"context_edit_reasoning","text":"x".repeat(5000),"field":null}),
                    json!({"_pi_context_edit_api":"openai-completions"}),
                ],
            })),
        };
        let request = ModelRequest {
            model: "model".into(),
            messages: vec![],
            tools: vec![],
            max_output_tokens: 16,
        };
        assert!(model.compatible_message(message.clone(), &model.replay_identity("model"))[0]["content"].as_str().unwrap().ends_with("\n\nanswer"));
        assert!(model.estimate_message_tokens(&request, &message) > 1000);
        let empty = Message::Assistant {
            content: String::new(),
            calls: vec![],
            replay: if let Message::Assistant { replay, .. } = message {
                replay
            } else {
                unreachable!()
            },
        };
        assert!(
            model
                .compatible_message(empty, &model.replay_identity("other"))
                .is_empty()
        );
    }

    #[test]
    fn image_tool_results_keep_pairing_and_move_images_to_a_user_content_block() {
        let content = json!({"data":{"content":[{"type":"text","text":"A chart"},{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}]},"attachments":[]}).to_string();
        let messages = encode_tool_result("call-1", content, Some(true));
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["tool_call_id"], "call-1");
        assert!(
            !messages[0]["content"]
                .as_str()
                .unwrap()
                .contains("aW1hZ2U=")
        );
        assert_eq!(
            messages[1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aW1hZ2U="
        );
        assert_eq!(
            encode_tool_result("text", "unchanged".into(), Some(false))[0]["content"],
            "unchanged"
        );
    }
    #[test]
    fn plain_json_tool_output_stays_verbatim_and_has_no_image_message() {
        let text = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        let messages = encode_message(Message::Tool {
            call_id: "call".into(),
            content: text.into(),
            is_error: false,
            is_structured: Some(false),
        });
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["content"], text);
        let legacy =
            serde_json::from_value(json!({"role":"tool","call_id":"call","content":text})).unwrap();
        let messages = encode_message(legacy);
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aW1hZ2U="
        );
    }

    #[test]
    fn user_images_are_sent_in_the_original_user_message() {
        let messages = encode_message(Message::UserWithImages {
            content: "look".into(),
            images: vec![pi_rs_core::ImageContent {
                data: "aW1hZ2U=".into(),
                mime_type: "image/png".into(),
            }],
        });
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"][0]["text"], "look");
        assert_eq!(
            messages[0]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aW1hZ2U="
        );
    }
}

#[cfg(test)]
mod constrained_chat_tests {
    use super::*;
    use pi_rs_core::{
        EffectiveToolSampling, StrictToolPolicy, ToolConstraint, ToolDeclaration,
        ToolDispatchContext, ToolSpec,
    };

    fn fixture(policy: StrictToolPolicy, schema: Value) -> (ModelRequest, ToolDeclaration) {
        let spec = ToolSpec {
            name: "read".into(),
            description: "Read".into(),
            parameters: schema,
        };
        (
            ModelRequest {
                model: "m".into(),
                messages: vec![],
                tools: vec![spec.clone()],
                max_output_tokens: 16,
            },
            ToolDeclaration {
                spec,
                constrained_sampling: Some(ToolConstraint::JsonSchema { strict: policy }),
            },
        )
    }

    fn model(support: Option<bool>, endpoint: &str) -> Ollama {
        let mut model = Ollama::new(endpoint, None).unwrap();
        Arc::make_mut(&mut model.compatibility).supports_strict_mode = support;
        model
    }

    #[test]
    fn chat_compatibility_overrides_are_local_to_each_client_clone() {
        let base = Ollama::new("http://127.0.0.1:11434", None).unwrap();
        let configured = base
            .clone()
            .with_strict_tool_support(false)
            .with_grammar_tool_support(true);
        let mistral = base.clone().with_mistral(false, false, None);
        assert_eq!(configured.compatibility.supports_strict_mode, Some(false));
        assert_eq!(
            configured.compatibility.supports_openai_grammar_tools(),
            Some(true)
        );
        assert_eq!(mistral.compatibility.supports_strict_mode, Some(true));
        assert_eq!(base.compatibility.supports_strict_mode, None);
        assert_eq!(base.compatibility.supports_openai_grammar_tools(), None);
    }

    #[test]
    fn constrained_chat_resolution_preserves_defaults_and_fails_closed() {
        let schema = json!({"type":"object", "properties":{"path":{"type":"string"},"limit":{"type":"integer"}}, "required":["path"]});
        let (request, preferred) = fixture(StrictToolPolicy::Prefer, schema.clone());
        for support in [None, Some(false), Some(true)] {
            let (wire, decisions) = resolved_constrained_tools(
                &request,
                std::slice::from_ref(&preferred),
                support == Some(true),
                false,
            )
            .unwrap();
            if support == Some(true) {
                assert_eq!(wire.declarations[0]["function"]["strict"], true);
                assert_eq!(
                    wire.declarations[0]["function"]["parameters"]["required"],
                    json!(["path", "limit"])
                );
                assert_eq!(
                    wire.declarations[0]["function"]["parameters"]["properties"]["limit"],
                    json!({"anyOf":[{"type":"integer"},{"type":"null"}]})
                );
                assert!(matches!(
                    decisions[0].sampling,
                    EffectiveToolSampling::StrictJsonSchema { .. }
                ));
            } else {
                assert!(wire.declarations[0]["function"].get("strict").is_none());
                assert_eq!(wire.declarations[0]["function"]["parameters"], schema);
                assert!(matches!(
                    decisions[0].sampling,
                    EffectiveToolSampling::Unconstrained
                ));
            }
        }
        let (request, required) = fixture(StrictToolPolicy::Require, schema);
        assert!(resolved_constrained_tools(&request, &[required], false, false).is_err());
        let (request, preferred) = fixture(
            StrictToolPolicy::Prefer,
            json!({"type":"object","$ref":"#/x"}),
        );
        let (wire, decisions) =
            resolved_constrained_tools(&request, &[preferred], true, false).unwrap();
        assert_eq!(wire.declarations[0]["function"]["strict"], false);
        assert!(matches!(
            decisions[0].sampling,
            EffectiveToolSampling::Unconstrained
        ));
        let (request, required) = fixture(
            StrictToolPolicy::Require,
            json!({"type":"object","$ref":"#/x"}),
        );
        assert!(resolved_constrained_tools(&request, &[required], true, false).is_err());
        let (_, mismatch) = fixture(StrictToolPolicy::Prefer, json!({"type":"object"}));
        assert!(resolved_constrained_tools(&request, &[mismatch], true, false).is_err());
    }

    #[tokio::test]
    async fn chat_grammar_tools_use_custom_wire_calls_and_stream_json_arguments() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut content_length = None;
            loop {
                let mut line = String::new();
                assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(length) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                    content_length = Some(length.trim().parse::<usize>().unwrap());
                }
            }
            let mut bytes = vec![0; content_length.unwrap()];
            socket.read_exact(&mut bytes).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                body["tools"][0],
                json!({"type":"custom","custom":{"name":"read","description":"Read","format":{
                    "type":"grammar","grammar":{"syntax":"lark","definition":"start: /.+/"}
                }}})
            );
            let historical = &body["messages"][0]["tool_calls"];
            assert_eq!(
                historical[0],
                json!({"id":"replay","type":"custom","custom":{"name":"read","input":"old"}})
            );
            assert_eq!(
                historical[1],
                json!({"id":"legacy","type":"function","function":{"name":"read","arguments":r#"{"path":"old","line":3}"#}})
            );
            let events = [
                json!({"choices":[{"index":0,"delta":{"tool_calls":[
                    {"index":0,"id":"c","type":"custom","custom":{"name":"read","input":"a\""}},
                    {"index":1,"id":"d","type":"custom","custom":{"name":"read","input":""}}
                ]}}]}),
                json!({"choices":[{"index":0,"delta":{"tool_calls":[
                    {"index":0,"custom":{"name":"_file","input":"\\\n界"}}
                ]},"finish_reason":"tool_calls"}]}),
            ]
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>()
                + "data: [DONE]\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}",
                events.len()
            );
            socket
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
        });

        let client = model(None, &endpoint).with_grammar_tool_support(true);
        let (mut request, mut short) = fixture(
            StrictToolPolicy::Prefer,
            json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}),
        );
        short.constrained_sampling = Some(ToolConstraint::Grammar {
            variants: pi_rs_core::ToolGrammarVariants {
                openai_lark: Some("start: /.+/".into()),
                openai_regex: Some(".+".into()),
            },
        });
        let mut long = short.clone();
        long.spec.name = "read_file".into();
        request.tools.push(long.spec.clone());
        request.messages.push(Message::Assistant {
            content: String::new(),
            calls: vec![
                ToolCall {
                    id: "replay".into(),
                    name: "read".into(),
                    arguments: r#"{"path":"old"}"#.into(),
                },
                ToolCall {
                    id: "legacy".into(),
                    name: "read".into(),
                    arguments: r#"{"path":"old","line":3}"#.into(),
                },
            ],
            replay: None,
        });
        for id in ["replay", "legacy"] {
            request.messages.push(Message::Tool {
                call_id: id.into(),
                content: "ok".into(),
                is_structured: Some(false),
                is_error: false,
            });
        }
        let declarations = [short, long];
        let context = ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "fixture".into(),
            summary: false,
        };
        let prepared = client
            .prepare_bound_tool_dispatch(&request, &declarations, &context)
            .unwrap();
        assert!(matches!(
            prepared.effective_tools()[0].sampling,
            EffectiveToolSampling::OpenAiGrammar { .. }
        ));
        let mut stream = prepared.dispatch().await.unwrap();
        let mut fragments: BTreeMap<u64, String> = BTreeMap::new();
        let mut completed = None;
        while let Some(event) = stream.next().await {
            match event.unwrap().event {
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(delta)) => {
                    fragments
                        .entry(delta.index)
                        .or_default()
                        .push_str(&delta.arguments);
                }
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete { calls, .. }) => {
                    completed = Some(calls);
                }
                _ => {}
            }
        }
        let completed = completed.unwrap();
        for (index, expected) in ["a\"\\\n界", ""].into_iter().enumerate() {
            let call = &completed[index];
            assert_eq!(fragments[&(index as u64)], call.arguments);
            assert_eq!(
                serde_json::from_str::<Value>(&call.arguments).unwrap(),
                json!({"path":expected})
            );
        }
        assert_eq!(completed[0].name, "read_file");
        server.await.unwrap();

        let fallback = client
            .with_grammar_tool_support(false)
            .prepare_tool_request(request, &declarations)
            .unwrap();
        assert_eq!(fallback.wire_tools.declarations[0]["type"], "function");
        assert!(matches!(
            fallback.effective_tools()[0].sampling,
            EffectiveToolSampling::Unconstrained
        ));
    }

    #[tokio::test]
    async fn constrained_chat_stream_only_preferred_fallback_keeps_raw_tool_arguments() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut buf = [0; 8192];
            let _ = socket.read(&mut buf).unwrap();
            let events = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c","type":"function","function":{"name":"read","arguments":"{\"optional\":null}"}}]},"finish_reason":"tool_calls"}]})
            );
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", events.len(), events).unwrap();
        });
        let client = model(Some(false), &endpoint);
        let (request, declaration) = fixture(
            StrictToolPolicy::Prefer,
            json!({"type":"object","properties":{"optional":{"type":"string"}}}),
        );
        let events = client
            .stream_detailed_with_tool_declarations(
                uuid::Uuid::nil(),
                uuid::Uuid::nil(),
                "session",
                request,
                &[declaration],
            )
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let calls = events
            .into_iter()
            .find_map(|event| match event.unwrap().event {
                pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete { calls, .. }) => {
                    Some(calls)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(calls[0].arguments, r#"{"optional":null}"#);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn constrained_chat_stream_only_rejects_strict_or_unsupported_before_dispatch() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        for (support, policy) in [
            (Some(true), StrictToolPolicy::Require),
            (Some(true), StrictToolPolicy::Prefer),
            (Some(false), StrictToolPolicy::Require),
        ] {
            let client = model(support, &endpoint);
            let (request, declaration) = fixture(
                policy,
                json!({"type":"object","properties":{"optional":{"type":"string"}}}),
            );
            for summary in [false, true] {
                let result = if summary {
                    client
                        .stream_detailed_summary_with_tool_declarations(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            request.clone(),
                            std::slice::from_ref(&declaration),
                        )
                        .await
                } else {
                    client
                        .stream_detailed_with_tool_declarations(
                            uuid::Uuid::nil(),
                            uuid::Uuid::nil(),
                            "session",
                            request.clone(),
                            std::slice::from_ref(&declaration),
                        )
                        .await
                };
                assert!(matches!(result, Err(Error::Config(_))));
            }
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn constrained_chat_prepared_wire_matches_decision_in_session_and_summary() {
        use std::io::{Read, Write};
        for (summary, direct) in [(false, false), (true, false), (false, true), (true, true)] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0; 4096];
                let (start, length) = loop {
                    let count = socket.read(&mut buf).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buf[..count]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..pos]);
                        let length: usize = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|s| s.parse().ok())
                            })
                            .unwrap();
                        break (pos + 4, length);
                    }
                };
                while bytes.len() < start + length {
                    let n = socket.read(&mut buf).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let body: Value = serde_json::from_slice(&bytes[start..start + length]).unwrap();
                let response = format!(
                    "data: {}\n\ndata: [DONE]\n\n",
                    json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read","arguments":"{\"optional\":null}"}}]},"finish_reason":"tool_calls"}]})
                );
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).unwrap();
                body
            });
            let client = model(Some(true), &format!("http://{address}"))
                .with_prompt_cache_policy(true, true)
                .with_session_cache_key("stale")
                .with_cache_retention(CacheRetention::Short);
            let (request, declaration) = fixture(
                StrictToolPolicy::Require,
                json!({"type":"object", "properties":{"optional":{"type":"string"}}}),
            );
            let context = ToolDispatchContext {
                request_id: uuid::Uuid::new_v4(),
                session_id: uuid::Uuid::new_v4(),
                external_id: "session".into(),
                summary,
            };
            let prepared = client
                .prepare_bound_tool_dispatch(&request, std::slice::from_ref(&declaration), &context)
                .unwrap();
            pi_rs_core::validate_prepared_tool_dispatch(
                &*prepared,
                &context,
                &request,
                std::slice::from_ref(&declaration),
            )
            .unwrap();
            let expected = prepared.reservation_request().tools[0].parameters.clone();
            assert!(matches!(
                prepared.effective_tools()[0].sampling,
                EffectiveToolSampling::StrictJsonSchema { .. }
            ));
            if direct {
                let result = if summary {
                    client
                        .stream_detailed_summary_with_tool_declarations(
                            context.request_id,
                            context.session_id,
                            request.clone(),
                            std::slice::from_ref(&declaration),
                        )
                        .await
                } else {
                    client
                        .stream_detailed_with_tool_declarations(
                            context.request_id,
                            context.session_id,
                            &context.external_id,
                            request.clone(),
                            std::slice::from_ref(&declaration),
                        )
                        .await
                };
                assert!(
                    matches!(result, Err(Error::Config(message)) if message.contains("requires prepared dispatch"))
                );
            }
            let stream = prepared.dispatch().await.unwrap();
            let events = stream.collect::<Vec<_>>().await;
            let calls = events
                .into_iter()
                .find_map(|event| match event.unwrap().event {
                    pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete {
                        calls, ..
                    }) => Some(calls),
                    _ => None,
                })
                .expect("completed tool call");
            assert_eq!(calls[0].arguments, r#"{"optional":null}"#);
            let arguments: Value = serde_json::from_str(&calls[0].arguments).unwrap();
            assert_eq!(
                pi_rs_core::normalize_strict_tool_arguments(
                    &declaration.spec.parameters,
                    &arguments
                )
                .unwrap(),
                json!({})
            );
            assert_eq!(arguments, json!({"optional":null}));
            let body = worker.join().unwrap();
            assert_eq!(body["tools"][0]["function"]["parameters"], expected);
            assert_eq!(body["tools"][0]["function"]["strict"], true);
            assert_eq!(
                body.get("prompt_cache_key").and_then(Value::as_str),
                (!summary).then_some("session")
            );
            assert_eq!(
                body.get("prompt_cache_retention").and_then(Value::as_str),
                (!summary).then_some("24h")
            );
            assert_eq!(body["tools"][0].get("cache_control").is_some(), !summary);
        }
    }
}
