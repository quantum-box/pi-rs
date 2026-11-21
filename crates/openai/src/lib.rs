//! OpenAI Responses transport for ChatGPT plan OAuth credentials.
//!
//! Credential acquisition and renewal are the responsibility of the caller.
pub mod oauth;

use base64::Engine;
use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::StreamExt;
use pi_rs_core::{
    CacheRetention, Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    ProviderReplay, ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub const CODEX_ENDPOINT: &str = "https://chatgpt.com/backend-api/codex/responses";
pub const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/responses";
const MAX_STREAM: usize = 32 * 1024 * 1024;
const NAMESPACE: &str = "pi";

#[derive(Debug)]
enum RawStreamError {
    LimitExceeded,
    Transport,
}

fn recognized_response_event(kind: &str) -> bool {
    matches!(
        kind,
        "response.created"
            | "response.queued"
            | "response.in_progress"
            | "response.output_item.added"
            | "response.output_item.done"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.delta"
            | "response.output_text.done"
            | "response.refusal.delta"
            | "response.refusal.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done"
            | "response.function_call_arguments.delta"
            | "response.function_call_arguments.done"
            | "response.custom_tool_call_input.delta"
            | "response.custom_tool_call_input.done"
            | "response.completed"
            | "response.incomplete"
            | "response.failed"
            | "response.done"
    )
}

/// Routing headers used by an API-key Responses endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionAffinityFormat {
    OpenAi,
    OpenAiNoSession,
    OpenRouter,
    Disabled,
}

#[derive(Clone)]
pub struct OpenAiResponses {
    client: Client,
    endpoint: Url,
    endpoint_source: String,
    token: String,
    azure_api_version: Option<String>,
    azure_off_reasoning: Option<String>,
    deployments: Arc<BTreeMap<String, String>>,
    codex_account: Option<String>,
    codex_off_effort: Option<String>,
    thinking: Option<String>,
    provider: String,
    headers: reqwest::header::HeaderMap,
    sampling: BTreeMap<String, Value>,
    identity_safe_transport: bool,
    cache_retention: CacheRetention,
    explicit_cache_mode: bool,
    session_cache_key: Option<String>,
    session_affinity_format: Option<SessionAffinityFormat>,
    prompt_cache_key_enabled: bool,
    strict_tool_support: bool,
    use_developer_role: bool,
}

/// A Responses request whose wire tools and effective sampling are fixed.
pub struct PreparedResponsesRequest {
    client: OpenAiResponses,
    request: ModelRequest,
    reservation_request: ModelRequest,
    wire_tools: Vec<Value>,
    effective_tools: Vec<pi_rs_core::EffectiveToolDeclaration>,
}

impl PreparedResponsesRequest {
    /// The exact per-tool decisions associated with this prepared request.
    pub fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
        &self.effective_tools
    }

    /// Dispatch using an external session identity.
    pub async fn stream_in_session(
        self,
        external_id: &str,
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let client = self
            .client
            .with_session_cache_key(external_id)
            .with_session_routing(external_id)?;
        let stream = client
            .stream_enriched_with_tools(self.request, Some(self.wire_tools))
            .await?;
        Ok(Box::pin(stream.map(|event| event.map(Into::into))))
    }

    /// Dispatch as a summary without reusing the session cache.
    pub async fn stream_for_summary(self) -> Result<pi_rs_core::DetailedModelStream> {
        let stream = self
            .client
            .without_cache()
            .stream_enriched_with_tools(self.request, Some(self.wire_tools))
            .await?;
        Ok(Box::pin(stream.map(|event| event.map(Into::into))))
    }
}

struct PreparedResponsesDispatch {
    request: PreparedResponsesRequest,
    context: pi_rs_core::ToolDispatchContext,
    snapshot: pi_rs_core::ToolDecisionSnapshot,
}

#[async_trait::async_trait]
impl pi_rs_core::PreparedToolDispatch for PreparedResponsesDispatch {
    fn effective_tools(&self) -> &[pi_rs_core::EffectiveToolDeclaration] {
        self.request.effective_tools()
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
        if self.context.summary {
            self.request.stream_for_summary().await
        } else {
            self.request
                .stream_in_session(&self.context.external_id)
                .await
        }
    }
}

impl OpenAiResponses {
    /// Prepare an owned dispatch bound to its request and session context.
    pub fn prepare_bound_tool_dispatch(
        &self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        context: &pi_rs_core::ToolDispatchContext,
    ) -> Result<Box<dyn pi_rs_core::PreparedToolDispatch>> {
        let prepared = self.prepare_tool_request(request.clone(), declarations)?;
        let snapshot = pi_rs_core::ToolDecisionSnapshot::capture(
            context,
            request,
            declarations,
            prepared.effective_tools(),
        )
        .map_err(|error| Error::Config(error.to_string()))?;
        Ok(Box::new(PreparedResponsesDispatch {
            request: prepared,
            context: context.clone(),
            snapshot,
        }))
    }

    /// Resolve tool constraints once and bind their decisions to this request.
    pub fn prepare_tool_request(
        &self,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<PreparedResponsesRequest> {
        let (wire_tools, effective_tools) = resolved_constrained_tools(
            &request,
            declarations,
            self.strict_tool_support,
            self.has_implicit_strict_tools(),
        )?;
        let mut reservation_request = request.clone();
        for (spec, wire) in reservation_request.tools.iter_mut().zip(&wire_tools) {
            spec.parameters = wire["parameters"].clone();
        }
        Ok(PreparedResponsesRequest {
            client: self.clone(),
            request,
            reservation_request,
            wire_tools,
            effective_tools,
        })
    }
    fn has_implicit_strict_tools(&self) -> bool {
        // OpenAI Responses may enable strict sampling when the field is omitted.
        self.provider == "openai-chatgpt" || self.endpoint.host_str() == Some("api.openai.com")
    }

    pub fn with_http_options(
        mut self,
        timeout: Option<std::time::Duration>,
        proxies: &[reqwest::Proxy],
    ) -> Result<Self> {
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none());
        if let Some(timeout) = timeout {
            builder = builder.read_timeout(timeout);
        }
        for proxy in proxies {
            builder = builder.proxy(proxy.clone());
        }
        self.identity_safe_transport = proxies.is_empty();
        self.client = builder
            .build()
            .map_err(|_| Error::Config("could not initialize OpenAI client".into()))?;
        Ok(self)
    }
    pub fn new(endpoint: &str, token: String) -> Result<Self> {
        Self::create(endpoint, token, true)
    }

    /// Connect with an OpenAI API key, rather than a ChatGPT plan token.
    pub fn new_api(endpoint: &str, token: String) -> Result<Self> {
        Self::create(endpoint, token, false)
    }

    /// Connect to an API-key Responses provider with its own model identity.
    pub fn new_api_provider(endpoint: &str, token: String, provider: &str) -> Result<Self> {
        if provider.is_empty() || provider == "openai-chatgpt" {
            return Err(Error::Config("invalid Responses provider identity".into()));
        }
        let mut client = Self::create(endpoint, token, false)?;
        client.provider = provider.to_owned();
        Ok(client)
    }

    /// Connect to an Azure Responses endpoint using its API-key authentication.
    pub fn new_azure(endpoint: &str, token: String, provider: &str) -> Result<Self> {
        let mut client = Self::new_api_provider(endpoint, token, provider)?;
        client.azure_api_version = Some("v1".into());
        client.session_affinity_format = Some(SessionAffinityFormat::Disabled);
        Ok(client)
    }

    pub fn is_azure(&self) -> bool {
        self.azure_api_version.is_some()
    }

    pub fn new_codex(endpoint: &str, token: String, provider: &str) -> Result<Self> {
        let account = codex_account_id(&token).ok_or_else(|| {
            Error::Config("OpenAI Codex requires a bearer token with an account ID".into())
        })?;
        let mut client = Self::new_api_provider(endpoint, token, provider)?;
        client.codex_account = Some(account);
        client.codex_off_effort = Some("none".into());
        client.strict_tool_support = true;
        Ok(client)
    }

    pub fn with_codex_off_effort(mut self, effort: Option<&str>) -> Self {
        self.codex_off_effort = effort.map(str::to_owned);
        self
    }

    fn api(&self) -> pi_rs_core::ModelApi {
        if self.codex_account.is_some() {
            pi_rs_core::ModelApi::OpenaiCodexResponses
        } else {
            pi_rs_core::ModelApi::OpenaiResponses
        }
    }

    fn create(endpoint: &str, token: String, plan: bool) -> Result<Self> {
        let endpoint_source = endpoint.to_owned();
        let endpoint = Url::parse(endpoint)
            .map_err(|_| Error::Config("invalid OpenAI Responses endpoint".into()))?;
        let secure = endpoint.as_str() == DEFAULT_ENDPOINT
            || (endpoint.scheme() == "http"
                && matches!(
                    endpoint.host_str(),
                    Some("127.0.0.1" | "localhost" | "[::1]")
                ))
            || (!plan && endpoint.scheme() == "https");
        if !secure
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Config("ChatGPT plan requests require the official OpenAI Responses endpoint or loopback HTTP".into()));
        }
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(Error::Config("invalid OpenAI bearer token".into()));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Config("could not initialize OpenAI client".into()))?;
        Ok(Self {
            client,
            endpoint,
            endpoint_source,
            token,
            azure_api_version: None,
            azure_off_reasoning: None,
            deployments: Arc::default(),
            codex_account: None,
            codex_off_effort: None,
            thinking: None,
            identity_safe_transport: true,
            provider: if plan { "openai-chatgpt" } else { "openai" }.into(),
            headers: reqwest::header::HeaderMap::new(),
            sampling: BTreeMap::new(),
            cache_retention: CacheRetention::Short,
            explicit_cache_mode: false,
            session_cache_key: None,
            session_affinity_format: None,
            prompt_cache_key_enabled: true,
            strict_tool_support: false,
            use_developer_role: false,
        })
    }

    /// Set the API version and model-to-deployment map for Azure Responses.
    pub fn with_azure_routes(
        mut self,
        version: &str,
        deployments: &BTreeMap<String, String>,
        off_reasoning: Option<&str>,
    ) -> Self {
        if self.azure_api_version.is_some() {
            self.azure_api_version = Some(version.to_owned());
            self.deployments = Arc::new(deployments.clone());
            self.azure_off_reasoning = off_reasoning.map(str::to_owned);
        }
        self
    }

    fn replay_provider(&self, model: &str) -> String {
        if self.azure_api_version.is_some() {
            let deployment = self.deployments.get(model).map_or(model, String::as_str);
            pi_rs_core::endpoint_replay_scope(
                &self.provider,
                &format!("{}\n{deployment}", self.endpoint),
            )
        } else {
            self.provider.clone()
        }
    }

    pub fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking = level.map(|level| if level == "off" { "none" } else { level }.to_owned());
        self
    }

    /// Set endpoint-wide strict JSON Schema tool support for this client.
    /// Callers routing different models must ensure the setting applies to each.
    pub fn with_strict_tool_support(mut self, supported: bool) -> Self {
        self.strict_tool_support = supported;
        self
    }

    /// Configure Responses prompt cache retention for the selected model.
    pub fn with_cache_retention(mut self, retention: CacheRetention, explicit_mode: bool) -> Self {
        self.cache_retention = retention;
        self.explicit_cache_mode = explicit_mode;
        self
    }

    /// Select whether model instructions use the developer role. Defaults to
    /// the system role when model reasoning capability is not supplied.
    pub fn with_developer_role(mut self, enabled: bool) -> Self {
        self.use_developer_role = enabled;
        self
    }

    /// Set the Pi session identity used for prompt cache affinity.
    pub fn with_session_cache_key(mut self, session_id: &str) -> Self {
        self.session_cache_key =
            (!session_id.is_empty()).then(|| session_id.chars().take(64).collect());
        self
    }

    /// Override the automatically detected API-key session routing format.
    pub fn with_session_affinity_format(mut self, format: SessionAffinityFormat) -> Self {
        self.session_affinity_format = Some(format);
        self
    }

    fn with_session_routing(mut self, session_id: &str) -> Result<Self> {
        if session_id.is_empty()
            || self.provider == "openai-chatgpt"
            || self.cache_retention == CacheRetention::None
        {
            return Ok(self);
        }
        let format = self.session_affinity_format.unwrap_or_else(|| {
            if self.provider == "openrouter" || self.endpoint_source.contains("openrouter.ai") {
                SessionAffinityFormat::OpenRouter
            } else {
                SessionAffinityFormat::OpenAi
            }
        });
        let names: &[&str] = match format {
            SessionAffinityFormat::OpenAi => &["session-id", "x-client-request-id"],
            SessionAffinityFormat::OpenAiNoSession => &["x-client-request-id"],
            SessionAffinityFormat::OpenRouter => &["x-session-id"],
            SessionAffinityFormat::Disabled => &[],
        };
        if names.iter().all(|name| self.headers.contains_key(*name)) {
            return Ok(self);
        }
        let bytes = session_id
            .chars()
            .map(|ch| u8::try_from(u32::from(ch)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error::Config("Invalid session affinity ID".into()))?;
        let whitespace = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\r' | b'\n');
        let start = bytes
            .iter()
            .position(|byte| !whitespace(byte))
            .unwrap_or(bytes.len());
        let end = bytes
            .iter()
            .rposition(|byte| !whitespace(byte))
            .map_or(start, |index| index + 1);
        let value = reqwest::header::HeaderValue::from_bytes(&bytes[start..end])
            .map_err(|_| Error::Config("Invalid session affinity ID".into()))?;
        for name in names {
            self.headers.entry(*name).or_insert_with(|| value.clone());
        }
        Ok(self)
    }

    /// Enable or disable the automatic session-derived prompt cache key.
    pub fn with_prompt_cache_key_enabled(mut self, enabled: bool) -> Self {
        self.prompt_cache_key_enabled = enabled;
        self
    }

    /// Disable retention while preserving the model cache protocol.
    pub fn without_cache(mut self) -> Self {
        self.cache_retention = CacheRetention::None;
        self
    }

    /// Return the configured request headers for protected context identity.
    pub fn context_headers(&self) -> reqwest::header::HeaderMap {
        self.headers.clone()
    }

    pub fn with_headers(mut self, headers: reqwest::header::HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Add automatic headers without replacing explicitly configured values.
    pub fn with_default_headers(mut self, headers: &reqwest::header::HeaderMap) -> Self {
        for (name, value) in headers {
            self.headers
                .entry(name.clone())
                .or_insert_with(|| value.clone());
        }
        self
    }

    /// Set request body defaults from the selected model's sampling parameters.
    pub fn with_sampling(mut self, sampling: BTreeMap<String, Value>) -> Result<Self> {
        validate_sampling(&sampling)?;
        self.sampling = sampling;
        Ok(self)
    }

    fn apply_cache(&self, body: &mut Value) {
        if self.prompt_cache_key_enabled
            && self.cache_retention != CacheRetention::None
            && let Some(key) = &self.session_cache_key
        {
            body.as_object_mut()
                .expect("Responses request object")
                .entry("prompt_cache_key")
                .or_insert_with(|| json!(key));
        }
        if self.provider == "openai-chatgpt" || self.codex_account.is_some() {
            let object = body.as_object_mut().expect("Responses request object");
            object.remove("prompt_cache_retention");
            object.remove("prompt_cache_options");
            if self.cache_retention == CacheRetention::None {
                object.remove("prompt_cache_key");
            }
            return;
        }
        if self.cache_retention == CacheRetention::None {
            let object = body.as_object_mut().expect("Responses request object");
            object.remove("prompt_cache_key");
            object.remove("prompt_cache_retention");
            object.remove("prompt_cache_options");
        }
        let cache = match (self.explicit_cache_mode, self.cache_retention) {
            (true, CacheRetention::None) => {
                Some(("prompt_cache_options", json!({"mode":"explicit"})))
            }
            (true, CacheRetention::Long) => Some(("prompt_cache_options", json!({"ttl":"30m"}))),
            (false, CacheRetention::Long) => Some(("prompt_cache_retention", json!("24h"))),
            _ => None,
        };
        if let Some((key, value)) = cache {
            let object = body.as_object_mut().expect("Responses request object");
            // An explicitly configured cache policy takes precedence as a whole.
            if !object.contains_key("prompt_cache_options")
                && !object.contains_key("prompt_cache_retention")
            {
                object.insert(key.into(), value);
            }
        }
    }

    fn authenticate(
        &self,
        request: reqwest::RequestBuilder,
        mut headers: reqwest::header::HeaderMap,
        azure_version: Option<&str>,
    ) -> Result<reqwest::RequestBuilder> {
        if let Some(version) = azure_version {
            if !headers.contains_key("api-key") {
                let mut key = reqwest::header::HeaderValue::from_str(&self.token)
                    .map_err(|_| Error::Config("invalid Azure OpenAI API key".into()))?;
                key.set_sensitive(true);
                headers.insert("api-key", key);
            }
            Ok(request.headers(headers).query(&[("api-version", version)]))
        } else {
            Ok(request.headers(headers).bearer_auth(&self.token))
        }
    }

    /// Return the signed-in account's displayable models in server order.
    pub async fn models(&self) -> Result<Vec<String>> {
        self.models_with_azure_version(None).await
    }

    /// List models using the configured Azure API version when applicable.
    pub async fn models_with_azure_version(
        &self,
        azure_version: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut endpoint = self.endpoint.clone();
        let version = self
            .azure_api_version
            .as_deref()
            .map(|version| azure_version.unwrap_or(version));
        if version.is_some() {
            let path = endpoint.path().trim_end_matches('/');
            let base = path
                .strip_suffix("/responses")
                .ok_or_else(|| Error::Config("Azure discovery requires a /responses URL".into()))?;
            endpoint.set_path(&format!("{base}/models"));
        } else {
            endpoint.set_path("/v1/models");
        }
        let mut response = self
            .authenticate(self.client.get(endpoint), self.headers.clone(), version)?
            .send()
            .await
            .map_err(|_| Error::RetryableModel("OpenAI model listing failed".into()))?;
        if !response.status().is_success() {
            return Err(Error::Model(format!(
                "OpenAI model listing failed ({})",
                response.status()
            )));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::RetryableModel("OpenAI model listing interrupted".into()))?
        {
            if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                return Err(Error::Model("OpenAI model listing too large".into()));
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Model("invalid OpenAI model listing".into()))?;
        let models = body[if self.provider != "openai-chatgpt" {
            "data"
        } else {
            "models"
        }]
        .as_array()
        .ok_or_else(|| Error::Model("invalid OpenAI model listing".into()))?;
        Ok(models
            .iter()
            .filter(|entry| self.provider != "openai-chatgpt" || entry["visibility"] == "list")
            .filter_map(|entry| {
                entry[if self.provider != "openai-chatgpt" {
                    "id"
                } else {
                    "slug"
                }]
                .as_str()
                .filter(|slug| !slug.is_empty())
                .map(str::to_owned)
            })
            .collect())
    }
}

/// Protect fields that control the agent's conversation, tools, and transport.
pub fn validate_sampling(sampling: &BTreeMap<String, Value>) -> Result<()> {
    if sampling.contains_key("prompt_cache_options")
        && sampling.contains_key("prompt_cache_retention")
    {
        return Err(Error::Config(
            "sampling parameters cannot combine prompt_cache_options and prompt_cache_retention"
                .into(),
        ));
    }
    if let Some(key) = sampling.keys().find(|key| {
        matches!(
            key.as_str(),
            "model"
                | "input"
                | "messages"
                | "instructions"
                | "previous_response_id"
                | "conversation"
                | "prompt"
                | "tools"
                | "functions"
                | "function_call"
                | "stream"
                | "stream_options"
                | "background"
                | "store"
                | "max_output_tokens"
                | "max_tokens"
                | "max_completion_tokens"
                | "n"
                | "reasoning"
                | "reasoning_effort"
                | "include"
        )
    }) {
        return Err(Error::Config(format!(
            "sampling parameter {key:?} is managed by the agent"
        )));
    }
    Ok(())
}

/// Supported reasoning levels for known models. Unknown models use their default.
pub fn thinking_levels(model: &str) -> Vec<String> {
    let levels: &[&str] = if matches!(model, "gpt-6.1-sol" | "gpt-6-astra") {
        &["low", "medium", "high", "xhigh", "max"]
    } else if matches!(model, "gpt-6-sol" | "gpt-6-luna" | "gpt-6.1-terra") {
        &["off", "low", "medium", "high", "xhigh", "max"]
    } else {
        &["off"]
    };
    levels.iter().map(|level| (*level).into()).collect()
}

/// Extract the account routing hint from a Codex access token.
pub fn codex_account_id(token: &str) -> Option<String> {
    if token.len() > 64 * 1024 {
        return None;
    }
    let mut parts = token.split('.');
    let (header, payload, signature) = (parts.next()?, parts.next()?, parts.next()?);
    if header.is_empty() || signature.is_empty() || parts.next().is_some() {
        return None;
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let json: Value = serde_json::from_slice(&decoded).ok()?;
    let account = json["https://api.openai.com/auth"]["chatgpt_account_id"].as_str()?;
    (!account.is_empty()
        && account.len() <= 128
        && account.bytes().all(|byte| byte.is_ascii_graphic()))
    .then(|| account.to_owned())
}

#[cfg(test)]
fn payload(
    request: &ModelRequest,
    provider: &str,
    edited_scope: &str,
    instruction_role: &str,
) -> Value {
    responses_payload(
        request,
        provider,
        edited_scope,
        instruction_role,
        pi_rs_core::ModelApi::OpenaiResponses,
    )
}

fn responses_payload(
    request: &ModelRequest,
    provider: &str,
    edited_scope: &str,
    instruction_role: &str,
    api: pi_rs_core::ModelApi,
) -> Value {
    let codex = api == pi_rs_core::ModelApi::OpenaiCodexResponses;
    let mut input = Vec::new();
    let mut instructions = "You are a helpful assistant.";
    for (index, message) in request.messages.iter().enumerate() {
        match message {
            Message::System { content } if codex && index == 0 => {
                if !content.is_empty() {
                    instructions = content;
                }
            }
            Message::System { content } => {
                input.push(json!({"role":instruction_role, "content": content}))
            }
            Message::User { content } => input.push(json!({"role":"user", "content": content})),
            Message::UserWithImages { content, images } => {
                let mut blocks = vec![json!({"type":"input_text", "text":content})];
                blocks.extend(images.iter().map(|image| json!({"type":"input_image", "image_url":format!("data:{};base64,{}",image.mime_type,image.data)})));
                input.push(json!({"role":"user", "content":blocks}));
            }
            Message::Assistant {
                content,
                calls,
                replay,
            } => {
                if let Some(replay) = replay.as_deref().filter(|r| {
                    r.identity.provider
                        == if codex || r.is_context_edit() {
                            edited_scope
                        } else {
                            provider
                        }
                        && r.identity.model == request.model
                        && r.supports_api(api)
                }) {
                    let edited = replay
                        .blocks
                        .iter()
                        .any(|block| block.get("_pi_context_edit_api").is_some());
                    input.extend(
                        replay
                            .blocks
                            .iter()
                            .filter(|block| block.get("_pi_context_edit_api").is_none())
                            .cloned()
                            .map(|mut block| {
                                if edited
                                    && provider == "openai-chatgpt"
                                    && block["type"] == "function_call"
                                {
                                    block["namespace"] = json!(NAMESPACE);
                                }
                                block
                            }),
                    );
                } else {
                    if !content.is_empty() {
                        input.push(json!({"role":"assistant", "content":content}));
                    }
                    input.extend(calls.iter().map(|call| {
                        let mut item = json!({"type":"function_call", "call_id":call.id, "name":call.name, "arguments":call.arguments});
                        if provider == "openai-chatgpt" { item["namespace"] = json!(NAMESPACE); }
                        item
                    }));
                }
            }
            Message::Tool {
                call_id,
                content,
                is_structured,
                ..
            } => {
                let (text, images) =
                    if pi_rs_core::tool_content_is_structured(content, *is_structured) {
                        pi_rs_core::extract_tool_images(content.clone())
                    } else {
                        (content.clone(), Vec::new())
                    };
                let output = if images.is_empty() {
                    json!(text)
                } else {
                    let mut blocks = vec![json!({"type":"input_text", "text":text})];
                    blocks.extend(images.iter().map(|image| json!({"type":"input_image", "image_url":format!("data:{};base64,{}",image.mime_type,image.data)})));
                    json!(blocks)
                };
                input.push(
                    json!({"type":"function_call_output", "call_id":call_id, "output":output}),
                );
            }
        }
    }
    let mut body = if codex {
        json!({"model":request.model, "input":input, "store":false, "stream":true,
            "instructions":instructions, "text":{"verbosity":"low"}, "include":["reasoning.encrypted_content"],
            "tool_choice":"auto", "parallel_tool_calls":true})
    } else {
        json!({"model":request.model, "input":input, "store":false, "stream":true, "max_output_tokens":request.max_output_tokens})
    };
    if !request.tools.is_empty() {
        let tools: Vec<_> = request.tools.iter().map(|tool| json!({"type":"function", "name":tool.name, "description":tool.description, "parameters":tool.parameters, "strict":false})).collect();
        body["tools"] = if provider == "openai-chatgpt" {
            json!([{"type":"namespace", "name":NAMESPACE, "description":"Local tools", "tools":tools}])
        } else {
            json!(tools)
        };
    }
    body
}

fn resolved_constrained_tools(
    request: &ModelRequest,
    declarations: &[pi_rs_core::ToolDeclaration],
    supports_strict: bool,
    implicit_strict_tools: bool,
) -> Result<(Vec<Value>, Vec<pi_rs_core::EffectiveToolDeclaration>)> {
    if request.tools.len() != declarations.len()
        || request
            .tools
            .iter()
            .zip(declarations)
            .any(|(spec, declaration)| spec != &declaration.spec)
    {
        return Err(Error::Config(
            "tool declarations do not match the effective request".into(),
        ));
    }
    let mut wire = Vec::with_capacity(declarations.len());
    let mut effective = Vec::with_capacity(declarations.len());
    for declaration in declarations {
        let spec = &declaration.spec;
        let strict_schema = match &declaration.constrained_sampling {
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
            // Grammar is sent as an ordinary function until custom tool streaming
            // and replay are supported by this path.
            _ => None,
        };
        let mut tool = json!({"type":"function","name":spec.name,"description":spec.description,"parameters":strict_schema.as_ref().unwrap_or(&spec.parameters)});
        if supports_strict || implicit_strict_tools {
            tool["strict"] = json!(strict_schema.is_some());
        }
        let sampling = if strict_schema.is_some() {
            pi_rs_core::EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters.clone(),
                normalization_version: pi_rs_core::STRICT_TOOL_NORMALIZATION_VERSION,
            }
        } else {
            pi_rs_core::EffectiveToolSampling::Unconstrained
        };
        wire.push(tool);
        effective.push(pi_rs_core::EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling,
        });
    }
    pi_rs_core::validate_effective_tool_declarations(request, declarations, &effective)
        .map_err(|error| Error::Config(error.to_string()))?;
    Ok((wire, effective))
}

#[cfg(test)]
fn constrained_tools(
    request: &ModelRequest,
    declarations: &[pi_rs_core::ToolDeclaration],
    supports_strict: bool,
) -> Result<Vec<Value>> {
    resolved_constrained_tools(request, declarations, supports_strict, false).map(|(wire, _)| wire)
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::Model(format!("invalid OpenAI event field {name}")))
}

fn response_error(code: Option<&str>, status: Option<reqwest::StatusCode>) -> Error {
    match code {
        Some("subscription_sharing_usage_limit_exceeded") => Error::Model(
            "ChatGPT plan usage limit reached; see https://chatgpt.com/settings/usage".into(),
        ),
        Some("subscription_sharing_user_not_eligible") => {
            Error::Model("ChatGPT plan usage is unavailable for this account or workspace".into())
        }
        Some("subscription_sharing_usage_unavailable") => {
            Error::RetryableModel("ChatGPT plan usage is temporarily unavailable".into())
        }
        _ if status.is_some_and(|status| status.is_server_error()) => {
            Error::RetryableModel("OpenAI service temporarily unavailable".into())
        }
        _ => Error::Model(status.map_or_else(
            || "OpenAI response did not complete".into(),
            |status| format!("OpenAI request failed ({status})"),
        )),
    }
}

#[async_trait::async_trait]
impl Model for OpenAiResponses {
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
    fn context_identity(&self) -> Option<String> {
        if !self.identity_safe_transport {
            return None;
        }
        let mut headers: Vec<_> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort();
        let base =
            if self.cache_retention == CacheRetention::Short || self.provider == "openai-chatgpt" {
                pi_rs_core::protected_model_fingerprint(&(
                    headers,
                    "openai-responses-v1",
                    self.endpoint.as_str(),
                    &self.provider,
                    &self.token,
                    &self.thinking,
                    &self.sampling,
                ))
            } else {
                pi_rs_core::protected_model_fingerprint(&(
                    headers,
                    "openai-responses-v1",
                    self.endpoint.as_str(),
                    &self.provider,
                    &self.token,
                    &self.thinking,
                    &self.sampling,
                    match self.cache_retention {
                        CacheRetention::None => "none",
                        CacheRetention::Short => "short",
                        CacheRetention::Long => "long",
                    },
                    self.explicit_cache_mode,
                ))
            };
        let base = if let Some(version) = &self.azure_api_version {
            pi_rs_core::protected_model_fingerprint(&(
                base,
                version,
                self.deployments.as_ref(),
                &self.azure_off_reasoning,
            ))
        } else {
            base
        };
        let base = if self.session_affinity_format.is_none()
            && self.provider != "openrouter"
            && self.endpoint_source.contains("openrouter.ai")
                != self.endpoint.as_str().contains("openrouter.ai")
        {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(
                    base,
                    self.endpoint_source.contains("openrouter.ai"),
                ))
            })
        } else {
            base
        };
        let base = match self.session_affinity_format {
            None => base,
            Some(format) => base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(base, format!("{format:?}")))
            }),
        };
        let base = if self.codex_account.is_some() {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(
                    base,
                    "codex-sse-v1",
                    &self.codex_off_effort,
                ))
            })
        } else {
            base
        };
        let base = if self.prompt_cache_key_enabled {
            base
        } else {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(base, "prompt-cache-key-disabled"))
            })
        };
        let base = if self.use_developer_role {
            base
        } else {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(base, "system-instruction-role"))
            })
        };
        if self.strict_tool_support {
            base.and_then(|base| {
                pi_rs_core::protected_model_fingerprint(&(base, "strict-tools-enabled"))
            })
        } else {
            base
        }
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        let replay_provider = self.replay_provider(&request.model);
        match message {
            Message::Assistant {
                replay: Some(replay),
                ..
            } if replay.identity.provider
                == if self.codex_account.is_some() || replay.is_context_edit() {
                    pi_rs_core::endpoint_replay_scope(&replay_provider, self.endpoint.as_str())
                } else {
                    replay_provider
                }
                && replay.identity.model == request.model
                && replay.supports_api(self.api()) =>
            {
                pi_rs_core::estimate_replay_tokens(&replay.blocks)
                    .max(pi_rs_core::estimate_message_tokens(message))
            }
            _ => pi_rs_core::estimate_message_tokens(message),
        }
    }

    async fn stream_in_session(
        &self,
        _request_id: uuid::Uuid,
        session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.clone()
            .with_session_cache_key(&session_id.to_string())
            .with_session_routing(&session_id.to_string())?
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
            .with_session_routing(external_id)?
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
        let stream = self.stream_enriched(request).await?;
        Ok(Box::pin(stream.filter_map(|event| async move {
            match event {
                Ok(pi_rs_core::EnrichedModelEvent::Legacy(event)) => Some(Ok(event)),
                Ok(pi_rs_core::EnrichedModelEvent::TextDelta { delta, .. }) => {
                    Some(Ok(ModelEvent::Text(delta)))
                }
                Ok(pi_rs_core::EnrichedModelEvent::ReasoningDelta { delta, .. }) => {
                    Some(Ok(ModelEvent::Reasoning(delta)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })))
    }

    async fn stream_enriched_in_session_with_identity(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        self.clone()
            .with_session_cache_key(external_id)
            .with_session_routing(external_id)?
            .stream_enriched(request)
            .await
    }

    async fn stream_enriched_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        self.clone().without_cache().stream_enriched(request).await
    }

    async fn stream_detailed_with_tool_declarations(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let tools =
            self.stream_only_constrained_tools(&request, declarations, self.strict_tool_support)?;
        let stream = self
            .clone()
            .with_session_cache_key(external_id)
            .with_session_routing(external_id)?
            .stream_enriched_with_tools(request, Some(tools))
            .await?;
        Ok(Box::pin(stream.map(|event| event.map(Into::into))))
    }

    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
    ) -> Result<pi_rs_core::DetailedModelStream> {
        let tools =
            self.stream_only_constrained_tools(&request, declarations, self.strict_tool_support)?;
        let stream = self
            .clone()
            .without_cache()
            .stream_enriched_with_tools(request, Some(tools))
            .await?;
        Ok(Box::pin(stream.map(|event| event.map(Into::into))))
    }
}

impl OpenAiResponses {
    fn stream_only_constrained_tools(
        &self,
        request: &ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        supports_strict: bool,
    ) -> Result<Vec<Value>> {
        let (wire, effective) = resolved_constrained_tools(
            request,
            declarations,
            supports_strict,
            self.has_implicit_strict_tools(),
        )?;
        if effective.iter().any(|tool| {
            matches!(
                tool.sampling,
                pi_rs_core::EffectiveToolSampling::StrictJsonSchema { .. }
            )
        }) {
            return Err(Error::Config("strict tool sampling requires prepared dispatch so its effective decisions can be retained".into()));
        }
        Ok(wire)
    }

    /// Stream with explicit per-tool declarations and strict capability.
    ///
    /// Strict decisions require the prepared-dispatch API; unconstrained
    /// fallbacks can be streamed here without losing a normalization decision.
    pub async fn stream_with_tool_declarations(
        &self,
        request: ModelRequest,
        declarations: &[pi_rs_core::ToolDeclaration],
        supports_strict: bool,
    ) -> Result<ModelStream> {
        let tools = self.stream_only_constrained_tools(&request, declarations, supports_strict)?;
        let stream = self
            .stream_enriched_with_tools(request, Some(tools))
            .await?;
        Ok(Box::pin(stream.filter_map(|event| async move {
            match event {
                Ok(pi_rs_core::EnrichedModelEvent::Legacy(event)) => Some(Ok(event)),
                Ok(pi_rs_core::EnrichedModelEvent::TextDelta { delta, .. }) => {
                    Some(Ok(ModelEvent::Text(delta)))
                }
                Ok(pi_rs_core::EnrichedModelEvent::ReasoningDelta { delta, .. }) => {
                    Some(Ok(ModelEvent::Reasoning(delta)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })))
    }

    async fn stream_enriched(
        &self,
        request: ModelRequest,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        self.stream_enriched_with_tools(request, None).await
    }

    async fn stream_enriched_with_tools(
        &self,
        request: ModelRequest,
        tools: Option<Vec<Value>>,
    ) -> Result<pi_rs_core::EnrichedModelStream> {
        let model = request.model.clone();
        let replay_provider = self.replay_provider(&model);
        let edited_scope =
            pi_rs_core::endpoint_replay_scope(&replay_provider, self.endpoint.as_str());
        let api = self.api();
        let mut body = responses_payload(
            &request,
            &replay_provider,
            &edited_scope,
            if self.use_developer_role {
                "developer"
            } else {
                "system"
            },
            api,
        );
        if self.azure_api_version.is_some() {
            if let Some(deployment) = self.deployments.get(&model) {
                body["model"] = json!(deployment);
            }
            body["max_output_tokens"] = json!(request.max_output_tokens.max(16));
        }
        if let Some(tools) = tools
            && !tools.is_empty()
        {
            body["tools"] = if self.provider == "openai-chatgpt" {
                json!([{"type":"namespace","name":NAMESPACE,"description":"Local tools","tools":tools}])
            } else {
                json!(tools)
            };
        }
        let provider = self.provider.clone();
        if let Some(thinking) = &self.thinking {
            body["reasoning"] = json!({"effort":thinking, "summary":"auto"});
            body["include"] = json!(["reasoning.encrypted_content"]);
        } else if let Some(off) = self
            .codex_off_effort
            .as_ref()
            .or(self.azure_off_reasoning.as_ref())
        {
            body["reasoning"] = json!({"effort": off});
        }
        if self.codex_account.is_some()
            && !self.strict_tool_support
            && let Some(tools) = body["tools"].as_array_mut()
        {
            for tool in tools {
                if let Some(object) = tool.as_object_mut() {
                    object.remove("strict");
                }
            }
        }
        body.as_object_mut()
            .expect("Responses request object")
            .extend(
                self.sampling
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        self.apply_cache(&mut body);
        let error_secrets = pi_rs_core::provider_error_secrets(
            Some(self.token.as_str()),
            self.headers.iter().filter_map(|(name, value)| {
                value.to_str().ok().map(|value| (name.as_str(), value))
            }),
        );
        let mut headers = self.headers.clone();
        if provider == "github-copilot" {
            for (name, value) in pi_rs_core::copilot_request_headers(&request.messages) {
                headers
                    .entry(name)
                    .or_insert_with(|| reqwest::header::HeaderValue::from_static(value));
            }
        }
        let mut request = self.client.post(self.endpoint.clone());
        if let Some(account) = &self.codex_account {
            headers.remove("chatgpt-account-id");
            headers.remove("openai-beta");
            headers.remove(reqwest::header::AUTHORIZATION);
            request = request
                .header("chatgpt-account-id", account)
                .header("openai-beta", "responses=experimental");
            if !self.headers.contains_key("originator") {
                request = request.header("originator", "pi");
            }
        }
        let request = self.authenticate(request, headers, self.azure_api_version.as_deref())?;
        let mut response = request
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("OpenAI request failed".into()))?;
        pi_rs_core::observe_provider_response(
            response.status(),
            response.headers(),
            &error_secrets,
            &self.headers,
        )
        .await;
        if !response.status().is_success() {
            let status = response.status();
            if status.as_u16() == 413 {
                return Err(Error::Model("413 request too large".into()));
            }
            if status.as_u16() == 429 || status.is_server_error() {
                let info =
                    pi_rs_core::provider_limit_info(response.bytes_stream(), &error_secrets).await;
                return Err(if info.permanent {
                    if info.recognized_code == Some("subscription_sharing_usage_limit_exceeded") {
                        response_error(info.recognized_code, Some(status))
                    } else {
                        Error::Model("OpenAI account quota or billing limit reached".into())
                    }
                } else if info.recognized_code.is_some() {
                    response_error(info.recognized_code, Some(status))
                } else {
                    Error::RetryableModel(format!("OpenAI request failed ({status})"))
                });
            }
            let mut bytes = Vec::new();
            while let Ok(Some(chunk)) = response.chunk().await {
                if bytes.len() + chunk.len() > 64 * 1024 {
                    break;
                }
                bytes.extend_from_slice(&chunk);
            }
            let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            let body = pi_rs_core::sanitized_provider_error(&body, &error_secrets);
            if pi_rs_core::provider_context_overflow(&body, &[]) {
                return Err(Error::Model("context window exceeded".into()));
            }
            return Err(response_error(body["error"]["code"].as_str(), Some(status)));
        }
        let content_type = response.headers().get(reqwest::header::CONTENT_TYPE);
        // ChatGPT plan responses can be valid SSE without a Content-Type header.
        // The event parser still requires a valid response.completed event.
        let declared_sse = content_type
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        if !(declared_sse || (content_type.is_none() && provider == "openai-chatgpt")) {
            return Err(Error::Model("expected OpenAI event stream".into()));
        }
        // Bound wire bytes before the SSE parser sees them. Coalesce incomplete
        // lines so its parser does not repeatedly rescan a growing line.
        let bytes = async_stream::stream! {
            let mut source = response.bytes_stream();
            let mut bytes_seen = 0usize;
            let mut pending = Vec::new();
            while let Some(chunk) = source.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(_) => { yield Err(RawStreamError::Transport); return; }
                };
                bytes_seen = bytes_seen.saturating_add(chunk.len());
                if bytes_seen > MAX_STREAM {
                    yield Err(RawStreamError::LimitExceeded);
                    return;
                }
                if let Some(end) = chunk.iter().rposition(|byte| matches!(byte, b'\n' | b'\r')) {
                    pending.extend_from_slice(&chunk[..=end]);
                    yield Ok(std::mem::take(&mut pending));
                    pending.extend_from_slice(&chunk[end + 1..]);
                } else {
                    pending.extend_from_slice(&chunk);
                }
            }
            if !pending.is_empty() { yield Ok(pending); }
        };
        let mut source = Box::pin(bytes.eventsource());
        Ok(Box::pin(async_stream::try_stream! {
            let mut started = false;
            let mut calls: BTreeMap<u64, ToolCall> = BTreeMap::new();
            let mut reasoning: BTreeMap<u64, String> = BTreeMap::new();
            let mut text_items: BTreeMap<u64, String> = BTreeMap::new();
            while let Some(item) = source.next().await {
                let item = item.map_err(|error| match error {
                    EventStreamError::Transport(RawStreamError::LimitExceeded) => Error::Model("OpenAI stream exceeds 32 MiB".into()),
                    EventStreamError::Transport(RawStreamError::Transport) => Error::RetryableModel("OpenAI stream interrupted".into()),
                    _ => Error::Model("invalid OpenAI event stream".into()),
                })?;
                if item.data == "[DONE]" { continue; }
                let event: Value = serde_json::from_str(&item.data).map_err(|_| Error::Model("invalid OpenAI stream event".into()))?;
                let kind = field(&event, "type")?;
                let kind = if api == pi_rs_core::ModelApi::OpenaiCodexResponses && kind == "response.done" {
                    match event["response"]["status"].as_str() {
                        Some("completed") => "response.completed",
                        Some("incomplete") => "response.incomplete",
                        Some("failed") => "response.failed",
                        _ => kind,
                    }
                } else { kind };
                started |= recognized_response_event(kind);
                match kind {
                    "response.output_item.added" if event["item"]["type"] == "reasoning" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI reasoning index".into()))?;
                        yield pi_rs_core::EnrichedModelEvent::ReasoningStart { index };
                    }
                    "response.output_item.added" if event["item"]["type"] == "message" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI text index".into()))?;
                        text_items.entry(index).or_default();
                        yield pi_rs_core::EnrichedModelEvent::TextStart { index };
                    }
                    "response.output_text.delta" | "response.refusal.delta" => {
                        let delta = field(&event,"delta")?;
                        if let Some(index) = event["output_index"].as_u64() {
                            text_items.entry(index).or_default().push_str(delta);
                            yield pi_rs_core::EnrichedModelEvent::TextDelta { index, delta: delta.into() };
                        } else {
                            yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Text(delta.into()));
                        }
                    }
                    "response.output_item.done" if event["item"]["type"] == "message" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI text index".into()))?;
                        let content = event["item"]["content"].as_array().map(|parts| parts.iter().filter_map(|part| match part["type"].as_str() {
                            Some("output_text") => part["text"].as_str(),
                            Some("refusal") => part["refusal"].as_str(),
                            _ => None,
                        }).collect::<String>()).unwrap_or_else(|| text_items.get(&index).cloned().unwrap_or_default());
                        yield pi_rs_core::EnrichedModelEvent::TextEnd { index, content };
                    }
                    "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => { let text = field(&event,"delta")?; let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI reasoning index".into()))?; reasoning.entry(index).or_default().push_str(text); yield pi_rs_core::EnrichedModelEvent::ReasoningDelta { index, delta:text.into() }; },
                    "response.reasoning_summary_part.done" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI reasoning index".into()))?;
                        if let Some(text) = reasoning.get_mut(&index) {
                            text.push_str("\n\n");
                            yield pi_rs_core::EnrichedModelEvent::ReasoningDelta { index, delta:"\n\n".into() };
                        }
                    }
                    "response.output_item.done" if event["item"]["type"] == "reasoning" => {
                        let item = &event["item"];
                        let text = |key: &str| item[key].as_array().map(|parts| parts.iter().filter_map(|part| part["text"].as_str()).collect::<Vec<_>>().join("\n\n")).unwrap_or_default();
                        let summary = text("summary");
                        let content = text("content");
                        let finalized = if summary.is_empty() { content } else { summary };
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI reasoning index".into()))?;
                        let content = if finalized.is_empty() { reasoning.get(&index).cloned().unwrap_or_default() } else { finalized };
                        reasoning.insert(index, content.clone());
                        yield pi_rs_core::EnrichedModelEvent::ReasoningEnd { index, content };
                    }
                    "response.output_item.added" if event["item"]["type"] == "function_call" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let item = &event["item"];
                        if provider == "openai-chatgpt" && item["namespace"] != NAMESPACE { Err(Error::Model("unexpected OpenAI tool namespace".into()))?; }
                        let initial = item.get("arguments").map(|value| value.as_str().ok_or_else(|| Error::Model("invalid OpenAI tool arguments".into()))).transpose()?.unwrap_or("");
                        let call = ToolCall { id:field(item,"call_id")?.into(), name:field(item,"name")?.into(), arguments:initial.into() };
                        if calls.insert(index, call.clone()).is_some() { Err(Error::Model("duplicate OpenAI tool index".into()))?; }
                        yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(ToolCallDelta { index, id:call.id, name:call.name, arguments:call.arguments }));
                    }
                    "response.function_call_arguments.delta" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let delta = field(&event,"delta")?;
                        calls.get_mut(&index).ok_or_else(|| Error::Model("unknown OpenAI tool call".into()))?.arguments.push_str(delta);
                        yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(ToolCallDelta { index, id:String::new(), name:String::new(), arguments:delta.into() }));
                    }
                    "response.function_call_arguments.done" | "response.output_item.done" if event["type"] == "response.function_call_arguments.done" || event["item"]["type"] == "function_call" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let item = if event["type"] == "response.output_item.done" { &event["item"] } else { &event };
                        let call = calls.get_mut(&index).ok_or_else(|| Error::Model("unknown OpenAI tool call".into()))?;
                        if item.get("call_id").is_some_and(|id| id != &call.id) || item.get("name").is_some_and(|name| name != &call.name) { Err(Error::Model("OpenAI tool call changed during stream".into()))?; }
                        if let Some(value) = item.get("arguments") {
                            let final_arguments = value.as_str().ok_or_else(|| Error::Model("invalid OpenAI tool arguments".into()))?;
                            let suffix = final_arguments.strip_prefix(&call.arguments).ok_or_else(|| Error::Model("OpenAI tool call changed during stream".into()))?.to_owned();
                            call.arguments.push_str(&suffix);
                            if !suffix.is_empty() { yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(ToolCallDelta { index, id:String::new(), name:String::new(), arguments:suffix })); }
                        }
                    }
                    "response.completed" | "response.incomplete" => {
                        let response = &event["response"];
                        let finish_reason = match (Some(kind), response["status"].as_str(), response["incomplete_details"]["reason"].as_str()) {
                            (Some("response.completed"), Some("completed"), _) => "stop",
                            (Some("response.incomplete"), Some("incomplete"), Some("max_output_tokens")) => "length",
                            (Some("response.incomplete"), Some("incomplete"), reason) => {
                                Err(Error::Model(reason.map_or_else(|| "OpenAI response incomplete without a reason".to_owned(), |reason| format!("OpenAI response incomplete: {reason}"))))?
                            }
                            _ => Err(Error::Model("invalid OpenAI completion status".into()))?,
                        };
                        let output = response["output"].as_array().ok_or_else(|| Error::Model("OpenAI completion missing output".into()))?;
                        let reported_calls: Vec<_> = output.iter().filter(|item| item["type"] == "function_call").collect();
                        if reported_calls.len() != calls.len() { Err(Error::Model("incomplete OpenAI tool stream".into()))?; }
                        for ((index, call), item) in calls.iter_mut().zip(reported_calls) {
                            if item["call_id"] != call.id || item["name"] != call.name || (provider == "openai-chatgpt" && item["namespace"] != NAMESPACE) { Err(Error::Model("OpenAI tool call changed during stream".into()))?; }
                            let final_arguments = field(item, "arguments")?;
                            let suffix = final_arguments.strip_prefix(&call.arguments).ok_or_else(|| Error::Model("OpenAI tool call changed during stream".into()))?.to_owned();
                            call.arguments.push_str(&suffix);
                            if !suffix.is_empty() { yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::ToolCallDelta(ToolCallDelta { index:*index, id:String::new(), name:String::new(), arguments:suffix })); }
                        }
                        let usage = response["usage"]["input_tokens"].as_u64().zip(response["usage"]["output_tokens"].as_u64()).map(|(input_tokens,output_tokens)| Usage { input_tokens, output_tokens });
                        let identity = ResolvedModelIdentity { provider:provider.clone(), model:model.clone() };
                        let reasoning: String = reasoning.into_values().collect();
                        yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Metadata(ModelMetadata { provider_api:Some(api), replay_identity:Some(ResolvedModelIdentity { provider: edited_scope.clone(), model: model.clone() }), resolved_model:Some(identity.clone()), finish_reason:Some(finish_reason.into()), cached_input_tokens:response["usage"]["input_tokens_details"]["cached_tokens"].as_u64(), reasoning:(!reasoning.is_empty()).then_some(reasoning), replay:(finish_reason != "length").then(|| Box::new(ProviderReplay {
                            identity: if api == pi_rs_core::ModelApi::OpenaiCodexResponses {
                                ResolvedModelIdentity { provider: edited_scope.clone(), model: model.clone() }
                            } else { ResolvedModelIdentity { provider: replay_provider.clone(), model: model.clone() } },
                            blocks:output.clone()
                        })), ..Default::default() }));
                        yield pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete { calls:calls.into_values().collect(), usage });
                        return;
                    }
                    "response.failed" | "error" => {
                        let error = event["response"].get("error").filter(|error| !error.is_null()).or_else(|| event.get("error")).unwrap_or(&Value::Null);
                        let safe_error = pi_rs_core::sanitized_provider_error(error, &error_secrets);
                        let code = safe_error["code"].as_str();
                        Err(if matches!(code, Some("subscription_sharing_usage_limit_exceeded" | "subscription_sharing_user_not_eligible" | "subscription_sharing_usage_unavailable")) {
                            response_error(code, None)
                        } else if pi_rs_core::permanent_provider_error(error, &error_secrets) {
                            Error::Model("OpenAI account quota or billing limit reached".into())
                        } else if pi_rs_core::retryable_provider_error(error, &error_secrets) {
                            Error::RetryableModel("OpenAI returned a transient stream error".into())
                        } else if pi_rs_core::provider_context_overflow(error, &error_secrets) {
                            Error::Model("context window exceeded".into())
                        } else {
                            response_error(code, None)
                        })?;
                    },
                    _ => {}
                }
            }
            if declared_sse || started {
                Err(Error::RetryableModel("OpenAI stream ended without response.completed".into()))?;
            } else {
                Err(Error::Model("invalid OpenAI event stream".into()))?;
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn codex_sse_uses_subscription_headers_payload_and_completion_alias() {
        use std::io::{BufRead, BufReader, Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut socket = BufReader::new(socket);
            let mut line = String::new();
            socket.read_line(&mut line).unwrap();
            assert_eq!(line.trim(), "POST /backend-api/codex/responses HTTP/1.1");
            let mut headers = String::new();
            let mut length = 0;
            loop {
                line.clear();
                socket.read_line(&mut line).unwrap();
                if line.trim().is_empty() {
                    break;
                }
                let line = line.to_ascii_lowercase();
                if let Some(value) = line.strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
                headers.push_str(&line);
            }
            for header in [
                "chatgpt-account-id: fixture-account",
                "openai-beta: responses=experimental",
                "originator: pi",
                "authorization: bearer",
            ] {
                assert!(headers.contains(header), "missing {header}");
            }
            let mut body = vec![0; length];
            socket.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["instructions"], "fixture system");
            assert_eq!(body["input"][0]["role"], "user");
            assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
            assert_eq!(body["reasoning"], json!({"effort":"none"}));
            assert_eq!(body["tool_choice"], "auto");
            assert!(body.get("max_output_tokens").is_none());
            let response = format!(
                "data: {}\n\n",
                json!({"type":"response.done", "response":{"status":"completed", "output":[]}})
            );
            write!(socket.get_mut(),
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len()
            ).unwrap();
        });
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"fixture-account"}}"#);
        let client = OpenAiResponses::new_codex(
            &format!("http://{address}/backend-api/codex/responses"),
            format!("e30.{payload}.fixture"),
            "openai-codex",
        )
        .unwrap();
        let request = ModelRequest {
            model: "gpt-6.1-sol".into(),
            messages: vec![
                Message::System {
                    content: "fixture system".into(),
                },
                Message::User {
                    content: "hello".into(),
                },
            ],
            tools: vec![],
            max_output_tokens: 64,
        };
        let events = client
            .stream(request)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Ok(ModelEvent::Metadata(metadata))
            if metadata.provider_api == Some(pi_rs_core::ModelApi::OpenaiCodexResponses)))
        );
        assert!(matches!(
            events.last(),
            Some(Ok(ModelEvent::Complete { .. }))
        ));
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn interleaved_text_items_have_boundaries_and_legacy_projection() {
        use std::io::{Read, Write};
        for legacy in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let _ = socket.read(&mut [0; 8192]).unwrap();
                let events = vec![
                    json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message"}}),
                    json!({"type":"response.output_text.delta","output_index":0,"delta":"a"}),
                    json!({"type":"response.output_item.added","output_index":1,"item":{"type":"reasoning"}}),
                    json!({"type":"response.reasoning_text.delta","output_index":1,"delta":"r"}),
                    json!({"type":"response.output_item.added","output_index":2,"item":{"type":"message"}}),
                    json!({"type":"response.refusal.delta","output_index":2,"delta":"b"}),
                    json!({"type":"response.output_text.delta","output_index":0,"delta":"c"}),
                    json!({"type":"response.output_item.done","output_index":2,"item":{"type":"message","content":[{"type":"refusal","refusal":"B"}]}}),
                    json!({"type":"response.output_item.done","output_index":1,"item":{"type":"reasoning","content":[{"text":"R"}]}}),
                    json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","content":[{"type":"output_text","text":"AC"}]}}),
                    json!({"type":"response.completed","response":{"status":"completed","output":[]}}),
                ];
                let body: String = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = OpenAiResponses::new_api_provider(
                &format!("http://{address}"),
                "key".into(),
                "fixture",
            )
            .unwrap();
            let request = ModelRequest {
                model: "m".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 16,
            };
            let mut stream: pi_rs_core::EnrichedModelStream = if legacy {
                Box::pin(
                    model
                        .stream(request)
                        .await
                        .unwrap()
                        .map(|event| event.map(pi_rs_core::EnrichedModelEvent::Legacy)),
                )
            } else {
                model.stream_enriched(request).await.unwrap()
            };
            let mut seen = Vec::new();
            while let Some(event) = stream.next().await {
                use pi_rs_core::EnrichedModelEvent::*;
                match event.unwrap() {
                    TextStart { index } => seen.push(format!("start:{index}")),
                    TextDelta { index, delta } => seen.push(format!("delta:{index}:{delta}")),
                    TextEnd { index, content } => seen.push(format!("end:{index}:{content}")),
                    ReasoningStart { index } => seen.push(format!("reasoning-start:{index}")),
                    ReasoningDelta { index, delta } => {
                        seen.push(format!("reasoning-delta:{index}:{delta}"))
                    }
                    ReasoningEnd { index, content } => {
                        seen.push(format!("reasoning-end:{index}:{content}"))
                    }
                    Legacy(ModelEvent::Text(delta)) => seen.push(format!("text:{delta}")),
                    Legacy(ModelEvent::Reasoning(delta)) => seen.push(format!("reasoning:{delta}")),
                    _ => {}
                }
            }
            worker.join().unwrap();
            let expected = if legacy {
                vec!["text:a", "reasoning:r", "text:b", "text:c"]
            } else {
                vec![
                    "start:0",
                    "delta:0:a",
                    "reasoning-start:1",
                    "reasoning-delta:1:r",
                    "start:2",
                    "delta:2:b",
                    "delta:0:c",
                    "end:2:B",
                    "reasoning-end:1:R",
                    "end:0:AC",
                ]
            };
            assert_eq!(seen, expected);
        }
    }

    #[tokio::test]
    async fn streams_reasoning_text_and_retains_it_in_metadata() {
        use std::io::{Read, Write};
        for (mixed, legacy) in [(false, false), (true, false), (false, true)] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let mut events = vec![
                    json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}),
                    json!({"type":"response.reasoning_text.delta","output_index":0,"delta":"Consider "}),
                    json!({"type":"response.reasoning_text.delta","output_index":0,"delta":"this."}),
                ];
                let summary = if mixed {
                    json!([{"text":"Short summary"}])
                } else {
                    json!([])
                };
                if mixed {
                    events.push(json!({"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"Short summary"}));
                    events.push(
                        json!({"type":"response.reasoning_summary_part.done","output_index":0}),
                    );
                }
                let item = json!({"type":"reasoning","id":"rs_1","summary":summary,"content":[{"type":"reasoning_text","text":"Consider this."}]});
                events
                    .push(json!({"type":"response.output_item.done","output_index":0,"item":item}));
                events.push(json!({"type":"response.completed","response":{"status":"completed","output":[item]}}));
                let body: String = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = OpenAiResponses::new_api_provider(
                &format!("http://{address}"),
                "key".into(),
                "fixture",
            )
            .unwrap();
            let request = ModelRequest {
                model: "m".into(),
                messages: vec![],
                tools: vec![],
                max_output_tokens: 16,
            };
            let mut stream: pi_rs_core::EnrichedModelStream = if legacy {
                Box::pin(
                    model
                        .stream(request)
                        .await
                        .unwrap()
                        .map(|event| event.map(pi_rs_core::EnrichedModelEvent::Legacy)),
                )
            } else {
                model.stream_enriched(request).await.unwrap()
            };
            let mut deltas = Vec::new();
            let mut ended = None;
            let mut reasoning = None;
            let mut complete = false;
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    pi_rs_core::EnrichedModelEvent::ReasoningDelta { delta, .. } => {
                        deltas.push(delta)
                    }
                    pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Reasoning(delta)) => {
                        deltas.push(delta)
                    }
                    pi_rs_core::EnrichedModelEvent::ReasoningEnd { index, content } => {
                        ended = Some((index, content))
                    }
                    pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Metadata(metadata)) => {
                        reasoning = metadata.reasoning
                    }
                    pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete { .. }) => {
                        complete = true
                    }
                    _ => {}
                }
            }
            worker.join().unwrap();
            let expected_deltas = if mixed {
                vec!["Consider ", "this.", "Short summary", "\n\n"]
            } else {
                vec!["Consider ", "this."]
            };
            assert_eq!(deltas, expected_deltas);
            assert_eq!(
                reasoning.as_deref(),
                Some(if mixed {
                    "Short summary"
                } else {
                    "Consider this."
                })
            );
            assert_eq!(
                ended,
                (!legacy).then(|| (
                    0,
                    if mixed {
                        "Short summary"
                    } else {
                        "Consider this."
                    }
                    .to_owned()
                ))
            );
            assert!(complete);
        }
    }

    #[tokio::test]
    async fn http_400_does_not_classify_echoed_credential_as_subscription_code() {
        use std::io::{Read, Write};
        const CODE: &str = "subscription_sharing_usage_unavailable";
        for echo in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let body = json!({"error":{"code":CODE}}).to_string();
                write!(socket, "HTTP/1.1 400 Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = OpenAiResponses::new_api_provider(
                &format!("http://{address}"),
                "ordinary-key".into(),
                "fixture",
            )
            .unwrap()
            .with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "x-api-key",
                    if echo { CODE } else { "other-key" }.parse().unwrap(),
                );
                headers
            });
            let error = match model.stream(request()).await {
                Err(error) => error,
                Ok(_) => panic!("expected error"),
            };
            assert_eq!(matches!(error, Error::RetryableModel(_)), !echo);
            assert!(!error.to_string().contains(CODE));
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn subscription_stream_codes_take_precedence_over_transient_message() {
        use std::io::{Read, Write};
        for (code, retryable) in [
            ("subscription_sharing_usage_limit_exceeded", false),
            ("subscription_sharing_user_not_eligible", false),
            ("subscription_sharing_usage_unavailable", true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let event = json!({"type":"response.failed","response":{"error":{"code":code,"message":"rate limit exceeded"}}});
                let body = format!("data: {event}\n\n");
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = OpenAiResponses::new_api_provider(
                &format!("http://{address}"),
                "token".into(),
                "fixture",
            )
            .unwrap();
            let mut stream = model.stream(request()).await.unwrap();
            let error = loop {
                match stream.next().await {
                    Some(Err(error)) => break error,
                    Some(Ok(_)) => {}
                    None => panic!("missing error"),
                }
            };
            assert_eq!(
                matches!(error, Error::RetryableModel(_)),
                retryable,
                "{code}"
            );
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn auth_header_error_literals_are_not_classified() {
        use std::io::{Read, Write};
        for (name, value) in [("x-api-key", "quota exceeded"), ("x-auth-token", "timeout")] {
            for streaming in [false, true] {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let address = listener.local_addr().unwrap();
                let event = json!({"type":"response.failed","response":{"error":{"code":"error","message":value}}});
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
                let model = OpenAiResponses::new_api_provider(
                    &format!("http://{address}"),
                    "ordinary-key".into(),
                    "fixture",
                )
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
            ("billing limit reached", true, false),
            ("rate limit exceeded sk-billing-timeout-secret", false, true),
            (
                "request to https://example.invalid failed, reason: unexpected",
                false,
                true,
            ),
            ("rate---limit", false, false),
            ("rate:limit sk-billing-timeout-secret", false, true),
            (
                "http/2 stream failed sk-billing-timeout-secret",
                false,
                true,
            ),
            ("service-is-unavailable", false, true),
            ("sk-header-billing-timeout-secret", false, false),
            ("sk-billing-timeout-secret", false, false),
            ("sk-timeout-secret", false, false),
            ("timeout sk-billing-timeout-secret", false, true),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let event = json!({"type":"response.failed","response":{"error":{"code":"error","message":message,"request_id":"billing-sentinel-secret"}}});
            let worker = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut request = [0; 8192];
                let _ = socket.read(&mut request).unwrap();
                let body = format!("data: {event}\n\n");
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            });
            let model = OpenAiResponses::new_api_provider(
                &format!("http://{address}"),
                "sk-billing-timeout-secret".into(),
                "fixture",
            )
            .unwrap()
            .with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-tenant", "billing".parse().unwrap());
                headers.insert("x-policy", "quota".parse().unwrap());
                headers.insert("x-plan", "quota exceeded".parse().unwrap());
                headers.insert("x-routing", "timeout".parse().unwrap());
                headers.insert("x-secret", "sk-timeout-secret".parse().unwrap());
                headers.insert(
                    "x-bearer",
                    "Bearer  sk-header-billing-timeout-secret".parse().unwrap(),
                );
                headers.insert("x-service", "service-is-unavailable".parse().unwrap());
                headers.insert("x-rate", "rate:".parse().unwrap());
                headers.insert("x-proto", "http/2".parse().unwrap());
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
                503,
                r#"{"error":{"message":"billing limit reached"}}"#,
                true,
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
            let model = OpenAiResponses::new_api_provider(
                &format!("http://{address}"),
                credential.into(),
                "fixture",
            )
            .unwrap()
            .with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("x-tenant", "billing".parse().unwrap());
                headers.insert("x-policy", "quota".parse().unwrap());
                headers.insert("x-plan", "quota exceeded".parse().unwrap());
                headers
            });
            let model = model.with_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    "x-bearer",
                    "Bearer  sk-header-billing-timeout-secret".parse().unwrap(),
                );
                headers.insert("x-service", "service-is-unavailable".parse().unwrap());
                headers
            });
            let result = model.stream(request()).await;
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
    fn edited_reasoning_is_endpoint_scoped_for_payload_and_budget() {
        let a = OpenAiResponses::new_api_provider(
            "https://one.example/v1/responses",
            "token".into(),
            "shared",
        )
        .unwrap();
        let b = OpenAiResponses::new_api_provider(
            "https://two.example/v1/responses",
            "token".into(),
            "shared",
        )
        .unwrap();
        let scope_a = pi_rs_core::endpoint_replay_scope("shared", a.endpoint.as_str());
        let scope_b = pi_rs_core::endpoint_replay_scope("shared", b.endpoint.as_str());
        let assistant = Message::Assistant {
            content: "answer".into(),
            calls: vec![],
            replay: Some(Box::new(ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: scope_a.clone(),
                    model: "m".into(),
                },
                blocks: vec![
                    json!({"type":"reasoning","encrypted_content":"x".repeat(5000)}),
                    json!({"role":"assistant","content":"answer"}),
                    json!({"_pi_context_edit_api":"openai-responses"}),
                ],
            })),
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![assistant.clone()],
            tools: vec![],
            max_output_tokens: 16,
        };
        assert_eq!(
            payload(&request, "shared", &scope_a, "developer")["input"][0]["type"],
            "reasoning"
        );
        assert!(
            !payload(&request, "shared", &scope_b, "developer")
                .to_string()
                .contains("encrypted_content")
        );
        assert!(a.estimate_message_tokens(&request, &assistant) > 1000);
        assert_eq!(
            b.estimate_message_tokens(&request, &assistant),
            pi_rs_core::estimate_message_tokens(&assistant)
        );
    }

    #[test]
    fn capacity_estimate_matches_image_and_replay_serialization() {
        let model = OpenAiResponses::new_api("https://example.com", "key".into()).unwrap();
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
            identity: ResolvedModelIdentity {
                provider: "openai".into(),
                model: "fixture".into(),
            },
            blocks: vec![
                json!({"type":"message","content":"answer"}),
                json!({"type":"reasoning","encrypted_content":"x".repeat(5000)}),
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
    use futures_util::TryStreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn request() -> ModelRequest {
        ModelRequest {
            model: "fixture".into(),
            messages: vec![
                Message::System {
                    content: "instructions".into(),
                },
                Message::User {
                    content: "hello".into(),
                },
            ],
            tools: vec![pi_rs_core::ToolSpec {
                name: "read".into(),
                description: "read a file".into(),
                parameters: json!({"type":"object"}),
            }],
            max_output_tokens: 100,
        }
    }

    #[test]
    fn constrained_responses_tools_match_capability_and_policy() {
        let mut req = request();
        req.tools[0].parameters = json!({"type":"object","properties":{"path":{"type":"string"}}});
        let mut declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Prefer,
            }),
        };
        let strict = constrained_tools(&req, std::slice::from_ref(&declaration), true).unwrap();
        assert_eq!(strict[0]["strict"], true);
        assert_eq!(strict[0]["parameters"]["required"], json!(["path"]));
        assert_eq!(
            strict[0]["parameters"]["properties"]["path"]["anyOf"][1],
            json!({"type":"null"})
        );
        let fallback = constrained_tools(&req, std::slice::from_ref(&declaration), false).unwrap();
        assert!(fallback[0].get("strict").is_none());
        assert_eq!(fallback[0]["parameters"], req.tools[0].parameters);
        declaration.constrained_sampling = Some(pi_rs_core::ToolConstraint::JsonSchema {
            strict: pi_rs_core::StrictToolPolicy::Require,
        });
        assert!(constrained_tools(&req, std::slice::from_ref(&declaration), false).is_err());
        declaration.spec.name = "other".into();
        assert!(constrained_tools(&req, &[declaration], true).is_err());
    }

    #[test]
    fn prepared_request_exposes_actual_strict_and_fallback_decisions() {
        let req = request();
        let mut declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Prefer,
            }),
        };
        let model =
            OpenAiResponses::new_api("https://example.test/responses", "fixture".into()).unwrap();
        let fallback = model
            .prepare_tool_request(req.clone(), std::slice::from_ref(&declaration))
            .unwrap();
        assert!(matches!(
            fallback.effective_tools()[0].sampling,
            pi_rs_core::EffectiveToolSampling::Unconstrained
        ));
        let supported = model.with_strict_tool_support(true);
        let prepared = supported
            .prepare_tool_request(req.clone(), std::slice::from_ref(&declaration))
            .unwrap();
        assert!(
            matches!(&prepared.effective_tools()[0].sampling, pi_rs_core::EffectiveToolSampling::StrictJsonSchema { original_schema, normalization_version:1 } if original_schema == &req.tools[0].parameters)
        );
        declaration.spec.name = "changed".into();
        assert!(supported.prepare_tool_request(req, &[declaration]).is_err());
    }

    #[test]
    fn official_responses_explicitly_disables_implicit_strict_sampling() {
        let model = OpenAiResponses::new_api(DEFAULT_ENDPOINT, "fixture".into()).unwrap();
        let req = request();
        let declarations = [pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Prefer,
            }),
        }];
        let prepared = model
            .prepare_tool_request(req.clone(), &declarations)
            .unwrap();
        assert_eq!(prepared.wire_tools[0]["strict"], false);
        assert_eq!(
            prepared.wire_tools[0]["parameters"],
            req.tools[0].parameters
        );
        let stream_tools = model
            .stream_only_constrained_tools(&req, &declarations, false)
            .unwrap();
        assert_eq!(stream_tools, prepared.wire_tools);
    }

    #[tokio::test]
    async fn prepared_fallback_session_sends_the_reported_unconstrained_tool() {
        assert_prepared_fallback_session(false).await;
        assert_prepared_fallback_session(true).await;
    }

    async fn assert_prepared_fallback_session(plan: bool) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(10))
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let end = loop {
                let mut block = [0; 4096];
                let count = socket.read(&mut block).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&block[..count]);
                if let Some(position) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    break position + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while bytes.len() < end + length {
                let mut block = [0; 4096];
                let count = socket.read(&mut block).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&block[..count]);
            }
            let body: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            socket
                .write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            (headers, body)
        });
        let req = request();
        let declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Prefer,
            }),
        };
        let endpoint = format!("http://{address}/responses");
        let model = if plan {
            OpenAiResponses::new(&endpoint, "fixture".into()).unwrap()
        } else {
            OpenAiResponses::new_api(&endpoint, "fixture".into()).unwrap()
        };
        let mut context = pi_rs_core::ToolDispatchContext {
            request_id: uuid::Uuid::nil(),
            session_id: uuid::Uuid::nil(),
            external_id: "prepared-session".into(),
            summary: false,
        };
        let prepared = (&model as &dyn Model)
            .prepare_tool_dispatch(&req, &[declaration], &context)
            .await
            .unwrap()
            .unwrap();
        context.external_id = "changed-after-preparation".into();
        assert_eq!(context.external_id, "changed-after-preparation");
        assert!(matches!(
            prepared.effective_tools()[0].sampling,
            pi_rs_core::EffectiveToolSampling::Unconstrained
        ));
        let _ = prepared.dispatch().await;
        let (headers, body) = server.join().unwrap();
        let tool = if plan {
            let tool = &body["tools"][0]["tools"][0];
            assert_eq!(tool["strict"], false);
            tool
        } else {
            assert!(
                headers
                    .lines()
                    .any(|line| line == "session-id: prepared-session")
            );
            let tool = &body["tools"][0];
            assert!(tool.get("strict").is_none());
            tool
        };
        assert_eq!(body["prompt_cache_key"], "prepared-session");
        assert_eq!(tool["parameters"], req.tools[0].parameters);
    }

    #[tokio::test]
    async fn responses_stream_only_rejects_strict_or_unsupported_before_dispatch() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}/responses", listener.local_addr().unwrap());
        for (support, policy) in [
            (true, pi_rs_core::StrictToolPolicy::Require),
            (true, pi_rs_core::StrictToolPolicy::Prefer),
            (false, pi_rs_core::StrictToolPolicy::Require),
        ] {
            let model = OpenAiResponses::new_api(&endpoint, "fixture".into())
                .unwrap()
                .with_strict_tool_support(support);
            let req = request();
            let declaration = pi_rs_core::ToolDeclaration {
                spec: req.tools[0].clone(),
                constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                    strict: policy,
                }),
            };
            let declarations = std::slice::from_ref(&declaration);
            assert!(matches!(
                model
                    .stream_detailed_with_tool_declarations(
                        uuid::Uuid::nil(),
                        uuid::Uuid::nil(),
                        "session",
                        req.clone(),
                        declarations
                    )
                    .await,
                Err(Error::Config(_))
            ));
            assert!(matches!(
                model
                    .stream_detailed_summary_with_tool_declarations(
                        uuid::Uuid::nil(),
                        uuid::Uuid::nil(),
                        req.clone(),
                        declarations
                    )
                    .await,
                Err(Error::Config(_))
            ));
            assert!(matches!(
                model
                    .stream_with_tool_declarations(req, declarations, support)
                    .await,
                Err(Error::Config(_))
            ));
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn declaration_stream_defaults_to_unsupported_and_changes_identity_when_enabled() {
        let model =
            OpenAiResponses::new_api("https://example.test/responses", "fixture".into()).unwrap();
        let req = request();
        let declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Require,
            }),
        };
        assert!(
            model
                .stream_detailed_with_tool_declarations(
                    uuid::Uuid::nil(),
                    uuid::Uuid::nil(),
                    "s",
                    req.clone(),
                    std::slice::from_ref(&declaration)
                )
                .await
                .is_err()
        );
        assert!(
            model
                .stream_detailed_summary_with_tool_declarations(
                    uuid::Uuid::nil(),
                    uuid::Uuid::nil(),
                    req,
                    &[declaration]
                )
                .await
                .is_err()
        );
        assert_ne!(
            model.context_identity(),
            model
                .clone()
                .with_strict_tool_support(true)
                .context_identity()
        );
    }

    #[tokio::test]
    async fn constrained_summary_sends_strict_tools_without_session_cache() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(10))
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let end = loop {
                let mut block = [0; 4096];
                let count = socket.read(&mut block).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&block[..count]);
                if let Some(position) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    break position + 4;
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
                let mut block = [0; 4096];
                let count = socket.read(&mut block).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&block[..count]);
            }
            let body: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            socket
                .write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            body
        });
        let mut req = request();
        req.messages = vec![Message::System {
            content: "rules".into(),
        }];
        let declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Require,
            }),
        };
        let model =
            OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                .unwrap()
                .with_strict_tool_support(true)
                .with_developer_role(true)
                .with_session_cache_key("prior-session");
        let prepared = model.prepare_tool_request(req, &[declaration]).unwrap();
        assert!(matches!(
            prepared.effective_tools()[0].sampling,
            pi_rs_core::EffectiveToolSampling::StrictJsonSchema { .. }
        ));
        let _ = prepared.stream_for_summary().await;
        let body = server.join().unwrap();
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["input"][0]["role"], "developer");
        assert!(body.get("prompt_cache_key").is_none());
    }

    #[tokio::test]
    async fn sdk_prepared_responses_sends_strict_schema() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(10))
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut data = Vec::new();
            let end = loop {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                data.extend_from_slice(&chunk[..count]);
                if let Some(pos) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8_lossy(&data[..end]);
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while data.len() < end + length {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                data.extend_from_slice(&chunk[..count]);
            }
            let body: Value = serde_json::from_slice(&data[end..end + length]).unwrap();
            let final_arguments = r#"{"path":null}"#;
            let events = [
                json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"read","arguments":"{\"path\":"}}),
                json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":final_arguments}),
                json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"read","arguments":final_arguments}}),
                json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","call_id":"c1","name":"read","arguments":final_arguments}]}}),
            ];
            let events = events
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect::<String>();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
            body
        });
        let mut req = request();
        req.messages = vec![Message::System {
            content: "rules".into(),
        }];
        req.tools[0].parameters = json!({"type":"object","properties":{"path":{"type":"string"}}});
        let declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Require,
            }),
        };
        let model =
            OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                .unwrap()
                .with_strict_tool_support(true);
        let context = pi_rs_core::ToolDispatchContext {
            request_id: uuid::Uuid::new_v4(),
            session_id: uuid::Uuid::new_v4(),
            external_id: "strict-session".into(),
            summary: false,
        };
        let prepared = model
            .prepare_bound_tool_dispatch(&req, std::slice::from_ref(&declaration), &context)
            .unwrap();
        let original = match &prepared.effective_tools()[0].sampling {
            pi_rs_core::EffectiveToolSampling::StrictJsonSchema {
                original_schema, ..
            } => original_schema.clone(),
            _ => panic!("expected strict decision"),
        };
        let mut stream = prepared.dispatch().await.unwrap();
        let mut calls = None;
        while let Some(event) = stream.next().await {
            if let pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete {
                calls: completed,
                ..
            }) = event.unwrap().event
            {
                calls = Some(completed);
            }
        }
        let raw = calls.unwrap()[0].arguments.clone();
        assert_eq!(raw, r#"{"path":null}"#);
        let args: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            pi_rs_core::normalize_strict_tool_arguments(&original, &args).unwrap(),
            json!({})
        );
        assert_eq!(args, json!({"path":null}));
        let body = server.join().unwrap();
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["tools"][0]["parameters"]["required"], json!(["path"]));
        assert_eq!(body["prompt_cache_key"], "strict-session");
    }

    #[tokio::test]
    async fn responses_preferred_fallback_stream_keeps_raw_arguments() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(std::time::Duration::from_millis(10))
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut data = Vec::new();
            let end = loop {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                data.extend_from_slice(&chunk[..count]);
                if let Some(pos) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8_lossy(&data[..end]);
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while data.len() < end + length {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0);
                data.extend_from_slice(&chunk[..count]);
            }
            let body: Value = serde_json::from_slice(&data[end..end + length]).unwrap();
            let final_arguments = r#"{"path":null}"#;
            let events = [
                json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"read","arguments":"{\"path\":"}}),
                json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":final_arguments}),
                json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"read","arguments":final_arguments}}),
                json!({"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","call_id":"c1","name":"read","arguments":final_arguments}]}}),
            ];
            let events = events
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect::<String>();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
            body
        });
        let mut req = request();
        req.messages = vec![Message::System {
            content: "rules".into(),
        }];
        req.tools[0].parameters = json!({"type":"object","properties":{"path":{"type":"string"}}});
        let declaration = pi_rs_core::ToolDeclaration {
            spec: req.tools[0].clone(),
            constrained_sampling: Some(pi_rs_core::ToolConstraint::JsonSchema {
                strict: pi_rs_core::StrictToolPolicy::Prefer,
            }),
        };
        let model =
            OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                .unwrap()
                .with_strict_tool_support(false);
        let original = req.tools[0].parameters.clone();
        let mut stream = model
            .stream_detailed_with_tool_declarations(
                uuid::Uuid::nil(),
                uuid::Uuid::nil(),
                "strict-session",
                req,
                &[declaration],
            )
            .await
            .unwrap();
        let mut calls = None;
        while let Some(event) = stream.next().await {
            if let pi_rs_core::EnrichedModelEvent::Legacy(ModelEvent::Complete {
                calls: completed,
                ..
            }) = event.unwrap().event
            {
                calls = Some(completed);
            }
        }
        let raw = calls.unwrap()[0].arguments.clone();
        assert_eq!(raw, r#"{"path":null}"#);
        let body = server.join().unwrap();
        assert!(body["tools"][0].get("strict").is_none());
        assert_eq!(body["tools"][0]["parameters"], original);
        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["prompt_cache_key"], "strict-session");
    }

    #[tokio::test]
    async fn final_tool_arguments_can_arrive_on_item_or_completion() {
        use std::io::{Read, Write};
        for item_done in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let _ = socket.read(&mut [0; 8192]).unwrap();
                let final_arguments = r#"{"path":"hello"}"#;
                let item = json!({"type":"function_call","call_id":"c1","name":"read","arguments":final_arguments});
                let mut events = vec![
                    json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"read","arguments":"{\"path\":"}}),
                ];
                if item_done {
                    events.push(
                        json!({"type":"response.output_item.done","output_index":0,"item":item}),
                    );
                }
                events.push(json!({"type":"response.completed","response":{"status":"completed","output":[item]}}));
                let events = events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect::<String>();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{events}", events.len()).unwrap();
            });
            let model =
                OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                    .unwrap();
            let mut stream = model.stream(request()).await.unwrap();
            let mut emitted = String::new();
            let mut completed = None;
            while let Some(event) = stream.next().await {
                match event.unwrap() {
                    ModelEvent::ToolCallDelta(delta) => emitted.push_str(&delta.arguments),
                    ModelEvent::Complete { calls, .. } => completed = Some(calls),
                    _ => {}
                }
            }
            assert_eq!(emitted, r#"{"path":"hello"}"#);
            assert_eq!(completed.unwrap()[0].arguments, emitted);
            server.join().unwrap();
        }
    }

    #[test]
    fn sampling_rejects_responses_fields_managed_by_the_agent() {
        let client =
            OpenAiResponses::new_api("https://example.com/v1/responses", "token".into()).unwrap();
        for key in [
            "model",
            "input",
            "messages",
            "instructions",
            "previous_response_id",
            "conversation",
            "prompt",
            "tools",
            "functions",
            "function_call",
            "stream",
            "stream_options",
            "background",
            "store",
            "max_output_tokens",
            "max_tokens",
            "max_completion_tokens",
            "n",
            "reasoning",
            "reasoning_effort",
            "include",
        ] {
            let error = match client
                .clone()
                .with_sampling(BTreeMap::from([(key.into(), json!(null))]))
            {
                Ok(_) => panic!("accepted managed field {key}"),
                Err(error) => error,
            };
            assert!(error.to_string().contains(key));
        }
        assert!(
            client
                .with_sampling(BTreeMap::from([
                    ("temperature".into(), json!(0.5)),
                    ("top_p".into(), json!(0.9)),
                    ("custom_sampling".into(), json!({"value": 1})),
                    ("parallel_tool_calls".into(), json!(false)),
                    ("max_tool_calls".into(), json!(3)),
                    ("tool_choice".into(), json!("required")),
                ]))
                .is_ok()
        );
    }

    #[test]
    fn plan_payload_uses_stateless_stream_and_namespaced_tools() {
        let body = payload(&request(), "openai-chatgpt", "openai-chatgpt", "developer");
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["input"][0]["role"], "developer");
        assert_eq!(body["tools"][0]["type"], "namespace");
        assert_eq!(body["tools"][0]["tools"][0]["name"], "read");
        assert_eq!(body["max_output_tokens"], 100);
        assert!(OpenAiResponses::new("http://example.com/responses", "token".into()).is_err());
    }

    #[test]
    fn api_key_payload_uses_flat_functions_and_preserves_provider_identity() {
        let mut request = request();
        request.messages.push(Message::Assistant {
            content: String::new(),
            calls: vec![ToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
            replay: None,
        });
        let body = payload(&request, "openai", "openai", "developer");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["name"], "read");
        assert!(body["input"][2].get("namespace").is_none());
        assert!(
            OpenAiResponses::new_api("https://example.com/v1/responses", "token".into()).is_ok()
        );
        assert!(OpenAiResponses::new("https://example.com/v1/responses", "token".into()).is_err());
    }

    #[test]
    fn tool_images_are_sent_as_images_not_base64_text() {
        let mut request = request();
        request.messages.push(Message::Tool { is_structured: Some(true), is_error: false, call_id:"call".into(), content:json!({"data":{"content":[{"type":"text","text":"picture"},{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}]}}).to_string() });
        let body = payload(&request, "openai-chatgpt", "openai-chatgpt", "developer");
        let output = &body["input"][2]["output"];
        assert_eq!(output[0]["type"], "input_text");
        assert!(!output[0]["text"].as_str().unwrap().contains("aW1hZ2U="));
        assert_eq!(
            output[1],
            json!({"type":"input_image","image_url":"data:image/png;base64,aW1hZ2U="})
        );
    }

    #[test]
    fn plain_json_tool_output_is_never_extracted_as_an_image() {
        let text = r#"{ "data": { "content": [{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}] } }"#;
        let mut request = request();
        request.messages.push(Message::Tool {
            call_id: "call".into(),
            content: text.into(),
            is_error: false,
            is_structured: Some(false),
        });
        assert_eq!(
            payload(&request, "openai", "openai", "developer")["input"][2]["output"],
            text
        );
        request.messages[2] =
            serde_json::from_value(json!({"role":"tool","call_id":"call","content":text})).unwrap();
        let output = &payload(&request, "openai", "openai", "developer")["input"][2]["output"];
        assert_eq!(output[1]["type"], "input_image");
        assert_eq!(output[1]["image_url"], "data:image/png;base64,aW1hZ2U=");
    }

    #[test]
    fn responses_cache_modes_follow_model_capabilities() {
        let client =
            OpenAiResponses::new_api("https://example.com/v1/responses", "token".into()).unwrap();
        for (retention, explicit, expected) in [
            (CacheRetention::Short, false, json!({})),
            (CacheRetention::Short, true, json!({})),
            (
                CacheRetention::Long,
                false,
                json!({"prompt_cache_retention":"24h"}),
            ),
            (
                CacheRetention::Long,
                true,
                json!({"prompt_cache_options":{"ttl":"30m"}}),
            ),
            (
                CacheRetention::None,
                true,
                json!({"prompt_cache_options":{"mode":"explicit"}}),
            ),
        ] {
            let mut body = json!({});
            client
                .clone()
                .with_cache_retention(retention, explicit)
                .apply_cache(&mut body);
            assert_eq!(body, expected);
        }
        let mut body = json!({"prompt_cache_retention":"user"});
        client
            .clone()
            .with_cache_retention(CacheRetention::Long, false)
            .apply_cache(&mut body);
        assert_eq!(body["prompt_cache_retention"], "user");
        let mut body = json!({"prompt_cache_retention":"24h"});
        client
            .clone()
            .with_cache_retention(CacheRetention::Long, true)
            .apply_cache(&mut body);
        assert_eq!(body, json!({"prompt_cache_retention":"24h"}));
        let mut body = json!({"prompt_cache_options":{"mode":"explicit"}});
        client
            .clone()
            .with_cache_retention(CacheRetention::Long, false)
            .apply_cache(&mut body);
        assert_eq!(body, json!({"prompt_cache_options":{"mode":"explicit"}}));
        assert!(
            validate_sampling(&BTreeMap::from([
                ("prompt_cache_retention".into(), json!("24h")),
                ("prompt_cache_options".into(), json!({"ttl":"30m"})),
            ]))
            .is_err()
        );
        let mut body = json!({"prompt_cache_key":"key", "prompt_cache_retention":"24h", "prompt_cache_options":{"ttl":"30m"}});
        client
            .clone()
            .with_cache_retention(CacheRetention::Long, true)
            .without_cache()
            .apply_cache(&mut body);
        assert_eq!(body, json!({"prompt_cache_options":{"mode":"explicit"}}));
        let plan = OpenAiResponses::new("http://localhost/responses", "token".into()).unwrap();
        let mut body = json!({});
        plan.with_cache_retention(CacheRetention::Long, true)
            .apply_cache(&mut body);
        assert_eq!(body, json!({}));
        let plan = OpenAiResponses::new("http://localhost/responses", "token".into()).unwrap();
        let mut body = json!({"prompt_cache_key":"key", "prompt_cache_retention":"24h", "prompt_cache_options":{"ttl":"30m"}});
        plan.without_cache().apply_cache(&mut body);
        assert_eq!(body, json!({}));
    }

    #[tokio::test]
    async fn sdk_instruction_role_defaults_to_system_and_can_enable_developer() {
        use std::io::{Read, Write};
        for (developer, expected) in [
            (None, "system"),
            (Some(true), "developer"),
            (Some(false), "system"),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && std::time::Instant::now() < deadline =>
                        {
                            std::thread::sleep(std::time::Duration::from_millis(10))
                        }
                        Err(e) => panic!("fixture accept failed: {e}"),
                    }
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut block = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let size: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while bytes.len() < end + size {
                    let n = socket.read(&mut block).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&block[..n]);
                }
                let body: Value = serde_json::from_slice(&bytes[end..end + size]).unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                body
            });
            let model =
                OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                    .unwrap();
            let model = if let Some(enabled) = developer {
                model.with_developer_role(enabled)
            } else {
                model
            };
            let mut req = request();
            req.messages = vec![Message::System {
                content: "rules".into(),
            }];
            let _ = model.stream(req).await;
            assert_eq!(server.join().unwrap()["input"][0]["role"], expected);
        }
    }

    #[test]
    fn instruction_role_is_applied_and_changes_recovery_identity() {
        let model =
            OpenAiResponses::new_api("http://localhost/responses", "fixture".into()).unwrap();
        assert_ne!(
            model.context_identity(),
            model.clone().with_developer_role(true).context_identity()
        );
        assert_eq!(
            model.context_identity(),
            model
                .clone()
                .with_developer_role(true)
                .with_developer_role(false)
                .context_identity()
        );
        let mut req = request();
        req.messages = vec![
            Message::System {
                content: "rules".into(),
            },
            Message::User {
                content: "hello".into(),
            },
        ];
        let body = payload(&req, "openai", "openai", "system");
        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["input"][1]["role"], "user");
    }

    #[test]
    fn sdk_session_routing_formats_preserve_explicit_headers() {
        let model = OpenAiResponses::new_api_provider(
            "https://openrouter.ai/api/v1/responses",
            "fixture".into(),
            "openrouter",
        )
        .unwrap();
        let router = model.clone().with_session_routing("session-one").unwrap();
        assert_eq!(router.headers["x-session-id"], "session-one");
        let openai = model
            .clone()
            .with_session_affinity_format(SessionAffinityFormat::OpenAi)
            .with_session_routing("session-one")
            .unwrap();
        assert_eq!(openai.headers["session-id"], "session-one");
        assert_eq!(openai.headers["x-client-request-id"], "session-one");
        let nosession = model
            .clone()
            .with_session_affinity_format(SessionAffinityFormat::OpenAiNoSession)
            .with_session_routing("session-one")
            .unwrap();
        assert!(!nosession.headers.contains_key("session-id"));
        assert_eq!(nosession.headers["x-client-request-id"], "session-one");
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-session-id", "explicit".parse().unwrap());
        let overridden = model
            .clone()
            .with_headers(headers)
            .with_session_routing("日本")
            .unwrap();
        assert_eq!(overridden.headers["x-session-id"], "explicit");
        assert!(model.clone().with_session_routing("日本").is_err());
        let disabled_cache = model
            .clone()
            .without_cache()
            .with_session_routing("日本")
            .unwrap();
        assert!(disabled_cache.headers.is_empty());
        let subdomain = OpenAiResponses::new_api_provider(
            "https://edge.openrouter.ai/v1/responses",
            "fixture".into(),
            "proxy",
        )
        .unwrap()
        .with_session_routing("id")
        .unwrap();
        assert_eq!(subdomain.headers["x-session-id"], "id");
        let uppercase = OpenAiResponses::new_api_provider(
            "https://OPENROUTER.AI/v1/responses",
            "fixture".into(),
            "proxy",
        )
        .unwrap()
        .with_session_routing("id")
        .unwrap();
        assert_eq!(uppercase.headers["session-id"], "id");
        let path = OpenAiResponses::new_api_provider(
            "https://example.com/openrouter.ai/../v1/responses",
            "fixture".into(),
            "proxy",
        )
        .unwrap()
        .with_session_routing("id")
        .unwrap();
        assert_eq!(path.headers["x-session-id"], "id");
        assert!(
            model
                .clone()
                .with_session_routing("")
                .unwrap()
                .headers
                .is_empty()
        );
        assert!(
            model
                .with_session_affinity_format(SessionAffinityFormat::Disabled)
                .with_session_routing("日本")
                .unwrap()
                .headers
                .is_empty()
        );
    }

    #[tokio::test]
    async fn sdk_session_routing_sends_headers_and_summary_omits_them() {
        use std::io::{Read, Write};
        for (summary, enriched) in [(false, false), (true, false), (false, true), (true, true)] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut data = Vec::new();
                while !data.windows(4).any(|part| part == b"\r\n\r\n") {
                    let mut chunk = [0; 2048];
                    let n = socket.read(&mut chunk).unwrap();
                    assert!(n > 0);
                    data.extend_from_slice(&chunk[..n]);
                }
                socket
                    .write_all(
                        b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                String::from_utf8(data).unwrap().to_ascii_lowercase()
            });
            let model =
                OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                    .unwrap();
            match (summary, enriched) {
                (true, true) => {
                    let _ = model
                        .stream_enriched_for_summary(
                            uuid::Uuid::new_v4(),
                            uuid::Uuid::new_v4(),
                            request(),
                        )
                        .await;
                }
                (true, false) => {
                    let _ = model
                        .stream_for_summary(uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), request())
                        .await;
                }
                (false, true) => {
                    let _ = model
                        .stream_enriched_in_session_with_identity(
                            uuid::Uuid::new_v4(),
                            uuid::Uuid::new_v4(),
                            "session-one",
                            request(),
                        )
                        .await;
                }
                (false, false) => {
                    let _ = model
                        .stream_in_session_with_identity(
                            uuid::Uuid::new_v4(),
                            uuid::Uuid::new_v4(),
                            "session-one",
                            request(),
                        )
                        .await;
                }
            }
            let sent = server.join().unwrap();
            assert_eq!(sent.contains("session-id: session-one\r\n"), !summary);
            assert_eq!(
                sent.contains("x-client-request-id: session-one\r\n"),
                !summary
            );
        }
    }

    #[test]
    fn session_cache_keys_are_clamped_and_respect_explicit_keys() {
        let model =
            OpenAiResponses::new_api("http://localhost/responses", "fixture".into()).unwrap();
        let identity = format!("{}tail", "猫".repeat(64));
        assert_ne!(
            model.context_identity(),
            model
                .clone()
                .with_prompt_cache_key_enabled(false)
                .context_identity()
        );
        assert_eq!(
            model.context_identity(),
            model
                .clone()
                .with_prompt_cache_key_enabled(false)
                .with_prompt_cache_key_enabled(true)
                .context_identity()
        );
        let mut body = json!({});
        model
            .clone()
            .with_session_cache_key(&identity)
            .apply_cache(&mut body);
        assert_eq!(body["prompt_cache_key"], "猫".repeat(64));
        let mut body = json!({"prompt_cache_key":"configured"});
        model
            .clone()
            .with_session_cache_key(&identity)
            .apply_cache(&mut body);
        assert_eq!(body["prompt_cache_key"], "configured");
        let mut body = json!({});
        model
            .clone()
            .with_prompt_cache_key_enabled(false)
            .with_session_cache_key(&identity)
            .apply_cache(&mut body);
        assert!(body.get("prompt_cache_key").is_none());
        let mut body = json!({"prompt_cache_key":"explicit"});
        model
            .clone()
            .with_prompt_cache_key_enabled(false)
            .with_session_cache_key(&identity)
            .apply_cache(&mut body);
        assert_eq!(body["prompt_cache_key"], "explicit");
        let mut body = json!({});
        model
            .clone()
            .with_session_cache_key("")
            .apply_cache(&mut body);
        assert!(body.get("prompt_cache_key").is_none());
        let mut body = json!({});
        model
            .with_session_cache_key(&identity)
            .without_cache()
            .apply_cache(&mut body);
        assert!(body.get("prompt_cache_key").is_none());
    }

    #[tokio::test]
    async fn disabled_prompt_cache_key_is_omitted_from_sdk_session_request() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut block = [0; 4096];
            let end = loop {
                let n = socket.read(&mut block).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&block[..n]);
                if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..end]);
            let size: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while bytes.len() < end + size {
                let n = socket.read(&mut block).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&block[..n]);
            }
            let body: Value = serde_json::from_slice(&bytes[end..end + size]).unwrap();
            socket
                .write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            body
        });
        let model =
            OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                .unwrap()
                .with_prompt_cache_key_enabled(false);
        let _ = model
            .stream_enriched_in_session_with_identity(
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4(),
                "external-session",
                request(),
            )
            .await;
        assert!(server.join().unwrap().get("prompt_cache_key").is_none());
    }

    #[tokio::test]
    async fn configured_cache_policy_is_sent_without_an_automatic_alternative() {
        use std::io::{Read, Write};
        for (explicit, key, value) in [
            (true, "prompt_cache_retention", json!("24h")),
            (false, "prompt_cache_options", json!({"mode":"explicit"})),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut buffer).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
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
                    let n = socket.read(&mut buffer).unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                }
                let body: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
                socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                body
            });
            let client =
                OpenAiResponses::new_api(&format!("http://{address}/responses"), "fixture".into())
                    .unwrap()
                    .with_cache_retention(CacheRetention::Long, explicit)
                    .with_sampling(BTreeMap::from([(key.into(), value.clone())]))
                    .unwrap();
            let _ = client.stream(request()).await;
            let body = server.join().unwrap();
            assert_eq!(body[key], value);
            let other = if explicit {
                "prompt_cache_options"
            } else {
                "prompt_cache_retention"
            };
            assert!(body.get(other).is_none());
        }
    }

    async fn fixture(events: &str) -> (OpenAiResponses, tokio::task::JoinHandle<()>) {
        fixture_with_content_type(events, Some("text/event-stream")).await
    }

    async fn fixture_with_content_type(
        events: &str,
        content_type: Option<&str>,
    ) -> (OpenAiResponses, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = OpenAiResponses::new(
            &format!("http://{}/responses", listener.local_addr().unwrap()),
            "fixture".into(),
        )
        .unwrap();
        let body = events.to_owned();
        let content_type = content_type.map(str::to_owned);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let headers = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
            assert!(headers.contains("authorization: bearer fixture"));
            let header =
                content_type.map_or_else(String::new, |value| format!("Content-Type: {value}\r\n"));
            let response = format!(
                "HTTP/1.1 200 OK\r\n{header}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (client, server)
    }

    #[tokio::test]
    async fn provider_response_hook_runs_before_openai_reads_an_error_body() {
        use pi_rs_core::{
            Agent, CancellationToken, Extension, ExtensionResult, ExtensionState, Limits,
            MemoryStore, ProviderResponseEvent, Registry, Session,
        };

        struct Observer(tokio::sync::Notify);
        #[async_trait::async_trait]
        impl Extension for Observer {
            fn id(&self) -> &str {
                "http-observer"
            }
            fn handles_after_provider_response(&self) -> bool {
                true
            }
            async fn after_provider_response(
                &self,
                event: &ProviderResponseEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                assert_eq!(event.status, 401);
                assert_eq!(event.headers["x-request-id"], "request-1");
                self.0.notify_one();
                Ok(())
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model = OpenAiResponses::new_api(
            &format!("http://{}/responses", listener.local_addr().unwrap()),
            "fixture".into(),
        )
        .unwrap();
        let callback = std::sync::Arc::new(Observer(tokio::sync::Notify::new()));
        let mut registry = Registry::default();
        registry.register(callback.clone()).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            let header_end = loop {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
                if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let length: usize = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .unwrap()
                .1
                .trim()
                .parse()
                .unwrap();
            while request.len() < header_end + length {
                let read = socket.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                request.extend_from_slice(&buffer[..read]);
            }
            socket.write_all(b"HTTP/1.1 401 Unauthorized\r\nX-Request-ID: request-1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n").await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), callback.0.notified())
                .await
                .expect("observer must run before the provider releases the body");
            socket.write_all(b"{}").await.unwrap();
        });
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = Session::with_store(MemoryStore::default()).unwrap();
        let (_tx, mut inputs) = tokio::sync::mpsc::channel::<String>(1);
        let error = agent
            .run(
                &mut session,
                Some("question".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |_| {},
            )
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(matches!(error, Error::Model(_)));
    }

    #[tokio::test]
    async fn chatgpt_accepts_missing_content_type_only_for_a_valid_stream() {
        let (client, server) = fixture_with_content_type(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
            None,
        ).await;
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::Text(text) if text == "hello"))
        );
        assert!(matches!(events.last(), Some(ModelEvent::Complete { .. })));
        server.await.unwrap();

        let (client, server) =
            fixture_with_content_type("<html>not an event stream</html>", None).await;
        let error = client
            .stream(request())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Model(message) if message == "invalid OpenAI event stream"));
        server.await.unwrap();

        let (client, server) =
            fixture_with_content_type("data: {\"type\":\"response.created\"}\n\n", None).await;
        let error = client
            .stream(request())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::RetryableModel(message) if message.contains("without response.completed"))
        );
        server.await.unwrap();

        let (client, server) =
            fixture_with_content_type("<html>wrong content type</html>", Some("text/html")).await;
        assert!(client.stream(request()).await.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn empty_declared_sse_remains_retryable_for_all_responses_providers() {
        for provider in ["openai-chatgpt", "openai"] {
            let (mut client, server) =
                fixture_with_content_type("", Some("text/event-stream")).await;
            client.provider = provider.into();
            let error = client
                .stream(request())
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::RetryableModel(message) if message.contains("without response.completed"))
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn headerless_unknown_events_do_not_mark_a_response_started() {
        for kind in ["unrelated", "response.not_a_real_event"] {
            let body = format!("data: {{\"type\":\"{kind}\"}}\n\n");
            let (client, server) = fixture_with_content_type(&body, None).await;
            let error = client
                .stream(request())
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::Model(message) if message == "invalid OpenAI event stream")
            );
            server.await.unwrap();
        }
        let (client, server) = fixture_with_content_type(
            "data: {\"type\":\"unrelated\"}\n\ndata: {\"type\":\"response.created\"}\n\n",
            None,
        )
        .await;
        let error = client
            .stream(request())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::RetryableModel(message) if message.contains("without response.completed"))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn headerless_unterminated_body_is_limited_before_sse_parsing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = OpenAiResponses::new(
            &format!("http://{}/responses", listener.local_addr().unwrap()),
            "fixture".into(),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let mut buffer = [0u8; 4096];
            while !data.windows(4).any(|part| part == b"\r\n\r\n") {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buffer[..n]);
            }
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_STREAM + 65536
            );
            socket.write_all(header.as_bytes()).await.unwrap();
            let block = vec![b'x'; 65536];
            for _ in 0..=(MAX_STREAM / block.len()) {
                if socket.write_all(&block).await.is_err() {
                    break;
                }
            }
        });
        let error = client
            .stream(request())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Model(message) if message == "OpenAI stream exceeds 32 MiB")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn headerless_stream_handles_split_utf8_and_line_endings() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = OpenAiResponses::new(
            &format!("http://{}/responses", listener.local_addr().unwrap()),
            "fixture".into(),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            let mut buffer = [0u8; 4096];
            while !data.windows(4).any(|part| part == b"\r\n\r\n") {
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buffer[..n]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let chunks: &[&[u8]] = &[
                b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"",
                &[0xe6], &[0x97, 0xa5],
                b"\"}\r", b"\n\r", b"\n",
                b"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\r\n\r\n",
            ];
            for chunk in chunks {
                socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(chunk).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                tokio::task::yield_now().await;
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ModelEvent::Text(text) if text == "日"))
        );
        assert!(matches!(events.last(), Some(ModelEvent::Complete { .. })));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn api_key_stream_accepts_flat_tool_calls_and_keeps_its_identity() {
        let events = concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"call\",\"name\":\"read\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{}\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"call_id\":\"call\",\"name\":\"read\",\"arguments\":\"{}\"}]}}\n\n"
        );
        let (mut client, server) = fixture(events).await;
        client.provider = "openai".into();
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(events.iter().any(|event| matches!(event, ModelEvent::Metadata(metadata)
            if metadata.resolved_model.as_ref().is_some_and(|identity| identity.provider == "openai"))));
        assert!(
            matches!(events.last(), Some(ModelEvent::Complete { calls, .. }) if calls[0].name == "read")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn named_api_provider_keeps_its_identity() {
        let (fixture, server) = fixture("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n").await;
        let client =
            OpenAiResponses::new_api_provider(fixture.endpoint.as_str(), "fixture".into(), "xai")
                .unwrap();
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(events.iter().any(|event| matches!(event, ModelEvent::Metadata(metadata)
            if metadata.resolved_model.as_ref().is_some_and(|identity| identity.provider == "xai"))));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn streaming_requires_completed_terminal_event() {
        let (client, server) = fixture("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":4,\"output_tokens\":1}}}\n\n").await;
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(matches!(&events[0], ModelEvent::Text(text) if text == "hi"));
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Complete {
                usage: Some(Usage {
                    input_tokens: 4,
                    output_tokens: 1
                }),
                ..
            })
        ));
        server.await.unwrap();
        let (client, server) =
            fixture("data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
                .await;
        assert!(
            client
                .stream(request())
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn output_limit_keeps_partial_text_and_reports_length() {
        let events = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[],\"usage\":{\"input_tokens\":4,\"output_tokens\":7}}}\n\n"
        );
        let (client, server) = fixture(events).await;
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(matches!(&events[0], ModelEvent::Text(text) if text == "partial"));
        assert!(events.iter().any(|event| matches!(event, ModelEvent::Metadata(metadata) if metadata.finish_reason.as_deref() == Some("length"))));
        assert!(matches!(
            events.last(),
            Some(ModelEvent::Complete {
                usage: Some(Usage {
                    input_tokens: 4,
                    output_tokens: 7
                }),
                ..
            })
        ));
        server.await.unwrap();

        let (client, server) = fixture("data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"content_filter\"},\"output\":[]}}\n\n").await;
        let error = client
            .stream(request())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("content_filter"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn output_limit_preserves_partial_tool_for_safe_rejection() {
        let events = concat!(
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"namespace\":\"pi\",\"call_id\":\"call\",\"name\":\"read\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{\"}\n\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"output\":[{\"type\":\"function_call\",\"namespace\":\"pi\",\"call_id\":\"call\",\"name\":\"read\",\"arguments\":\"{\"}]}}\n\n"
        );
        let (client, server) = fixture(events).await;
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(events.iter().any(|event| matches!(event, ModelEvent::Metadata(metadata) if metadata.finish_reason.as_deref() == Some("length") && metadata.replay.is_none())));
        assert!(
            matches!(events.last(), Some(ModelEvent::Complete { calls, .. }) if calls[0].arguments == "{")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tool_calls_are_only_completed_after_matching_terminal_output() {
        let events = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"namespace\":\"pi\",\"call_id\":\"call\",\"name\":\"read\"}}\n\ndata: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{}\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"namespace\":\"pi\",\"call_id\":\"call\",\"name\":\"read\",\"arguments\":\"{}\"}]}}\n\n";
        let (client, server) = fixture(events).await;
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(
            matches!(events.last(), Some(ModelEvent::Complete { calls, .. }) if calls.len()==1 && calls[0].name=="read")
        );
        server.await.unwrap();
    }

    #[test]
    fn usage_errors_do_not_trigger_generic_retries() {
        assert!(matches!(
            response_error(
                Some("subscription_sharing_usage_limit_exceeded"),
                Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
            ),
            Error::Model(_)
        ));
        assert!(matches!(
            response_error(
                Some("subscription_sharing_usage_unavailable"),
                Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
            ),
            Error::RetryableModel(_)
        ));
        assert!(matches!(
            response_error(
                Some("subscription_sharing_user_not_eligible"),
                Some(reqwest::StatusCode::FORBIDDEN)
            ),
            Error::Model(_)
        ));
    }

    #[tokio::test]
    async fn refusal_text_is_visible() {
        let events = "data: {\"type\":\"response.refusal.delta\",\"delta\":\"Cannot comply\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n";
        let (client, server) = fixture(events).await;
        let events: Vec<_> = client
            .stream(request())
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(matches!(&events[0], ModelEvent::Text(text) if text == "Cannot comply"));
        server.await.unwrap();
    }

    #[test]
    fn known_reasoning_models_only_expose_supported_levels() {
        let levels = thinking_levels("gpt-6.1-sol");
        assert!(levels.contains(&"low".into()));
        assert!(!levels.contains(&"off".into()));
        assert!(!levels.contains(&"minimal".into()));
        assert_eq!(thinking_levels("unknown-model"), vec!["off"]);
    }
}
