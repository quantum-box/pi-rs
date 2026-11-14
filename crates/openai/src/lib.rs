//! OpenAI Responses transport for ChatGPT plan OAuth credentials.
//!
//! Credential acquisition and renewal are the responsibility of the caller.
pub mod oauth;

use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay,
    ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use std::collections::BTreeMap;

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

#[derive(Clone)]
pub struct OpenAiResponses {
    client: Client,
    endpoint: Url,
    token: String,
    thinking: Option<String>,
    provider: String,
    headers: reqwest::header::HeaderMap,
    sampling: BTreeMap<String, Value>,
    identity_safe_transport: bool,
}

impl OpenAiResponses {
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

    fn create(endpoint: &str, token: String, plan: bool) -> Result<Self> {
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
            token,
            thinking: None,
            identity_safe_transport: true,
            provider: if plan { "openai-chatgpt" } else { "openai" }.into(),
            headers: reqwest::header::HeaderMap::new(),
            sampling: BTreeMap::new(),
        })
    }

    pub fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking = level.map(|level| if level == "off" { "none" } else { level }.to_owned());
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

    /// Set request body defaults from the selected model's sampling parameters.
    pub fn with_sampling(mut self, sampling: BTreeMap<String, Value>) -> Result<Self> {
        validate_sampling(&sampling)?;
        self.sampling = sampling;
        Ok(self)
    }

    /// Return the signed-in account's displayable models in server order.
    pub async fn models(&self) -> Result<Vec<String>> {
        let mut endpoint = self.endpoint.clone();
        endpoint.set_path("/v1/models");
        let mut response = self
            .client
            .get(endpoint)
            .headers(self.headers.clone())
            .bearer_auth(&self.token)
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

fn payload(request: &ModelRequest, provider: &str, edited_scope: &str) -> Value {
    let mut input = Vec::new();
    for message in &request.messages {
        match message {
            Message::System { content } => {
                input.push(json!({"role":"developer", "content": content}))
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
                        == if r.is_context_edit() {
                            edited_scope
                        } else {
                            provider
                        }
                        && r.identity.model == request.model
                        && r.supports_api(pi_rs_core::ModelApi::OpenaiResponses)
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
    let mut body = json!({"model":request.model, "input":input, "store":false, "stream":true, "max_output_tokens":request.max_output_tokens});
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
        pi_rs_core::protected_model_fingerprint(&(
            headers,
            "openai-responses-v1",
            self.endpoint.as_str(),
            &self.provider,
            &self.token,
            &self.thinking,
            &self.sampling,
        ))
    }

    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        match message {
            Message::Assistant {
                replay: Some(replay),
                ..
            } if replay.identity.provider
                == if replay.is_context_edit() {
                    pi_rs_core::endpoint_replay_scope(&self.provider, self.endpoint.as_str())
                } else {
                    self.provider.clone()
                }
                && replay.identity.model == request.model
                && replay.supports_api(pi_rs_core::ModelApi::OpenaiResponses) =>
            {
                pi_rs_core::estimate_replay_tokens(&replay.blocks)
                    .max(pi_rs_core::estimate_message_tokens(message))
            }
            _ => pi_rs_core::estimate_message_tokens(message),
        }
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let model = request.model.clone();
        let edited_scope =
            pi_rs_core::endpoint_replay_scope(&self.provider, self.endpoint.as_str());
        let mut body = payload(&request, &self.provider, &edited_scope);
        let provider = self.provider.clone();
        if let Some(thinking) = &self.thinking {
            body["reasoning"] = json!({"effort":thinking, "summary":"auto"});
            body["include"] = json!(["reasoning.encrypted_content"]);
        }
        body.as_object_mut()
            .expect("Responses request object")
            .extend(
                self.sampling
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        let error_secrets = pi_rs_core::provider_error_secrets(
            Some(self.token.as_str()),
            self.headers.values().filter_map(|v| v.to_str().ok()),
        );
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .headers(self.headers.clone())
            .bearer_auth(&self.token)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("OpenAI request failed".into()))?;
        if !response.status().is_success() {
            let status = response.status();
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
            let mut reasoning = String::new();
            while let Some(item) = source.next().await {
                let item = item.map_err(|error| match error {
                    EventStreamError::Transport(RawStreamError::LimitExceeded) => Error::Model("OpenAI stream exceeds 32 MiB".into()),
                    EventStreamError::Transport(RawStreamError::Transport) => Error::RetryableModel("OpenAI stream interrupted".into()),
                    _ => Error::Model("invalid OpenAI event stream".into()),
                })?;
                if item.data == "[DONE]" { continue; }
                let event: Value = serde_json::from_str(&item.data).map_err(|_| Error::Model("invalid OpenAI stream event".into()))?;
                let kind = field(&event, "type")?;
                started |= recognized_response_event(kind);
                match kind {
                    "response.output_text.delta" | "response.refusal.delta" => yield ModelEvent::Text(field(&event,"delta")?.into()),
                    "response.reasoning_summary_text.delta" => { let text = field(&event,"delta")?; reasoning.push_str(text); yield ModelEvent::Reasoning(text.into()); },
                    "response.output_item.added" if event["item"]["type"] == "function_call" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let item = &event["item"];
                        if provider == "openai-chatgpt" && item["namespace"] != NAMESPACE { Err(Error::Model("unexpected OpenAI tool namespace".into()))?; }
                        let call = ToolCall { id:field(item,"call_id")?.into(), name:field(item,"name")?.into(), arguments:String::new() };
                        if calls.insert(index, call.clone()).is_some() { Err(Error::Model("duplicate OpenAI tool index".into()))?; }
                        yield ModelEvent::ToolCallDelta(ToolCallDelta { index, id:call.id, name:call.name, arguments:String::new() });
                    }
                    "response.function_call_arguments.delta" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let delta = field(&event,"delta")?;
                        calls.get_mut(&index).ok_or_else(|| Error::Model("unknown OpenAI tool call".into()))?.arguments.push_str(delta);
                        yield ModelEvent::ToolCallDelta(ToolCallDelta { index, id:String::new(), name:String::new(), arguments:delta.into() });
                    }
                    "response.completed" | "response.incomplete" => {
                        let response = &event["response"];
                        let finish_reason = match (event["type"].as_str(), response["status"].as_str(), response["incomplete_details"]["reason"].as_str()) {
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
                        for (call, item) in calls.values().zip(reported_calls) {
                            if item["call_id"] != call.id || item["name"] != call.name || item["arguments"] != call.arguments || (provider == "openai-chatgpt" && item["namespace"] != NAMESPACE) { Err(Error::Model("OpenAI tool call changed during stream".into()))?; }
                        }
                        let usage = response["usage"]["input_tokens"].as_u64().zip(response["usage"]["output_tokens"].as_u64()).map(|(input_tokens,output_tokens)| Usage { input_tokens, output_tokens });
                        let identity = ResolvedModelIdentity { provider:provider.clone(), model:model.clone() };
                        yield ModelEvent::Metadata(ModelMetadata { provider_api:Some(pi_rs_core::ModelApi::OpenaiResponses), replay_identity:Some(ResolvedModelIdentity { provider: edited_scope.clone(), model: model.clone() }), resolved_model:Some(identity.clone()), finish_reason:Some(finish_reason.into()), cached_input_tokens:response["usage"]["input_tokens_details"]["cached_tokens"].as_u64(), reasoning:(!reasoning.is_empty()).then_some(reasoning), replay:(finish_reason != "length").then(|| Box::new(ProviderReplay { identity, blocks:output.clone() })), ..Default::default() });
                        yield ModelEvent::Complete { calls:calls.into_values().collect(), usage };
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
            payload(&request, "shared", &scope_a)["input"][0]["type"],
            "reasoning"
        );
        assert!(
            !payload(&request, "shared", &scope_b)
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
        let body = payload(&request(), "openai-chatgpt", "openai-chatgpt");
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
        let body = payload(&request, "openai", "openai");
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
        let body = payload(&request, "openai-chatgpt", "openai-chatgpt");
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
            payload(&request, "openai", "openai")["input"][2]["output"],
            text
        );
        request.messages[2] =
            serde_json::from_value(json!({"role":"tool","call_id":"call","content":text})).unwrap();
        let output = &payload(&request, "openai", "openai")["input"][2]["output"];
        assert_eq!(output[1]["type"], "input_image");
        assert_eq!(output[1]["image_url"], "data:image/png;base64,aW1hZ2U=");
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
