//! Native Pi Messages request and stream handling shared by the SDK and CLI.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs::{
    Error, ImageContent, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    ProviderReplay, ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use reqwest::{Client, Url, header::HeaderMap};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::image_profile::{ImageResizeProfile, normalize_content};

const MAX_STREAM: usize = 32 * 1024 * 1024;
const MAX_BLOCKS: usize = 256;
const REPLAY_ORIGIN: &str = "pi-rs:pi-messages-origin";

pub type ProgressObserver = Arc<dyn Fn(Progress) + Send + Sync>;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Progress {
    WaitingForModel,
    Thinking,
    Generating,
    PreparingTool,
}

#[derive(Default)]
pub struct RequestOptions {
    pub headers: HeaderMap,
    pub thinking: Option<String>,
    pub cache_retention: Option<&'static str>,
    pub progress: Option<ProgressObserver>,
    pub image_resize: Option<ImageResizeProfile>,
}

/// A fixed request endpoint and provider identity. Callers supply credentials
/// and request options explicitly. Radius account and catalog state live in the CLI.
#[derive(Clone)]
pub struct Transport {
    client: Client,
    endpoint: Url,
    identity: String,
    origin_hash: String,
}

impl Transport {
    pub fn new(provider: &str, endpoint: &str) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .map_err(|_| Error::Config("Invalid Pi Messages endpoint".into()))?;
        validate_endpoint(&endpoint)?;
        Ok(Self {
            client: http_client(None, &[])?,
            origin_hash: endpoint_hash(&endpoint),
            endpoint,
            identity: provider.into(),
        })
    }

    pub fn with_http_options(
        &mut self,
        timeout: Option<Duration>,
        proxies: &[reqwest::Proxy],
    ) -> Result<()> {
        self.client = http_client(timeout, proxies)?;
        Ok(())
    }

    pub fn http_client(&self) -> &Client {
        &self.client
    }

    /// Apply a Radius catalog route subject to the same rules as the original route.
    pub fn set_endpoint(&mut self, endpoint: Url) -> Result<()> {
        validate_endpoint(&endpoint)?;
        self.origin_hash = endpoint_hash(&endpoint);
        self.endpoint = endpoint;
        Ok(())
    }

    pub fn context_identity(
        &self,
        token: Option<&str>,
        headers: &HeaderMap,
        thinking: Option<&str>,
        cache_disabled: bool,
    ) -> Option<String> {
        let mut headers: Vec<_> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes()))
            .collect();
        headers.sort();
        pi_rs::protected_model_fingerprint(&(
            headers,
            "pi-messages-v1",
            self.endpoint.as_str(),
            &self.identity,
            token,
            thinking,
            cache_disabled,
        ))
    }

    pub fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        match message {
            Message::Assistant {
                replay: Some(replay),
                ..
            } if replay.identity.provider == self.identity
                && replay.identity.model == request.model
                && replay.blocks.first().is_some_and(|marker| {
                    marker["type"] == REPLAY_ORIGIN && marker["endpoint"] == self.origin_hash
                }) =>
            {
                pi_rs::estimate_replay_tokens(&replay.blocks[1..])
                    .max(pi_rs::estimate_message_tokens(message))
            }
            _ => pi_rs::estimate_message_tokens(message),
        }
    }

    pub fn encode(&self, request: ModelRequest, options: &RequestOptions) -> Value {
        let mut system = Vec::new();
        let mut messages = Vec::new();
        let mut tool_names = HashMap::new();
        for message in request.messages {
            let value = match message {
                Message::System { content } => {
                    system.push(content);
                    continue;
                }
                Message::User { content } => json!({"role":"user","content":content,"timestamp":0}),
                Message::UserWithImages { content, images } => {
                    let mut blocks = Vec::new();
                    if !content.is_empty() {
                        blocks.push(json!({"type":"text","text":content}));
                    }
                    blocks.extend(images.into_iter().map(|image| {
                        json!({"type":"image","mimeType":image.mime_type,"data":image.data})
                    }));
                    json!({"role":"user","content":blocks,"timestamp":0})
                }
                Message::Assistant {
                    content,
                    calls,
                    replay,
                } => {
                    tool_names.extend(
                        calls
                            .iter()
                            .map(|call| (call.id.clone(), call.name.clone())),
                    );
                    let blocks = replay
                        .as_deref()
                        .filter(|replay| {
                            replay.identity.provider == self.identity
                                && replay.identity.model == request.model
                                && replay.supports_api(pi_rs::ModelApi::PiMessages)
                                && replay.blocks.first().is_some_and(|marker| {
                                    marker["type"] == REPLAY_ORIGIN
                                        && marker["endpoint"] == self.origin_hash
                                })
                        })
                        .map_or_else(
                            || {
                                let mut blocks = Vec::new();
                                if !content.is_empty() {
                                    blocks.push(json!({"type":"text","text":content}));
                                }
                                blocks.extend(calls.iter().map(tool_call_value));
                                blocks
                            },
                            |replay| {
                                replay.blocks[1..]
                                    .iter()
                                    .filter(|block| block.get("_pi_context_edit_api").is_none())
                                    .cloned()
                                    .collect()
                            },
                        );
                    if blocks.is_empty() {
                        continue;
                    }
                    json!({
                        "role":"assistant","content":blocks,"provider":self.identity,"api":"pi-messages","model":request.model,
                        "timestamp":0,"stopReason":if calls.is_empty() {"stop"} else {"toolUse"},
                        "usage":usage_value(Usage::default(), &ModelMetadata::default())
                    })
                }
                Message::Tool {
                    call_id,
                    content,
                    is_error,
                    is_structured,
                } => {
                    let mut value = json!({"content":tool_result_content(&content, is_structured)});
                    value["role"] = json!("toolResult");
                    value["toolCallId"] = json!(call_id);
                    value["toolName"] = json!(tool_names.get(&call_id).map_or("", String::as_str));
                    value["isError"] = json!(is_error);
                    value["timestamp"] = json!(0);
                    value
                }
            };
            messages.push(value);
        }
        let mut request_options = json!({"maxTokens":request.max_output_tokens});
        if let Some(level) = &options.thinking {
            request_options["reasoning"] = json!(level);
        }
        if let Some(retention) = options.cache_retention {
            request_options["cacheRetention"] = json!(retention);
        }
        json!({
            "model":request.model,
            "context":{"systemPrompt":system.join("\n\n"),"messages":messages,"tools":request.tools},
            "options":request_options
        })
    }

    pub async fn stream(
        &self,
        token: &str,
        request: ModelRequest,
        options: RequestOptions,
    ) -> Result<ModelStream> {
        let error_secrets = pi_rs::provider_error_secrets(
            Some(token),
            options.headers.iter().filter_map(|(name, value)| {
                value.to_str().ok().map(|value| (name.as_str(), value))
            }),
        );
        let requested_model = request.model.clone();
        let mut payload = self.encode(request, &options);
        if let Some(profile) = options.image_resize {
            resize_request_images(&mut payload, profile)?;
        }
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(token)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&payload)
            .headers(options.headers.clone())
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() && !error.is_connect() {
                    Error::RetryableModel("HTTP idle timeout".into())
                } else if error.is_connect() {
                    Error::RetryableModel("Pi Messages connection failed".into())
                } else {
                    Error::Model("Pi Messages request failed".into())
                }
            })?;
        pi_rs::observe_provider_response(
            response.status(),
            response.headers(),
            &error_secrets,
            &options.headers,
        )
        .await;
        if !response.status().is_success() {
            let status = response.status();
            let message = format!("Pi Messages request failed ({status})");
            if status.as_u16() == 413 {
                return Err(Error::Model("413 request too large".into()));
            }
            if matches!(status.as_u16(), 400 | 422) {
                let overflow = pi_rs::provider_context_overflow_stream(
                    response.bytes_stream(),
                    &error_secrets,
                )
                .await;
                return Err(Error::Model(if overflow {
                    "context window exceeded".into()
                } else {
                    message
                }));
            }
            let permanent_quota = (status.as_u16() == 429 || status.is_server_error())
                && pi_rs::permanent_provider_limit(response.bytes_stream(), &error_secrets).await;
            return Err(if permanent_quota {
                Error::Model("Pi Messages account quota or billing limit reached".into())
            } else if status.as_u16() == 429 || status.is_server_error() {
                Error::RetryableModel(message)
            } else {
                Error::Model(message)
            });
        }
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            return Err(Error::Model("Expected a Pi Messages event stream".into()));
        }
        let mut events = response.bytes_stream().eventsource();
        let observe_stream = pi_rs::observes_provider_stream();
        let identity = self.identity.clone();
        let origin_hash = self.origin_hash.clone();
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0_usize;
            let mut blocks = Vec::<Block>::new();
            let mut calls = BTreeMap::new();
            let mut reasoning = String::new();
            let report = |stage| {
                if let Some(observer) = &options.progress {
                    observer(stage);
                }
            };
            while let Some(event) = events.next().await {
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) if error.is_timeout() => Error::RetryableModel("HTTP idle timeout".into()),
                    _ => Error::RetryableModel("Pi Messages event stream broke".into()),
                })?;
                size = size.saturating_add(event.data.len());
                if size > MAX_STREAM {
                    Err(Error::Model("Pi Messages stream exceeds 32 MiB".into()))?;
                }
                if event.data == "[DONE]" {
                    continue;
                }
                let event: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("Invalid Pi Messages event".into()))?;
                if observe_stream {
                    pi_rs::observe_provider_stream(
                        &identity, pi_rs::ModelApi::PiMessages, &requested_model,
                        &event, &error_secrets, &options.headers,
                    ).await;
                }
                match field(&event, "type")? {
                    "start" => {}
                    "text_start" => {
                        report(Progress::Generating);
                        push_block(&mut blocks, index(&event)?, Block::start(Kind::Text, json!({"type":"text","text":""})))?;
                    }
                    "thinking_start" => {
                        report(Progress::Thinking);
                        push_block(&mut blocks, index(&event)?, Block::start(Kind::Thinking, json!({"type":"thinking","thinking":""})))?;
                    }
                    kind @ ("text_delta" | "thinking_delta") => {
                        let text = kind == "text_delta";
                        let key = if text {"text"} else {"thinking"};
                        let block = open(&mut blocks, index(&event)?, if text {Kind::Text} else {Kind::Thinking})?;
                        let delta = field(&event, "delta")?;
                        if let Value::String(content) = &mut block.raw[key] {
                            content.push_str(delta);
                        }
                        if text {
                            yield ModelEvent::Text(delta.into());
                        } else {
                            reasoning.push_str(delta);
                            yield ModelEvent::Reasoning(delta.into());
                        }
                    }
                    kind @ ("text_end" | "thinking_end") => {
                        let text = kind == "text_end";
                        let key = if text {"text"} else {"thinking"};
                        let block = open(&mut blocks, index(&event)?, if text {Kind::Text} else {Kind::Thinking})?;
                        let content = field(&event, "content")?;
                        let previous = field(&block.raw, key)?;
                        let suffix = content.strip_prefix(previous).ok_or_else(|| {
                            Error::Model("Pi Messages content changed after streaming".into())
                        })?;
                        if !suffix.is_empty() {
                            if text {
                                yield ModelEvent::Text(suffix.into());
                            } else if event["redacted"] != true {
                                reasoning.push_str(suffix);
                                yield ModelEvent::Reasoning(suffix.into());
                            }
                        }
                        block.raw[key] = json!(content);
                        if let Some(signature) = event["contentSignature"].as_str() {
                            block.raw[if text {"textSignature"} else {"thinkingSignature"}] = json!(signature);
                        }
                        if !text && event["redacted"] == true {
                            block.raw["redacted"] = json!(true);
                        }
                        block.complete = true;
                    }
                    "toolcall_start" => {
                        report(Progress::PreparingTool);
                        let index = index(&event)?;
                        let id = field(&event, "id")?;
                        let name = field(&event, "toolName")?;
                        push_block(&mut blocks, index, Block::start(Kind::Tool, json!({"type":"toolCall","id":id,"name":name,"arguments":{}})))?;
                        yield ModelEvent::ToolCallDelta(ToolCallDelta {
                            index: index as u64, id: id.into(), name: name.into(), arguments: String::new(),
                        });
                    }
                    "toolcall_delta" => {
                        let index = index(&event)?;
                        let block = open(&mut blocks, index, Kind::Tool)?;
                        let delta = field(&event, "delta")?;
                        block.arguments.push_str(delta);
                        yield ModelEvent::ToolCallDelta(ToolCallDelta {
                            index: index as u64, id: String::new(), name: String::new(), arguments: delta.into(),
                        });
                    }
                    "toolcall_end" => {
                        let index = index(&event)?;
                        let block = open(&mut blocks, index, Kind::Tool)?;
                        let call = &event["toolCall"];
                        let id = field(call, "id")?;
                        let name = field(call, "name")?;
                        if block.raw["id"] != id || block.raw["name"] != name || id.is_empty() || name.is_empty() {
                            Err(Error::Model("Pi Messages tool call identity changed while streaming".into()))?;
                        }
                        let arguments = if call["argumentsError"].is_string() {
                            let raw = std::mem::take(&mut block.arguments);
                            if serde_json::from_str::<Value>(&raw).is_ok_and(|value| value.is_object()) {
                                String::new()
                            } else {
                                raw
                            }
                        } else if call["arguments"].is_object() {
                            call["arguments"].to_string()
                        } else {
                            Err(Error::Model("Pi Messages tool arguments are not an object".into()))?
                        };
                        block.raw = json!({"type":"toolCall","id":id,"name":name,"arguments":call["arguments"]});
                        if let Some(error) = call["argumentsError"].as_str() {
                            block.raw["argumentsError"] = json!(error);
                        }
                        block.complete = true;
                        calls.insert(index, ToolCall { id: id.into(), name: name.into(), arguments });
                    }
                    "done" => {
                        let reason = field(&event, "reason")?;
                        let finish = match reason {
                            "stop" if calls.is_empty() => "stop",
                            "toolUse" if !calls.is_empty() => "tool_calls",
                            "length" => { calls.clear(); "length" }
                            _ => Err(Error::Model("Pi Messages returned an invalid stop reason".into()))?,
                        };
                        if reason != "length" && blocks.iter().any(|block| !block.complete) {
                            Err(Error::Model("Pi Messages ended with an open content block".into()))?;
                        }
                        let (usage, mut metadata) = terminal_metadata(&event, &identity, &requested_model, finish);
                        metadata.reasoning = (!reasoning.is_empty()).then_some(reasoning);
                        let mut replay = vec![json!({"type":REPLAY_ORIGIN,"endpoint":origin_hash})];
                        replay.extend(blocks.into_iter().filter(|block| reason != "length" || block.kind != Kind::Tool).map(|block| block.raw));
                        metadata.replay = Some(Box::new(ProviderReplay {
                            identity: ResolvedModelIdentity { provider: identity, model: requested_model },
                            blocks: replay,
                        }));
                        yield ModelEvent::Metadata(metadata);
                        yield ModelEvent::Complete { calls: calls.into_values().collect(), usage };
                        return;
                    }
                    "error" => {
                        Err(if event["reason"] == "aborted" { Error::Cancelled } else if pi_rs::permanent_provider_error(&event["errorMessage"], &error_secrets) { Error::Model("Pi Messages account quota or billing limit reached".into()) } else if pi_rs::retryable_provider_error(&event["errorMessage"], &error_secrets) { Error::RetryableModel("Pi Messages returned a transient stream error".into()) } else if pi_rs::provider_context_overflow(&event["errorMessage"], &error_secrets) { Error::Model("context window exceeded".into()) } else { Error::Model("Pi Messages returned a stream error".into()) })?;
                    }
                    _ => Err(Error::Model("Unknown Pi Messages stream event".into()))?,
                }
            }
            Err(Error::RetryableModel("Pi Messages stream ended without a terminal event".into()))?;
        }))
    }
}

fn http_client(timeout: Option<Duration>, proxies: &[reqwest::Proxy]) -> Result<Client> {
    let mut builder = Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none());
    for proxy in proxies {
        builder = builder.proxy(proxy.clone());
    }
    if let Some(timeout) = timeout {
        builder = builder.read_timeout(timeout);
    }
    builder
        .build()
        .map_err(|_| Error::Config("Could not initialize the Pi Messages client".into()))
}

fn validate_endpoint(endpoint: &Url) -> Result<()> {
    let loopback = matches!(
        endpoint.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    );
    if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
        || endpoint.host().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(Error::Config(
            "Pi Messages requires HTTPS or loopback HTTP without credentials, query or fragment"
                .into(),
        ));
    }
    Ok(())
}

fn endpoint_hash(endpoint: &Url) -> String {
    format!("{:x}", Sha256::digest(endpoint.as_str()))
}

#[derive(Clone)]
pub(crate) struct HostModel {
    transport: Transport,
    token: String,
    thinking: Option<String>,
}

impl HostModel {
    pub(crate) fn new(provider: &str, endpoint: &str, token: String) -> Result<Self> {
        Ok(Self {
            transport: Transport::new(provider, endpoint)?,
            token,
            thinking: None,
        })
    }

    pub(crate) fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking = level.map(str::to_owned);
        self
    }
}

#[async_trait::async_trait]
impl Model for HostModel {
    fn estimate_message_tokens(&self, request: &ModelRequest, message: &Message) -> u64 {
        self.transport.estimate_message_tokens(request, message)
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        self.transport
            .stream(
                &self.token,
                request,
                RequestOptions {
                    thinking: self.thinking.clone(),
                    ..RequestOptions::default()
                },
            )
            .await
    }
}

fn resize_request_images(payload: &mut Value, profile: ImageResizeProfile) -> Result<()> {
    let Some(messages) = payload["context"]["messages"].as_array_mut() else {
        return Ok(());
    };
    for message in messages
        .iter_mut()
        .filter(|message| matches!(message["role"].as_str(), Some("user" | "toolResult")))
    {
        for part in message["content"].as_array_mut().into_iter().flatten() {
            if part["type"] == "image" {
                let content = normalize_content(
                    ImageContent {
                        mime_type: field(part, "mimeType")?.into(),
                        data: field(part, "data")?.into(),
                    },
                    profile,
                )?;
                part["mimeType"] = json!(content.mime_type);
                part["data"] = json!(content.data);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Text,
    Thinking,
    Tool,
}

struct Block {
    kind: Kind,
    raw: Value,
    arguments: String,
    complete: bool,
}

impl Block {
    fn start(kind: Kind, raw: Value) -> Self {
        Self {
            kind,
            raw,
            arguments: String::new(),
            complete: false,
        }
    }
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .ok_or_else(|| Error::Model(format!("Pi Messages event has invalid {name}")))
}

fn index(event: &Value) -> Result<usize> {
    event["contentIndex"]
        .as_u64()
        .and_then(|index| usize::try_from(index).ok())
        .filter(|index| *index < MAX_BLOCKS)
        .ok_or_else(|| Error::Model("Pi Messages content index is invalid".into()))
}

fn open(blocks: &mut [Block], index: usize, kind: Kind) -> Result<&mut Block> {
    blocks
        .get_mut(index)
        .filter(|block| block.kind == kind && !block.complete)
        .ok_or_else(|| Error::Model("Pi Messages event has no matching open block".into()))
}

fn push_block(blocks: &mut Vec<Block>, index: usize, block: Block) -> Result<()> {
    if index != blocks.len() {
        return Err(Error::Model(
            "Pi Messages block index is out of order".into(),
        ));
    }
    blocks.push(block);
    Ok(())
}

pub fn is_pi_messages_replay(replay: &ProviderReplay) -> bool {
    replay
        .blocks
        .first()
        .is_some_and(|marker| marker["type"] == REPLAY_ORIGIN)
}

fn terminal_metadata(
    event: &Value,
    provider: &str,
    model: &str,
    finish: &str,
) -> (Option<Usage>, ModelMetadata) {
    let stats = &event["usage"];
    let read = stats["cacheRead"].as_u64();
    let write = stats["cacheWrite"].as_u64();
    let usage = stats["input"]
        .as_u64()
        .zip(stats["output"].as_u64())
        .map(|(input, output)| Usage {
            input_tokens: input
                .saturating_add(read.unwrap_or_default())
                .saturating_add(write.unwrap_or_default()),
            output_tokens: output,
        });
    let metadata = ModelMetadata {
        provider_request_id: event["responseId"]
            .as_str()
            .filter(|id| id.len() <= 256)
            .map(str::to_owned),
        cached_input_tokens: read,
        cache_creation_input_tokens: write,
        cache_write_1h_tokens: stats["cacheWrite1h"].as_u64(),
        reasoning_output_tokens: stats["reasoning"].as_u64(),
        cost_usd: stats["cost"]["total"].as_number().cloned(),
        cost_breakdown: pi_rs::UsageCostBreakdown::from_value(&stats["cost"]),
        cost_from_provider: stats["cost"]["totalFromProvider"].as_bool(),
        provider_api: Some(pi_rs::ModelApi::PiMessages),
        resolved_model: Some(ResolvedModelIdentity {
            provider: provider.into(),
            model: event["responseModel"].as_str().unwrap_or(model).into(),
        }),
        finish_reason: Some(finish.into()),
        ..ModelMetadata::default()
    };
    (usage, metadata)
}

pub fn tool_call_value(call: &ToolCall) -> Value {
    let arguments = pi_rs::parse_tool_arguments(&call.arguments);
    let mut value = json!({"type":"toolCall","id":call.id,"name":call.name,"arguments":arguments.as_ref().cloned().unwrap_or_else(|_| json!({}))});
    if let Err(error) = arguments {
        value["argumentsError"] = json!(error);
    }
    value
}

pub fn tool_result_content(text: &str, is_structured: Option<bool>) -> Vec<Value> {
    let (text, images) = if pi_rs::tool_content_is_structured(text, is_structured) {
        pi_rs::extract_tool_images(text.to_owned())
    } else {
        (text.to_owned(), Vec::new())
    };
    let mut content = vec![json!({"type":"text","text":text})];
    content.extend(
        images
            .into_iter()
            .map(|image| json!({"type":"image","data":image.data,"mimeType":image.mime_type})),
    );
    content
}

pub fn usage_value(usage: Usage, metadata: &ModelMetadata) -> Value {
    let read = metadata.cached_input_tokens.unwrap_or_default();
    let write = metadata.cache_creation_input_tokens.unwrap_or_default();
    let breakdown = metadata.cost_breakdown.clone().unwrap_or_default();
    let mut value = json!({
        "input":usage.input_tokens.saturating_sub(read).saturating_sub(write),
        "output":usage.output_tokens,"cacheRead":read,"cacheWrite":write,
        "totalTokens":usage.input_tokens.saturating_add(usage.output_tokens),
        "cost":{
            "input":breakdown.input,"output":breakdown.output,
            "cacheRead":breakdown.cache_read,"cacheWrite":breakdown.cache_write,
            "total":metadata.cost_usd.as_ref().and_then(serde_json::Number::as_f64).unwrap_or_default()
        },
    });
    if let Some(reasoning) = metadata.reasoning_output_tokens {
        value["reasoning"] = json!(reasoning);
    }
    if let Some(long) = metadata.cache_write_1h_tokens {
        value["cacheWrite1h"] = json!(long);
    }
    if let Some(from_provider) = metadata.cost_from_provider {
        value["cost"]["totalFromProvider"] = json!(from_provider);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_endpoint_changes_leave_the_original_route_intact() {
        let original = "https://fixture.example/v1/messages";
        let mut transport = Transport::new("fixture", original).unwrap();
        for invalid in [
            "http://public.example/v1/messages",
            "https://fixture.example/v1/messages?query=1",
            "https://some-user@fixture.example/v1/messages",
        ] {
            assert!(Transport::new("fixture", invalid).is_err());
            assert!(
                transport
                    .set_endpoint(Url::parse(invalid).unwrap())
                    .is_err()
            );
            assert_eq!(transport.endpoint.as_str(), original);
            assert_eq!(transport.origin_hash, endpoint_hash(&transport.endpoint));
        }
    }

    #[test]
    fn request_resize_preserves_replay_and_resizes_user_and_tool_images() {
        use base64::Engine;
        use std::io::Cursor;

        let mut bytes = Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(40, 20)
            .write_to(&mut bytes, image::ImageFormat::Png)
            .unwrap();
        let encoding = base64::engine::general_purpose::STANDARD;
        let part = json!({"type":"image", "mimeType":"image/png", "data":encoding.encode(bytes.get_ref())});
        let mut payload = json!({"context":{"messages":[
            {"role":"user", "content":[{"type":"text", "text":"prompt"}, part]},
            {"role":"toolResult", "content":[part]},
            {"role":"assistant", "content":[part]}
        ]}});
        resize_request_images(
            &mut payload,
            ImageResizeProfile {
                max_width: Some(16),
                ..Default::default()
            },
        )
        .unwrap();
        let messages = &payload["context"]["messages"];
        for image in [&messages[0]["content"][1], &messages[1]["content"][0]] {
            let decoded = encoding.decode(image["data"].as_str().unwrap()).unwrap();
            let image = image::load_from_memory(&decoded).unwrap();
            assert_eq!((image.width(), image.height()), (16, 8));
        }
        assert_eq!(messages[0]["content"][0]["text"], "prompt");
        assert_eq!(messages[2]["content"][0], part);
    }
}
