//! A native adapter for Pi's gateway message protocol.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs::{
    Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay,
    ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use reqwest::{Client, Url, header::HeaderMap};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const MAX_STREAM: usize = 32 * 1024 * 1024;
const MAX_BLOCKS: usize = 256;
const REPLAY_ORIGIN: &str = "pi-rs:pi-messages-origin";

type ProgressObserver = Arc<dyn Fn(Progress) + Send + Sync>;
type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Progress {
    WaitingForModel,
    Thinking,
    Generating,
    PreparingTool,
}

#[derive(Clone)]
pub(super) struct PiMessages {
    client: Client,
    endpoint: Url,
    identity: String,
    origin_hash: String,
    token: String,
    headers: HeaderMap,
    thinking: Option<String>,
    progress: Option<ProgressObserver>,
    reasoning: Option<ReasoningObserver>,
}

impl PiMessages {
    pub(super) fn new(provider: &str, endpoint: &str, token: String) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .map_err(|_| Error::Config("Invalid Pi Messages endpoint".into()))?;
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
            return Err(Error::Config("Pi Messages requires HTTPS or loopback HTTP without credentials, query or fragment".into()));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Config("Could not initialize the Pi Messages client".into()))?;
        let origin_hash = format!("{:x}", Sha256::digest(endpoint.as_str()));
        Ok(Self {
            client,
            endpoint,
            identity: provider.into(),
            origin_hash,
            token,
            headers: HeaderMap::new(),
            thinking: None,
            progress: None,
            reasoning: None,
        })
    }

    pub(super) fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    pub(super) fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking = level.map(str::to_owned);
        self
    }

    pub(super) fn observe(
        mut self,
        progress: impl Fn(Progress) + Send + Sync + 'static,
        reasoning: impl Fn(&str) + Send + Sync + 'static,
    ) -> Self {
        self.progress = Some(Arc::new(progress));
        self.reasoning = Some(Arc::new(reasoning));
        self
    }

    fn encode(&self, request: ModelRequest) -> Value {
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
                                blocks.extend(calls.iter().map(super::rpc_events::tool_call_value));
                                blocks
                            },
                            |replay| replay.blocks[1..].to_vec(),
                        );
                    json!({
                        "role":"assistant","content":blocks,"provider":self.identity,"api":"pi-messages","model":request.model,
                        "timestamp":0,"stopReason":if calls.is_empty() {"stop"} else {"toolUse"},
                        "usage":super::rpc_events::usage_value(Usage::default(), &ModelMetadata::default())
                    })
                }
                Message::Tool { call_id, content } => {
                    let error = serde_json::from_str::<Value>(&content)
                        .ok()
                        .is_some_and(|value| value["data"]["isError"] == true);
                    let mut value = super::rpc_events::tool_result_value(&content, None);
                    value["role"] = json!("toolResult");
                    value["toolCallId"] = json!(call_id);
                    value["toolName"] = json!(tool_names.get(&call_id).map_or("", String::as_str));
                    value["isError"] = json!(error);
                    value["timestamp"] = json!(0);
                    value
                }
            };
            messages.push(value);
        }
        let mut options = json!({"maxTokens":request.max_output_tokens});
        if let Some(level) = &self.thinking {
            options["reasoning"] = json!(level);
        }
        if std::env::var("PI_CACHE_RETENTION").is_ok_and(|value| value == "long") {
            options["cacheRetention"] = json!("long");
        }
        json!({
            "model":request.model,
            "context":{"systemPrompt":system.join("\n\n"),"messages":messages,"tools":request.tools},
            "options":options
        })
    }
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

pub(super) fn is_pi_messages_replay(replay: &ProviderReplay) -> bool {
    replay
        .blocks
        .first()
        .is_some_and(|marker| marker["type"] == REPLAY_ORIGIN)
}

#[async_trait::async_trait]
impl Model for PiMessages {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let requested_model = request.model.clone();
        let payload = self.encode(request);
        if let Some(progress) = &self.progress {
            progress(Progress::WaitingForModel);
        }
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&payload)
            .headers(self.headers.clone())
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() || error.is_timeout() {
                    Error::RetryableModel("Pi Messages connection failed".into())
                } else {
                    Error::Model("Pi Messages request failed".into())
                }
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let message = format!("Pi Messages request failed ({status})");
            return Err(if status.as_u16() == 429 || status.is_server_error() {
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
        let client = self.clone();
        let mut events = response.bytes_stream().eventsource();
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0_usize;
            let mut blocks = Vec::<Block>::new();
            let mut calls = BTreeMap::new();
            let mut reasoning = String::new();
            let report = |stage| {
                if let Some(observer) = &client.progress {
                    observer(stage);
                }
            };
            while let Some(event) = events.next().await {
                let event = event.map_err(|_| Error::RetryableModel("Pi Messages event stream broke".into()))?;
                size = size.saturating_add(event.data.len());
                if size > MAX_STREAM {
                    Err(Error::Model("Pi Messages stream exceeds 32 MiB".into()))?;
                }
                if event.data == "[DONE]" {
                    continue;
                }
                let event: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("Invalid Pi Messages event".into()))?;
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
                            if let Some(observer) = &client.reasoning {
                                observer(delta);
                            }
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
                                if let Some(observer) = &client.reasoning {
                                    observer(suffix);
                                }
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
                        let (usage, mut metadata) = terminal_metadata(&event, &client.identity, &requested_model, finish);
                        metadata.reasoning = (!reasoning.is_empty()).then_some(reasoning);
                        let mut replay = vec![json!({"type":REPLAY_ORIGIN,"endpoint":client.origin_hash})];
                        replay.extend(blocks.into_iter().filter(|block| reason != "length" || block.kind != Kind::Tool).map(|block| block.raw));
                        metadata.replay = Some(Box::new(ProviderReplay {
                            identity: ResolvedModelIdentity { provider: client.identity, model: requested_model },
                            blocks: replay,
                        }));
                        yield ModelEvent::Metadata(metadata);
                        yield ModelEvent::Complete { calls: calls.into_values().collect(), usage };
                        return;
                    }
                    "error" => {
                        Err(if event["reason"] == "aborted" { Error::Cancelled } else { Error::Model("Pi Messages returned a stream error".into()) })?;
                    }
                    _ => Err(Error::Model("Unknown Pi Messages stream event".into()))?,
                }
            }
            Err(Error::RetryableModel("Pi Messages stream ended without a terminal event".into()))?;
        }))
    }
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
        cost_usd: stats["cost"]["total"].as_number().cloned(),
        resolved_model: Some(ResolvedModelIdentity {
            provider: provider.into(),
            model: event["responseModel"].as_str().unwrap_or(model).into(),
        }),
        finish_reason: Some(finish.into()),
        ..ModelMetadata::default()
    };
    (usage, metadata)
}
