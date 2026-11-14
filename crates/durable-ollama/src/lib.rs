//! Explicitly installed durable adapter for Ollama's OpenAI-compatible endpoint.
//! Encoding and SSE decoding adapted from pi-rs-ollama, under the MIT license.
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, ModelApi, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Result,
    ToolCall, ToolCallDelta, Usage,
};
use pi_rs_durable::model::{ProviderAdapter, ProviderAttempt};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::IpAddr, time::Duration};

/// Loopback-only client; installing and selecting it in the durable harness is explicit.
pub struct OllamaAdapter {
    client: Client,
    endpoint: Url,
}
impl OllamaAdapter {
    /// Accepts a numeric loopback base URL, e.g. `http://127.0.0.1:11434`.
    pub fn new(base: &str) -> Result<Self> {
        let mut endpoint =
            Url::parse(base).map_err(|_| Error::Config("invalid Ollama URL".into()))?;
        let loopback = endpoint
            .host_str()
            .and_then(|s| s.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if endpoint.scheme() != "http"
            || !loopback
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !matches!(endpoint.path(), "" | "/")
        {
            return Err(Error::Config(
                "Ollama URL must be a numeric loopback HTTP origin".into(),
            ));
        }
        endpoint.set_path("/v1/chat/completions");
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| Error::Config("could not create HTTP client".into()))?;
        Ok(Self { client, endpoint })
    }
}
#[async_trait::async_trait]
impl ProviderAdapter for OllamaAdapter {
    fn id(&self) -> &str {
        "ollama-local-openai/v1"
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value> {
        let messages: Vec<_> = request
            .messages
            .clone()
            .into_iter()
            .flat_map(encode_message)
            .collect();
        let tools: Vec<_> = request
            .tools
            .iter()
            .map(|function| json!({"type":"function","function":function}))
            .collect();
        let mut body = json!({"model":request.model,"messages":messages,"stream":true,"max_tokens":request.max_output_tokens,"stream_options":{"include_usage":true}});
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        Ok(body)
    }
    fn validate(&self, _: &ProviderAttempt, request: &ModelRequest, body: &Value) -> Result<()> {
        let fail = || Error::Config("invalid or unauthorized Ollama request body".into());
        let obj = body.as_object().ok_or_else(fail)?;
        if body["model"] != request.model
            || body["stream"] != true
            || !body["messages"].is_array()
            || body["max_tokens"]
                .as_u64()
                .is_none_or(|n| n == 0 || n > u64::from(request.max_output_tokens))
            || obj.contains_key("max_completion_tokens")
            || obj.contains_key("max_output_tokens")
            || obj.contains_key("functions")
            || obj.contains_key("function_call")
            || obj.get("n").is_some_and(|n| n.as_u64() != Some(1))
            || serde_json::to_vec(body)?.len() > 4 * 1024 * 1024
        {
            return Err(fail());
        }
        let expected_tools: Vec<_> = request
            .tools
            .iter()
            .map(|function| json!({"type":"function","function":function}))
            .collect();
        if (expected_tools.is_empty() && obj.contains_key("tools"))
            || (!expected_tools.is_empty() && body["tools"] != json!(expected_tools))
        {
            return Err(fail());
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        attempt: &ProviderAttempt,
        request: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream> {
        self.validate(attempt, request, &body)?;
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("HTTP connection failed".into()))?;
        let status = response.status();
        if !status.is_success() {
            let s = format!("HTTP status {status}");
            return Err(
                if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
                    Error::RetryableModel(s)
                } else {
                    Error::Model(s)
                },
            );
        }
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected an SSE response".into()));
        }
        Ok(Box::pin(async_stream::try_stream! {
            let mut bytes = 0usize;
            let mut events = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|_| Error::RetryableModel("broken HTTP stream".into()))?;
                bytes = bytes.saturating_add(chunk.len());
                if bytes > 4 * 1024 * 1024 { return Err(Error::Model("model stream exceeds size limit".into())); }
                Ok(chunk)
            }).eventsource();
            let mut calls = BTreeMap::<usize, CallDelta>::new();
            let mut usage = None;
            let mut metadata = ModelMetadata { provider_api: Some(ModelApi::OpenaiCompletions), ..Default::default() };
            let mut reasoning = String::new();
            while let Some(event) = events.next().await {
                let event = event.map_err(|e| match e { eventsource_stream::EventStreamError::Transport(e) => e, _ => Error::RetryableModel("broken SSE stream".into()) })?;
                if event.data == "[DONE]" {
                    match metadata.finish_reason.as_deref() { Some("stop" | "length") if calls.is_empty() => {}, Some("tool_calls") if !calls.is_empty() => {}, _ => Err(Error::Model("model response did not finish successfully".into()))? }
                    let mut ids = std::collections::HashSet::new();
                    if calls.values().any(|c| c.id.is_empty() || c.name.is_empty() || !c.has_arguments || !ids.insert(&c.id)) {
                        Err(Error::Model("incomplete or duplicate tool call".into()))?;
                    }
                    if !reasoning.is_empty() { metadata.reasoning = Some(reasoning); yield ModelEvent::Metadata(metadata); }
                    yield ModelEvent::Complete { calls: calls.into_values().map(|c| ToolCall { id:c.id, name:c.name, arguments:c.arguments }).collect(), usage };
                    return;
                }
                let chunk: Chunk = serde_json::from_str(&event.data).map_err(|_| Error::Model("malformed SSE chunk".into()))?;
                if chunk.error.is_some() { Err(Error::Model("provider returned a stream error".into()))?; }
                let previous = metadata.clone();
                if let Some(id) = chunk.id.filter(|id| id.len() <= 128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))) { metadata.provider_request_id = Some(id); }
                if let Some(stats) = chunk.usage { usage = Some(Usage { input_tokens: stats.prompt_tokens, output_tokens: stats.completion_tokens }); }
                for choice in chunk.choices {
                    if choice.index != 0 || metadata.finish_reason.is_some() { Err(Error::Model("unexpected choice in model stream".into()))?; }
                    if let Some(text) = [choice.delta.reasoning, choice.delta.reasoning_content, choice.delta.reasoning_text].into_iter().flatten().find(|s| !s.is_empty()) { reasoning.push_str(&text); yield ModelEvent::Reasoning(text); }
                    if let Some(text) = choice.delta.content.filter(|s| !s.is_empty()) { yield ModelEvent::Text(text); }
                    for delta in choice.delta.tool_calls.unwrap_or_default() {
                        if delta.index >= 64 { Err(Error::Model("invalid tool call index".into()))?; }
                        let (name, arguments, has_arguments) = delta.function.map(|f| (f.name.unwrap_or_default(), f.arguments.clone().unwrap_or_default(), f.arguments.is_some())).unwrap_or_default();
                        let id = delta.id.unwrap_or_default(); let call = calls.entry(delta.index).or_default();
                        call.has_arguments |= has_arguments;
                        call.id.push_str(&id); call.name.push_str(&name); call.arguments.push_str(&arguments);
                        yield ModelEvent::ToolCallDelta(ToolCallDelta { index: delta.index as u64, id, name, arguments });
                    }
                    metadata.finish_reason = choice.finish_reason.map(|r| match r.as_str() { "stop"|"tool_calls"|"length"|"content_filter" => r, "end" => "stop".into(), "function_call" => "tool_calls".into(), _ => "other".into() });
                }
                if metadata != previous { yield ModelEvent::Metadata(metadata.clone()); }
            }
            Err(Error::RetryableModel("SSE closed before the end marker".into()))?;
        }))
    }
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
        Message::Tool { call_id, content } => encode_tool_result(&call_id, content),
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

fn encode_tool_result(call_id: &str, content: String) -> Vec<Value> {
    let mut messages = Vec::new();
    let (content, images) = pi_rs_core::extract_tool_images(content);
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
    prompt_tokens: u64,
    completion_tokens: u64,
}
#[derive(Deserialize)]
struct Choice {
    index: u32,
    delta: Delta,
    finish_reason: Option<String>,
}
#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
    reasoning: Option<String>,
    reasoning_content: Option<String>,
    reasoning_text: Option<String>,
    tool_calls: Option<Vec<WireCall>>,
}
#[derive(Deserialize)]
struct WireCall {
    index: usize,
    id: Option<String>,
    function: Option<WireFunction>,
}
#[derive(Deserialize)]
struct WireFunction {
    name: Option<String>,
    arguments: Option<String>,
}
#[derive(Default)]
struct CallDelta {
    id: String,
    name: String,
    arguments: String,
    has_arguments: bool,
}
