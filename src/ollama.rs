//! Streaming adapter for Ollama's /v1/chat/completions interface.
use std::collections::BTreeMap;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Result, ToolCall, Usage,
};

pub struct Ollama {
    client: Client,
    endpoint: Url,
    token: Option<String>,
}

impl Ollama {
    /// Accept a loopback HTTP service or a TLS-protected relay, without redirects or proxy discovery.
    pub fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
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
            endpoint,
            token,
        })
    }
}

#[async_trait::async_trait]
impl Model for Ollama {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let messages: Vec<_> = request.messages.into_iter().map(encode_message).collect();
        let tools: Vec<_> = request
            .tools
            .into_iter()
            .map(|function| json!({"type": "function", "function": function}))
            .collect();
        let mut body = json!({
            "model": request.model, "messages": messages, "stream": true,
            "stream_options": {"include_usage": true},
            "max_tokens": request.max_output_tokens,
        });
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        let mut builder = self.client.post(self.endpoint.clone()).json(&body);
        if let Some(token) = &self.token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| Error::Model("HTTP connection failed".into()))?;
        if !response.status().is_success() {
            return Err(Error::Model(format!("HTTP status {}", response.status())));
        }
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected an SSE response".into()));
        }

        Ok(Box::pin(async_stream::try_stream! {
            let mut bytes = 0usize;
            let mut events = response.bytes_stream().map(move |chunk| {
                let chunk = chunk.map_err(|_| Error::Model("broken HTTP stream".into()))?;
                bytes = bytes.saturating_add(chunk.len());
                if bytes > 4 * 1024 * 1024 {
                    return Err(Error::Model("model stream exceeds size limit".into()));
                }
                Ok(chunk)
            }).eventsource();
            let mut calls = BTreeMap::<usize, CallDelta>::new();
            let mut usage = None;
            let mut finish = None;
            while let Some(event) = events.next().await {
                let event = event.map_err(|_| Error::Model("broken SSE stream".into()))?;
                if event.data == "[DONE]" {
                    match finish.as_deref() {
                        Some("stop") if calls.is_empty() => {},
                        Some("tool_calls") if !calls.is_empty() => {},
                        _ => Err(Error::Model("model response did not finish successfully".into()))?,
                    }
                    let calls = calls.into_values().map(|call| ToolCall {
                        id: call.id, name: call.name, arguments: call.arguments,
                    }).collect();
                    yield ModelEvent::Complete { calls, usage };
                    return;
                }
                let chunk: Chunk = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("malformed SSE chunk".into()))?;
                if chunk.error.is_some() {
                    Err(Error::Model("provider returned a stream error".into()))?;
                }
                if let Some(stats) = chunk.usage {
                    usage = Some(Usage { input_tokens: stats.prompt_tokens, output_tokens: stats.completion_tokens });
                }
                for choice in chunk.choices {
                    if choice.index != 0 || finish.is_some() {
                        Err(Error::Model("unexpected choice in model stream".into()))?;
                    }
                    if let Some(content) = choice.delta.content.filter(|content| !content.is_empty()) {
                        yield ModelEvent::Text(content);
                    }
                    for delta in choice.delta.tool_calls.unwrap_or_default() {
                        if delta.index >= 64 {
                            Err(Error::Model("invalid tool call index".into()))?;
                        }
                        let call = calls.entry(delta.index).or_default();
                        call.id.push_str(delta.id.as_deref().unwrap_or(""));
                        if let Some(function) = delta.function {
                            call.name.push_str(function.name.as_deref().unwrap_or(""));
                            call.arguments.push_str(function.arguments.as_deref().unwrap_or(""));
                        }
                    }
                    finish = choice.finish_reason;
                }
            }
            Err(Error::Model("SSE closed before the end marker".into()))?;
        }))
    }
}

fn encode_message(message: Message) -> Value {
    match message {
        Message::System { content } => json!({"role": "system", "content": content}),
        Message::User { content } => json!({"role": "user", "content": content}),
        Message::Tool { call_id, content } => {
            json!({"role": "tool", "tool_call_id": call_id, "content": content})
        }
        Message::Assistant { content, calls } => {
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
            message
        }
    }
}

#[derive(Deserialize)]
struct Chunk {
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
}
