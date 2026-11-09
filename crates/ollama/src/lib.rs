//! Streaming adapter for Ollama's /v1/chat/completions interface.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};

use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Result, ToolCall,
    Usage,
};

type ReasoningObserver = Arc<dyn Fn(&str) + Send + Sync>;

#[derive(Clone)]
pub struct Ollama {
    client: Client,
    endpoint: Url,
    token: Option<String>,
    progress: Option<Arc<dyn Fn(ModelProgress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
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
            progress: None,
            reasoning: None,
        })
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

    async fn model_loaded(&self, model: &str) -> Option<bool> {
        let mut endpoint = self.endpoint.clone();
        endpoint.set_path("/api/ps");
        let response = self
            .client
            .get(endpoint)
            .timeout(Duration::from_millis(250))
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?;
        let value: Value = response.json().await.ok()?;
        Some(value.get("models")?.as_array()?.iter().any(|item| {
            item.get("name")
                .or_else(|| item.get("model"))
                .and_then(Value::as_str)
                .is_some_and(|name| {
                    name.trim_end_matches(":latest") == model.trim_end_matches(":latest")
                })
        }))
    }
}

#[async_trait::async_trait]
impl Model for Ollama {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let requested_model = request.model.clone();
        let mut last_progress = None;
        report_progress(
            &self.progress,
            &mut last_progress,
            ModelProgress::WaitingForModel,
        );
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
        let monitor_loading = self.progress.is_some()
            && !requested_model.ends_with(":cloud")
            && self.endpoint.port_or_known_default() == Some(11434)
            && matches!(
                self.endpoint.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]")
            );
        let response = builder.send();
        tokio::pin!(response);
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        let response = loop {
            tokio::select! {
                result = &mut response => break result.map_err(|_| Error::RetryableModel("HTTP connection failed".into()))?,
                _ = poll.tick(), if monitor_loading => {
                    if let Some(loaded) = self.model_loaded(&requested_model).await {
                        report_progress(&self.progress, &mut last_progress, if loaded {
                            ModelProgress::WaitingForModel
                        } else {
                            ModelProgress::LoadingModel
                        });
                    }
                }
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            let description = format!("HTTP status {status}");
            return Err(
                if status.is_server_error() || matches!(status.as_u16(), 408 | 429) {
                    Error::RetryableModel(description)
                } else {
                    Error::Model(description)
                },
            );
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
            let mut metadata = ModelMetadata::default();
            let mut reasoning_text = String::new();
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
                let Some(event) = event else { break; };
                let event = event.map_err(|_| Error::RetryableModel("broken SSE stream".into()))?;
                if event.data == "[DONE]" {
                    match metadata.finish_reason.as_deref() {
                        Some("stop") if calls.is_empty() => {},
                        Some("tool_calls") if !calls.is_empty() => {},
                        _ => Err(Error::Model("model response did not finish successfully".into()))?,
                    }
                    let calls = calls.into_values().map(|call| ToolCall {
                        id: call.id, name: call.name, arguments: call.arguments,
                    }).collect();
                    if !reasoning_text.is_empty() {
                        metadata.reasoning = Some(reasoning_text);
                        yield ModelEvent::Metadata(metadata);
                    }
                    yield ModelEvent::Complete { calls, usage };
                    return;
                }
                let chunk: Chunk = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("malformed SSE chunk".into()))?;
                if chunk.error.is_some() {
                    Err(Error::Model("provider returned a stream error".into()))?;
                }
                let previous = metadata.clone();
                if let Some(id) = chunk.id
                    && id.len() <= 128 && id.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                {
                    metadata.provider_request_id = Some(id);
                }
                if let Some(stats) = chunk.usage {
                    usage = Some(Usage { input_tokens: stats.prompt_tokens, output_tokens: stats.completion_tokens });
                    metadata.cached_input_tokens = stats.prompt_tokens_details.and_then(|details| details.cached_tokens);
                }
                for choice in chunk.choices {
                    if choice.index != 0 || metadata.finish_reason.is_some() {
                        Err(Error::Model("unexpected choice in model stream".into()))?;
                    }
                    if let Some(text) = choice.delta.reasoning.as_ref().or(choice.delta.reasoning_content.as_ref()).filter(|text| !text.is_empty()) {
                        report_progress(&progress, &mut last_progress, ModelProgress::Thinking);
                        reasoning_text.push_str(text);
                        if let Some(observer) = &reasoning { observer(text); }
                    }
                    if let Some(content) = choice.delta.content.filter(|content| !content.is_empty()) {
                        report_progress(&progress, &mut last_progress, ModelProgress::Generating);
                        yield ModelEvent::Text(content);
                    }
                    for delta in choice.delta.tool_calls.unwrap_or_default() {
                        report_progress(&progress, &mut last_progress, ModelProgress::PreparingTool);
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
                    metadata.finish_reason = choice.finish_reason.map(|reason| {
                        match reason.as_str() {
                            "stop" | "tool_calls" | "length" | "content_filter" => reason,
                            _ => "other".into(),
                        }
                    });
                }
                if metadata != previous {
                    yield ModelEvent::Metadata(metadata.clone());
                }
            }
            Err(Error::RetryableModel("SSE closed before the end marker".into()))?;
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
    prompt_tokens_details: Option<PromptTokenDetails>,
}

#[derive(Deserialize)]
struct PromptTokenDetails {
    cached_tokens: Option<u64>,
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
