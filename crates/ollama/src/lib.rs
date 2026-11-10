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
    reasoning_effort: Option<String>,
    native_probes: bool,
    progress: Option<Arc<dyn Fn(ModelProgress) + Send + Sync>>,
    reasoning: Option<ReasoningObserver>,
}

/// Capabilities reported by the native Ollama service.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
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
            reasoning_effort: None,
            native_probes: true,
            progress: None,
            reasoning: None,
        })
    }

    /// Enable or suppress auxiliary native endpoint probes.
    pub fn with_native_probes(mut self, enabled: bool) -> Self {
        self.native_probes = enabled;
        self
    }

    /// Override provider thinking effort. None preserves the model default.
    pub fn with_reasoning_effort(mut self, effort: Option<&str>) -> Self {
        let local_ollama = self.endpoint.port_or_known_default() == Some(11434);
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
        self.reasoning_effort =
            effort.map(|level| if level == "off" { "none" } else { level }.to_owned());
        self
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
        let mut request = self.client.get(endpoint).timeout(Duration::from_secs(10));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
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
        let mut request = self
            .client
            .post(endpoint)
            .timeout(Duration::from_millis(750))
            .json(&json!({"model": model}));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let value = read_native_response(request).await?;
        Some(decode_model_details(&value))
    }

    async fn running_models(&self) -> Option<Value> {
        let mut request = self
            .client
            .get(self.native_endpoint("ps")?)
            .timeout(Duration::from_millis(250));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
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
    async fn context_window(&self, model: &str) -> Option<u32> {
        let value = self.running_models().await?;
        running_context_window(&value, model)
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let requested_model = request.model.clone();
        let mut last_progress = None;
        report_progress(
            &self.progress,
            &mut last_progress,
            ModelProgress::WaitingForModel,
        );
        let messages: Vec<_> = request
            .messages
            .into_iter()
            .flat_map(encode_message)
            .collect();
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
        if let Some(effort) = &self.reasoning_effort {
            body["reasoning_effort"] = json!(effort);
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
                        Some("stop" | "length") if calls.is_empty() => {},
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

#[cfg(test)]
mod content_tests {
    use super::*;
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
    fn image_tool_results_keep_pairing_and_move_images_to_a_user_content_block() {
        let content = json!({"data":{"content":[{"type":"text","text":"A chart"},{"type":"image","mimeType":"image/png","data":"aW1hZ2U="}]},"attachments":[]}).to_string();
        let messages = encode_tool_result("call-1", content);
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
            encode_tool_result("text", "unchanged".into())[0]["content"],
            "unchanged"
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
