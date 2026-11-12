//! OpenAI Responses transport for ChatGPT plan OAuth credentials.
//!
//! Credential acquisition and renewal are the responsibility of the caller.
pub mod oauth;

use eventsource_stream::Eventsource;
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

#[derive(Clone)]
pub struct OpenAiResponses {
    client: Client,
    endpoint: Url,
    token: String,
    thinking: Option<String>,
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
        self.client = builder
            .build()
            .map_err(|_| Error::Config("could not initialize OpenAI client".into()))?;
        Ok(self)
    }
    pub fn new(endpoint: &str, token: String) -> Result<Self> {
        let endpoint = Url::parse(endpoint)
            .map_err(|_| Error::Config("invalid OpenAI Responses endpoint".into()))?;
        let secure = endpoint.as_str() == DEFAULT_ENDPOINT
            || (endpoint.scheme() == "http"
                && matches!(
                    endpoint.host_str(),
                    Some("127.0.0.1" | "localhost" | "[::1]")
                ));
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
        })
    }

    pub fn with_thinking(mut self, level: Option<&str>) -> Self {
        self.thinking = level.map(|level| if level == "off" { "none" } else { level }.to_owned());
        self
    }

    /// Return the signed-in account's displayable models in server order.
    pub async fn models(&self) -> Result<Vec<String>> {
        let mut endpoint = self.endpoint.clone();
        endpoint.set_path("/v1/models");
        let mut response = self
            .client
            .get(endpoint)
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
        let models = body["models"]
            .as_array()
            .ok_or_else(|| Error::Model("invalid OpenAI model listing".into()))?;
        Ok(models
            .iter()
            .filter(|entry| entry["visibility"] == "list")
            .filter_map(|entry| {
                entry["slug"]
                    .as_str()
                    .filter(|slug| !slug.is_empty())
                    .map(str::to_owned)
            })
            .collect())
    }
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

fn payload(request: &ModelRequest) -> Value {
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
                    r.identity.provider == "openai-chatgpt" && r.identity.model == request.model
                }) {
                    input.extend(replay.blocks.iter().cloned());
                } else {
                    if !content.is_empty() {
                        input.push(json!({"role":"assistant", "content":content}));
                    }
                    input.extend(calls.iter().map(|call| json!({"type":"function_call", "call_id":call.id, "name":call.name, "namespace":NAMESPACE, "arguments":call.arguments})));
                }
            }
            Message::Tool { call_id, content } => input
                .push(json!({"type":"function_call_output", "call_id":call_id, "output":content})),
        }
    }
    let mut body = json!({"model":request.model, "input":input, "store":false, "stream":true});
    if !request.tools.is_empty() {
        let tools: Vec<_> = request.tools.iter().map(|tool| json!({"type":"function", "name":tool.name, "description":tool.description, "parameters":tool.parameters, "strict":false})).collect();
        body["tools"] = json!([{"type":"namespace", "name":NAMESPACE, "description":"Local tools", "tools":tools}]);
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
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let model = request.model.clone();
        let mut body = payload(&request);
        if let Some(thinking) = &self.thinking {
            body["reasoning"] = json!({"effort":thinking, "summary":"auto"});
            body["include"] = json!(["reasoning.encrypted_content"]);
        }
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("OpenAI request failed".into()))?;
        if !response.status().is_success() {
            let status = response.status();
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
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected OpenAI event stream".into()));
        }
        let mut source = response.bytes_stream().eventsource();
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0usize;
            let mut calls: BTreeMap<u64, ToolCall> = BTreeMap::new();
            let mut reasoning = String::new();
            while let Some(item) = source.next().await {
                let item = item.map_err(|_| Error::RetryableModel("OpenAI stream interrupted".into()))?;
                size = size.saturating_add(item.data.len());
                if size > MAX_STREAM { Err(Error::Model("OpenAI stream exceeds 32 MiB".into()))?; }
                if item.data == "[DONE]" { continue; }
                let event: Value = serde_json::from_str(&item.data).map_err(|_| Error::Model("invalid OpenAI stream event".into()))?;
                match field(&event, "type")? {
                    "response.output_text.delta" | "response.refusal.delta" => yield ModelEvent::Text(field(&event,"delta")?.into()),
                    "response.reasoning_summary_text.delta" => { let text = field(&event,"delta")?; reasoning.push_str(text); yield ModelEvent::Reasoning(text.into()); },
                    "response.output_item.added" if event["item"]["type"] == "function_call" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let item = &event["item"];
                        if item["namespace"] != NAMESPACE { Err(Error::Model("unexpected OpenAI tool namespace".into()))?; }
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
                    "response.completed" => {
                        let response = &event["response"];
                        if response["status"] != "completed" { Err(Error::Model("invalid OpenAI completion status".into()))?; }
                        let output = response["output"].as_array().ok_or_else(|| Error::Model("OpenAI completion missing output".into()))?;
                        let reported_calls: Vec<_> = output.iter().filter(|item| item["type"] == "function_call").collect();
                        if reported_calls.len() != calls.len() { Err(Error::Model("incomplete OpenAI tool stream".into()))?; }
                        for (call, item) in calls.values().zip(reported_calls) {
                            if item["call_id"] != call.id || item["name"] != call.name || item["arguments"] != call.arguments || item["namespace"] != NAMESPACE { Err(Error::Model("OpenAI tool call changed during stream".into()))?; }
                        }
                        let usage = response["usage"]["input_tokens"].as_u64().zip(response["usage"]["output_tokens"].as_u64()).map(|(input_tokens,output_tokens)| Usage { input_tokens, output_tokens });
                        let identity = ResolvedModelIdentity { provider:"openai-chatgpt".into(), model:model.clone() };
                        yield ModelEvent::Metadata(ModelMetadata { provider_api:Some(pi_rs_core::ModelApi::OpenaiResponses), resolved_model:Some(identity.clone()), finish_reason:Some("stop".into()), cached_input_tokens:response["usage"]["input_tokens_details"]["cached_tokens"].as_u64(), reasoning:(!reasoning.is_empty()).then_some(reasoning), replay:Some(Box::new(ProviderReplay { identity, blocks:output.clone() })), ..Default::default() });
                        yield ModelEvent::Complete { calls:calls.into_values().collect(), usage };
                        return;
                    }
                    "response.failed" | "response.incomplete" | "error" => Err(response_error(event["response"]["error"]["code"].as_str().or_else(|| event["error"]["code"].as_str()), None))?,
                    _ => {}
                }
            }
            Err(Error::RetryableModel("OpenAI stream ended without response.completed".into()))?;
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn plan_payload_uses_stateless_stream_and_namespaced_tools() {
        let body = payload(&request());
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        assert_eq!(body["input"][0]["role"], "developer");
        assert_eq!(body["tools"][0]["type"], "namespace");
        assert_eq!(body["tools"][0]["tools"][0]["name"], "read");
        assert!(body.get("max_output_tokens").is_none());
        assert!(OpenAiResponses::new("http://example.com/responses", "token".into()).is_err());
    }

    async fn fixture(events: &str) -> (OpenAiResponses, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = OpenAiResponses::new(
            &format!("http://{}/responses", listener.local_addr().unwrap()),
            "fixture".into(),
        )
        .unwrap();
        let body = events.to_owned();
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
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        (client, server)
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
