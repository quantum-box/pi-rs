//! Opt-in durable Responses adapter. Wire encoding and event interpretation are
//! adapted from pi-rs-openai under the repository's MIT license.
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay,
    ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use pi_rs_durable::model::{ProviderAdapter, ProviderAttempt};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::IpAddr, time::Duration};

const MAX_STREAM: usize = 4 * 1024 * 1024;
const NAMESPACE: &str = "pi";

/// Explicitly installed API-key Responses adapter. Credentials never enter hook payloads.
pub struct OpenAiResponsesAdapter {
    client: Client,
    endpoint: Url,
    token: String,
}
impl OpenAiResponsesAdapter {
    /// Accept HTTPS or numeric-loopback HTTP. The caller controls the endpoint and token.
    pub fn new(endpoint: &str, token: String) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid Responses endpoint".into()))?;
        let loopback = endpoint
            .host_str()
            .and_then(|s| s.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if endpoint.host().is_none()
            || !(endpoint.scheme() == "https" || (endpoint.scheme() == "http" && loopback))
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Config("invalid Responses endpoint".into()));
        }
        if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::Config("invalid bearer token".into()));
        }
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| Error::Config("could not initialize Responses client".into()))?;
        Ok(Self {
            client,
            endpoint,
            token,
        })
    }
}
fn payload(request: &ModelRequest, provider: &str) -> Value {
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
                    r.identity.provider == provider && r.identity.model == request.model
                }) {
                    input.extend(replay.blocks.iter().cloned());
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
            Message::Tool { call_id, content } => {
                let (text, images) = pi_rs_core::extract_tool_images(content.clone());
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
impl ProviderAdapter for OpenAiResponsesAdapter {
    fn id(&self) -> &str {
        "openai-responses-api/v1"
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value> {
        Ok(payload(request, "openai"))
    }
    fn validate(&self, _: &ProviderAttempt, request: &ModelRequest, body: &Value) -> Result<()> {
        let fail = || Error::Config("invalid or unauthorized Responses request body".into());
        let obj = body.as_object().ok_or_else(fail)?;
        // Only support fields for which the adapter can enforce the host's basic limits.
        const ALLOWED: &[&str] = &[
            "model",
            "input",
            "store",
            "stream",
            "max_output_tokens",
            "tools",
            "reasoning",
            "include",
            "temperature",
            "top_p",
            "tool_choice",
            "parallel_tool_calls",
            "text",
            "truncation",
            "prompt_cache_key",
            "safety_identifier",
        ];
        if obj.keys().any(|k| !ALLOWED.contains(&k.as_str()))
            || body["model"] != request.model
            || body["stream"] != true
            || body["store"] != false
            || !body["input"].is_array()
            || body["max_output_tokens"]
                .as_u64()
                .is_none_or(|n| n == 0 || n > u64::from(request.max_output_tokens))
            || serde_json::to_vec(body)?.len() > 4 * 1024 * 1024
        {
            return Err(fail());
        }
        let expected = payload(request, "openai");
        if obj.get("tools") != expected.as_object().and_then(|o| o.get("tools")) {
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
        let model = request.model.clone();
        let provider = "openai".to_owned();
        let response = self
            .client
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("OpenAI request failed".into()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(response_error(None, Some(status)));
        }
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"))
        {
            return Err(Error::Model("expected OpenAI event stream".into()));
        }
        let mut size = 0usize;
        let mut source = response
            .bytes_stream()
            .map(move |chunk| {
                let chunk =
                    chunk.map_err(|_| Error::RetryableModel("OpenAI stream interrupted".into()))?;
                size = size.saturating_add(chunk.len());
                if size > MAX_STREAM {
                    return Err(Error::Model("OpenAI stream exceeds 4 MiB".into()));
                }
                Ok(chunk)
            })
            .eventsource();
        Ok(Box::pin(async_stream::try_stream! {

            let mut calls: BTreeMap<u64, ToolCall> = BTreeMap::new();
            let mut reasoning = String::new();
            while let Some(item) = source.next().await {
                let item = item.map_err(|e| match e { eventsource_stream::EventStreamError::Transport(e) => e, _ => Error::RetryableModel("OpenAI stream interrupted".into()) })?;
                if item.data == "[DONE]" { continue; }
                let event: Value = serde_json::from_str(&item.data).map_err(|_| Error::Model("invalid OpenAI stream event".into()))?;
                match field(&event, "type")? {
                    "response.output_text.delta" | "response.refusal.delta" => yield ModelEvent::Text(field(&event,"delta")?.into()),
                    "response.reasoning_summary_text.delta" => { let text = field(&event,"delta")?; reasoning.push_str(text); yield ModelEvent::Reasoning(text.into()); },
                    "response.output_item.added" if event["item"]["type"] == "function_call" => {
                        let index = event["output_index"].as_u64().ok_or_else(|| Error::Model("invalid OpenAI tool index".into()))?;
                        let item = &event["item"];
                        if provider == "openai-chatgpt" && item["namespace"] != NAMESPACE { Err(Error::Model("unexpected OpenAI tool namespace".into()))?; }
                        let call = ToolCall { id:field(item,"call_id")?.into(), name:field(item,"name")?.into(), arguments:String::new() };
                        if index >= 64 || calls.values().any(|existing| existing.id == call.id) || calls.insert(index, call.clone()).is_some() { Err(Error::Model("duplicate OpenAI tool index".into()))?; }
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
                            (Some("response.incomplete"), Some("incomplete"), _) => {
                                Err(Error::Model("OpenAI response incomplete".into()))?
                            }
                            _ => Err(Error::Model("invalid OpenAI completion status".into()))?,
                        };
                        let output = response["output"].as_array().ok_or_else(|| Error::Model("OpenAI completion missing output".into()))?;
                        let reported_calls: Vec<_> = output.iter().filter(|item| item["type"] == "function_call").collect();
                        if reported_calls.len() != calls.len() { Err(Error::Model("incomplete OpenAI tool stream".into()))?; }
                        for (index, call) in &calls {
                            let item = output.get(*index as usize).ok_or_else(|| Error::Model("incomplete OpenAI tool stream".into()))?;
                            if item["type"] != "function_call" || item["call_id"] != call.id || item["name"] != call.name || item["arguments"] != call.arguments || (provider == "openai-chatgpt" && item["namespace"] != NAMESPACE) { Err(Error::Model("OpenAI tool call changed during stream".into()))?; }
                            if finish_reason != "length" && (call.arguments.trim().is_empty() || item.get("status").is_some_and(|status| status != "completed")) { Err(Error::Model("incomplete OpenAI tool call".into()))?; }
                        }
                        let usage = response["usage"]["input_tokens"].as_u64().zip(response["usage"]["output_tokens"].as_u64()).map(|(input_tokens,output_tokens)| Usage { input_tokens, output_tokens });
                        let identity = ResolvedModelIdentity { provider:provider.clone(), model:model.clone() };
                        yield ModelEvent::Metadata(ModelMetadata { provider_api:Some(pi_rs_core::ModelApi::OpenaiResponses), resolved_model:Some(identity.clone()), finish_reason:Some(finish_reason.into()), cached_input_tokens:response["usage"]["input_tokens_details"]["cached_tokens"].as_u64(), reasoning:(!reasoning.is_empty()).then_some(reasoning), replay:(finish_reason != "length").then(|| Box::new(ProviderReplay { identity, blocks:output.clone() })), ..Default::default() });
                        yield ModelEvent::Complete { calls:calls.into_values().collect(), usage };
                        return;
                    }
                    "response.failed" | "error" => Err(response_error(event["response"]["error"]["code"].as_str().or_else(|| event["error"]["code"].as_str()), None))?,
                    _ => {}
                }
            }
            Err(Error::RetryableModel("OpenAI stream ended without response.completed".into()))?;
        }))
    }
}
