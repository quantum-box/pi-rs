//! Opt-in durable Responses adapter. Wire encoding and event interpretation are
//! adapted from pi-rs-openai under the repository's MIT license.
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay,
    ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use pi_rs_durable::model::{
    DurableRequest, DurableToolRequest, ProviderAdapter, ProviderAttempt, ProviderResultRequest,
};
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
    model_inputs: pi_rs_durable::tool_projection::ModelInputs,
}
impl OpenAiResponsesAdapter {
    /// Override the bundled model image profile using an upstream-shaped descriptor.
    pub fn set_model_descriptor(
        &mut self,
        model: impl Into<String>,
        descriptor: &Value,
    ) -> Result<()> {
        self.model_inputs.insert(model, descriptor)
    }
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
            model_inputs: Default::default(),
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

fn durable_payload(durable: &DurableRequest) -> Result<Value> {
    let request = &durable.request;
    if durable.blocks.is_empty() {
        return Ok(payload(request, "openai"));
    }
    let mut ids = std::collections::HashSet::new();
    if durable
        .blocks
        .values()
        .any(|binding| binding.message_id.is_nil() || !ids.insert(binding.message_id))
    {
        return Err(Error::Config("invalid durable occurrence IDs".into()));
    }
    let mut body = payload(request, "openai");
    let mut input = Vec::new();
    for (index, message) in request.messages.iter().enumerate() {
        if let Some(binding) = durable.blocks.get(&index) {
            if !matches!(message, Message::User { content } if content.is_empty()) {
                return Err(Error::Config("durable block placeholder mismatch".into()));
            }
            let mut blocks = Vec::new();
            for block in &binding.blocks {
                let object = block
                    .as_object()
                    .ok_or_else(|| Error::Config("invalid content block".into()))?;
                match block["type"].as_str() {
                    Some("text") if object.len() == 2 => {
                        let text = block["text"]
                            .as_str()
                            .ok_or_else(|| Error::Config("invalid text block".into()))?;
                        blocks.push(json!({"type":"input_text", "text":text}));
                    }
                    Some("image")
                        if object.keys().all(|k| {
                            ["type", "data", "mimeType", "detail"].contains(&k.as_str())
                        }) =>
                    {
                        let image = pi_rs_core::ImageContent {
                            data: block["data"]
                                .as_str()
                                .ok_or_else(|| Error::Config("invalid image".into()))?
                                .into(),
                            mime_type: block["mimeType"]
                                .as_str()
                                .ok_or_else(|| Error::Config("invalid image".into()))?
                                .into(),
                        };
                        image.validate()?;
                        let mut encoded = json!({"type":"input_image", "image_url":format!("data:{};base64,{}", image.mime_type, image.data)});
                        if let Some(detail) = object.get("detail") {
                            if !matches!(detail.as_str(), Some("low" | "high" | "auto")) {
                                return Err(Error::Config("invalid image detail".into()));
                            }
                            encoded["detail"] = detail.clone();
                        }
                        blocks.push(encoded);
                    }
                    _ => return Err(Error::Config("unsupported durable block metadata".into())),
                }
            }
            if blocks.is_empty() {
                return Err(Error::Config(
                    "empty user block array unsupported by Responses".into(),
                ));
            }
            input.push(json!({"role":"user", "content":blocks}));
        } else {
            let mut one = request.clone();
            one.messages = vec![message.clone()];
            input.extend(
                payload(&one, "openai")["input"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .cloned(),
            );
        }
    }
    if durable.blocks.keys().any(|i| *i >= request.messages.len()) {
        return Err(Error::Config("durable block index out of bounds".into()));
    }
    body["input"] = json!(input);
    Ok(body)
}

fn result_payload(
    request: &ProviderResultRequest,
    profile: Option<pi_rs_durable::images::ModelImageProfile>,
) -> Result<Value> {
    use pi_rs_durable::tool_projection::{self, Block};
    request.validate_bindings()?;
    let request = tool_projection::project_request(request, profile);
    let tool = &request.request;
    let mut body = durable_payload(&tool.request)?;
    let input = body["input"]
        .as_array_mut()
        .ok_or_else(|| Error::Config("invalid input".into()))?;
    for (index, message) in tool.request.request.messages.iter().enumerate() {
        let Message::Tool { call_id, .. } = message else {
            continue;
        };
        let positions: Vec<_> = input
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                item["type"] == "function_call_output" && item["call_id"] == *call_id
            })
            .map(|(i, _)| i)
            .collect();
        if positions.len() != 1 {
            return Err(Error::Config("ambiguous tool result call ID".into()));
        }
        let mut texts = Vec::new();
        let mut images = Vec::new();
        for block in tool_projection::blocks(tool, index, true)? {
            match block {
                Block::Text(text) => texts.push(text),
                Block::Image { image, detail } => images.push((image, detail)),
            }
        }
        let text = texts.join("\n");
        let output = if images.is_empty() {
            json!(if tool_projection::has_text(&text) {
                text.as_str()
            } else {
                "(no tool output)"
            })
        } else {
            let mut values = Vec::new();
            if tool_projection::has_text(&text) {
                values.push(json!({"type":"input_text","text":text}));
            }
            for (image, detail) in images {
                values.push(json!({"type":"input_image","detail":detail.as_deref().unwrap_or("auto"),"image_url":format!("data:{};base64,{}",image.mime_type,image.data)}));
            }
            json!(values)
        };
        input[positions[0]]["output"] = output;
    }
    Ok(body)
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
    fn image_profile(&self, model: &str) -> Option<pi_rs_durable::images::ModelImageProfile> {
        self.model_inputs.profile(self.id(), model)
    }
    fn supports_tool_result_metadata(&self) -> bool {
        true
    }
    async fn prepare_with_result_metadata(
        &self,
        _: &ProviderAttempt,
        request: &ProviderResultRequest,
    ) -> Result<Value> {
        result_payload(
            request,
            self.image_profile(&request.request.request.request.model),
        )
    }
    fn validate_with_result_metadata(
        &self,
        attempt: &ProviderAttempt,
        request: &ProviderResultRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate(attempt, &request.request.request.request, body)?;
        if body.get("input")
            != result_payload(
                request,
                self.image_profile(&request.request.request.request.model),
            )?
            .get("input")
        {
            return Err(Error::Config("provider hook changed durable input".into()));
        }
        Ok(())
    }
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &ProviderAttempt,
        request: &DurableToolRequest,
    ) -> Result<Value> {
        result_payload(
            &ProviderResultRequest {
                request: request.clone(),
                tool_errors: Default::default(),
            },
            self.image_profile(&request.request.request.model),
        )
    }
    fn validate_with_tool_blocks(
        &self,
        attempt: &ProviderAttempt,
        request: &DurableToolRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate_with_result_metadata(
            attempt,
            &ProviderResultRequest {
                request: request.clone(),
                tool_errors: Default::default(),
            },
            body,
        )
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    async fn prepare_durable(
        &self,
        _: &ProviderAttempt,
        request: &DurableRequest,
    ) -> Result<Value> {
        result_payload(
            &pi_rs_durable::tool_projection::wrap_durable(request),
            self.image_profile(&request.request.model),
        )
    }
    fn validate_durable(
        &self,
        attempt: &ProviderAttempt,
        request: &DurableRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate(attempt, &request.request, body)?;
        if (!request.blocks.is_empty()
            || request
                .request
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. })))
            && body.get("input")
                != result_payload(
                    &pi_rs_durable::tool_projection::wrap_durable(request),
                    self.image_profile(&request.request.model),
                )?
                .get("input")
        {
            return Err(Error::Config("provider hook changed durable input".into()));
        }
        Ok(())
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value> {
        result_payload(
            &pi_rs_durable::tool_projection::wrap_plain(request),
            self.image_profile(&request.model),
        )
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
