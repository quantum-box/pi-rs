//! Opt-in durable Google Gemini adapter. Wire encoding and SSE decoding are
//! adapted from crates/google under the repository MIT license.
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use pi_rs_core::{
    Error, Message, ModelApi, ModelEvent, ModelMetadata, ModelRequest, ModelStream, ProviderReplay,
    ResolvedModelIdentity, Result, ToolCall, Usage, extract_tool_images,
};
use pi_rs_durable::model::{DurableRequest, DurableToolRequest, ProviderAdapter, ProviderAttempt};
use reqwest::{Client, Response, Url, header::HeaderValue};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    time::Duration,
};
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// API-key adapter; its base URL and credentials are controlled by the host.
pub struct GoogleAdapter {
    client: Client,
    endpoint: Url,
    key: HeaderValue,
}
impl GoogleAdapter {
    pub fn new(endpoint: &str, key: &str) -> Result<Self> {
        let endpoint =
            Url::parse(endpoint).map_err(|_| Error::Config("invalid Google endpoint".into()))?;
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
            return Err(Error::Config("invalid Google endpoint".into()));
        }
        let mut key = HeaderValue::from_str(key)
            .ok()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| Error::Config("invalid Google API key".into()))?;
        key.set_sensitive(true);
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(|_| Error::Config("could not initialize Google client".into()))?;
        Ok(Self {
            client,
            endpoint,
            key,
        })
    }
}
fn model_id(model: &str) -> &str {
    model.strip_prefix("models/").unwrap_or(model)
}

fn valid_model_id(id: &str) -> bool {
    !id.is_empty()
        && !matches!(id, "." | "..")
        && id.len() <= 160
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ThinkingFamily {
    LevelPro,
    LevelFlash,
    BudgetPro,
    BudgetFlash,
}

fn family(id: &str) -> Option<ThinkingFamily> {
    if gemini_major(id).is_some_and(|version| version >= 3)
        || matches!(id, "gemini-flash-latest" | "gemini-flash-lite-latest")
        || id.starts_with("gemma-4")
        || id.starts_with("gemma4-")
    {
        Some(if id.contains("-pro") {
            ThinkingFamily::LevelPro
        } else {
            ThinkingFamily::LevelFlash
        })
    } else if id.starts_with("gemini-2.5") {
        Some(if id.contains("-pro") {
            ThinkingFamily::BudgetPro
        } else {
            ThinkingFamily::BudgetFlash
        })
    } else {
        None
    }
}

fn gemini_major(id: &str) -> Option<u32> {
    let version = id.strip_prefix("gemini-")?;
    let version = version.strip_prefix("live-").unwrap_or(version);
    version.split(['.', '-']).next()?.parse().ok()
}

/// Whether the model expects call IDs in both function calls and their responses.
pub fn uses_tool_call_ids(model: &str) -> bool {
    let id = model_id(model);
    gemini_major(id).is_some_and(|version| version >= 3)
        || id.starts_with("claude-")
        || id.starts_with("gpt-oss-")
}

pub fn model_thinking_levels(id: &str) -> Vec<String> {
    let id = model_id(id);
    available_thinking(id, family(id).is_some())
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn available_thinking(id: &str, enabled: bool) -> Vec<&'static str> {
    if !enabled {
        return vec!["off"];
    }
    match family(id) {
        Some(ThinkingFamily::LevelPro) => vec!["low", "high"],
        Some(ThinkingFamily::LevelFlash) => vec!["minimal", "low", "medium", "high"],
        Some(ThinkingFamily::BudgetPro) => vec!["minimal", "low", "medium", "high"],
        Some(ThinkingFamily::BudgetFlash) => {
            vec!["off", "minimal", "low", "medium", "high"]
        }
        None => vec!["low"],
    }
}

fn thinking_config(id: &str, level: Option<&str>, max_tokens: u32) -> Option<Value> {
    let level = level?;
    let family = family(id)?;
    match family {
        ThinkingFamily::LevelPro | ThinkingFamily::LevelFlash => {
            let high = matches!(level, "high" | "xhigh" | "max");
            let native = if high {
                "HIGH"
            } else if family == ThinkingFamily::LevelPro {
                "LOW"
            } else {
                match level {
                    "off" | "minimal" => "MINIMAL",
                    "medium" => "MEDIUM",
                    _ => "LOW",
                }
            };
            Some(json!({"thinkingLevel":native,"includeThoughts":level!="off"}))
        }
        ThinkingFamily::BudgetPro | ThinkingFamily::BudgetFlash => {
            let minimum = if family == ThinkingFamily::BudgetPro {
                128
            } else {
                0
            };
            let requested = match level {
                "off" => 0,
                "minimal" if id.contains("flash-lite") => 512,
                "minimal" => 128,
                "low" => 2_048,
                "medium" => 8_192,
                _ if family == ThinkingFamily::BudgetPro => 32_768,
                _ => 24_576,
            };
            let budget = requested.min(max_tokens.saturating_sub(1)).max(minimum);
            Some(json!({"thinkingBudget":budget,"includeThoughts":level!="off"}))
        }
    }
}

fn check_status(response: Response) -> Result<Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let message = format!("Google Gemini returned HTTP {status}");
    if status.as_u16() == 429 || status.is_server_error() {
        Err(Error::RetryableModel(message))
    } else {
        Err(Error::Model(message))
    }
}

fn identity(provider: &str, model: &str) -> ResolvedModelIdentity {
    ResolvedModelIdentity {
        provider: provider.into(),
        model: model.into(),
    }
}

fn push_content(contents: &mut Vec<Value>, role: &str, parts: Vec<Value>, merge: bool) {
    if parts.is_empty() {
        return;
    }
    let tool_result = parts
        .first()
        .is_some_and(|part| part.get("functionResponse").is_some());
    if merge
        && let Some(last) = contents.last_mut().filter(|last| {
            last["role"] == role
                && last["parts"][0].get("functionResponse").is_some() == tool_result
        })
    {
        last["parts"]
            .as_array_mut()
            .expect("content parts")
            .extend(parts);
    } else {
        contents.push(json!({"role":role,"parts":parts}));
    }
}

fn wire_request(
    request: &ModelRequest,
    thinking: Option<&str>,
    replay_scope: &str,
) -> Result<Value> {
    wire_request_with_blocks(request, thinking, replay_scope, None, None)
}

fn wire_request_with_blocks(
    request: &ModelRequest,
    thinking: Option<&str>,
    replay_scope: &str,
    durable: Option<&DurableRequest>,
    tools: Option<&DurableToolRequest>,
) -> Result<Value> {
    if let Some(durable) = durable {
        let mut ids = BTreeSet::new();
        if durable.blocks.iter().any(|(index, binding)| {
            *index >= request.messages.len()
                || binding.message_id.is_nil()
                || !ids.insert(binding.message_id)
        }) {
            return Err(Error::Config("invalid durable content bindings".into()));
        }
    }
    let identity = identity(replay_scope, model_id(&request.model));
    let gemini_v3 = gemini_major(&identity.model).is_some_and(|version| version >= 3);
    let explicit_tool_ids = uses_tool_call_ids(&identity.model);
    let mut contents = Vec::new();
    let mut system = Vec::new();
    let mut calls = BTreeMap::new();
    let mut tool_images = Vec::new();
    for (index, message) in request.messages.iter().enumerate() {
        if !matches!(message, Message::Tool { .. }) && !tool_images.is_empty() {
            push_content(
                &mut contents,
                "user",
                std::mem::take(&mut tool_images),
                false,
            );
        }
        if let Some(binding) = durable.and_then(|d| d.blocks.get(&index)) {
            if !matches!(message, Message::User { content } if content.is_empty()) {
                return Err(Error::Config("durable block placeholder mismatch".into()));
            }
            let mut parts = Vec::new();
            for block in &binding.blocks {
                let object = block
                    .as_object()
                    .ok_or_else(|| Error::Config("invalid content block".into()))?;
                match block["type"].as_str() {
                    Some("text") if object.len() == 2 => {
                        let value = block["text"]
                            .as_str()
                            .ok_or_else(|| Error::Config("invalid text block".into()))?;
                        parts.push(json!({"text":value}));
                    }
                    Some("image")
                        if object.len() == 3
                            && object
                                .keys()
                                .all(|k| ["type", "data", "mimeType"].contains(&k.as_str())) =>
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
                        parts.push(
                            json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}}),
                        );
                    }
                    _ => return Err(Error::Config("unsupported durable block metadata".into())),
                }
            }
            if parts.is_empty() {
                return Err(Error::Config(
                    "empty user block array unsupported by Google".into(),
                ));
            }
            push_content(&mut contents, "user", parts, true);
            continue;
        }
        match message {
            Message::System { content } => system.push(content.as_str()),
            Message::User { content } => {
                push_content(&mut contents, "user", vec![json!({"text":content})], true)
            }
            Message::UserWithImages { content, images } => {
                let mut parts = Vec::new();
                if !content.is_empty() {
                    parts.push(json!({"text":content}));
                }
                for image in images {
                    image.validate()?;
                    parts
                        .push(json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}}));
                }
                push_content(&mut contents, "user", parts, true);
            }
            Message::Assistant {
                content,
                calls: invocations,
                replay,
            } => {
                let parts = if let Some(replay) =
                    replay.as_ref().filter(|replay| replay.identity == identity)
                {
                    replay.blocks.clone()
                } else {
                    let mut parts = Vec::new();
                    if !content.is_empty() {
                        parts.push(json!({"text":content}));
                    }
                    for call in invocations {
                        let arguments = serde_json::from_str::<Value>(&call.arguments)
                            .ok()
                            .filter(Value::is_object)
                            .unwrap_or_else(|| json!({}));
                        let mut part = json!({"functionCall":{"name":call.name,"args":arguments}});
                        if explicit_tool_ids {
                            part["functionCall"]["id"] = json!(call.id);
                        }
                        // Google accepts this sentinel when a turn originated on another provider.
                        if gemini_v3 {
                            part["thoughtSignature"] = json!("skip_thought_signature_validator");
                        }
                        parts.push(part);
                    }
                    parts
                };
                for (call, part) in invocations
                    .iter()
                    .zip(parts.iter().filter(|part| part["functionCall"].is_object()))
                {
                    let id = part["functionCall"]["id"].as_str();
                    calls.insert(
                        call.id.as_str(),
                        (call.name.as_str(), id.map(str::to_owned)),
                    );
                }
                push_content(&mut contents, "model", parts, false);
            }
            Message::Tool { call_id, content } => {
                let Some((name, id)) = calls.get(call_id.as_str()) else {
                    return Err(Error::Model(
                        "Google tool result has no preceding call".into(),
                    ));
                };
                let (text, images) = if let Some(binding) =
                    tools.and_then(|t| t.tool_blocks.get(&index))
                {
                    if request.messages.iter().filter(|m| matches!(m, Message::Tool { call_id: other, .. } if other == call_id)).count() != 1 {
                        return Err(Error::Config("ambiguous tool result call ID".into()));
                    }
                    let mut text = String::new();
                    let mut images = Vec::new();
                    let mut seen_text = false;
                    for block in &binding.blocks {
                        let obj = block
                            .as_object()
                            .ok_or_else(|| Error::Config("invalid tool block".into()))?;
                        match block["type"].as_str() {
                            Some("text") if obj.len() == 2 && !seen_text && images.is_empty() => {
                                text = block["text"]
                                    .as_str()
                                    .ok_or_else(|| Error::Config("invalid text".into()))?
                                    .to_string();
                                seen_text = true;
                            }
                            Some("image")
                                if obj.len() == 3
                                    && obj.keys().all(|k| {
                                        ["type", "data", "mimeType"].contains(&k.as_str())
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
                                images.push(image);
                            }
                            _ => {
                                return Err(Error::Config(
                                    "Google cannot preserve this tool block order or metadata"
                                        .into(),
                                ));
                            }
                        }
                    }
                    if binding.blocks.is_empty() || (!images.is_empty() && !gemini_v3) {
                        return Err(Error::Config(
                            "Google cannot preserve these tool blocks for this model".into(),
                        ));
                    }
                    (text, images)
                } else {
                    extract_tool_images(content.clone())
                };
                let mut response = json!({"name":name,"response":{"output":text}});
                if let Some(id) = id {
                    response["id"] = json!(id);
                }
                if gemini_v3 {
                    if !images.is_empty() {
                        response["parts"] = json!(images.iter().map(|image| json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}})).collect::<Vec<_>>());
                    }
                } else if !images.is_empty() {
                    tool_images.push(json!({"text":"Tool result image:"}));
                    tool_images.extend(images.iter().map(|image| json!({"inlineData":{"mimeType":image.mime_type,"data":image.data}})));
                }
                push_content(
                    &mut contents,
                    "user",
                    vec![json!({"functionResponse":response})],
                    true,
                );
            }
        }
    }
    push_content(&mut contents, "user", tool_images, false);
    let mut body = json!({"contents":contents,"generationConfig":{"maxOutputTokens":request.max_output_tokens,"candidateCount":1}});
    if !system.is_empty() {
        body["systemInstruction"] = json!({"parts":[{"text":system.join("\n\n")}]});
    }
    if !request.tools.is_empty() {
        let declarations = request.tools.iter().map(|tool| json!({
            "name":tool.name,"description":tool.description,"parametersJsonSchema":tool.parameters
        })).collect::<Vec<_>>();
        body["tools"] = json!([{"functionDeclarations":declarations}]);
        body["toolConfig"] = json!({"functionCallingConfig":{"mode":"AUTO"}});
    }
    if let Some(config) = thinking_config(&identity.model, thinking, request.max_output_tokens) {
        body["generationConfig"]["thinkingConfig"] = config;
    }
    Ok(body)
}

// Hooks may change conversation content, but malformed content must not be sent.
fn valid_content(body: &Value) -> bool {
    let valid_parts = |parts: &Value| {
        parts.as_array().is_some_and(|parts| {
            !parts.is_empty()
                && parts.iter().all(|part| {
                    let Some(obj) = part.as_object() else {
                        return false;
                    };
                    let kinds = ["text", "inlineData", "functionCall", "functionResponse"];
                    if kinds.iter().filter(|key| obj.contains_key(**key)).count() != 1 {
                        return false;
                    }
                    obj.iter().all(|(key, value)| match key.as_str() {
                        "text" | "thoughtSignature" => value.is_string(),
                        "thought" => value.is_boolean(),
                        "inlineData" => {
                            value.is_object()
                                && value["mimeType"].is_string()
                                && value["data"].is_string()
                        }
                        "functionCall" => {
                            value.is_object()
                                && value["name"].as_str().is_some_and(|s| !s.is_empty())
                                && value
                                    .get("args")
                                    .is_none_or(|args| args.is_null() || args.is_object())
                                && value.get("partialArgs").is_none()
                                && value.get("willContinue").is_none()
                        }
                        "functionResponse" => {
                            value.is_object()
                                && value["name"].as_str().is_some_and(|s| !s.is_empty())
                                && value["response"].is_object()
                        }
                        _ => false,
                    })
                })
        })
    };
    body["contents"].as_array().is_some_and(|contents| {
        contents.iter().all(|content| {
            content.as_object().is_some_and(|obj| {
                matches!(
                    obj.get("role").and_then(Value::as_str),
                    Some("user" | "model")
                ) && obj.get("parts").is_some_and(&valid_parts)
            })
        })
    }) && body.get("systemInstruction").is_none_or(|instruction| {
        instruction
            .as_object()
            .is_some_and(|obj| obj.get("parts").is_some_and(valid_parts))
    })
}

#[async_trait::async_trait]
impl ProviderAdapter for GoogleAdapter {
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &ProviderAttempt,
        request: &DurableToolRequest,
    ) -> Result<Value> {
        request.validate_bindings()?;
        if !valid_model_id(model_id(&request.request.request.model)) {
            return Err(Error::Config("invalid Google model ID".into()));
        }
        wire_request_with_blocks(
            &request.request.request,
            None,
            "google",
            Some(&request.request),
            Some(request),
        )
    }
    fn validate_with_tool_blocks(
        &self,
        attempt: &ProviderAttempt,
        request: &DurableToolRequest,
        body: &Value,
    ) -> Result<()> {
        request.validate_bindings()?;
        self.validate(attempt, &request.request.request, body)?;
        let expected = wire_request_with_blocks(
            &request.request.request,
            None,
            "google",
            Some(&request.request),
            Some(request),
        )?;
        if body.get("contents") != expected.get("contents") {
            return Err(Error::Config("provider hook changed durable input".into()));
        }
        Ok(())
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    async fn prepare_durable(
        &self,
        _: &ProviderAttempt,
        request: &DurableRequest,
    ) -> Result<Value> {
        if !valid_model_id(model_id(&request.request.model)) {
            return Err(Error::Config("invalid Google model ID".into()));
        }
        wire_request_with_blocks(&request.request, None, "google", Some(request), None)
    }
    fn validate_durable(
        &self,
        attempt: &ProviderAttempt,
        request: &DurableRequest,
        body: &Value,
    ) -> Result<()> {
        self.validate(attempt, &request.request, body)?;
        if !request.blocks.is_empty()
            && body.get("contents")
                != wire_request_with_blocks(&request.request, None, "google", Some(request), None)?
                    .get("contents")
        {
            return Err(Error::Config("provider hook changed durable input".into()));
        }
        Ok(())
    }
    fn id(&self) -> &str {
        "google-generative-ai/v1"
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value> {
        if !valid_model_id(model_id(&request.model)) {
            return Err(Error::Config("invalid Google model ID".into()));
        }
        wire_request(request, None, "google")
    }
    fn validate(&self, _: &ProviderAttempt, request: &ModelRequest, body: &Value) -> Result<()> {
        let fail = || Error::Config("invalid or unauthorized Google request body".into());
        if !valid_model_id(model_id(&request.model)) {
            return Err(fail());
        }
        let obj = body.as_object().ok_or_else(fail)?;
        let expected = wire_request(request, None, "google")?;
        let config = body
            .get("generationConfig")
            .and_then(Value::as_object)
            .ok_or_else(fail)?;
        if obj.keys().any(|k| {
            ![
                "contents",
                "generationConfig",
                "systemInstruction",
                "tools",
                "toolConfig",
            ]
            .contains(&k.as_str())
        }) || !valid_content(body)
            || config
                .keys()
                .any(|k| !["maxOutputTokens", "candidateCount"].contains(&k.as_str()))
            || config.get("candidateCount") != Some(&json!(1))
            || config
                .get("maxOutputTokens")
                .and_then(Value::as_u64)
                .is_none_or(|n| n == 0 || n > u64::from(request.max_output_tokens))
            || obj.get("tools") != expected.get("tools")
            || obj.get("toolConfig") != expected.get("toolConfig")
            || serde_json::to_vec(body)?.len() > MAX_RESPONSE_BYTES
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
        let model = model_id(&request.model).to_owned();
        let provider = "google".to_string();
        let replay_scope = "google".to_string();
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| Error::Config("invalid Google endpoint".into()))?
            .pop_if_empty()
            .push("models")
            .push(&format!("{model}:streamGenerateContent"));
        url.query_pairs_mut().append_pair("alt", "sse");
        let response = self
            .client
            .post(url)
            .header("x-goog-api-key", self.key.clone())
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|_| Error::RetryableModel("Google Gemini request failed".into()))?;
        let response = check_status(response)?;
        if !response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"))
            })
        {
            return Err(Error::Model("expected Google Gemini event stream".into()));
        }
        Ok(Box::pin(async_stream::try_stream! {
            let mut size = 0usize;
            let mut stream = response.bytes_stream().map(move |chunk| {
                let bytes = chunk.map_err(|_| Error::RetryableModel("Google Gemini stream interrupted".into()))?;
                size = size.saturating_add(bytes.len());
                if size > MAX_RESPONSE_BYTES { return Err(Error::Model("Google Gemini response exceeds size limit".into())); }
                Ok(bytes)
            }).eventsource();
            let mut metadata = ModelMetadata {
                provider_api: Some(ModelApi::GoogleGenerativeAi),
                ..ModelMetadata::default()
            };
            let mut calls = Vec::new();
            let mut call_ids = BTreeSet::new();
            let mut blocks = Vec::new();
            let mut reasoning = String::new();
            let mut total_bytes = 0usize;
            let mut usage = None;
            let mut finish = None;
            while let Some(event) = stream.next().await {
                let event = event.map_err(|error| match error {
                    eventsource_stream::EventStreamError::Transport(error) => error,
                    _ => Error::RetryableModel("Google Gemini stream interrupted".into()),
                })?;
                total_bytes = total_bytes.saturating_add(event.data.len());
                if total_bytes > MAX_RESPONSE_BYTES {
                    Err(Error::Model("Google Gemini response exceeds size limit".into()))?;
                }
                if event.data == "[DONE]" {
                    break;
                }
                let value: Value = serde_json::from_str(&event.data)
                    .map_err(|_| Error::Model("invalid Google Gemini stream chunk".into()))?;
                if let Some(error) = value.get("error") {
                    let code = error["code"].as_u64().unwrap_or_default();
                    Err(if code == 429 || (500..600).contains(&code) {
                        Error::RetryableModel("Google Gemini returned a transient stream error".into())
                    } else {
                        Error::Model("Google Gemini returned a stream error".into())
                    })?;
                }
                if let Some(id) = value["responseId"].as_str().filter(|id| id.len() <= 160) {
                    metadata.provider_request_id = Some(id.into());
                }
                if let Some(version) = value["modelVersion"].as_str().filter(|version| valid_model_id(version)) {
                    metadata.resolved_model = Some(identity(&provider, version));
                }
                if let Some(stats) = value.get("usageMetadata") {
                    let count = |key: &str| stats[key].as_u64().unwrap_or_default();
                    usage = Some(Usage {
                        input_tokens: count("promptTokenCount"),
                        output_tokens: count("candidatesTokenCount").saturating_add(count("thoughtsTokenCount")),
                    });
                    metadata.cached_input_tokens = stats["cachedContentTokenCount"].as_u64();
                }
                if value["promptFeedback"]["blockReason"].as_str().is_some_and(|reason| !reason.is_empty() && reason != "BLOCK_REASON_UNSPECIFIED") {
                    Err(Error::Model("Google Gemini blocked the prompt".into()))?;
                }
                let Some(candidates) = value["candidates"].as_array() else {
                    continue;
                };
                if candidates.len() > 1 || candidates.first().is_some_and(|candidate| candidate["index"].as_u64().unwrap_or_default() != 0) {
                    Err(Error::Model("unexpected Google Gemini candidate".into()))?;
                }
                let Some(candidate) = candidates.first() else {
                    continue;
                };
                if finish.is_some() {
                    Err(Error::Model("Google Gemini returned a candidate after its finish reason".into()))?;
                }
                if candidate.get("content").is_some_and(|content| !content.is_object() || content.get("parts").is_some_and(|parts| !parts.is_array())) {
                    Err(Error::Model("invalid Google Gemini candidate content".into()))?;
                }
                if let Some(parts) = candidate["content"]["parts"].as_array() {
                    for part in parts {
                        if !part.is_object() {
                            Err(Error::Model("invalid Google Gemini part".into()))?;
                        }
                        let thought = part["thought"].as_bool().unwrap_or(false);
                        if let Some(text) = part["text"].as_str().filter(|text| !text.is_empty()) {
                            if thought {
                                reasoning.push_str(text);
                                yield ModelEvent::Reasoning(text.into());
                            } else {
                                yield ModelEvent::Text(text.into());
                            }
                        }
                        if let Some(call) = part.get("functionCall") {
                            if !call.is_object() || call.get("partialArgs").is_some() || call.get("willContinue").is_some() {
                                Err(Error::Model("unsupported partial Google Gemini function call".into()))?;
                            }
                            let name = call["name"].as_str().filter(|name| !name.is_empty()).ok_or_else(|| Error::Model("invalid Google Gemini function name".into()))?;
                            let arguments = match call.get("args") {
                                None | Some(Value::Null) => "{}".to_owned(),
                                Some(args) if args.is_object() => args.to_string(),
                                _ => Err(Error::Model("Google Gemini function arguments are not an object".into()))?,
                            };
                            let provided = call["id"].as_str().filter(|id| !id.is_empty() && id.len() <= 160);
                            if call.get("id").is_some() && provided.is_none() {
                                Err(Error::Model("invalid Google Gemini function call ID".into()))?;
                            }
                            let id = provided.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
                            if !call_ids.insert(id.clone()) {
                                Err(Error::Model("duplicate Google Gemini function call ID".into()))?;
                            }
                            if calls.len() == 64 {
                                Err(Error::Model("too many Google Gemini function calls".into()))?;
                            }
                            let index = calls.len() as u64;
                            calls.push(ToolCall { id: id.clone(), name: name.into(), arguments: arguments.clone() });
                            yield ModelEvent::ToolCallDelta(pi_rs_core::ToolCallDelta { index, id, name: name.into(), arguments });
                        }
                        blocks.push(part.clone());
                    }
                }
                if let Some(reason) = candidate["finishReason"].as_str().filter(|reason| *reason != "FINISH_REASON_UNSPECIFIED") {
                    finish = Some(reason.to_owned());
                }
            }
            metadata.finish_reason = Some(match finish.as_deref() {
                Some("STOP") if calls.is_empty() => "stop",
                Some("STOP") => "tool_calls",
                Some("MAX_TOKENS") => {
                    calls.clear();
                    blocks.retain(|part| part.get("functionCall").is_none());
                    "length"
                }
                None => Err(Error::RetryableModel("Google Gemini stream ended without a finish reason".into()))?,
                Some(_) => Err(Error::Model("Google Gemini did not complete the response".into()))?,
            }.into());
            if !blocks.is_empty() {
                metadata.replay = Some(Box::new(ProviderReplay { identity: identity(&replay_scope, &model), blocks }));
            }
            metadata.reasoning = (!reasoning.is_empty()).then_some(reasoning);
            yield ModelEvent::Metadata(metadata);
            yield ModelEvent::Complete { calls, usage };
        }))
    }
}
