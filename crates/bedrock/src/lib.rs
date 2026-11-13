//! Amazon Bedrock ConverseStream adapter.
mod options;
use aws_sdk_bedrockruntime::{
    Client,
    types::{
        self, ContentBlock, ContentBlockDelta, ContentBlockStart, ConversationRole,
        ConverseStreamOutput,
    },
};
use aws_smithy_types::{Blob, Document, Number};
use aws_types::request_id::RequestId;
use base64::Engine;
pub use options::{ReasoningLevel, thinking_levels};
use pi_rs_core::{
    CacheRetention, Error, Message, Model, ModelEvent, ModelMetadata, ModelRequest, ModelStream,
    ProviderReplay, ResolvedModelIdentity, Result, ToolCall, ToolCallDelta, Usage,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// A client configured with the AWS SDK's region and refreshing credential provider chain.
#[derive(Clone)]
pub struct Bedrock {
    client: std::sync::Arc<tokio::sync::OnceCell<Client>>,
    unsupported: Option<String>,
    bearer: Option<String>,
    model_name: Option<(String, String)>,
    reasoning: Option<ReasoningLevel>,
    mapped_effort: Option<String>,
    cache_retention: CacheRetention,
    long_cache_support: Option<bool>,
    force_cache: bool,
}
impl Bedrock {
    /// Resolve the region and standard AWS credential chain lazily.
    pub fn from_env() -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            unsupported: None,
            bearer: None,
            model_name: None,
            reasoning: None,
            mapped_effort: None,
            cache_retention: default_cache_retention(),
            long_cache_support: None,
            force_cache: std::env::var("AWS_BEDROCK_FORCE_CACHE").is_ok_and(|v| v == "1"),
        }
    }
    /// Use a caller-configured SDK client, including custom profile or endpoint settings.
    pub fn new(client: Client) -> Self {
        Self {
            client: std::sync::Arc::new(tokio::sync::OnceCell::new_with(Some(client))),
            unsupported: None,
            bearer: None,
            model_name: None,
            reasoning: None,
            mapped_effort: None,
            cache_retention: default_cache_retention(),
            long_cache_support: None,
            force_cache: std::env::var("AWS_BEDROCK_FORCE_CACHE").is_ok_and(|v| v == "1"),
        }
    }
    /// Prefer bearer authentication over the AWS credential chain.
    pub fn with_bearer_token(mut self, token: String) -> Result<Self> {
        if token.is_empty() || !token.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return Err(Error::Config("invalid Bedrock bearer token".into()));
        }
        let client = self
            .client
            .get()
            .map(|client| bearer_client(client.config().to_builder(), &token));
        self.client = std::sync::Arc::new(tokio::sync::OnceCell::new_with(client));
        self.bearer = Some(token);
        Ok(self)
    }
    /// Bind a configured model name to its ID for model-specific Converse behavior.
    pub fn with_model_name(mut self, model: &str, name: Option<&str>) -> Self {
        self.model_name = name.map(|name| (model.to_owned(), name.to_owned()));
        self
    }
    fn is_claude(&self, model: &str) -> bool {
        model.to_ascii_lowercase().contains("claude")
            || self.model_name.as_ref().is_some_and(|(id, name)| {
                id == model && name.to_ascii_lowercase().contains("claude")
            })
    }
    /// Configure Pi's reasoning level and an optional model-specific effort mapping.
    pub fn with_reasoning(
        mut self,
        level: Option<&str>,
        mapped_effort: Option<&str>,
    ) -> Result<Self> {
        self.reasoning = level
            .filter(|level| *level != "off" || mapped_effort.is_some())
            .map(ReasoningLevel::parse)
            .transpose()?;
        self.mapped_effort = mapped_effort.map(str::to_owned);
        Ok(self)
    }
    /// Set explicit cache retention. Use `None` for compaction and other one-off requests.
    pub fn with_cache_retention(mut self, retention: CacheRetention) -> Self {
        self.cache_retention = retention;
        self
    }
    /// Override model-based one-hour cache support; `None` uses known model capabilities.
    pub fn with_long_cache_support(mut self, supported: Option<bool>) -> Self {
        self.long_cache_support = supported;
        self
    }
    /// Prepare a one-off summary without cache checkpoints or optional thinking.
    pub fn for_summary(mut self) -> Self {
        self.cache_retention = CacheRetention::None;
        self.reasoning = None;
        self.mapped_effort = None;
        self
    }
    /// Enable cache checkpoints for a model whose capabilities cannot be inferred from its name.
    pub fn with_force_cache(mut self, enabled: bool) -> Self {
        self.force_cache = enabled;
        self
    }
    /// Mark unsupported host settings so they fail explicitly when used.
    pub fn unsupported(mut self, reason: impl Into<String>) -> Self {
        self.unsupported = Some(reason.into());
        self
    }
}
fn default_cache_retention() -> CacheRetention {
    if std::env::var("PI_CACHE_RETENTION").is_ok_and(|v| v == "long") {
        CacheRetention::Long
    } else {
        CacheRetention::Short
    }
}
#[derive(Default)]
struct ToolIds {
    mapped: BTreeMap<String, String>,
    used: BTreeSet<String>,
}
impl ToolIds {
    fn get(&mut self, original: &str) -> String {
        if let Some(id) = self.mapped.get(original) {
            return id.clone();
        }
        let base: String = original
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .take(64)
            .collect();
        let base = if base.is_empty() {
            "_".to_owned()
        } else {
            base
        };
        let mut candidate = base.clone();
        let mut suffix = 2_u64;
        while self.used.contains(&candidate) {
            let tail = format!("_{suffix}");
            candidate = format!("{}{}", &base[..base.len().min(64 - tail.len())], tail);
            suffix += 1;
        }
        self.used.insert(candidate.clone());
        self.mapped.insert(original.to_owned(), candidate.clone());
        candidate
    }
}
fn required_text(text: String) -> String {
    if text.trim().is_empty() {
        "<empty>".into()
    } else {
        text
    }
}
fn diagnostic(
    meta: &aws_smithy_types::error::metadata::ErrorMetadata,
    fallback_id: Option<&str>,
) -> String {
    let clean = |s: &str| {
        s.chars()
            .filter(|c| !c.is_control())
            .take(512)
            .collect::<String>()
    };
    let mut parts = vec!["Bedrock error".to_owned()];
    if let Some(code) = meta.code().filter(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.#".contains(&b))
    }) {
        parts.push(code.into());
    }
    if let Some(message) = meta.message().map(clean).filter(|s| !s.trim().is_empty()) {
        parts.push(message);
    }
    if let Some(id) = meta.request_id().or(fallback_id).filter(|s| {
        s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    }) {
        parts.push(format!("request id: {id}"));
    }
    parts.join(": ")
}
fn bearer_client(builder: aws_sdk_bedrockruntime::config::Builder, token: &str) -> Client {
    let config = builder
        .bearer_token(aws_sdk_bedrockruntime::config::Token::new(
            token.to_owned(),
            None,
        ))
        .auth_scheme_preference([
            aws_smithy_runtime_api::client::auth::http::HTTP_BEARER_AUTH_SCHEME_ID,
        ])
        .build();
    Client::from_conf(config)
}
fn image_block(image: pi_rs_core::ImageContent) -> Result<types::ImageBlock> {
    image.validate()?;
    let format = match image.mime_type.as_str() {
        "image/png" => types::ImageFormat::Png,
        "image/jpeg" => types::ImageFormat::Jpeg,
        "image/gif" => types::ImageFormat::Gif,
        "image/webp" => types::ImageFormat::Webp,
        _ => return Err(Error::Config("unsupported Bedrock image format".into())),
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(image.data)
        .map_err(build_error)?;
    types::ImageBlock::builder()
        .format(format)
        .source(types::ImageSource::Bytes(Blob::new(bytes)))
        .build()
        .map_err(build_error)
}
#[derive(Default)]
struct ReasoningBlock {
    text: String,
    signature: String,
    redacted: Vec<u8>,
}

fn replay_blocks(
    replay: &ProviderReplay,
    model: &str,
    is_claude: bool,
    text: &str,
    calls: &[ToolCall],
    ids: &mut ToolIds,
) -> Option<Vec<ContentBlock>> {
    if replay.identity.provider != "amazon-bedrock" || replay.identity.model != model {
        return None;
    }
    let rendered: String = replay
        .blocks
        .iter()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect();
    if rendered != text {
        return None;
    }
    let replay_calls: Vec<_> = replay
        .blocks
        .iter()
        .filter(|b| b["type"] == "tool_use")
        .collect();
    if replay_calls.len() != calls.len()
        || replay_calls.iter().zip(calls).any(|(b, c)| {
            b["id"] != c.id
                || b["name"] != c.name
                || b.get("raw_arguments")
                    .and_then(Value::as_str)
                    .is_some_and(|raw| !matches_journal_arguments(raw, &c.arguments))
                || history_tool_input(&c.arguments) != b["input"]
        })
    {
        return None;
    }
    let mut out = Vec::new();
    for b in &replay.blocks {
        match b["type"].as_str()? {
            "text" => out.push(ContentBlock::Text(b["text"].as_str()?.to_owned())),
            "tool_use" => out.push(ContentBlock::ToolUse(
                types::ToolUseBlock::builder()
                    .tool_use_id(ids.get(b["id"].as_str()?))
                    .name(b["name"].as_str()?)
                    .input(doc(b["input"].clone()))
                    .build()
                    .ok()?,
            )),
            "reasoning" => {
                let reasoning = if let Some(encoded) = b["redacted"].as_str() {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()?;
                    if bytes.is_empty() {
                        return None;
                    }
                    types::ReasoningContentBlock::RedactedContent(Blob::new(bytes))
                } else {
                    let text = b["text"].as_str()?;
                    let signature = b["signature"].as_str().filter(|s| !s.is_empty());
                    if text.is_empty() {
                        return None;
                    }
                    if is_claude && signature.is_none() {
                        out.push(ContentBlock::Text(text.to_owned()));
                        continue;
                    }
                    let sig = if is_claude {
                        signature.map(str::to_owned)
                    } else {
                        None
                    };
                    types::ReasoningContentBlock::ReasoningText(
                        types::ReasoningTextBlock::builder()
                            .text(text)
                            .set_signature(sig)
                            .build()
                            .ok()?,
                    )
                };
                out.push(ContentBlock::ReasoningContent(reasoning));
            }
            _ => return None,
        }
    }
    Some(out)
}
fn matches_journal_arguments(raw: &str, current: &str) -> bool {
    if raw == current {
        return true;
    }
    if serde_json::from_str::<Value>(raw).is_ok_and(|value| value.is_object()) {
        return false;
    }
    let projected = pi_rs_core::parse_tool_arguments(raw)
        .map_or_else(|_| "{}".to_owned(), |value| value.to_string());
    current == projected
}
fn history_tool_input(raw: &str) -> Value {
    pi_rs_core::parse_tool_arguments(raw).unwrap_or_else(|_| serde_json::json!({}))
}
fn doc(v: Value) -> Document {
    match v {
        Value::Null => Document::Null,
        Value::Bool(x) => Document::Bool(x),
        Value::String(x) => Document::String(x),
        Value::Number(x) => Document::Number(if let Some(n) = x.as_i64() {
            if n < 0 {
                Number::NegInt(n)
            } else {
                Number::PosInt(n as u64)
            }
        } else if let Some(n) = x.as_u64() {
            Number::PosInt(n)
        } else {
            Number::Float(x.as_f64().unwrap_or_default())
        }),
        Value::Array(xs) => Document::Array(xs.into_iter().map(doc).collect()),
        Value::Object(xs) => Document::Object(xs.into_iter().map(|(k, v)| (k, doc(v))).collect()),
    }
}
fn build_error(e: impl std::fmt::Display) -> Error {
    Error::Config(format!("invalid Bedrock request: {e}"))
}

fn request_error(
    e: aws_sdk_bedrockruntime::error::SdkError<
        aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamError,
    >,
) -> Error {
    if let Some(service) = e.as_service_error() {
        let message = diagnostic(service.meta(), service.request_id());
        if service.is_throttling_exception()
            || service.is_model_timeout_exception()
            || service.is_service_unavailable_exception()
            || service.is_internal_server_exception()
        {
            Error::RetryableModel(message)
        } else {
            Error::Model(message)
        }
    } else {
        match &e {
                    aws_sdk_bedrockruntime::error::SdkError::TimeoutError(_) => Error::RetryableModel("Bedrock transport timed out".into()),
                    aws_sdk_bedrockruntime::error::SdkError::DispatchFailure(failure) if failure.is_io() || failure.is_timeout() || matches!(failure.as_other(), Some(aws_smithy_types::retry::ErrorKind::TransientError | aws_smithy_types::retry::ErrorKind::ThrottlingError | aws_smithy_types::retry::ErrorKind::ServerError)) => Error::RetryableModel("Bedrock transport failed".into()),
                    aws_sdk_bedrockruntime::error::SdkError::DispatchFailure(_) | aws_sdk_bedrockruntime::error::SdkError::ConstructionFailure(_) => Error::Config("Bedrock could not resolve AWS credentials or configure the request; check AWS_PROFILE, credentials, and region".into()),
                    _ => Error::Model("Bedrock request failed".into()),
                }
    }
}

#[async_trait::async_trait]
impl Model for Bedrock {
    async fn stream_for_summary(
        &self,
        _request_id: uuid::Uuid,
        _session_id: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.clone().for_summary().stream(request).await
    }
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        if let Some(reason) = &self.unsupported {
            return Err(Error::Config(reason.clone()));
        }
        if request.max_output_tokens == 0 || request.model.contains(":prompt/") {
            return Err(Error::Config("Bedrock requires a positive max output token limit and does not support managed prompt ARNs".into()));
        }
        let tool_free = request.tools.is_empty();
        let is_claude = self.is_claude(&request.model);
        let model_name = self
            .model_name
            .as_ref()
            .filter(|(id, _)| id == &request.model)
            .map(|(_, name)| name.as_str());
        let checkpoint = if options::supports_cache(&request.model, model_name, self.force_cache) {
            let retention = if self.cache_retention == CacheRetention::Long
                && !self
                    .long_cache_support
                    .unwrap_or_else(|| options::supports_long_cache(&request.model, model_name))
            {
                CacheRetention::Short
            } else {
                self.cache_retention
            };
            options::cache_point(retention)?
        } else {
            None
        };
        let mut ids = ToolIds::default();
        let mut system = Vec::new();
        let mut messages: Vec<types::Message> = Vec::new();
        let mut pending_error_images = Vec::new();
        for message in request.messages {
            if !matches!(&message, Message::Tool { .. })
                && let Some(last) = messages.last_mut()
            {
                last.content.append(&mut pending_error_images);
            }
            let (role, blocks) = match message {
                Message::System { content } => {
                    if !content.trim().is_empty() {
                        system.push(types::SystemContentBlock::Text(content));
                    }
                    continue;
                }
                Message::User { content } => (
                    ConversationRole::User,
                    vec![ContentBlock::Text(required_text(content))],
                ),
                Message::UserWithImages { content, images } => {
                    let mut blocks = Vec::new();
                    if !content.trim().is_empty() {
                        blocks.push(ContentBlock::Text(content));
                    }
                    for image in images {
                        blocks.push(ContentBlock::Image(image_block(image)?));
                    }
                    if blocks.is_empty() {
                        blocks.push(ContentBlock::Text("<empty>".into()));
                    }
                    (ConversationRole::User, blocks)
                }
                Message::Assistant {
                    content,
                    calls,
                    replay,
                } => {
                    if let Some(blocks) = replay
                        .as_deref()
                        .filter(|_| !tool_free || calls.is_empty())
                        .and_then(|r| {
                            replay_blocks(r, &request.model, is_claude, &content, &calls, &mut ids)
                        })
                    {
                        (ConversationRole::Assistant, blocks)
                    } else {
                        let mut blocks = Vec::new();
                        if !content.trim().is_empty() {
                            blocks.push(ContentBlock::Text(content));
                        }
                        for call in calls {
                            if tool_free {
                                blocks.push(ContentBlock::Text(format!(
                                    "Historical tool call ({}; {}):\n{}",
                                    call.id, call.name, call.arguments
                                )));
                                continue;
                            }
                            let input = history_tool_input(&call.arguments);
                            blocks.push(ContentBlock::ToolUse(
                                types::ToolUseBlock::builder()
                                    .tool_use_id(ids.get(&call.id))
                                    .name(call.name)
                                    .input(doc(input))
                                    .build()
                                    .map_err(build_error)?,
                            ));
                        }
                        (ConversationRole::Assistant, blocks)
                    }
                }
                Message::Tool {
                    call_id,
                    content,
                    is_error,
                    is_structured,
                } => {
                    if tool_free {
                        let (content, images) =
                            if pi_rs_core::tool_content_is_structured(&content, is_structured) {
                                pi_rs_core::extract_tool_images(content)
                            } else {
                                (content, Vec::new())
                            };
                        let mut blocks = vec![ContentBlock::Text(format!(
                            "Historical tool result ({}; {}):\n{}",
                            call_id,
                            if is_error { "error" } else { "success" },
                            content
                        ))];
                        for image in images {
                            blocks.push(ContentBlock::Image(image_block(image)?));
                        }
                        (ConversationRole::User, blocks)
                    } else {
                        let (content, images) =
                            if pi_rs_core::tool_content_is_structured(&content, is_structured) {
                                pi_rs_core::extract_tool_images(content)
                            } else {
                                (content, Vec::new())
                            };
                        let mut tool = types::ToolResultBlock::builder()
                            .tool_use_id(ids.get(&call_id))
                            .content(types::ToolResultContentBlock::Text(required_text(content)))
                            .status(if is_error {
                                types::ToolResultStatus::Error
                            } else {
                                types::ToolResultStatus::Success
                            });
                        let mut trailing = Vec::new();
                        let claude_error = is_error && is_claude;
                        for image in images {
                            let image = image_block(image)?;
                            if claude_error {
                                trailing.push(ContentBlock::Image(image));
                            } else {
                                tool = tool.content(types::ToolResultContentBlock::Image(image));
                            }
                        }
                        let blocks =
                            vec![ContentBlock::ToolResult(tool.build().map_err(build_error)?)];
                        if !trailing.is_empty() {
                            pending_error_images.push(ContentBlock::Text(
                                "Images from the tool error result:".into(),
                            ));
                            pending_error_images.extend(trailing);
                        }
                        (ConversationRole::User, blocks)
                    }
                }
            };
            if blocks.is_empty() {
                continue;
            }
            if let Some(last) = messages.last_mut()
                && last.role == role
            {
                last.content.extend(blocks);
                continue;
            }
            messages.push(
                types::Message::builder()
                    .role(role)
                    .set_content(Some(blocks))
                    .build()
                    .map_err(build_error)?,
            );
        }
        if let Some(last) = messages.last_mut() {
            last.content.append(&mut pending_error_images);
        }
        if let Some(point) = &checkpoint {
            if !system.is_empty() {
                system.push(types::SystemContentBlock::CachePoint(point.clone()));
            }
            if let Some(last) = messages.last_mut()
                && last.role == ConversationRole::User
            {
                last.content.push(ContentBlock::CachePoint(point.clone()));
            }
        }
        let tools: Vec<_> = request
            .tools
            .into_iter()
            .map(|t| {
                Ok(types::Tool::ToolSpec(
                    types::ToolSpecification::builder()
                        .name(t.name)
                        .description(t.description)
                        .input_schema(types::ToolInputSchema::Json(doc(t.parameters)))
                        .build()
                        .map_err(build_error)?,
                ))
            })
            .collect::<Result<_>>()?;
        let client = self
            .client
            .get_or_init(|| async {
                {
                    let config =
                        aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                    if let Some(token) = &self.bearer {
                        bearer_client(
                            aws_sdk_bedrockruntime::config::Builder::from(&config),
                            token,
                        )
                    } else {
                        Client::new(&config)
                    }
                }
            })
            .await;
        if client.config().region().is_none() {
            return Err(Error::Config("Bedrock requires an AWS region; set AWS_REGION or configure a region in the selected AWS profile".into()));
        }
        let fields = options::reasoning_fields(
            &request.model,
            model_name,
            self.reasoning,
            self.mapped_effort.as_deref(),
            request.max_output_tokens,
            client.config().region().map(|region| region.as_ref()),
            tool_free,
        )?;
        let requested_model = request.model.clone();
        let mut command = client
            .converse_stream()
            .model_id(request.model)
            .set_messages(Some(messages))
            .set_system((!system.is_empty()).then_some(system))
            .inference_config(
                types::InferenceConfiguration::builder()
                    .max_tokens(i32::try_from(request.max_output_tokens).map_err(build_error)?)
                    .build(),
            );
        if let Some(fields) = fields {
            command = command.additional_model_request_fields(doc(fields));
        }
        if !tools.is_empty() {
            command = command.tool_config(
                types::ToolConfiguration::builder()
                    .set_tools(Some(tools))
                    .build()
                    .map_err(build_error)?,
            );
        }
        let mut response = command.send().await.map_err(request_error)?;
        let request_id = response.request_id().map(str::to_owned);
        Ok(Box::pin(async_stream::try_stream! {
            let mut calls: BTreeMap<i32, ToolCall> = BTreeMap::new();
            let mut usage=None; let mut stop=None; let mut cache_read=None; let mut cache_write=None; let mut cache_write_1h=None; let mut received=0usize;
            let mut open_tools = std::collections::BTreeSet::new();
            let mut text_blocks: BTreeMap<i32, String> = BTreeMap::new();
            let mut reasoning: BTreeMap<i32, ReasoningBlock> = BTreeMap::new();
            let mut stopped_blocks = BTreeSet::new();
            while let Some(event)=response.stream.recv().await.map_err(|e| {
                if let Some(service) = e.as_service_error() {
                    let mut message=diagnostic(service.meta(), request_id.as_deref());
                    if service.meta().code().is_none() {
                        let code = if service.is_validation_exception() { Some("ValidationException") } else if service.is_throttling_exception() { Some("ThrottlingException") } else if service.is_service_unavailable_exception() { Some("ServiceUnavailableException") } else if service.is_internal_server_exception() { Some("InternalServerException") } else if service.is_model_stream_error_exception() { Some("ModelStreamErrorException") } else { None };
                        if let Some(code) = code { message.insert_str(0, &format!("{code}: ")); }
                    }
                    if service.is_throttling_exception() || service.is_service_unavailable_exception() || service.is_internal_server_exception() || service.is_model_stream_error_exception() { Error::RetryableModel(message) } else { Error::Model(message) }
                } else { Error::RetryableModel("Bedrock stream transport failed".into()) }
            })? {
                match event {
                    ConverseStreamOutput::ContentBlockStart(event) => if let Some(ContentBlockStart::ToolUse(tool))=event.start {
                        let index=event.content_block_index;
                        if index < 0 || calls.contains_key(&index) || text_blocks.contains_key(&index) || reasoning.contains_key(&index) || stopped_blocks.contains(&index) { Err(Error::Model("invalid Bedrock tool index".into()))?; }
                        open_tools.insert(index);
                        let call=ToolCall {id:tool.tool_use_id().into(), name:tool.name().into(), arguments:String::new()};
                        yield ModelEvent::ToolCallDelta(ToolCallDelta {index:index as u64,id:call.id.clone(),name:call.name.clone(),arguments:String::new()}); calls.insert(index,call);
                    },
                    ConverseStreamOutput::ContentBlockDelta(event) => match event.delta {
                        Some(ContentBlockDelta::Text(text)) => { let index=event.content_block_index; if index < 0 || stopped_blocks.contains(&index) || reasoning.contains_key(&index) || calls.contains_key(&index) { Err(Error::Model("invalid Bedrock text block".into()))?; } received=received.saturating_add(text.len()); if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; } text_blocks.entry(index).or_default().push_str(&text); yield ModelEvent::Text(text); },
                        Some(ContentBlockDelta::ReasoningContent(delta)) => {
                            let index=event.content_block_index;
                            if index < 0 || stopped_blocks.contains(&index) || text_blocks.contains_key(&index) || calls.contains_key(&index) { Err(Error::Model("invalid Bedrock reasoning block".into()))?; }
                            let block=reasoning.entry(index).or_default();
                            match delta {
                                types::ReasoningContentBlockDelta::Text(text) => { if !block.redacted.is_empty() { Err(Error::Model("mixed Bedrock reasoning block".into()))?; } received=received.saturating_add(text.len()); block.text.push_str(&text); yield ModelEvent::Reasoning(text); },
                                types::ReasoningContentBlockDelta::Signature(signature) => { if !block.redacted.is_empty() { Err(Error::Model("mixed Bedrock reasoning block".into()))?; } received=received.saturating_add(signature.len()); block.signature.push_str(&signature); },
                                types::ReasoningContentBlockDelta::RedactedContent(blob) => { if !block.text.is_empty() || !block.signature.is_empty() { Err(Error::Model("mixed Bedrock reasoning block".into()))?; } received=received.saturating_add(blob.as_ref().len()); block.redacted.extend_from_slice(blob.as_ref()); },
                                _ => Err(Error::Model("unsupported Bedrock reasoning delta".into()))?,
                            }
                            if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; }
                        },
                        Some(ContentBlockDelta::ToolUse(delta)) => { let index=event.content_block_index; if !open_tools.contains(&index) { Err(Error::Model("Bedrock tool delta without open block".into()))?; } received=received.saturating_add(delta.input().len()); if received > 4 * 1024 * 1024 { Err(Error::Model("Bedrock stream exceeds size limit".into()))?; } let call=calls.get_mut(&index).ok_or_else(|| Error::Model("Bedrock tool delta without start".into()))?; call.arguments.push_str(delta.input()); yield ModelEvent::ToolCallDelta(ToolCallDelta {index:index as u64,id:String::new(),name:String::new(),arguments:delta.input().into()}); },
                        _ => Err(Error::Model("unsupported Bedrock content delta".into()))?,
                    },
                    ConverseStreamOutput::MessageStop(event) => { let reason=event.stop_reason().as_str().to_owned(); if !matches!(reason.as_str(),"end_turn"|"stop_sequence"|"tool_use"|"max_tokens"|"model_context_window_exceeded") { Err(Error::Model(format!("Bedrock stopped with: {reason}")))?; } stop=Some(reason); },
                    ConverseStreamOutput::Metadata(event) => if let Some(u)=event.usage {
                        cache_read=u.cache_read_input_tokens().map(|n| n.max(0) as u64); cache_write=u.cache_write_input_tokens().map(|n| n.max(0) as u64);
                        cache_write_1h = u.cache_details.as_ref().map(|details| details.iter().filter(|d| d.ttl().as_str()=="1h").fold(0u64, |sum,d|sum.saturating_add(d.input_tokens().max(0) as u64)));
                        usage=Some(Usage {input_tokens:(u.input_tokens().max(0) as u64).saturating_add(cache_read.unwrap_or(0)).saturating_add(cache_write.unwrap_or(0)),output_tokens:u.output_tokens().max(0) as u64});
                    },
                    ConverseStreamOutput::ContentBlockStop(event) => { open_tools.remove(&event.content_block_index); stopped_blocks.insert(event.content_block_index); },
                    ConverseStreamOutput::MessageStart(_) => {},
                    _ => Err(Error::Model("unsupported Bedrock stream event".into()))?,
                }
            }
            let reason=stop.ok_or_else(|| Error::RetryableModel("Bedrock stream ended before messageStop".into()))?;
            if !open_tools.is_empty() || (!calls.is_empty() && reason != "tool_use") || (calls.is_empty() && reason == "tool_use") { Err(Error::Model("incomplete Bedrock tool response".into()))?; }
            if reasoning.keys().any(|i| !stopped_blocks.contains(i)) { Err(Error::Model("incomplete Bedrock reasoning block".into()))?; }
            let reasoning_text: String = reasoning.values().map(|b| b.text.as_str()).collect();
            let mut ordered = BTreeMap::new();
            for (i,text) in text_blocks { ordered.insert(i, serde_json::json!({"type":"text", "text":text})); }
            for (i,call) in &calls { let input=history_tool_input(&call.arguments); ordered.insert(*i, serde_json::json!({"type":"tool_use", "id":call.id,"name":call.name,"input":input,"raw_arguments":call.arguments})); }
            for (i,block) in &reasoning {
                let value=if block.redacted.is_empty() { serde_json::json!({"type":"reasoning", "text":block.text, "signature":block.signature}) } else { serde_json::json!({"type":"reasoning", "redacted":base64::engine::general_purpose::STANDARD.encode(&block.redacted)}) };
                ordered.insert(*i,value);
            }
            let replay=(!reasoning.is_empty() && reason != "max_tokens" && reason != "model_context_window_exceeded").then(|| Box::new(ProviderReplay { identity:ResolvedModelIdentity {provider:"amazon-bedrock".into(),model:requested_model},blocks:ordered.into_values().collect() }));
            let finish_reason = match reason.as_str() { "end_turn" | "stop_sequence" => "stop", "tool_use" => "tool_calls", "max_tokens" | "model_context_window_exceeded" => "length", _ => unreachable!() };
            yield ModelEvent::Metadata(ModelMetadata {finish_reason:Some(finish_reason.into()), cached_input_tokens:cache_read, cache_creation_input_tokens:cache_write, cache_write_1h_tokens:cache_write_1h, provider_api:Some(pi_rs_core::ModelApi::BedrockConverseStream), reasoning:(!reasoning_text.is_empty()).then_some(reasoning_text), replay, ..Default::default()});
            yield ModelEvent::Complete { calls:calls.into_values().collect(), usage };
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replay_requires_unchanged_model_text_and_tool_calls() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "anthropic.claude-test".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"reasoning","text":"thought","signature":"sig"}),
                serde_json::json!({"type":"text","text":"answer"}),
                serde_json::json!({"type":"tool_use","id":"id!","name":"lookup","input":{"x":1}}),
            ],
        };
        let call = ToolCall {
            id: "id!".into(),
            name: "lookup".into(),
            arguments: "{\"x\":1}".into(),
        };
        assert!(
            replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "answer",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_some()
        );
        assert!(
            replay_blocks(
                &replay,
                "other-model",
                false,
                "answer",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_none()
        );
        assert!(
            replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "edited",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_none()
        );
        let changed = ToolCall {
            arguments: "{\"x\":2}".into(),
            ..call
        };
        assert!(
            replay_blocks(
                &replay,
                "anthropic.claude-test",
                true,
                "answer",
                &[changed],
                &mut ToolIds::default()
            )
            .is_none()
        );
    }

    #[test]
    fn malformed_replayed_arguments_must_remain_unchanged() {
        let replay = ProviderReplay {
            identity: ResolvedModelIdentity {
                provider: "amazon-bedrock".into(),
                model: "model".into(),
            },
            blocks: vec![
                serde_json::json!({"type":"tool_use","id":"id","name":"tool","input":{},"raw_arguments":"{bad"}),
            ],
        };
        let call = ToolCall {
            id: "id".into(),
            name: "tool".into(),
            arguments: "{bad".into(),
        };
        assert!(
            replay_blocks(
                &replay,
                "model",
                false,
                "",
                std::slice::from_ref(&call),
                &mut ToolIds::default()
            )
            .is_some()
        );
        let changed = ToolCall {
            arguments: "{different".into(),
            ..call
        };
        assert!(
            replay_blocks(
                &replay,
                "model",
                false,
                "",
                &[changed],
                &mut ToolIds::default()
            )
            .is_none()
        );
    }

    #[test]
    fn journal_projection_preserves_reasoning_and_detects_changed_arguments() {
        for raw in ["", "{broken", "[1]", r#"{"path":"C:\work"}"#] {
            let input = history_tool_input(raw);
            let replay = ProviderReplay {
                identity: ResolvedModelIdentity {
                    provider: "amazon-bedrock".into(),
                    model: "model".into(),
                },
                blocks: vec![
                    serde_json::json!({"type":"reasoning","text":"think","signature":"sig"}),
                    serde_json::json!({"type":"tool_use","id":"id","name":"tool","input":input,"raw_arguments":raw}),
                ],
            };
            let normalized = pi_rs_core::parse_tool_arguments(raw)
                .map_or_else(|_| "{}".to_owned(), |value| value.to_string());
            let call = ToolCall {
                id: "id".into(),
                name: "tool".into(),
                arguments: normalized,
            };
            let blocks = replay_blocks(
                &replay,
                "model",
                true,
                "",
                std::slice::from_ref(&call),
                &mut ToolIds::default(),
            )
            .expect("journal-normalized arguments should replay");
            assert!(
                matches!(&blocks[0], ContentBlock::ReasoningContent(types::ReasoningContentBlock::ReasoningText(text)) if text.signature()==Some("sig"))
            );
            let edited = ToolCall {
                arguments: "{\"edited\":true}".into(),
                ..call
            };
            assert!(
                replay_blocks(
                    &replay,
                    "model",
                    true,
                    "",
                    &[edited],
                    &mut ToolIds::default()
                )
                .is_none()
            );
        }
        assert!(!matches_journal_arguments("{broken", "{different"));
        assert!(!matches_journal_arguments("{\"x\":1}", "{\"x\": 1}"));
    }

    #[test]
    fn configured_model_name_is_scoped_to_its_id() {
        let model = Bedrock::from_env().with_model_name("opaque-profile", Some("Claude Sonnet"));
        assert!(model.is_claude("opaque-profile"));
        assert!(!model.is_claude("another-profile"));
        assert!(model.is_claude("anthropic.claude-sonnet"));
    }

    #[test]
    fn dispatch_retry_classification_respects_smithy_transient_kind() {
        use aws_sdk_bedrockruntime::error::{ConnectorError, SdkError};
        use aws_smithy_types::retry::ErrorKind;
        let error = |kind| {
            SdkError::dispatch_failure(ConnectorError::other(
                Box::new(std::io::Error::other("incomplete HTTP response")),
                kind,
            ))
        };
        for kind in [
            ErrorKind::TransientError,
            ErrorKind::ThrottlingError,
            ErrorKind::ServerError,
        ] {
            assert!(matches!(
                request_error(error(Some(kind))),
                Error::RetryableModel(_)
            ));
        }
        assert!(matches!(request_error(error(None)), Error::Config(_)));
        assert!(matches!(
            request_error(error(Some(ErrorKind::ClientError))),
            Error::Config(_)
        ));
    }

    #[test]
    fn diagnostic_strips_control_characters_and_rejects_unsafe_ids() {
        let meta = aws_smithy_types::error::metadata::ErrorMetadata::builder()
            .code("ValidationException")
            .message("bad\u{1b}[31m input\nnext")
            .build();
        let text = diagnostic(&meta, Some("unsafe\nrequest-id"));
        assert_eq!(
            text,
            "Bedrock error: ValidationException: bad[31m inputnext"
        );
    }
}
