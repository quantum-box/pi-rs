use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("session: {0}")]
    Session(String),
    #[error("model: {0}")]
    Model(String),
    #[error("model temporarily unavailable: {0}")]
    RetryableModel(String),
    #[error("extension {id}: {message}")]
    Extension { id: String, message: String },
    #[error("configuration: {0}")]
    Config(String),
    #[error("authorization required: {message}")]
    AuthorizationRequired {
        message: String,
        challenge: Option<String>,
    },
    #[error("execution cancelled")]
    Cancelled,
    #[error("execution budget exhausted")]
    Budget,
    #[error("tool operation {0} requires reconciliation")]
    Reconciliation(Uuid),
    #[error("tool operation {0} is still pending at its execution service")]
    PendingOperation(Uuid),
    #[error("run {0} is already recorded; use explicit resume to continue it")]
    DuplicateRun(Uuid),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InputKind {
    Steer,
    FollowUp,
}

/// Controls whether a safe boundary consumes one pending message or every message.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum QueueMode {
    #[default]
    All,
    OneAtATime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueuedInput {
    pub id: Uuid,
    pub kind: InputKind,
    pub content: String,
}

impl From<String> for QueuedInput {
    fn from(content: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            kind: InputKind::Steer,
            content,
        }
    }
}

/// Messages and acknowledged queue controls accepted during an agent run.
pub enum AgentInput {
    Message(QueuedMessage),
    /// Accounting for a cache refresh sent by the local embedding runtime.
    CacheWarm {
        session_id: Uuid,
        request_id: Uuid,
        metadata: Box<ModelMetadata>,
        usage: Usage,
        note: Option<String>,
    },
    Enqueue {
        message: QueuedMessage,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    EnqueueCustom {
        message: crate::QueuedCustomMessage,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    ClearQueue {
        reply: tokio::sync::oneshot::Sender<Result<Vec<QueuedMessage>>>,
    },
    SetQueueMode {
        kind: InputKind,
        mode: QueueMode,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Persist a display name through the session owner during an active run.
    SetSessionName {
        name: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Persist a bookmark through the session owner during an active run.
    SetSessionLabel {
        target: u64,
        label: Option<String>,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
}
impl<T: Into<QueuedMessage>> From<T> for AgentInput {
    fn from(message: T) -> Self {
        Self::Message(message.into())
    }
}

/// A queued message extends the existing text input without changing its shape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct QueuedMessage {
    #[serde(flatten)]
    pub input: QueuedInput,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<ImageContent>,
}
impl QueuedMessage {
    pub fn validate(&self) -> Result<()> {
        if self.input.content.trim().is_empty() && self.images.is_empty() {
            return Err(Error::Config("queued input is empty".into()));
        }
        for image in &self.images {
            image.validate()?;
        }
        if serde_json::to_vec(self)?.len() > 8 * 1024 * 1024 {
            return Err(Error::Config("queued input exceeds size limit".into()));
        }
        Ok(())
    }
}
impl<T: Into<QueuedInput>> From<T> for QueuedMessage {
    fn from(input: T) -> Self {
        Self {
            input: input.into(),
            images: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// Retain raw arguments so malformed model output can be handled as a tool error.
    pub arguments: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ImageContent {
    pub data: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
}

impl ImageContent {
    pub fn validate(&self) -> Result<()> {
        use base64::Engine;
        if !matches!(
            self.mime_type.as_str(),
            "image/png" | "image/jpeg" | "image/gif" | "image/webp"
        ) || self.data.is_empty()
            || self.data.len() > 8 * 1024 * 1024
            || base64::engine::general_purpose::STANDARD
                .decode(&self.data)
                .is_err()
        {
            return Err(Error::Config("invalid or oversized image content".into()));
        }
        Ok(())
    }
}

/// Resolve provenance for tool messages written before the structured flag existed.
/// Unknown legacy messages retain the historical structured-envelope detection.
pub fn tool_content_is_structured(content: &str, is_structured: Option<bool>) -> bool {
    is_structured.unwrap_or_else(|| {
        serde_json::from_str::<Value>(content)
            .ok()
            .is_some_and(|value| value["data"]["content"].is_array())
    })
}

/// Separate supported images from native structured tool output for provider encoding.
pub fn extract_tool_images(mut content: String) -> (String, Vec<ImageContent>) {
    let mut images = Vec::new();
    if let Ok(mut result) = serde_json::from_str::<Value>(&content)
        && let Some(blocks) = result
            .get_mut("data")
            .and_then(|data| data.get_mut("content"))
            .and_then(Value::as_array_mut)
    {
        for block in blocks {
            if block["type"] != "image" {
                continue;
            }
            let Some(data) = block["data"].as_str() else {
                continue;
            };
            let Some(mime) = block["mimeType"].as_str() else {
                continue;
            };
            if !matches!(
                mime,
                "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ) || data.len() > 8 * 1024 * 1024
            {
                continue;
            }
            images.push(ImageContent {
                data: data.to_owned(),
                mime_type: mime.to_owned(),
            });
            block["data"] = serde_json::json!("[image attached]");
        }
        content = result.to_string();
    }
    (content, images)
}

/// Content-only edits preserve raw history and never dispatch tools.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ContextContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ContentBlock {
    Text {
        text: String,
        #[serde(
            default,
            rename = "textSignature",
            skip_serializing_if = "Option::is_none"
        )]
        text_signature: Option<String>,
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
    },
    Thinking {
        thinking: String,
        #[serde(
            default,
            rename = "thinkingSignature",
            skip_serializing_if = "Option::is_none"
        )]
        thinking_signature: Option<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        redacted: bool,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
        #[serde(
            default,
            rename = "thoughtSignature",
            skip_serializing_if = "Option::is_none"
        )]
        thought_signature: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    System {
        content: String,
    },
    User {
        content: String,
    },
    UserWithImages {
        content: String,
        images: Vec<ImageContent>,
    },
    Assistant {
        content: String,
        calls: Vec<ToolCall>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replay: Option<Box<ProviderReplay>>,
    },
    Tool {
        call_id: String,
        content: String,
        /// Whether the tool result represents an error; absent in older messages.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        is_error: bool,
        /// None denotes an older message whose provenance was not recorded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        is_structured: Option<bool>,
    },
}

/// Headers required by Copilot for the initiator and capabilities of a turn.
pub fn copilot_request_headers(
    messages: &[Message],
) -> impl Iterator<Item = (&'static str, &'static str)> {
    let initiator = match messages.last() {
        None | Some(Message::User { .. } | Message::UserWithImages { .. }) => "user",
        Some(_) => "agent",
    };
    let images = messages.iter().any(|message| match message {
        Message::UserWithImages { images, .. } => !images.is_empty(),
        Message::Tool {
            content,
            is_structured,
            ..
        } if *is_structured != Some(false) => serde_json::from_str::<Value>(content)
            .ok()
            .is_some_and(|body| {
                body.pointer("/data/content")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| parts.iter().any(|part| part["type"] == "image"))
            }),
        _ => false,
    });
    [
        ("x-initiator", initiator),
        ("openai-intent", "conversation-edits"),
    ]
    .into_iter()
    .chain(images.then_some(("copilot-vision-request", "true")))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// Usage accounting supplied by a compatible provider. BYOK charges can
/// include separate gateway and upstream costs.
#[derive(Default, Deserialize)]
pub struct ReportedUsageCost {
    #[serde(default)]
    cost: Value,
    #[serde(default)]
    is_byok: Value,
    #[serde(default)]
    cost_details: Value,
}

impl ReportedUsageCost {
    pub fn total(&self) -> Option<serde_json::Number> {
        fn amount(value: &Value) -> Option<f64> {
            value
                .as_f64()
                .filter(|value| value.is_finite() && *value >= 0.0)
        }

        let gateway = amount(&self.cost)?;
        let total = match self.is_byok {
            Value::Bool(true) => {
                let upstream = match self.cost_details.get("upstream_inference_cost")? {
                    Value::Null => {
                        amount(&self.cost_details["upstream_inference_prompt_cost"])?
                            + amount(&self.cost_details["upstream_inference_completions_cost"])?
                    }
                    total => amount(total)?,
                };
                gateway + upstream
            }
            Value::Bool(false) | Value::Null => gateway,
            _ => return None,
        };
        serde_json::Number::from_f64(total)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub max_output_tokens: u32,
}

/// The priced portions of a measured usage record, in USD.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct UsageCostBreakdown {
    pub input: serde_json::Number,
    pub output: serde_json::Number,
    pub cache_read: serde_json::Number,
    pub cache_write: serde_json::Number,
}

impl Default for UsageCostBreakdown {
    fn default() -> Self {
        Self {
            input: 0.into(),
            output: 0.into(),
            cache_read: 0.into(),
            cache_write: 0.into(),
        }
    }
}

impl UsageCostBreakdown {
    pub fn new(input: f64, output: f64, cache_read: f64, cache_write: f64) -> Option<Self> {
        let number = serde_json::Number::from_f64;
        Some(Self {
            input: number(input)?,
            output: number(output)?,
            cache_read: number(cache_read)?,
            cache_write: number(cache_write)?,
        })
    }

    fn parts(&self) -> [f64; 4] {
        [
            &self.input,
            &self.output,
            &self.cache_read,
            &self.cache_write,
        ]
        .map(|part| part.as_f64().unwrap_or_default())
    }

    pub fn total(&self) -> Option<serde_json::Number> {
        serde_json::Number::from_f64(self.parts().into_iter().sum())
    }

    pub fn from_value(value: &Value) -> Option<Self> {
        value.as_object()?;
        let cost = Self::deserialize(value).ok()?;
        cost.parts()
            .into_iter()
            .all(|value| value.is_finite() && value >= 0.0)
            .then_some(cost)
    }
}

/// Retention for an explicit provider prompt-cache breakpoint.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheRetention {
    #[default]
    None,
    Short,
    Long,
}

impl CacheRetention {
    pub fn anthropic_control(self) -> Option<Value> {
        match self {
            Self::None => None,
            Self::Short => Some(serde_json::json!({"type":"ephemeral"})),
            Self::Long => Some(serde_json::json!({"type":"ephemeral","ttl":"1h"})),
        }
    }
}

/// Native chat transports recorded in a session.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ModelApi {
    OpenaiCompletions,
    MistralConversations,
    OpenaiResponses,
    OpenaiCodexResponses,
    AnthropicMessages,
    GoogleGenerativeAi,
    GoogleVertex,
    BedrockConverseStream,
    PiMessages,
}

impl ModelApi {
    pub fn named(name: &str) -> Option<Self> {
        match name {
            "openai-completions" => Some(Self::OpenaiCompletions),
            "mistral-conversations" => Some(Self::MistralConversations),
            "openai-responses" => Some(Self::OpenaiResponses),
            "openai-codex-responses" => Some(Self::OpenaiCodexResponses),
            "anthropic-messages" => Some(Self::AnthropicMessages),
            "google-generative-ai" => Some(Self::GoogleGenerativeAi),
            "google-vertex" => Some(Self::GoogleVertex),
            "bedrock-converse-stream" => Some(Self::BedrockConverseStream),
            "pi-messages" => Some(Self::PiMessages),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenaiCompletions => "openai-completions",
            Self::MistralConversations => "mistral-conversations",
            Self::OpenaiResponses => "openai-responses",
            Self::OpenaiCodexResponses => "openai-codex-responses",
            Self::AnthropicMessages => "anthropic-messages",
            Self::GoogleGenerativeAi => "google-generative-ai",
            Self::GoogleVertex => "google-vertex",
            Self::BedrockConverseStream => "bedrock-converse-stream",
            Self::PiMessages => "pi-messages",
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelMetadata {
    /// Provider, importer, or catalog estimate in USD, absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<serde_json::Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_breakdown: Option<UsageCostBreakdown>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_from_provider: Option<bool>,
    /// Service tier multiplier applied to catalog estimates for this reply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_cost_multiplier: Option<serde_json::Number>,
    pub provider_request_id: Option<String>,
    pub cached_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    /// One-hour cache writes included in cache_creation_input_tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_1h_tokens: Option<u64>,
    /// Provider-native prompt count when the host reports a normalized total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_prompt_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_model: Option<ResolvedModelIdentity>,
    /// Provider adapter scope used to replay edited context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_identity: Option<ResolvedModelIdentity>,
    /// API actually used for this request, independent of the provider's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_api: Option<ModelApi>,
    pub finish_reason: Option<String>,
    /// Provider-supplied visible reasoning, retained for session presentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<AssistantDiagnostic>,
    /// Complete ordered assistant content required for provider-specific continuations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay: Option<Box<ProviderReplay>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantDiagnostic {
    AnthropicInputTransformations {
        timestamp: i64,
        details: AnthropicInputTransformations,
    },
}

impl AssistantDiagnostic {
    pub fn from_pi_diagnostics(value: &Value) -> Vec<Self> {
        value
            .as_array()
            .into_iter()
            .flatten()
            .take(16)
            .filter_map(|diagnostic| {
                if diagnostic["type"] != "anthropic_input_transformations" {
                    return None;
                }
                Some(Self::AnthropicInputTransformations {
                    timestamp: diagnostic["timestamp"].as_i64()?,
                    details: AnthropicInputTransformations::from_value(
                        &diagnostic["details"]["transformations"],
                    )?,
                })
            })
            .collect()
    }

    pub fn dropped_thinking_blocks(&self) -> usize {
        match self {
            Self::AnthropicInputTransformations { details, .. } => details
                .transformations
                .iter()
                .filter(|transformation| transformation.kind == "thinking_dropped")
                .count(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnthropicInputTransformations {
    transformations: Vec<AnthropicInputTransformation>,
}

impl AnthropicInputTransformations {
    pub fn from_value(value: &Value) -> Option<Self> {
        let transformations: Vec<_> = value
            .as_array()?
            .iter()
            .take(256)
            .filter_map(|transformation| {
                let field = |key: &str| {
                    transformation[key]
                        .as_str()
                        .filter(|text| text.len() <= 1024)
                        .map(str::to_owned)
                };
                Some(AnthropicInputTransformation {
                    kind: field("type")?,
                    path: field("path"),
                    reason: field("reason"),
                })
            })
            .collect();
        (!transformations.is_empty()).then_some(Self { transformations })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct AnthropicInputTransformation {
    #[serde(rename = "type")]
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

/// Canonical provider and model names reported by the host adapter.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedModelIdentity {
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderReplay {
    pub identity: ResolvedModelIdentity,
    pub blocks: Vec<Value>,
}

impl ProviderReplay {
    /// Preserve the recorded Anthropic effort without placing private data on the wire.
    pub fn record_anthropic_effort(&mut self, effort: &str) {
        if let Some(effort) = Self::valid_anthropic_effort(effort) {
            self.blocks
                .retain(|block| block.get("_pi_anthropic_effort").is_none());
            self.blocks
                .push(serde_json::json!({"_pi_anthropic_effort":effort}));
        }
    }

    pub fn anthropic_effort(&self) -> Option<&'static str> {
        self.blocks.iter().find_map(|block| {
            Self::valid_anthropic_effort(block.get("_pi_anthropic_effort")?.as_str()?)
        })
    }

    pub fn valid_anthropic_effort(effort: &str) -> Option<&'static str> {
        match effort {
            "low" => Some("low"),
            "medium" => Some("medium"),
            "high" => Some("high"),
            "xhigh" => Some("xhigh"),
            "max" => Some("max"),
            _ => None,
        }
    }

    pub fn is_context_edit(&self) -> bool {
        self.blocks
            .iter()
            .any(|b| b.get("_pi_context_edit_api").is_some())
    }

    /// Edited replay is scoped to the originating transport as well as the model.
    pub fn supports_api(&self, api: ModelApi) -> bool {
        self.blocks
            .iter()
            .find_map(|b| b.get("_pi_context_edit_api").and_then(Value::as_str))
            .is_none_or(|name| name == api.as_str())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsageReport {
    Measured { usage: Usage },
    Estimated { output_tokens: u64 },
    Unconfirmed,
}

#[derive(Clone, Debug)]
// Keep the SDK event shape stable as optional provider metadata grows.
#[allow(clippy::large_enum_variant)]
pub enum ModelEvent {
    Text(String),
    Reasoning(String),
    ToolCallDelta(ToolCallDelta),
    Metadata(ModelMetadata),
    Complete {
        calls: Vec<ToolCall>,
        usage: Option<Usage>,
    },
}

/// Provider fragments for display while a tool call is being generated.
/// Only a completed model response can authorize execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCallDelta {
    pub index: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub arguments: String,
}

pub type ModelStream = futures_util::stream::BoxStream<'static, Result<ModelEvent>>;

/// Model events with optional provider reasoning item boundaries and identity.
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum EnrichedModelEvent {
    Legacy(ModelEvent),
    TextStart { index: u64 },
    TextDelta { index: u64, delta: String },
    TextEnd { index: u64, content: String },
    ReasoningStart { index: u64 },
    ReasoningDelta { index: u64, delta: String },
    ReasoningEnd { index: u64, content: String },
}

pub type EnrichedModelStream = futures_util::stream::BoxStream<'static, Result<EnrichedModelEvent>>;

/// An enriched event with an optional redacted-thinking start marker.
#[derive(Clone)]
pub struct DetailedModelEvent {
    pub event: EnrichedModelEvent,
    redacted: bool,
}
impl DetailedModelEvent {
    pub fn redacted_start(index: u64) -> Self {
        Self {
            event: EnrichedModelEvent::ReasoningStart { index },
            redacted: true,
        }
    }
    pub fn is_redacted_start(&self) -> bool {
        self.redacted
    }
}
impl From<EnrichedModelEvent> for DetailedModelEvent {
    fn from(event: EnrichedModelEvent) -> Self {
        Self {
            event,
            redacted: false,
        }
    }
}
pub type DetailedModelStream = futures_util::stream::BoxStream<'static, Result<DetailedModelEvent>>;

/// Identity and cache purpose fixed when preparing a model dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDispatchContext {
    pub request_id: Uuid,
    pub session_id: Uuid,
    pub external_id: String,
    pub summary: bool,
}

/// An immutable prepared model dispatch and its actual tool-sampling decisions.
#[async_trait::async_trait]
pub trait PreparedToolDispatch: Send {
    fn effective_tools(&self) -> &[crate::EffectiveToolDeclaration];
    /// Snapshot bound to the final request, decisions and dispatch context.
    fn decision_snapshot(&self) -> &crate::ToolDecisionSnapshot;
    /// Request after host filtering and before tool-schema transformation.
    fn dispatch_request(&self) -> &ModelRequest;
    /// Request with the tool schemas actually sent, for budget estimation.
    fn reservation_request(&self) -> &ModelRequest;
    async fn dispatch(self: Box<Self>) -> Result<DetailedModelStream>;
}

/// Verify a prepared adapter's snapshot and exposed decisions against the
/// caller's final declarations before recording or dispatching it.
pub fn validate_prepared_tool_dispatch<'a>(
    prepared: &'a dyn PreparedToolDispatch,
    context: &ToolDispatchContext,
    request: &ModelRequest,
    declarations: &[crate::ToolDeclaration],
) -> Result<crate::ValidatedToolDecisionSnapshot<'a>> {
    let validated = prepared
        .decision_snapshot()
        .validate(context, request, prepared.dispatch_request(), declarations)
        .map_err(|error| Error::Config(error.to_string()))?;
    if validated.effective_tools() != prepared.effective_tools() {
        return Err(Error::Config(
            "prepared tool decisions do not match snapshot".into(),
        ));
    }
    Ok(validated)
}

#[async_trait::async_trait]
pub trait Model: Send + Sync {
    /// Resolve a request-specific adapter once for estimation and dispatch.
    async fn prepare_context<'a>(
        &'a self,
        _request: &ModelRequest,
    ) -> Result<Option<std::sync::Arc<dyn Model + 'a>>> {
        Ok(None)
    }

    /// Prepare the actual tool decisions and dispatch together. A legacy
    /// adapter may return `None` only after rejecting required constraints;
    /// in that case every tool is treated as unconstrained.
    async fn prepare_tool_dispatch<'a>(
        &'a self,
        request: &ModelRequest,
        declarations: &[crate::ToolDeclaration],
        _context: &ToolDispatchContext,
    ) -> Result<Option<Box<dyn PreparedToolDispatch + 'a>>> {
        validate_legacy_tool_declarations(request, declarations)?;
        Ok(None)
    }

    /// Stable identity of all configuration affecting the effective request.
    /// Return None when the adapter cannot prove that identity across requests.
    /// Implementations must not return credentials or other secrets in plaintext.
    fn context_identity(&self) -> Option<String> {
        None
    }

    /// Estimate a message in the form this adapter sends to its provider.
    fn estimate_message_tokens(&self, _request: &ModelRequest, message: &Message) -> u64 {
        crate::estimate_message_tokens(message)
    }

    /// Estimate the complete request, including provider-specific message serialization.
    fn estimate_request_tokens(&self, request: &ModelRequest) -> u64 {
        let mut base = request.clone();
        base.messages.clear();
        let messages = request
            .messages
            .iter()
            .map(|message| self.estimate_message_tokens(request, message))
            .fold(0_u64, u64::saturating_add);
        crate::estimate_request_tokens(&base).saturating_add(messages)
    }

    /// Effective context capacity, when the provider can discover it.
    async fn context_window(&self, _model: &str) -> Option<u32> {
        None
    }

    /// Smallest positive output budget accepted by this adapter.
    fn minimum_output_tokens(&self) -> u32 {
        1
    }

    async fn stream(&self, request: ModelRequest) -> Result<ModelStream>;

    /// Host adapters can reserve budget using this ID before calling a provider.
    async fn stream_with_id(
        &self,
        _request_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream(request).await
    }

    /// Provide the active session identity while preserving request-ID handling.
    async fn stream_in_session(
        &self,
        request_id: Uuid,
        _session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_with_id(request_id, request).await
    }

    /// Provide a Pi-compatible external identity without breaking existing models.
    async fn stream_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        _external_id: &str,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_in_session(request_id, session_id, request)
            .await
    }

    /// Summaries use their own prompt and should bypass reusable conversation caches.
    async fn stream_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream> {
        self.stream_in_session(request_id, session_id, request)
            .await
    }

    /// Stream with item-level reasoning events, defaulting to the legacy session stream.
    async fn stream_enriched_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        use futures_util::StreamExt;
        Ok(self
            .stream_in_session_with_identity(request_id, session_id, external_id, request)
            .await?
            .map(|event| event.map(EnrichedModelEvent::Legacy))
            .boxed())
    }

    /// Stream a summary with item-level reasoning events, defaulting to the legacy summary stream.
    async fn stream_enriched_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        use futures_util::StreamExt;
        Ok(self
            .stream_for_summary(request_id, session_id, request)
            .await?
            .map(|event| event.map(EnrichedModelEvent::Legacy))
            .boxed())
    }

    /// Enriched stream with optional redacted-thinking start marker.
    async fn stream_detailed_in_session_with_identity(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
    ) -> Result<DetailedModelStream> {
        use futures_util::StreamExt;
        Ok(self
            .stream_enriched_in_session_with_identity(request_id, session_id, external_id, request)
            .await?
            .map(|event| event.map(Into::into))
            .boxed())
    }

    /// Summary counterpart of the detailed stream.
    async fn stream_detailed_for_summary(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
    ) -> Result<DetailedModelStream> {
        use futures_util::StreamExt;
        Ok(self
            .stream_enriched_for_summary(request_id, session_id, request)
            .await?
            .map(|event| event.map(Into::into))
            .boxed())
    }

    /// Stream with a snapshot of per-tool constraints. Legacy models may fall
    /// back for preferred constraints, but must fail closed for required ones.
    async fn stream_detailed_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        external_id: &str,
        request: ModelRequest,
        declarations: &[crate::ToolDeclaration],
    ) -> Result<DetailedModelStream> {
        validate_legacy_tool_declarations(&request, declarations)?;
        self.stream_detailed_in_session_with_identity(request_id, session_id, external_id, request)
            .await
    }

    /// Summary counterpart of the constrained detailed stream.
    async fn stream_detailed_summary_with_tool_declarations(
        &self,
        request_id: Uuid,
        session_id: Uuid,
        request: ModelRequest,
        declarations: &[crate::ToolDeclaration],
    ) -> Result<DetailedModelStream> {
        validate_legacy_tool_declarations(&request, declarations)?;
        self.stream_detailed_for_summary(request_id, session_id, request)
            .await
    }

    /// Called after the usage record is durable and before tools or another request.
    async fn record_usage(&self, _request_id: Uuid, _usage: &UsageReport) -> Result<()> {
        Ok(())
    }

    /// Settle usage with the metadata already committed in the journal.
    /// Hosted implementations must be idempotent for a request ID.
    async fn record_usage_with_metadata(
        &self,
        request_id: Uuid,
        usage: &UsageReport,
        _metadata: &ModelMetadata,
    ) -> Result<()> {
        self.record_usage(request_id, usage).await
    }
}

/// Check declarations before falling back to a legacy model adapter.
pub fn validate_legacy_tool_declarations(
    request: &ModelRequest,
    declarations: &[crate::ToolDeclaration],
) -> Result<()> {
    let mut names = std::collections::BTreeSet::new();
    if request.tools.len() != declarations.len()
        || request
            .tools
            .iter()
            .zip(declarations)
            .any(|(spec, declaration)| spec != &declaration.spec || !names.insert(&spec.name))
    {
        return Err(Error::Config(
            "tool declarations do not match the effective request".into(),
        ));
    }
    if let Some(declaration) = declarations.iter().find(|declaration| {
        matches!(
            declaration.constrained_sampling,
            Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Require
            })
        )
    }) {
        return Err(Error::Config(format!(
            "Tool \"{}\" requires JSON-schema constrained sampling, but strict tools are unsupported.",
            declaration.spec.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod declaration_dispatch_tests {
    use super::*;

    struct LegacyModel;
    struct InconsistentDispatch {
        snapshot: crate::ToolDecisionSnapshot,
        request: ModelRequest,
        effective: Vec<crate::EffectiveToolDeclaration>,
    }
    #[async_trait::async_trait]
    impl PreparedToolDispatch for InconsistentDispatch {
        fn effective_tools(&self) -> &[crate::EffectiveToolDeclaration] {
            &self.effective
        }
        fn decision_snapshot(&self) -> &crate::ToolDecisionSnapshot {
            &self.snapshot
        }
        fn dispatch_request(&self) -> &ModelRequest {
            &self.request
        }
        fn reservation_request(&self) -> &ModelRequest {
            &self.request
        }
        async fn dispatch(self: Box<Self>) -> Result<DetailedModelStream> {
            panic!("must not dispatch")
        }
    }
    #[async_trait::async_trait]
    impl Model for LegacyModel {
        async fn stream(&self, _request: ModelRequest) -> Result<ModelStream> {
            Ok(Box::pin(futures_util::stream::empty()))
        }
    }

    #[test]
    fn prepared_dispatch_rejects_decisions_that_disagree_with_snapshot() {
        let spec = ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        };
        let request = ModelRequest {
            model: "m".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 1,
        };
        let declaration = crate::ToolDeclaration {
            spec: spec.clone(),
            constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Prefer,
            }),
        };
        let strict = crate::EffectiveToolDeclaration {
            spec: spec.clone(),
            sampling: crate::EffectiveToolSampling::StrictJsonSchema {
                original_schema: spec.parameters.clone(),
                normalization_version: crate::STRICT_TOOL_NORMALIZATION_VERSION,
            },
        };
        let context = ToolDispatchContext {
            request_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            external_id: "s".into(),
            summary: false,
        };
        let snapshot = crate::ToolDecisionSnapshot::capture(
            &context,
            &request,
            std::slice::from_ref(&declaration),
            &[strict],
        )
        .unwrap();
        let dispatch = InconsistentDispatch {
            snapshot,
            request: request.clone(),
            effective: vec![crate::EffectiveToolDeclaration {
                spec,
                sampling: crate::EffectiveToolSampling::Unconstrained,
            }],
        };
        assert!(
            validate_prepared_tool_dispatch(&dispatch, &context, &request, &[declaration]).is_err()
        );
    }

    #[tokio::test]
    async fn legacy_models_fail_closed_for_required_tool_constraints() {
        let spec = ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        };
        let request = ModelRequest {
            model: "test".into(),
            messages: vec![],
            tools: vec![spec.clone()],
            max_output_tokens: 1,
        };
        let mut declaration = crate::ToolDeclaration {
            spec,
            constrained_sampling: Some(crate::ToolConstraint::JsonSchema {
                strict: crate::StrictToolPolicy::Prefer,
            }),
        };
        let model = LegacyModel;
        let context = ToolDispatchContext {
            request_id: Uuid::nil(),
            session_id: Uuid::nil(),
            external_id: "id".into(),
            summary: false,
        };
        assert!(
            model
                .prepare_tool_dispatch(&request, std::slice::from_ref(&declaration), &context)
                .await
                .unwrap()
                .is_none()
        );
        let duplicate = ModelRequest {
            tools: vec![request.tools[0].clone(), request.tools[0].clone()],
            ..request.clone()
        };
        assert!(
            validate_legacy_tool_declarations(
                &duplicate,
                &[declaration.clone(), declaration.clone()]
            )
            .is_err()
        );
        assert!(
            model
                .stream_detailed_with_tool_declarations(
                    Uuid::nil(),
                    Uuid::nil(),
                    "id",
                    request.clone(),
                    std::slice::from_ref(&declaration)
                )
                .await
                .is_ok()
        );
        declaration.constrained_sampling = Some(crate::ToolConstraint::JsonSchema {
            strict: crate::StrictToolPolicy::Require,
        });
        assert!(
            model
                .prepare_tool_dispatch(&request, std::slice::from_ref(&declaration), &context)
                .await
                .is_err()
        );
        assert!(
            model
                .stream_detailed_with_tool_declarations(
                    Uuid::nil(),
                    Uuid::nil(),
                    "id",
                    request.clone(),
                    std::slice::from_ref(&declaration)
                )
                .await
                .is_err()
        );
        assert!(
            model
                .stream_detailed_summary_with_tool_declarations(
                    Uuid::nil(),
                    Uuid::nil(),
                    request.clone(),
                    std::slice::from_ref(&declaration)
                )
                .await
                .is_err()
        );
        declaration.spec.name = "changed".into();
        assert!(
            model
                .stream_detailed_with_tool_declarations(
                    Uuid::nil(),
                    Uuid::nil(),
                    "id",
                    request,
                    &[declaration]
                )
                .await
                .is_err()
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
pub enum Outcome {
    Succeeded(String),
    /// Successful text output with journaled details excluded from model context.
    SucceededWithDetails {
        output: String,
        details: Value,
    },
    Failed(String),
    /// A patched text result with independent model content, metadata and error status.
    TextResult {
        output: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
        is_error: bool,
    },
    /// Text sent to the model with a separate structured result for event consumers.
    TextResultWithStructuredContent {
        output: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
        structured_content: Value,
        is_error: bool,
    },
    Skipped(String),
    /// An effect may have occurred. Only an authoritative reconciliation can clear this state.
    Unknown(String),
    /// A remote job was accepted and needs an authoritative terminal result.
    Pending {
        reference: String,
    },
    Truncated {
        original_bytes: usize,
    },
    Structured {
        data: Value,
        attachments: Vec<AttachmentRef>,
    },
    /// Structured output with metadata retained outside model-facing content.
    StructuredWithDetails {
        data: Value,
        attachments: Vec<AttachmentRef>,
        details: Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttachmentRef {
    /// A host-owned reference, never an access token or a signed download URL.
    pub id: String,
    pub media_type: String,
}

impl Outcome {
    pub fn is_error(&self) -> bool {
        match self {
            Self::Failed(_) | Self::Skipped(_) | Self::Unknown(_) => true,
            Self::TextResult { is_error, .. }
            | Self::TextResultWithStructuredContent { is_error, .. } => *is_error,
            Self::Structured { data, .. } | Self::StructuredWithDetails { data, .. } => {
                data["isError"] == true
            }
            _ => false,
        }
    }

    /// Whether this outcome contains structured tool data.
    pub fn is_structured(&self) -> bool {
        matches!(
            self,
            Self::Structured { .. } | Self::StructuredWithDetails { .. }
        )
    }

    /// Lossy projection for older native executable protocols.
    pub fn legacy_text_result(&self) -> Self {
        match self {
            Self::TextResult {
                output,
                details,
                is_error: false,
            } => match details {
                Some(details) => Self::SucceededWithDetails {
                    output: output.clone(),
                    details: details.clone(),
                },
                None => Self::Succeeded(output.clone()),
            },
            Self::TextResult {
                output,
                is_error: true,
                ..
            } => Self::Failed(output.clone()),
            Self::TextResultWithStructuredContent {
                output,
                details,
                is_error,
                ..
            } => Self::TextResult {
                output: output.clone(),
                details: details.clone(),
                is_error: *is_error,
            }
            .legacy_text_result(),
            _ => self.clone(),
        }
    }

    /// Project the structured-result extension to the existing text-result protocol.
    pub fn protocol_text_result(&self) -> Self {
        match self {
            Self::TextResultWithStructuredContent {
                output,
                details,
                is_error,
                ..
            } => Self::TextResult {
                output: output.clone(),
                details: details.clone(),
                is_error: *is_error,
            },
            _ => self.clone(),
        }
    }

    pub fn tool_content(&self) -> String {
        match self {
            Self::Succeeded(output) | Self::SucceededWithDetails { output, .. } => output.clone(),
            Self::Failed(reason) => format!("Tool failed: {reason}"),
            Self::TextResult { output, .. }
            | Self::TextResultWithStructuredContent { output, .. } => output.clone(),
            Self::Skipped(reason) => format!("Tool did not start: {reason}"),
            Self::Unknown(reason) => format!("Tool outcome unknown: {reason}"),
            Self::Pending { .. } => "Tool operation is still pending".into(),
            Self::Truncated { original_bytes } => format!(
                "Tool succeeded. Output omitted ({original_bytes} bytes exceeded the limit)."
            ),
            Self::Structured { data, attachments }
            | Self::StructuredWithDetails {
                data, attachments, ..
            } => serde_json::json!({ "data": data, "attachments": attachments }).to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Invocation {
    pub operation_id: Uuid,
    pub call: ToolCall,
}

/// A tool result and an optional replacement for its owning extension state.
/// Both are committed together; the state is excluded from model-facing output.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolReply {
    pub outcome: Outcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ExtensionState>,
}
impl From<Outcome> for ToolReply {
    fn from(outcome: Outcome) -> Self {
        Self {
            outcome,
            state: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtensionState {
    pub version: u32,
    pub data: Value,
}

/// User-initiated shell output, retained even when excluded from model context.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BashExecution {
    pub command: String,
    #[serde(default)]
    pub output: String,
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub full_output_path: Option<String>,
    #[serde(default)]
    pub exclude_from_context: bool,
}
impl BashExecution {
    pub fn context_text(&self) -> String {
        let mut text = format!("Ran `{}`\n", self.command);
        if self.output.is_empty() {
            text.push_str("(no output)");
        } else {
            text.push_str(&format!("```\n{}\n```", self.output));
        }
        if self.cancelled {
            text.push_str("\n\n(command cancelled)");
        } else if let Some(code) = self.exit_code.filter(|code| *code != 0) {
            text.push_str(&format!("\n\nCommand exited with code {code}"));
        }
        if self.truncated
            && let Some(path) = &self.full_output_path
        {
            text.push_str(&format!("\n\n[Output truncated. Full output: {path}]"));
        }
        text
    }
}
