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
    ClearQueue {
        reply: tokio::sync::oneshot::Sender<Result<Vec<QueuedMessage>>>,
    },
    SetQueueMode {
        kind: InputKind,
        mode: QueueMode,
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
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
    },
    Thinking {
        thinking: String,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
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
    },
    Tool {
        call_id: String,
        content: String,
    },
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ModelRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub max_output_tokens: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelMetadata {
    pub provider_request_id: Option<String>,
    pub cached_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    /// Provider-native prompt count when the host reports a normalized total.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_prompt_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_model: Option<ResolvedModelIdentity>,
    pub finish_reason: Option<String>,
    /// Provider-supplied visible reasoning, retained for session presentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
}

/// Canonical provider and model names reported by the host adapter.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedModelIdentity {
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UsageReport {
    Measured { usage: Usage },
    Estimated { output_tokens: u64 },
    Unconfirmed,
}

#[derive(Clone, Debug)]
pub enum ModelEvent {
    Text(String),
    Metadata(ModelMetadata),
    Complete {
        calls: Vec<ToolCall>,
        usage: Option<Usage>,
    },
}

pub type ModelStream = futures_util::stream::BoxStream<'static, Result<ModelEvent>>;

#[async_trait::async_trait]
pub trait Model: Send + Sync {
    /// Effective context capacity, when the provider can discover it.
    async fn context_window(&self, _model: &str) -> Option<u32> {
        None
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
pub enum Outcome {
    Succeeded(String),
    Failed(String),
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttachmentRef {
    /// A host-owned reference, never an access token or a signed download URL.
    pub id: String,
    pub media_type: String,
}

impl Outcome {
    pub fn tool_content(&self) -> String {
        match self {
            Self::Succeeded(output) => output.clone(),
            Self::Failed(reason) => format!("Tool failed: {reason}"),
            Self::Skipped(reason) => format!("Tool did not start: {reason}"),
            Self::Unknown(reason) => format!("Tool outcome unknown: {reason}"),
            Self::Pending { .. } => "Tool operation is still pending".into(),
            Self::Truncated { original_bytes } => format!(
                "Tool succeeded. Output omitted ({original_bytes} bytes exceeded the limit)."
            ),
            Self::Structured { data, attachments } => {
                serde_json::json!({ "data": data, "attachments": attachments }).to_string()
            }
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
