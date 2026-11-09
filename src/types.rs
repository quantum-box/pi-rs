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
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// Retain raw arguments so malformed model output can be handled as a tool error.
    pub arguments: String,
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

#[derive(Clone, Debug)]
pub enum ModelEvent {
    Text(String),
    Complete {
        calls: Vec<ToolCall>,
        usage: Option<Usage>,
    },
}

pub type ModelStream = futures_util::stream::BoxStream<'static, Result<ModelEvent>>;

#[async_trait::async_trait]
pub trait Model: Send + Sync {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "detail", rename_all = "snake_case")]
pub enum Outcome {
    Succeeded(String),
    Failed(String),
    Skipped(String),
    /// An effect may have occurred. Only an authoritative reconciliation can clear this state.
    Unknown(String),
}

impl Outcome {
    pub fn tool_content(&self) -> String {
        match self {
            Self::Succeeded(output) => output.clone(),
            Self::Failed(reason) => format!("Tool failed: {reason}"),
            Self::Skipped(reason) => format!("Tool did not start: {reason}"),
            Self::Unknown(reason) => format!("Tool outcome unknown: {reason}"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Invocation {
    pub operation_id: Uuid,
    pub call: ToolCall,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtensionState {
    pub version: u32,
    pub data: Value,
}
