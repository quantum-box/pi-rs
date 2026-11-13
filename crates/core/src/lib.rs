//! Model, session, and tool execution primitives for an embeddable agent.
mod agent;
mod context;
mod context_fingerprint;
mod context_replay;
mod extension;
mod journal;
mod lifecycle;
mod retry;
mod sdk;
mod statistics;
mod tokens;
mod tool_arguments;
mod types;
mod ui;

pub use agent::*;
pub use context::*;
pub use context_fingerprint::protected_model_fingerprint;
pub use context_replay::endpoint_replay_scope;
pub use extension::*;
pub use journal::*;
pub use lifecycle::{
    BeforeSessionTransition, CompactionReason, ExtensionTreeSummary, ForkPosition, LifecycleEvent,
    LifecycleMessage, ModelSelectSource, SessionShutdownReason, SessionStartReason,
    SessionSwitchReason, TreeDecision, TreePreparation, TreeSummary,
    notify as notify_extension_lifecycle,
};
pub use retry::*;
pub use sdk::*;
pub use statistics::{ContextUsage, SessionStats, TokenTotals};
pub use tokens::{
    ContextUsageEstimate, estimate_message_tokens, estimate_replay_tokens, estimate_request_tokens,
    fingerprint_model_context,
};
pub use tokio_util::sync::CancellationToken;
pub use tool_arguments::parse_tool_arguments;
pub use types::*;
pub use ui::*;
