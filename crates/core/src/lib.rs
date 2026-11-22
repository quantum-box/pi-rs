//! Model, session, and tool execution primitives for an embeddable agent.
mod agent;
mod compaction_files;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod context;
mod context_fingerprint;
mod context_replay;
mod extension;
mod extension_boundary;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod host_auth;
mod journal;
mod lifecycle;
mod mcp_registration;
mod model_catalog;
mod model_price;
mod persistent_map;
#[doc(hidden)]
pub mod pi_import;
mod provider_hooks;
mod provider_retry;
mod resource;
mod retry;
mod runtime;
mod sdk;
mod statistics;
mod strict_tool_schema;
mod tokens;
mod tool_execution_snapshot;
mod tool_render;
mod types;
mod ui;
mod ui_prompt;

pub use agent::*;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use context::*;
pub use context_fingerprint::protected_model_fingerprint;
pub use context_replay::endpoint_replay_scope;
pub use extension::*;
pub use extension_boundary::{
    AgentActivityOutcome, AgentBeforeSettle, BoundaryContext, BoundaryDecision, BoundaryDraft,
};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use host_auth::*;
pub use journal::*;
pub use lifecycle::{
    BeforeSessionTransition, CompactionReason, ExtensionTreeSummary, ForkPosition, LifecycleEvent,
    LifecycleMessage, ModelSelectSource, SessionShutdownReason, SessionStartReason,
    SessionSwitchReason, TreeDecision, TreePreparation, TreeSummary,
    notify as notify_extension_lifecycle, notify_filtered as notify_extension_lifecycle_filtered,
    notify_in_session as notify_extension_lifecycle_in_session,
};
pub use mcp_registration::{McpServerDeclaration, RegisteredMcpServer};
pub use model_catalog::*;
pub use model_price::ChatPrice;
pub use pi_rs_tool_arguments::{StreamingJson, parse_tool_arguments};
pub use provider_hooks::{
    ProviderResponseEvent, ProviderStreamEvent, observe_provider_response, observe_provider_stream,
    observes_provider_response, observes_provider_stream,
};
pub use provider_retry::ProviderRetry;
pub use resource::Resource;
pub use retry::*;
pub use sdk::*;
pub use statistics::{ContextUsage, SessionStats, TokenTotals};
pub use strict_tool_schema::{
    EffectiveToolDeclaration, EffectiveToolSampling, STRICT_TOOL_NORMALIZATION_VERSION,
    StrictSchemaError, StrictToolPolicy, ToolConstraint, ToolDecisionSnapshot, ToolDeclaration,
    ToolGrammarSyntax, ToolGrammarVariants, ValidatedToolDecisionSnapshot,
    lossless_openai_grammar_input, normalize_strict_tool_arguments, resolve_openai_tool_grammar,
    resolve_strict_tool_schema, strict_tool_schema, validate_effective_tool_declarations,
};
pub use tokens::{
    ContextUsageEstimate, estimate_assistant_output_tokens, estimate_message_tokens,
    estimate_replay_tokens, estimate_request_tokens, fingerprint_model_context,
};
pub use tokio_util::sync::CancellationToken;
pub use tool_execution_snapshot::{ToolExecutionSnapshot, ValidatedToolExecutionSnapshot};
pub use tool_render::*;
pub use types::*;
pub use ui::*;
pub use ui_prompt::{UiPromptKind, UiPromptReason};
