//! Durable conversation execution with a single local scheduler owner.
//!
//! A committed phase is the authority for recovery. External calls never run
//! inside storage transactions. Independent conversations can run concurrently.
pub use crate::state::{AdmissionError, AdmissionOptions, InputMode};
/// Host-attested identities for the summary model and usage settlement service.
/// They must change whenever dispatch or request-ID settlement semantics change.
pub use crate::summary_store::ProviderIdentity as SummaryProviderIdentity;
use crate::{
    model::{
        CompletedGeneration, GenerationProgress, collect_generation_with_metadata, model_request,
    },
    recovery::{ReplayPolicy, ToolRecovery, UncertainPolicy, recover_tool},
    retry_policy::{Decision, Failure, RetryPolicy, RetryState, StopReason, is_context_overflow},
};
use durable_store::{
    Check, Expected, Mutation,
    owner::{OwnedStore, OwnerError},
};
use futures_util::FutureExt;
pub use pi_rs_core::QueueMode;
use pi_rs_core::{
    CancellationToken, ExtensionState, Invocation, Message, Model, Outcome, Tool, ToolCall,
    ToolSpec, UsageReport,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

type InheritedApplicationPrefix = (u64, Uuid, Vec<crate::application_entry::ApplicationEntry>);
type ApplicationEntryCut = (
    u64,
    u64,
    Vec<crate::application_entry::ApplicationEntry>,
    Option<InheritedApplicationPrefix>,
);
type ResolvedOccurrences = (
    Vec<(Message, Option<MessageHistory>)>,
    std::collections::HashSet<Uuid>,
);
type ResolvedCompactionTarget = (Vec<Message>, (usize, usize));

#[cfg(test)]
#[path = "summary_worker_tests.rs"]
mod summary_worker_tests;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("owner: {0}")]
    Owner(#[from] OwnerError),
    #[error("storage: {0}")]
    Storage(#[from] durable_store::Error),
    #[error("serialization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("observation: {0}")]
    Observer(#[from] crate::observer::Error),
    #[error("generation progress: {0}")]
    Progress(#[from] crate::progress::Error),
    #[error("tool progress: {0}")]
    ToolProgress(#[from] crate::tool_progress::Error),
    #[error("transcript: {0}")]
    Transcript(#[from] crate::transcript::Error),
    #[error("conversation signal: {0}")]
    Signal(#[from] crate::conversation_signal::Error),
    #[error("document: {0}")]
    Document(#[from] crate::document::Error),
    #[error("fork: {0}")]
    Fork(#[from] crate::fork::Error),
    #[error("task scheduler: {0}")]
    TaskScheduler(#[from] crate::task_scheduler::Error),
    #[error("model: {0}")]
    Model(#[from] pi_rs_core::Error),
    #[error("conversation not found")]
    NotFound,
    #[error("submission not found in conversation")]
    SubmissionNotFound,
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error("session failed; reopen before further operations")]
    Failed,
    #[error("execution interrupted; reopen to recover")]
    Interrupted,
    #[error("wait cancelled")]
    WaitCancelled,
    #[error("invalid operation: {0}")]
    Invalid(String),
}

// Bound synchronous retries so unrelated sustained writes cannot block a caller forever.
const FORK_DOCUMENT_ATTEMPTS: usize = 8;
const FORK_DOCUMENT_FENCE: &[u8] = b"durable/document-fork-revision";
// Independent placement intentionally rejects histories requiring more work.
const SUMMARY_MAX_HISTORY_RECORDS: usize = 128;
const SUMMARY_MAX_HISTORY_BYTES: usize = 4 * 1024 * 1024;
const SUMMARY_MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SummaryPlacement {
    Placed,
    Stale,
    Queued,
}

fn retry_document_fork(mut attempt: impl FnMut() -> Result<(), Error>) -> Result<(), Error> {
    for remaining in (0..FORK_DOCUMENT_ATTEMPTS).rev() {
        match attempt() {
            Err(Error::Storage(durable_store::Error::Conflict { key }))
                if key == FORK_DOCUMENT_FENCE && remaining > 0 =>
            {
                continue;
            }
            result => return result,
        }
    }
    unreachable!()
}

#[derive(Clone)]
pub struct RegisteredTool {
    pub tool: Arc<dyn Tool>,
    pub replay: ReplayPolicy,
}

#[derive(Clone)]
pub struct Options {
    pub model: Arc<dyn Model>,
    pub tools: Vec<RegisteredTool>,
    pub restart_limit: u32,
    pub max_turns: u32,
    pub uncertain_policy: UncertainPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub model: String,
    pub system_prompt: String,
    pub max_output_tokens: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Status {
    Queued,
    Running,
    Done { answer: String },
    Unanswered { reason: String },
}
/// Persisted record of the most recent run that ended without an answer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunFailure {
    pub inputs: Vec<Uuid>,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Durable outcome of an idle manual compaction operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ManualCompactionStatus {
    Running,
    Applied,
    Declined,
    Aborted,
    RestartLimit,
    Stale,
}

/// Status of the opt-in idle-only independent compaction path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndependentCompactionStatus {
    Running,
    Aborting,
    Applied,
    Stale,
    Aborted,
    Declined,
}

/// An operation ID is stable across summary attempts and process recovery.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManualCompaction {
    pub id: Uuid,
    pub status: ManualCompactionStatus,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionKind {
    #[default]
    Input,
    Continue,
}

/// An input withdrawn by an atomic conversation abort, in its conversation's queue order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WithdrawnInput {
    pub conversation_id: Uuid,
    pub mode: InputMode,
    pub submission: Submission,
}

/// Inputs withdrawn by a conversation abort.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConversationAbortResult {
    pub withdrawn: Vec<WithdrawnInput>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Submission {
    #[serde(default)]
    pub kind: SubmissionKind,
    pub id: Uuid,
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    /// Model-visible preparation diagnostics for an image admission.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_notes: Vec<String>,
    /// Ordered fitted input, including its durable occurrence identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_blocks: Option<crate::transcript::MessageContentBlocks>,
    pub status: Status,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FittedResetReceipt {
    reset: Uuid,
    occurrences: Vec<Uuid>,
    notes: Vec<String>,
}
/// A durable application entry. Queued entries wait for their preceding reset.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApplicationWrite {
    pub id: Uuid,
    pub request_id: Option<String>,
    pub kind: String,
    pub data: Option<serde_json::Value>,
    pub placed: bool,
    /// Terminal withdrawal of a write that was still queued.
    #[serde(default)]
    pub withdrawn: bool,
    /// The last reset preceding this write, if any.
    pub after_reset: Option<Uuid>,
}
/// A committed terminal result for a submission in this conversation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubmissionSettlement {
    Input {
        answer: String,
        answer_id: Option<Uuid>,
    },
    Continue {
        answer: String,
        answer_id: Option<Uuid>,
    },
    Unanswered {
        kind: SubmissionKind,
        reason: String,
    },
    /// The display-only application write has been placed.
    Write,
    /// The queued display-only write was withdrawn before placement.
    WriteUnanswered { reason: String },
}

/// Result of attempting to withdraw a single submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WithdrawResult {
    Aborted,
    AlreadyPlaced,
    Settled,
    NotFound,
}
/// A persisted interrupted partial, for display only; it is never sent to a model.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AbortedGeneration {
    pub request_id: Uuid,
    pub text: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Phase {
    Idle,
    /// A failed run retains its queued inputs until a new input arrives.
    FailedIdle,
    ModelReady {
        turns: u32,
    },
    ModelRunning {
        turns: u32,
        request_id: Uuid,
        restarts: u32,
    },
    AbandonedModel {
        turns: u32,
        request_id: Uuid,
        restarts: u32,
    },
    FailedModel {
        turns: u32,
        request_id: Uuid,
        reason: String,
        decision: ModelFailureDecision,
    },
    RetryWait {
        turns: u32,
    },
    CompactionHook {
        preparation: Box<crate::compact::Preparation>,
        operation_id: Uuid,
        reason: crate::extension::CompactionReason,
    },
    CompactionReady {
        preparation: Box<crate::compact::Preparation>,
        restarts: u32,
    },
    CompactionRunning {
        preparation: Box<crate::compact::Preparation>,
        request_id: Uuid,
        restarts: u32,
    },
    CompactionAbandoned {
        preparation: Box<crate::compact::Preparation>,
        request_id: Uuid,
        restarts: u32,
    },
    CompactionAccounting {
        result: Box<crate::compact::CompactionResult>,
        request_id: Uuid,
    },
    /// A completed, accounted summary awaiting an atomic placement decision.
    CompactionPlacement {
        result: Box<crate::compact::CompactionResult>,
        /// Resets admitted before this manual result. None preserves legacy and
        /// blocking automatic compaction behavior.
        #[serde(default)]
        resets_before: Option<Vec<Uuid>>,
    },
    Accounting {
        turns: u32,
        request_id: Uuid,
        response: Box<CompletedGeneration>,
    },
    Boundary {
        turns: u32,
        answer: Option<String>,
    },
    OnYield {
        turns: u32,
        answer: String,
        operation_id: Uuid,
    },
    /// A completed on-yield decision, persisted before checking the inbox.
    YieldDecision {
        turns: u32,
        answer: String,
        continuation: Option<crate::content::UserContent>,
    },
    YieldTasksWaiting {
        turns: u32,
        answer: String,
        children: Vec<Uuid>,
    },
    AfterTools {
        turns: u32,
        calls: Vec<ToolCall>,
        operation_id: Uuid,
    },
    AfterRun {
        answer: String,
        operation_id: Uuid,
    },
    HookTasksWaiting {
        children: Vec<Uuid>,
        turns: Option<u32>,
    },
    Tools {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
    },
    /// The operation identity is committed before extension code runs.
    ToolPreparing {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        #[serde(default)]
        wake: Option<serde_json::Value>,
    },
    ToolGateSleeping {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        until_millis: Option<i64>,
        signals: Vec<String>,
    },
    ToolRunning {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        replay: ReplayPolicy,
        arguments: serde_json::Value,
        restarts: u32,
        #[serde(default)]
        replay_ready: bool,
        #[serde(default)]
        dispatch_identity: Option<DispatchIdentity>,
    },
    /// An owned scheduler task and its tool operation were admitted atomically.
    TaskPending {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        arguments: serde_json::Value,
        task: Uuid,
        #[serde(default)]
        identity: Option<crate::task_tool::Identity>,
    },
    /// The tool outcome and its state are committed before after-tool hooks run.
    ToolFinished {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        arguments: serde_json::Value,
        content: String,
    },
    Blocked {
        #[serde(default)]
        tool_panicked: bool,
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        reason: String,
        #[serde(default)]
        arguments: Option<serde_json::Value>,
    },
    Pending {
        turns: u32,
        calls: Vec<ToolCall>,
        index: usize,
        operation_id: Uuid,
        reference: String,
        #[serde(default)]
        arguments: Option<serde_json::Value>,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum DispatchIdentity {
    Other,
    Task(crate::task_tool::Identity),
}

fn dispatch_identity(tool: &Arc<dyn pi_rs_core::Tool>) -> Result<DispatchIdentity, Error> {
    match crate::task_tool::lookup(tool) {
        Some(adapter) => adapter
            .identity()
            .map(DispatchIdentity::Task)
            .ok_or_else(|| {
                Error::Invalid(
                    "task adapter requires a nonempty kind and nonzero definition version".into(),
                )
            }),
        None => Ok(DispatchIdentity::Other),
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ModelFailureDecision {
    Retry,
    Overflow,
    Stop,
}

#[derive(Clone, Serialize, Deserialize)]
enum TranscriptChange {
    Append(Vec<Message>),
    Replace(Vec<Message>),
    AppendV2 {
        messages: Vec<Message>,
        ids: Vec<Uuid>,
        commit: u64,
        origin: Uuid,
    },
    AppendV3 {
        messages: Vec<Message>,
        ids: Vec<Uuid>,
        commit: u64,
        origin: Uuid,
        stable_prefix: usize,
    },
    AppendV4 {
        messages: Vec<Message>,
        ids: Vec<Uuid>,
        commit: u64,
        origin: Uuid,
        stable_prefix: usize,
    },
    ReplaceV2 {
        messages: Vec<Message>,
        ids: Vec<Uuid>,
        commit: u64,
        origin: Uuid,
    },
    /// A compaction projection of the immediately preceding active transcript.
    /// Retained provenance is derived from that link, never from message text.
    ForkCut {
        messages: Vec<Message>,
        histories: Vec<Option<MessageHistory>>,
        commit: u64,
        origin: Uuid,
    },
    ReplaceCompacted {
        messages: Vec<Message>,
        system_end: usize,
        first_kept: usize,
        summary_id: Uuid,
        commit: u64,
        origin: Uuid,
        histories: Vec<Option<MessageHistory>>,
    },
}

type TranscriptChangeParts = (Vec<Message>, bool, Option<(Vec<Uuid>, u64, Uuid)>);

impl TranscriptChange {
    fn compacted_histories(&self) -> Result<Option<&[Option<MessageHistory>]>, Error> {
        if let Self::ForkCut {
            messages,
            histories,
            ..
        } = self
        {
            let mut seen = std::collections::HashSet::new();
            if histories.len() != messages.len()
                || histories.iter().any(|history| {
                    history.as_ref().is_none_or(|h| {
                        h.id.is_nil() || h.origin.is_nil() || h.commit == 0 || !seen.insert(h.id)
                    })
                })
            {
                return Err(Error::Invalid("invalid fork cut metadata".into()));
            }
            return Ok(Some(histories));
        }
        let Self::ReplaceCompacted {
            messages,
            system_end,
            first_kept,
            summary_id,
            commit,
            origin,
            histories,
        } = self
        else {
            return Ok(None);
        };
        let mut seen = std::collections::HashSet::new();
        if *system_end > *first_kept
            || *system_end >= messages.len()
            || histories.len() != messages.len()
            || summary_id.is_nil()
            || origin.is_nil()
            || *commit == 0
            || !matches!(messages.get(*system_end), Some(Message::User { .. }))
            || histories[*system_end].as_ref()
                != Some(&MessageHistory {
                    id: *summary_id,
                    commit: *commit,
                    origin: *origin,
                    commit_boundary: *system_end + 1 == messages.len(),
                })
            || histories
                .iter()
                .flatten()
                .any(|h| h.id.is_nil() || h.origin.is_nil() || h.commit == 0 || !seen.insert(h.id))
        {
            return Err(Error::Invalid(
                "invalid compacted transcript metadata".into(),
            ));
        }
        Ok(Some(histories))
    }
    fn parts(self) -> Result<TranscriptChangeParts, Error> {
        self.compacted_histories()?;
        let (messages, base, metadata) = match self {
            Self::Append(messages) => (messages, false, None),
            Self::Replace(messages) => (messages, true, None),
            Self::AppendV2 {
                messages,
                ids,
                commit,
                origin,
            } => (messages, false, Some((ids, commit, origin))),
            Self::AppendV3 {
                messages,
                ids,
                commit,
                origin,
                ..
            } => (messages, false, Some((ids, commit, origin))),
            Self::AppendV4 {
                messages,
                ids,
                commit,
                origin,
                ..
            } => (messages, false, Some((ids, commit, origin))),
            Self::ReplaceV2 {
                messages,
                ids,
                commit,
                origin,
            } => (messages, true, Some((ids, commit, origin))),
            Self::ReplaceCompacted { messages, .. } | Self::ForkCut { messages, .. } => {
                (messages, true, None)
            }
        };
        if let Some((ids, commit, origin)) = &metadata {
            let unique: std::collections::HashSet<_> = ids.iter().collect();
            if ids.len() != messages.len()
                || unique.len() != ids.len()
                || ids.iter().any(Uuid::is_nil)
                || *commit == 0
                || origin.is_nil()
            {
                return Err(Error::Invalid("invalid transcript entry metadata".into()));
            }
        }
        Ok((messages, base, metadata))
    }
}

/// Action taken for inputs queued behind a terminal failed run.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AfterFailedRun {
    #[default]
    Keep,
    Continue,
    Withdraw,
}

/// Delivery selection at durable conversation boundaries.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct QueueModes {
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
}
impl Default for QueueModes {
    fn default() -> Self {
        Self {
            steering_mode: QueueMode::OneAtATime,
            follow_up_mode: QueueMode::OneAtATime,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Conversation {
    pub id: Uuid,
    pub config: Config,
    #[serde(default)]
    pub queue_modes: QueueModes,
    #[serde(default)]
    pub after_failed_run: AfterFailedRun,
    /// Installed extension names in execution order. Older conversations select none.
    #[serde(default)]
    pub extensions: Vec<String>,
    /// Stable adapter identity, if provider-body hooks are enabled.
    #[serde(default)]
    pub provider_adapter: Option<String>,
    /// Enabled tool names; None preserves the host default, Some([]) disables all.
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    /// Optional exact model-facing selection, bounded by enabled tools.
    #[serde(default)]
    pub model_tools: Option<Vec<String>>,
    /// Persisted remove filters and model-facing deferred-tool declarations.
    #[serde(default)]
    pub tool_filters: ToolFilters,
    /// Durable metadata for terminal tool operations, independent of model messages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<ToolResultMetadata>,
    #[serde(skip)]
    pending_tool_result: Option<ToolResultMetadata>,
    pub messages: Vec<Message>,
    /// Lossless user or tool block arrays indexed by active message position.
    /// Reconstructed from the transcript; never serialized in the snapshot.
    #[serde(skip)]
    pub content_blocks: std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks>,
    /// In-memory proof of a reset placed in the current transaction; never persisted.
    #[serde(skip)]
    block_reset_proof: Option<BlockResetProof>,
    #[serde(skip)]
    block_fork_cut: Option<usize>,
    /// Identity of the active context since its last reset or compaction.
    /// Older persisted conversations acquire one at the next compaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_generation: Option<Uuid>,
    /// False for stores whose earlier reset identity boundaries cannot be proved.
    #[serde(default)]
    occurrence_epoch_proven: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transcript: Option<crate::transcript::Cursor>,
    pub submissions: Vec<Submission>,
    #[serde(default)]
    pub manual_compactions: Vec<ManualCompaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_manual_compaction: Option<Uuid>,
    #[serde(default)]
    pub application_writes: Vec<ApplicationWrite>,
    #[serde(default)]
    pub aborted_generations: Vec<AbortedGeneration>,
    #[serde(default)]
    pub resets: Vec<crate::reset::Reset>,
    #[serde(default)]
    pending_resets: Vec<crate::reset::Reset>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pending_reset_blocks:
        std::collections::BTreeMap<Uuid, Vec<crate::transcript::MessageContentBlocks>>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    fitted_reset_receipts: std::collections::BTreeMap<String, FittedResetReceipt>,
    #[serde(default)]
    pending_independent_summary: Option<PendingIndependentSummary>,
    #[serde(default)]
    tool_states: HashMap<String, ExtensionState>,
    #[serde(default)]
    round_terminates: Vec<bool>,
    #[serde(default)]
    round_handoffs: Vec<Option<String>>,
    #[serde(default)]
    round_added_tools: Vec<Vec<String>>,
    #[serde(default)]
    input_modes: HashMap<Uuid, InputMode>,
    #[serde(default)]
    answer_ids: HashMap<Uuid, Uuid>,
    #[serde(default)]
    auto_compaction_attempted: bool,
    #[serde(default)]
    retry_state: RetryState,
    #[serde(default)]
    overflow_compaction_attempted: bool,
    #[serde(default)]
    compaction_return_turns: Option<u32>,
    active: Option<Uuid>,
    /// Most recent failed run, retained through its automatic replacement run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_failure: Option<RunFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    failure_continuation_inputs: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_attempt: Option<ToolAttempt>,
    #[serde(default)]
    pub abort_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nested_resume: Option<NestedResume>,
    phase: Phase,
}

/// A diagnostic associated with a tool result, not part of model output.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolDiagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticSeverity {
    Info,
    Warn,
    Error,
}
/// Separately retained metadata for one terminal tool operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolResultMetadata {
    pub operation_id: Uuid,
    pub call_id: String,
    pub tool: String,
    /// Occurrence ID of a block-valued result; absent in legacy or text results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_block_id: Option<Uuid>,
    pub is_error: bool,
    #[serde(default)]
    pub redacted: bool,
    #[serde(default)]
    pub metadata_truncated: bool,
    #[serde(default)]
    pub schema_validated: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_json"
    )]
    pub details: Option<serde_json::Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_json"
    )]
    pub structured_output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<pi_rs_core::AttachmentRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ToolDiagnostic>,
}

fn deserialize_present_json<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(deserializer).map(Some)
}

fn record_tool_result(
    c: &mut Conversation,
    calls: &[ToolCall],
    index: usize,
    operation_id: Uuid,
    outcome: &Outcome,
    mut diagnostics: Vec<ToolDiagnostic>,
) -> Result<(), Error> {
    let call = calls
        .get(index)
        .ok_or_else(|| Error::Invalid("invalid tool result index".into()))?;
    if c.tool_results
        .iter()
        .any(|result| result.operation_id == operation_id)
    {
        return Ok(());
    }
    let (mut details, mut structured_output, mut attachments) = match outcome {
        Outcome::SucceededWithDetails { details, .. } => (Some(details.clone()), None, Vec::new()),
        Outcome::Structured { data, attachments } => {
            (None, Some(data.clone()), attachments.clone())
        }
        _ => (None, None, Vec::new()),
    };
    if let Outcome::Failed(reason) | Outcome::Skipped(reason) = outcome {
        let code = if matches!(outcome, Outcome::Skipped(_)) {
            "tool_skipped"
        } else {
            "tool_failed"
        };
        diagnostics.push(ToolDiagnostic {
            severity: DiagnosticSeverity::Error,
            message: reason.clone(),
            code: Some(code.into()),
        });
    }
    if let Outcome::Truncated { original_bytes } = outcome {
        diagnostics.push(ToolDiagnostic {
            severity: DiagnosticSeverity::Warn,
            message: format!("tool output omitted ({original_bytes} bytes)"),
            code: Some("output_truncated".into()),
        });
    }
    let size = serde_json::to_vec(&(&details, &structured_output, &attachments, &diagnostics))
        .map_or(usize::MAX, |v| v.len());
    let metadata_truncated = size > 65_536;
    if metadata_truncated {
        details = None;
        structured_output = None;
        attachments.clear();
        diagnostics.clear();
        diagnostics.push(ToolDiagnostic {
            severity: DiagnosticSeverity::Warn,
            message: "tool result metadata exceeded 65536 bytes".into(),
            code: Some("metadata_truncated".into()),
        });
    }
    c.tool_results.push(ToolResultMetadata {
        operation_id,
        call_id: call.id.clone(),
        tool: call.name.clone(),
        content_block_id: None,
        is_error: matches!(outcome, Outcome::Failed(_) | Outcome::Skipped(_)),
        redacted: false,
        metadata_truncated,
        schema_validated: false,
        details,
        structured_output,
        attachments,
        diagnostics,
    });
    Ok(())
}

/// Resolve statuses only when one committed occurrence can be proved. Unknown
/// legacy statuses remain absent; ambiguous reused IDs never acquire a guess.
fn tool_error_bindings(c: &Conversation) -> Result<std::collections::BTreeMap<usize, bool>, Error> {
    let mut statuses = std::collections::BTreeMap::new();
    for (index, message) in c.messages.iter().enumerate() {
        let Message::Tool { call_id, .. } = message else {
            continue;
        };
        let candidates: Vec<_> = c
            .tool_results
            .iter()
            .filter(|m| m.call_id == *call_id)
            .collect();
        let block_id = c.content_blocks.get(&index).map(|b| b.message_id);
        let exact: Vec<_> = candidates
            .iter()
            .filter(|m| block_id.is_some() && m.content_block_id == block_id)
            .collect();
        let found = if exact.len() == 1 {
            Some(*exact[0])
        } else if candidates.is_empty() {
            None
        } else {
            return Err(Error::Invalid(
                "ambiguous durable tool result metadata".into(),
            ));
        };
        if let Some(metadata) = found {
            statuses.insert(index + 1, metadata.is_error);
        }
    }
    Ok(statuses)
}

pub(crate) fn local_schema_references(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().all(|(key, value)| {
            if key == "$ref" || key == "$dynamicRef" {
                value
                    .as_str()
                    .is_some_and(|reference| reference.starts_with('#'))
            } else {
                local_schema_references(value)
            }
        }),
        serde_json::Value::Array(values) => values.iter().all(local_schema_references),
        _ => true,
    }
}

fn validate_tool_metadata(filters: &ToolFilters, metadata: &mut ToolResultMetadata) {
    if metadata.schema_validated {
        return;
    }
    metadata.schema_validated = true;
    if metadata.redacted || metadata.metadata_truncated {
        return;
    }
    let Some(schema) = filters.output_schemas.get(&metadata.tool) else {
        return;
    };
    let broken = match &metadata.structured_output {
        None if metadata.is_error => false,
        None => true,
        Some(value) => {
            !local_schema_references(schema)
                || jsonschema::validator_for(schema)
                    .map_or(true, |validator| !validator.is_valid(value))
        }
    };
    if broken {
        metadata.structured_output = None;
        metadata.attachments.clear();
        metadata.diagnostics.push(ToolDiagnostic {
            severity: DiagnosticSeverity::Error,
            code: Some("invalid_structured_output".into()),
            message: "structured output missing or does not match the configured schema".into(),
        });
        if serde_json::to_vec(&(
            &metadata.details,
            &metadata.structured_output,
            &metadata.attachments,
            &metadata.diagnostics,
        ))
        .map_or(true, |bytes| bytes.len() > 65_536)
        {
            metadata.details = None;
            metadata.structured_output = None;
            metadata.attachments.clear();
            metadata.diagnostics = vec![ToolDiagnostic {
                severity: DiagnosticSeverity::Warn,
                code: Some("metadata_truncated".into()),
                message: "tool result metadata exceeded 65536 bytes".into(),
            }];
            metadata.metadata_truncated = true;
        }
    }
}

fn model_tool_content(
    filters: &ToolFilters,
    calls: &[ToolCall],
    index: usize,
    outcome: &Outcome,
) -> String {
    if matches!(outcome, Outcome::Structured { .. })
        && calls
            .get(index)
            .is_some_and(|call| filters.output_schemas.contains_key(&call.name))
    {
        String::new()
    } else {
        outcome.tool_content()
    }
}

fn hide_pending_tool_output(state: &mut Conversation) {
    if let Phase::ToolFinished { content, .. } = &mut state.phase {
        content.clear();
    }
}

fn redact_tool_metadata(metadata: &mut ToolResultMetadata, code: &str, message: &str) {
    metadata.details = None;
    metadata.structured_output = None;
    metadata.attachments.clear();
    metadata.diagnostics.clear();
    metadata.redacted = true;
    metadata.diagnostics.push(ToolDiagnostic {
        severity: DiagnosticSeverity::Info,
        code: Some(code.into()),
        message: message.into(),
    });
}

/// Name-based filter metadata. Deferred names affect model exposure only.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolFilters {
    pub excluded: Vec<String>,
    pub model_excluded: Vec<String>,
    pub deferred: Vec<String>,
    pub model_add: Vec<String>,
    /// Overrides by tool name. Missing names permit both caller kinds.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub callers: std::collections::BTreeMap<String, ToolCallers>,
    /// Optional structured result schemas keyed by registered tool name.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub output_schemas: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Caller kinds permitted to invoke an enabled tool.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallers {
    Both,
    ModelOnly,
    ToolsOnly,
    Neither,
}

impl ToolCallers {
    fn model(self) -> bool {
        matches!(self, Self::Both | Self::ModelOnly)
    }
    fn tools(self) -> bool {
        matches!(self, Self::Both | Self::ToolsOnly)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PendingIndependentSummary {
    id: Uuid,
    submissions: usize,
    writes: usize,
    resets: usize,
    #[serde(default)]
    resets_before: Vec<Uuid>,
}

#[derive(Clone, Debug)]
struct BlockResetProof {
    previous_context: Option<Uuid>,
    previous_resets: Vec<crate::reset::Reset>,
    previous_messages: Vec<Message>,
    previous_blocks: std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks>,
    placed_resets: Vec<crate::reset::Reset>,
    messages: Vec<Message>,
    blocks: std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks>,
    overwritten_blocks: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NestedResume {
    operation_id: Uuid,
    restarts: u32,
    dispatch_identity: DispatchIdentity,
    #[serde(default)]
    explicit_suspend: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_wake: Option<crate::nested::PendingWake>,
}

/// Identity of one execution of a durable tool operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolAttempt {
    pub operation_id: Uuid,
    pub attempt_id: Uuid,
}

fn terminate_tool_round(c: &mut Conversation) -> Result<Phase, Error> {
    let answer = c
        .messages
        .iter()
        .rev()
        .find_map(|message| match message {
            Message::Assistant { content, calls, .. } if !calls.is_empty() => Some(content.clone()),
            _ => None,
        })
        .ok_or_else(|| Error::Invalid("missing tool-calling answer".into()))?;
    finish_answer(c, answer.clone());
    Ok(Phase::AfterRun {
        answer,
        operation_id: Uuid::new_v4(),
    })
}

/// Read-only readiness of a persisted conversation, independent of live workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationWake {
    Ready,
    /// Parked until a future Unix-millisecond deadline, or an external event.
    Parked {
        at: Option<i64>,
    },
    /// Persisted state alone cannot determine whether execution can proceed.
    Unknown,
}

/// Inspect one persisted conversation without executing or changing it.
///
/// This is a best-effort observation, not an atomic reservation: conversation,
/// signal and child reads may race commits, including from another store handle.
/// Recheck when acting on it. It excludes running workers, independent summaries,
/// task scheduling, and recovery or availability of external handlers.
pub fn conversation_wake(
    store: &durable_store::Store,
    id: Uuid,
    now_millis: i64,
) -> Result<ConversationWake, Error> {
    use ConversationWake::{Parked, Ready, Unknown};
    let entry = store.get(&Harness::key(id))?.ok_or(Error::NotFound)?;
    let c: Conversation = serde_json::from_slice(&entry.value)?;
    if c.id != id {
        return Err(Error::Invalid("conversation identity mismatch".into()));
    }
    let tasks = crate::task_store::TaskStore::new(store);
    if c.abort_requested {
        // Abort can require cancellation of owned or nested work; the stored
        // conversation alone does not prove that cleanup has drained.
        return Ok(Unknown);
    }
    let deadline = |at: i64| {
        if now_millis >= at {
            Ready
        } else {
            Parked { at: Some(at) }
        }
    };
    Ok(match &c.phase {
        Phase::Idle => {
            if c.active.is_some()
                || !c.pending_resets.is_empty()
                || c.submissions.iter().any(|s| s.status == Status::Queued)
            {
                Ready
            } else {
                Parked { at: None }
            }
        }
        Phase::Blocked { .. } if c.nested_resume.is_some() => Unknown,
        Phase::FailedIdle | Phase::Blocked { .. } => Parked { at: None },
        Phase::RetryWait { .. } => match c.retry_state.deadline_ms {
            Some(at) if at <= i64::MAX as u64 => deadline(at as i64),
            _ => Unknown,
        },
        Phase::ToolGateSleeping {
            until_millis,
            signals,
            ..
        } => {
            if crate::conversation_signal::Inbox::new(store)
                .prepare_wake(id, signals, *until_millis, now_millis)?
                .is_some()
            {
                Ready
            } else {
                Parked { at: *until_millis }
            }
        }
        Phase::YieldTasksWaiting { children, .. } | Phase::HookTasksWaiting { children, .. } => {
            let mut all_terminal = true;
            for child in children {
                let task = tasks
                    .load(*child)
                    .map_err(|e| Error::Invalid(format!("task load: {e}")))?
                    .ok_or_else(|| Error::Invalid(format!("missing hook child: {child}")))?;
                if task.task.owner() != &crate::task::TaskOwner::Conversation(id) {
                    return Err(Error::Invalid("hook child owner mismatch".into()));
                }
                all_terminal &= task.task.is_terminal();
            }
            if all_terminal {
                Ready
            } else {
                Parked { at: None }
            }
        }
        Phase::TaskPending {
            task, operation_id, ..
        } => {
            let child = tasks
                .load(*task)
                .map_err(|e| Error::Invalid(format!("task load: {e}")))?
                .ok_or_else(|| Error::Invalid("task-backed tool task missing".into()))?;
            if task != operation_id
                || child.task.owner() != &crate::task::TaskOwner::Conversation(id)
            {
                return Err(Error::Invalid("task-backed tool owner mismatch".into()));
            }
            // Adapter identity must still be checked by the executor.
            if child.task.is_terminal() {
                Unknown
            } else {
                Parked { at: None }
            }
        }
        Phase::ModelRunning { .. }
        | Phase::CompactionRunning { .. }
        | Phase::ToolRunning { .. }
        | Phase::Pending { .. } => Unknown,
        Phase::ModelReady { .. }
        | Phase::AbandonedModel { .. }
        | Phase::FailedModel { .. }
        | Phase::CompactionHook { .. }
        | Phase::CompactionReady { .. }
        | Phase::CompactionAbandoned { .. }
        | Phase::CompactionAccounting { .. }
        | Phase::CompactionPlacement { .. }
        | Phase::Accounting { .. }
        | Phase::Boundary { .. }
        | Phase::OnYield { .. }
        | Phase::YieldDecision { .. }
        | Phase::AfterTools { .. }
        | Phase::AfterRun { .. }
        | Phase::Tools { .. }
        | Phase::ToolPreparing { .. }
        | Phase::ToolFinished { .. } => Ready,
    })
}

impl Conversation {
    /// Whether a replay-safe keyed tool explicitly suspended on pending
    /// nested jobs. An abort keeps the suspension visible until reconciled.
    pub fn nested_suspended(&self) -> bool {
        self.nested_resume
            .as_ref()
            .is_some_and(|resume| resume.explicit_suspend)
            && matches!(self.phase, Phase::Blocked { .. })
    }
    pub(crate) fn active_tool_attempt(&self) -> Option<ToolAttempt> {
        if self.abort_requested
            || !matches!(
                self.phase,
                Phase::ToolRunning {
                    replay_ready: false,
                    ..
                }
            )
        {
            return None;
        }
        self.tool_attempt()
    }
    pub(crate) fn admit_initial_input(&mut self, id: Uuid, content: String) {
        self.submissions.push(Submission {
            kind: SubmissionKind::Input,
            id,
            request_id: None,
            content: content.clone(),
            image_notes: Vec::new(),
            input_blocks: None,
            status: Status::Running,
        });
        self.input_modes.insert(id, InputMode::FollowUp);
        self.active = Some(id);
        self.messages.push(Message::User { content });
        self.phase = Phase::ModelReady { turns: 0 };
    }
    pub(crate) fn is_working(&self) -> bool {
        self.active.is_some()
            || !self.pending_resets.is_empty()
            || !matches!(self.phase, Phase::Idle | Phase::FailedIdle)
            || self
                .submissions
                .iter()
                .any(|s| matches!(s.status, Status::Queued | Status::Running))
    }
    /// The most recent attempt for the currently running, pending, blocked or
    /// finishing operation. Legacy records may have no known attempt identity.
    pub fn tool_attempt(&self) -> Option<ToolAttempt> {
        let operation_id = match &self.phase {
            Phase::ToolRunning { operation_id, .. }
            | Phase::Pending { operation_id, .. }
            | Phase::TaskPending { operation_id, .. }
            | Phase::Blocked { operation_id, .. }
            | Phase::ToolFinished { operation_id, .. } => *operation_id,
            _ => return None,
        };
        self.tool_attempt
            .filter(|attempt| attempt.operation_id == operation_id)
    }
    /// Identity of the answer settling a submission. Older persisted answers
    /// predate answer IDs and return None.
    pub fn answer_id(&self, submission: Uuid) -> Option<Uuid> {
        self.answer_ids.get(&submission).copied()
    }
}

/// A committed conversation snapshot. `state.messages` is empty; use `entries`
/// and `older` to read the captured transcript, newest first.
pub struct ConversationView {
    pub state: Conversation,
    pub live: Option<crate::progress::State>,
    /// Latest committed partial for the current tool attempt.
    pub tool_progress: Option<crate::tool_progress::State>,
    pub entries: Vec<Message>,
    pub older: Option<ViewCursor>,
}

/// First terminal reason of a pull-based view watch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ViewWatchEnd {
    Stopped,
    Aborted,
    Ended,
    Failed(String),
}

/// A pull-based watch of a bounded conversation view. `initial` is captured at
/// attachment. Later calls to `next` return the latest changed snapshot; commits
/// between polls may be coalesced. Dropping a pending `next` is cancellation-safe.
/// Call `close` (or drop the watch) to release its store ownership.
pub struct ViewWatch {
    pub initial: ConversationView,
    end: Option<ViewWatchEnd>,
    reader: Option<Harness>,
    id: Uuid,
    limit: std::num::NonZeroUsize,
    interval: Duration,
    last: serde_json::Value,
}

impl ViewWatch {
    /// Wait for a changed snapshot, or return `None` after close. A slow reader
    /// retains no queued frames and sees the latest snapshot on its next poll.
    pub async fn next(&mut self) -> Result<Option<ConversationView>, Error> {
        loop {
            let Some(reader) = self.reader.as_ref() else {
                return Ok(None);
            };
            let result = reader
                .view(self.id, self.limit)
                .and_then(|view| Ok((view_fingerprint(&view)?, view)));
            let (fingerprint, view) = match result {
                Ok(value) => value,
                Err(error) => {
                    self.end = Some(ViewWatchEnd::Failed(error.to_string()));
                    self.reader = None;
                    return Err(error);
                }
            };
            if fingerprint != self.last {
                self.last = fingerprint;
                return Ok(Some(view));
            }
            tokio::time::sleep(self.interval).await;
        }
    }

    /// Wait for a change or end the watch when the supplied token is cancelled.
    /// Cancelling by dropping this future alone leaves the watch open.
    pub async fn next_with_cancel(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Option<ConversationView>, Error> {
        if self.end.is_some() {
            return Ok(None);
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                self.abort();
                Ok(None)
            }
            result = self.next() => result,
        }
    }

    /// The first terminal reason, if this watch has ended.
    pub fn end_reason(&self) -> Option<&ViewWatchEnd> {
        self.end.as_ref()
    }

    /// End with an abort reason and release store ownership. Idempotent.
    pub fn abort(&mut self) {
        if self.end.is_none() {
            self.end = Some(ViewWatchEnd::Aborted);
            self.reader = None;
        }
    }

    /// Stop watching and release the reader's store ownership. Idempotent.
    pub fn close(&mut self) {
        if self.end.is_none() {
            self.end = Some(ViewWatchEnd::Stopped);
            self.reader = None;
        }
    }
}

/// Exclusive position in one conversation's committed snapshot stream.
/// Persist with the same database; it is not portable between stores.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CommittedViewCursor {
    pub conversation: Uuid,
    pub position: u64,
}

/// One complete committed conversation observation, expressed as a root
/// replacement. This is a historical observation, not the live bounded view.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedViewFrame {
    pub cursor: CommittedViewCursor,
    pub value: serde_json::Value,
    pub ops: Vec<serde_json::Value>,
    /// Conversation observation frames do not represent document retirement.
    pub retired: bool,
}

/// Pull one validated conversation commit at a time; no committed observations
/// are coalesced. Stop or abort releases its store owner.
pub struct CommittedViewWatch {
    pub initial: CommittedViewFrame,
    reader: Option<Harness>,
    cursor: CommittedViewCursor,
    interval: Duration,
    end: Option<ViewWatchEnd>,
}
impl CommittedViewWatch {
    pub fn cursor(&self) -> &CommittedViewCursor {
        &self.cursor
    }
    pub fn end_reason(&self) -> Option<&ViewWatchEnd> {
        self.end.as_ref()
    }
    pub fn close(&mut self) {
        self.end_with(ViewWatchEnd::Stopped);
    }
    pub fn abort(&mut self) {
        self.end_with(ViewWatchEnd::Aborted);
    }
    fn end_with(&mut self, reason: ViewWatchEnd) {
        if self.end.is_none() {
            self.end = Some(reason);
            self.reader = None;
        }
    }
    /// A dropped wait does not advance the cursor or end the watch.
    pub async fn next(&mut self) -> Result<Option<CommittedViewFrame>, Error> {
        loop {
            let Some(reader) = self.reader.as_ref() else {
                return Ok(None);
            };
            let result = reader.next_committed_view(&self.cursor);
            match result {
                Ok(Some(frame)) => {
                    self.cursor = frame.cursor.clone();
                    return Ok(Some(frame));
                }
                Ok(None) => tokio::time::sleep(self.interval).await,
                Err(error) => {
                    self.end_with(ViewWatchEnd::Failed(error.to_string()));
                    return Err(error);
                }
            }
        }
    }
    pub async fn next_with_cancel(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Option<CommittedViewFrame>, Error> {
        if self.end.is_some() {
            return Ok(None);
        }
        tokio::select! { biased;
            _ = cancel.cancelled() => { self.abort(); Ok(None) }
            frame = self.next() => frame,
        }
    }
}

fn view_fingerprint(view: &ConversationView) -> Result<serde_json::Value, Error> {
    Ok(serde_json::json!({
        "state": serde_json::to_value(&view.state)?,
        "live": serde_json::to_value(&view.live)?,
        "tool_progress": serde_json::to_value(&view.tool_progress)?,
    }))
}

/// Opaque position in one immutable conversation transcript snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewCursor {
    cursor: crate::transcript::Cursor,
    remaining: Option<usize>,
}

pub struct ViewPage {
    /// Newest first, including within a transcript batch.
    pub entries: Vec<Message>,
    pub older: Option<ViewCursor>,
}

/// A message and its position in a captured transcript, inclusive of that message.
/// This identifies a transcript position only; it does not establish a safe fork.
pub struct PositionedMessage {
    pub message: Message,
    pub position: ViewCursor,
    /// Identity and atomic commit association, absent for legacy batches.
    pub history: Option<MessageHistory>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageHistory {
    pub id: Uuid,
    pub commit: u64,
    /// Conversation whose observation stream owns this commit.
    pub origin: Uuid,
    /// Only the final message in a batch has a state at its exact boundary.
    pub commit_boundary: bool,
}

pub struct PositionedViewPage {
    /// Newest first, including within a transcript batch.
    pub entries: Vec<PositionedMessage>,
    pub older: Option<ViewCursor>,
}

/// A durable, host-readable batch of extension errors for one execution phase.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionReportBatch {
    pub conversation_id: Uuid,
    pub operation_id: Uuid,
    pub phase: String,
    pub reports: Vec<crate::extension::Report>,
}

struct RuntimeStreamOutput {
    owner: std::sync::Weak<OwnedStore>,
    source: std::sync::Weak<MergedSourceLease>,
    conversation: Uuid,
    attempt: ToolAttempt,
    storage_failed: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl crate::keyed_tool::StreamSink for RuntimeStreamOutput {
    async fn record(&self, text: &str) -> pi_rs_core::ExtensionResult<()> {
        let _lease = self
            .source
            .upgrade()
            .ok_or_else(|| "stream source closed".to_owned())?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| "stream owner closed".to_string())?;
        let store = owner.store();
        loop {
            let result = (|| -> Result<bool, Error> {
                let key = Harness::key(self.conversation);
                let Some(entry) = store.get(&key)? else {
                    return Err(Error::Invalid("stream attempt missing".into()));
                };
                let state: Conversation = serde_json::from_slice(&entry.value)?;
                if state.abort_requested
                    || state.tool_attempt() != Some(self.attempt)
                    || !matches!(
                        state.phase,
                        Phase::ToolRunning {
                            replay_ready: false,
                            ..
                        }
                    )
                {
                    return Err(Error::Invalid("stream attempt is no longer active".into()));
                }
                let output_key = crate::streamed_output::key(self.attempt.attempt_id);
                let previous = store.get(&output_key)?;
                let checks = [
                    Check {
                        key,
                        expected: Expected::Version(entry.version),
                    },
                    Check {
                        key: output_key.clone(),
                        expected: previous
                            .as_ref()
                            .map_or(Expected::Missing, |v| Expected::Version(v.version)),
                    },
                ];
                let mutations = [Mutation::Put {
                    key: output_key,
                    value: text.as_bytes().to_vec(),
                }];
                match store.commit(&checks, &mutations) {
                    Ok(()) => Ok(true),
                    Err(durable_store::Error::Conflict { .. }) => Ok(false),
                    Err(error) => {
                        self.storage_failed.store(true, Ordering::Release);
                        Err(error.into())
                    }
                }
            })();
            match result {
                Ok(true) => return Ok(()),
                Ok(false) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}

struct RuntimeToolProgress {
    owner: Arc<OwnedStore>,
    source: std::sync::Weak<MergedSourceLease>,
    conversation: Uuid,
    attempt: ToolAttempt,
    storage_failed: Arc<AtomicBool>,
}

fn diagnostic_key(conversation: Uuid, operation: Uuid) -> Vec<u8> {
    format!("runtime/tool-diagnostics/{conversation}/{operation}").into_bytes()
}
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct DiagnosticJournal {
    diagnostics: Vec<ToolDiagnostic>,
    #[serde(default)]
    keys: std::collections::BTreeMap<String, usize>,
}
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum DiagnosticCheckpoint {
    Legacy(Vec<ToolDiagnostic>),
    Journal(DiagnosticJournal),
}
impl DiagnosticCheckpoint {
    fn journal(self) -> DiagnosticJournal {
        match self {
            Self::Legacy(diagnostics) => DiagnosticJournal {
                diagnostics,
                keys: Default::default(),
            },
            Self::Journal(journal) => journal,
        }
    }
}
#[cfg(test)]
mod diagnostic_identity_tests {
    use super::*;

    #[test]
    fn legacy_records_are_retained_without_inventing_event_ids() {
        let diagnostic = ToolDiagnostic {
            severity: DiagnosticSeverity::Warn,
            message: "same".into(),
            code: None,
        };
        let bytes = serde_json::to_vec(&vec![diagnostic.clone()]).unwrap();
        let mut journal = serde_json::from_slice::<DiagnosticCheckpoint>(&bytes)
            .unwrap()
            .journal();
        insert_diagnostic(
            &mut journal.diagnostics,
            &mut journal.keys,
            Some("new"),
            diagnostic.clone(),
        )
        .unwrap();
        insert_diagnostic(
            &mut journal.diagnostics,
            &mut journal.keys,
            Some("new"),
            diagnostic.clone(),
        )
        .unwrap();
        assert_eq!(journal.diagnostics, vec![diagnostic.clone(), diagnostic]);
        let encoded = serde_json::to_vec(&journal).unwrap();
        let reopened = serde_json::from_slice::<DiagnosticCheckpoint>(&encoded)
            .unwrap()
            .journal();
        assert_eq!(reopened.keys.get("new"), Some(&1));
        assert_eq!(reopened.diagnostics.len(), 2);
    }
}
pub(crate) fn insert_diagnostic(
    diagnostics: &mut Vec<ToolDiagnostic>,
    keys: &mut std::collections::BTreeMap<String, usize>,
    key: Option<&str>,
    diagnostic: ToolDiagnostic,
) -> Result<(), String> {
    if let Some(key) = key
        && let Some(index) = keys.get(key)
    {
        return match diagnostics.get(*index) {
            Some(existing) if existing == &diagnostic => Ok(()),
            Some(_) => Err("tool diagnostic key already has different content".into()),
            None => Err("invalid tool diagnostic key index".into()),
        };
    }
    if diagnostics.len() >= 32 {
        return Err("too many tool diagnostics".into());
    }
    if let Some(key) = key {
        keys.insert(key.to_owned(), diagnostics.len());
    }
    diagnostics.push(diagnostic);
    if serde_json::to_vec(&(&diagnostics, &keys))
        .map_err(|e| e.to_string())?
        .len()
        > 65_536
    {
        return Err("tool diagnostics exceed size limit".into());
    }
    Ok(())
}
fn read_tool_diagnostics(
    owner: &OwnedStore,
    conversation: Uuid,
    operation: Uuid,
) -> Result<Vec<ToolDiagnostic>, Error> {
    owner
        .store()
        .get(&diagnostic_key(conversation, operation))?
        .map(|entry| {
            serde_json::from_slice::<DiagnosticCheckpoint>(&entry.value)
                .map(|v| v.journal().diagnostics)
                .map_err(Error::from)
        })
        .transpose()
        .map(|value| value.unwrap_or_default())
}
struct RuntimeDiagnostic {
    owner: Arc<OwnedStore>,
    conversation: Uuid,
    attempt: ToolAttempt,
    cancel: CancellationToken,
    storage_failed: Arc<AtomicBool>,
    closed: std::sync::Mutex<bool>,
}
impl RuntimeDiagnostic {
    fn close(&self) {
        *self.closed.lock().unwrap_or_else(|e| e.into_inner()) = true;
    }
}
impl crate::keyed_tool::DiagnosticSink for RuntimeDiagnostic {
    fn record(
        &self,
        diagnostic_id: Option<&str>,
        diagnostic: ToolDiagnostic,
    ) -> Result<(), String> {
        let closed = self
            .closed
            .lock()
            .map_err(|_| "tool diagnostic lock poisoned")?;
        if *closed {
            return Err("tool attempt is no longer active".into());
        }
        let store = self.owner.store();
        loop {
            if self.cancel.is_cancelled() {
                return Err("tool attempt cancelled".into());
            }
            let result = (|| -> Result<bool, Error> {
                let key = Harness::key(self.conversation);
                let entry = store.get(&key)?.ok_or(Error::NotFound)?;
                let state: Conversation = serde_json::from_slice(&entry.value)?;
                if state.abort_requested
                    || state.tool_attempt() != Some(self.attempt)
                    || !matches!(
                        state.phase,
                        Phase::ToolRunning {
                            replay_ready: false,
                            ..
                        }
                    )
                {
                    return Err(Error::Invalid("tool attempt is no longer active".into()));
                }
                let diag_key = diagnostic_key(self.conversation, self.attempt.operation_id);
                let prior = store.get(&diag_key)?;
                let mut journal = prior
                    .as_ref()
                    .map(|e| {
                        serde_json::from_slice::<DiagnosticCheckpoint>(&e.value)
                            .map(DiagnosticCheckpoint::journal)
                    })
                    .transpose()?
                    .unwrap_or_default();
                insert_diagnostic(
                    &mut journal.diagnostics,
                    &mut journal.keys,
                    diagnostic_id,
                    diagnostic.clone(),
                )
                .map_err(Error::Invalid)?;
                let value = serde_json::to_vec(&journal)?;
                match store.commit(
                    &[
                        Check {
                            key,
                            expected: Expected::Version(entry.version),
                        },
                        Check {
                            key: diag_key.clone(),
                            expected: prior
                                .as_ref()
                                .map_or(Expected::Missing, |e| Expected::Version(e.version)),
                        },
                    ],
                    &[Mutation::Put {
                        key: diag_key,
                        value,
                    }],
                ) {
                    Ok(()) => Ok(true),
                    Err(durable_store::Error::Conflict { .. }) => Ok(false),
                    Err(error) => Err(error.into()),
                }
            })();
            match result {
                Ok(true) => return Ok(()),
                Ok(false) => continue,
                Err(error) => {
                    if matches!(error, Error::Storage(_)) {
                        self.storage_failed.store(true, Ordering::Release);
                    }
                    return Err(error.to_string());
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::tool_progress::Sink for RuntimeToolProgress {
    async fn update(&self, partial: serde_json::Value) -> pi_rs_core::ExtensionResult<()> {
        let Some(_lease) = self.source.upgrade() else {
            return Ok(());
        };
        let store = self.owner.store();
        loop {
            let result = (|| -> Result<bool, Error> {
                let key = Harness::key(self.conversation);
                let Some(entry) = store.get(&key)? else {
                    return Ok(true);
                };
                let state: Conversation = serde_json::from_slice(&entry.value)?;
                if state.abort_requested
                    || state.tool_attempt() != Some(self.attempt)
                    || !matches!(
                        state.phase,
                        Phase::ToolRunning {
                            replay_ready: false,
                            ..
                        }
                    )
                {
                    return Ok(true);
                }
                let mut snapshot =
                    crate::tool_progress::Snapshot::load(store, self.attempt.attempt_id)?;
                if !matches!(snapshot.state(), crate::tool_progress::State::Running(_)) {
                    return Ok(true);
                }
                snapshot.update(partial.clone())?;
                let mut batch = snapshot.prepare_checkpoint()?;
                batch.checks.push(Check {
                    key,
                    expected: Expected::Version(entry.version),
                });
                let event =
                    crate::observer::Observer::tool_progress(store, self.attempt.attempt_id)
                        .prepare_append(&serde_json::json!({
                            "conversation_id": self.conversation,
                            "attempt": self.attempt,
                            "state": snapshot.state(),
                            "partial_is_null": matches!(snapshot.state(), crate::tool_progress::State::Running(Some(serde_json::Value::Null))),
                        }))?;
                batch.checks.extend(event.checks);
                batch.mutations.extend(event.mutations);
                let history = prepare_progress_history(
                    store,
                    self.conversation,
                    vec![ProgressHistoryChange::Tool {
                        attempt: self.attempt,
                        state: snapshot.state().clone(),
                        partial_is_null: matches!(
                            snapshot.state(),
                            crate::tool_progress::State::Running(Some(serde_json::Value::Null))
                        ),
                    }],
                )?;
                batch.checks.extend(history.checks);
                batch.mutations.extend(history.mutations);
                match store.commit(&batch.checks, &batch.mutations) {
                    Ok(()) => Ok(true),
                    Err(durable_store::Error::Conflict { .. }) => Ok(false),
                    Err(error) => {
                        self.storage_failed.store(true, Ordering::Release);
                        Err(error.into())
                    }
                }
            })();
            match result {
                Ok(true) => return Ok(()),
                Ok(false) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}

/// Versioned exclusive cursor; the boundary returned by `progress_history_from_now`
/// deliberately excludes any overwritten progress before attachment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProgressHistoryCursor {
    pub version: u32,
    pub conversation: Uuid,
    pub position: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProgressHistoryChange {
    Generation {
        request_id: Uuid,
        state: Box<crate::progress::State>,
    },
    Tool {
        attempt: ToolAttempt,
        state: crate::tool_progress::State,
        partial_is_null: bool,
    },
}
fn same_progress_identity(a: &ProgressHistoryChange, b: &ProgressHistoryChange) -> bool {
    match (a, b) {
        (
            ProgressHistoryChange::Generation { request_id: a, .. },
            ProgressHistoryChange::Generation { request_id: b, .. },
        ) => a == b,
        (
            ProgressHistoryChange::Tool { attempt: a, .. },
            ProgressHistoryChange::Tool { attempt: b, .. },
        ) => a == b,
        _ => false,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct ProgressHistoryRecord {
    version: u32,
    conversation: Uuid,
    changes: Vec<ProgressHistoryChange>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ProgressHistoryFrame {
    pub cursor: ProgressHistoryCursor,
    /// Database-local store commit containing this entire record.
    pub commit_sequence: u64,
    pub changes: Vec<ProgressHistoryChange>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ProgressHistoryPage {
    pub records: Vec<ProgressHistoryFrame>,
    pub next: ProgressHistoryCursor,
    pub has_more: bool,
}
/// A versioned, forward-only boundary for the merged observation/progress stream.
/// Persist this opaque value with the same database and conversation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MergedProgressCursor {
    version: u32,
    conversation: Uuid,
    after_sequence: u64,
    observation_position: u64,
    progress_position: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    document_position: Option<u64>,
}
/// One conversation-scoped document's final state in a transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct MergedDocumentChange {
    pub address: crate::document::Address,
    pub document: Option<crate::document::Document>,
}
/// Records written in one store transaction. No ordering is implied within a frame.
#[derive(Clone, Debug, PartialEq)]
pub struct MergedProgressFrame {
    pub commit_sequence: u64,
    pub observation: Option<CommittedViewFrame>,
    pub progress: Option<ProgressHistoryFrame>,
    /// Final document values, with `None` representing deletion.
    pub documents: Vec<MergedDocumentChange>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct MergedProgressPage {
    pub frames: Vec<MergedProgressFrame>,
    pub next: MergedProgressCursor,
    pub has_more: bool,
}

fn merged_source_page(
    reader: &Harness,
    source: &MergedSourceSignal,
    cursor: &MergedProgressCursor,
    limit: std::num::NonZeroUsize,
) -> Result<(MergedProgressPage, bool), Error> {
    if source.closed.is_cancelled() {
        return Ok((
            reader.merged_progress_through(cursor, limit, source.boundary()?)?,
            true,
        ));
    }
    let page = reader.merged_progress(cursor, limit)?;
    // Serialize this decision with boundary capture. If closure started while
    // the read ran, wait for its boundary and re-read before advancing.
    if let Some(boundary) = source.boundary_if_closed()? {
        return Ok((
            reader.merged_progress_through(cursor, limit, boundary)?,
            true,
        ));
    }
    Ok((page, false))
}

// Source-capable harnesses and workers retain this lease. Read-only watch
// workers retain only the signal, so they cannot keep the source alive.
struct MergedSourceLease {
    owner: std::sync::Weak<OwnedStore>,
    signal: Arc<MergedSourceSignal>,
}
struct MergedSourceSignal {
    closed: CancellationToken,
    boundary: std::sync::Mutex<Option<Result<u64, String>>>,
}
impl Drop for MergedSourceLease {
    fn drop(&mut self) {
        if let Ok(mut stored) = self.signal.boundary.lock() {
            let boundary = self
                .owner
                .upgrade()
                .ok_or_else(|| "source store already closed".to_owned())
                .and_then(|owner| {
                    owner
                        .store()
                        .commit_sequence()
                        .map_err(|error| error.to_string())
                });
            *stored = Some(boundary);
        }
        self.signal.closed.cancel();
    }
}
impl MergedSourceSignal {
    fn boundary_if_closed(&self) -> Result<Option<u64>, Error> {
        let stored = self.boundary.lock().map_err(|_| Error::Failed)?;
        match stored.as_ref() {
            Some(Ok(sequence)) => Ok(Some(*sequence)),
            Some(Err(error)) => Err(Error::Invalid(format!("source close boundary: {error}"))),
            None => Ok(None),
        }
    }
    fn boundary(&self) -> Result<u64, Error> {
        self.boundary_if_closed()?
            .ok_or_else(|| Error::Invalid("missing source close boundary".into()))
    }
}

/// A document change and the proven predecessor used for its operations.
#[derive(Clone, Debug, PartialEq)]
pub struct MergedDocumentOperations {
    pub address: crate::document::Address,
    pub previous: Option<crate::document::Document>,
    pub ops: Vec<serde_json::Value>,
    pub retired: bool,
    /// True when this is the verified original batch from an opt-in write.
    pub actual: bool,
}
/// A progress operation and its proven predecessor.
#[derive(Clone, Debug, PartialEq)]
pub struct MergedProgressOperations {
    pub previous: Option<ProgressHistoryChange>,
    pub ops: Vec<serde_json::Value>,
}
/// One complete merged transaction with derived Chord operations.
#[derive(Clone, Debug, PartialEq)]
pub struct IncrementalMergedFrame {
    pub frame: MergedProgressFrame,
    /// Present only when a predecessor was verified; otherwise ops replace root.
    pub observation_previous: Option<serde_json::Value>,
    /// Aligned with progress changes and applied to each serialized entire change.
    pub progress: Vec<MergedProgressOperations>,
    pub documents: Vec<MergedDocumentOperations>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct IncrementalMergedPage {
    pub frames: Vec<IncrementalMergedFrame>,
    pub next: MergedProgressCursor,
    pub has_more: bool,
}

/// A resumable pull watch over complete merged commit frames. The cursor
/// advances only when a page is returned; persist it after processing the page.
pub struct MergedProgressWatch {
    reader: Option<Harness>,
    source: Arc<MergedSourceSignal>,
    cursor: MergedProgressCursor,
    limit: std::num::NonZeroUsize,
    interval: Duration,
    end: Option<ViewWatchEnd>,
}
impl MergedProgressWatch {
    /// Wait for the last source-capable lease to close and return its store
    /// commit boundary. The watch ends only after pages through it are drained.
    pub async fn source_closed(&self) -> Result<u64, Error> {
        self.source.closed.cancelled().await;
        self.source.boundary()
    }
    pub fn cursor(&self) -> &MergedProgressCursor {
        &self.cursor
    }
    pub fn end_reason(&self) -> Option<&ViewWatchEnd> {
        self.end.as_ref()
    }
    pub fn close(&mut self) {
        self.end_with(ViewWatchEnd::Stopped);
    }
    pub fn abort(&mut self) {
        self.end_with(ViewWatchEnd::Aborted);
    }
    fn end_with(&mut self, reason: ViewWatchEnd) {
        if self.end.is_none() {
            self.end = Some(reason);
            self.reader = None;
        }
    }
    /// Dropping a pending wait neither ends the watch nor advances its cursor.
    pub async fn next(&mut self) -> Result<Option<MergedProgressPage>, Error> {
        loop {
            let Some(reader) = self.reader.as_ref() else {
                return Ok(None);
            };
            match merged_source_page(reader, &self.source, &self.cursor, self.limit) {
                Ok((page, _)) if !page.frames.is_empty() => {
                    self.cursor = page.next.clone();
                    return Ok(Some(page));
                }
                Ok((_, true)) => {
                    self.end_with(ViewWatchEnd::Ended);
                    return Ok(None);
                }
                Ok((_, false)) => {
                    tokio::select! { _ = self.source.closed.cancelled() => {}, _ = tokio::time::sleep(self.interval) => {} }
                }
                Err(error) => {
                    self.end_with(ViewWatchEnd::Failed(error.to_string()));
                    return Err(error);
                }
            }
        }
    }
    /// Like `next`, but derive operations only from proven predecessor states.
    /// A failed conversion restores the input cursor and ends the watch.
    pub async fn next_operations(&mut self) -> Result<Option<IncrementalMergedPage>, Error> {
        let before = self.cursor.clone();
        let Some(page) = self.next().await? else {
            return Ok(None);
        };
        let result = self
            .reader
            .as_ref()
            .expect("reader remains open for a returned page")
            .incremental_merged_page(page);
        match result {
            Ok(page) => Ok(Some(page)),
            Err(error) => {
                self.cursor = before;
                self.end_with(ViewWatchEnd::Failed(error.to_string()));
                Err(error)
            }
        }
    }

    /// An explicitly cancelled token ends the watch as aborted.
    pub async fn next_with_cancel(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Option<MergedProgressPage>, Error> {
        if self.end.is_some() {
            return Ok(None);
        }
        tokio::select! { biased;
            _ = cancel.cancelled() => { self.abort(); Ok(None) }
            page = self.next() => page,
        }
    }
}

/// Terminal result of a merged listener. Source-lease closure drains through
/// its captured boundary; the optional host source-close signal ends directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergedListenerEnd {
    Stopped,
    Aborted,
    Ended,
    ListenerError(String),
    Failed(String),
}

/// Independent, first-observed termination signals for a listener.
#[derive(Clone, Default)]
pub struct MergedListenerSignals {
    pub stop: CancellationToken,
    pub abort: CancellationToken,
    pub source_closed: CancellationToken,
}

/// Serialized callback delivery with at-least-once replay from the last
/// acknowledged page. This does not spawn a worker or buffer pages.
pub struct MergedProgressListener {
    reader: Option<Harness>,
    source: Arc<MergedSourceSignal>,
    cursor: MergedProgressCursor,
    limit: std::num::NonZeroUsize,
    interval: Duration,
    end: Option<MergedListenerEnd>,
}
impl MergedProgressWatch {
    /// Transfer this watch to a single callback driver. An ended watch cannot
    /// be restarted; it may instead be reattached from its saved cursor.
    pub fn into_listener(mut self) -> Result<MergedProgressListener, Error> {
        let reader = self
            .reader
            .take()
            .ok_or_else(|| Error::Invalid("merged watch already ended".into()))?;
        Ok(MergedProgressListener {
            reader: Some(reader),
            source: self.source,
            cursor: self.cursor,
            limit: self.limit,
            interval: self.interval,
            end: None,
        })
    }
}
impl MergedProgressListener {
    /// Wait for source-lease closure without acknowledging or discarding pages.
    pub async fn source_closed(&self) -> Result<u64, Error> {
        self.source.closed.cancelled().await;
        self.source.boundary()
    }
    pub fn cursor(&self) -> &MergedProgressCursor {
        &self.cursor
    }
    pub fn end_reason(&self) -> Option<&MergedListenerEnd> {
        self.end.as_ref()
    }
    fn finish(&mut self, end: MergedListenerEnd) -> MergedListenerEnd {
        if self.end.is_none() {
            self.end = Some(end);
            self.reader = None;
        }
        self.end.as_ref().expect("end recorded").clone()
    }
    /// Run one listener at a time. The callback accepts a whole page; an error,
    /// panic, or dropped run leaves its cursor unchanged. Signals are checked
    /// before delivery; an already running callback is awaited so it can be
    /// acknowledged safely. The callback must eventually return.
    pub async fn run<F, Fut>(
        &mut self,
        signals: &MergedListenerSignals,
        mut listener: F,
    ) -> MergedListenerEnd
    where
        F: FnMut(MergedProgressPage) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        if let Some(end) = &self.end {
            return end.clone();
        }
        loop {
            if signals.stop.is_cancelled() {
                return self.finish(MergedListenerEnd::Stopped);
            }
            if signals.abort.is_cancelled() {
                return self.finish(MergedListenerEnd::Aborted);
            }
            if signals.source_closed.is_cancelled() {
                return self.finish(MergedListenerEnd::Ended);
            }
            let (page, source_ended) = match merged_source_page(
                self.reader.as_ref().expect("open listener"),
                &self.source,
                &self.cursor,
                self.limit,
            ) {
                Ok(result) => result,
                Err(error) => return self.finish(MergedListenerEnd::Failed(error.to_string())),
            };
            if page.frames.is_empty() && source_ended {
                return self.finish(MergedListenerEnd::Ended);
            }
            if page.frames.is_empty() {
                tokio::select! { biased;
                    _ = self.source.closed.cancelled() => continue,
                    _ = signals.stop.cancelled() => return self.finish(MergedListenerEnd::Stopped),
                    _ = signals.abort.cancelled() => return self.finish(MergedListenerEnd::Aborted),
                    _ = signals.source_closed.cancelled() => return self.finish(MergedListenerEnd::Ended),
                    _ = tokio::time::sleep(self.interval) => {},
                }
                continue;
            }
            // A synchronous storage read may have overlapped termination.
            if signals.stop.is_cancelled() {
                return self.finish(MergedListenerEnd::Stopped);
            }
            if signals.abort.is_cancelled() {
                return self.finish(MergedListenerEnd::Aborted);
            }
            if signals.source_closed.is_cancelled() {
                return self.finish(MergedListenerEnd::Ended);
            }
            let callback = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener(page.clone())
            })) {
                Ok(callback) => callback,
                Err(payload) => {
                    crate::extension::discard_panic_payload(payload);
                    return self
                        .finish(MergedListenerEnd::ListenerError("listener panicked".into()));
                }
            };
            let callback = std::panic::AssertUnwindSafe(callback).catch_unwind();
            tokio::pin!(callback);
            let (result, signalled) = tokio::select! { biased;
                result = &mut callback => (result, None),
                _ = signals.stop.cancelled() => (callback.await, Some(MergedListenerEnd::Stopped)),
                _ = signals.abort.cancelled() => (callback.await, Some(MergedListenerEnd::Aborted)),
                _ = signals.source_closed.cancelled() => (callback.await, Some(MergedListenerEnd::Ended)),
            };
            let failure = match result {
                Ok(Ok(())) => {
                    self.cursor = page.next;
                    None
                }
                Ok(Err(error)) => Some(MergedListenerEnd::ListenerError(error)),
                Err(payload) => {
                    crate::extension::discard_panic_payload(payload);
                    Some(MergedListenerEnd::ListenerError("listener panicked".into()))
                }
            };
            if let Some(end) = signalled.or(failure) {
                return self.finish(end);
            }
        }
    }
}

const MAX_PROGRESS_HISTORY_BYTES: usize = 2 * 1024 * 1024 + 8192;
fn prepare_progress_history(
    store: &durable_store::Store,
    conversation: Uuid,
    changes: Vec<ProgressHistoryChange>,
) -> Result<crate::observer::Append, Error> {
    let record = ProgressHistoryRecord {
        version: 1,
        conversation,
        changes,
    };
    let value = serde_json::to_value(record)?;
    if serde_json::to_vec(&value)?.len() > MAX_PROGRESS_HISTORY_BYTES {
        return Err(Error::Invalid(
            "progress history event exceeds size limit".into(),
        ));
    }
    Ok(crate::observer::Observer::progress_history(store, conversation).prepare_append(&value)?)
}

/// Trusted cursor for a finite database-local unified history capture.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UnifiedHistoryCursor {
    conversation: Uuid,
    after: u64,
    through: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnifiedHistoryCommit {
    pub sequence: u64,
    pub snapshots: Vec<crate::observer::Record>,
    /// Full application log at this commit, including inherited fork entries.
    pub applications: Option<Vec<crate::application_entry::ApplicationEntry>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnifiedHistoryPage {
    pub commits: Vec<UnifiedHistoryCommit>,
    pub next: UnifiedHistoryCursor,
    pub has_more: bool,
}

pub struct Harness {
    pub(crate) owner: Arc<OwnedStore>,
    merged_source: Option<Arc<MergedSourceLease>>,
    options: Options,
    tools: HashMap<String, RegisteredTool>,
    provider_adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
    summary_provider: Option<SummaryProviderIdentity>,
    extensions: Arc<std::sync::RwLock<crate::extension::Registry>>,
    failed: bool,
    dirty: Arc<AtomicBool>,
    merge_admissions: bool,
    recovering: bool,
    storage_failed: Arc<AtomicBool>,
    /// Snapshots loaded by this worker, used to distinguish admission-only
    /// conflicts from changes to execution state.
    loaded: std::sync::Mutex<HashMap<Uuid, (u64, Conversation)>>,
    execution_gate: Option<Arc<std::sync::Mutex<()>>>,
    task_driver: Arc<std::sync::Mutex<Option<std::sync::Weak<crate::task_driver::Shared>>>>,
    activity: crate::worker_activity::WorkerActivity,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}
/// A snapshot of all store changes for one or more conversation saves.
/// Prepared checks can become stale; a conflict requires preparing the entire batch again.
pub(crate) struct PreparedConversationSave {
    owner: Arc<OwnedStore>,
    pub(crate) checks: Vec<Check>,
    pub(crate) mutations: Vec<Mutation>,
}

impl PreparedConversationSave {
    /// Reject overlapping keys: store checks observe the pre-commit state, so
    /// independently prepared writes to the same key cannot safely be combined.
    pub(crate) fn extend(&mut self, other: Self) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.owner, &other.owner) {
            return Err(Error::Invalid(
                "prepared saves belong to different stores".into(),
            ));
        }
        let keys: std::collections::HashSet<&[u8]> = self
            .checks
            .iter()
            .map(|c| c.key.as_slice())
            .chain(self.mutations.iter().map(|m| match m {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key.as_slice(),
            }))
            .collect();
        if other
            .checks
            .iter()
            .map(|c| c.key.as_slice())
            .chain(other.mutations.iter().map(|m| match m {
                Mutation::Put { key, .. } | Mutation::Delete { key } => key.as_slice(),
            }))
            .any(|key| keys.contains(key))
        {
            return Err(Error::Invalid(
                "overlapping prepared conversation saves".into(),
            ));
        }
        self.checks.extend(other.checks);
        self.mutations.extend(other.mutations);
        Ok(())
    }
}
/// Prepare an unchanged runtime snapshot and an ordered task-entry proof.
/// The caller composes this batch with the task's ancestry and entry checks.
pub(crate) fn prepare_ordered_task_checkpoint(
    store: &durable_store::Store,
    id: Uuid,
    ancestry_checks: &[Check],
    first: usize,
    checkpoint: usize,
    entries: &[crate::application_entry::ApplicationEntry],
    config: Option<&Config>,
) -> Result<crate::task_store::OwnedBatch, crate::document::Error> {
    use crate::document::Error as DError;
    let invalid = || DError::InvalidApplicationEntry;
    if first >= checkpoint || checkpoint > entries.len() {
        return Err(invalid());
    }
    let key = Harness::key(id);
    let root = store.get(&key)?.ok_or_else(invalid)?;
    if !ancestry_checks
        .iter()
        .any(|check| check.key == key && check.expected == Expected::Version(root.version))
    {
        return Err(durable_store::Error::Conflict { key }.into());
    }
    let seq = store
        .commit_for_version(&key, root.version)?
        .ok_or_else(invalid)?;
    let prefix = format!("runtime/fork/{id}/");
    let (_, old_key) = store
        .runtime_snapshot_at(&key, prefix.as_bytes(), seq)?
        .filter(|(s, _)| *s == seq)
        .ok_or_else(invalid)?;
    let old = store.get(&old_key)?.ok_or_else(invalid)?;
    let mut root_value: serde_json::Value = serde_json::from_slice(&root.value)?;
    let mut snapshot_value: serde_json::Value = serde_json::from_slice(&old.value)?;
    if let Some(config) = config {
        let state: Conversation = serde_json::from_value(root_value.clone())?;
        if state.id != id
            || state.abort_requested
            || state.active.is_some()
            || !matches!(state.phase, Phase::Idle)
            || !state.pending_resets.is_empty()
            || state.active_manual_compaction.is_some()
            || state.tool_attempt.is_some()
            || state
                .submissions
                .iter()
                .any(|s| matches!(s.status, Status::Queued | Status::Running))
            || state
                .application_writes
                .iter()
                .any(|w| !w.placed && !w.withdrawn)
            || snapshot_value.get("config") != root_value.get("config")
        {
            return Err(invalid());
        }
        root_value["config"] = serde_json::to_value(config)?;
        snapshot_value["config"] = serde_json::to_value(config)?;
    }
    let observer = crate::observer::Observer::new(store, id);
    let head = observer.head().map_err(|_| invalid())?;
    if head.0 == 0 {
        return Err(invalid());
    }
    if old_key != format!("{prefix}{:020}", head.0).into_bytes() {
        return Err(durable_store::Error::Conflict { key }.into());
    }
    let old_event = observer
        .read(
            crate::observer::Cursor(head.0 - 1),
            std::num::NonZeroUsize::MIN,
        )
        .map_err(|_| invalid())?;
    let event = old_event
        .records
        .first()
        .filter(|r| r.cursor == head)
        .ok_or_else(invalid)?;
    let append = observer
        .prepare_append(&event.value)
        .map_err(|_| invalid())?;
    if head.0.checked_add(1) != Some(append.cursor.0) {
        return Err(durable_store::Error::Conflict { key }.into());
    }
    let snapshot_key = format!("{prefix}{:020}", append.cursor.0).into_bytes();
    let order_key = crate::application_entry::ordered_cut_key(id, append.cursor.0);
    let proof = crate::application_entry::OrderedCut {
        version: if config.is_some() { 2 } else { 1 },
        previous_runtime_version: root.version,
        first_index: first,
        checkpoint_index: checkpoint,
        entry_ids: entries[first..].iter().map(|e| e.id).collect(),
    };
    let mut checks = append.checks;
    checks.extend([
        Check {
            key: old_key,
            expected: Expected::Version(old.version),
        },
        Check {
            key: snapshot_key.clone(),
            expected: Expected::Missing,
        },
        Check {
            key: order_key.clone(),
            expected: Expected::Missing,
        },
    ]);
    let mut mutations = append.mutations;
    mutations.extend([
        Mutation::Put {
            key,
            value: if config.is_some() {
                serde_json::to_vec(&root_value)?
            } else {
                root.value
            },
        },
        Mutation::Put {
            key: snapshot_key,
            value: if config.is_some() {
                serde_json::to_vec(&snapshot_value)?
            } else {
                old.value
            },
        },
        Mutation::Put {
            key: order_key,
            value: serde_json::to_vec(&proof)?,
        },
    ]);
    Ok(crate::task_store::OwnedBatch { checks, mutations })
}

struct ResumeGuard {
    dirty: Arc<AtomicBool>,
    complete: bool,
}
impl Drop for ResumeGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.dirty.store(true, Ordering::Release);
        }
    }
}
pub(crate) fn validate_allowed_tools(names: &Option<Vec<String>>) -> Result<(), Error> {
    if let Some(names) = names {
        let mut seen = std::collections::HashSet::new();
        if names
            .iter()
            .any(|name| name.is_empty() || !seen.insert(name))
        {
            return Err(Error::Invalid("empty or duplicate selected tool".into()));
        }
    }
    Ok(())
}

fn model_tool_selected(c: &Conversation, name: &str) -> bool {
    caller_allowed(c, name, true)
        && c.allowed_tools
            .as_ref()
            .is_none_or(|names| names.iter().any(|n| n == name))
        && !c.tool_filters.excluded.iter().any(|n| n == name)
        && match &c.model_tools {
            Some(names) => names.iter().any(|n| n == name),
            None => {
                !c.tool_filters.model_excluded.iter().any(|n| n == name)
                    && (!c.tool_filters.deferred.iter().any(|n| n == name)
                        || c.tool_filters.model_add.iter().any(|n| n == name))
            }
        }
}

fn caller_allowed(c: &Conversation, name: &str, model: bool) -> bool {
    c.tool_filters.callers.get(name).is_none_or(|callers| {
        if model {
            callers.model()
        } else {
            callers.tools()
        }
    })
}

pub(crate) fn default_subagent_tools(options: &Options) -> Vec<String> {
    let mut allowed: Vec<_> = options
        .tools
        .iter()
        .map(|t| t.tool.spec().name)
        .filter(|name| name != "subagent")
        .collect();
    allowed.sort();
    allowed.dedup();
    allowed
}

impl Harness {
    /// Read committed extension error batches. Batches remain available after reopening.
    pub fn extension_reports(&self) -> Result<Vec<ExtensionReportBatch>, Error> {
        self.owner
            .store()
            .list(b"runtime/extension-report/")?
            .into_iter()
            .map(|(_, entry)| serde_json::from_slice(&entry.value).map_err(Error::from))
            .collect()
    }

    /// Subscribe to committed extension batches, including batches from older databases.
    pub fn subscribe_extension_reports(&self) -> Result<crate::report::Subscription<'_>, Error> {
        let result = crate::report::Subscription::open(self.owner.store());
        self.poison_on_report_storage_error(&result);
        result
    }

    /// Start an opt-in host-owned asynchronous report listener.
    pub fn dispatch_extension_reports<F, Fut>(
        &self,
        cursor: u64,
        poll: Duration,
        listener: F,
    ) -> Result<crate::report::Dispatcher, Error>
    where
        F: FnMut(crate::report::Delivery) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        let result = crate::report::Dispatcher::start(self.owner.clone(), cursor, poll, listener);
        self.poison_on_report_storage_error(&result);
        result
    }

    fn poison_on_report_storage_error<T>(&self, result: &Result<T, Error>) {
        if matches!(
            result,
            Err(Error::Storage(_)) | Err(Error::Observer(crate::observer::Error::Store(_)))
        ) {
            self.storage_failed.store(true, Ordering::Release);
        }
    }

    fn record_extension_reports(
        &self,
        conversation_id: Uuid,
        operation_id: Uuid,
        phase: &str,
        reports: Vec<crate::extension::Report>,
    ) -> Result<(), Error> {
        if reports.is_empty() {
            return Ok(());
        }
        let key = format!("runtime/extension-report/{conversation_id}/{operation_id}/{phase}")
            .into_bytes();
        let batch = ExtensionReportBatch {
            conversation_id,
            operation_id,
            phase: phase.into(),
            reports,
        };
        let result = crate::report::index(self.owner.store(), key, &batch, false);
        self.poison_on_report_storage_error(&result);
        result
    }

    pub(crate) async fn run_independent_before_compact(
        &self,
        operation: Uuid,
        conversation: Uuid,
        preparation: &crate::compact::Preparation,
        checkpoint: &crate::summary_store::HookCheckpoint,
        cancel: &CancellationToken,
    ) -> Result<crate::extension::BeforeCompact, Error> {
        let state: Conversation = serde_json::from_value(checkpoint.snapshot.clone())?;
        if state.id != conversation
            || state.extensions != checkpoint.extensions
            || !preparation.source_matches(&state.messages)
        {
            return Err(Error::Invalid("invalid independent hook snapshot".into()));
        }
        let selected = self.selected_extensions(&state)?;
        if preparation.has_blocks() {
            if !selected.supports_compaction_blocks()
                || (preparation.has_tool_blocks() && !selected.supports_compaction_tool_blocks())
            {
                return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
            }
            let cursor = state
                .transcript
                .clone()
                .ok_or(crate::transcript::Error::Corrupt)?;
            let (messages, blocks) = self.read_transcript_blocks(conversation, cursor)?;
            if !preparation.matches_blocks(&messages, &blocks, state.context_generation)
                || messages != state.messages
            {
                return Err(Error::Invalid(
                    "invalid independent hook block snapshot".into(),
                ));
            }
        }
        let memo = self.extension_memo(operation)?;
        let context = crate::extension::Context {
            snapshot: &checkpoint.snapshot,
            memo: &memo,
        };
        let input = preparation.hook_input(crate::extension::CompactionReason::Manual);
        let gate = self.owner.conversation_controls().get(conversation).gate;
        let (decision, reports) = fenced_poll(
            selected.before_compact(&input, &context),
            Some(&gate),
            cancel,
        )
        .await
        .ok_or(Error::Interrupted)?;
        self.subagent_store()?;
        self.record_extension_reports(conversation, operation, "before_compact", reports)?;
        Ok(decision)
    }

    /// Opens under exclusive scheduler ownership and resumes all unfinished work.
    /// A cancelled or failed open drops the owner; the next open can recover it.
    pub async fn open(
        path: impl AsRef<Path>,
        options: Options,
        cancel: &CancellationToken,
    ) -> Result<Self, Error> {
        let mut harness = Self::open_paused(path, options)?;
        harness.resume(cancel).await?;
        Ok(harness)
    }
    /// Acquire ownership and recover persisted phases without executing them.
    /// This lets an operator inspect blocked or failing conversations.
    pub fn open_paused(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        if options.restart_limit == 0 || options.max_turns == 0 {
            return Err(Error::Invalid("limits must be positive".into()));
        }
        let mut tools = HashMap::new();
        for registered in &options.tools {
            let spec = registered.tool.spec();
            if spec.name.is_empty()
                || tools
                    .insert(spec.name.clone(), registered.clone())
                    .is_some()
            {
                return Err(Error::Invalid(format!(
                    "duplicate or empty tool name: {}",
                    spec.name
                )));
            }
        }
        let owner = Arc::new(OwnedStore::open(path)?);
        let merged_source = Arc::new(MergedSourceLease {
            owner: Arc::downgrade(&owner),
            signal: Arc::new(MergedSourceSignal {
                closed: CancellationToken::new(),
                boundary: std::sync::Mutex::new(None),
            }),
        });
        let mut harness = Self {
            owner,
            merged_source: Some(merged_source),
            options,
            tools,
            provider_adapter: None,
            summary_provider: None,
            extensions: Arc::new(std::sync::RwLock::new(crate::extension::Registry::default())),
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
            merge_admissions: false,
            recovering: false,
            storage_failed: Arc::new(AtomicBool::new(false)),
            loaded: std::sync::Mutex::new(HashMap::new()),
            execution_gate: None,
            task_driver: Arc::new(std::sync::Mutex::new(None)),
            activity: Default::default(),
            clock: Arc::new(wall_time_millis),
        };
        harness.recover()?;
        Ok(harness)
    }
    pub(crate) fn worker(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            merged_source: self.merged_source.clone(),
            options: self.options.clone(),
            tools: self.tools.clone(),
            provider_adapter: self.provider_adapter.clone(),
            summary_provider: self.summary_provider.clone(),
            extensions: self.extensions.clone(),
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
            merge_admissions: true,
            recovering: false,
            storage_failed: self.storage_failed.clone(),
            loaded: std::sync::Mutex::new(HashMap::new()),
            execution_gate: None,
            task_driver: self.task_driver.clone(),
            activity: self.activity.clone(),
            clock: self.clock.clone(),
        }
    }
    fn watch_worker(&self) -> Self {
        let mut worker = self.worker();
        worker.merged_source = None;
        worker
    }
    pub(crate) async fn drive_owned_cleanup(
        &mut self,
        id: Uuid,
        stop: &CancellationToken,
    ) -> Result<(), Error> {
        let activity = self.activity.enter_at(
            crate::worker_activity::WorkerKind::Conversation,
            id,
            crate::worker_activity::WorkerPhase::Queued,
        );
        let lock = self.owner.conversation_controls().get(id).worker;
        let guard = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            guard = lock.lock() => guard,
        };
        let control = self.owner.conversation_controls().get(id);
        self.execution_gate = Some(control.gate);
        let mut guard = Some(guard);
        let errors = self.owner.conversation_controls().worker_errors();
        let result = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            result = std::panic::AssertUnwindSafe(drive_scheduled(self, id, &control.cleanup_cancel, &lock, &mut guard, Some(&errors), activity)).catch_unwind() => result,
        };
        let failure = match &result {
            Ok(Err(error)) => Some(error.to_string()),
            Err(_) => Some("worker panicked".into()),
            Ok(Ok(())) => None,
        };
        if let Some(failure) = failure {
            errors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id, failure);
        }
        match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    pub(crate) fn subagent_store(&self) -> Result<&durable_store::Store, Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        Ok(self.owner.store())
    }
    /// Obtain memo storage for a stable task identity. Entries survive reopen.
    pub fn extension_memo(&self, task: Uuid) -> Result<crate::extension::DurableMemo, Error> {
        self.subagent_store()?;
        Ok(crate::extension::DurableMemo::new(
            self.owner.clone(),
            task,
            self.storage_failed.clone(),
        ))
    }
    /// Atomically publish a new registry snapshot to subsequent execution phases.
    pub fn install_extension(&self, extension: crate::extension::Extension) -> Result<(), Error> {
        self.extensions
            .write()
            .map_err(|_| Error::Failed)?
            .install(extension)
            .map_err(|e| Error::Invalid(format!("extension registry: {e}")))
    }
    pub fn uninstall_extension(&self, name: &str) -> Result<(), Error> {
        self.extensions
            .write()
            .map_err(|_| Error::Failed)?
            .uninstall(name);
        Ok(())
    }
    fn selected_extensions(&self, c: &Conversation) -> Result<crate::extension::Resolved, Error> {
        if c.extensions.iter().any(|name| {
            name.starts_with(crate::subagent_management::EXTENSION_PREFIX)
                && name != &crate::subagent_management::extension_name(c.id)
        }) {
            return Err(Error::Invalid(
                "subagent management extension belongs to another conversation".into(),
            ));
        }
        let snapshot = self
            .extensions
            .read()
            .map_err(|_| Error::Failed)?
            .snapshot();
        let available = snapshot.names();
        if let Some(missing) = c
            .extensions
            .iter()
            .find(|name| !available.contains(&name.as_str()))
        {
            return Err(Error::Invalid(format!(
                "selected extension unavailable: {missing}"
            )));
        }
        Ok(snapshot.resolve(Some(&c.extensions)))
    }
    fn resolved_tools(&self, c: &Conversation) -> Result<HashMap<String, RegisteredTool>, Error> {
        let mut tools = self.tools.clone();
        for tool in self.selected_extensions(c)?.tools() {
            tools.insert(
                tool.spec().name,
                RegisteredTool {
                    tool,
                    replay: ReplayPolicy::Unsafe,
                },
            );
        }
        if let Some(allowed) = &c.allowed_tools {
            tools.retain(|name, _| allowed.contains(name));
        }
        tools.retain(|name, _| !c.tool_filters.excluded.contains(name));
        Ok(tools)
    }
    fn ordered_tool_specs(&self, c: &Conversation) -> Result<Vec<ToolSpec>, Error> {
        let mut tools = self.resolved_tools(c)?;
        tools.retain(|name, _| caller_allowed(c, name, true));
        if let Some(allowed) = c.model_tools.as_ref() {
            return Ok(allowed
                .iter()
                .filter_map(|name| tools.remove(name))
                .map(|tool| tool.tool.spec())
                .collect());
        }
        let mut specs = if let Some(allowed) = &c.allowed_tools {
            allowed
                .iter()
                .filter_map(|name| tools.remove(name))
                .map(|tool| tool.tool.spec())
                .collect::<Vec<_>>()
        } else {
            let mut specs: Vec<_> = tools.into_values().map(|tool| tool.tool.spec()).collect();
            specs.sort_by(|a, b| a.name.cmp(&b.name));
            specs
        };
        let mut deferred = HashMap::new();
        specs.retain(|spec| {
            if c.tool_filters.deferred.contains(&spec.name) {
                deferred.insert(spec.name.clone(), spec.clone());
                false
            } else {
                !c.tool_filters.model_excluded.contains(&spec.name)
            }
        });
        for name in &c.tool_filters.model_add {
            if !c.tool_filters.model_excluded.contains(name)
                && let Some(spec) = deferred.remove(name)
            {
                specs.push(spec);
            }
        }
        Ok(specs)
    }
    fn supports_blocks(&self, c: &Conversation) -> bool {
        c.provider_adapter
            .as_ref()
            .and_then(|id| self.provider_adapter.as_ref().filter(|a| a.id() == id))
            .is_some_and(|a| a.supports_durable_blocks())
    }
    fn status_aware(&self, c: &Conversation) -> bool {
        c.provider_adapter
            .as_ref()
            .and_then(|id| self.provider_adapter.as_ref().filter(|a| a.id() == id))
            .is_some_and(|a| a.supports_tool_result_metadata())
    }
    /// Append a finalized result and bind the operation identity when the
    /// selected provider consumes result metadata.
    fn append_tool_with_status(
        &self,
        c: &mut Conversation,
        calls: &[ToolCall],
        index: usize,
        operation_id: Uuid,
        content: String,
    ) -> Result<(), Error> {
        if !self.status_aware(c) {
            return append_tool(c, calls, index, content);
        }
        let metadata = c
            .tool_results
            .iter_mut()
            .find(|m| m.operation_id == operation_id)
            .ok_or_else(|| Error::Invalid("missing tool result metadata".into()))?;
        let message_id = Uuid::new_v4();
        metadata.content_block_id = Some(message_id);
        let position = c.messages.len();
        append_tool(c, calls, index, String::new())?;
        c.content_blocks.insert(
            position,
            crate::transcript::MessageContentBlocks {
                message_id,
                blocks: vec![serde_json::json!({"type":"text","text":content})],
            },
        );
        Ok(())
    }
    fn append_skipped_with_status(
        &self,
        c: &mut Conversation,
        calls: &[ToolCall],
        index: usize,
        reason: &str,
    ) -> Result<(), Error> {
        let outcome = Outcome::Skipped(reason.into());
        if !self.status_aware(c) {
            return append_tool(c, calls, index, outcome.tool_content());
        }
        let operation_id = Uuid::new_v4();
        record_tool_result(c, calls, index, operation_id, &outcome, Vec::new())?;
        self.append_tool_with_status(c, calls, index, operation_id, outcome.tool_content())
    }
    fn supports_context_blocks(&self, c: &Conversation) -> bool {
        let adapter = c
            .provider_adapter
            .as_ref()
            .and_then(|id| self.provider_adapter.as_ref().filter(|a| a.id() == id));
        c.content_blocks
            .keys()
            .all(|index| match c.messages.get(*index) {
                Some(Message::User { .. }) => adapter.is_some_and(|a| a.supports_durable_blocks()),
                Some(Message::Tool { .. }) => {
                    adapter.is_some_and(|a| a.supports_durable_tool_blocks())
                }
                _ => false,
            })
    }
    fn append_yield(c: &mut Conversation, input: crate::content::UserContent, durable: bool) {
        if durable && let crate::content::UserContent::Blocks(blocks) = &input {
            let index = c.messages.len();
            c.messages.push(Message::User {
                content: String::new(),
            });
            c.content_blocks.insert(
                index,
                crate::transcript::MessageContentBlocks {
                    message_id: Uuid::new_v4(),
                    blocks: blocks.clone(),
                },
            );
        } else if let Some(message) = input.try_core() {
            c.messages.push(message);
        }
    }
    /// Install a host adapter before resuming any conversation that selected it.
    pub fn install_provider_adapter(
        &mut self,
        adapter: Arc<dyn crate::model::ProviderAdapter>,
    ) -> Result<(), Error> {
        if adapter.id().trim().is_empty() {
            return Err(Error::Invalid("empty provider adapter identity".into()));
        }
        if adapter.supports_tool_result_metadata() && !adapter.supports_durable_tool_blocks() {
            return Err(Error::Invalid(
                "tool-result metadata requires durable tool blocks".into(),
            ));
        }
        self.provider_adapter = Some(adapter);
        Ok(())
    }
    /// Resolve the current model's image profile for admission-time fitting.
    /// Returns None when the selected adapter or model is unknown.
    pub fn model_image_profile(
        &self,
        id: Uuid,
    ) -> Result<Option<crate::images::ModelImageProfile>, Error> {
        let (c, _) = self.load(id)?;
        Ok(self
            .provider_adapter
            .as_ref()
            .filter(|a| c.provider_adapter.as_deref() == Some(a.id()))
            .and_then(|a| a.image_profile(&c.config.model)))
    }
    /// Persist opt-in for a settled conversation. Reopen requires the same adapter ID.
    pub fn select_provider_adapter(&mut self, id: Uuid, enabled: bool) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "adapter selection requires a settled conversation".into(),
            ));
        }
        c.provider_adapter = if enabled {
            Some(
                self.provider_adapter
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("provider adapter unavailable".into()))?
                    .id()
                    .to_owned(),
            )
        } else {
            None
        };
        self.save(&c, Expected::Version(version))
    }
    /// Persist an exact tool allowlist on a settled conversation. None restores the host default.
    /// Names are retained even when a tool is not currently registered.
    pub fn select_tools(&mut self, id: Uuid, names: Option<Vec<String>>) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "tool selection requires a settled conversation".into(),
            ));
        }
        validate_allowed_tools(&names)?;
        c.allowed_tools = names;
        c.tool_filters.excluded.clear();
        self.save(&c, Expected::Version(version))
    }
    /// Persist an exact model-facing tool selection. None offers enabled,
    /// non-deferred tools by default.
    /// This does not change which enabled tools nested calls may execute.
    pub fn select_model_tools(
        &mut self,
        id: Uuid,
        names: Option<Vec<String>>,
    ) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "model tool selection requires a settled conversation".into(),
            ));
        }
        validate_allowed_tools(&names)?;
        c.model_tools = names;
        c.tool_filters.model_excluded.clear();
        c.tool_filters.model_add.clear();
        self.save(&c, Expected::Version(version))
    }
    /// Replace the enabled-tool selection with a remove filter.
    pub fn select_tools_except(&mut self, id: Uuid, names: Vec<String>) -> Result<(), Error> {
        validate_allowed_tools(&Some(names.clone()))?;
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "tool selection requires a settled conversation".into(),
            ));
        }
        c.allowed_tools = None;
        c.tool_filters.excluded = names;
        self.save(&c, Expected::Version(version))
    }
    /// Replace the model-facing selection with a remove/add filter.
    pub fn select_model_tools_patch(
        &mut self,
        id: Uuid,
        remove: Vec<String>,
        add: Vec<String>,
    ) -> Result<(), Error> {
        validate_allowed_tools(&Some(remove.clone()))?;
        validate_allowed_tools(&Some(add.clone()))?;
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "model tool selection requires a settled conversation".into(),
            ));
        }
        c.model_tools = None;
        c.tool_filters.model_excluded = remove;
        c.tool_filters.model_add = add;
        self.save(&c, Expected::Version(version))
    }
    /// Declare deferred names for this conversation; nested calls remain enabled.
    pub fn set_deferred_tools(&mut self, id: Uuid, names: Vec<String>) -> Result<(), Error> {
        validate_allowed_tools(&Some(names.clone()))?;
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "deferred tool selection requires a settled conversation".into(),
            ));
        }
        c.tool_filters.deferred = names;
        self.save(&c, Expected::Version(version))
    }
    /// Set caller kinds for a registered tool on a settled conversation.
    /// None removes the override and restores the default of both callers.
    pub fn set_tool_callers(
        &mut self,
        id: Uuid,
        name: String,
        callers: Option<ToolCallers>,
    ) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "tool caller selection requires a settled conversation".into(),
            ));
        }
        if name.is_empty()
            || (!self.tools.contains_key(&name)
                && !self
                    .selected_extensions(&c)?
                    .tools()
                    .iter()
                    .any(|tool| tool.spec().name == name))
        {
            return Err(Error::Invalid(
                "tool caller selection requires a registered tool".into(),
            ));
        }
        if let Some(callers) = callers {
            c.tool_filters.callers.insert(name, callers);
        } else {
            c.tool_filters.callers.remove(&name);
        }
        self.save(&c, Expected::Version(version))
    }
    /// Persist an optional JSON schema for a registered tool's structured output.
    pub fn set_tool_output_schema(
        &mut self,
        id: Uuid,
        name: String,
        schema: Option<serde_json::Value>,
    ) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "tool schema selection requires a settled conversation".into(),
            ));
        }
        if name.is_empty()
            || (!self.tools.contains_key(&name)
                && !self
                    .selected_extensions(&c)?
                    .tools()
                    .iter()
                    .any(|tool| tool.spec().name == name))
        {
            return Err(Error::Invalid(
                "tool schema selection requires a registered tool".into(),
            ));
        }
        if let Some(schema) = schema {
            if serde_json::to_vec(&schema)?.len() > 65_536
                || !local_schema_references(&schema)
                || jsonschema::validator_for(&schema).is_err()
            {
                return Err(Error::Invalid(
                    "invalid or oversized tool output schema".into(),
                ));
            }
            c.tool_filters.output_schemas.insert(name, schema);
        } else {
            c.tool_filters.output_schemas.remove(&name);
        }
        self.save(&c, Expected::Version(version))
    }
    /// Select installed names for a settled conversation, preserving their order.
    pub fn select_extensions(&mut self, id: Uuid, names: Vec<String>) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle | Phase::FailedIdle) || c.active.is_some() {
            return Err(Error::Invalid(
                "extension selection requires a settled conversation".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        if names.iter().any(|name| !seen.insert(name.clone())) {
            return Err(Error::Invalid("duplicate selected extension".into()));
        }
        c.extensions = names;
        self.selected_extensions(&c)?;
        self.save(&c, Expected::Version(version))
    }
    pub(crate) fn subagent_storage_failed(&mut self) {
        self.failed = true;
        self.storage_failed.store(true, Ordering::Release);
    }
    pub(crate) fn key(id: Uuid) -> Vec<u8> {
        format!("runtime/conversation/{id}").into_bytes()
    }
    fn load(&self, id: Uuid) -> Result<(Conversation, u64), Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let entry = self
            .owner
            .store()
            .get(&Self::key(id))?
            .ok_or(Error::NotFound)?;
        let mut state: Conversation = serde_json::from_slice(&entry.value)?;
        self.hydrate(&mut state)?;
        self.loaded
            .lock()
            .map_err(|_| Error::Failed)?
            .insert(id, (entry.version, state.clone()));
        Ok((state, entry.version))
    }

    // Preflight the complete immutable chain before using the existing
    // provenance resolver, whose general-purpose interface is unbounded.
    fn load_for_summary_placement(&self, id: Uuid) -> Result<(Conversation, u64), Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let entry = self
            .owner
            .store()
            .get(&Self::key(id))?
            .ok_or(Error::NotFound)?;
        let mut bytes = entry.value.len();
        if bytes > SUMMARY_MAX_HISTORY_BYTES {
            return Err(Error::Invalid(
                "independent summary history exceeds placement limits".into(),
            ));
        }
        let mut state: Conversation = serde_json::from_slice(&entry.value)?;
        let Some(mut cursor) = state.transcript.clone() else {
            return Err(Error::Invalid(
                "independent summary requires a transcript".into(),
            ));
        };
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut records = 0usize;
        loop {
            let mut page = transcript.read_with_blocks(&cursor, std::num::NonZeroUsize::MIN)?;
            let Some(record) = page.records.pop() else {
                break;
            };
            records += 1;
            bytes = bytes.saturating_add(serde_json::to_vec(&record.value)?.len());
            if let Some(blocks) = &record.content_blocks {
                bytes = bytes.saturating_add(serde_json::to_vec(blocks)?.len());
            }
            if records > SUMMARY_MAX_HISTORY_RECORDS || bytes > SUMMARY_MAX_HISTORY_BYTES {
                return Err(Error::Invalid(
                    "independent summary history exceeds placement limits".into(),
                ));
            }
            let change: TranscriptChange = serde_json::from_value(record.value)?;
            let origin = match &change {
                TranscriptChange::AppendV2 { origin, commit, .. }
                | TranscriptChange::AppendV3 { origin, commit, .. }
                | TranscriptChange::AppendV4 { origin, commit, .. }
                | TranscriptChange::ReplaceV2 { origin, commit, .. }
                | TranscriptChange::ReplaceCompacted { origin, commit, .. }
                | TranscriptChange::ForkCut { origin, commit, .. } => Some((*origin, *commit)),
                _ => None,
            };
            if let Some((origin, commit)) = origin {
                let after = commit
                    .checked_sub(1)
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                let page = crate::observer::Observer::new(self.owner.store(), origin)
                    .read(crate::observer::Cursor(after), std::num::NonZeroUsize::MIN)?;
                if let Some(record) = page.records.first() {
                    bytes = bytes.saturating_add(serde_json::to_vec(&record.value)?.len());
                }
                let key = format!("runtime/fork/{origin}/{commit:020}");
                if let Some(snapshot) = self.owner.store().get(key.as_bytes())? {
                    bytes = bytes.saturating_add(snapshot.value.len());
                }
                if bytes > SUMMARY_MAX_HISTORY_BYTES {
                    return Err(Error::Invalid(
                        "independent summary history exceeds placement limits".into(),
                    ));
                }
            }
            if !page.has_more {
                break;
            }
            cursor = page.next;
        }
        if bytes > SUMMARY_MAX_HISTORY_BYTES {
            return Err(Error::Invalid(
                "independent summary history exceeds placement limits".into(),
            ));
        }
        self.hydrate(&mut state)?;
        self.loaded
            .lock()
            .map_err(|_| Error::Failed)?
            .insert(id, (entry.version, state.clone()));
        Ok((state, entry.version))
    }
    fn hydrate(&self, state: &mut Conversation) -> Result<(), Error> {
        if let Phase::ToolFinished { operation_id, .. } = &state.phase {
            let key = format!("runtime/tool-result-pending/{}/{}", state.id, operation_id);
            state.pending_tool_result = self
                .owner
                .store()
                .get(key.as_bytes())?
                .map(|entry| serde_json::from_slice(&entry.value))
                .transpose()?;
        }
        if let Some(cursor) = &state.transcript {
            let (messages, blocks) = self.read_transcript_blocks(state.id, cursor.clone())?;
            state.messages = messages;
            state.content_blocks = blocks;
        }
        validate_pending_reset_blocks(state)?;
        for (key, receipt) in &state.fitted_reset_receipts {
            if key.is_empty()
                || receipt.reset.is_nil()
                || receipt.occurrences.is_empty()
                || receipt.occurrences.iter().any(Uuid::is_nil)
                || receipt
                    .occurrences
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != receipt.occurrences.len()
                || !state
                    .resets
                    .iter()
                    .chain(&state.pending_resets)
                    .any(|r| r.id == receipt.reset)
                || state
                    .pending_reset_blocks
                    .get(&receipt.reset)
                    .is_some_and(|b| {
                        b.iter()
                            .map(|b| b.message_id)
                            .ne(receipt.occurrences.iter().copied())
                    })
            {
                return Err(Error::Invalid("invalid fitted reset receipt".into()));
            }
        }
        let mut ids = std::collections::HashSet::new();
        for submission in &state.submissions {
            if let Some(binding) = &submission.input_blocks
                && (submission.kind != SubmissionKind::Input
                    || !submission.content.is_empty()
                    || binding.message_id.is_nil()
                    || !ids.insert(binding.message_id)
                    || crate::content::UserContent::from_yield(
                        serde_json::Value::Array(binding.blocks.clone()),
                        false,
                    )
                    .is_none())
            {
                return Err(Error::Invalid("invalid fitted input binding".into()));
            }
        }
        Ok(())
    }
    fn read_transcript_blocks(
        &self,
        id: Uuid,
        mut cursor: crate::transcript::Cursor,
    ) -> Result<
        (
            Vec<Message>,
            std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks>,
        ),
        Error,
    > {
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut parts = Vec::new();
        loop {
            let page = transcript.read_with_blocks(&cursor, std::num::NonZeroUsize::MIN)?;
            let record = page
                .records
                .into_iter()
                .next()
                .ok_or(crate::transcript::Error::Corrupt)?;
            let change: TranscriptChange = serde_json::from_value(record.value)?;
            let base = !matches!(
                change,
                TranscriptChange::ReplaceCompacted { .. } | TranscriptChange::ForkCut { .. }
            ) && change.clone().parts()?.1;
            parts.push((cursor.clone(), change, record.content_blocks));
            if base {
                break;
            }
            if !page.has_more {
                return Err(Error::Invalid("transcript has no base".into()));
            }
            cursor = page.next;
        }
        let mut messages: Vec<Message> = Vec::new();
        let mut blocks: std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks> =
            std::collections::BTreeMap::new();
        for (position, change, bindings) in parts.into_iter().rev() {
            if let TranscriptChange::ForkCut {
                messages: next,
                histories,
                ..
            } = &change
            {
                let resolved = self.resolved_occurrences(id, position)?;
                if resolved.iter().map(|(m, _)| m).ne(next.iter())
                    || next.len() >= messages.len()
                    || next.as_slice() != &messages[..next.len()]
                    || histories.len() != next.len()
                {
                    return Err(crate::transcript::Error::Corrupt.into());
                }
                blocks.retain(|index, _| *index < next.len());
                let stored = bindings.unwrap_or_default();
                let mut expected = Vec::new();
                for (index, binding) in &blocks {
                    if resolved
                        .get(*index)
                        .and_then(|(_, h)| h.as_ref())
                        .map(|h| h.id)
                        != Some(binding.message_id)
                    {
                        return Err(crate::transcript::Error::Corrupt.into());
                    }
                    expected.push(binding.clone());
                }
                if expected != stored {
                    return Err(crate::transcript::Error::Corrupt.into());
                }
                messages = next.clone();
            } else if let TranscriptChange::ReplaceCompacted {
                messages: next,
                system_end,
                first_kept,
                ..
            } = &change
            {
                if bindings.is_some() {
                    return Err(crate::transcript::Error::Corrupt.into());
                }
                let resolved = self.resolved_occurrences(id, position)?;
                if resolved.iter().map(|(m, _)| m).ne(next.iter())
                    || *system_end > *first_kept
                    || *first_kept > messages.len()
                    || next.len() != system_end + 1 + messages.len() - first_kept
                    || next[..*system_end] != messages[..*system_end]
                    || next[*system_end + 1..] != messages[*first_kept..]
                {
                    return Err(crate::transcript::Error::Corrupt.into());
                }
                blocks = project_compaction_blocks(&blocks, *system_end, *first_kept)?;
                for (index, binding) in &blocks {
                    if resolved
                        .get(*index)
                        .and_then(|(_, h)| h.as_ref())
                        .map(|h| h.id)
                        != Some(binding.message_id)
                    {
                        return Err(crate::transcript::Error::Corrupt.into());
                    }
                }
                messages = next.clone();
            } else {
                let (part, base, metadata) = change.parts()?;
                if base {
                    messages.clear();
                    blocks.clear();
                }
                let offset = messages.len();
                if let Some(bindings) = bindings {
                    let (ids, _, _) = metadata.ok_or(crate::transcript::Error::Corrupt)?;
                    for binding in bindings {
                        let index = ids
                            .iter()
                            .position(|id| *id == binding.message_id)
                            .ok_or(crate::transcript::Error::Corrupt)?;
                        blocks.insert(offset + index, binding);
                    }
                }
                messages.extend(part);
            }
        }
        Ok((messages, blocks))
    }
    fn expand_event(&self, id: Uuid, value: &mut serde_json::Value) -> Result<(), Error> {
        if let Some(reference) = value.get_mut("_transcript").map(std::mem::take) {
            let cursor = serde_json::from_value(reference)?;
            let (messages, blocks) = self.read_transcript_blocks(id, cursor)?;
            value["messages"] = serde_json::to_value(messages)?;
            if !blocks.is_empty() {
                value["content_blocks"] = serde_json::to_value(blocks)?;
            }
            value
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("invalid observation".into()))?
                .remove("_transcript");
        }
        if let Some(object) = value.as_object_mut() {
            object.remove("_reset_baseline");
            object.remove("_reset_baseline_sha256");
        }
        Ok(())
    }
    fn save(&mut self, state: &Conversation, expected: Expected) -> Result<(), Error> {
        self.save_composed(state, expected, None, &[])
    }
    fn save_with_wake(
        &mut self,
        state: &Conversation,
        expected: Expected,
        wake: Option<&crate::conversation_signal::PreparedWake>,
    ) -> Result<(), Error> {
        self.save_composed(state, expected, wake, &[])
    }
    fn save_composed(
        &mut self,
        state: &Conversation,
        expected: Expected,
        wake: Option<&crate::conversation_signal::PreparedWake>,
        children: &[crate::task::Task],
    ) -> Result<(), Error> {
        let owner = self.owner.clone();
        let _gate = if children.is_empty() {
            None
        } else {
            Some(owner.task_gate().lock().map_err(|_| Error::Failed)?)
        };
        self.save_composed_locked(state, expected, wake, children, &[], None)
    }
    fn save_hook_children(
        &mut self,
        state: &Conversation,
        expected: Expected,
        children: &[crate::task::Task],
        handlers: &[Arc<dyn crate::task_scheduler::Handler>],
    ) -> Result<(), Error> {
        if children.is_empty() {
            return self.save_composed(state, expected, None, children);
        }
        let driver = self
            .task_driver
            .lock()
            .map_err(|_| Error::Failed)?
            .as_ref()
            .and_then(|driver| driver.upgrade())
            .ok_or_else(|| Error::Invalid("task driver is not running".into()))?;
        driver.with_hook_children(children, handlers, state.id, || {
            self.save_composed_locked(state, expected, None, children, &[], None)
        })
    }
    fn save_composed_locked(
        &mut self,
        state: &Conversation,
        expected: Expected,
        wake: Option<&crate::conversation_signal::PreparedWake>,
        children: &[crate::task::Task],
        abort_tasks: &[crate::task_store::StoredTask],
        documents: Option<&crate::task_store::OwnedBatch>,
    ) -> Result<(), Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let key = Self::key(state.id);
        let mut state = state.clone();
        let failed_transition = {
            let loaded = self.loaded.lock().map_err(|_| Error::Failed)?;
            loaded.get(&state.id).is_some_and(|(_, base)| {
                // Only the persisted abort mark identifies abort cleanup: a
                // provider failure can have the same text as an abort reason.
                matches!(state.phase, Phase::FailedIdle)
                    && !matches!(base.phase, Phase::FailedIdle)
                    && !base.abort_requested
            })
        };
        let mut expected = expected;
        loop {
            let mut committed = state.clone();
            if failed_transition {
                apply_failed_run_policy(&mut committed);
            }
            let state_for_commit = &committed;
            let mut prepared = if wake.is_none() && children.is_empty() && abort_tasks.is_empty() {
                self.prepare_saves(&[(state_for_commit, expected.clone())])?
            } else {
                self.prepare_conversation_save(
                    state_for_commit,
                    expected.clone(),
                    wake,
                    children,
                    abort_tasks,
                )?
            };
            if let Some(batch) = documents {
                prepared.extend(PreparedConversationSave {
                    owner: self.owner.clone(),
                    checks: batch.checks.clone(),
                    mutations: batch.mutations.clone(),
                })?;
            }
            match self.commit_conversation_save(prepared) {
                Ok(()) => break,
                Err(Error::Storage(durable_store::Error::Conflict { key: conflict }))
                    if conflict.starts_with(b"durable/tool-progress/")
                        && self
                            .owner
                            .store()
                            .get(&key)?
                            .is_some_and(|entry| expected == Expected::Version(entry.version)) =>
                {
                    // A callback won the race with sealing. Reload its latest
                    // partial and retry the whole atomic transition.
                    continue;
                }
                Err(Error::Storage(durable_store::Error::Conflict { key }))
                    if key == FORK_DOCUMENT_FENCE =>
                {
                    return Err(Error::Storage(durable_store::Error::Conflict { key }));
                }
                Err(Error::Storage(durable_store::Error::Conflict { key: conflict }))
                    if self.merge_admissions =>
                {
                    let entry = self.owner.store().get(&key)?.ok_or(Error::NotFound)?;
                    // Merging admissions can only resolve a changed conversation.
                    // A task ID collision (or any other independently conflicting
                    // record) otherwise retries the same transaction forever.
                    if expected == Expected::Version(entry.version) {
                        return Err(Error::Storage(durable_store::Error::Conflict {
                            key: conflict,
                        }));
                    }
                    let mut latest: Conversation = serde_json::from_slice(&entry.value)?;
                    self.hydrate(&mut latest)?;
                    let base = self
                        .loaded
                        .lock()
                        .map_err(|_| Error::Failed)?
                        .get(&state.id)
                        .cloned();
                    let Some((base_version, base)) = base else {
                        return Err(Error::Invalid("missing conflict baseline".into()));
                    };
                    if expected != Expected::Version(base_version)
                        || !admission_only(&base, &latest)?
                    {
                        return Err(Error::Invalid(
                            "conversation changed during execution; reopen before retry".into(),
                        ));
                    }
                    if base.pending_independent_summary.is_none()
                        && latest.pending_independent_summary.is_some()
                    {
                        state.pending_independent_summary =
                            latest.pending_independent_summary.clone();
                    }
                    // Settings changes can race external model/tool calls. Preserve the
                    // newest values; boundary commits retry selection separately.
                    state.queue_modes = latest.queue_modes;
                    state.after_failed_run = latest.after_failed_run;
                    for marker in latest.pending_resets.iter().skip(base.pending_resets.len()) {
                        state.pending_resets.push(marker.clone());
                    }
                    for (key, receipt) in &latest.fitted_reset_receipts {
                        if !base.fitted_reset_receipts.contains_key(key)
                            && state
                                .fitted_reset_receipts
                                .insert(key.clone(), receipt.clone())
                                .is_some()
                        {
                            return Err(Error::Invalid("duplicate fitted reset receipt".into()));
                        }
                    }
                    for (id, blocks) in &latest.pending_reset_blocks {
                        if !base.pending_reset_blocks.contains_key(id)
                            && state
                                .pending_reset_blocks
                                .insert(*id, blocks.clone())
                                .is_some()
                        {
                            return Err(Error::Invalid("duplicate pending reset blocks".into()));
                        }
                    }
                    // A manual summary is admitted at this commit, not when
                    // its hook or usage callback began. Reset admissions merged
                    // into this commit therefore precede the summary.
                    if let Phase::CompactionPlacement {
                        resets_before: Some(before),
                        ..
                    } = &mut state.phase
                    {
                        *before = state.pending_resets.iter().map(|reset| reset.id).collect();
                    }
                    for (original, current) in base
                        .application_writes
                        .iter()
                        .zip(&latest.application_writes)
                    {
                        if !original.withdrawn
                            && current.withdrawn
                            && let Some(write) = state
                                .application_writes
                                .iter_mut()
                                .find(|w| w.id == current.id)
                        {
                            write.placed = false;
                            write.withdrawn = true;
                        }
                    }
                    for mut write in latest
                        .application_writes
                        .iter()
                        .skip(base.application_writes.len())
                        .cloned()
                    {
                        if !write.withdrawn
                            && state.pending_independent_summary.is_none()
                            && write
                                .after_reset
                                .is_some_and(|id| state.resets.iter().any(|r| r.id == id))
                        {
                            write.placed = true;
                        }
                        state.application_writes.push(write);
                    }
                    for submission in latest.submissions {
                        if let Some(existing) =
                            state.submissions.iter_mut().find(|s| s.id == submission.id)
                        {
                            if existing.status == Status::Queued
                                && matches!(&submission.status, Status::Unanswered { reason } if reason == "aborted")
                            {
                                existing.status = submission.status.clone();
                            } else if let Some(original) =
                                base.submissions.iter().find(|s| s.id == submission.id)
                                && original.status != submission.status
                            {
                                return Err(Error::Invalid(
                                    "queued input changed while being placed; retry execution"
                                        .into(),
                                ));
                            }
                        } else {
                            if let Some(mode) = latest.input_modes.get(&submission.id) {
                                state.input_modes.insert(submission.id, *mode);
                            }
                            state.submissions.push(submission);
                        }
                    }
                    if base.abort_requested
                        && !state.abort_requested
                        && matches!(state.phase, Phase::FailedIdle)
                        && state.submissions.iter().any(|s| s.status == Status::Queued)
                    {
                        state.phase = Phase::Idle;
                    }
                    expected = Expected::Version(entry.version);
                    self.loaded.lock().map_err(|_| Error::Failed)?.insert(
                        state.id,
                        (entry.version, {
                            let mut c: Conversation = serde_json::from_slice(&entry.value)?;
                            self.hydrate(&mut c)?;
                            c
                        }),
                    );
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    fn prepare_conversation_save(
        &self,
        state: &Conversation,
        expected: Expected,
        wake: Option<&crate::conversation_signal::PreparedWake>,
        children: &[crate::task::Task],
        abort_tasks: &[crate::task_store::StoredTask],
    ) -> Result<PreparedConversationSave, Error> {
        self.prepare_conversation_save_inner(state, expected, wake, children, abort_tasks, None)
    }

    fn prepare_conversation_save_inner(
        &self,
        state: &Conversation,
        expected: Expected,
        wake: Option<&crate::conversation_signal::PreparedWake>,
        children: &[crate::task::Task],
        abort_tasks: &[crate::task_store::StoredTask],
        independent: Option<&crate::compact::CompactionResult>,
    ) -> Result<PreparedConversationSave, Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let mut state = state.clone();
        validate_pending_reset_blocks(&state)?;
        let key = Self::key(state.id);
        let base = self
            .loaded
            .lock()
            .map_err(|_| Error::Failed)?
            .get(&state.id)
            .cloned();
        let old = base
            .as_ref()
            .filter(|(v, _)| expected == Expected::Version(*v))
            .map(|(_, s)| s);
        // Tool outcomes are checkpointed privately before hooks; only the
        // finalized metadata is published in observations and fork snapshots.
        if let Phase::ToolFinished { operation_id, .. } = &state.phase
            && state.pending_tool_result.is_none()
            && let Some(position) = state
                .tool_results
                .iter()
                .position(|r| r.operation_id == *operation_id)
            && old.is_none_or(|prior| {
                !prior
                    .tool_results
                    .iter()
                    .any(|r| r.operation_id == *operation_id)
            })
        {
            state.pending_tool_result = Some(state.tool_results.remove(position));
        }
        if !matches!(state.phase, Phase::ToolFinished { .. })
            && let Some(prior) = old
        {
            for metadata in &mut state.tool_results {
                if !prior
                    .tool_results
                    .iter()
                    .any(|r| r.operation_id == metadata.operation_id)
                {
                    validate_tool_metadata(&state.tool_filters, metadata);
                }
            }
        }
        let mut append = crate::observer::Observer::new(self.owner.store(), state.id)
            .prepare_append(&serde_json::json!({}))?;
        let commit = append.cursor.0;
        let create = old.and_then(|s| s.transcript.as_ref()).is_none();
        let compacted = old
            .map(|previous| -> Result<Option<(usize, usize)>, Error> {
                if create
                    || previous.context_generation == state.context_generation
                    || previous.resets != state.resets
                {
                    return Ok(None);
                }
                let result = match (independent, &previous.phase) {
                    (Some(result), _) => result,
                    (None, Phase::CompactionPlacement { result, .. }) => result,
                    _ => return Ok(None),
                };
                let applied =
                    if independent.is_none() && previous.active_manual_compaction.is_some() {
                        self.apply_manual_compaction_target(previous, result)?
                    } else {
                        result
                            .apply_in_context(&previous.messages, previous.context_generation)
                            .ok()
                            .map(|messages| (messages, result.retained_range()))
                    };
                Ok(applied
                    .and_then(|(messages, range)| (messages == state.messages).then_some(range)))
            })
            .transpose()?
            .flatten();
        let block_compaction = if let (Some(previous), Some((system_end, first_kept))) =
            (old, compacted)
        {
            let result = match (independent, &previous.phase) {
                (Some(result), _) => Some(result),
                (None, Phase::CompactionPlacement { result, .. }) => Some(result.as_ref()),
                _ => None,
            };
            result.is_some_and(|result| {
                result.has_blocks()
                    && (if independent.is_some() {
                        result.matches_blocks_prefix(
                            &previous.messages,
                            &previous.content_blocks,
                            previous.context_generation,
                        )
                    } else {
                        result.matches_blocks(
                            &previous.messages,
                            &previous.content_blocks,
                            previous.context_generation,
                        )
                    })
                    && project_compaction_blocks(&previous.content_blocks, system_end, first_kept)
                        .is_ok_and(|blocks| blocks == state.content_blocks)
            })
        } else {
            false
        };
        let reset_proven = old.is_some_and(|previous| {
            state.block_reset_proof.as_ref().is_some_and(|proof| {
                proof.previous_context == previous.context_generation
                    && proof.previous_resets == previous.resets
                    && proof.previous_messages == previous.messages
                    && proof.previous_blocks == previous.content_blocks
                    && !proof.placed_resets.is_empty()
                    && !proof.overwritten_blocks
                    && state.resets.len() == proof.previous_resets.len() + proof.placed_resets.len()
                    && state.resets.starts_with(&proof.previous_resets)
                    && state.resets[proof.previous_resets.len()..] == proof.placed_resets
                    && state.context_generation != previous.context_generation
                    && state.content_blocks == proof.blocks
                    && state.messages == proof.messages
            })
        });
        if let Some(previous) = old {
            for (id, expected_blocks) in &previous.pending_reset_blocks {
                if state.pending_reset_blocks.get(id) == Some(expected_blocks) {
                    continue;
                }
                let placed = reset_proven
                    && state.block_reset_proof.as_ref().is_some_and(|proof| {
                        proof
                            .placed_resets
                            .last()
                            .is_some_and(|reset| reset.id == *id)
                            && state
                                .content_blocks
                                .values()
                                .take(expected_blocks.len())
                                .eq(expected_blocks.iter())
                    });
                if !placed {
                    return Err(Error::Invalid(
                        "pending reset blocks changed or disappeared".into(),
                    ));
                }
            }
        }
        if let Some(previous) = old
            && !previous.content_blocks.is_empty()
        {
            let unchanged = state.messages.starts_with(&previous.messages)
                && previous
                    .content_blocks
                    .iter()
                    .all(|(i, b)| state.content_blocks.get(i) == Some(b))
                && previous.context_generation == state.context_generation
                && state.resets == previous.resets;
            if !unchanged && !reset_proven && !block_compaction {
                return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
            }
        }
        if let Some(previous) = old
            && state.resets != previous.resets
            && !state.content_blocks.is_empty()
            && !reset_proven
        {
            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
        }
        if reset_proven && !state.content_blocks.is_empty() {
            let previous = old.unwrap();
            let source = previous
                .transcript
                .clone()
                .ok_or(crate::transcript::Error::UnsupportedContentBlocks)?;
            if !previous.occurrence_epoch_proven {
                return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
            }
            let (_, created) = self.resolved_occurrences_with_ids(state.id, source)?;
            if state
                .content_blocks
                .values()
                .any(|binding| created.contains(&binding.message_id))
            {
                return Err(Error::Invalid(
                    "reset block occurrence identity collision".into(),
                ));
            }
        }
        let old_len = old.map_or(0, |s| s.messages.len());
        let mut block_ids = std::collections::HashSet::new();
        for (&index, binding) in &state.content_blocks {
            if !matches!(
                state.messages.get(index),
                Some(Message::User { content } | Message::Tool { content, .. }) if content.is_empty()
            ) || binding.message_id.is_nil()
                || !block_ids.insert(binding.message_id)
                || crate::content::UserContent::from_yield(
                    serde_json::Value::Array(binding.blocks.clone()),
                    false,
                )
                .is_none()
                || (!block_compaction
                    && !reset_proven
                    && index < old_len
                    && old.and_then(|s| s.content_blocks.get(&index)) != Some(binding))
            {
                return Err(Error::Invalid("invalid transcript block binding".into()));
            }
        }
        if independent.is_some() && compacted.is_none() {
            return Err(Error::Invalid(
                "independent compaction proof changed".into(),
            ));
        }
        let change = if let Some((system_end, first_kept)) = compacted {
            let previous = old.unwrap();
            let (source, created) =
                self.resolved_occurrences_with_ids(state.id, previous.transcript.clone().unwrap())?;
            if source
                .iter()
                .map(|(message, _)| message)
                .ne(previous.messages.iter())
            {
                return Err(Error::Invalid(
                    "compaction source transcript changed".into(),
                ));
            }
            if block_compaction
                && previous.content_blocks.iter().any(|(index, binding)| {
                    source
                        .get(*index)
                        .and_then(|(_, history)| history.as_ref())
                        .map(|history| history.id)
                        != Some(binding.message_id)
                })
            {
                return Err(crate::transcript::Error::Corrupt.into());
            }
            let summary_id = Uuid::new_v4();
            if created.contains(&summary_id) {
                return Err(Error::Invalid(
                    "compaction occurrence identity collision".into(),
                ));
            }
            let mut histories: Vec<_> = source[..system_end]
                .iter()
                .map(|(_, history)| history.clone())
                .collect();
            histories.push(Some(MessageHistory {
                id: summary_id,
                commit,
                origin: state.id,
                commit_boundary: system_end + 1 == state.messages.len(),
            }));
            histories.extend(
                source[first_kept..]
                    .iter()
                    .map(|(_, history)| history.clone()),
            );
            Some(TranscriptChange::ReplaceCompacted {
                messages: state.messages.clone(),
                system_end,
                first_kept,
                summary_id,
                commit,
                origin: state.id,
                histories,
            })
        } else {
            match old {
                // A reset is an identity boundary even if its resulting text
                // happens to equal the previous messages.
                Some(previous) if !create && state.resets != previous.resets => {
                    Some(TranscriptChange::Replace(state.messages.clone()))
                }
                Some(previous) if !create && state.messages == previous.messages => None,
                Some(previous) if !create && state.messages.starts_with(&previous.messages) => {
                    Some(TranscriptChange::Append(
                        state.messages[previous.messages.len()..].to_vec(),
                    ))
                }
                _ => Some(TranscriptChange::Replace(state.messages.clone())),
            }
        };
        // An interior cut is provable only when every forked state field is
        // unchanged across the entire atomic append. Never infer this for old
        // batches or replacement snapshots.
        let stable_prefix = old
            .filter(|previous| {
                !create
                    && state.messages.starts_with(&previous.messages)
                    && matches!((serde_json::to_value((
                        &state.config,
                        &state.queue_modes,
                        &state.after_failed_run,
                        &state.extensions,
                        &state.provider_adapter,
                        &state.allowed_tools,
                        &state.model_tools,
                        &state.tool_filters,
                        &state.tool_results,
                        &state.occurrence_epoch_proven,
                        &state.resets,
                        &state.application_writes,
                        &state.tool_states,
                    )), serde_json::to_value((
                            &previous.config,
                            &previous.queue_modes,
                            &previous.after_failed_run,
                            &previous.extensions,
                            &previous.provider_adapter,
                            &previous.allowed_tools,
                            &previous.model_tools,
                            &previous.tool_filters,
                            &previous.tool_results,
                            &previous.occurrence_epoch_proven,
                            &previous.resets,
                            &previous.application_writes,
                            &previous.tool_states,
                        ))), (Ok(a), Ok(b)) if a == b)
            })
            .map(|previous| previous.messages.len());
        // A tool-free append may atomically change only model configuration.
        // Upstream forks use the final state of the entry's commit. Excluding
        // tool-bearing batches avoids copying effects from beyond the cut.
        let config_prefix = old.filter(|previous| {
            !create && state.messages.starts_with(&previous.messages)
                && state.messages[previous.messages.len()..].iter().all(|message| match message {
                    Message::Tool { .. } => false,
                    Message::Assistant { calls, .. } => calls.is_empty(),
                    _ => true,
                })
                && matches!((serde_json::to_value((
                    &state.queue_modes, &state.after_failed_run, &state.extensions, &state.provider_adapter,
                    &state.allowed_tools, &state.model_tools, &state.tool_filters, &state.tool_results, &state.occurrence_epoch_proven, &state.resets, &state.application_writes, &state.tool_states,
                )), serde_json::to_value((
                    &previous.queue_modes, &previous.after_failed_run, &previous.extensions, &previous.provider_adapter,
                    &previous.allowed_tools, &previous.model_tools, &previous.tool_filters, &previous.tool_results, &previous.occurrence_epoch_proven, &previous.resets, &previous.application_writes, &previous.tool_states,
                ))), (Ok(a), Ok(b)) if a == b)
        }).map(|previous| previous.messages.len());
        // Inline legacy messages predate this commit; their original identities and
        // state cannot be reconstructed. Keep the migration batch unannotated.
        let change = change.map(|change| {
            if create && old.is_some() {
                return change;
            }
            match change {
                TranscriptChange::Append(messages) if stable_prefix.is_some() => {
                    TranscriptChange::AppendV3 {
                        ids: messages.iter().map(|_| Uuid::new_v4()).collect(),
                        messages,
                        commit,
                        origin: state.id,
                        stable_prefix: stable_prefix.unwrap(),
                    }
                }
                TranscriptChange::Append(messages) if config_prefix.is_some() => {
                    TranscriptChange::AppendV4 {
                        ids: messages.iter().map(|_| Uuid::new_v4()).collect(),
                        messages,
                        commit,
                        origin: state.id,
                        stable_prefix: config_prefix.unwrap(),
                    }
                }
                TranscriptChange::Append(messages) => TranscriptChange::AppendV2 {
                    ids: messages.iter().map(|_| Uuid::new_v4()).collect(),
                    messages,
                    commit,
                    origin: state.id,
                },
                TranscriptChange::Replace(messages) => TranscriptChange::ReplaceV2 {
                    ids: messages.iter().map(|_| Uuid::new_v4()).collect(),
                    messages,
                    commit,
                    origin: state.id,
                },
                change @ TranscriptChange::ReplaceCompacted { .. } => change,
                _ => unreachable!(),
            }
        });
        let mut change = change;
        let new_blocks: Vec<_> = state
            .content_blocks
            .range(if reset_proven { 0 } else { old_len }..)
            .filter(|_| !block_compaction)
            .map(|(&index, binding)| (index, binding.clone()))
            .collect();
        if !new_blocks.is_empty() {
            let (ids, offset) = match change.as_mut() {
                Some(
                    TranscriptChange::AppendV2 { ids, .. }
                    | TranscriptChange::AppendV3 { ids, .. }
                    | TranscriptChange::AppendV4 { ids, .. },
                ) => (ids, old_len),
                Some(TranscriptChange::ReplaceV2 { ids, .. }) if old_len == 0 || reset_proven => {
                    (ids, 0)
                }
                _ => return Err(crate::transcript::Error::UnsupportedContentBlocks.into()),
            };
            for (index, binding) in &new_blocks {
                *ids.get_mut(index - offset)
                    .ok_or(crate::transcript::Error::Corrupt)? = binding.message_id;
            }
            let mut unique = std::collections::HashSet::new();
            if ids.iter().any(|id| !unique.insert(*id)) {
                return Err(crate::transcript::Error::Corrupt.into());
            }
        }
        let prepared = if expected == Expected::Missing
            && let Some(len) = state.block_fork_cut
        {
            let source = state
                .transcript
                .as_ref()
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let (messages, blocks) =
                self.read_transcript_blocks(source.transcript_id(), source.clone())?;
            let occurrences = self.resolved_occurrences(source.transcript_id(), source.clone())?;
            let expected_blocks: std::collections::BTreeMap<_, _> = blocks
                .into_iter()
                .filter(|(index, _)| *index < len)
                .collect();
            if !state.occurrence_epoch_proven
                || len >= messages.len()
                || messages.get(..len) != Some(state.messages.as_slice())
                || expected_blocks != state.content_blocks
                || occurrences.len() != messages.len()
                || occurrences[..len].iter().any(|(_, h)| h.is_none())
            {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            let cut = TranscriptChange::ForkCut {
                messages: state.messages.clone(),
                histories: occurrences[..len]
                    .iter()
                    .map(|(_, history)| history.clone())
                    .collect(),
                commit,
                origin: state.id,
            };
            let (batch, cursor) =
                crate::transcript::Transcript::new(self.owner.store(), source.transcript_id())
                    .prepare_fork_projection(
                        source,
                        state.id,
                        &serde_json::to_value(cut)?,
                        state.content_blocks.values().cloned().collect(),
                    )?;
            state.transcript = Some(cursor);
            Some(batch)
        } else if expected == Expected::Missing && state.transcript.is_some() {
            let source = state.transcript.as_ref().unwrap();
            let (messages, blocks) =
                self.read_transcript_blocks(source.transcript_id(), source.clone())?;
            if messages != state.messages || blocks != state.content_blocks {
                return Err(Error::Invalid("fork transcript does not match seed".into()));
            }
            // Verify occurrence identity and origin before sharing immutable history.
            if !blocks.is_empty() {
                let occurrences =
                    self.resolved_occurrences(source.transcript_id(), source.clone())?;
                for (index, binding) in &blocks {
                    if occurrences
                        .get(*index)
                        .and_then(|(_, history)| history.as_ref())
                        .map(|h| h.id)
                        != Some(binding.message_id)
                    {
                        return Err(crate::fork::Error::InvalidSnapshot.into());
                    }
                }
            }
            let (batch, cursor) =
                crate::transcript::Transcript::new(self.owner.store(), source.transcript_id())
                    .prepare_fork_at(source, state.id)?;
            state.transcript = Some(cursor);
            Some(batch)
        } else if let Some(change) = change {
            let (batch, cursor) = crate::transcript::Transcript::new(self.owner.store(), state.id)
                .prepare_write_with_blocks(
                    &serde_json::to_value(change)?,
                    if new_blocks.is_empty() {
                        None
                    } else {
                        Some(new_blocks.into_iter().map(|(_, b)| b).collect())
                    },
                    create,
                )?;
            state.transcript = Some(cursor);
            Some(batch)
        } else {
            state.transcript = old.and_then(|s| s.transcript.clone());
            None
        };
        let mut persisted = state.clone();
        persisted.messages.clear();
        let value = serde_json::to_vec(&persisted)?;
        let mut event = serde_json::json!({"_transcript": state.transcript, "submissions": state.submissions, "aborted_generations": state.aborted_generations, "resets": state.resets, "abort_requested": state.abort_requested, "application_writes": state.application_writes, "tool_results": state.tool_results});
        let reset_baseline: Option<serde_json::Value> = if state.pending_reset_blocks.is_empty() {
            None
        } else {
            Some(serde_json::from_slice(&value)?)
        };
        if let Some(baseline) = &reset_baseline {
            event["_reset_baseline_sha256"] =
                serde_json::Value::String(reset_baseline_digest(baseline)?);
        }
        append.set_value(&event)?;
        let fork_key = format!("runtime/fork/{}/{:020}", state.id, append.cursor.0).into_bytes();
        let fork_snapshot = crate::fork::Snapshot {
            version: 1,
            conversation_id: state.id,
            config: state.config.clone(),
            messages: vec![],
            submissions: state.submissions.clone(),
            tool_states: state.tool_states.clone(),
            documents: HashMap::from([
                (
                    "pi-rs/runtime/queue-modes".into(),
                    serde_json::to_value(state.queue_modes)?,
                ),
                (
                    "pi-rs/runtime/after-failed-run".into(),
                    serde_json::to_value(state.after_failed_run)?,
                ),
                (
                    "pi-rs/runtime/extensions".into(),
                    serde_json::to_value(&state.extensions)?,
                ),
                (
                    "pi-rs/runtime/provider-adapter".into(),
                    serde_json::to_value(&state.provider_adapter)?,
                ),
                (
                    "pi-rs/runtime/allowed-tools".into(),
                    serde_json::to_value(&state.allowed_tools)?,
                ),
                (
                    "pi-rs/runtime/model-tools".into(),
                    serde_json::to_value(&state.model_tools)?,
                ),
                (
                    "pi-rs/runtime/tool-filters".into(),
                    serde_json::to_value(&state.tool_filters)?,
                ),
                (
                    "pi-rs/runtime/tool-results".into(),
                    serde_json::to_value(&state.tool_results)?,
                ),
                (
                    "pi-rs/runtime/occurrence-epoch-proven".into(),
                    serde_json::to_value(state.occurrence_epoch_proven)?,
                ),
                (
                    "pi-rs/runtime/resets".into(),
                    serde_json::to_value(&state.resets)?,
                ),
                (
                    "pi-rs/runtime/application-writes".into(),
                    serde_json::to_value(&state.application_writes)?,
                ),
            ]),
            active: state.active,
            pending_resets: state.pending_resets.clone(),
            phase: serde_json::to_value(&state.phase)?,
        };
        let mut fork_value = serde_json::to_value(&fork_snapshot)?;
        fork_value["_transcript"] = serde_json::to_value(&state.transcript)?;
        // Retain the exact serialized root while block resets are pending so
        // a later same-commit proof can replay the committed predecessor.
        if let Some(baseline) = reset_baseline {
            fork_value["_reset_baseline"] = baseline;
        }
        let mut checks = vec![Check {
            key: key.clone(),
            expected: expected.clone(),
        }];
        let transcript_mutations = if let Some(batch) = prepared {
            checks.extend(batch.checks);
            batch.mutations
        } else {
            Vec::new()
        };
        checks.extend(append.checks);
        checks.push(Check {
            key: fork_key.clone(),
            expected: Expected::Missing,
        });
        let mut mutations = vec![Mutation::Put {
            key: key.clone(),
            value,
        }];
        let old_pending = old.and_then(|prior| prior.pending_tool_result.as_ref());
        let new_pending = state.pending_tool_result.as_ref();
        if old_pending != new_pending {
            if let Some(previous) = old_pending {
                let pending_key = format!(
                    "runtime/tool-result-pending/{}/{}",
                    state.id, previous.operation_id
                )
                .into_bytes();
                let entry = self
                    .owner
                    .store()
                    .get(&pending_key)?
                    .ok_or_else(|| Error::Invalid("pending tool result missing".into()))?;
                checks.push(Check {
                    key: pending_key.clone(),
                    expected: Expected::Version(entry.version),
                });
                mutations.push(Mutation::Delete { key: pending_key });
            }
            if let Some(current) = new_pending {
                let pending_key = format!(
                    "runtime/tool-result-pending/{}/{}",
                    state.id, current.operation_id
                )
                .into_bytes();
                checks.push(Check {
                    key: pending_key.clone(),
                    expected: Expected::Missing,
                });
                mutations.push(Mutation::Put {
                    key: pending_key,
                    value: serde_json::to_vec(current)?,
                });
            }
        }
        mutations.extend(transcript_mutations);
        mutations.extend(append.mutations);
        mutations.push(Mutation::Put {
            key: fork_key,
            value: serde_json::to_vec(&fork_value)?,
        });
        // Publish only newly claimed invocations, in the same commit as the
        // running state and before the tool can report progress.
        if let Some(previous) = old
            && matches!(
                state.phase,
                Phase::ToolRunning {
                    replay_ready: false,
                    ..
                }
            )
            && let Some(attempt) = state.tool_attempt()
            && previous.tool_attempt() != Some(attempt)
        {
            let event = crate::observer::Observer::tool_attempts(self.owner.store(), state.id)
                .prepare_append(&serde_json::json!({
                    "conversation_id": state.id,
                    "attempt": attempt,
                }))?;
            checks.extend(event.checks);
            mutations.extend(event.mutations);
        }
        let mut progress_history = Vec::new();
        let terminal = if state.abort_requested {
            match &state.phase {
                Phase::ModelRunning { request_id, .. }
                | Phase::AbandonedModel { request_id, .. }
                | Phase::FailedModel { request_id, .. } => Some((*request_id, false)),
                _ => None,
            }
        } else {
            None
        }
        .or(match &state.phase {
            Phase::Accounting { request_id, .. } => Some((*request_id, true)),
            Phase::AbandonedModel { request_id, .. } | Phase::FailedModel { request_id, .. } => {
                Some((*request_id, false))
            }
            _ => None,
        });
        if let Some((request_id, complete)) = terminal {
            let snapshot = crate::progress::Snapshot::load(self.owner.store(), request_id)?;
            if snapshot.is_persisted()
                && matches!(snapshot.state(), crate::progress::State::Running(_))
            {
                let prepared = if complete {
                    snapshot.prepare_complete()?
                } else {
                    snapshot.prepare_abort()?
                };
                checks.extend(prepared.checks);
                mutations.extend(prepared.mutations);
                let terminal_state = if complete {
                    crate::progress::State::Completed
                } else if let crate::progress::State::Running(partial) = snapshot.state() {
                    crate::progress::State::Aborted(partial.clone())
                } else {
                    unreachable!()
                };
                progress_history.push(ProgressHistoryChange::Generation {
                    request_id,
                    state: Box::new(terminal_state),
                });
            }
        }
        // Seal the invocation in the same transaction that leaves its running
        // phase, requests abort, or prepares a replay. The attempt ID prevents
        // callbacks from a previous invocation from affecting a replacement.
        if let Some(previous) = old
            && let Phase::ToolRunning {
                replay_ready: false,
                ..
            } = &previous.phase
            && let Some(attempt) = previous.tool_attempt()
        {
            let still_running = matches!(
                &state.phase,
                Phase::ToolRunning {
                    replay_ready: false,
                    ..
                }
            ) && state.tool_attempt() == Some(attempt)
                && !state.abort_requested;
            if !still_running {
                let snapshot =
                    crate::tool_progress::Snapshot::load(self.owner.store(), attempt.attempt_id)?;
                if matches!(snapshot.state(), crate::tool_progress::State::Running(_)) {
                    let complete = !self.recovering
                        && !state.abort_requested
                        && matches!(&state.phase, Phase::ToolFinished { .. });
                    let prepared = if complete {
                        snapshot.prepare_complete()?
                    } else {
                        snapshot.prepare_abort()?
                    };
                    checks.extend(prepared.checks);
                    mutations.extend(prepared.mutations);
                    let terminal = if complete {
                        crate::tool_progress::State::Completed
                    } else if let crate::tool_progress::State::Running(partial) = snapshot.state() {
                        crate::tool_progress::State::Aborted(partial.clone())
                    } else {
                        unreachable!()
                    };
                    let retained_partial = if complete {
                        match snapshot.state() {
                            crate::tool_progress::State::Running(partial) => partial.clone(),
                            _ => unreachable!(),
                        }
                    } else {
                        None
                    };
                    let event = crate::observer::Observer::tool_progress(
                        self.owner.store(),
                        attempt.attempt_id,
                    )
                    .prepare_append(&serde_json::json!({
                        "conversation_id": state.id,
                        "attempt": attempt,
                        "state": terminal,
                        "retained_partial": retained_partial,
                        "partial_is_null": matches!(snapshot.state(), crate::tool_progress::State::Running(Some(serde_json::Value::Null))),
                    }))?;
                    checks.extend(event.checks);
                    mutations.extend(event.mutations);
                    progress_history.push(ProgressHistoryChange::Tool {
                        attempt,
                        state: terminal,
                        partial_is_null: matches!(
                            snapshot.state(),
                            crate::tool_progress::State::Running(Some(serde_json::Value::Null))
                        ),
                    });
                }
            }
        }
        if !progress_history.is_empty() {
            let event = prepare_progress_history(self.owner.store(), state.id, progress_history)?;
            checks.extend(event.checks);
            mutations.extend(event.mutations);
        }
        if !children.is_empty() {
            let current = self.owner.store().get(&key)?.ok_or(Error::NotFound)?;
            let current: Conversation = serde_json::from_slice(&current.value)?;
            if current.abort_requested {
                return Err(Error::Invalid("conversation is aborting".into()));
            }
            let batch = crate::task_store::TaskStore::new(self.owner.store())
                .prepare_owned_tasks(
                    &crate::task::TaskOwner::Conversation(state.id),
                    &key,
                    expected.clone(),
                    &serde_json::to_vec(&persisted)?,
                    children,
                )
                .map_err(|e| Error::Invalid(format!("task batch: {e}")))?;
            // The conversation check and mutation are already part of this save.
            checks.extend(batch.checks.into_iter().skip(1));
            mutations.extend(batch.mutations.into_iter().skip(1));
        }
        for task in abort_tasks {
            let key = crate::task_store::key(task.task.id());
            checks.push(Check {
                key: key.clone(),
                expected: Expected::Version(task.version),
            });
            mutations.push(Mutation::Put {
                key,
                value: serde_json::to_vec(&task.task)?,
            });
        }
        if let Some(wake) = wake {
            checks.extend(wake.checks.clone());
            mutations.extend(wake.mutations.clone());
        }
        Ok(PreparedConversationSave {
            owner: self.owner.clone(),
            checks,
            mutations,
        })
    }
    /// Prepare a plain conversation save for composition. Callers must hold the
    /// task gate when composing task ownership changes with these saves.
    pub(crate) fn prepare_save(
        &self,
        state: &Conversation,
        expected: Expected,
    ) -> Result<PreparedConversationSave, Error> {
        self.prepare_conversation_save(state, expected, None, &[], &[])
    }

    /// Prepare independent saves against one store snapshot. Any overlap is
    /// rejected, and callers must discard the whole batch after a conflict.
    pub(crate) fn prepare_saves(
        &self,
        saves: &[(&Conversation, Expected)],
    ) -> Result<PreparedConversationSave, Error> {
        let mut batch = PreparedConversationSave {
            owner: self.owner.clone(),
            checks: Vec::new(),
            mutations: Vec::new(),
        };
        for (state, expected) in saves {
            batch.extend(self.prepare_save(state, expected.clone())?)?;
        }
        Ok(batch)
    }

    pub(crate) fn commit_conversation_save(
        &mut self,
        batch: PreparedConversationSave,
    ) -> Result<(), Error> {
        self.commit_conversation_save_inner(batch, None)
    }

    fn commit_conversation_save_inner(
        &mut self,
        batch: PreparedConversationSave,
        prefix_fence: Option<u64>,
    ) -> Result<(), Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        if !Arc::ptr_eq(&self.owner, &batch.owner) {
            return Err(Error::Invalid(
                "prepared save belongs to a different store".into(),
            ));
        }
        let store = self.owner.store();
        let result = match prefix_fence {
            Some(boundary) => store.commit_with_prefix_put_fence(
                &batch.checks,
                &batch.mutations,
                &[crate::task_store::PREFIX, crate::ownership_edge::PREFIX],
                boundary,
            ),
            None => store.commit(&batch.checks, &batch.mutations),
        };
        match result {
            Ok(()) => Ok(()),
            Err(e @ durable_store::Error::Conflict { .. }) => Err(e.into()),
            Err(e) => {
                self.failed = true;
                self.storage_failed.store(true, Ordering::Release);
                Err(e.into())
            }
        }
    }
    /// Persist delivery modes, including while a run is active. Each boundary reads the latest values.
    pub fn set_queue_modes(&mut self, id: Uuid, modes: QueueModes) -> Result<(), Error> {
        loop {
            let (mut state, version) = self.load(id)?;
            state.queue_modes = modes;
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let result = self.save(&state, Expected::Version(version));
            self.merge_admissions = merge;
            match result {
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                other => return other,
            }
        }
    }
    /// Persist the policy used at the next failed-run boundary.
    pub fn set_after_failed_run(&mut self, id: Uuid, policy: AfterFailedRun) -> Result<(), Error> {
        loop {
            let (mut state, version) = self.load(id)?;
            state.after_failed_run = policy;
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let result = self.save(&state, Expected::Version(version));
            self.merge_admissions = merge;
            match result {
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                other => return other,
            }
        }
    }
    pub fn create_conversation(&mut self, config: Config) -> Result<Uuid, Error> {
        self.create_conversation_with_id(Uuid::new_v4(), config)
    }
    pub(crate) fn create_conversation_with_id(
        &mut self,
        id: Uuid,
        config: Config,
    ) -> Result<Uuid, Error> {
        if config.model.is_empty() || config.max_output_tokens == 0 {
            return Err(Error::Invalid("model and output limit required".into()));
        }
        let state = Self::empty_conversation(id, config);
        self.save(&state, Expected::Missing)?;
        Ok(id)
    }
    pub(crate) fn empty_conversation(id: Uuid, config: Config) -> Conversation {
        Conversation {
            id,
            config,
            queue_modes: QueueModes::default(),
            after_failed_run: AfterFailedRun::default(),
            extensions: vec![],
            provider_adapter: None,
            allowed_tools: None,
            model_tools: None,
            tool_filters: ToolFilters::default(),
            tool_results: Vec::new(),
            pending_tool_result: None,
            messages: vec![],
            content_blocks: Default::default(),
            block_reset_proof: None,
            block_fork_cut: None,
            context_generation: Some(Uuid::new_v4()),
            occurrence_epoch_proven: true,
            transcript: None,
            submissions: vec![],
            manual_compactions: vec![],
            active_manual_compaction: None,
            aborted_generations: vec![],
            resets: vec![],
            pending_resets: vec![],
            pending_reset_blocks: Default::default(),
            fitted_reset_receipts: Default::default(),
            pending_independent_summary: None,
            application_writes: vec![],
            tool_states: HashMap::new(),
            round_terminates: Vec::new(),
            round_handoffs: Vec::new(),
            round_added_tools: Vec::new(),
            input_modes: HashMap::new(),
            answer_ids: HashMap::new(),
            auto_compaction_attempted: false,
            retry_state: RetryState::default(),
            overflow_compaction_attempted: false,
            compaction_return_turns: None,
            active: None,
            run_failure: None,
            failure_continuation_inputs: Vec::new(),
            tool_attempt: None,
            abort_requested: false,
            nested_resume: None,
            phase: Phase::Idle,
        }
    }
    pub(crate) fn subagent_seed(
        &self,
        parent: Uuid,
        child: Uuid,
        config: Config,
    ) -> Result<Conversation, Error> {
        let (mut state, _) = self.load(parent)?;
        if !state.content_blocks.is_empty() {
            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
        }
        if !matches!(state.phase, Phase::Idle | Phase::FailedIdle)
            || state.active.is_some()
            || !state.pending_resets.is_empty()
            || state.pending_independent_summary.is_some()
            || state
                .submissions
                .iter()
                .any(|s| matches!(s.status, Status::Queued | Status::Running))
        {
            return Err(Error::Invalid(
                "subagent history fork requires a settled parent".into(),
            ));
        }
        state.tool_attempt = None;
        state.id = child;
        state.context_generation = Some(Uuid::new_v4());
        state
            .extensions
            .retain(|name| !name.starts_with(crate::subagent_management::EXTENSION_PREFIX));
        state.config = config;
        state.submissions.clear();
        state.manual_compactions.clear();
        state.active_manual_compaction = None;
        state.application_writes.retain(|write| write.placed);
        for write in &mut state.application_writes {
            write.request_id = None;
        }
        state.aborted_generations.clear();
        state.resets.clear();
        state.pending_resets.clear();
        state.pending_reset_blocks.clear();
        state.fitted_reset_receipts.clear();
        state.input_modes.clear();
        state.answer_ids.clear();
        state.auto_compaction_attempted = false;
        state.retry_state = RetryState::default();
        state.overflow_compaction_attempted = false;
        state.compaction_return_turns = None;
        state.phase = Phase::Idle;
        Ok(state)
    }
    pub(crate) fn create_subagent_from_seed(&mut self, seed: &Conversation) -> Result<(), Error> {
        self.save(seed, Expected::Missing)
    }
    /// Fork the current settled conversation. Queued inputs and request IDs
    /// are not inherited. Use `fork_at` for historical observation cuts.
    pub fn fork(&mut self, id: Uuid) -> Result<Uuid, Error> {
        self.fork_with_load_hook(id, || {})
    }

    fn fork_with_load_hook<F: FnMut()>(
        &mut self,
        id: Uuid,
        mut after_load: F,
    ) -> Result<Uuid, Error> {
        let mut attempts = 0;
        let (mut state, revision) = loop {
            let (state, version) = self.load(id)?;
            after_load();
            let revision = self.owner.store().document_snapshot()?;
            // The parent must remain unchanged through the document snapshot.
            // Otherwise the two reads may straddle an atomic admission commit.
            if self
                .owner
                .store()
                .get(&Self::key(id))?
                .is_some_and(|entry| entry.version == version)
            {
                break (state, revision);
            }
            attempts += 1;
            if attempts == FORK_DOCUMENT_ATTEMPTS {
                return Err(durable_store::Error::Conflict { key: Self::key(id) }.into());
            }
        };
        if !matches!(state.phase, Phase::Idle | Phase::FailedIdle)
            || state.active.is_some()
            || !state.pending_resets.is_empty()
            || state.pending_independent_summary.is_some()
        {
            return Err(Error::Invalid(
                "fork requires a settled conversation".into(),
            ));
        }
        if !state.content_blocks.is_empty() {
            crate::fork::validate_context(&state.messages)?;
        }
        state.tool_attempt = None;
        state.id = Uuid::new_v4();
        state.context_generation = Some(Uuid::new_v4());
        state.active = None;
        state.abort_requested = false;
        state.submissions.clear();
        state.manual_compactions.clear();
        state.active_manual_compaction = None;
        state.application_writes.retain(|write| write.placed);
        for write in &mut state.application_writes {
            write.request_id = None;
        }
        state.aborted_generations.clear();
        state.input_modes.clear();
        state.answer_ids.clear();
        state.auto_compaction_attempted = false;
        state.retry_state = RetryState::default();
        state.overflow_compaction_attempted = false;
        state.compaction_return_turns = None;
        state.phase = Phase::Idle;
        retry_document_fork(|| {
            let batch = crate::document::Documents::new(self.owner.store())
                .fork_scope(id, state.id, revision)?;
            self.save_composed_locked(&state, Expected::Missing, None, &[], &[], Some(&batch))
        })?;
        Ok(state.id)
    }
    /// Fork a committed observation cut, including cuts during active work.
    /// Incomplete tool batches and snapshots without historical state fail closed.
    pub fn fork_at(&mut self, id: Uuid, cursor: crate::observer::Cursor) -> Result<Uuid, Error> {
        self.fork_at_cut(id, cursor, None)
    }
    fn fork_at_cut(
        &mut self,
        id: Uuid,
        cursor: crate::observer::Cursor,
        prefix: Option<(usize, usize)>,
    ) -> Result<Uuid, Error> {
        self.fork_at_selected(id, cursor, prefix, None)
    }

    fn fork_at_selected(
        &mut self,
        id: Uuid,
        cursor: crate::observer::Cursor,
        prefix: Option<(usize, usize)>,
        application_cut: Option<(u64, Uuid, Vec<crate::application_entry::ApplicationEntry>)>,
    ) -> Result<Uuid, Error> {
        self.load(id)?;
        if cursor.0 == 0 {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let key = format!("runtime/fork/{}/{:020}", id, cursor.0);
        let entry = self
            .owner
            .store()
            .get(key.as_bytes())?
            .ok_or(crate::fork::Error::InvalidCursor)?;
        let revision = match &application_cut {
            Some((revision, _, _)) => *revision,
            None => self
                .owner
                .store()
                .document_cut(key.as_bytes())?
                .ok_or(crate::fork::Error::InvalidSnapshot)?,
        };
        if prefix.is_some() {
            let previous = cursor
                .0
                .checked_sub(1)
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let previous_key = format!("runtime/fork/{}/{:020}", id, previous);
            if self.owner.store().document_cut(previous_key.as_bytes())? != Some(revision) {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
        }
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value)?;
        let transcript = value
            .get("_transcript")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        self.expand_event(id, &mut value)?;
        let mut blocks: std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks> =
            value
                .as_object_mut()
                .ok_or(crate::fork::Error::InvalidSnapshot)?
                .remove("content_blocks")
                .map(serde_json::from_value)
                .transpose()?
                .unwrap_or_default();
        let block_fork_cut = prefix.filter(|_| !blocks.is_empty()).map(|(len, _)| len);
        if let Some((len, expected_len)) = prefix {
            let messages = value
                .get_mut("messages")
                .and_then(serde_json::Value::as_array_mut)
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            if messages.len() != expected_len || len > messages.len() {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            messages.truncate(len);
            if block_fork_cut.is_some() {
                blocks.retain(|index, _| *index < len);
            }
        }
        let record = crate::observer::Record { cursor, value };
        let point = crate::fork::extract_cut(id, &record)?;
        let state = Conversation {
            id: Uuid::new_v4(),
            config: point.config,
            queue_modes: point
                .documents
                .get("pi-rs/runtime/queue-modes")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            after_failed_run: point
                .documents
                .get("pi-rs/runtime/after-failed-run")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            extensions: point
                .documents
                .get("pi-rs/runtime/extensions")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            provider_adapter: point
                .documents
                .get("pi-rs/runtime/provider-adapter")
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()?
                .unwrap_or_default(),
            allowed_tools: point
                .documents
                .get("pi-rs/runtime/allowed-tools")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            model_tools: point
                .documents
                .get("pi-rs/runtime/model-tools")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            tool_filters: point
                .documents
                .get("pi-rs/runtime/tool-filters")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            tool_results: point
                .documents
                .get("pi-rs/runtime/tool-results")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default(),
            pending_tool_result: None,
            messages: point.messages,
            content_blocks: blocks,
            block_reset_proof: None,
            block_fork_cut,
            context_generation: Some(Uuid::new_v4()),
            occurrence_epoch_proven: point
                .documents
                .get("pi-rs/runtime/occurrence-epoch-proven")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or(false),
            transcript: if prefix.is_some() && block_fork_cut.is_none() {
                None
            } else {
                transcript
            },
            submissions: vec![],
            manual_compactions: vec![],
            active_manual_compaction: None,
            aborted_generations: vec![],
            resets: serde_json::from_value(
                point
                    .documents
                    .get("pi-rs/runtime/resets")
                    .ok_or(crate::fork::Error::InvalidSnapshot)?
                    .clone(),
            )?,
            pending_resets: vec![],
            pending_reset_blocks: Default::default(),
            fitted_reset_receipts: Default::default(),
            pending_independent_summary: None,
            application_writes: point
                .documents
                .get("pi-rs/runtime/application-writes")
                .map(|value| serde_json::from_value::<Vec<ApplicationWrite>>(value.clone()))
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .filter(|write| write.placed)
                .map(|mut write| {
                    write.request_id = None;
                    write
                })
                .collect(),
            tool_states: point.tool_states,
            round_terminates: Vec::new(),
            round_handoffs: Vec::new(),
            round_added_tools: Vec::new(),
            input_modes: HashMap::new(),
            answer_ids: HashMap::new(),
            auto_compaction_attempted: false,
            retry_state: RetryState::default(),
            overflow_compaction_attempted: false,
            compaction_return_turns: None,
            active: None,
            run_failure: None,
            failure_continuation_inputs: Vec::new(),
            tool_attempt: None,
            abort_requested: false,
            nested_resume: None,
            phase: Phase::Idle,
        };
        retry_document_fork(|| {
            let current_parent = application_cut
                .as_ref()
                .map_or(id, |(_, parent, _)| *parent);
            let mut batch = crate::document::Documents::new(self.owner.store()).fork_scope_from(
                id,
                current_parent,
                state.id,
                revision,
            )?;
            if let Some((_, _, entries)) = &application_cut {
                let target = crate::application_entry::key(state.id);
                let mutation = batch
                    .mutations
                    .iter_mut()
                    .find(|mutation| {
                        matches!(mutation,
                    durable_store::Mutation::Put { key, .. } if *key == target)
                    })
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                if let durable_store::Mutation::Put { value, .. } = mutation {
                    *value = serde_json::to_vec(entries)?;
                }
            }
            self.save_composed_locked(&state, Expected::Missing, None, &[], &[], Some(&batch))
        })?;
        Ok(state.id)
    }
    /// Locate a visible application entry in at most 256 recorded log versions.
    /// A foreign prefix is accepted only when installed atomically with the
    /// conversation's first tracked runtime snapshot; later appends must be local.
    fn application_entry_cut_unverified(
        &self,
        id: Uuid,
        entry_id: Uuid,
    ) -> Result<ApplicationEntryCut, Error> {
        let key = crate::application_entry::key(id);
        let versions = self.owner.store().application_history(&key, 257)?;
        let mut previous: Vec<crate::application_entry::ApplicationEntry> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut inherited = None;
        for (revision, sequence, snapshot) in versions.into_iter().take(256) {
            let sequence = sequence.ok_or(crate::fork::Error::InvalidSnapshot)?;
            let entries: Vec<crate::application_entry::ApplicationEntry> =
                serde_json::from_slice(&snapshot.value)?;
            if !entries.starts_with(&previous) || entries.len() <= previous.len() {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            let added = &entries[previous.len()..];
            let foreign = added.iter().any(|entry| entry.conversation_id != id);
            if foreign {
                if !previous.is_empty() || added.iter().any(|entry| entry.conversation_id == id) {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                let runtime_key = format!("runtime/conversation/{id}");
                let prefix = format!("runtime/fork/{id}/");
                let first = self
                    .owner
                    .store()
                    .first_commit_for_key(runtime_key.as_bytes())?;
                let snapshot = self.owner.store().runtime_snapshot_at(
                    runtime_key.as_bytes(),
                    prefix.as_bytes(),
                    sequence,
                )?;
                if first != Some(sequence) || snapshot.as_ref().map(|s| s.0) != Some(sequence) {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                let lineage_key = crate::application_entry::lineage_key(id);
                let lineage_entry = self
                    .owner
                    .store()
                    .get(&lineage_key)?
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                if self
                    .owner
                    .store()
                    .commit_for_version(&lineage_key, lineage_entry.version)?
                    != Some(sequence)
                {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                let lineage: crate::application_entry::Lineage =
                    serde_json::from_slice(&lineage_entry.value)?;
                if lineage.parent.is_nil()
                    || lineage.parent == id
                    || lineage.parent_revision >= revision
                {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                let parent_key = crate::application_entry::key(lineage.parent);
                let parent_entry = self
                    .owner
                    .store()
                    .application_entries_at(&parent_key, lineage.parent_revision)?
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                let parent_entries: Vec<crate::application_entry::ApplicationEntry> =
                    serde_json::from_slice(&parent_entry.value)?;
                if !parent_entries.starts_with(&entries) {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                inherited = Some((sequence, lineage.parent, entries.clone()));
            }
            for entry in added {
                if entry.id.is_nil() || entry.conversation_id.is_nil() || !seen.insert(entry.id) {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
            }
            if let Some(index) = added.iter().position(|entry| entry.id == entry_id) {
                let index = previous.len() + index;
                let visible = entries[..=index].to_vec();
                if let Some((_, _, prefix)) = &mut inherited {
                    prefix.truncate(visible.len());
                }
                return Ok((revision, sequence, visible, inherited));
            }
            previous = entries;
        }
        Err(crate::fork::Error::InvalidCursor.into())
    }

    // Every inherited prefix, including one preceding a local selection, must
    // match its recorded immediate parent. Strictly decreasing commit sequences also
    // make forged cycles fail closed.
    fn application_entry_cut(
        &self,
        id: Uuid,
        entry_id: Uuid,
    ) -> Result<(u64, u64, Vec<crate::application_entry::ApplicationEntry>), Error> {
        let (revision, sequence, visible, mut inherited) =
            self.application_entry_cut_unverified(id, entry_id)?;
        for _ in 0..256 {
            let Some((installed, parent, prefix)) = inherited else {
                return Ok((revision, sequence, visible));
            };
            let last = prefix.last().ok_or(crate::fork::Error::InvalidSnapshot)?;
            let (_, origin_sequence, origin_prefix, origin_inherited) =
                self.application_entry_cut_unverified(parent, last.id)?;
            if origin_sequence >= installed || origin_prefix != prefix {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            inherited = origin_inherited;
        }
        Err(crate::fork::Error::InvalidSnapshot.into())
    }

    /// Fork a visible task application entry, including a proven inherited
    /// prefix. Historical state comes from its owner, current-policy documents
    /// from the immediate parent. Unprovable histories fail closed.
    pub fn fork_at_application_entry(&mut self, id: Uuid, entry_id: Uuid) -> Result<Uuid, Error> {
        self.load(id)?;
        if entry_id.is_nil() || self.owner.store().legacy_version_boundary()? != 0 {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let (mut revision, mut sequence, visible) = self.application_entry_cut(id, entry_id)?;
        let origin = visible
            .last()
            .ok_or(crate::fork::Error::InvalidSnapshot)?
            .conversation_id;
        if origin != id {
            let (origin_revision, origin_sequence, origin_prefix) =
                self.application_entry_cut(origin, entry_id)?;
            if origin_prefix != visible
                || origin_prefix
                    .last()
                    .is_none_or(|entry| entry.conversation_id != origin)
            {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            revision = origin_revision;
            sequence = origin_sequence;
        }
        let runtime_key = format!("runtime/conversation/{origin}");
        let prefix = format!("runtime/fork/{origin}/");
        let (runtime_sequence, snapshot_key) = self
            .owner
            .store()
            .runtime_snapshot_at(runtime_key.as_bytes(), prefix.as_bytes(), sequence)?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        if runtime_sequence > sequence {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let suffix = snapshot_key
            .strip_prefix(prefix.as_bytes())
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let cursor = std::str::from_utf8(suffix)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        if runtime_sequence == sequence {
            let proof_key = crate::application_entry::ordered_cut_key(origin, cursor);
            let proof_record = self
                .owner
                .store()
                .get(&proof_key)?
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            if self
                .owner
                .store()
                .commit_for_version(&proof_key, proof_record.version)?
                != Some(sequence)
            {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            let proof: crate::application_entry::OrderedCut =
                serde_json::from_slice(&proof_record.value)?;
            let full = self
                .owner
                .store()
                .application_entries_at(&crate::application_entry::key(origin), revision)?
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let full: Vec<crate::application_entry::ApplicationEntry> =
                serde_json::from_slice(&full.value)?;
            let previous_seq = self
                .owner
                .store()
                .commit_for_version(runtime_key.as_bytes(), proof.previous_runtime_version)?
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let immediately_before = self
                .owner
                .store()
                .runtime_snapshot_at(runtime_key.as_bytes(), prefix.as_bytes(), sequence - 1)?
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let previous_entries = self
                .owner
                .store()
                .application_entries_at(
                    &crate::application_entry::key(origin),
                    revision
                        .checked_sub(1)
                        .ok_or(crate::fork::Error::InvalidSnapshot)?,
                )?
                .map(|entry| {
                    serde_json::from_slice::<Vec<crate::application_entry::ApplicationEntry>>(
                        &entry.value,
                    )
                })
                .transpose()?
                .unwrap_or_default();
            let (_, prior_key) = self
                .owner
                .store()
                .runtime_snapshot_at(runtime_key.as_bytes(), prefix.as_bytes(), previous_seq)?
                .filter(|(s, _)| *s == previous_seq)
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let prior = self
                .owner
                .store()
                .get(&prior_key)?
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let current = self
                .owner
                .store()
                .get(&snapshot_key)?
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let snapshots_match = match proof.version {
                1 => prior.value == current.value,
                2 => {
                    let mut before: serde_json::Value = serde_json::from_slice(&prior.value)?;
                    let mut after: serde_json::Value = serde_json::from_slice(&current.value)?;
                    let before_config = before.as_object_mut().and_then(|v| v.remove("config"));
                    let after_config = after.as_object_mut().and_then(|v| v.remove("config"));
                    let valid = |value: Option<serde_json::Value>| {
                        value
                            .and_then(|v| serde_json::from_value::<Config>(v).ok())
                            .is_some_and(|c| !c.model.is_empty() && c.max_output_tokens > 0)
                    };
                    valid(before_config) && valid(after_config) && before == after
                }
                _ => false,
            };
            if !snapshots_match
                || previous_seq >= sequence
                || immediately_before.0 != previous_seq
                || proof.first_index != previous_entries.len()
                || !full.starts_with(&previous_entries)
                || proof.first_index >= proof.checkpoint_index
                || proof.checkpoint_index > full.len()
                || full.get(proof.first_index..).is_none_or(|tail| {
                    tail.iter().map(|e| e.id).collect::<Vec<_>>() != proof.entry_ids
                })
                || !proof.entry_ids.contains(&entry_id)
            {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
        }
        self.fork_at_selected(
            origin,
            crate::observer::Cursor(cursor),
            None,
            Some((revision, id, visible)),
        )
    }

    /// Fork at the commit that first exposed this complete message batch.
    /// Interior positions require proof of unchanged fork state or a tool-free
    /// append changing only config; incomplete tool transcripts are rejected.
    pub fn fork_at_position(&mut self, id: Uuid, position: ViewCursor) -> Result<Uuid, Error> {
        self.load(id)?;
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let page = transcript.read_with_blocks(&position.cursor, std::num::NonZeroUsize::MIN)?;
        let record = page
            .records
            .first()
            .ok_or(crate::fork::Error::InvalidCursor)?;
        let change: TranscriptChange = serde_json::from_value(record.value.clone())?;
        if matches!(&change, TranscriptChange::ForkCut { .. }) {
            let count = position
                .remaining
                .ok_or(crate::fork::Error::InvalidCursor)?;
            let occurrences = self.resolved_occurrences(id, position.cursor.clone())?;
            let (message, history) = occurrences
                .get(
                    count
                        .checked_sub(1)
                        .ok_or(crate::fork::Error::InvalidCursor)?,
                )
                .ok_or(crate::fork::Error::InvalidCursor)?;
            let history = history
                .as_ref()
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let original = self.original_occurrence_position(history, message)?;
            return self.fork_at_position(history.origin, original);
        }
        let compacted_metadata = match &change {
            TranscriptChange::ReplaceCompacted {
                commit,
                origin,
                system_end,
                ..
            } => {
                let occurrences = self.resolved_occurrences(id, position.cursor.clone())?;
                Some((*commit, *origin, *system_end, occurrences))
            }
            _ => None,
        };
        let stable_prefix = match &change {
            TranscriptChange::AppendV3 { stable_prefix, .. }
            | TranscriptChange::AppendV4 { stable_prefix, .. } => Some(*stable_prefix),
            _ => None,
        };
        let (messages, _, metadata) = change.parts()?;
        let (commit, origin) = metadata
            .map(|(_, commit, origin)| (commit, origin))
            .or_else(|| {
                compacted_metadata
                    .as_ref()
                    .map(|(commit, origin, _, _)| (*commit, *origin))
            })
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        if origin != id {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        self.authenticate_fork_anchor(origin, commit)?;
        let count = position
            .remaining
            .ok_or(crate::fork::Error::InvalidCursor)?;
        if messages.is_empty() || count == 0 || count > messages.len() {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let observer = crate::observer::Observer::new(self.owner.store(), id);
        let after = commit
            .checked_sub(1)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let page = observer.read(crate::observer::Cursor(after), std::num::NonZeroUsize::MIN)?;
        let record = page
            .records
            .first()
            .filter(|record| record.cursor.0 == commit)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let reference: Option<crate::transcript::Cursor> = serde_json::from_value(
            record
                .value
                .get("_transcript")
                .cloned()
                .ok_or(crate::fork::Error::InvalidSnapshot)?,
        )?;
        if reference.as_ref() != Some(&position.cursor) {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let key = format!("runtime/fork/{}/{:020}", id, record.cursor.0);
        let entry = self
            .owner
            .store()
            .get(key.as_bytes())?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let value: serde_json::Value = serde_json::from_slice(&entry.value)?;
        let captured: Option<crate::transcript::Cursor> = serde_json::from_value(
            value
                .get("_transcript")
                .cloned()
                .ok_or(crate::fork::Error::InvalidSnapshot)?,
        )?;
        if captured.as_ref() != Some(&position.cursor) {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let documents = value
            .get("documents")
            .and_then(serde_json::Value::as_object)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        for name in [
            "pi-rs/runtime/queue-modes",
            "pi-rs/runtime/extensions",
            "pi-rs/runtime/provider-adapter",
            "pi-rs/runtime/allowed-tools",
            "pi-rs/runtime/resets",
            "pi-rs/runtime/application-writes",
        ] {
            if !documents.contains_key(name) {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
        }
        if let Some((_, _, summary_index, occurrences)) = &compacted_metadata
            && count - 1 != *summary_index
        {
            let (message, history) = occurrences
                .get(count - 1)
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let history = history
                .as_ref()
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            let original = self.original_occurrence_position(history, message)?;
            return self.fork_at_position(history.origin, original);
        }
        if count == messages.len() {
            return self.fork_at(id, record.cursor);
        }
        let prefix = stable_prefix.ok_or(crate::fork::Error::InvalidSnapshot)?;
        let total = prefix
            .checked_add(count)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let expected_len = prefix
            .checked_add(messages.len())
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        self.fork_at_cut(id, record.cursor, Some((total, expected_len)))
    }

    fn original_occurrence_position(
        &self,
        history: &MessageHistory,
        expected: &Message,
    ) -> Result<ViewCursor, Error> {
        let observer = crate::observer::Observer::new(self.owner.store(), history.origin);
        let after = history
            .commit
            .checked_sub(1)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let page = observer.read(crate::observer::Cursor(after), std::num::NonZeroUsize::MIN)?;
        let record = page
            .records
            .first()
            .filter(|record| record.cursor.0 == history.commit)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let cursor: Option<crate::transcript::Cursor> = serde_json::from_value(
            record
                .value
                .get("_transcript")
                .cloned()
                .ok_or(crate::fork::Error::InvalidSnapshot)?,
        )?;
        let cursor = cursor.ok_or(crate::fork::Error::InvalidSnapshot)?;
        let transcript = crate::transcript::Transcript::new(self.owner.store(), history.origin);
        let batch = transcript.read_with_blocks(&cursor, std::num::NonZeroUsize::MIN)?;
        let change: TranscriptChange = serde_json::from_value(
            batch
                .records
                .first()
                .ok_or(crate::fork::Error::InvalidSnapshot)?
                .value
                .clone(),
        )?;
        let index = match &change {
            TranscriptChange::ReplaceCompacted {
                messages,
                system_end,
                summary_id,
                commit,
                origin,
                ..
            } => {
                self.resolved_occurrences(history.origin, cursor.clone())?;
                if *summary_id != history.id
                    || *commit != history.commit
                    || *origin != history.origin
                    || messages.get(*system_end) != Some(expected)
                    || history.commit_boundary != (*system_end + 1 == messages.len())
                {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                *system_end
            }
            _ => {
                let (messages, _, metadata) = change.parts()?;
                let (ids, commit, origin) = metadata.ok_or(crate::fork::Error::InvalidSnapshot)?;
                let index = ids
                    .iter()
                    .position(|id| *id == history.id)
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                if commit != history.commit
                    || origin != history.origin
                    || messages.get(index) != Some(expected)
                    || history.commit_boundary != (index + 1 == messages.len())
                {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                index
            }
        };
        Ok(ViewCursor {
            cursor,
            remaining: Some(index + 1),
        })
    }

    /// Fork at the exact committed boundary of a message occurrence.
    /// Interior messages require a proven safe append. Inherited entries must
    /// be visible in the captured shared chain and have an origin snapshot.
    /// Local retained observations are scanned across resets.
    pub fn fork_by_entry_id(&mut self, id: Uuid, entry_id: Uuid) -> Result<Uuid, Error> {
        self.load(id)?;
        if entry_id.is_nil() {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let observer = crate::observer::Observer::new(self.owner.store(), id);
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut cursor = crate::observer::Cursor(0);
        let mut last_reference = None;
        let mut local_match = None;
        loop {
            let page = observer.read(cursor, std::num::NonZeroUsize::new(128).unwrap())?;
            for record in &page.records {
                let Some(value) = record.value.get("_transcript") else {
                    continue;
                };
                let reference: Option<crate::transcript::Cursor> =
                    serde_json::from_value(value.clone())?;
                let Some(reference) = reference else { continue };
                if last_reference.as_ref() == Some(&reference) {
                    continue;
                }
                last_reference = Some(reference.clone());
                let batch = transcript.read_with_blocks(&reference, std::num::NonZeroUsize::MIN)?;
                let Some(batch) = batch.records.first() else {
                    continue;
                };
                let change = serde_json::from_value::<TranscriptChange>(batch.value.clone())?;
                if let TranscriptChange::ReplaceCompacted {
                    system_end,
                    summary_id,
                    commit,
                    origin,
                    histories,
                    ..
                } = &change
                {
                    if histories
                        .iter()
                        .flatten()
                        .any(|history| history.id == entry_id)
                    {
                        self.resolved_occurrences(id, reference.clone())?;
                    }
                    if *origin == id && *summary_id == entry_id {
                        if local_match.is_some() || *commit != record.cursor.0 {
                            return Err(crate::fork::Error::InvalidSnapshot.into());
                        }
                        local_match = Some(ViewCursor {
                            cursor: reference.clone(),
                            remaining: Some(*system_end + 1),
                        });
                    }
                    continue;
                }
                let (_, _, metadata) = change.parts()?;
                let Some((ids, commit, origin)) = metadata else {
                    continue;
                };
                if origin == id {
                    let mut matches = ids
                        .iter()
                        .enumerate()
                        .filter(|(_, candidate)| **candidate == entry_id);
                    if let Some((index, _)) = matches.next() {
                        if matches.next().is_some()
                            || local_match.is_some()
                            || commit != record.cursor.0
                        {
                            return Err(crate::fork::Error::InvalidSnapshot.into());
                        }
                        local_match = Some(ViewCursor {
                            cursor: reference,
                            remaining: Some(index + 1),
                        });
                    }
                }
            }
            if !page.has_more {
                break;
            }
            cursor = page.next;
        }
        // Inherited identities are valid only while their immutable batch is
        // reachable from the child's captured head, up to its replacement base.
        let (state, _) = self.load(id)?;
        let mut next_position = state.transcript;
        let mut found = None;
        while let Some(current) = next_position {
            let position = current;
            let page = transcript.read_with_blocks(&position, std::num::NonZeroUsize::MIN)?;
            let Some(batch) = page.records.first() else {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            };
            let change: TranscriptChange = serde_json::from_value(batch.value.clone())?;
            if matches!(
                &change,
                TranscriptChange::ReplaceCompacted { .. } | TranscriptChange::ForkCut { .. }
            ) {
                let active = self.resolved_occurrences(id, position.clone())?;
                if let Some((message, history)) = active
                    .iter()
                    .find(|(_, history)| history.as_ref().is_some_and(|h| h.id == entry_id))
                {
                    let history = history.as_ref().unwrap();
                    let original = self.original_occurrence_position(history, message)?;
                    if history.origin == id {
                        if local_match.as_ref().is_none_or(|local| {
                            local.cursor != original.cursor || local.remaining != original.remaining
                        }) {
                            return Err(crate::fork::Error::InvalidSnapshot.into());
                        }
                    } else if local_match.is_some() || found.is_some() {
                        return Err(crate::fork::Error::InvalidSnapshot.into());
                    }
                    return self.fork_at_position(history.origin, original);
                }
                break;
            }
            let (_, base, metadata) = change.parts()?;
            let Some((ids, _commit, origin)) = metadata else {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            };
            let mut matches = ids
                .iter()
                .enumerate()
                .filter(|(_, candidate)| **candidate == entry_id);
            if let Some((index, _)) = matches.next() {
                if matches.next().is_some() {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                if origin == id {
                    if local_match.as_ref().is_none_or(|local| {
                        local.cursor != position || local.remaining != Some(index + 1)
                    }) {
                        return Err(crate::fork::Error::InvalidSnapshot.into());
                    }
                } else {
                    if local_match.is_some() || found.is_some() {
                        return Err(crate::fork::Error::InvalidSnapshot.into());
                    }
                    // Rebind only the cursor's owner; fork_at_position independently
                    // proves the link belongs to the origin's recorded observation.
                    let mut cursor = serde_json::to_value(&position)?;
                    cursor["transcript"] = serde_json::to_value(origin)?;
                    found = Some((
                        origin,
                        ViewCursor {
                            cursor: serde_json::from_value(cursor)?,
                            remaining: Some(index + 1),
                        },
                    ));
                }
            }
            if base {
                break;
            }
            if !page.has_more {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            next_position = Some(page.next);
        }
        match (local_match, found) {
            (Some(position), None) => self.fork_at_position(id, position),
            (None, Some((origin, position))) => self.fork_at_position(origin, position),
            _ => Err(crate::fork::Error::InvalidCursor.into()),
        }
    }
    pub fn conversation(&self, id: Uuid) -> Result<Conversation, Error> {
        self.load(id).map(|x| {
            let mut state = x.0;
            hide_pending_tool_output(&mut state);
            state
        })
    }
    /// Read the latest committed application document under this store owner.
    pub fn document(
        &self,
        address: &crate::document::Address,
    ) -> Result<Option<crate::document::Document>, Error> {
        Ok(crate::document::Documents::new(self.subagent_store()?).get(address)?)
    }
    /// Read a known document address at a committed conversation observation.
    /// Legacy cuts without a recorded document boundary fail closed.
    pub fn document_at_cut(
        &self,
        id: Uuid,
        cursor: crate::observer::Cursor,
        address: &crate::document::Address,
    ) -> Result<Option<crate::document::Document>, Error> {
        self.subagent_store()?;
        if cursor.0 == 0 {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let key = format!("runtime/fork/{}/{:020}", id, cursor.0);
        if self.owner.store().get(key.as_bytes())?.is_none() {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let revision = self
            .owner
            .store()
            .document_cut(key.as_bytes())?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        Ok(crate::document::Documents::new(self.owner.store()).get_at(address, revision)?)
    }
    /// Capture current state and at most `limit` newest messages. The live
    /// checkpoint and conversation record are read consistently by retrying if
    /// the latter changes while the checkpoint is read.
    pub fn view(&self, id: Uuid, limit: std::num::NonZeroUsize) -> Result<ConversationView, Error> {
        self.subagent_store()?;
        let store = self.owner.store();
        let (mut state, live, tool_progress) = loop {
            let entry = store.get(&Self::key(id))?.ok_or(Error::NotFound)?;
            let state: Conversation = serde_json::from_slice(&entry.value)?;
            let request = match &state.phase {
                Phase::ModelRunning { request_id, .. }
                | Phase::CompactionRunning { request_id, .. } => Some(*request_id),
                _ => None,
            };
            let live = request
                .map(|request| crate::progress::Snapshot::load(store, request))
                .transpose()?
                .and_then(|snapshot| snapshot.is_persisted().then(|| snapshot.state().clone()));
            let tool_progress = state
                .tool_attempt()
                .map(|attempt| crate::tool_progress::Snapshot::load(store, attempt.attempt_id))
                .transpose()?
                .and_then(|snapshot| snapshot.is_persisted().then(|| snapshot.state().clone()));
            if store
                .get(&Self::key(id))?
                .is_some_and(|current| current.version == entry.version)
            {
                break (state, live, tool_progress);
            }
        };
        let cursor = state.transcript.clone().ok_or_else(|| {
            Error::Invalid(
                "legacy conversation has no immutable transcript; save it before viewing".into(),
            )
        })?;
        state.messages.clear();
        hide_pending_tool_output(&mut state);
        let page = self.view_page(
            id,
            ViewCursor {
                cursor,
                remaining: None,
            },
            limit,
        )?;
        Ok(ConversationView {
            state,
            live,
            tool_progress,
            entries: page.entries,
            older: page.older,
        })
    }

    fn committed_view_frame(&self, id: Uuid, position: u64) -> Result<CommittedViewFrame, Error> {
        if position == 0 {
            return Err(Error::Invalid(
                "committed view cursor has no snapshot".into(),
            ));
        }
        let store = self.owner.store();
        let mut key = b"durable/observer/".to_vec();
        key.extend_from_slice(id.as_bytes());
        key.extend_from_slice(b"/r");
        key.extend_from_slice(&position.to_be_bytes());
        let event = store
            .get(&key)?
            .ok_or_else(|| Error::Invalid("missing committed view observation".into()))?;
        let fork_key = format!("runtime/fork/{id}/{position:020}");
        let fork = store
            .get(fork_key.as_bytes())?
            .ok_or_else(|| Error::Invalid("unproven committed view snapshot".into()))?;
        let sequence = store.commit_for_version(&key, event.version)?;
        if sequence.is_none()
            || sequence != store.commit_for_version(fork_key.as_bytes(), fork.version)?
        {
            return Err(Error::Invalid("unproven committed view transaction".into()));
        }
        let mut value: serde_json::Value = serde_json::from_slice(&event.value)?;
        let snapshot: serde_json::Value = serde_json::from_slice(&fork.value)?;
        if snapshot["version"] != 1
            || snapshot["conversation_id"] != serde_json::json!(id)
            || value
                .get("_transcript")
                .is_none_or(serde_json::Value::is_null)
            || value.get("_transcript") != snapshot.get("_transcript")
            || !value
                .get("submissions")
                .is_some_and(serde_json::Value::is_array)
            || !value.get("resets").is_some_and(serde_json::Value::is_array)
            || !value
                .get("application_writes")
                .is_some_and(serde_json::Value::is_array)
            || !value
                .get("aborted_generations")
                .is_some_and(serde_json::Value::is_array)
            || !value
                .get("abort_requested")
                .is_some_and(serde_json::Value::is_boolean)
            || value.get("submissions") != snapshot.get("submissions")
            || value.get("resets") != snapshot.pointer("/documents/pi-rs~1runtime~1resets")
            || value.get("application_writes")
                != snapshot.pointer("/documents/pi-rs~1runtime~1application-writes")
        {
            return Err(Error::Invalid(
                "inconsistent committed view snapshot".into(),
            ));
        }
        self.expand_event(id, &mut value)?;
        Ok(CommittedViewFrame {
            cursor: CommittedViewCursor {
                conversation: id,
                position,
            },
            ops: vec![serde_json::json!(["r", value])],
            retired: false,
            value,
        })
    }
    fn next_committed_view(
        &self,
        cursor: &CommittedViewCursor,
    ) -> Result<Option<CommittedViewFrame>, Error> {
        let page = crate::observer::Observer::new(self.owner.store(), cursor.conversation).read(
            crate::observer::Cursor(cursor.position),
            std::num::NonZeroUsize::MIN,
        )?;
        page.records
            .first()
            .map(|record| self.committed_view_frame(cursor.conversation, record.cursor.0))
            .transpose()
    }
    /// Attach at the latest durable observation, or resume after a consumed
    /// cursor. The initial frame is the snapshot at that exact cursor.
    pub fn watch_committed_view(
        &self,
        id: Uuid,
        after: Option<CommittedViewCursor>,
        interval: Duration,
    ) -> Result<CommittedViewWatch, Error> {
        if interval.is_zero() {
            return Err(Error::Invalid("view watch interval must be nonzero".into()));
        }
        self.load(id)?;
        let head = crate::observer::Observer::new(self.owner.store(), id).head()?;
        let cursor = after.unwrap_or(CommittedViewCursor {
            conversation: id,
            position: head.0,
        });
        if cursor.conversation != id || cursor.position > head.0 {
            return Err(Error::Invalid(
                "committed view cursor does not belong to conversation".into(),
            ));
        }
        let initial = self.committed_view_frame(id, cursor.position)?;
        Ok(CommittedViewWatch {
            initial,
            reader: Some(self.watch_worker()),
            cursor,
            interval,
            end: None,
        })
    }

    /// Attach a bounded, polling view watch. The interval must be nonzero.
    pub fn watch_view(
        &self,
        id: Uuid,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<ViewWatch, Error> {
        if interval.is_zero() {
            return Err(Error::Invalid("view watch interval must be nonzero".into()));
        }
        let initial = self.view(id, limit)?;
        let last = view_fingerprint(&initial)?;
        Ok(ViewWatch {
            initial,
            end: None,
            reader: Some(self.watch_worker()),
            id,
            limit,
            interval,
            last,
        })
    }

    /// Read an older portion of the captured active transcript. At most `limit`
    /// messages and `limit` transcript records are visited per call. Empty
    /// pages can have an `older` cursor when batches contain no messages.
    pub fn view_page(
        &self,
        id: Uuid,
        position: ViewCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<ViewPage, Error> {
        let page = self.view_positioned_page(id, position, limit)?;
        Ok(ViewPage {
            entries: page
                .entries
                .into_iter()
                .map(|entry| entry.message)
                .collect(),
            older: page.older,
        })
    }

    /// Capture the active transcript and return its newest positioned messages.
    /// Positions can be serialized and revisited after later appends or reopening.
    pub fn view_positioned(
        &self,
        id: Uuid,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedViewPage, Error> {
        self.subagent_store()?;
        let entry = self
            .owner
            .store()
            .get(&Self::key(id))?
            .ok_or(Error::NotFound)?;
        let state: Conversation = serde_json::from_slice(&entry.value)?;
        let cursor = state.transcript.ok_or_else(|| {
            Error::Invalid(
                "legacy conversation has no immutable transcript; save it before viewing".into(),
            )
        })?;
        self.view_positioned_page(
            id,
            ViewCursor {
                cursor,
                remaining: None,
            },
            limit,
        )
    }

    /// Resolve a manual result against the immutable source and current range.
    /// None means the retained target is not provably active.
    fn apply_manual_compaction_target(
        &self,
        c: &Conversation,
        result: &crate::compact::CompactionResult,
    ) -> Result<Option<ResolvedCompactionTarget>, Error> {
        if result.has_blocks() {
            return if result.matches_blocks(&c.messages, &c.content_blocks, c.context_generation) {
                Ok(Some((
                    result.apply_in_context(&c.messages, c.context_generation)?,
                    result.retained_range(),
                )))
            } else {
                Ok(None)
            };
        }
        let Some(target) = result.retained_target() else {
            return if result.is_stale(&c.messages, c.context_generation) {
                Ok(None)
            } else {
                Ok(Some((
                    result.apply_in_context(&c.messages, c.context_generation)?,
                    result.retained_range(),
                )))
            };
        };
        if !c.occurrence_epoch_proven || c.resets.last().map(|reset| reset.id) != target.reset_id {
            return Ok(None);
        }
        let Some(mut cursor) = c.transcript.clone() else {
            return Ok(None);
        };
        if target.id.is_nil() || target.source.transcript_id() != c.id {
            return Ok(None);
        }
        let transcript = crate::transcript::Transcript::new(self.owner.store(), c.id);
        let mut ancestor = false;
        loop {
            if cursor == target.source {
                ancestor = true;
                break;
            }
            let page = transcript.read(&cursor, std::num::NonZeroUsize::MIN)?;
            if !page.has_more {
                break;
            }
            cursor = page.next;
        }
        if !ancestor {
            return Ok(None);
        }
        let source = self.resolved_occurrences(c.id, target.source.clone())?;
        let source_messages: Vec<_> = source.iter().map(|(message, _)| message.clone()).collect();
        if !result.source_matches(&source_messages) {
            return Ok(None);
        }
        let (_, first_kept) = result.retained_range();
        let Some(source_history) = source
            .get(first_kept)
            .and_then(|(_, history)| history.as_ref())
        else {
            return Ok(None);
        };
        if source_history.id != target.id {
            return Ok(None);
        }
        let Some(head) = c.transcript.clone() else {
            return Ok(None);
        };
        let current = self.resolved_occurrences(c.id, head)?;
        if current
            .iter()
            .map(|(message, _)| message)
            .ne(c.messages.iter())
        {
            return Ok(None);
        }
        let mut matches = current.iter().enumerate().filter(|(_, (_, history))| {
            history
                .as_ref()
                .is_some_and(|history| history.id == target.id)
        });
        let Some((index, (_, Some(history)))) = matches.next() else {
            return Ok(None);
        };
        if matches.next().is_some() || history != source_history {
            return Ok(None);
        }
        Ok(Some(result.apply_at_target(&c.messages, index)?))
    }

    /// Resolve provenance through immutable predecessor links. A compacted
    /// replacement is accepted only when its retained messages are exactly the
    /// stated projection of its immediate predecessor.
    fn resolved_occurrences(
        &self,
        id: Uuid,
        cursor: crate::transcript::Cursor,
    ) -> Result<Vec<(Message, Option<MessageHistory>)>, Error> {
        Ok(self.resolved_occurrences_with_ids(id, cursor)?.0)
    }

    fn resolved_occurrences_with_ids(
        &self,
        id: Uuid,
        mut cursor: crate::transcript::Cursor,
    ) -> Result<ResolvedOccurrences, Error> {
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut changes = Vec::new();
        loop {
            let mut page = transcript.read_with_blocks(&cursor, std::num::NonZeroUsize::MIN)?;
            let Some(record) = page.records.pop() else {
                break;
            };
            changes.push((
                cursor.clone(),
                serde_json::from_value::<TranscriptChange>(record.value)?,
            ));
            if !page.has_more {
                break;
            }
            cursor = page.next;
        }
        changes.reverse();
        let mut active: Vec<(Message, Option<MessageHistory>)> = Vec::new();
        let mut created = std::collections::HashSet::new();
        for (position, change) in changes {
            change.compacted_histories()?;
            if let TranscriptChange::ForkCut {
                messages,
                histories,
                commit,
                origin,
            } = change
            {
                self.verify_occurrence_origin(id, &position, origin, commit)?;
                let predecessor =
                    transcript.read_with_blocks(&position, std::num::NonZeroUsize::MIN)?;
                if !predecessor.has_more {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                let source_page =
                    transcript.read_with_blocks(&predecessor.next, std::num::NonZeroUsize::MIN)?;
                let source: TranscriptChange = serde_json::from_value(
                    source_page
                        .records
                        .first()
                        .ok_or(crate::fork::Error::InvalidSnapshot)?
                        .value
                        .clone(),
                )?;
                let (stable_prefix, source_commit, source_origin, batch_len) = match source {
                    TranscriptChange::AppendV3 {
                        stable_prefix,
                        commit,
                        origin,
                        messages,
                        ..
                    }
                    | TranscriptChange::AppendV4 {
                        stable_prefix,
                        commit,
                        origin,
                        messages,
                        ..
                    } => (stable_prefix, commit, origin, messages.len()),
                    _ => return Err(crate::fork::Error::InvalidSnapshot.into()),
                };
                let current_key = format!("runtime/fork/{source_origin}/{source_commit:020}");
                let previous = source_commit
                    .checked_sub(1)
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                let previous_key = format!("runtime/fork/{source_origin}/{previous:020}");
                let revision = self
                    .owner
                    .store()
                    .document_cut(current_key.as_bytes())?
                    .ok_or(crate::fork::Error::InvalidSnapshot)?;
                if self.owner.store().document_cut(previous_key.as_bytes())? != Some(revision)
                    || stable_prefix.checked_add(batch_len) != Some(active.len())
                    || messages.len() <= stable_prefix
                    || messages.len() >= active.len()
                    || histories.len() != messages.len()
                    || histories.iter().any(Option::is_none)
                    || messages
                        .iter()
                        .zip(&active)
                        .any(|(message, (old, _))| message != old)
                    || histories
                        .iter()
                        .zip(&active)
                        .any(|(history, (_, old))| history != old)
                    || crate::fork::validate_context(&messages).is_err()
                {
                    return Err(crate::fork::Error::InvalidSnapshot.into());
                }
                active.truncate(messages.len());
            } else if let TranscriptChange::ReplaceCompacted {
                messages,
                system_end,
                first_kept,
                summary_id,
                commit,
                origin,
                histories,
            } = change
            {
                self.verify_occurrence_origin(id, &position, origin, commit)?;
                if summary_id.is_nil()
                    || origin.is_nil()
                    || commit == 0
                    || system_end > first_kept
                    || first_kept >= active.len()
                    || active[..system_end]
                        .iter()
                        .any(|(message, _)| !matches!(message, Message::System { .. }))
                    || !matches!(
                        &active[first_kept].0,
                        Message::User { .. } | Message::UserWithImages { .. }
                    )
                    || messages.len() != system_end + 1 + active.len() - first_kept
                    || !messages[..system_end]
                        .iter()
                        .zip(&active[..system_end])
                        .all(|(m, (old, _))| m == old)
                    || !messages[system_end + 1..]
                        .iter()
                        .zip(&active[first_kept..])
                        .all(|(m, (old, _))| m == old)
                    || !created.insert(summary_id)
                {
                    return Err(Error::Invalid(
                        "invalid compacted transcript provenance".into(),
                    ));
                }
                let mut next = active[..system_end].to_vec();
                next.push((
                    messages[system_end].clone(),
                    Some(MessageHistory {
                        id: summary_id,
                        commit,
                        origin,
                        commit_boundary: system_end + 1 == messages.len(),
                    }),
                ));
                next.extend_from_slice(&active[first_kept..]);
                if next.iter().map(|(_, history)| history).ne(histories.iter()) {
                    return Err(Error::Invalid(
                        "compacted transcript provenance mismatch".into(),
                    ));
                }
                active = next;
            } else {
                let (messages, base, metadata) = change.parts()?;
                if let Some((_, commit, origin)) = &metadata {
                    self.verify_occurrence_origin(id, &position, *origin, *commit)?;
                }
                if base {
                    active.clear();
                }
                let n = messages.len();
                for (index, message) in messages.into_iter().enumerate() {
                    let history = if let Some((ids, commit, origin)) = &metadata {
                        if !created.insert(ids[index]) {
                            return Err(Error::Invalid(
                                "duplicate transcript occurrence identity".into(),
                            ));
                        }
                        Some(MessageHistory {
                            id: ids[index],
                            commit: *commit,
                            origin: *origin,
                            commit_boundary: index + 1 == n,
                        })
                    } else {
                        None
                    };
                    active.push((message, history));
                }
            }
        }
        Ok((active, created))
    }

    // The baseline must be the current committed root at the authenticated
    // observation; fork snapshots alone omit pending block-reset bindings.
    fn derive_anchored_reset_states(
        &self,
        id: Uuid,
        expected_version: u64,
        steps: &[ResetDerivationStep],
    ) -> Result<Vec<Conversation>, Error> {
        let (baseline, version) = self.load(id)?;
        let root = Self::key(id);
        if version != expected_version {
            return Err(durable_store::Error::Conflict { key: root }.into());
        }
        let store = self.owner.store();
        let sequence = store
            .commit_for_version(&root, version)?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let prefix = format!("runtime/fork/{id}/");
        let (_, fork_key) = store
            .runtime_snapshot_at(&root, prefix.as_bytes(), sequence)?
            .filter(|(commit, _)| *commit == sequence)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let suffix = fork_key
            .strip_prefix(prefix.as_bytes())
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let observation = std::str::from_utf8(suffix)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        self.authenticate_fork_anchor(id, observation)?;
        let fork = store
            .get(&fork_key)?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let snapshot: serde_json::Value = serde_json::from_slice(&fork.value)?;
        if snapshot.get("conversation_id") != Some(&serde_json::to_value(id)?)
            || snapshot.get("_transcript") != Some(&serde_json::to_value(&baseline.transcript)?)
            || snapshot.get("config") != Some(&serde_json::to_value(&baseline.config)?)
            || snapshot.get("submissions") != Some(&serde_json::to_value(&baseline.submissions)?)
            || snapshot.get("tool_states") != Some(&serde_json::to_value(&baseline.tool_states)?)
            || snapshot.get("active") != Some(&serde_json::to_value(baseline.active)?)
            || snapshot.get("pending_resets")
                != Some(&serde_json::to_value(&baseline.pending_resets)?)
            || snapshot.get("phase") != Some(&serde_json::to_value(&baseline.phase)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1queue-modes")
                != Some(&serde_json::to_value(baseline.queue_modes)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1after-failed-run")
                != Some(&serde_json::to_value(baseline.after_failed_run)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1extensions")
                != Some(&serde_json::to_value(&baseline.extensions)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1provider-adapter")
                != Some(&serde_json::to_value(&baseline.provider_adapter)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1allowed-tools")
                != Some(&serde_json::to_value(&baseline.allowed_tools)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1model-tools")
                != Some(&serde_json::to_value(&baseline.model_tools)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1tool-filters")
                != Some(&serde_json::to_value(&baseline.tool_filters)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1tool-results")
                != Some(&serde_json::to_value(&baseline.tool_results)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1occurrence-epoch-proven")
                != Some(&serde_json::to_value(baseline.occurrence_epoch_proven)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1resets")
                != Some(&serde_json::to_value(&baseline.resets)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1application-writes")
                != Some(&serde_json::to_value(&baseline.application_writes)?)
        {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let previous = observation
            .checked_sub(1)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let events = crate::observer::Observer::new(store, id).read(
            crate::observer::Cursor(previous),
            std::num::NonZeroUsize::MIN,
        )?;
        let has_digest = events.records.first().is_some_and(|event| {
            event.cursor.0 == observation && event.value.get("_reset_baseline_sha256").is_some()
        });
        if snapshot.get("_reset_baseline").is_some() && has_digest {
            let (historical, _) = self.authenticated_reset_baseline(id, observation)?;
            if serde_json::to_value(&historical)? != serde_json::to_value(&baseline)?
                || historical.messages != baseline.messages
                || historical.content_blocks != baseline.content_blocks
            {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            derive_ordered_reset_states(&historical, steps)
        } else {
            // Existing stores can have pending resets from before capture was
            // introduced. Current-version derivation remains supported.
            derive_ordered_reset_states(&baseline, steps)
        }
    }

    // Read a captured predecessor after the live root has advanced. The
    // immutable fork record, observer and root were committed together.
    fn authenticated_reset_baseline(
        &self,
        id: Uuid,
        observation: u64,
    ) -> Result<(Conversation, u64), Error> {
        self.authenticate_fork_anchor(id, observation)?;
        let key = format!("runtime/fork/{id}/{observation:020}");
        let store = self.owner.store();
        let fork = store
            .get(key.as_bytes())?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let snapshot: serde_json::Value = serde_json::from_slice(&fork.value)?;
        let value = snapshot
            .get("_reset_baseline")
            .cloned()
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let after = observation
            .checked_sub(1)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let page = crate::observer::Observer::new(store, id)
            .read(crate::observer::Cursor(after), std::num::NonZeroUsize::MIN)?;
        let event = page
            .records
            .first()
            .filter(|r| r.cursor.0 == observation)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        if event
            .value
            .get("_reset_baseline_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(reset_baseline_digest(&value)?.as_str())
            || event.value.get("_transcript") != snapshot.get("_transcript")
        {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let mut baseline: Conversation = serde_json::from_value(value)?;
        // Pending tool-result metadata lives at a separately mutable key and
        // cannot be reconstructed historically by this baseline format.
        if baseline.id != id
            || !baseline.messages.is_empty()
            || baseline.pending_reset_blocks.is_empty()
            || matches!(baseline.phase, Phase::ToolFinished { .. })
            || snapshot.get("conversation_id") != Some(&serde_json::to_value(id)?)
            || snapshot.get("_transcript") != Some(&serde_json::to_value(&baseline.transcript)?)
            || snapshot.get("config") != Some(&serde_json::to_value(&baseline.config)?)
            || snapshot.get("submissions") != Some(&serde_json::to_value(&baseline.submissions)?)
            || snapshot.get("tool_states") != Some(&serde_json::to_value(&baseline.tool_states)?)
            || snapshot.get("active") != Some(&serde_json::to_value(baseline.active)?)
            || snapshot.get("pending_resets")
                != Some(&serde_json::to_value(&baseline.pending_resets)?)
            || snapshot.get("phase") != Some(&serde_json::to_value(&baseline.phase)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1queue-modes")
                != Some(&serde_json::to_value(baseline.queue_modes)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1after-failed-run")
                != Some(&serde_json::to_value(baseline.after_failed_run)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1extensions")
                != Some(&serde_json::to_value(&baseline.extensions)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1provider-adapter")
                != Some(&serde_json::to_value(&baseline.provider_adapter)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1allowed-tools")
                != Some(&serde_json::to_value(&baseline.allowed_tools)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1model-tools")
                != Some(&serde_json::to_value(&baseline.model_tools)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1tool-filters")
                != Some(&serde_json::to_value(&baseline.tool_filters)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1tool-results")
                != Some(&serde_json::to_value(&baseline.tool_results)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1occurrence-epoch-proven")
                != Some(&serde_json::to_value(baseline.occurrence_epoch_proven)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1resets")
                != Some(&serde_json::to_value(&baseline.resets)?)
            || snapshot.pointer("/documents/pi-rs~1runtime~1application-writes")
                != Some(&serde_json::to_value(&baseline.application_writes)?)
        {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let cursor = baseline
            .transcript
            .clone()
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let (messages, blocks) = self.read_transcript_blocks(id, cursor)?;
        baseline.messages = messages;
        baseline.content_blocks = blocks;
        validate_pending_reset_blocks(&baseline)?;
        let revision = store
            .document_cut(key.as_bytes())?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        Ok((baseline, revision))
    }

    // A matching observer payload alone is not evidence of a runtime commit:
    // both reserved records must be the versions written by the same commit.
    fn authenticate_fork_anchor(&self, origin: Uuid, commit: u64) -> Result<(), Error> {
        if commit == 0 {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let mut observation = b"durable/observer/".to_vec();
        observation.extend_from_slice(origin.as_bytes());
        observation.extend_from_slice(b"/r");
        observation.extend_from_slice(&commit.to_be_bytes());
        let fork = format!("runtime/fork/{origin}/{commit:020}").into_bytes();
        let store = self.owner.store();
        let event = store
            .get(&observation)?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let snapshot = store
            .get(&fork)?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let Some(sequence) = store.commit_for_version(&observation, event.version)? else {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        };
        if Some(sequence) != store.commit_for_version(&fork, snapshot.version)?
            || store.document_cut(&fork)?.is_none()
        {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let root = format!("runtime/conversation/{origin}");
        let prefix = format!("runtime/fork/{origin}/");
        if !store.runtime_snapshot_exact_at(root.as_bytes(), prefix.as_bytes(), &fork, sequence)? {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        Ok(())
    }

    fn verify_occurrence_origin(
        &self,
        transcript_id: Uuid,
        position: &crate::transcript::Cursor,
        origin: Uuid,
        commit: u64,
    ) -> Result<(), Error> {
        self.authenticate_fork_anchor(origin, commit)?;
        let after = commit
            .checked_sub(1)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let page = crate::observer::Observer::new(self.owner.store(), origin)
            .read(crate::observer::Cursor(after), std::num::NonZeroUsize::MIN)?;
        let record = page
            .records
            .first()
            .filter(|record| record.cursor.0 == commit)
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let source: Option<crate::transcript::Cursor> = serde_json::from_value(
            record
                .value
                .get("_transcript")
                .cloned()
                .ok_or(crate::fork::Error::InvalidSnapshot)?,
        )?;
        let source = source.ok_or(crate::fork::Error::InvalidSnapshot)?;
        // Observer streams accept arbitrary public appends. Only the runtime's
        // separately reserved fork snapshot proves this was a runtime commit.
        let key = format!("runtime/fork/{origin}/{commit:020}");
        let snapshot = self
            .owner
            .store()
            .get(key.as_bytes())?
            .ok_or(crate::fork::Error::InvalidSnapshot)?;
        let snapshot: serde_json::Value = serde_json::from_slice(&snapshot.value)?;
        let captured: Option<crate::transcript::Cursor> = serde_json::from_value(
            snapshot
                .get("_transcript")
                .cloned()
                .ok_or(crate::fork::Error::InvalidSnapshot)?,
        )?;
        if captured.as_ref() != Some(&source)
            || snapshot.get("conversation_id") != Some(&serde_json::to_value(origin)?)
        {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let mut rebound = serde_json::to_value(source)?;
        rebound["transcript"] = serde_json::to_value(transcript_id)?;
        if serde_json::from_value::<crate::transcript::Cursor>(rebound)? != *position {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        Ok(())
    }

    /// Read a position inclusively, with stable positions for every returned message.
    /// A position is scoped to its conversation and is not a fork authorization.
    pub fn view_positioned_page(
        &self,
        id: Uuid,
        mut position: ViewCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedViewPage, Error> {
        self.subagent_store()?;
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut entries = Vec::new();
        let mut older = None;
        for _ in 0..limit.get() {
            let mut page = transcript.read(&position.cursor, std::num::NonZeroUsize::MIN)?;
            let Some(record) = page.records.pop() else {
                older = None;
                break;
            };
            let change: TranscriptChange = serde_json::from_value(record.value)?;
            if matches!(&change, TranscriptChange::ForkCut { .. }) {
                self.resolved_occurrences(id, position.cursor.clone())?;
            }
            let histories = change
                .compacted_histories()?
                .map(|histories| histories.to_vec());
            let (messages, base, metadata) = change.parts()?;
            let batch_len = messages.len();
            let end = position.remaining.unwrap_or(messages.len());
            if end > messages.len() {
                return Err(Error::Invalid("invalid view cursor".into()));
            }
            let start = end.saturating_sub(limit.get() - entries.len());
            entries.extend(
                messages
                    .into_iter()
                    .take(end)
                    .enumerate()
                    .skip(start)
                    .rev()
                    .map(|(index, message)| PositionedMessage {
                        message,
                        position: ViewCursor {
                            cursor: position.cursor.clone(),
                            remaining: Some(index + 1),
                        },
                        history: histories
                            .as_ref()
                            .and_then(|entries| entries[index].clone())
                            .or_else(|| {
                                metadata
                                    .as_ref()
                                    .map(|(ids, commit, origin)| MessageHistory {
                                        id: ids[index],
                                        commit: *commit,
                                        origin: *origin,
                                        commit_boundary: index + 1 == batch_len,
                                    })
                            }),
                    }),
            );
            if start > 0 {
                position.remaining = Some(start);
                older = Some(position);
                break;
            }
            if base {
                older = None;
                break;
            }
            if !page.has_more {
                return Err(Error::Invalid("transcript has no base".into()));
            }
            position = ViewCursor {
                cursor: page.next,
                remaining: None,
            };
            if entries.len() == limit.get() {
                older = Some(position);
                break;
            }
            older = Some(position.clone());
        }
        Ok(PositionedViewPage { entries, older })
    }
    /// Override wall time for durable gate deadlines. Set before driving work.
    pub fn set_signal_clock(&mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) {
        self.clock = clock;
    }
    /// Durably deliver an idempotent signal to a conversation hook gate.
    pub fn send_signal(&self, signal: &crate::conversation_signal::Signal) -> Result<(), Error> {
        self.conversation(signal.conversation_id)?;
        crate::conversation_signal::Inbox::new(self.owner.store())
            .send_with_clock(signal, self.clock.as_ref())?;
        Ok(())
    }
    /// Observe live task and conversation executors without waiting on their locks.
    pub fn worker_activity(&self) -> crate::worker_activity::WorkerActivity {
        self.activity.clone()
    }
    /// Attach one generic task scheduler to the same locked store. Its owner
    /// guard survives this handle; callers explicitly drive its ticks.
    pub fn attach_tasks(
        &self,
        registry: crate::task_scheduler::Registry,
    ) -> Result<crate::task_scheduler::Scheduler, Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let owner = self.owner.clone();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Failed)?;
        self.validate_task_tool_registry(&registry)?;
        let mut scheduler = crate::task_scheduler::Scheduler::attach(self.owner.clone(), registry)?;
        scheduler.bind_harness(self.worker());
        Ok(scheduler)
    }
    pub(crate) fn validate_task_tool_registry(
        &self,
        registry: &crate::task_scheduler::Registry,
    ) -> Result<(), Error> {
        for id in self.conversations()? {
            let (c, _) = self.load(id)?;
            if let Phase::TaskPending {
                ref calls,
                index,
                task,
                operation_id,
                ref identity,
                ..
            } = c.phase
            {
                let identity = identity
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("legacy task adapter identity missing".into()))?;
                let call = calls
                    .get(index)
                    .ok_or_else(|| Error::Invalid("invalid task tool index".into()))?;
                let tools = self.resolved_tools(&c)?;
                let adapter = tools
                    .get(&call.name)
                    .and_then(|r| crate::task_tool::lookup(&r.tool))
                    .ok_or_else(|| Error::Invalid("task adapter missing".into()))?;
                let child = crate::task_store::TaskStore::new(self.owner.store())
                    .load(task)
                    .map_err(|e| Error::Invalid(format!("task load: {e}")))?
                    .ok_or_else(|| Error::Invalid("task-backed tool task missing".into()))?;
                if task != operation_id
                    || adapter.identity().as_ref() != Some(identity)
                    || !registry.matches_task_tool(identity, &adapter.handler())
                    || child.task.owner() != &crate::task::TaskOwner::Conversation(id)
                    || child.task.kind() != Some(identity.kind.as_str())
                    || child.task.definition_version() != identity.definition_version
                    || child.task.replay_safe() != identity.replay_safe
                {
                    return Err(Error::Invalid(
                        "task adapter or definition identity changed".into(),
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn start_tasks(
        &self,
        registry: crate::task_scheduler::Registry,
    ) -> Result<crate::task_driver::Driver, Error> {
        let driver = crate::task_driver::Driver::start_with_harness(
            self.attach_tasks(registry)?,
            self.worker(),
        );
        *self.task_driver.lock().map_err(|_| Error::Failed)? = Some(driver.weak());
        Ok(driver)
    }
    /// Capture a finite unified history range. Legacy stores require an explicit
    /// opt-in to reading only commits recorded after sequence tracking began.
    pub fn unified_history_start(
        &self,
        id: Uuid,
        allow_legacy_boundary: bool,
    ) -> Result<UnifiedHistoryCursor, Error> {
        self.load(id)?;
        let store = self.owner.store();
        if store.legacy_version_boundary()? != 0 && !allow_legacy_boundary {
            return Err(Error::Invalid(
                "legacy commit order unavailable; explicitly start after the migration boundary"
                    .into(),
            ));
        }
        Ok(UnifiedHistoryCursor {
            conversation: id,
            after: 0,
            through: store.commit_sequence()?,
        })
    }

    /// Read at most `limit` complete commits in the captured range. Each model
    /// record is an expanded observation snapshot; application records are full
    /// log snapshots, not individually ordered transcript events.
    pub fn unified_history(
        &self,
        cursor: &UnifiedHistoryCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<UnifiedHistoryPage, Error> {
        if limit.get() > 255 {
            return Err(Error::Invalid("history page limit exceeds 255".into()));
        }
        self.load(cursor.conversation)?;
        let mut prefix = b"durable/observer/".to_vec();
        prefix.extend_from_slice(cursor.conversation.as_bytes());
        prefix.extend_from_slice(b"/r");
        let app_key = crate::application_entry::key(cursor.conversation);
        let mut frames = self.owner.store().conversation_commits(
            &prefix,
            &app_key,
            cursor.after,
            cursor.through,
            limit.get() + 1,
        )?;
        let has_more = frames.len() > limit.get();
        frames.truncate(limit.get());
        let mut commits = Vec::with_capacity(frames.len());
        for (sequence, records) in frames {
            let mut snapshots = Vec::new();
            let mut applications = None;
            for (key, entry) in records {
                if key == app_key {
                    if applications.is_some() {
                        return Err(Error::Invalid("duplicate application snapshot".into()));
                    }
                    applications = Some(serde_json::from_slice(&entry.value)?);
                } else {
                    let suffix = key
                        .strip_prefix(prefix.as_slice())
                        .ok_or_else(|| Error::Invalid("invalid observation key".into()))?;
                    let observation = u64::from_be_bytes(
                        suffix
                            .try_into()
                            .map_err(|_| Error::Invalid("invalid observation key".into()))?,
                    );
                    let mut value = serde_json::from_slice(&entry.value)?;
                    self.expand_event(cursor.conversation, &mut value)?;
                    snapshots.push(crate::observer::Record {
                        cursor: crate::observer::Cursor(observation),
                        value,
                    });
                }
            }
            commits.push(UnifiedHistoryCommit {
                sequence,
                snapshots,
                applications,
            });
        }
        let mut next = cursor.clone();
        if let Some(last) = commits.last() {
            next.after = last.sequence;
        }
        Ok(UnifiedHistoryPage {
            commits,
            next,
            has_more,
        })
    }

    fn merged_observation(
        &self,
        conversation: Uuid,
        after: u64,
    ) -> Result<Option<(u64, CommittedViewFrame)>, Error> {
        let page = crate::observer::Observer::new(self.owner.store(), conversation)
            .read(crate::observer::Cursor(after), std::num::NonZeroUsize::MIN)?;
        let Some(record) = page.records.first() else {
            return Ok(None);
        };
        let frame = self.committed_view_frame(conversation, record.cursor.0)?;
        let mut key = b"durable/observer/".to_vec();
        key.extend_from_slice(conversation.as_bytes());
        key.extend_from_slice(b"/r");
        key.extend_from_slice(&record.cursor.0.to_be_bytes());
        let entry = self
            .owner
            .store()
            .get(&key)?
            .ok_or_else(|| Error::Invalid("missing merged observation".into()))?;
        let sequence = self
            .owner
            .store()
            .commit_for_version(&key, entry.version)?
            .ok_or_else(|| Error::Invalid("untracked merged observation".into()))?;
        Ok(Some((sequence, frame)))
    }
    fn merged_progress_record(
        &self,
        conversation: Uuid,
        after: u64,
    ) -> Result<Option<ProgressHistoryFrame>, Error> {
        let cursor = ProgressHistoryCursor {
            version: 1,
            conversation,
            position: after,
        };
        Ok(self
            .progress_history(&cursor, std::num::NonZeroUsize::MIN)?
            .records
            .into_iter()
            .next())
    }
    fn merged_document_record(
        &self,
        conversation: Uuid,
        after: u64,
    ) -> Result<Option<(u64, u64, Vec<MergedDocumentChange>)>, Error> {
        let scope = conversation.to_string();
        let mut encoded = serde_json::to_vec(&[scope])?;
        if encoded.pop() != Some(b']') {
            return Err(Error::Invalid("invalid document scope".into()));
        }
        encoded.push(b',');
        let mut prefix = b"durable/document/".to_vec();
        prefix.extend(encoded);
        let Some((revision, sequence, changes)) = self
            .owner
            .store()
            .next_scoped_document_commit(&prefix, after)?
        else {
            return Ok(None);
        };
        let changes = changes
            .into_iter()
            .map(|(key, entry)| -> Result<_, Error> {
                let address = crate::document::Address::from_storage_key(&key)?;
                if address.scope != conversation.to_string() {
                    return Err(Error::Invalid("document scope mismatch".into()));
                }
                Ok(MergedDocumentChange {
                    address,
                    document: crate::document::Documents::decode(entry)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((revision, sequence, changes)))
    }
    /// Attach a lifecycle-aware merged watch from now or after a consumed cursor.
    /// Both v1 and v2 cursor semantics are preserved on resume.
    pub fn watch_merged_progress_handle(
        &self,
        conversation: Uuid,
        after: Option<MergedProgressCursor>,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<MergedProgressWatch, Error> {
        if interval.is_zero() || limit.get() > 256 {
            return Err(Error::Invalid(
                "invalid merged watch interval or page limit".into(),
            ));
        }
        let cursor = match after {
            Some(cursor) => {
                if cursor.conversation != conversation {
                    return Err(Error::Invalid(
                        "merged cursor belongs to another conversation".into(),
                    ));
                }
                self.validate_merged_progress_cursor(&cursor)?;
                cursor
            }
            None => self.merged_progress_from_now(conversation)?,
        };
        let source = self
            .merged_source
            .as_ref()
            .ok_or_else(|| Error::Invalid("merged watch source already closed".into()))?
            .signal
            .clone();
        Ok(MergedProgressWatch {
            reader: Some(self.watch_worker()),
            source,
            cursor,
            limit,
            interval,
            end: None,
        })
    }

    /// Start after a consistent current store boundary. Prior progress is not
    /// backfilled; this is explicit even in stores with no legacy metadata.
    pub fn merged_progress_from_now(
        &self,
        conversation: Uuid,
    ) -> Result<MergedProgressCursor, Error> {
        self.load(conversation)?;
        let store = self.owner.store();
        for _ in 0..128 {
            let before = store.commit_sequence()?;
            let observation = crate::observer::Observer::new(store, conversation)
                .head()?
                .0;
            let progress = crate::observer::Observer::progress_history(store, conversation)
                .head()?
                .0;
            let document_position = store.document_snapshot()?;
            if before != store.commit_sequence()? {
                continue;
            }
            let cursor = MergedProgressCursor {
                version: 2,
                conversation,
                after_sequence: before,
                observation_position: observation,
                progress_position: progress,
                document_position: Some(document_position),
            };
            self.validate_merged_progress_cursor(&cursor)?;
            return Ok(cursor);
        }
        Err(Error::Invalid(
            "could not capture stable merged progress boundary".into(),
        ))
    }
    fn validate_merged_progress_cursor(&self, cursor: &MergedProgressCursor) -> Result<(), Error> {
        if !matches!(cursor.version, 1 | 2)
            || (cursor.version == 1 && cursor.document_position.is_some())
            || (cursor.version == 2 && cursor.document_position.is_none())
            || cursor.after_sequence > self.owner.store().commit_sequence()?
        {
            return Err(Error::Invalid("invalid merged progress cursor".into()));
        }
        self.load(cursor.conversation)?;
        if let Some(position) = cursor.document_position {
            let (boundary, boundary_sequence) = self.owner.store().document_commit_boundary()?;
            if position < boundary
                || cursor.after_sequence < boundary_sequence
                || position > self.owner.store().document_snapshot()?
            {
                return Err(Error::Invalid("invalid document tracking boundary".into()));
            }
            if position > boundary
                && self
                    .owner
                    .store()
                    .document_commit_sequence(position)?
                    .is_none_or(|sequence| sequence > cursor.after_sequence)
            {
                return Err(Error::Invalid("untracked document cursor".into()));
            }
        }
        if cursor.observation_position > 0 {
            let last = self
                .merged_observation(cursor.conversation, cursor.observation_position - 1)?
                .ok_or_else(|| Error::Invalid("missing merged observation cursor".into()))?;
            if last.1.cursor.position != cursor.observation_position
                || last.0 > cursor.after_sequence
            {
                return Err(Error::Invalid(
                    "inconsistent merged observation cursor".into(),
                ));
            }
        }
        if cursor.progress_position > 0 {
            let last = self
                .merged_progress_record(cursor.conversation, cursor.progress_position - 1)?
                .ok_or_else(|| Error::Invalid("missing merged progress cursor".into()))?;
            if last.cursor.position != cursor.progress_position
                || last.commit_sequence > cursor.after_sequence
            {
                return Err(Error::Invalid("inconsistent merged progress cursor".into()));
            }
        }
        Ok(())
    }
    fn next_merged_progress(
        &self,
        cursor: &MergedProgressCursor,
        through: u64,
    ) -> Result<Option<(MergedProgressFrame, MergedProgressCursor)>, Error> {
        let observation =
            self.merged_observation(cursor.conversation, cursor.observation_position)?;
        let progress =
            self.merged_progress_record(cursor.conversation, cursor.progress_position)?;
        let documents = cursor
            .document_position
            .map(|position| self.merged_document_record(cursor.conversation, position))
            .transpose()?
            .flatten();
        if observation
            .as_ref()
            .is_some_and(|(sequence, _)| *sequence <= cursor.after_sequence)
            || progress
                .as_ref()
                .is_some_and(|frame| frame.commit_sequence <= cursor.after_sequence)
            || documents
                .as_ref()
                .is_some_and(|(_, sequence, _)| *sequence <= cursor.after_sequence)
        {
            return Err(Error::Invalid(
                "merged cursor skips a committed record".into(),
            ));
        }
        let sequence = observation
            .as_ref()
            .map(|v| v.0)
            .into_iter()
            .chain(progress.as_ref().map(|v| v.commit_sequence))
            .chain(documents.as_ref().map(|(_, sequence, _)| *sequence))
            .min();
        let Some(sequence) = sequence.filter(|sequence| *sequence <= through) else {
            return Ok(None);
        };
        let mut next = cursor.clone();
        next.after_sequence = sequence;
        let observation = observation
            .filter(|(seq, _)| *seq == sequence)
            .map(|(_, frame)| {
                next.observation_position = frame.cursor.position;
                frame
            });
        let progress = progress
            .filter(|frame| frame.commit_sequence == sequence)
            .inspect(|frame| {
                next.progress_position = frame.cursor.position;
            });
        let documents = documents
            .filter(|(_, seq, _)| *seq == sequence)
            .map(|(revision, _, changes)| {
                next.document_position = Some(revision);
                changes
            })
            .unwrap_or_default();
        // Runtime writers commit at most one record per source per transaction.
        // If external writes violate that contract, fail instead of splitting it.
        if observation.is_some()
            && self
                .merged_observation(cursor.conversation, next.observation_position)?
                .is_some_and(|(seq, _)| seq <= sequence)
            || progress.is_some()
                && self
                    .merged_progress_record(cursor.conversation, next.progress_position)?
                    .is_some_and(|frame| frame.commit_sequence <= sequence)
        {
            return Err(Error::Invalid(
                "duplicate or unordered merged transaction".into(),
            ));
        }
        Ok(Some((
            MergedProgressFrame {
                commit_sequence: sequence,
                observation,
                progress,
                documents,
            },
            next,
        )))
    }
    fn prior_progress_for_operations(
        &self,
        current: &ProgressHistoryFrame,
    ) -> Result<Option<ProgressHistoryFrame>, Error> {
        if current.cursor.position <= 1 {
            return Ok(None);
        }
        let cursor = ProgressHistoryCursor {
            version: 1,
            conversation: current.cursor.conversation,
            position: current.cursor.position - 2,
        };
        let mut page = match self.progress_history(&cursor, std::num::NonZeroUsize::MIN) {
            Ok(page) => page,
            Err(error @ Error::Storage(_))
            | Err(error @ Error::Observer(crate::observer::Error::Store(_))) => return Err(error),
            Err(_) => return Ok(None),
        };
        let Some(previous) = page.records.pop() else {
            return Ok(None);
        };
        if previous.cursor.position.checked_add(1) != Some(current.cursor.position)
            || previous.commit_sequence >= current.commit_sequence
        {
            return Err(Error::Invalid(
                "invalid progress predecessor ordering".into(),
            ));
        }
        Ok(Some(previous))
    }
    fn incremental_merged_page(
        &self,
        page: MergedProgressPage,
    ) -> Result<IncrementalMergedPage, Error> {
        let store = self.owner.store();
        let mut frames = Vec::with_capacity(page.frames.len());
        for mut frame in page.frames {
            let mut observation_previous = None;
            if let Some(observation) = &mut frame.observation {
                let position = observation.cursor.position;
                if position > 1 {
                    match self.committed_view_frame(observation.cursor.conversation, position - 1) {
                        Ok(previous) => {
                            observation.ops =
                                crate::chord_ops::diff(&previous.value, &observation.value);
                            observation_previous = Some(previous.value);
                        }
                        Err(error @ Error::Storage(_)) | Err(error @ Error::Observer(_)) => {
                            return Err(error);
                        }
                        Err(_) => {} // The proven current snapshot remains a safe root replacement.
                    }
                }
            }
            let mut progress = Vec::new();
            if let Some(current) = &frame.progress {
                let previous = self.prior_progress_for_operations(current)?;
                for change in &current.changes {
                    let matching: Vec<_> = previous
                        .as_ref()
                        .into_iter()
                        .flat_map(|record| &record.changes)
                        .filter(|candidate| same_progress_identity(candidate, change))
                        .collect();
                    let unique_current = current
                        .changes
                        .iter()
                        .filter(|candidate| same_progress_identity(candidate, change))
                        .count()
                        == 1;
                    let prior = if matching.len() == 1 && unique_current {
                        match matching[0] {
                            ProgressHistoryChange::Generation { state, .. }
                                if matches!(state.as_ref(), crate::progress::State::Running(_)) =>
                            {
                                Some(matching[0].clone())
                            }
                            ProgressHistoryChange::Tool {
                                state: crate::tool_progress::State::Running(_),
                                ..
                            } => Some(matching[0].clone()),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    let value = serde_json::to_value(change)?;
                    let ops = match &prior {
                        Some(prior) => {
                            crate::chord_ops::progress_diff(&serde_json::to_value(prior)?, &value)
                        }
                        None => crate::chord_ops::replacement(&value),
                    };
                    progress.push(MergedProgressOperations {
                        previous: prior,
                        ops,
                    });
                }
            }
            let mut documents = Vec::with_capacity(frame.documents.len());
            if !frame.documents.is_empty() {
                let revision = store
                    .document_revision_for_commit(frame.commit_sequence)?
                    .ok_or_else(|| Error::Invalid("missing document commit revision".into()))?;
                let prior_boundary = revision
                    .checked_sub(1)
                    .ok_or_else(|| Error::Invalid("invalid document revision".into()))?;
                let tracking_boundary = store.document_commit_boundary()?.0;
                let docs = crate::document::Documents::new(store);
                for change in &frame.documents {
                    if docs.get_at(&change.address, revision)? != change.document {
                        return Err(Error::Invalid("document commit changed".into()));
                    }
                    let previous = docs.get_at(&change.address, prior_boundary)?;
                    let key = change.address.storage_key()?;
                    let previous_revision = store.document_last_revision(&key, prior_boundary)?;
                    let proven = match (&previous, previous_revision) {
                        (Some(document), Some(previous_revision))
                            if previous_revision > tracking_boundary =>
                        {
                            let seq = store.document_commit_sequence(previous_revision)?;
                            seq.is_some()
                                && seq == store.commit_for_version(&key, document.version)?
                        }
                        _ => false,
                    };
                    let actual_ops = match (&previous, &change.document) {
                        (Some(old), Some(new)) => crate::document::committed_operations(
                            store,
                            &change.address,
                            old,
                            new,
                            frame.commit_sequence,
                        )?,
                        _ => None,
                    };
                    let actual = actual_ops.is_some();
                    let ops = if let Some(ops) = actual_ops {
                        ops
                    } else {
                        match (&previous, &change.document) {
                            (Some(old), Some(new)) if proven => crate::chord_ops::diff(
                                &serde_json::Value::Object(old.value.clone()),
                                &serde_json::Value::Object(new.value.clone()),
                            ),
                            (_, Some(new)) => crate::chord_ops::replacement(
                                &serde_json::Value::Object(new.value.clone()),
                            ),
                            (_, None) => Vec::new(),
                        }
                    };
                    documents.push(MergedDocumentOperations {
                        address: change.address.clone(),
                        previous: if proven || actual { previous } else { None },
                        ops,
                        actual,
                        retired: change.document.is_none(),
                    });
                }
            }
            frames.push(IncrementalMergedFrame {
                frame,
                observation_previous,
                progress,
                documents,
            });
        }
        Ok(IncrementalMergedPage {
            frames,
            next: page.next,
            has_more: page.has_more,
        })
    }
    /// Page committed transactions with derived incremental operations.
    pub fn merged_progress_operations(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<IncrementalMergedPage, Error> {
        self.incremental_merged_page(self.merged_progress(cursor, limit)?)
    }

    /// Page complete transactions after a saved cursor. Each page captures one
    /// store boundary so a racing commit cannot be split across sources.
    pub fn merged_progress(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<MergedProgressPage, Error> {
        if limit.get() > 256 {
            return Err(Error::Invalid(
                "merged progress page limit exceeds 256".into(),
            ));
        }
        let through = self.owner.store().commit_sequence()?;
        self.merged_progress_through(cursor, limit, through)
    }
    fn merged_progress_through(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
        through: u64,
    ) -> Result<MergedProgressPage, Error> {
        if limit.get() > 256
            || through < cursor.after_sequence
            || through > self.owner.store().commit_sequence()?
        {
            return Err(Error::Invalid("invalid merged progress boundary".into()));
        }
        self.validate_merged_progress_cursor(cursor)?;
        let mut next = cursor.clone();
        let mut frames = Vec::new();
        while frames.len() < limit.get() {
            let Some((frame, updated)) = self.next_merged_progress(&next, through)? else {
                break;
            };
            frames.push(frame);
            next = updated;
        }
        let has_more = self.next_merged_progress(&next, through)?.is_some();
        Ok(MergedProgressPage {
            frames,
            next,
            has_more,
        })
    }
    pub async fn watch_merged_progress(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<MergedProgressPage, Error> {
        if interval.is_zero() {
            return Err(Error::Invalid(
                "merged progress watch interval must be nonzero".into(),
            ));
        }
        loop {
            let page = self.merged_progress(cursor, limit)?;
            if !page.frames.is_empty() {
                return Ok(page);
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Capture an explicit forward-only boundary. Earlier overwritten progress
    /// cannot be reconstructed, even when this cursor's position is zero.
    pub fn progress_history_from_now(
        &self,
        conversation: Uuid,
    ) -> Result<ProgressHistoryCursor, Error> {
        self.load(conversation)?;
        let position =
            crate::observer::Observer::progress_history(self.owner.store(), conversation)
                .head()?
                .0;
        Ok(ProgressHistoryCursor {
            version: 1,
            conversation,
            position,
        })
    }
    /// Read at most 256 atomic progress records after the supplied cursor.
    pub fn progress_history(
        &self,
        cursor: &ProgressHistoryCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<ProgressHistoryPage, Error> {
        if cursor.version != 1 || limit.get() > 256 {
            return Err(Error::Invalid(
                "unsupported progress cursor or page limit".into(),
            ));
        }
        self.load(cursor.conversation)?;
        let page =
            crate::observer::Observer::progress_history(self.owner.store(), cursor.conversation)
                .read(crate::observer::Cursor(cursor.position), limit)?;
        let records = page
            .records
            .into_iter()
            .map(|record| -> Result<_, Error> {
                let mut key = b"durable/progress-history/".to_vec();
                key.extend_from_slice(cursor.conversation.as_bytes());
                key.extend_from_slice(b"/r");
                key.extend_from_slice(&record.cursor.0.to_be_bytes());
                let entry = self
                    .owner
                    .store()
                    .get(&key)?
                    .ok_or_else(|| Error::Invalid("missing progress history record".into()))?;
                if entry.value.len() > MAX_PROGRESS_HISTORY_BYTES {
                    return Err(Error::Invalid(
                        "progress history event exceeds size limit".into(),
                    ));
                }
                let persisted: serde_json::Value = serde_json::from_slice(&entry.value)?;
                if persisted != record.value {
                    return Err(Error::Invalid("changed progress history record".into()));
                }
                let commit_sequence = self
                    .owner
                    .store()
                    .commit_for_version(&key, entry.version)?
                    .ok_or_else(|| Error::Invalid("untracked progress history record".into()))?;
                let event: ProgressHistoryRecord = serde_json::from_value(persisted)?;
                if event.version != 1
                    || event.conversation != cursor.conversation
                    || event.changes.is_empty()
                    || event.changes.len() > 2
                {
                    return Err(Error::Invalid("invalid progress history record".into()));
                }
                Ok(ProgressHistoryFrame {
                    cursor: ProgressHistoryCursor {
                        version: 1,
                        conversation: cursor.conversation,
                        position: record.cursor.0,
                    },
                    commit_sequence,
                    changes: event.changes,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProgressHistoryPage {
            records,
            next: ProgressHistoryCursor {
                version: 1,
                conversation: cursor.conversation,
                position: page.next.0,
            },
            has_more: page.has_more,
        })
    }
    /// Poll for the next page; dropping the future does not consume a cursor.
    pub async fn watch_progress_history(
        &self,
        cursor: &ProgressHistoryCursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<ProgressHistoryPage, Error> {
        if interval.is_zero() {
            return Err(Error::Invalid(
                "progress watch interval must be nonzero".into(),
            ));
        }
        loop {
            let page = self.progress_history(cursor, limit)?;
            if !page.records.is_empty() {
                return Ok(page);
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Poll committed replacement and terminal events for one attempt. Cursors
    /// are exclusive and scoped to this attempt and database.
    pub fn tool_progress_events(
        &self,
        attempt_id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.subagent_store()?;
        Ok(
            crate::observer::Observer::tool_progress(self.owner.store(), attempt_id)
                .read(after, limit)?,
        )
    }

    /// Discover committed attempts in conversation order. Filter by operation ID
    /// after reading; the cursor is exclusive and scoped to this conversation.
    pub fn tool_attempts(
        &self,
        conversation_id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.subagent_store()?;
        if self
            .owner
            .store()
            .get(&Self::key(conversation_id))?
            .is_none()
        {
            return Err(Error::NotFound);
        }
        Ok(
            crate::observer::Observer::tool_attempts(self.owner.store(), conversation_id)
                .read(after, limit)?,
        )
    }

    /// Read committed snapshots after a cursor. Earlier cursors may be
    /// resumed; snapshots are retained independently of model context.
    pub fn history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.load(id)?;
        let mut page = crate::observer::Observer::new(self.owner.store(), id).read(after, limit)?;
        for record in &mut page.records {
            self.expand_event(id, &mut record.value)?;
        }
        Ok(page)
    }
    /// Capture a conversation-scoped boundary for finite history replay.
    pub fn history_head(&self, id: Uuid) -> Result<crate::observer::Cursor, Error> {
        self.load(id)?;
        Ok(crate::observer::Observer::new(self.owner.store(), id).head()?)
    }

    /// Replay expanded snapshots through a previously captured history head.
    /// Persist both cursors with the conversation ID when resuming after restart.
    pub fn history_through(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        through: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.load(id)?;
        let mut page = crate::observer::Observer::new(self.owner.store(), id)
            .read_through(after, through, limit)?;
        for record in &mut page.records {
            self.expand_event(id, &mut record.value)?;
        }
        Ok(page)
    }
    /// Persist a reset; busy conversations place it at the next safe boundary.
    pub fn reset(&mut self, id: Uuid, handoff: Option<String>) -> Result<Uuid, Error> {
        let marker = crate::reset::Reset::new(Uuid::new_v4(), handoff);
        loop {
            let (mut state, version) = self.load(id)?;
            if matches!(state.phase, Phase::Idle | Phase::FailedIdle)
                && state.active.is_none()
                && state.pending_resets.is_empty()
                && state.pending_independent_summary.is_none()
                && !state
                    .application_writes
                    .iter()
                    .any(|w| !w.placed && !w.withdrawn)
                && !state.submissions.iter().any(|s| s.status == Status::Queued)
            {
                place_reset(&mut state, marker.clone());
                state.phase = Phase::Idle;
            } else {
                state.pending_resets.push(marker.clone());
            }
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let saved = self.save(&state, Expected::Version(version));
            self.merge_admissions = merge;
            match saved {
                Ok(()) => return Ok(marker.id),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    /// Admit a reset and ordered user block occurrences as one durable operation.
    /// The handoff precedes the additions. Queued resets are placed in order,
    /// with separate commits where needed to preserve each block occurrence.
    pub fn reset_with_blocks(
        &mut self,
        id: Uuid,
        handoff: Option<String>,
        additions: Vec<Vec<serde_json::Value>>,
    ) -> Result<(Uuid, Vec<Uuid>), Error> {
        self.reset_with_blocks_at_version(id, handoff, additions, None, None)
    }
    fn reset_with_blocks_at_version(
        &mut self,
        id: Uuid,
        handoff: Option<String>,
        additions: Vec<Vec<serde_json::Value>>,
        expected_version: Option<u64>,
        receipt: Option<(String, Vec<String>)>,
    ) -> Result<(Uuid, Vec<Uuid>), Error> {
        if additions.is_empty()
            || additions.iter().any(|blocks| {
                crate::content::UserContent::from_yield(
                    serde_json::Value::Array(blocks.clone()),
                    false,
                )
                .is_none()
            })
        {
            return Err(Error::Invalid("invalid reset block additions".into()));
        }
        let marker = crate::reset::Reset::new(Uuid::new_v4(), handoff);
        let bindings: Vec<_> = additions
            .into_iter()
            .map(|blocks| crate::transcript::MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks,
            })
            .collect();
        let ids: Vec<_> = bindings.iter().map(|binding| binding.message_id).collect();
        loop {
            let (mut state, version) = self.load(id)?;
            if expected_version.is_some_and(|expected| expected != version) {
                return Err(Error::Invalid(
                    "conversation changed during image fitting".into(),
                ));
            }
            let source = state
                .transcript
                .clone()
                .ok_or(crate::transcript::Error::UnsupportedContentBlocks)?;
            if !state.occurrence_epoch_proven {
                return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
            }
            let (_, created) = self.resolved_occurrences_with_ids(id, source)?;
            let mut unique = std::collections::HashSet::new();
            if ids
                .iter()
                .any(|id| id.is_nil() || !unique.insert(*id) || created.contains(id))
                || state
                    .resets
                    .iter()
                    .chain(&state.pending_resets)
                    .any(|reset| reset.id == marker.id)
                || state
                    .pending_reset_blocks
                    .values()
                    .flatten()
                    .any(|binding| unique.contains(&binding.message_id))
            {
                return Err(Error::Invalid("reset identity collision".into()));
            }
            if let Some((key, notes)) = &receipt {
                if state.fitted_reset_receipts.contains_key(key) {
                    return Err(Error::Invalid("duplicate fitted reset request".into()));
                }
                state.fitted_reset_receipts.insert(
                    key.clone(),
                    FittedResetReceipt {
                        reset: marker.id,
                        occurrences: ids.clone(),
                        notes: notes.clone(),
                    },
                );
            }
            state
                .pending_reset_blocks
                .insert(marker.id, bindings.clone());
            if matches!(state.phase, Phase::Idle | Phase::FailedIdle)
                && state.active.is_none()
                && !state.abort_requested
                && state.pending_resets.is_empty()
                && state.pending_independent_summary.is_none()
                && !state
                    .application_writes
                    .iter()
                    .any(|write| !write.placed && !write.withdrawn)
                && !state
                    .submissions
                    .iter()
                    .any(|submission| submission.status == Status::Queued)
            {
                place_reset(&mut state, marker.clone());
                state.phase = Phase::Idle;
            } else {
                state.pending_resets.push(marker.clone());
            }
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let saved = self.save(&state, Expected::Version(version));
            self.merge_admissions = merge;
            match saved {
                Ok(()) => return Ok((marker.id, ids)),
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
                    if expected_version.is_none() =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }
    /// Fit a reset's additions before committing the reset and occurrences together.
    pub async fn reset_with_fitted_blocks(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        handoff: Option<String>,
        additions: Vec<Vec<serde_json::Value>>,
        admission: &crate::images::UserImageAdmission,
        cancel: &CancellationToken,
    ) -> Result<(Uuid, Vec<Uuid>, Vec<String>), Error> {
        let (state, version) = self.load(id)?;
        if let Some(key) = &request_id {
            if key.is_empty() {
                return Err(Error::Invalid("empty fitted reset request ID".into()));
            }
            if let Some(receipt) = state.fitted_reset_receipts.get(key) {
                return Ok((
                    receipt.reset,
                    receipt.occurrences.clone(),
                    receipt.notes.clone(),
                ));
            }
        }
        let adapter = self
            .provider_adapter
            .as_ref()
            .filter(|a| {
                state.provider_adapter.as_deref() == Some(a.id()) && a.supports_durable_blocks()
            })
            .ok_or_else(|| {
                Error::Invalid("image admission requires a selected block-aware adapter".into())
            })?;
        if additions.is_empty() {
            return Err(Error::Invalid("empty reset block additions".into()));
        }
        let profile = adapter.image_profile(&state.config.model);
        let limits = profile.map(|p| p.limits).unwrap_or_default();
        let mut output = Vec::with_capacity(additions.len());
        let mut notes = Vec::new();
        for blocks in additions {
            let (blocks, mut image_notes) = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Interrupted),
                result = admission.prepare_blocks(&blocks, limits) => result.map_err(Error::Invalid)?,
            };
            output.push(blocks);
            notes.append(&mut image_notes);
        }
        if cancel.is_cancelled() {
            return Err(Error::Interrupted);
        }
        if self
            .provider_adapter
            .as_ref()
            .filter(|a| state.provider_adapter.as_deref() == Some(a.id()))
            .and_then(|a| a.image_profile(&state.config.model))
            != profile
        {
            return Err(Error::Invalid(
                "model image profile changed during admission".into(),
            ));
        }
        let (reset, ids) = self.reset_with_blocks_at_version(
            id,
            handoff,
            output,
            Some(version),
            request_id.map(|key| (key, notes.clone())),
        )?;
        Ok((reset, ids, notes))
    }
    /// Persist a display-only application entry and return its submission ID.
    /// It is immediately placed unless a reset is queued ahead of it.
    pub fn submit_write(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        kind: String,
        data: Option<serde_json::Value>,
    ) -> Result<Uuid, Error> {
        loop {
            let (mut c, version) = self.load(id)?;
            if let Some(key) = request_id.as_ref() {
                if c.submissions
                    .iter()
                    .any(|s| s.request_id.as_ref() == Some(key))
                {
                    return Err(Error::Invalid(
                        "request ID identifies an input submission".into(),
                    ));
                }
                if let Some(write) = c
                    .application_writes
                    .iter()
                    .find(|w| w.request_id.as_ref() == Some(key))
                {
                    return Ok(write.id);
                }
            }
            if kind.is_empty() || kind.starts_with("pi.") {
                return Err(Error::Invalid(
                    "application entry kind must be nonempty and outside pi.".into(),
                ));
            }
            let after_reset = c
                .pending_resets
                .last()
                .map(|r| r.id)
                .or_else(|| c.resets.last().map(|r| r.id));
            let write = ApplicationWrite {
                id: Uuid::new_v4(),
                request_id: request_id.clone(),
                kind: kind.clone(),
                data: data.clone(),
                placed: c.pending_resets.is_empty()
                    && c.pending_independent_summary.is_none()
                    && !c
                        .application_writes
                        .iter()
                        .any(|w| !w.placed && !w.withdrawn),
                withdrawn: false,
                after_reset,
            };
            let write_id = write.id;
            c.application_writes.push(write);
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let result = self.save(&c, Expected::Version(version));
            self.merge_admissions = merge;
            match result {
                Ok(()) => return Ok(write_id),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    pub async fn watch_history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<crate::observer::Page, Error> {
        self.load(id)?;
        let mut page = crate::observer::Observer::new(self.owner.store(), id)
            .watch(after, limit, interval)
            .await?;
        for record in &mut page.records {
            self.expand_event(id, &mut record.value)?;
        }
        Ok(page)
    }
    /// Admit fitted follow-up blocks atomically using a selected block-aware adapter.
    /// Busy inputs queue; a concurrent state or model change makes the commit
    /// fail and the caller may retry with the same request ID.
    pub async fn submit_fitted_user_blocks(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        blocks: Vec<serde_json::Value>,
        admission: &crate::images::UserImageAdmission,
        cancel: &CancellationToken,
    ) -> Result<crate::images::ImageAdmissionResult, Error> {
        self.submit_fitted_user_blocks_with_options(
            id,
            request_id,
            blocks,
            admission,
            AdmissionOptions::default(),
            cancel,
        )
        .await
    }
    /// Fit and admit steering or follow-up blocks with ordinary admission policies.
    pub async fn submit_fitted_user_blocks_with_options(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        blocks: Vec<serde_json::Value>,
        admission: &crate::images::UserImageAdmission,
        options: AdmissionOptions,
        cancel: &CancellationToken,
    ) -> Result<crate::images::ImageAdmissionResult, Error> {
        if request_id
            .as_deref()
            .is_some_and(|id| id.starts_with(crate::subagent::REPORT_REQUEST_PREFIX))
        {
            return Err(Error::Invalid(
                "reserved subagent reporter request ID".into(),
            ));
        }
        let (mut state, version) = self.load(id)?;
        if let Some(key) = request_id.as_ref() {
            if state
                .application_writes
                .iter()
                .any(|w| w.request_id.as_ref() == Some(key))
            {
                return Err(Error::Invalid(
                    "request ID identifies a write submission".into(),
                ));
            }
            if let Some(existing) = state
                .submissions
                .iter()
                .find(|s| s.request_id.as_ref() == Some(key))
            {
                if existing.kind != SubmissionKind::Input {
                    return Err(Error::Invalid(
                        "request ID identifies a continue submission".into(),
                    ));
                }
                return Ok(crate::images::ImageAdmissionResult {
                    submission_id: existing.id,
                    notes: existing.image_notes.clone(),
                });
            }
        }
        let busy = state.active.is_some()
            || pending_failure_continuation(&state)
            || matches!(
                state.phase,
                Phase::AfterRun { .. } | Phase::HookTasksWaiting { .. }
            );
        if busy && options.reject_when_busy {
            return Err(AdmissionError::Busy.into());
        }
        if !busy && options.reject_when_idle {
            return Err(AdmissionError::Idle.into());
        }
        if state.abort_requested {
            return Err(Error::Invalid("abort in progress".into()));
        }
        let place_now = matches!(state.phase, Phase::Idle | Phase::FailedIdle)
            && state.active.is_none()
            && state.pending_independent_summary.is_none()
            && !state
                .application_writes
                .iter()
                .any(|w| !w.placed && !w.withdrawn)
            && state.pending_resets.is_empty()
            && !state.submissions.iter().any(|s| s.status == Status::Queued);
        let adapter = self
            .provider_adapter
            .as_ref()
            .filter(|a| {
                state.provider_adapter.as_deref() == Some(a.id()) && a.supports_durable_blocks()
            })
            .ok_or_else(|| {
                Error::Invalid("image admission requires a selected block-aware adapter".into())
            })?;
        if !self.supports_context_blocks(&state) {
            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
        }
        if blocks.is_empty()
            || crate::content::UserContent::from_yield(
                serde_json::Value::Array(blocks.clone()),
                false,
            )
            .is_none()
        {
            return Err(Error::Invalid(
                "invalid or empty user content blocks".into(),
            ));
        }
        let profile = adapter.image_profile(&state.config.model);
        let limits = profile.map(|p| p.limits).unwrap_or_default();
        let fitted = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Interrupted),
            result = admission.fit(&blocks, limits) => result.map_err(Error::Invalid)?,
        };
        if cancel.is_cancelled() {
            return Err(Error::Interrupted);
        }
        if self
            .provider_adapter
            .as_ref()
            .filter(|a| state.provider_adapter.as_deref() == Some(a.id()))
            .and_then(|a| a.image_profile(&state.config.model))
            != profile
        {
            return Err(Error::Invalid(
                "model image profile changed during admission".into(),
            ));
        }
        let (mut output, notes) = match fitted {
            crate::images::FittedImages::Unchanged => (blocks, Vec::new()),
            crate::images::FittedImages::Fitted { content, notes } => (content, notes),
        };
        if !notes.is_empty() {
            output.push(serde_json::json!({"type":"text", "text":notes.join(" ")}));
        }
        if output.is_empty() {
            return Err(Error::Invalid(
                "image admission produced empty content".into(),
            ));
        }
        let sid = Uuid::new_v4();
        state.submissions.push(Submission {
            kind: SubmissionKind::Input,
            id: sid,
            request_id,
            content: String::new(),
            image_notes: notes.clone(),
            input_blocks: Some(crate::transcript::MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks: output.clone(),
            }),
            status: if place_now {
                Status::Running
            } else {
                Status::Queued
            },
        });
        state.input_modes.insert(sid, options.mode);
        if place_now {
            state.run_failure = None;
            state.failure_continuation_inputs.clear();
            state.active = Some(sid);
            let binding = state
                .submissions
                .last()
                .unwrap()
                .input_blocks
                .clone()
                .unwrap();
            let index = state.messages.len();
            state.messages.push(Message::User {
                content: String::new(),
            });
            state.content_blocks.insert(index, binding);
            state.auto_compaction_attempted = false;
            state.retry_state = RetryState::default();
            state.overflow_compaction_attempted = false;
            state.phase = Phase::ModelReady { turns: 0 };
        } else if matches!(state.phase, Phase::FailedIdle) {
            state.phase = Phase::Idle;
        }
        let merge = self.merge_admissions;
        self.merge_admissions = false;
        let saved = self.save(&state, Expected::Version(version));
        self.merge_admissions = merge;
        saved?;
        Ok(crate::images::ImageAdmissionResult {
            submission_id: sid,
            notes,
        })
    }

    /// Append an ordered user block occurrence to an idle conversation.
    /// Forks, resets, and supported compactions preserve retained block history.
    pub fn append_user_blocks(
        &mut self,
        id: Uuid,
        blocks: Vec<serde_json::Value>,
    ) -> Result<Uuid, Error> {
        if crate::content::UserContent::from_yield(serde_json::Value::Array(blocks.clone()), false)
            .is_none()
        {
            return Err(Error::Invalid("invalid user content blocks".into()));
        }
        let (mut state, version) = self.load(id)?;
        if !matches!(state.phase, Phase::Idle)
            || state.active.is_some()
            || state.abort_requested
            || !state.pending_resets.is_empty()
            || state
                .submissions
                .iter()
                .any(|s| matches!(s.status, Status::Queued | Status::Running))
        {
            return Err(Error::Invalid(
                "appending blocks requires an idle conversation".into(),
            ));
        }
        let message_id = Uuid::new_v4();
        let index = state.messages.len();
        state.messages.push(Message::User {
            content: String::new(),
        });
        state.content_blocks.insert(
            index,
            crate::transcript::MessageContentBlocks { message_id, blocks },
        );
        self.save(&state, Expected::Version(version))?;
        Ok(message_id)
    }
    /// Admission is durable before returning. Call `resume` to drive work now;
    /// opening the database also drives all work left by a previous process.
    /// Commit a summary intent while idle. Previous snapshots remain in history.
    pub fn request_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        self.request_compaction_with_instructions(id, keep_recent_tokens, None)
    }
    /// Commit an idle manual compaction with optional instructions for the
    /// summary and before-compact hooks. The instructions survive recovery.
    pub fn request_compaction_with_instructions(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
        instructions: Option<&str>,
    ) -> Result<bool, Error> {
        Ok(self
            .request_manual_compaction(id, keep_recent_tokens, instructions)?
            .is_some())
    }
    fn compaction_hooks_support_context(&self, c: &Conversation) -> Result<bool, Error> {
        let selected = self.selected_extensions(c)?;
        Ok(selected.supports_compaction_blocks()
            && (!c
                .content_blocks
                .keys()
                .any(|index| matches!(c.messages.get(*index), Some(Message::Tool { .. })))
                || selected.supports_compaction_tool_blocks()))
    }
    fn prepare_runtime_compaction(
        &self,
        c: &Conversation,
        keep_recent_tokens: usize,
        instructions: Option<&str>,
    ) -> Result<Option<crate::compact::Preparation>, Error> {
        if c.content_blocks.is_empty() {
            return Ok(crate::compact::prepare(
                &c.messages,
                c.config.model.clone(),
                c.config.max_output_tokens,
                keep_recent_tokens,
                instructions,
            )?);
        }
        if !c.occurrence_epoch_proven
            || c.transcript.is_none()
            || !self.compaction_hooks_support_context(c)?
        {
            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
        }
        let occurrences = self.resolved_occurrences(c.id, c.transcript.clone().unwrap())?;
        if occurrences
            .iter()
            .map(|(message, _)| message)
            .ne(c.messages.iter())
            || c.content_blocks.iter().any(|(index, binding)| {
                occurrences
                    .get(*index)
                    .and_then(|(_, history)| history.as_ref())
                    .map(|history| history.id)
                    != Some(binding.message_id)
            })
        {
            return Err(crate::transcript::Error::Corrupt.into());
        }
        crate::compact::prepare_with_blocks(
            &c.messages,
            c.config.model.clone(),
            c.config.max_output_tokens,
            keep_recent_tokens,
            instructions,
            &c.content_blocks,
        )?
        .map(|preparation| {
            preparation
                .bind_blocks(&c.content_blocks)
                .map_err(Error::from)
        })
        .transpose()
    }
    /// Start an idle manual compaction and return its durable operation ID.
    /// None means there is no older complete turn to compact.
    pub fn request_manual_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
        instructions: Option<&str>,
    ) -> Result<Option<Uuid>, Error> {
        let (mut c, version) = self.load(id)?;
        if !c.content_blocks.is_empty()
            && (!c.occurrence_epoch_proven
                || c.transcript.is_none()
                || !self.compaction_hooks_support_context(&c)?)
        {
            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
        }
        if !matches!(c.phase, Phase::Idle) || c.active.is_some() || c.abort_requested {
            return Err(Error::Invalid(
                "compaction requires an idle conversation".into(),
            ));
        }
        let Some(preparation) =
            self.prepare_runtime_compaction(&c, keep_recent_tokens, instructions)?
        else {
            return Ok(None);
        };
        let preparation = if let Some(source) =
            c.transcript.clone().filter(|_| c.occurrence_epoch_proven)
        {
            let occurrences = self.resolved_occurrences(id, source.clone())?;
            if occurrences
                .iter()
                .map(|(message, _)| message)
                .ne(c.messages.iter())
            {
                return Err(Error::Invalid(
                    "compaction source transcript changed".into(),
                ));
            }
            if let Some(history) = occurrences
                .get(preparation.first_kept())
                .and_then(|(_, history)| history.as_ref())
            {
                preparation.bind_target(source, history.id, c.resets.last().map(|reset| reset.id))
            } else {
                preparation
            }
        } else {
            preparation
        };
        let generation = *c.context_generation.get_or_insert_with(Uuid::new_v4);
        let operation_id = Uuid::new_v4();
        c.manual_compactions.push(ManualCompaction {
            id: operation_id,
            status: ManualCompactionStatus::Running,
        });
        c.active_manual_compaction = Some(operation_id);
        c.phase = Phase::CompactionHook {
            preparation: Box::new(preparation.bind_context(generation)),
            operation_id,
            reason: crate::extension::CompactionReason::Manual,
        };
        c.compaction_return_turns = None;
        self.save(&c, Expected::Version(version))?;
        Ok(Some(operation_id))
    }
    pub fn submit(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
    ) -> Result<Uuid, Error> {
        self.submit_with_mode(id, request_id, content, InputMode::FollowUp)
    }
    /// Persist an input; steering joins the active run at its next safe boundary.
    pub fn submit_with_mode(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        mode: InputMode,
    ) -> Result<Uuid, Error> {
        self.submit_with_options(
            id,
            request_id,
            content,
            AdmissionOptions {
                mode,
                ..Default::default()
            },
        )
    }
    /// Persist an input with optional busy and idle rejection policies.
    pub fn submit_with_options(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        options: AdmissionOptions,
    ) -> Result<Uuid, Error> {
        if request_id
            .as_deref()
            .is_some_and(|id| id.starts_with(crate::subagent::REPORT_REQUEST_PREFIX))
        {
            return Err(Error::Invalid(
                "reserved subagent reporter request ID".into(),
            ));
        }
        self.submit_internal_with_options(id, request_id, content, options, &mut |_| Ok(()), None)
    }
    /// Atomically admit a new input with checked documents and registered tasks.
    /// The callback may run again after a conflict and must have no external
    /// effects. Duplicate IDs and rejected inputs never invoke it.
    pub fn submit_with_documents(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        options: AdmissionOptions,
        mut change: impl FnMut(
            &mut crate::document::AdmissionDocuments<'_>,
        ) -> Result<(), crate::document::Error>,
    ) -> Result<Uuid, Error> {
        if request_id
            .as_deref()
            .is_some_and(|id| id.starts_with(crate::subagent::REPORT_REQUEST_PREFIX))
        {
            return Err(Error::Invalid(
                "reserved subagent reporter request ID".into(),
            ));
        }
        self.submit_internal_with_options(id, request_id, content, options, &mut change, None)
    }
    pub(crate) fn submit_reporter_input(
        &mut self,
        id: Uuid,
        request_id: String,
        content: String,
        mode: InputMode,
        parent: Uuid,
        generation: u64,
    ) -> Result<Uuid, Error> {
        self.submit_internal_with_options(
            id,
            Some(request_id),
            content,
            AdmissionOptions {
                mode,
                ..Default::default()
            },
            &mut |_| Ok(()),
            Some((parent, generation)),
        )
    }
    fn submit_internal_with_options(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        options: AdmissionOptions,
        change: &mut impl FnMut(
            &mut crate::document::AdmissionDocuments<'_>,
        ) -> Result<(), crate::document::Error>,
        fence: Option<(Uuid, u64)>,
    ) -> Result<Uuid, Error> {
        loop {
            let fence_check = if let Some((parent, generation)) = fence {
                let (current, check) =
                    crate::subagent::generation_check(self.owner.store(), parent)?;
                if current != generation {
                    return Err(Error::Invalid("stale reporter intent".into()));
                }
                Some(check)
            } else {
                None
            };
            let (mut c, version) = self.load(id)?;
            if !self.supports_context_blocks(&c) {
                return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
            }
            if request_id.as_ref().is_some_and(|key| {
                c.application_writes
                    .iter()
                    .any(|w| w.request_id.as_ref() == Some(key))
            }) {
                return Err(Error::Invalid(
                    "request ID identifies a write submission".into(),
                ));
            }
            if let Some(key) = request_id.as_ref()
                && let Some(s) = c
                    .submissions
                    .iter()
                    .find(|s| s.request_id.as_ref() == Some(key))
            {
                if s.kind != SubmissionKind::Input {
                    return Err(Error::Invalid(
                        "request ID identifies a continue submission".into(),
                    ));
                }
                if let Some(check) = fence_check {
                    match self.owner.store().commit(&[check], &[]) {
                        Ok(()) => (),
                        Err(durable_store::Error::Conflict { .. }) => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                return Ok(s.id);
            }
            if content.trim().is_empty() {
                return Err(Error::Invalid("empty input".into()));
            }
            let busy = c.active.is_some()
                || pending_failure_continuation(&c)
                || matches!(
                    c.phase,
                    Phase::AfterRun { .. } | Phase::HookTasksWaiting { .. }
                );
            if busy && options.reject_when_busy {
                return Err(AdmissionError::Busy.into());
            }
            if !busy && options.reject_when_idle {
                return Err(AdmissionError::Idle.into());
            }
            let sid = Uuid::new_v4();
            let place_now = matches!(c.phase, Phase::Idle | Phase::FailedIdle)
                && c.active.is_none()
                && c.pending_independent_summary.is_none()
                && !c
                    .application_writes
                    .iter()
                    .any(|w| !w.placed && !w.withdrawn)
                && c.pending_resets.is_empty()
                && !c
                    .submissions
                    .iter()
                    .any(|item| item.status == Status::Queued);
            c.submissions.push(Submission {
                kind: SubmissionKind::Input,
                id: sid,
                request_id: request_id.clone(),
                content: content.clone(),
                image_notes: Vec::new(),
                input_blocks: None,
                status: if place_now {
                    Status::Running
                } else {
                    Status::Queued
                },
            });
            c.input_modes.insert(sid, options.mode);
            if place_now {
                c.run_failure = None;
                c.failure_continuation_inputs.clear();
                c.active = Some(sid);
                c.messages.push(Message::User {
                    content: content.clone(),
                });
                c.auto_compaction_attempted = false;
                c.retry_state = RetryState::default();
                c.overflow_compaction_attempted = false;
                c.phase = Phase::ModelReady { turns: 0 };
            } else if matches!(c.phase, Phase::FailedIdle) {
                c.phase = Phase::Idle;
            }
            // Re-evaluate request-ID uniqueness after every concurrent admission.
            // Merging by submission UUID could otherwise admit the same key twice.
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let mut documents = crate::document::AdmissionDocuments::new(self.owner.store());
            if let Err(error) = change(&mut documents) {
                self.merge_admissions = merge;
                return Err(error.into());
            }
            let tasks = documents.take_tasks();
            let batch = documents.into_batch().map(|mut batch| {
                if let Some(check) = fence_check {
                    batch.checks.push(check);
                }
                batch
            });
            let result = match batch {
                Ok(batch) if tasks.is_empty() => self.save_composed_locked(
                    &c,
                    Expected::Version(version),
                    None,
                    &[],
                    &[],
                    Some(&batch),
                ),
                Ok(batch) => {
                    let driver = self
                        .task_driver
                        .lock()
                        .ok()
                        .and_then(|guard| guard.as_ref().and_then(|driver| driver.upgrade()));
                    match driver {
                        Some(driver) => driver.with_admission(&tasks, id, || {
                            self.save_composed_locked(
                                &c,
                                Expected::Version(version),
                                None,
                                &tasks,
                                &[],
                                Some(&batch),
                            )
                        }),
                        None => Err(Error::Invalid("task driver is not running".into())),
                    }
                }
                Err(error) => Err(error.into()),
            };
            self.merge_admissions = merge;
            match result {
                Ok(()) => return Ok(sid),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    /// Start a run from the current context without appending user input.
    pub fn submit_continue(&mut self, id: Uuid, request_id: Option<String>) -> Result<Uuid, Error> {
        if request_id
            .as_deref()
            .is_some_and(|id| id.starts_with(crate::subagent::REPORT_REQUEST_PREFIX))
        {
            return Err(Error::Invalid(
                "reserved subagent reporter request ID".into(),
            ));
        }
        loop {
            let (mut c, version) = self.load(id)?;
            if let Some(key) = request_id.as_ref() {
                if c.application_writes
                    .iter()
                    .any(|w| w.request_id.as_ref() == Some(key))
                {
                    return Err(Error::Invalid(
                        "request ID identifies a write submission".into(),
                    ));
                }
                if let Some(s) = c
                    .submissions
                    .iter()
                    .find(|s| s.request_id.as_ref() == Some(key))
                {
                    return if s.kind == SubmissionKind::Continue {
                        Ok(s.id)
                    } else {
                        Err(Error::Invalid(
                            "request ID identifies an input submission".into(),
                        ))
                    };
                }
            }
            if !self.supports_context_blocks(&c) {
                return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
            }
            if c.active.is_some()
                || c.abort_requested
                || pending_failure_continuation(&c)
                || c.pending_independent_summary.is_some()
                || c.application_writes
                    .iter()
                    .any(|w| !w.placed && !w.withdrawn)
                || !matches!(c.phase, Phase::Idle | Phase::FailedIdle)
            {
                return Err(AdmissionError::Busy.into());
            }
            if !c.pending_reset_blocks.is_empty() && c.pending_resets.len() > 1 {
                let marker = c.pending_resets.remove(0);
                place_reset(&mut c, marker);
                let merge = self.merge_admissions;
                self.merge_admissions = false;
                let result = self.save(&c, Expected::Version(version));
                self.merge_admissions = merge;
                match result {
                    Ok(()) | Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                    Err(error) => return Err(error),
                }
            }
            for marker in std::mem::take(&mut c.pending_resets) {
                place_reset(&mut c, marker);
            }
            place_queued(&mut c, true);
            let sid = Uuid::new_v4();
            c.submissions.push(Submission {
                kind: SubmissionKind::Continue,
                id: sid,
                request_id: request_id.clone(),
                content: String::new(),
                image_notes: Vec::new(),
                input_blocks: None,
                status: Status::Running,
            });
            c.run_failure = None;
            c.failure_continuation_inputs.clear();
            c.active = Some(sid);
            c.auto_compaction_attempted = false;
            c.retry_state = RetryState::default();
            c.overflow_compaction_attempted = false;
            c.phase = Phase::ModelReady { turns: 0 };
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let result = self.save(&c, Expected::Version(version));
            self.merge_admissions = merge;
            match result {
                Ok(()) => return Ok(sid),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
    }
    /// Withdraw an input or write only while queued. Request IDs remain reserved.
    pub fn withdraw_submission(
        &mut self,
        id: Uuid,
        submission: Uuid,
    ) -> Result<WithdrawResult, Error> {
        loop {
            let (mut c, version) = self.load(id)?;
            if let Some(item) = c.submissions.iter_mut().find(|item| item.id == submission) {
                match item.status {
                    Status::Queued => {
                        item.status = Status::Unanswered {
                            reason: "aborted".into(),
                        }
                    }
                    Status::Running => return Ok(WithdrawResult::AlreadyPlaced),
                    _ => return Ok(WithdrawResult::Settled),
                }
            } else if let Some(write) = c
                .application_writes
                .iter_mut()
                .find(|write| write.id == submission)
            {
                if write.placed || write.withdrawn {
                    return Ok(WithdrawResult::Settled);
                }
                write.withdrawn = true;
            } else {
                return Ok(WithdrawResult::NotFound);
            }
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let result = self.save(&c, Expected::Version(version));
            self.merge_admissions = merge;
            match result {
                Ok(()) => return Ok(WithdrawResult::Aborted),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
    }
    /// Persist the abort mark and withdraw the inputs queued at this commit.
    /// The caller must stop any executing worker before driving the mark.
    pub fn request_abort(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_owned_graph(Some(id), None).map(|_| ())
    }

    /// Persist an abort across background ownership boundaries. The caller must
    /// stop executing workers before driving the persisted marks.
    pub fn request_abort_with_background(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_owned_graph_impl(Some(id), None, None, true)
            .map(|_| ())
    }

    /// Atomically persist an abort for a task and its foreground owned graph.
    /// The caller must stop executing workers before driving these marks.
    pub fn request_abort_task(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_owned_graph(None, Some(id)).map(|_| ())
    }

    pub(crate) fn abort_owned_graph(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
    ) -> Result<Vec<Uuid>, Error> {
        self.abort_owned_graph_once(conversation, task, None)
    }

    pub(crate) fn abort_owned_graph_once(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
        receipt: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<Uuid>, Error> {
        self.abort_owned_graph_impl(conversation, task, receipt, false)
            .map(|(conversations, _, _)| conversations)
    }

    fn abort_owned_graph_impl(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
        receipt: Option<(Vec<u8>, Vec<u8>)>,
        background: bool,
    ) -> Result<(Vec<Uuid>, Vec<Uuid>, ConversationAbortResult), Error> {
        self.abort_owned_graph_impl_with(conversation, task, receipt, background, || {})
    }

    pub(crate) fn abort_nested_task_tools(&mut self, parent: Uuid) -> Result<Vec<Uuid>, Error> {
        self.abort_owned_graph_impl_roots(None, None, None, false, Some(parent), || {})
            .map(|(conversations, _, _)| conversations)
    }

    fn abort_owned_graph_impl_with(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
        receipt: Option<(Vec<u8>, Vec<u8>)>,
        background: bool,
        before_commit: impl FnMut(),
    ) -> Result<(Vec<Uuid>, Vec<Uuid>, ConversationAbortResult), Error> {
        self.abort_owned_graph_impl_roots(
            conversation,
            task,
            receipt,
            background,
            None,
            before_commit,
        )
    }

    fn abort_owned_graph_impl_roots(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
        receipt: Option<(Vec<u8>, Vec<u8>)>,
        background: bool,
        nested_parent: Option<Uuid>,
        mut before_commit: impl FnMut(),
    ) -> Result<(Vec<Uuid>, Vec<Uuid>, ConversationAbortResult), Error> {
        use crate::ownership_edge::{EdgeStore, Mode};
        use crate::task::TaskOwner;
        use std::collections::{HashMap, HashSet, VecDeque};
        let owner = self.owner.clone();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Failed)?;
        loop {
            let store = owner.store();
            // Capture before discovery; creation can commit without rewriting
            // the observed owner. The checked prefixes catch such phantoms.
            let graph_version = store.put_version()?;
            if let Some((key, _)) = &receipt
                && store.get(key)?.is_some()
            {
                return Ok((Vec::new(), Vec::new(), ConversationAbortResult::default()));
            }
            let records = crate::task_store::TaskStore::new(store)
                .list()
                .map_err(|e| Error::Invalid(format!("task list: {e}")))?;
            let by_id: HashMap<_, _> = records.iter().map(|r| (r.task.id(), r)).collect();
            if let Some(id) = task {
                let root = by_id.get(&id).ok_or(Error::NotFound)?;
                if root.task.is_terminal() {
                    return Err(Error::Invalid("task is terminal".into()));
                }
            }
            let mut conversations = HashSet::new();
            let mut tasks = HashSet::new();
            let mut cq = VecDeque::new();
            let mut tq = VecDeque::new();
            if let Some(id) = conversation {
                cq.push_back(id);
            }
            if let Some(id) = task {
                tq.push_back(id);
            }
            if let Some(parent) = nested_parent {
                let record = by_id.get(&parent).ok_or(Error::NotFound)?;
                if !record.task.is_task_tool() || !record.task.is_completing() {
                    return Err(Error::Invalid("task tool parent is not completing".into()));
                }
                for child in &records {
                    if child.task.owner() == &TaskOwner::Task(parent)
                        && child.task.is_task_tool()
                        && !child.task.is_terminal()
                    {
                        tq.push_back(child.task.id());
                    }
                }
            }
            let mut states = Vec::new();
            let mut edge_checks = Vec::new();
            while !cq.is_empty() || !tq.is_empty() {
                while let Some(id) = cq.pop_front() {
                    if !conversations.insert(id) {
                        continue;
                    }
                    let (state, version) = self.load(id)?;
                    states.push((state, version));
                    for r in &records {
                        if r.task.owner() == &TaskOwner::Conversation(id)
                            && (background || !r.task.is_background())
                        {
                            tq.push_back(r.task.id());
                        }
                    }
                }
                while let Some(id) = tq.pop_front() {
                    if !tasks.insert(id) {
                        continue;
                    }
                    let record = by_id.get(&id).ok_or(Error::NotFound)?;
                    for r in &records {
                        if r.task.owner() == &TaskOwner::Task(id)
                            && (background || !r.task.is_background())
                        {
                            tq.push_back(r.task.id());
                        }
                    }
                    for edge in EdgeStore::new(store)
                        .children(id)
                        .map_err(|e| Error::Invalid(format!("ownership edges: {e}")))?
                    {
                        if edge.edge.task_owner != *record.task.owner() {
                            return Err(Error::Invalid("ownership edge owner mismatch".into()));
                        }
                        edge_checks.push(Check {
                            key: crate::ownership_edge::edge_key(edge.edge.child),
                            expected: Expected::Version(edge.version),
                        });
                        if background || edge.edge.mode == Mode::Foreground {
                            cq.push_back(edge.edge.child);
                        }
                    }
                }
            }
            let mut updates = Vec::new();
            let mut generation_updates = Vec::new();
            for id in &tasks {
                let mut record = (*by_id[id]).clone();
                // Repeated aborts must not invalidate a running cleanup's
                // reservation by rewriting an unchanged task record.
                if !record.task.is_terminal() && !record.task.abort_requested() {
                    record
                        .task
                        .request_abort()
                        .map_err(|e| Error::Invalid(format!("task abort: {e:?}")))?;
                    updates.push(record);
                }
            }
            if background {
                for (state, _) in &states {
                    let (generation, check) = crate::subagent::generation_check(store, state.id)?;
                    edge_checks.push(check);
                    let next = generation
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("reporter generation exhausted".into()))?;
                    /* composed with graph marks below */
                    generation_updates.push((state.id, next));
                }
            }
            let mut withdrawn = Vec::new();
            for (state, _) in &mut states {
                for submission in &mut state.submissions {
                    if submission.status == Status::Queued {
                        withdrawn.push(WithdrawnInput {
                            conversation_id: state.id,
                            mode: state
                                .input_modes
                                .get(&submission.id)
                                .copied()
                                .unwrap_or(InputMode::FollowUp),
                            submission: submission.clone(),
                        });
                        submission.status = Status::Unanswered {
                            reason: "aborted".into(),
                        };
                    }
                }
                state.abort_requested = true;
            }
            let pending_writes: std::collections::HashMap<Uuid, Uuid> = states
                .iter()
                .filter_map(|(state, _)| {
                    state
                        .pending_independent_summary
                        .as_ref()
                        .map(|marker| (marker.id, state.id))
                })
                .collect();
            let (summary_checks, summary_mutations) =
                crate::summary_store::SummaryStore::new(store)
                    .prepare_owned_abort(&conversations, &pending_writes)
                    .map_err(|error| {
                        if matches!(error, crate::summary_store::Error::Store(_)) {
                            self.storage_failed.store(true, Ordering::Release);
                        }
                        Error::Invalid(format!("summary abort preparation: {error}"))
                    })?;
            let refs: Vec<_> = states
                .iter()
                .map(|(s, v)| (s, Expected::Version(*v)))
                .collect();
            let mut batch = self.prepare_saves(&refs)?;
            batch.extend(PreparedConversationSave {
                owner: owner.clone(),
                checks: summary_checks,
                mutations: summary_mutations,
            })?;
            let task_batch = crate::task_store::TaskStore::new(store)
                .prepare_save(
                    &updates
                        .iter()
                        .map(crate::task_store::Save::Update)
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| Error::Invalid(format!("task batch: {e}")))?;
            batch.checks.extend(task_batch.checks);
            // Check unchanged marked or terminal tasks too, so a child/edge
            // change committed since traversal forces a retry. Do not rewrite
            // them: that would invalidate an in-flight cleanup attempt.
            for id in &tasks {
                let record = by_id[id];
                if record.task.is_terminal() || record.task.abort_requested() {
                    batch.checks.push(Check {
                        key: crate::task_store::key(*id),
                        expected: Expected::Version(record.version),
                    });
                }
            }
            if let Some((key, value)) = &receipt {
                batch.checks.push(Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                });
                batch.mutations.push(Mutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
            batch.checks.extend(edge_checks);
            if let Some(parent) = nested_parent {
                batch.checks.push(Check {
                    key: crate::task_store::key(parent),
                    expected: Expected::Version(by_id[&parent].version),
                });
            }
            for owner in tasks
                .iter()
                .copied()
                .map(TaskOwner::Task)
                .chain(conversations.iter().copied().map(TaskOwner::Conversation))
            {
                let (check, mutation) = crate::ownership_edge::prepare_abort_fence(store, &owner)?;
                batch.checks.push(check);
                batch.mutations.push(mutation);
            }
            for (id, generation) in generation_updates {
                batch.mutations.push(Mutation::Put {
                    key: crate::subagent::generation_key(id),
                    value: serde_json::to_vec(&generation)?,
                });
            }
            batch.mutations.extend(task_batch.mutations);
            before_commit();
            match self.commit_conversation_save_inner(batch, Some(graph_version)) {
                Ok(()) => {
                    return Ok((
                        conversations.into_iter().collect(),
                        tasks.into_iter().collect(),
                        ConversationAbortResult { withdrawn },
                    ));
                }
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    fn conversation_tasks(&self, id: Uuid) -> Result<Vec<crate::task_store::StoredTask>, Error> {
        use crate::task::TaskOwner;
        let records = crate::task_store::TaskStore::new(self.owner.store())
            .list()
            .map_err(|e| Error::Invalid(format!("task list: {e}")))?;
        let mut ids = std::collections::HashSet::new();
        loop {
            let before = ids.len();
            for record in &records {
                if record.task.is_background() {
                    continue;
                }
                if record.task.owner() == &TaskOwner::Conversation(id)
                    || matches!(record.task.owner(), TaskOwner::Task(parent) if ids.contains(parent))
                {
                    ids.insert(record.task.id());
                }
            }
            if before == ids.len() {
                break;
            }
        }
        Ok(records
            .into_iter()
            .filter(|record| ids.contains(&record.task.id()) && !record.task.is_terminal())
            .collect())
    }
    /// Admit and run pending work immediately. Repeating a request ID returns
    /// the original submission, even when its answer was committed earlier.
    pub async fn submit_and_run(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        cancel: &CancellationToken,
    ) -> Result<Uuid, Error> {
        let submission = self.submit(id, request_id, content)?;
        self.resume(cancel).await?;
        Ok(submission)
    }
    fn conversations(&self) -> Result<Vec<Uuid>, Error> {
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let mut ids = Vec::new();
        for (_, entry) in self.owner.store().list(b"runtime/conversation/")? {
            let state: Conversation = serde_json::from_slice(&entry.value)?;
            ids.push(state.id);
        }
        Ok(ids)
    }
    fn recover(&mut self) -> Result<(), Error> {
        for id in self.conversations()? {
            self.recover_conversation(id)?;
        }
        Ok(())
    }
    fn recover_conversation(&mut self, id: Uuid) -> Result<(), Error> {
        {
            let (mut c, version) = self.load(id)?;
            self.report_blocked_tool_panic(&c)?;
            if c.abort_requested {
                return Ok(());
            }
            if !c.content_blocks.is_empty() && c.active_manual_compaction.is_none() {
                let valid = match &c.phase {
                    Phase::CompactionHook { preparation, .. }
                    | Phase::CompactionReady { preparation, .. }
                    | Phase::CompactionRunning { preparation, .. }
                    | Phase::CompactionAbandoned { preparation, .. } => preparation.matches_blocks(
                        &c.messages,
                        &c.content_blocks,
                        c.context_generation,
                    ),
                    Phase::CompactionPlacement { result, .. }
                    | Phase::CompactionAccounting { result, .. } => {
                        result.matches_blocks(&c.messages, &c.content_blocks, c.context_generation)
                    }
                    _ => true,
                };
                if !valid {
                    return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
                }
            }
            match c.phase.clone() {
                Phase::ModelRunning {
                    turns,
                    request_id,
                    restarts,
                } => {
                    let progress = crate::progress::Snapshot::load(self.owner.store(), request_id)?;
                    if let crate::progress::State::Running(partial) = progress.state()
                        && !partial.text.is_empty()
                        && !c
                            .aborted_generations
                            .iter()
                            .any(|entry| entry.request_id == request_id)
                    {
                        c.aborted_generations.push(AbortedGeneration {
                            request_id,
                            text: partial.text.clone(),
                        });
                    }
                    let n = restarts.saturating_add(1);
                    c.phase = Phase::AbandonedModel {
                        turns,
                        request_id,
                        restarts: n,
                    };
                }
                Phase::CompactionRunning {
                    preparation,
                    request_id,
                    restarts,
                } => {
                    c.phase = Phase::CompactionAbandoned {
                        preparation,
                        request_id,
                        restarts: restarts.saturating_add(1),
                    };
                }
                Phase::ToolRunning {
                    turns,
                    calls,
                    index,
                    operation_id,
                    replay,
                    arguments,
                    restarts,
                    replay_ready,
                    dispatch_identity: stored_identity,
                } => {
                    // An explicit suspended replay retains its marker until the
                    // parent finishes. Registry installation happens after open,
                    // so park it again and validate the selected tools on drive.
                    let panic_key = format!(
                        "runtime/extension-report/{id}/{operation_id}/tool_execution_panic"
                    );
                    let panicked = self.owner.store().get(panic_key.as_bytes())?.is_some();
                    if !c.abort_requested
                        && !panicked
                        && replay == ReplayPolicy::Safe
                        && stored_identity == Some(DispatchIdentity::Other)
                        && restarts.saturating_add(1) < self.options.restart_limit
                        && crate::nested::recoverable_gate_intent(self.owner.store(), operation_id)?
                    {
                        // A replay may have already received a checkpoint wake
                        // before committing another nested gate. Keep that wake
                        // across recovery, but never carry it across operations.
                        let pending_wake = c
                            .nested_resume
                            .as_ref()
                            .filter(|resume| resume.operation_id == operation_id)
                            .and_then(|resume| resume.pending_wake.clone());
                        c.nested_resume = Some(NestedResume {
                            operation_id,
                            restarts,
                            dispatch_identity: DispatchIdentity::Other,
                            explicit_suspend: true,
                            pending_wake,
                        });
                        c.phase = Phase::Blocked {
                            tool_panicked: false,
                            turns,
                            calls,
                            index,
                            operation_id,
                            reason: "interrupted before nested gate suspension".into(),
                            arguments: Some(arguments),
                        };
                        self.recovering = true;
                        let result = self.save(&c, Expected::Version(version));
                        self.recovering = false;
                        return result;
                    }
                    if !c.extensions.is_empty()
                        && !panicked
                        && replay == ReplayPolicy::Safe
                        && c.nested_resume.as_ref().is_some_and(|resume| {
                            resume.explicit_suspend && resume.operation_id == operation_id
                        })
                    {
                        c.phase = Phase::Blocked {
                            tool_panicked: false,
                            turns,
                            calls,
                            index,
                            operation_id,
                            reason: "interrupted suspended replay".into(),
                            arguments: Some(arguments),
                        };
                        self.recovering = true;
                        let result = self.save(&c, Expected::Version(version));
                        self.recovering = false;
                        return result;
                    }
                    let n = if replay_ready {
                        restarts
                    } else {
                        restarts.saturating_add(1)
                    };
                    // Extensions are installed after opening the store. Their selection
                    // could override a host tool, so never replay against a guessed registration.
                    let current = if c.extensions.is_empty() {
                        calls
                            .get(index)
                            .filter(|call| model_tool_selected(&c, &call.name))
                            .and_then(|x| self.tools.get(&x.name))
                    } else {
                        None
                    };
                    let identity_matches = match (stored_identity.as_ref(), current) {
                        (Some(stored), Some(tool)) => {
                            dispatch_identity(&tool.tool).is_ok_and(|identity| &identity == stored)
                        }
                        (Some(DispatchIdentity::Other), None) => true,
                        _ => false,
                    };
                    // A panic can be indexed before the blocked phase is committed,
                    // including while nested calls are being sealed. Never replay it.
                    let panic_key = format!(
                        "runtime/extension-report/{id}/{operation_id}/tool_execution_panic"
                    );
                    let panicked = self.owner.store().get(panic_key.as_bytes())?.is_some();
                    match if panicked || !identity_matches {
                        ToolRecovery::Reconcile
                    } else {
                        recover_tool(
                            replay,
                            current.map(|tool| tool.replay),
                            n,
                            self.options.restart_limit,
                            self.options.uncertain_policy,
                        )
                    } {
                        ToolRecovery::Replay => {
                            c.phase = Phase::ToolRunning {
                                turns,
                                calls,
                                index,
                                operation_id,
                                replay,
                                arguments,
                                restarts: n,
                                replay_ready: true,
                                dispatch_identity: stored_identity,
                            }
                        }
                        ToolRecovery::Reconcile => {
                            c.phase = Phase::Blocked {
                                tool_panicked: panicked,
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason: if !identity_matches { "interrupted; dispatch identity missing or changed; reconciliation required" } else { "interrupted; external outcome requires reconciliation" }.into(),
                                arguments: Some(arguments),
                            }
                        }
                        ToolRecovery::Interrupted | ToolRecovery::RestartLimit => {
                            let reason = if n >= self.options.restart_limit {
                                "tool restart limit"
                            } else {
                                "tool interrupted and may have partially run"
                            };
                            let reported = read_tool_diagnostics(&self.owner, c.id, operation_id)?;
                            record_tool_result(&mut c, &calls, index, operation_id, &Outcome::Failed(reason.into()), reported)?;
                            c.phase = Phase::ToolFinished {
                                turns,
                                calls,
                                index,
                                operation_id,
                                arguments,
                                content: reason.to_owned(),
                            };
                        }
                    }
                }
                _ => return Ok(()),
            }
            self.recovering = true;
            let result = self.save(&c, Expected::Version(version));
            self.recovering = false;
            result?;
        }
        Ok(())
    }
    /// Drive committed pending work. Calling again is safe after success; after
    /// interruption the owner must be dropped and reopened before retrying.
    pub async fn resume(&mut self, cancel: &CancellationToken) -> Result<(), Error> {
        let mut guard = ResumeGuard {
            dirty: self.dirty.clone(),
            complete: false,
        };
        let ids = self.conversations()?;
        let results = futures_util::future::join_all(ids.into_iter().map(|id| {
            let mut worker = self.worker();
            async move { worker.drive(id, cancel).await }
        }))
        .await;
        let mut first_error = None;
        for result in results {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        guard.complete = true;
        Ok(())
    }
    /// Drive one conversation, allowing hosts to work around another
    /// conversation that repeatedly fails. A failed attempt still requires
    /// reopening before further calls.
    pub async fn resume_conversation(
        &mut self,
        id: Uuid,
        cancel: &CancellationToken,
    ) -> Result<(), Error> {
        let mut guard = ResumeGuard {
            dirty: self.dirty.clone(),
            complete: false,
        };
        self.drive(id, cancel).await?;
        guard.complete = true;
        Ok(())
    }
    /// Release queued submissions held by the default failed-run policy.
    pub fn continue_queued(&mut self, id: Uuid) -> Result<(), Error> {
        let (mut c, version) = self.load(id)?;
        if matches!(c.phase, Phase::FailedIdle) {
            c.phase = Phase::Idle;
            self.save(&c, Expected::Version(version))?;
        }
        Ok(())
    }
    fn task_tool_result(
        &self,
        conversation: Uuid,
        operation: Uuid,
        task: Uuid,
    ) -> Result<Option<Outcome>, Error> {
        let child = crate::task_store::TaskStore::new(self.owner.store())
            .load(task)
            .map_err(|e| Error::Invalid(format!("task load: {e}")))?
            .ok_or_else(|| Error::Invalid("task-backed tool task missing".into()))?;
        if task != operation
            || child.task.owner() != &crate::task::TaskOwner::Conversation(conversation)
        {
            return Err(Error::Invalid("task-backed tool owner mismatch".into()));
        }
        Ok(match child.task.state() {
            crate::task::TaskState::Succeeded(value) => Some(Outcome::Succeeded(
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            )),
            // Generic task failures do not distinguish a handler-reported error
            // from an interrupted effect. Do not turn uncertainty into a known result.
            crate::task::TaskState::Failed(reason) => Some(Outcome::Unknown(reason.clone())),
            crate::task::TaskState::Cancelled => Some(Outcome::Failed("task cancelled".into())),
            _ => None,
        })
    }
    fn validate_pending_task_identity(
        &self,
        c: &Conversation,
        calls: &[ToolCall],
        index: usize,
        identity: &Option<crate::task_tool::Identity>,
    ) -> Result<(), Error> {
        let current = calls
            .get(index)
            .and_then(|call| self.resolved_tools(c).ok()?.get(&call.name).cloned())
            .and_then(|reg| crate::task_tool::lookup(&reg.tool))
            .and_then(|adapter| adapter.identity());
        if identity.is_none() || current != *identity {
            return Err(Error::Invalid(
                "task adapter identity missing or changed".into(),
            ));
        }
        Ok(())
    }
    async fn settle_abort(
        &mut self,
        mut c: Conversation,
        version: u64,
        cancel: &CancellationToken,
    ) -> Result<bool, Error> {
        // Keep the conversation marked until all owned cleanup has finished.
        let summaries = crate::summary_store::SummaryStore::new(self.owner.store())
            .list()
            .map_err(|e| Error::Invalid(format!("summary abort lookup: {e}")))?;
        if let Some(summary) = summaries.into_iter().find(|s| {
            s.operation.session_id() == c.id
                && s.operation.is_live_owned()
                && !c
                    .pending_independent_summary
                    .as_ref()
                    .is_some_and(|marker| {
                        marker.id == s.operation.id()
                            && matches!(
                                s.operation.state(),
                                crate::summary_store::State::PlacementPending { .. }
                            )
                    })
        }) {
            if !matches!(
                summary.operation.state(),
                crate::summary_store::State::Aborting { .. }
            ) {
                return Err(Error::Invalid("owned summary has no abort mark".into()));
            }
            let provider = self.summary_provider.clone().ok_or_else(|| {
                Error::Invalid(
                    "summary provider identity is not configured for abort cleanup".into(),
                )
            })?;
            if summary.operation.provider() != Some(&provider) {
                return Err(Error::Invalid(
                    "summary provider identity does not match abort cleanup".into(),
                ));
            }
            crate::summary_worker::run(
                self.owner.clone(),
                self.options.model.clone(),
                summary.operation.id(),
                cancel.clone(),
                provider,
                None,
            )
            .await
            .map_err(|e| match e {
                crate::summary_worker::Error::Interrupted => Error::Interrupted,
                crate::summary_worker::Error::Store(crate::summary_store::Error::Store(error)) => {
                    self.storage_failed.store(true, Ordering::Release);
                    Error::Storage(error)
                }
                other => Error::Invalid(format!("summary abort cleanup: {other}")),
            })?;
            return Ok(true);
        }
        if !self.conversation_tasks(c.id)?.is_empty() {
            return Ok(false);
        }
        // A blocked operation remains a barrier until an authoritative result arrives.
        if matches!(c.phase, Phase::Blocked { .. }) && c.active.is_none() {
            return Ok(false);
        }
        let usage =
            match &c.phase {
                Phase::ModelRunning { request_id, .. }
                | Phase::AbandonedModel { request_id, .. }
                | Phase::FailedModel { request_id, .. } => {
                    Some((*request_id, UsageReport::Unconfirmed, None))
                }
                Phase::CompactionRunning { request_id, .. }
                | Phase::CompactionAbandoned { request_id, .. } => {
                    Some((*request_id, UsageReport::Unconfirmed, None))
                }
                Phase::Accounting {
                    request_id,
                    response,
                    ..
                } => Some((
                    *request_id,
                    response.usage.map_or(UsageReport::Unconfirmed, |usage| {
                        UsageReport::Measured { usage }
                    }),
                    Some(response.metadata.clone()),
                )),
                Phase::CompactionAccounting { request_id, result } => Some((
                    *request_id,
                    result
                        .generation
                        .usage
                        .map_or(UsageReport::Unconfirmed, |usage| UsageReport::Measured {
                            usage,
                        }),
                    Some(result.generation.metadata.clone()),
                )),
                _ => None,
            };
        if let Some((request_id, report, metadata)) = usage {
            let snapshot = crate::progress::Snapshot::load(self.owner.store(), request_id)?;
            if let crate::progress::State::Running(partial)
            | crate::progress::State::Aborted(partial) = snapshot.state()
                && !partial.text.is_empty()
                && !c
                    .aborted_generations
                    .iter()
                    .any(|p| p.request_id == request_id)
            {
                c.aborted_generations.push(AbortedGeneration {
                    request_id,
                    text: partial.text.clone(),
                });
            }
            let settlement = async {
                match metadata {
                    Some(metadata) => {
                        self.options
                            .model
                            .record_usage_with_metadata(request_id, &report, &metadata)
                            .await
                    }
                    None => self.options.model.record_usage(request_id, &report).await,
                }
            };
            tokio::select! {
                _ = cancel.cancelled() => return Err(Error::Interrupted),
                result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                    Ok(Ok(())) => (), Ok(Err(error)) => return Err(error.into()), Err(_) => return Err(Error::Interrupted),
                }
            }
        }
        match c.phase.clone() {
            Phase::ToolRunning {
                turns,
                calls,
                index,
                operation_id,
                arguments,
                ..
            } => {
                c.phase = Phase::Blocked {
                    tool_panicked: false,
                    turns,
                    calls,
                    index,
                    operation_id,
                    reason: "local abort after dispatch; external outcome requires reconciliation"
                        .into(),
                    arguments: Some(arguments),
                };
            }
            Phase::TaskPending {
                calls,
                index,
                operation_id,
                task,
                turns,
                arguments,
                identity,
            } => {
                self.validate_pending_task_identity(&c, &calls, index, &identity)?;
                let outcome = self
                    .task_tool_result(c.id, operation_id, task)?
                    .ok_or_else(|| Error::Invalid("task-backed tool cleanup incomplete".into()))?;
                if let Outcome::Unknown(reason) = outcome {
                    c.phase = Phase::Blocked {
                        tool_panicked: false,
                        turns,
                        calls,
                        index,
                        operation_id,
                        reason,
                        arguments: Some(arguments),
                    };
                } else {
                    let reported = read_tool_diagnostics(&self.owner, c.id, operation_id)?;
                    record_tool_result(&mut c, &calls, index, operation_id, &outcome, reported)?;
                    let content = model_tool_content(&c.tool_filters, &calls, index, &outcome);
                    self.append_tool_with_status(&mut c, &calls, index, operation_id, content)?;
                    for i in index + 1..calls.len() {
                        self.append_skipped_with_status(
                            &mut c,
                            &calls,
                            i,
                            "aborted before dispatch",
                        )?;
                    }
                }
            }
            Phase::Pending {
                turns,
                calls,
                index,
                operation_id,
                reference,
                arguments,
            } => {
                c.phase = Phase::Blocked {
                    tool_panicked: false,
                    turns,
                    calls,
                    index,
                    operation_id,
                    reason: format!(
                        "local abort; external operation pending ({reference}); reconciliation required"
                    ),
                    arguments,
                };
            }
            Phase::Blocked { .. } => (),
            Phase::ToolFinished {
                calls,
                index,
                operation_id,
                mut content,
                ..
            } => {
                if let Some(mut metadata) = c.pending_tool_result.take() {
                    if !c.extensions.is_empty() {
                        content = "tool result unavailable after abort before hooks".into();
                        redact_tool_metadata(
                            &mut metadata,
                            "hook_not_completed",
                            "tool result metadata withheld because after-tool hooks did not complete",
                        );
                    }
                    c.tool_results.push(metadata);
                }
                // The effect already committed; retain its result even if an
                // after-tool hook was interrupted by the abort.
                self.append_tool_with_status(&mut c, &calls, index, operation_id, content)?;
                for i in index + 1..calls.len() {
                    self.append_skipped_with_status(&mut c, &calls, i, "aborted before dispatch")?;
                }
            }
            Phase::Tools { calls, index, .. }
            | Phase::ToolPreparing { calls, index, .. }
            | Phase::ToolGateSleeping { calls, index, .. } => {
                for i in index..calls.len() {
                    self.append_skipped_with_status(&mut c, &calls, i, "aborted before dispatch")?;
                }
            }
            _ => (),
        }
        if matches!(c.phase, Phase::Blocked { .. }) {
            c.active = None;
            for s in &mut c.submissions {
                if s.status == Status::Running {
                    s.status = Status::Unanswered {
                        reason: "aborted".into(),
                    };
                }
            }
        } else {
            finish_manual_compaction(&mut c, ManualCompactionStatus::Aborted)?;
            c.abort_requested = false;
            finish_abort(&mut c);
        }
        self.save(&c, Expected::Version(version))?;
        Ok(true)
    }
    async fn run_lifecycle_hook(
        &self,
        c: &Conversation,
        operation_id: Uuid,
        event: crate::extension::GenerationEvent,
        value: serde_json::Value,
        cancel: &CancellationToken,
    ) -> Result<
        Option<(
            Option<serde_json::Value>,
            Vec<crate::task::Task>,
            Vec<Arc<dyn crate::task_scheduler::Handler>>,
        )>,
        Error,
    > {
        if c.extensions.is_empty() {
            return Ok(Some((None, vec![], vec![])));
        }
        let selected = self.selected_extensions(c)?;
        let mut snapshot = serde_json::to_value(c)?;
        if let Some(profile) = self
            .provider_adapter
            .as_ref()
            .filter(|a| c.provider_adapter.as_deref() == Some(a.id()))
            .and_then(|a| a.image_profile(&c.config.model))
        {
            snapshot["_model_image_profile"] = serde_json::to_value(profile)?;
        }
        let memo = self.extension_memo(operation_id)?;
        let context = crate::extension::Context {
            snapshot: &snapshot,
            memo: &memo,
        };
        let (_, continuation, tasks, reports) = fenced_poll(
            selected.lifecycle_generation(event, value, &context, self.supports_blocks(c)),
            self.execution_gate.as_ref(),
            cancel,
        )
        .await
        .ok_or(Error::Interrupted)?;
        self.subagent_store()?;
        self.record_extension_reports(c.id, operation_id, &format!("{event:?}"), reports)?;
        if tasks.is_empty() {
            return Ok(Some((continuation, vec![], vec![])));
        }
        let driver = self
            .task_driver
            .lock()
            .map_err(|_| Error::Failed)?
            .as_ref()
            .and_then(|driver| driver.upgrade());
        let Some(driver) = driver else {
            return Ok(None);
        };
        let mut children = Vec::with_capacity(tasks.len());
        let mut handlers = Vec::with_capacity(tasks.len());
        for task in tasks {
            let handler = selected.task(&task.name).ok_or_else(|| {
                Error::Invalid(format!("unselected task registration: {}", task.name))
            })?;
            let replay = driver.policy_for(&task.name, &handler).ok_or_else(|| {
                Error::Invalid(format!("task driver registration mismatch: {}", task.name))
            })?;
            let child = crate::task::Task::new(
                Uuid::new_v4(),
                crate::task::TaskOwner::Conversation(c.id),
                crate::task::Checkpoint {
                    phase: "initial".into(),
                    data: task.input,
                },
                self.options.restart_limit,
            )
            .map_err(|e| Error::Invalid(format!("task creation: {e:?}")))?
            .registered(task.name, replay);
            children.push(child);
            handlers.push(handler);
        }
        Ok(Some((continuation, children, handlers)))
    }
    fn prepare_nested_resume(&self, c: &mut Conversation) -> Result<bool, Error> {
        if c.abort_requested {
            return Ok(false);
        }
        let Some(resume) = c.nested_resume.clone() else {
            return Ok(false);
        };
        let Phase::Blocked {
            tool_panicked: false,
            turns,
            calls,
            index,
            operation_id,
            arguments: Some(arguments),
            ..
        } = c.phase.clone()
        else {
            return Ok(false);
        };
        if operation_id != resume.operation_id
            || resume.restarts.saturating_add(1) >= self.options.restart_limit
        {
            return Ok(false);
        }
        let Some(call) = calls.get(index) else {
            return Ok(false);
        };
        if !model_tool_selected(c, &call.name) {
            return Ok(false);
        }
        let tools = self.resolved_tools(c)?;
        let Some(reg) = tools.get(&call.name) else {
            return Ok(false);
        };
        if reg.replay != ReplayPolicy::Safe
            || dispatch_identity(&reg.tool).ok() != Some(resume.dispatch_identity.clone())
            || (resume.explicit_suspend
                && crate::nested::has_staged_results(self.owner.store(), operation_id)?)
            || !crate::nested::resolved_for_replay(self.owner.store(), operation_id)?
        {
            return Ok(false);
        }
        if resume.explicit_suspend && (!c.extensions.is_empty() || resume.pending_wake.is_some()) {
            c.nested_resume.as_mut().unwrap().restarts = resume.restarts.saturating_add(1);
        } else {
            c.nested_resume = None;
        }
        c.phase = Phase::ToolRunning {
            turns,
            calls,
            index,
            operation_id,
            arguments,
            replay: ReplayPolicy::Safe,
            restarts: resume.restarts.saturating_add(1),
            replay_ready: true,
            dispatch_identity: Some(resume.dispatch_identity),
        };
        Ok(true)
    }
    fn consume_independent_summary(
        &mut self,
        state: &mut Conversation,
        version: u64,
    ) -> Result<(), Error> {
        use crate::summary_store::{State, SummaryStore};
        let marker = state
            .pending_independent_summary
            .take()
            .ok_or(Error::NotFound)?;
        let owner = self.owner.clone();
        let store = SummaryStore::new(owner.store());
        let snap = store
            .get(marker.id)
            .map_err(|e| Error::Invalid(format!("summary: {e}")))?
            .ok_or(Error::NotFound)?;
        if snap.operation.session_id() != state.id {
            return Err(Error::Invalid("summary owner mismatch".into()));
        }
        if snap.operation.provider() != self.summary_provider.as_ref()
            || self.summary_provider.is_none()
        {
            return Err(Error::Invalid(
                "summary provider identity is missing or does not match".into(),
            ));
        }
        let result = match snap.operation.state() {
            State::PlacementPending { result, .. } => result,
            State::Aborted | State::Aborting { .. } => {
                return self.save(state, Expected::Version(version));
            }
            _ => return Err(Error::Invalid("invalid pending summary state".into())),
        };
        store
            .placement_ticket(&snap)
            .map_err(|e| Error::Invalid(format!("summary: {e}")))?;
        if marker.resets_before.len() > SUMMARY_MAX_HISTORY_RECORDS
            || marker.resets_before.len() > state.pending_resets.len()
            || !marker
                .resets_before
                .iter()
                .zip(&state.pending_resets)
                .all(|(id, reset)| *id == reset.id)
            || marker.writes > state.application_writes.len()
        {
            return Err(Error::Invalid(
                "independent summary ordering is inconsistent".into(),
            ));
        }
        if marker.resets_before.len() > 1 && !state.pending_reset_blocks.is_empty() {
            let reset = state.pending_resets.remove(0);
            let mut remaining = marker;
            remaining.resets_before.remove(0);
            let writes = remaining.writes;
            state.pending_independent_summary = Some(remaining);
            place_reset_before_writes(state, reset, writes);
            let merge = self.merge_admissions;
            self.merge_admissions = false;
            let result = self.save(state, Expected::Version(version));
            self.merge_admissions = merge;
            return result;
        }
        for reset in state
            .pending_resets
            .drain(..marker.resets_before.len())
            .collect::<Vec<_>>()
        {
            place_reset_before_writes(state, reset, marker.writes);
        }
        let placed = state.submissions.len() >= marker.submissions
            && state.application_writes.len() >= marker.writes
            && state.resets.len() == marker.resets
            && !state.abort_requested
            && (if result.has_blocks() {
                result.matches_blocks_prefix(
                    &state.messages,
                    &state.content_blocks,
                    state.context_generation,
                )
            } else {
                state.content_blocks.is_empty()
            })
            && !result.is_stale(&state.messages, state.context_generation);
        if placed {
            if result.has_blocks() {
                let (system_end, first_kept) = result.retained_range();
                state.content_blocks =
                    project_compaction_blocks(&state.content_blocks, system_end, first_kept)?;
            }
            state.messages = result.apply_in_context(&state.messages, state.context_generation)?;
            state.context_generation = Some(Uuid::new_v4());
        }
        let mut batch = self.prepare_conversation_save_inner(
            state,
            Expected::Version(version),
            None,
            &[],
            &[],
            if placed { Some(result) } else { None },
        )?;
        let (checks, mutations) = store
            .placement_batch(&snap, placed)
            .map_err(|e| Error::Invalid(format!("summary: {e}")))?;
        batch.extend(PreparedConversationSave {
            owner,
            checks,
            mutations,
        })?;
        let bytes = batch
            .mutations
            .iter()
            .map(|m| match m {
                Mutation::Put { key, value } => key.len().saturating_add(value.len()),
                Mutation::Delete { key } => key.len(),
            })
            .fold(0usize, usize::saturating_add);
        if bytes > SUMMARY_MAX_BATCH_BYTES || batch.checks.len() > 64 || batch.mutations.len() > 64
        {
            return Err(Error::Invalid(
                "independent summary placement batch exceeds limits".into(),
            ));
        }
        self.commit_conversation_save(batch)
    }

    async fn drive(&mut self, id: Uuid, cancel: &CancellationToken) -> Result<(), Error> {
        loop {
            if cancel.is_cancelled() {
                self.failed = true;
                return Err(Error::Interrupted);
            }
            let (mut c, version) = self.load(id)?;
            self.report_blocked_tool_panic(&c)?;
            if c.abort_requested {
                if self.settle_abort(c, version, cancel).await? {
                    continue;
                }
                return Ok(());
            }
            // A completed run's final boundary follows its after-run hook.
            // A yield continuation still places pending writes before resuming.
            let finish_run_before_summary = match &c.phase {
                Phase::Boundary {
                    answer: Some(_), ..
                } => true,
                Phase::YieldDecision { continuation, .. } => {
                    continuation.is_none()
                        || !c.pending_resets.is_empty()
                        || c.submissions.iter().any(|s| s.status == Status::Queued)
                }
                _ => false,
            };
            if c.pending_independent_summary.is_some()
                && !finish_run_before_summary
                && matches!(
                    c.phase,
                    Phase::Boundary { .. }
                        | Phase::YieldDecision { .. }
                        | Phase::Idle
                        | Phase::FailedIdle
                )
            {
                match self.consume_independent_summary(&mut c, version) {
                    Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                    other => other?,
                }
                continue;
            }
            if c.pending_independent_summary.is_none()
                && matches!(
                    c.phase,
                    Phase::Boundary { .. }
                        | Phase::YieldDecision { .. }
                        | Phase::Idle
                        | Phase::FailedIdle
                )
            {
                let preceding_reset = c.resets.last().map(|reset| reset.id);
                let mut changed = false;
                for write in &mut c.application_writes {
                    if !write.placed && !write.withdrawn && write.after_reset == preceding_reset {
                        write.placed = true;
                        changed = true;
                    }
                }
                if changed {
                    match self.save(&c, Expected::Version(version)) {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
                    continue;
                }
            }
            if self.prepare_nested_resume(&mut c)? {
                match self.save(&c, Expected::Version(version)) {
                    Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                    other => other?,
                }
                continue;
            }
            match c.phase.clone() {
                Phase::Idle => {
                    if !c.pending_resets.is_empty() {
                        if c.pending_reset_blocks.is_empty() {
                            for marker in take_reset_batch(&mut c) {
                                place_reset(&mut c, marker);
                            }
                        } else {
                            let step = ResetDerivationStep {
                                reset_id: c.pending_resets[0].id,
                                context_generation: Uuid::new_v4(),
                                write_limit: usize::MAX,
                            };
                            c = match self.derive_anchored_reset_states(id, version, &[step]) {
                                Ok(mut cuts) => cuts.pop().ok_or_else(|| {
                                    Error::Invalid("missing reset derivation".into())
                                })?,
                                Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                                    continue;
                                }
                                Err(error) => return Err(error),
                            };
                        }
                        let merge = self.merge_admissions;
                        self.merge_admissions = false;
                        let result = self.save(&c, Expected::Version(version));
                        self.merge_admissions = merge;
                        match result {
                            Ok(()) | Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                                continue;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    place_queued(&mut c, true);
                    if c.active.is_none() {
                        return Ok(());
                    }
                    c.auto_compaction_attempted = false;
                    c.retry_state = RetryState::default();
                    c.overflow_compaction_attempted = false;
                    c.phase = Phase::ModelReady { turns: 0 };
                    // Placement races with withdrawal and queued resets. Re-read
                    // rather than merging a stale placement over a newer inbox.
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let result = self.save(&c, Expected::Version(version));
                    self.merge_admissions = merge;
                    match result {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
                }
                Phase::CompactionHook {
                    preparation,
                    operation_id,
                    reason,
                } => {
                    let selected = self.selected_extensions(&c)?;
                    if preparation.has_blocks()
                        && (!selected.supports_compaction_blocks()
                            || (preparation.has_tool_blocks()
                                && !selected.supports_compaction_tool_blocks()))
                    {
                        return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
                    }
                    let snapshot = serde_json::to_value(&c)?;
                    let memo = self.extension_memo(operation_id)?;
                    let context = crate::extension::Context {
                        snapshot: &snapshot,
                        memo: &memo,
                    };
                    let input = preparation.hook_input(reason);
                    let (decision, reports) = fenced_poll(
                        selected.before_compact(&input, &context),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?;
                    self.subagent_store()?;
                    self.record_extension_reports(c.id, operation_id, "before_compact", reports)?;
                    match decision {
                        crate::extension::BeforeCompact::Pass => {
                            c.phase = Phase::CompactionReady {
                                preparation,
                                restarts: 0,
                            }
                        }
                        crate::extension::BeforeCompact::Decline => {
                            finish_manual_compaction(&mut c, ManualCompactionStatus::Declined)?;
                            c.phase = c
                                .compaction_return_turns
                                .take()
                                .map_or(Phase::Idle, |turns| Phase::ModelReady { turns })
                        }
                        crate::extension::BeforeCompact::Summary(summary) => {
                            let result = (*preparation).finish(CompletedGeneration {
                                content: summary,
                                calls: vec![],
                                usage: None,
                                metadata: Default::default(),
                            })?;
                            c.phase = Phase::CompactionPlacement {
                                result: Box::new(result),
                                resets_before: c.active_manual_compaction.map(|_| {
                                    c.pending_resets.iter().map(|reset| reset.id).collect()
                                }),
                            };
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionReady {
                    preparation,
                    restarts,
                } => {
                    c.phase = Phase::CompactionRunning {
                        preparation,
                        request_id: Uuid::new_v4(),
                        restarts,
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionRunning {
                    preparation,
                    request_id,
                    ..
                } => {
                    let result = match fenced_poll(
                        (*preparation).summarize(
                            self.options.model.as_ref(),
                            request_id,
                            id,
                            cancel,
                        ),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?
                    {
                        Ok(value) => value,
                        Err(error) => {
                            self.failed = true;
                            return Err(error.into());
                        }
                    };
                    c.phase = Phase::CompactionAccounting {
                        result: Box::new(result),
                        request_id,
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionAbandoned {
                    preparation,
                    request_id,
                    restarts,
                } => {
                    let settlement = self
                        .options
                        .model
                        .record_usage(request_id, &UsageReport::Unconfirmed);
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (), Ok(Err(error)) => { self.failed = true; return Err(error.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); }
                        }
                    }
                    if restarts >= self.options.restart_limit {
                        finish_manual_compaction(&mut c, ManualCompactionStatus::RestartLimit)?;
                        c.phase = c
                            .compaction_return_turns
                            .take()
                            .map_or(Phase::Idle, |turns| Phase::ModelReady { turns });
                    } else {
                        c.phase = Phase::CompactionReady {
                            preparation,
                            restarts,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionAccounting { result, request_id } => {
                    let usage = result
                        .generation
                        .usage
                        .map_or(UsageReport::Unconfirmed, |usage| UsageReport::Measured {
                            usage,
                        });
                    let settlement = self.options.model.record_usage_with_metadata(
                        request_id,
                        &usage,
                        &result.generation.metadata,
                    );
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        value = tokio::time::timeout(Duration::from_secs(120), settlement) => match value {
                            Ok(Ok(())) => (), Ok(Err(error)) => { self.failed = true; return Err(error.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); }
                        }
                    }
                    c.phase = Phase::CompactionPlacement {
                        result,
                        resets_before: c
                            .active_manual_compaction
                            .map(|_| c.pending_resets.iter().map(|reset| reset.id).collect()),
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::CompactionPlacement {
                    result,
                    resets_before,
                } => {
                    if let Some(before) = resets_before {
                        if before.len() > c.pending_resets.len()
                            || !before
                                .iter()
                                .zip(&c.pending_resets)
                                .all(|(id, reset)| *id == reset.id)
                        {
                            return Err(Error::Invalid(
                                "compaction reset ordering is inconsistent".into(),
                            ));
                        }
                        if before.len() > 1 && !c.pending_reset_blocks.is_empty() {
                            let reset = c.pending_resets.remove(0);
                            let mut remaining = before;
                            remaining.remove(0);
                            place_reset(&mut c, reset);
                            c.phase = Phase::CompactionPlacement {
                                result,
                                resets_before: Some(remaining),
                            };
                            let merge = self.merge_admissions;
                            self.merge_admissions = false;
                            let saved = self.save(&c, Expected::Version(version));
                            self.merge_admissions = merge;
                            match saved {
                                Ok(())
                                | Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        let prior: Vec<_> = c.pending_resets.drain(..before.len()).collect();
                        for reset in prior {
                            place_reset(&mut c, reset);
                        }
                    }
                    if c.active_manual_compaction.is_some() {
                        if let Some((messages, (system_end, first_kept))) =
                            self.apply_manual_compaction_target(&c, &result)?
                        {
                            if result.has_blocks() {
                                c.content_blocks = project_compaction_blocks(
                                    &c.content_blocks,
                                    system_end,
                                    first_kept,
                                )?;
                            }
                            c.messages = messages;
                            c.context_generation = Some(Uuid::new_v4());
                            finish_manual_compaction(&mut c, ManualCompactionStatus::Applied)?;
                        } else {
                            finish_manual_compaction(&mut c, ManualCompactionStatus::Stale)?;
                        }
                    } else {
                        if result.has_blocks() {
                            if !result.matches_blocks(
                                &c.messages,
                                &c.content_blocks,
                                c.context_generation,
                            ) {
                                return Err(
                                    crate::transcript::Error::UnsupportedContentBlocks.into()
                                );
                            }
                            let (system_end, first_kept) = result.retained_range();
                            c.content_blocks = project_compaction_blocks(
                                &c.content_blocks,
                                system_end,
                                first_kept,
                            )?;
                        } else if !c.content_blocks.is_empty() {
                            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
                        }
                        c.messages = result.apply_in_context(&c.messages, c.context_generation)?;
                        c.context_generation = Some(Uuid::new_v4());
                    }
                    c.phase = c
                        .compaction_return_turns
                        .take()
                        .map_or(Phase::Idle, |turns| Phase::ModelReady { turns });
                    // Placement must be decided again if an admission or abort
                    // commits after this snapshot, never merged over it.
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let saved = self.save(&c, Expected::Version(version));
                    self.merge_admissions = merge;
                    match saved {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
                }
                Phase::ModelReady { turns } => {
                    if !self.supports_context_blocks(&c) {
                        return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
                    }
                    if !matches!(
                        c.messages
                            .iter()
                            .rev()
                            .find(|m| !matches!(m, Message::System { .. })),
                        Some(
                            Message::User { .. }
                                | Message::UserWithImages { .. }
                                | Message::Tool { .. }
                        )
                    ) {
                        finish_failure(
                            &mut c,
                            "nothing_to_answer".into(),
                            FailureCause::NothingToAnswer,
                        );
                    } else if turns >= self.options.max_turns {
                        finish_failure(&mut c, "turn limit".into(), FailureCause::TurnLimit(turns));
                    } else {
                        if !c.auto_compaction_attempted
                            && (c.content_blocks.is_empty()
                                || (c.occurrence_epoch_proven
                                    && c.transcript.is_some()
                                    && self.compaction_hooks_support_context(&c)?))
                        {
                            let window = tokio::select! {
                                _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                                window = tokio::time::timeout(Duration::from_secs(5), self.options.model.context_window(&c.config.model)) => window.ok().flatten(),
                            };
                            if let Some(window) = window {
                                let mut messages = vec![Message::System {
                                    content: c.config.system_prompt.clone(),
                                }];
                                messages.extend(c.messages.clone());
                                let specs = self.ordered_tool_specs(&c)?;
                                let request = model_request(
                                    &c.config.model,
                                    messages,
                                    specs,
                                    c.config.max_output_tokens,
                                );
                                let window = u64::from(window);
                                let reserve = 16_384u64
                                    .min(window / 4)
                                    .max(u64::from(c.config.max_output_tokens).min(window));
                                let block_tokens =
                                    c.content_blocks.values().fold(0u64, |sum, binding| {
                                        sum.saturating_add(crate::compact::estimate_block_tokens(
                                            binding,
                                        ))
                                    });
                                let estimated = pi_rs_core::estimate_request_tokens(&request)
                                    .saturating_add(block_tokens);
                                if estimated >= window.saturating_sub(reserve) {
                                    c.auto_compaction_attempted = true;
                                    let message_tokens: u64 = c
                                        .messages
                                        .iter()
                                        .map(pi_rs_core::estimate_message_tokens)
                                        .sum::<u64>()
                                        .saturating_add(block_tokens);
                                    let overhead = estimated.saturating_sub(message_tokens);
                                    let keep =
                                        window.saturating_sub(reserve).saturating_sub(overhead) / 2;
                                    if let Some(preparation) = self.prepare_runtime_compaction(
                                        &c,
                                        usize::try_from(keep.min(20_000)).unwrap_or(20_000),
                                        None,
                                    )? {
                                        let generation =
                                            *c.context_generation.get_or_insert_with(Uuid::new_v4);
                                        c.compaction_return_turns = Some(turns);
                                        c.phase = Phase::CompactionHook {
                                            preparation: Box::new(
                                                preparation.bind_context(generation),
                                            ),
                                            operation_id: Uuid::new_v4(),
                                            reason: crate::extension::CompactionReason::Threshold,
                                        };
                                        self.save(&c, Expected::Version(version))?;
                                        continue;
                                    }
                                }
                            }
                        }
                        c.phase = Phase::ModelRunning {
                            turns,
                            request_id: Uuid::new_v4(),
                            restarts: 0,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ModelRunning {
                    turns,
                    request_id,
                    restarts,
                } => {
                    let mut system_prompt = c.config.system_prompt.clone();
                    if !c.extensions.is_empty() {
                        let selected = self.selected_extensions(&c)?;
                        let snapshot = serde_json::to_value(&c)?;
                        let memo = self.extension_memo(request_id)?;
                        let context = crate::extension::Context {
                            snapshot: &snapshot,
                            memo: &memo,
                        };
                        let (sections, reports) = fenced_poll(
                            selected.prompt(&context, None),
                            self.execution_gate.as_ref(),
                            cancel,
                        )
                        .await
                        .ok_or(Error::Interrupted)?;
                        // A callback may swallow a memo error. Never dispatch after
                        // an indeterminate store failure, even in that case.
                        self.subagent_store()?;
                        self.record_extension_reports(c.id, request_id, "section", reports)?;
                        if !sections.is_empty() {
                            if !system_prompt.is_empty() {
                                system_prompt.push_str("\n\n");
                            }
                            system_prompt.push_str(&sections);
                        }
                    }
                    let mut messages = vec![Message::System {
                        content: system_prompt,
                    }];
                    messages.extend(c.messages.clone());
                    let specs = self.ordered_tool_specs(&c)?;
                    let mut request =
                        model_request(&c.config.model, messages, specs, c.config.max_output_tokens);
                    let mut blocks = std::collections::BTreeMap::new();
                    let mut tool_blocks = std::collections::BTreeMap::new();
                    for (index, binding) in &c.content_blocks {
                        match c.messages.get(*index) {
                            Some(Message::User { .. }) => {
                                blocks.insert(index + 1, binding.clone());
                            }
                            Some(Message::Tool { .. }) => {
                                tool_blocks.insert(index + 1, binding.clone());
                            }
                            _ => return Err(crate::transcript::Error::Corrupt.into()),
                        }
                    }
                    let metadata_adapter = c
                        .provider_adapter
                        .as_ref()
                        .and_then(|id| self.provider_adapter.as_ref().filter(|a| a.id() == id))
                        .is_some_and(|a| a.supports_tool_result_metadata());
                    let tool_errors = if metadata_adapter {
                        tool_error_bindings(&c)?
                    } else {
                        Default::default()
                    };
                    let original_messages = request.messages.clone();
                    if !c.extensions.is_empty() {
                        let selected = self.selected_extensions(&c)?;
                        let snapshot = serde_json::to_value(&c)?;
                        let memo = self.extension_memo(request_id)?;
                        let context = crate::extension::Context {
                            snapshot: &snapshot,
                            memo: &memo,
                        };
                        let mut value = serde_json::json!({"messages": request.messages});
                        if !blocks.is_empty() {
                            value["content_blocks"] = serde_json::to_value(&blocks)?;
                        }
                        if !tool_blocks.is_empty() {
                            value["tool_content_blocks"] = serde_json::to_value(&tool_blocks)?;
                        }
                        let (value, _, _, reports) = fenced_poll(
                            selected.generation(
                                crate::extension::GenerationEvent::BeforeRequest,
                                value,
                                &context,
                            ),
                            self.execution_gate.as_ref(),
                            cancel,
                        )
                        .await
                        .ok_or(Error::Interrupted)?;
                        self.subagent_store()?;
                        if cancel.is_cancelled() {
                            self.failed = true;
                            return Err(Error::Interrupted);
                        }
                        self.record_extension_reports(c.id, request_id, "before_request", reports)?;
                        if !blocks.is_empty()
                            && value.get("content_blocks") != Some(&serde_json::to_value(&blocks)?)
                        {
                            return Err(Error::Invalid(
                                "before-request cannot replace durable content bindings".into(),
                            ));
                        }
                        if !tool_blocks.is_empty()
                            && value.get("tool_content_blocks")
                                != Some(&serde_json::to_value(&tool_blocks)?)
                        {
                            return Err(Error::Invalid(
                                "before-request cannot replace durable tool content bindings"
                                    .into(),
                            ));
                        }
                        request.messages =
                            serde_json::from_value(value.get("messages").cloned().ok_or_else(
                                || Error::Invalid("before-request hook omitted messages".into()),
                            )?)
                            .map_err(|e| {
                                Error::Invalid(format!("invalid before-request messages: {e}"))
                            })?;
                    }
                    if (!blocks.is_empty() || !tool_blocks.is_empty() || !tool_errors.is_empty())
                        && request.messages != original_messages
                    {
                        return Err(Error::Invalid(
                            "before-request cannot replace messages with durable blocks".into(),
                        ));
                    }
                    if let Some(profile) = self
                        .provider_adapter
                        .as_ref()
                        .filter(|a| c.provider_adapter.as_deref() == Some(a.id()))
                        .and_then(|a| a.image_profile(&request.model))
                        .filter(|p| !p.supports_images)
                    {
                        let _ = profile;
                        crate::images::project_nonvision_request(
                            &mut request,
                            &mut blocks,
                            &mut tool_blocks,
                        );
                    }
                    let durable_request = crate::model::DurableRequest {
                        request: request.clone(),
                        blocks,
                    };
                    let tool_request = crate::model::DurableToolRequest {
                        request: durable_request.clone(),
                        tool_blocks,
                    };
                    tool_request.validate_bindings()?;
                    let result_request = crate::model::ProviderResultRequest {
                        request: tool_request.clone(),
                        tool_errors,
                    };
                    result_request.validate_bindings()?;
                    let prepared = if let Some(adapter_id) = &c.provider_adapter {
                        let adapter = self
                            .provider_adapter
                            .as_ref()
                            .filter(|a| a.id() == adapter_id)
                            .ok_or_else(|| {
                                Error::Invalid(format!(
                                    "selected provider adapter unavailable: {adapter_id}"
                                ))
                            })?
                            .clone();
                        let attempt = crate::model::ProviderAttempt {
                            session_id: id,
                            request_id,
                            turn: turns,
                            retries: c.retry_state.retries_scheduled,
                            restarts,
                        };
                        let mut body = fenced_poll(
                            async {
                                if adapter.supports_tool_result_metadata() {
                                    adapter
                                        .prepare_with_result_metadata(&attempt, &result_request)
                                        .await
                                } else if tool_request.tool_blocks.is_empty() {
                                    adapter.prepare_durable(&attempt, &durable_request).await
                                } else {
                                    adapter
                                        .prepare_with_tool_blocks(&attempt, &tool_request)
                                        .await
                                }
                            },
                            self.execution_gate.as_ref(),
                            cancel,
                        )
                        .await
                        .ok_or(Error::Interrupted)??;
                        if !c.extensions.is_empty() {
                            let selected = self.selected_extensions(&c)?;
                            let snapshot = serde_json::to_value(&c)?;
                            let memo = self.extension_memo(request_id)?;
                            let context = crate::extension::Context {
                                snapshot: &snapshot,
                                memo: &memo,
                            };
                            let (value, _, _, reports) = fenced_poll(
                                selected.generation(
                                    crate::extension::GenerationEvent::BeforeProviderRequest,
                                    serde_json::json!({"model": request.model, "payload": body}),
                                    &context,
                                ),
                                self.execution_gate.as_ref(),
                                cancel,
                            )
                            .await
                            .ok_or(Error::Interrupted)?;
                            self.subagent_store()?;
                            if !reports.is_empty() {
                                return Err(Error::Invalid(format!(
                                    "before-provider-request hook failed: {reports:?}"
                                )));
                            }
                            body = value.get("payload").cloned().ok_or_else(|| {
                                Error::Invalid("provider hook omitted payload".into())
                            })?;
                        }
                        if cancel.is_cancelled() {
                            self.failed = true;
                            return Err(Error::Interrupted);
                        }
                        if !body.is_object() || serde_json::to_vec(&body)?.len() > 8 * 1024 * 1024 {
                            return Err(Error::Invalid(
                                "provider body must be an object within the size limit".into(),
                            ));
                        }
                        if adapter.supports_tool_result_metadata() {
                            adapter.validate_with_result_metadata(
                                &attempt,
                                &result_request,
                                &body,
                            )?;
                        } else if tool_request.tool_blocks.is_empty() {
                            adapter.validate_durable(&attempt, &durable_request, &body)?;
                        } else {
                            adapter.validate_with_tool_blocks(&attempt, &tool_request, &body)?;
                        }
                        Some((adapter, attempt, body))
                    } else {
                        if !durable_request.blocks.is_empty()
                            || !tool_request.tool_blocks.is_empty()
                        {
                            return Err(crate::transcript::Error::UnsupportedContentBlocks.into());
                        }
                        None
                    };
                    let store = self.owner.store();
                    let mut snapshot = crate::progress::Snapshot::load(store, request_id)?;
                    let mut last_flush: Option<Instant> = None;
                    let mut text_flushed = false;
                    let mut progress_failure = None;
                    let mut progress_full = false;
                    let request_cancel = cancel.child_token();
                    let response = fenced_poll(collect_generation_with_metadata(
                        async {
                            match prepared {
                                Some((adapter, attempt, body)) => adapter.dispatch(&attempt, &request, body).await,
                                None => self.options.model.stream_in_session(request_id, id, request).await,
                            }
                        },
                        &request_cancel,
                        |event| {
                            if progress_failure.is_some() || progress_full { return; }
                            let is_text = matches!(&event, GenerationProgress::Text(text) if !text.is_empty());
                            if let Err(error) = snapshot.push(&event) {
                                if matches!(error, crate::progress::Error::TooLarge) { progress_full = true; }
                                else { progress_failure = Some(error.to_string()); request_cancel.cancel(); }
                                return;
                            }
                            if (is_text && !text_flushed) || last_flush.is_none_or(|time| time.elapsed() >= Duration::from_millis(100)) {
                                let committed = (|| -> Result<crate::progress::Snapshot, Error> {
                                    loop {
                                        let mut batch = snapshot.prepare_checkpoint()?;
                                        let history = prepare_progress_history(store, id, vec![ProgressHistoryChange::Generation { request_id, state: Box::new(snapshot.state().clone()) }])?;
                                        let history_keys: Vec<_> = history.checks.iter().map(|check| check.key.clone()).collect();
                                        batch.checks.extend(history.checks);
                                        batch.mutations.extend(history.mutations);
                                        match store.commit(&batch.checks, &batch.mutations) {
                                            Ok(()) => return Ok(crate::progress::Snapshot::load(store, request_id)?),
                                            Err(durable_store::Error::Conflict { key }) if history_keys.contains(&key) => continue,
                                            Err(error) => return Err(error.into()),
                                        }
                                    }
                                })();
                                match committed {
                                    Ok(next) => { snapshot = next; last_flush = Some(Instant::now()); if is_text { text_flushed = true; } },
                                    Err(error) => { progress_failure = Some(error.to_string()); request_cancel.cancel(); }
                                }
                            }
                        },
                    ), self.execution_gate.as_ref(), cancel)
                    .await.ok_or(Error::Interrupted)?;
                    if let Some(error) = progress_failure {
                        self.failed = true;
                        return Err(Error::Invalid(format!(
                            "progress checkpoint failed: {error}"
                        )));
                    }
                    let response = match response {
                        Ok(r) => r,
                        Err(e) => {
                            if cancel.is_cancelled() || matches!(e, pi_rs_core::Error::Cancelled) {
                                self.failed = true;
                                return Err(Error::Interrupted);
                            }
                            let partial =
                                crate::progress::Snapshot::load(self.owner.store(), request_id)?;
                            if let crate::progress::State::Running(progress) = partial.state()
                                && !progress.text.is_empty()
                                && !c
                                    .aborted_generations
                                    .iter()
                                    .any(|entry| entry.request_id == request_id)
                            {
                                c.aborted_generations.push(AbortedGeneration {
                                    request_id,
                                    text: progress.text.clone(),
                                });
                            }
                            let decision =
                                Failure::from_error(&e).map_or(Decision::Stop, |failure| {
                                    c.retry_state
                                        .failed(RetryPolicy::default(), failure, unix_ms())
                                });
                            c.phase = Phase::FailedModel {
                                turns,
                                request_id,
                                reason: e.to_string(),
                                decision: match decision {
                                    Decision::WaitUntil(_) => ModelFailureDecision::Retry,
                                    Decision::ContextOverflow => ModelFailureDecision::Overflow,
                                    _ => ModelFailureDecision::Stop,
                                },
                            };
                            self.save(&c, Expected::Version(version))?;
                            continue;
                        }
                    };
                    c.phase = Phase::Accounting {
                        turns,
                        request_id,
                        response: Box::new(response),
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::FailedModel {
                    turns,
                    request_id,
                    reason,
                    decision,
                } => {
                    let settlement = self
                        .options
                        .model
                        .record_usage(request_id, &UsageReport::Unconfirmed);
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => { self.failed = true; return Err(e.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); },
                        }
                    }
                    match decision {
                        ModelFailureDecision::Retry => c.phase = Phase::RetryWait { turns },
                        ModelFailureDecision::Overflow => {
                            start_overflow_compaction(self, &mut c, turns, reason)?
                        }
                        ModelFailureDecision::Stop => {
                            finish_failure(&mut c, reason, FailureCause::ModelError)
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::RetryWait { turns } => {
                    match c.retry_state.poll(unix_ms()) {
                        Decision::WaitUntil(deadline) => {
                            // Scheduler-owned workers park outside the per-conversation
                            // lock so other admissions and aborts can make progress.
                            if self.execution_gate.is_some() {
                                return Ok(());
                            }
                            tokio::select! { biased;
                                _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                                _ = tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(unix_ms()))) => (),
                            }
                            continue;
                        }
                        Decision::Ready => c.phase = Phase::ModelReady { turns },
                        _ => return Err(Error::Invalid("retry deadline missing".into())),
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::AbandonedModel {
                    turns,
                    request_id,
                    restarts,
                } => {
                    let settlement = self
                        .options
                        .model
                        .record_usage(request_id, &UsageReport::Unconfirmed);
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => { self.failed = true; return Err(e.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); },
                        }
                    }
                    if restarts >= self.options.restart_limit {
                        finish_failure(
                            &mut c,
                            "model restart limit".into(),
                            FailureCause::RestartLimit(restarts),
                        );
                    } else {
                        c.phase = Phase::ModelRunning {
                            turns,
                            request_id: Uuid::new_v4(),
                            restarts,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Accounting {
                    turns,
                    request_id,
                    response,
                } => {
                    if !c.extensions.is_empty() {
                        let selected = self.selected_extensions(&c)?;
                        let snapshot = serde_json::to_value(&c)?;
                        let memo = self.extension_memo(request_id)?;
                        let context = crate::extension::Context {
                            snapshot: &snapshot,
                            memo: &memo,
                        };
                        let (_, _, _, reports) = fenced_poll(
                            selected.generation(
                                crate::extension::GenerationEvent::AfterResponse,
                                serde_json::to_value(&*response)?,
                                &context,
                            ),
                            self.execution_gate.as_ref(),
                            cancel,
                        )
                        .await
                        .ok_or(Error::Interrupted)?;
                        self.subagent_store()?;
                        if cancel.is_cancelled() {
                            self.failed = true;
                            return Err(Error::Interrupted);
                        }
                        self.record_extension_reports(c.id, request_id, "after_response", reports)?;
                    }
                    let usage = response.usage.map_or(UsageReport::Unconfirmed, |usage| {
                        UsageReport::Measured { usage }
                    });
                    let settlement = self.options.model.record_usage_with_metadata(
                        request_id,
                        &usage,
                        &response.metadata,
                    );
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        result = tokio::time::timeout(Duration::from_secs(120), settlement) => match result {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => { self.failed = true; return Err(e.into()); },
                            Err(_) => { self.failed = true; return Err(Error::Interrupted); },
                        }
                    }
                    let window = tokio::select! {
                        _ = cancel.cancelled() => { self.failed = true; return Err(Error::Interrupted); },
                        window = tokio::time::timeout(Duration::from_secs(5), self.options.model.context_window(&c.config.model)) => window.ok().flatten(),
                    };
                    let usage = response.usage.unwrap_or_default();
                    let stop_reason = match response.metadata.finish_reason.as_deref() {
                        Some("length") => StopReason::Length,
                        Some("stop") | Some("end_turn") => StopReason::Stop,
                        _ => StopReason::Other,
                    };
                    if is_context_overflow(Failure {
                        stop_reason,
                        provider: "",
                        message: "",
                        input_tokens: usage.input_tokens,
                        cache_read_tokens: 0,
                        output_tokens: usage.output_tokens,
                        context_window: window.map(u64::from),
                        transient_hint: false,
                    }) {
                        start_overflow_compaction(
                            self,
                            &mut c,
                            turns,
                            "model context overflow".into(),
                        )?;
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    }
                    c.retry_state = RetryState::default();
                    c.overflow_compaction_attempted = false;
                    if response.is_truncated() && !response.calls.is_empty() {
                        c.messages.push(response.assistant_message());
                        for index in 0..response.calls.len() {
                            self.append_skipped_with_status(
                                &mut c,
                                &response.calls,
                                index,
                                "tool call truncated before dispatch",
                            )?;
                        }
                        finish_failure(
                            &mut c,
                            "truncated tool calls".into(),
                            FailureCause::ModelError,
                        );
                    } else {
                        c.messages.push(response.assistant_message());
                        if response.calls.is_empty() {
                            c.phase = Phase::OnYield {
                                turns: turns + 1,
                                answer: response.content,
                                operation_id: Uuid::new_v4(),
                            };
                        } else {
                            c.round_terminates = vec![false; response.calls.len()];
                            c.round_handoffs = vec![None; response.calls.len()];
                            c.round_added_tools = vec![Vec::new(); response.calls.len()];
                            c.phase = Phase::Tools {
                                turns: turns + 1,
                                calls: response.calls,
                                index: 0,
                            };
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::OnYield {
                    turns,
                    answer,
                    operation_id,
                } => {
                    let Some((continuation, children, handlers)) = self
                        .run_lifecycle_hook(
                            &c,
                            operation_id,
                            crate::extension::GenerationEvent::OnYield,
                            serde_json::json!({"answer": answer}),
                            cancel,
                        )
                        .await?
                    else {
                        return Ok(());
                    };
                    if !children.is_empty() {
                        c.phase = Phase::YieldTasksWaiting {
                            turns,
                            answer,
                            children: children.iter().map(|t| t.id()).collect(),
                        };
                        self.save_hook_children(
                            &c,
                            Expected::Version(version),
                            &children,
                            &handlers,
                        )?;
                        continue;
                    }
                    let continuation = continuation.and_then(|value| {
                        if self.supports_blocks(&c) {
                            crate::content::UserContent::from_yield(value, false)
                        } else {
                            crate::extension::yield_input(&value).map(|_| {
                                crate::content::UserContent::from_yield(value, false).unwrap()
                            })
                        }
                    });
                    c.phase = Phase::YieldDecision {
                        turns,
                        answer,
                        continuation,
                    };
                    // Merge admissions into the saved decision, without rerunning
                    // the hook when a summary or input races its completion.
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::YieldTasksWaiting {
                    turns,
                    answer,
                    children,
                } => {
                    let store = crate::task_store::TaskStore::new(self.owner.store());
                    let mut continuation = None;
                    for id in children {
                        let task = store
                            .load(id)
                            .map_err(|e| Error::Invalid(format!("task load: {e}")))?
                            .ok_or_else(|| Error::Invalid(format!("missing hook child: {id}")))?
                            .task;
                        if task.owner() != &crate::task::TaskOwner::Conversation(c.id) {
                            return Err(Error::Invalid("hook child owner mismatch".into()));
                        }
                        if !task.is_terminal() {
                            return Ok(());
                        }
                        if let crate::task::TaskState::Failed(reason) = task.state() {
                            let kind = task.kind().unwrap_or("unknown");
                            let selected = self.selected_extensions(&c)?;
                            let owner = selected.task_owner(kind).unwrap_or("unknown");
                            self.record_extension_reports(
                                c.id,
                                id,
                                "task_failed",
                                vec![crate::extension::Report {
                                    extension: owner.into(),
                                    operation: "task_failed".into(),
                                    error: reason.clone(),
                                }],
                            )?;
                        }
                        if continuation.is_none()
                            && let crate::task::TaskState::Succeeded(value) = task.state()
                            && let Some(message) = value.get("continue").and_then(|value| {
                                if self.supports_blocks(&c) {
                                    crate::content::UserContent::from_yield(value.clone(), true)
                                } else {
                                    crate::extension::yield_input(value).and_then(|_| {
                                        crate::content::UserContent::from_yield(value.clone(), true)
                                    })
                                }
                            })
                        {
                            continuation = Some(message);
                        }
                    }
                    c.phase = Phase::YieldDecision {
                        turns,
                        answer,
                        continuation,
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::YieldDecision {
                    turns,
                    answer,
                    continuation,
                } => {
                    if let Some(message) = continuation.filter(|_| {
                        c.pending_resets.is_empty()
                            && !c.submissions.iter().any(|s| s.status == Status::Queued)
                    }) {
                        let durable = self.supports_blocks(&c);
                        Self::append_yield(&mut c, message, durable);
                        c.auto_compaction_attempted = false;
                        c.phase = Phase::ModelReady { turns: 0 };
                    } else {
                        c.phase = Phase::Boundary {
                            turns,
                            answer: Some(answer),
                        };
                    }
                    // Recheck the inbox atomically with the continuation decision.
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let result = self.save(&c, Expected::Version(version));
                    self.merge_admissions = merge;
                    match result {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
                }
                Phase::Boundary { turns, answer } => {
                    if !c.pending_resets.is_empty()
                        && let Some(answer) = answer.clone()
                    {
                        finish_answer(&mut c, answer.clone());
                        c.phase = Phase::AfterRun {
                            answer,
                            operation_id: Uuid::new_v4(),
                        };
                    } else if !c.pending_resets.is_empty() {
                        finish_failure(&mut c, "reset".into(), FailureCause::Reset);
                        c.phase = Phase::Idle;
                        for marker in take_reset_batch(&mut c) {
                            place_reset(&mut c, marker);
                        }
                        if c.pending_resets.is_empty() {
                            let failure = c.run_failure.clone();
                            place_queued(&mut c, true);
                            c.run_failure = failure;
                            if c.active.is_some() {
                                c.phase = Phase::ModelReady { turns: 0 };
                            }
                        }
                    } else if let Some(answer) = answer {
                        finish_answer(&mut c, answer.clone());
                        c.phase = Phase::AfterRun {
                            answer,
                            operation_id: Uuid::new_v4(),
                        };
                    } else if place_queued(&mut c, false) {
                        c.auto_compaction_attempted = false;
                        c.phase = Phase::ModelReady { turns };
                    } else {
                        c.phase = Phase::ModelReady { turns };
                    }
                    // A concurrent admission can change this decision. Reload and decide
                    // again instead of merging it after the boundary has passed.
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let result = self.save(&c, Expected::Version(version));
                    self.merge_admissions = merge;
                    match result {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
                }
                Phase::AfterTools {
                    turns,
                    calls,
                    operation_id,
                } => {
                    let Some((_, children, handlers)) = self
                        .run_lifecycle_hook(
                            &c,
                            operation_id,
                            crate::extension::GenerationEvent::AfterTools,
                            serde_json::to_value(&calls)?,
                            cancel,
                        )
                        .await?
                    else {
                        return Ok(());
                    };
                    c.phase = if children.is_empty() {
                        if !calls.is_empty()
                            && c.round_terminates.len() == calls.len()
                            && c.round_terminates.iter().all(|v| *v)
                        {
                            terminate_tool_round(&mut c)?
                        } else {
                            Phase::Boundary {
                                turns,
                                answer: None,
                            }
                        }
                    } else {
                        Phase::HookTasksWaiting {
                            children: children.iter().map(|t| t.id()).collect(),
                            turns: Some(turns),
                        }
                    };
                    if children.is_empty() {
                        place_round_handoff(&mut c);
                    }
                    // The phase transition and any hook-created children are
                    // committed together. Admission conflicts merge the queued
                    // summary without executing this completed hook again.
                    self.save_hook_children(&c, Expected::Version(version), &children, &handlers)?;
                }
                Phase::AfterRun {
                    answer,
                    operation_id,
                } => {
                    let Some((_, children, handlers)) = self
                        .run_lifecycle_hook(
                            &c,
                            operation_id,
                            crate::extension::GenerationEvent::AfterRun,
                            serde_json::json!({"answer": answer}),
                            cancel,
                        )
                        .await?
                    else {
                        return Ok(());
                    };
                    c.phase = if children.is_empty() {
                        Phase::Idle
                    } else {
                        Phase::HookTasksWaiting {
                            children: children.iter().map(|t| t.id()).collect(),
                            turns: None,
                        }
                    };
                    self.save_hook_children(&c, Expected::Version(version), &children, &handlers)?;
                }
                Phase::HookTasksWaiting { children, turns } => {
                    let store = crate::task_store::TaskStore::new(self.owner.store());
                    let mut terminate = false;
                    let mut completed = Vec::with_capacity(children.len());
                    for id in children {
                        let task = store
                            .load(id)
                            .map_err(|e| Error::Invalid(format!("task load: {e}")))?
                            .ok_or_else(|| Error::Invalid(format!("missing hook child: {id}")))?
                            .task;
                        if task.owner() != &crate::task::TaskOwner::Conversation(c.id) {
                            return Err(Error::Invalid("hook child owner mismatch".into()));
                        }
                        if !task.is_terminal() {
                            return Ok(());
                        }
                        completed.push((id, task));
                    }
                    for (id, task) in completed {
                        if let crate::task::TaskState::Failed(reason) = task.state() {
                            let kind = task.kind().unwrap_or("unknown");
                            let selected = self.selected_extensions(&c)?;
                            let owner = selected.task_owner(kind).unwrap_or("unknown");
                            self.record_extension_reports(
                                c.id,
                                id,
                                "task_failed",
                                vec![crate::extension::Report {
                                    extension: owner.into(),
                                    operation: "task_failed".into(),
                                    error: reason.clone(),
                                }],
                            )?;
                        }
                        if turns.is_some()
                            && matches!(task.state(), crate::task::TaskState::Succeeded(value)
                                if value.as_object().is_some_and(|object|
                                    object.len() == 1 && object.get("terminate") == Some(&serde_json::Value::Bool(true))))
                        {
                            terminate = true;
                        }
                        if turns.is_some()
                            && let crate::task::TaskState::Succeeded(value) = task.state()
                            && !value.is_null()
                            && !value.as_object().is_some_and(|object| {
                                object.is_empty()
                                    || (object.len() == 1
                                        && object.get("terminate")
                                            == Some(&serde_json::Value::Bool(true)))
                            })
                        {
                            let kind = task.kind().unwrap_or("unknown");
                            let selected = self.selected_extensions(&c)?;
                            let owner = selected.task_owner(kind).unwrap_or("unknown");
                            self.record_extension_reports(
                                c.id,
                                id,
                                "after_tools_invalid_decision",
                                vec![crate::extension::Report {
                                    extension: owner.into(),
                                    operation: "after_tools_invalid_decision".into(),
                                    error: format!("afterTools task {id} ({kind}) completed with no AfterToolsDecision"),
                                }],
                            )?;
                        }
                    }
                    c.phase = if terminate
                        || (turns.is_some()
                            && !c.round_terminates.is_empty()
                            && c.round_terminates.iter().all(|v| *v))
                    {
                        let answer = c
                            .messages
                            .iter()
                            .rev()
                            .find_map(|message| match message {
                                Message::Assistant { content, calls, .. } if !calls.is_empty() => {
                                    Some(content.clone())
                                }
                                _ => None,
                            })
                            .ok_or_else(|| Error::Invalid("missing tool-calling answer".into()))?;
                        finish_answer(&mut c, answer.clone());
                        Phase::AfterRun {
                            answer,
                            operation_id: Uuid::new_v4(),
                        }
                    } else {
                        turns.map_or(Phase::Idle, |turns| Phase::Boundary {
                            turns,
                            answer: None,
                        })
                    };
                    if turns.is_some() {
                        place_round_handoff(&mut c);
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Tools {
                    turns,
                    calls,
                    index,
                } => {
                    if index >= calls.len() {
                        c.phase = Phase::AfterTools {
                            turns,
                            calls,
                            operation_id: Uuid::new_v4(),
                        };
                    } else if !c.extensions.is_empty() {
                        c.phase = Phase::ToolPreparing {
                            turns,
                            calls,
                            index,
                            operation_id: Uuid::new_v4(),
                            wake: None,
                        };
                    } else if let Some(reg) = self
                        .resolved_tools(&c)?
                        .get(&calls[index].name)
                        .filter(|_| model_tool_selected(&c, &calls[index].name))
                    {
                        let operation_id = Uuid::new_v4();
                        let invocation = Invocation {
                            operation_id,
                            call: calls[index].clone(),
                        };
                        let prepared = fenced_poll(
                            crate::tool::prepare(
                                reg.tool.as_ref(),
                                &invocation,
                                cancel,
                                Duration::from_secs(120),
                                valid_schema,
                            ),
                            self.execution_gate.as_ref(),
                            cancel,
                        )
                        .await
                        .ok_or(Error::Interrupted)?;
                        if cancel.is_cancelled() {
                            self.failed = true;
                            return Err(Error::Interrupted);
                        }
                        match prepared {
                            Ok(prepared) => {
                                c.tool_attempt = Some(ToolAttempt {
                                    operation_id,
                                    attempt_id: Uuid::new_v4(),
                                });
                                c.phase = Phase::ToolRunning {
                                    turns,
                                    calls,
                                    index,
                                    operation_id,
                                    replay: reg.replay,
                                    arguments: prepared.arguments().clone(),
                                    restarts: 0,
                                    replay_ready: false,
                                    dispatch_identity: Some(dispatch_identity(&reg.tool)?),
                                }
                            }
                            Err(outcome) => {
                                record_tool_result(
                                    &mut c,
                                    &calls,
                                    index,
                                    operation_id,
                                    &outcome,
                                    Vec::new(),
                                )?;
                                self.append_tool_with_status(
                                    &mut c,
                                    &calls,
                                    index,
                                    operation_id,
                                    outcome.tool_content(),
                                )?;
                                c.phase = Phase::Tools {
                                    turns,
                                    calls,
                                    index: index + 1,
                                };
                            }
                        }
                    } else {
                        let operation_id = Uuid::new_v4();
                        record_tool_result(
                            &mut c,
                            &calls,
                            index,
                            operation_id,
                            &Outcome::Skipped("tool unavailable".into()),
                            Vec::new(),
                        )?;
                        self.append_tool_with_status(
                            &mut c,
                            &calls,
                            index,
                            operation_id,
                            "tool unavailable".into(),
                        )?;
                        c.phase = Phase::Tools {
                            turns,
                            calls,
                            index: index + 1,
                        };
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ToolGateSleeping {
                    turns,
                    calls,
                    index,
                    operation_id,
                    until_millis,
                    signals,
                } => {
                    let wake = crate::conversation_signal::Inbox::new(self.owner.store())
                        .prepare_wake(id, &signals, until_millis, (self.clock)())?;
                    let Some(wake) = wake else {
                        return Ok(());
                    };
                    let wake_value = match &wake.signal {
                        Some(signal) => {
                            serde_json::json!({"reason": "signal", "name": signal.name, "id": signal.id, "payload": signal.payload})
                        }
                        None if signals.is_empty() => serde_json::json!({"reason": "time"}),
                        None => serde_json::json!({"reason": "expired"}),
                    };
                    c.phase = Phase::ToolPreparing {
                        turns,
                        calls,
                        index,
                        operation_id,
                        wake: Some(wake_value),
                    };
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let result = self.save_with_wake(&c, Expected::Version(version), Some(&wake));
                    self.merge_admissions = merge;
                    match result {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
                }
                Phase::ToolPreparing {
                    turns,
                    calls,
                    index,
                    operation_id,
                    wake: _,
                } => {
                    let selected = self.selected_extensions(&c)?;
                    let call = calls
                        .get(index)
                        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
                    let tools = self.resolved_tools(&c)?;
                    let Some(reg) = tools
                        .get(&call.name)
                        .filter(|_| model_tool_selected(&c, &call.name))
                    else {
                        let arguments = pi_rs_core::parse_tool_arguments(&call.arguments)
                            .unwrap_or(serde_json::Value::Null);
                        record_tool_result(
                            &mut c,
                            &calls,
                            index,
                            operation_id,
                            &Outcome::Skipped("tool unavailable".into()),
                            Vec::new(),
                        )?;
                        c.phase = Phase::ToolFinished {
                            turns,
                            calls,
                            index,
                            operation_id,
                            arguments,
                            content: "tool unavailable".into(),
                        };
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    };
                    let invocation = Invocation {
                        operation_id,
                        call: call.clone(),
                    };
                    let prepared = fenced_poll(
                        crate::tool::prepare(
                            reg.tool.as_ref(),
                            &invocation,
                            cancel,
                            Duration::from_secs(120),
                            valid_schema,
                        ),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?;
                    if cancel.is_cancelled() {
                        self.failed = true;
                        return Err(Error::Interrupted);
                    }
                    let prepared = match prepared {
                        Ok(prepared) => prepared,
                        Err(outcome) => {
                            let arguments =
                                pi_rs_core::parse_tool_arguments(&calls[index].arguments)
                                    .unwrap_or(serde_json::Value::Null);
                            record_tool_result(
                                &mut c,
                                &calls,
                                index,
                                operation_id,
                                &outcome,
                                Vec::new(),
                            )?;
                            c.phase = Phase::ToolFinished {
                                turns,
                                calls,
                                index,
                                operation_id,
                                arguments,
                                content: outcome.tool_content(),
                            };
                            self.save(&c, Expected::Version(version))?;
                            continue;
                        }
                    };
                    let snapshot = serde_json::to_value(&c)?;
                    let memo = self.extension_memo(operation_id)?;
                    let context = crate::extension::Context {
                        snapshot: &snapshot,
                        memo: &memo,
                    };
                    let hook_call = crate::extension::ToolCall {
                        name: call.name.clone(),
                        arguments: prepared.arguments().clone(),
                    };
                    let (rewritten, decision, reports) = fenced_poll(
                        selected.before_tool(hook_call, &context),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?;
                    self.subagent_store()?;
                    if cancel.is_cancelled() {
                        self.failed = true;
                        return Err(Error::Interrupted);
                    }
                    if let crate::extension::BeforeTool::Sleep(sleep) = &decision {
                        let invalid_shape = (sleep.until_millis.is_none()
                            && sleep.signals.is_empty())
                            || sleep.until_millis.is_some_and(|time| time < 0)
                            || sleep.signals.iter().any(String::is_empty);
                        let wait = if invalid_shape {
                            None
                        } else {
                            match crate::conversation_signal::Inbox::new(self.owner.store())
                                .prepare_wait(id, &sleep.signals)
                            {
                                Ok(wait) => Some(wait),
                                Err(crate::conversation_signal::Error::InvalidWait) => None,
                                Err(error) => return Err(error.into()),
                            }
                        };
                        if let Some(wait) = wait {
                            c.phase = Phase::ToolGateSleeping {
                                turns,
                                calls,
                                index,
                                operation_id,
                                until_millis: sleep.until_millis,
                                signals: sleep.signals.clone(),
                            };
                            let merge = self.merge_admissions;
                            self.merge_admissions = false;
                            let result =
                                self.save_with_wake(&c, Expected::Version(version), Some(&wait));
                            self.merge_admissions = merge;
                            match result {
                                Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                                    continue;
                                }
                                other => other?,
                            }
                            return Ok(());
                        }
                        self.record_extension_reports(c.id, operation_id, "before_tool", reports)?;
                        let message = format!(
                            "Tool call {} cannot wait at its gates: {}",
                            call.id,
                            if invalid_shape {
                                "invalid extension sleep"
                            } else {
                                "signal key was already consumed or expired"
                            }
                        );
                        record_tool_result(
                            &mut c,
                            &calls,
                            index,
                            operation_id,
                            &Outcome::Failed(message.clone()),
                            Vec::new(),
                        )?;
                        if let Some(diagnostic) = c
                            .tool_results
                            .iter_mut()
                            .find(|metadata| metadata.operation_id == operation_id)
                            .and_then(|metadata| {
                                metadata.diagnostics.iter_mut().find(|diagnostic| {
                                    diagnostic.code.as_deref() == Some("tool_failed")
                                })
                            })
                        {
                            diagnostic.code = Some("invalid_park".into());
                        }
                        c.phase = Phase::ToolFinished {
                            turns,
                            calls,
                            index,
                            operation_id,
                            arguments: rewritten.arguments,
                            content: message,
                        };
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    }
                    self.record_extension_reports(
                        c.id,
                        operation_id,
                        "before_tool",
                        reports.clone(),
                    )?;
                    let rejection = match decision {
                        crate::extension::BeforeTool::Pass => None,
                        crate::extension::BeforeTool::Block(reason) => {
                            Some(if reports.is_empty() {
                                reason
                            } else {
                                format!("{reason}: {reports:?}")
                            })
                        }
                        crate::extension::BeforeTool::Sleep(_) => unreachable!(),
                        crate::extension::BeforeTool::Rewrite(_) => {
                            Some("invalid before-tool decision".into())
                        }
                    };
                    if let Some(reason) = rejection {
                        record_tool_result(
                            &mut c,
                            &calls,
                            index,
                            operation_id,
                            &Outcome::Skipped(reason.clone()),
                            Vec::new(),
                        )?;
                        c.phase = Phase::ToolFinished {
                            turns,
                            calls,
                            index,
                            operation_id,
                            arguments: rewritten.arguments,
                            content: reason,
                        };
                    } else {
                        match crate::tool::restore(
                            reg.tool.as_ref(),
                            &invocation,
                            rewritten.arguments.clone(),
                            valid_schema,
                        ) {
                            Ok(prepared) => {
                                c.tool_attempt = Some(ToolAttempt {
                                    operation_id,
                                    attempt_id: Uuid::new_v4(),
                                });
                                c.phase = Phase::ToolRunning {
                                    turns,
                                    calls,
                                    index,
                                    operation_id,
                                    replay: reg.replay,
                                    arguments: prepared.arguments().clone(),
                                    restarts: 0,
                                    replay_ready: false,
                                    dispatch_identity: Some(dispatch_identity(&reg.tool)?),
                                }
                            }
                            Err(outcome) => {
                                record_tool_result(
                                    &mut c,
                                    &calls,
                                    index,
                                    operation_id,
                                    &outcome,
                                    Vec::new(),
                                )?;
                                c.phase = Phase::ToolFinished {
                                    turns,
                                    calls,
                                    index,
                                    operation_id,
                                    arguments: rewritten.arguments,
                                    content: outcome.tool_content(),
                                };
                            }
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ToolRunning {
                    turns,
                    calls,
                    index,
                    operation_id,
                    arguments,
                    replay,
                    restarts,
                    replay_ready,
                    dispatch_identity: stored_identity,
                } => {
                    if replay_ready {
                        c.tool_attempt = Some(ToolAttempt {
                            operation_id,
                            attempt_id: Uuid::new_v4(),
                        });
                        if let Phase::ToolRunning { replay_ready, .. } = &mut c.phase {
                            *replay_ready = false;
                        }
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    }
                    let call = calls
                        .get(index)
                        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
                    let tools = self.resolved_tools(&c)?;
                    let search_candidates = if tools
                        .get(&call.name)
                        .is_some_and(|reg| crate::tool_search::is_search(&reg.tool))
                    {
                        let offered = self.ordered_tool_specs(&c)?;
                        let mut candidates: Vec<_> = tools
                            .iter()
                            .filter(|(name, _)| {
                                c.tool_filters.deferred.contains(name)
                                    && caller_allowed(&c, name, true)
                                    && !offered.iter().any(|tool| &tool.name == *name)
                            })
                            .map(|(_, reg)| reg.tool.spec())
                            .collect();
                        candidates.sort_by(|a, b| a.name.cmp(&b.name));
                        candidates
                    } else {
                        Vec::new()
                    };
                    let control =
                        Arc::new(std::sync::Mutex::new(crate::reset::ToolControl::default()));
                    let diagnostics = c.tool_attempt().map(|attempt| {
                        Arc::new(RuntimeDiagnostic {
                            owner: self.owner.clone(),
                            conversation: id,
                            attempt,
                            cancel: cancel.clone(),
                            storage_failed: self.storage_failed.clone(),
                            closed: std::sync::Mutex::new(false),
                        })
                    });
                    let mut tool_panicked = false;
                    let mut nested_resume_candidate = false;
                    let mut explicitly_suspended = false;
                    let mut parked_checkpoint = None;
                    let task_adapter = tools
                        .get(&call.name)
                        .and_then(|reg| crate::task_tool::lookup(&reg.tool));
                    if !tools.get(&call.name).is_some_and(|reg| {
                        dispatch_identity(&reg.tool).ok().as_ref() == stored_identity.as_ref()
                            && stored_identity.is_some()
                    }) {
                        c.phase = Phase::Blocked {
                            tool_panicked: false,
                            turns,
                            calls,
                            index,
                            operation_id,
                            reason: "dispatch identity missing or changed; reconciliation required"
                                .into(),
                            arguments: Some(arguments),
                        };
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    }
                    let reply = if let Some(reg) = tools
                        .get(&call.name)
                        .filter(|_| model_tool_selected(&c, &call.name))
                    {
                        let invocation = Invocation {
                            operation_id,
                            call: call.clone(),
                        };
                        match crate::tool::restore(
                            reg.tool.as_ref(),
                            &invocation,
                            arguments.clone(),
                            valid_schema,
                        ) {
                            Ok(prepared) => {
                                match crate::nested::NestedHost::new_with_schemas(
                                    self.owner.clone(),
                                    operation_id,
                                    tools
                                        .iter()
                                        .filter(|(name, tool)| {
                                            caller_allowed(&c, name, false)
                                                && !crate::tool_search::is_search(&tool.tool)
                                        })
                                        .map(|(_, tool)| tool.clone())
                                        .collect(),
                                    cancel.child_token(),
                                    Duration::from_secs(120),
                                    8,
                                    (
                                        c.tool_filters.output_schemas.clone(),
                                        if c.extensions.is_empty() {
                                            None
                                        } else {
                                            Some(crate::nested::NestedHooks {
                                                selected: self.selected_extensions(&c)?,
                                                snapshot: serde_json::to_value(&c)?,
                                                conversation: c.id,
                                                storage_failed: self.storage_failed.clone(),
                                                conversation_fence: None,
                                            })
                                        },
                                    ),
                                ) {
                                    Ok(host) => {
                                        let resumed = c
                                            .nested_resume
                                            .as_ref()
                                            .filter(|resume| resume.operation_id == operation_id)
                                            .and_then(|resume| resume.pending_wake.clone());
                                        host.set_resumed_checkpoint(resumed)
                                            .map_err(Error::Invalid)?;
                                        let scope = c.tool_attempt().map(|attempt| {
                                            crate::tool_progress::scoped_host(
                                                host.clone(),
                                                Arc::new(RuntimeToolProgress {
                                                    owner: self.owner.clone(),
                                                    source: self
                                                        .merged_source
                                                        .as_ref()
                                                        .map(Arc::downgrade)
                                                        .unwrap_or_default(),
                                                    conversation: id,
                                                    attempt,
                                                    storage_failed: self.storage_failed.clone(),
                                                }),
                                            )
                                        });
                                        let progress_host = scope
                                            .as_ref()
                                            .map(|(host, _)| host.clone())
                                            .unwrap_or_else(|| host.clone());
                                        let reply = fenced_poll(
                                            std::panic::AssertUnwindSafe(
                                                crate::tool_search::scope(
                                                    search_candidates,
                                                    crate::keyed_tool::scope_with_control(
                                                        host.clone(),
                                                        reg.tool.clone(),
                                                        invocation.operation_id,
                                                        progress_host.clone(),
                                                        control.clone(),
                                                        (
                                                            diagnostics.as_ref().map(|sink| sink.clone() as Arc<dyn crate::keyed_tool::DiagnosticSink>),
                                                            c.tool_attempt().map(|attempt| {
                                                                Arc::new(RuntimeStreamOutput {
                                                            owner: Arc::downgrade(&self.owner),
                                                            source: self
                                                                .merged_source
                                                                .as_ref()
                                                                .map(Arc::downgrade)
                                                                .unwrap_or_default(),
                                                            conversation: id,
                                                            attempt,
                                                            storage_failed: self
                                                                .storage_failed
                                                                .clone(),
                                                        })
                                                            as Arc<
                                                                dyn crate::keyed_tool::StreamSink,
                                                            >
                                                            }),
                                                        ),
                                                        crate::task_tool::scope(
                                                            task_adapter.clone(),
                                                            operation_id,
                                                            crate::tool::execute(
                                                                reg.tool.as_ref(),
                                                                &invocation,
                                                                prepared,
                                                                c.tool_states.get(&call.name),
                                                                progress_host,
                                                                cancel,
                                                                Duration::from_secs(120),
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                            )
                                            .catch_unwind(),
                                            self.execution_gate.as_ref(),
                                            cancel,
                                        )
                                        .await;
                                        if let Some(sink) = &diagnostics {
                                            sink.close();
                                        }
                                        drop(scope);
                                        let reply = reply.map(|result| match result {
                                            Ok(reply) => Ok(reply),
                                            Err(payload) => {
                                                tool_panicked = true;
                                                crate::extension::discard_panic_payload(payload);
                                                Err(())
                                            }
                                        });
                                        // Record before awaiting detached nested calls or observing
                                        // cancellation. Recovery uses this identity as a replay barrier.
                                        let panic_report = if tool_panicked {
                                            self.record_tool_panic(c.id, operation_id)
                                        } else {
                                            Ok(())
                                        };
                                        // Even a failed report write must seal the nested host.
                                        let inspection = host.seal_and_wait().await;
                                        panic_report?;
                                        explicitly_suspended = host.explicitly_suspended();
                                        parked_checkpoint =
                                            host.parked_checkpoint().map_err(Error::Invalid)?;
                                        nested_resume_candidate = inspection.is_err()
                                            && host.only_pending_failure()
                                            && !tool_panicked
                                            && matches!(&reply, Some(Ok(reply)) if reply.state.is_none() && !matches!(reply.outcome, Outcome::Unknown(_) | Outcome::Pending { .. }));
                                        match inspection {
                                            Ok(()) => match reply {
                                                Some(Ok(reply)) => reply,
                                                None => {
                                                    *control
                                                        .lock()
                                                        .expect("control mutex poisoned") =
                                                        Default::default();
                                                    Outcome::Unknown(
                                                        "local cancellation after dispatch".into(),
                                                    )
                                                    .into()
                                                }
                                                Some(Err(_)) => {
                                                    *control
                                                        .lock()
                                                        .expect("control mutex poisoned") =
                                                        Default::default();
                                                    Outcome::Unknown(
                                                        "parent tool panicked after dispatch"
                                                            .into(),
                                                    )
                                                    .into()
                                                }
                                            },
                                            Err(error) => {
                                                *control.lock().expect("control mutex poisoned") =
                                                    Default::default();
                                                Outcome::Unknown(format!(
                                                    "nested tool requires reconciliation: {error}"
                                                ))
                                                .into()
                                            }
                                        }
                                    }
                                    Err(error) => {
                                        Outcome::Unknown(format!("nested tool host: {error}"))
                                            .into()
                                    }
                                }
                            }
                            Err(outcome) => outcome.into(),
                        }
                    } else {
                        Outcome::Failed(format!("tool {} unavailable", call.name)).into()
                    };
                    if cancel.is_cancelled() {
                        self.failed = true;
                        return Err(Error::Interrupted);
                    }
                    if !matches!(reply.outcome, Outcome::Unknown(_)) {
                        c.nested_resume = None;
                    }
                    if let Some(state) = reply.state {
                        c.tool_states.insert(call.name.clone(), state);
                    }
                    if let Some(adapter) = task_adapter
                        && matches!(&reply.outcome, Outcome::Pending { reference } if reference == &format!("task:{operation_id}"))
                    {
                        let task = adapter
                            .task(
                                c.id,
                                operation_id,
                                arguments.clone(),
                                self.options.restart_limit,
                            )
                            .map_err(Error::Invalid)?;
                        c.phase = Phase::TaskPending {
                            turns,
                            calls,
                            index,
                            operation_id,
                            arguments,
                            task: task.id(),
                            identity: adapter.identity(),
                        };
                        self.save_hook_children(
                            &c,
                            Expected::Version(version),
                            &[task],
                            &[adapter.handler()],
                        )?;
                        continue;
                    }
                    match reply.outcome {
                        Outcome::Unknown(reason) => {
                            c.nested_resume = if nested_resume_candidate
                                && replay == ReplayPolicy::Safe
                                && restarts.saturating_add(1) < self.options.restart_limit
                                && (c.extensions.is_empty() || explicitly_suspended)
                                && stored_identity == Some(DispatchIdentity::Other)
                            {
                                Some(NestedResume {
                                    operation_id,
                                    restarts,
                                    dispatch_identity: DispatchIdentity::Other,
                                    explicit_suspend: explicitly_suspended,
                                    pending_wake: parked_checkpoint,
                                })
                            } else {
                                None
                            };
                            c.phase = Phase::Blocked {
                                tool_panicked,
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason,
                                arguments: Some(arguments),
                            }
                        }
                        Outcome::Pending { reference } => {
                            c.phase = Phase::Pending {
                                turns,
                                calls,
                                index,
                                operation_id,
                                reference,
                                arguments: Some(arguments),
                            }
                        }
                        other => {
                            let reported = read_tool_diagnostics(&self.owner, c.id, operation_id)?;
                            record_tool_result(
                                &mut c,
                                &calls,
                                index,
                                operation_id,
                                &other,
                                reported,
                            )?;
                            // Older persisted rounds may predate either control vector.
                            c.round_terminates.resize(calls.len(), false);
                            c.round_handoffs.resize(calls.len(), None);
                            c.round_added_tools.resize(calls.len(), Vec::new());
                            let control = control.lock().expect("control mutex poisoned");
                            c.round_terminates[index] = control.terminate;
                            c.round_handoffs[index] = control.handoff.clone();
                            c.round_added_tools[index] = control.add_tools.clone();
                            let content =
                                model_tool_content(&c.tool_filters, &calls, index, &other);
                            c.phase = Phase::ToolFinished {
                                turns,
                                calls,
                                index,
                                operation_id,
                                arguments,
                                content,
                            };
                        }
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::ToolFinished {
                    turns,
                    calls,
                    index,
                    operation_id,
                    arguments,
                    content,
                } => {
                    let selected = self.selected_extensions(&c)?;
                    let mut snapshot = serde_json::to_value(&c)?;
                    if let Some(profile) = self
                        .provider_adapter
                        .as_ref()
                        .filter(|a| c.provider_adapter.as_deref() == Some(a.id()))
                        .and_then(|a| a.image_profile(&c.config.model))
                    {
                        snapshot["_model_image_profile"] = serde_json::to_value(profile)?;
                    }
                    let memo = self.extension_memo(operation_id)?;
                    let context = crate::extension::Context {
                        snapshot: &snapshot,
                        memo: &memo,
                    };
                    let call = calls
                        .get(index)
                        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
                    let hook_call = crate::extension::ToolCall {
                        name: call.name.clone(),
                        arguments,
                    };
                    let (result, reports) = fenced_poll(
                        selected.after_tool(
                            &hook_call,
                            serde_json::Value::String(content.clone()),
                            &context,
                        ),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?;
                    self.subagent_store()?;
                    if cancel.is_cancelled() {
                        self.failed = true;
                        return Err(Error::Interrupted);
                    }
                    self.record_extension_reports(c.id, operation_id, "after_tool", reports)?;
                    let replaced = result.as_str().is_some_and(|value| value != content);
                    let mut content = result.as_str().unwrap_or(&content).to_owned();
                    let pending = c.pending_tool_result.take();
                    let is_pending = pending.is_some();
                    let metadata = pending.or_else(|| {
                        c.tool_results
                            .iter()
                            .find(|metadata| metadata.operation_id == operation_id)
                            .cloned()
                    });
                    if let Some(mut metadata) = metadata {
                        if replaced {
                            redact_tool_metadata(
                                &mut metadata,
                                "output_redacted",
                                "tool metadata withheld after output replacement",
                            );
                        }
                        let value = crate::extension::ToolResult {
                            output: content.clone(),
                            is_error: metadata.is_error,
                            details: metadata.details.clone(),
                            structured_output: metadata.structured_output.clone(),
                            attachments: metadata.attachments.clone(),
                            diagnostics: metadata.diagnostics.clone(),
                        };
                        let (value, explicitly_replaced, reports) = fenced_poll(
                            selected.after_tool_result(&hook_call, value, &context),
                            self.execution_gate.as_ref(),
                            cancel,
                        )
                        .await
                        .ok_or(Error::Interrupted)?;
                        self.subagent_store()?;
                        if cancel.is_cancelled() {
                            self.failed = true;
                            return Err(Error::Interrupted);
                        }
                        self.record_extension_reports(
                            c.id,
                            operation_id,
                            "after_tool_result",
                            reports,
                        )?;
                        if explicitly_replaced {
                            content = value.output;
                            metadata.is_error = value.is_error;
                            metadata.details = value.details;
                            metadata.structured_output = value.structured_output;
                            metadata.attachments = value.attachments;
                            metadata.diagnostics = value.diagnostics;
                            metadata.redacted = false;
                            metadata.metadata_truncated = false;
                            metadata.schema_validated = false;
                        }
                        validate_tool_metadata(&c.tool_filters, &mut metadata);
                        if is_pending {
                            c.tool_results.push(metadata);
                        } else if let Some(existing) = c
                            .tool_results
                            .iter_mut()
                            .find(|item| item.operation_id == operation_id)
                        {
                            *existing = metadata;
                        }
                    }
                    let metadata = c
                        .tool_results
                        .iter()
                        .find(|m| m.operation_id == operation_id);
                    let mut extra = std::collections::BTreeMap::new();
                    if let Some(metadata) = metadata {
                        extra.insert("isError".into(), serde_json::json!(metadata.is_error));
                        if let Some(details) = &metadata.details {
                            extra.insert("details".into(), details.clone());
                        }
                        if let Some(data) = &metadata.structured_output {
                            extra.insert("structuredOutput".into(), data.clone());
                        }
                        extra.insert(
                            "attachments".into(),
                            serde_json::to_value(&metadata.attachments)?,
                        );
                    }
                    let block_result = crate::extension::ToolBlockResult {
                        output: vec![serde_json::json!({"type":"text", "text":content})],
                        diagnostics: metadata.map(|m| m.diagnostics.clone()).unwrap_or_default(),
                        extra,
                    };
                    let original_block_result = block_result.clone();
                    let (block_result, reports) = fenced_poll(
                        selected.runtime_after_tool_blocks(
                            &hook_call,
                            block_result,
                            false,
                            &context,
                        ),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?
                    .map_err(Error::Invalid)?;
                    self.subagent_store()?;
                    if cancel.is_cancelled() {
                        self.failed = true;
                        return Err(Error::Interrupted);
                    }
                    if !reports.is_empty() {
                        self.record_extension_reports(
                            c.id,
                            operation_id,
                            "after_tool_blocks",
                            reports,
                        )?;
                        return Err(Error::Invalid("block-aware after-tool hook failed".into()));
                    }
                    if block_result != original_block_result
                        && (block_result.diagnostics.len() > 32
                            || block_result.diagnostics.iter().any(|d| {
                                d.message.len() > 4096
                                    || d.code.as_ref().is_some_and(|code| code.len() > 256)
                            })
                            || serde_json::to_vec(&block_result.diagnostics)?.len() > 65_536
                            || serde_json::to_vec(&block_result.output)?.len() > 8 * 1024 * 1024)
                    {
                        return Err(Error::Invalid(
                            "block-aware tool result exceeds limits".into(),
                        ));
                    }
                    if let Some(metadata) = c
                        .tool_results
                        .iter_mut()
                        .find(|m| m.operation_id == operation_id)
                    {
                        if block_result != original_block_result
                            && serde_json::to_vec(&(
                                &metadata.details,
                                &metadata.structured_output,
                                &metadata.attachments,
                                &block_result.diagnostics,
                            ))?
                            .len()
                                > 65_536
                        {
                            return Err(Error::Invalid(
                                "block-aware tool metadata exceeds limits".into(),
                            ));
                        }
                        metadata.diagnostics = block_result.diagnostics;
                    }
                    let simple = block_result.output.len() == 1
                        && block_result.output[0]
                            .as_object()
                            .is_some_and(|o| o.len() == 2)
                        && block_result.output[0]["type"] == "text";
                    // A metadata-aware adapter requires a persistent occurrence ID
                    // even for a single text block, so a later fork or compaction
                    // cannot attach another call's status to it.
                    let needs_occurrence = self.status_aware(&c);
                    if simple && !needs_occurrence {
                        content = block_result.output[0]["text"].as_str().unwrap().to_owned();
                        append_tool(&mut c, &calls, index, content)?;
                    } else {
                        let position = c.messages.len();
                        append_tool(&mut c, &calls, index, String::new())?;
                        let message_id = Uuid::new_v4();
                        if let Some(metadata) = c
                            .tool_results
                            .iter_mut()
                            .find(|m| m.operation_id == operation_id)
                        {
                            metadata.content_block_id = Some(message_id);
                        }
                        c.content_blocks.insert(
                            position,
                            crate::transcript::MessageContentBlocks {
                                message_id,
                                blocks: block_result.output,
                            },
                        );
                    }
                    c.phase = Phase::Tools {
                        turns,
                        calls,
                        index: index + 1,
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::TaskPending {
                    turns,
                    calls,
                    index,
                    operation_id,
                    arguments,
                    task,
                    identity,
                } => {
                    self.validate_pending_task_identity(&c, &calls, index, &identity)?;
                    let Some(outcome) = self.task_tool_result(c.id, operation_id, task)? else {
                        return Ok(());
                    };
                    c.phase = if let Outcome::Unknown(reason) = outcome {
                        Phase::Blocked {
                            tool_panicked: false,
                            turns,
                            calls,
                            index,
                            operation_id,
                            reason,
                            arguments: Some(arguments),
                        }
                    } else {
                        let reported = read_tool_diagnostics(&self.owner, c.id, operation_id)?;
                        record_tool_result(
                            &mut c,
                            &calls,
                            index,
                            operation_id,
                            &outcome,
                            reported,
                        )?;
                        let content = model_tool_content(&c.tool_filters, &calls, index, &outcome);
                        Phase::ToolFinished {
                            turns,
                            calls,
                            index,
                            operation_id,
                            arguments,
                            content,
                        }
                    };
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::Pending {
                    turns,
                    calls,
                    index,
                    operation_id,
                    reference,
                    arguments,
                } => {
                    let call = calls
                        .get(index)
                        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
                    let tools = self.resolved_tools(&c)?;
                    let Some(tool) = tools.get(&call.name) else {
                        return Ok(());
                    };
                    let result = fenced_poll(
                        crate::poll::poll_once(
                            tool.tool.as_ref(),
                            operation_id,
                            &reference,
                            cancel,
                            Duration::from_secs(120),
                        ),
                        self.execution_gate.as_ref(),
                        cancel,
                    )
                    .await
                    .ok_or(Error::Interrupted)?;
                    match result {
                        crate::poll::PollResult::Outcome(Outcome::Pending { reference: next }) => {
                            if next != reference {
                                c.phase = Phase::Pending {
                                    turns,
                                    calls,
                                    index,
                                    operation_id,
                                    reference: next,
                                    arguments,
                                };
                                self.save(&c, Expected::Version(version))?;
                            }
                            return Ok(());
                        }
                        crate::poll::PollResult::Outcome(Outcome::Unknown(reason)) => {
                            c.phase = Phase::Blocked {
                                tool_panicked: false,
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason,
                                arguments,
                            };
                        }
                        crate::poll::PollResult::Outcome(outcome) => {
                            let reported = read_tool_diagnostics(&self.owner, c.id, operation_id)?;
                            record_tool_result(
                                &mut c,
                                &calls,
                                index,
                                operation_id,
                                &outcome,
                                reported,
                            )?;
                            let content =
                                model_tool_content(&c.tool_filters, &calls, index, &outcome);
                            let arguments = arguments.unwrap_or_else(|| {
                                pi_rs_core::parse_tool_arguments(&calls[index].arguments)
                                    .unwrap_or(serde_json::Value::Null)
                            });
                            c.phase = Phase::ToolFinished {
                                turns,
                                calls,
                                index,
                                operation_id,
                                arguments,
                                content,
                            };
                        }
                        crate::poll::PollResult::Cancelled => return Err(Error::Interrupted),
                        crate::poll::PollResult::Unavailable
                        | crate::poll::PollResult::TimedOut => return Ok(()),
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::FailedIdle if !c.pending_resets.is_empty() => {
                    for marker in take_reset_batch(&mut c) {
                        place_reset(&mut c, marker);
                    }
                    c.phase = Phase::Idle;
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let result = self.save(&c, Expected::Version(version));
                    self.merge_admissions = merge;
                    match result {
                        Ok(()) | Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Phase::Blocked { .. } | Phase::FailedIdle => return Ok(()),
            }
        }
    }
    // The report also serves as a recovery barrier before the blocked phase
    // is committed. Indexing is idempotent across restart and reconciliation.
    fn record_tool_panic(&self, id: Uuid, operation_id: Uuid) -> Result<(), Error> {
        self.record_extension_reports(
            id,
            operation_id,
            "tool_execution_panic",
            vec![crate::extension::Report {
                extension: "runtime".into(),
                operation: "tool_execution_panic".into(),
                error: "tool execution panicked; outcome requires reconciliation".into(),
            }],
        )
    }
    fn report_blocked_tool_panic(&self, c: &Conversation) -> Result<(), Error> {
        if let Phase::Blocked {
            tool_panicked: true,
            operation_id,
            ..
        } = &c.phase
        {
            self.record_tool_panic(c.id, *operation_id)?;
        }
        Ok(())
    }
    /// List nested pending jobs belonging to the currently blocked root operation.
    pub fn nested_pending_jobs(
        &self,
        id: Uuid,
        root: Uuid,
    ) -> Result<Vec<crate::nested::PendingJob>, Error> {
        let (c, _) = self.load(id)?;
        if !matches!(c.phase, Phase::Blocked { operation_id, .. } if operation_id == root) {
            return Err(Error::Invalid(
                "conversation is not blocked on this operation".into(),
            ));
        }
        crate::nested::pending_jobs(self.owner.store(), root)
    }
    /// Record a terminal observation for a nested pending job. An authoritative
    /// cancellation should be supplied as a failed outcome by the host after the
    /// remote cancellation is confirmed. This does not cancel the remote job.
    pub fn reconcile_nested_pending(
        &mut self,
        id: Uuid,
        root: Uuid,
        operation: Uuid,
        reference: &str,
        outcome: Outcome,
    ) -> Result<(), Error> {
        let (c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Blocked { operation_id, .. } if operation_id == root) {
            return Err(Error::Invalid(
                "conversation is not blocked on this operation".into(),
            ));
        }
        crate::nested::resolve_pending(
            self.owner.store(),
            (Self::key(id), version),
            root,
            operation,
            reference,
            outcome,
            false,
        )
    }
    /// Record confirmation that the remote job was cancelled. This performs no
    /// remote cancellation; the caller must obtain authoritative confirmation.
    pub fn cancel_nested_pending(
        &mut self,
        id: Uuid,
        root: Uuid,
        operation: Uuid,
        reference: &str,
        reason: String,
    ) -> Result<(), Error> {
        let (c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Blocked { operation_id, .. } if operation_id == root) {
            return Err(Error::Invalid(
                "conversation is not blocked on this operation".into(),
            ));
        }
        crate::nested::resolve_pending(
            self.owner.store(),
            (Self::key(id), version),
            root,
            operation,
            reference,
            Outcome::Failed(reason),
            true,
        )
    }
    /// Supply an authoritative outcome for an interrupted or pending operation.
    pub fn reconcile(
        &mut self,
        id: Uuid,
        operation_id: Uuid,
        outcome: Outcome,
    ) -> Result<(), Error> {
        if !matches!(
            outcome,
            Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. } | Outcome::Failed(_)
        ) {
            return Err(Error::Invalid(
                "reconciliation requires a known outcome".into(),
            ));
        }
        let (mut c, version) = self.load(id)?;
        self.report_blocked_tool_panic(&c)?;
        let (turns, calls, index, expected, arguments) = match c.phase.clone() {
            Phase::Blocked {
                turns,
                calls,
                index,
                operation_id,
                arguments,
                ..
            }
            | Phase::Pending {
                turns,
                calls,
                index,
                operation_id,
                arguments,
                ..
            } => (turns, calls, index, operation_id, arguments),
            _ => return Err(Error::Invalid("conversation is not blocked".into())),
        };
        if operation_id != expected {
            return Err(Error::Invalid("operation ID mismatch".into()));
        }
        {
            let prefix = format!("nested/{operation_id}/");
            for (_, entry) in self.owner.store().list(prefix.as_bytes())? {
                let record: serde_json::Value = serde_json::from_slice(&entry.value)?;
                let nested = record.get("outcome").filter(|v| !v.is_null());
                let status = nested
                    .and_then(|v| v.get("status"))
                    .and_then(|v| v.as_str());
                if !matches!(
                    status,
                    Some(
                        "succeeded"
                            | "succeeded_with_details"
                            | "failed"
                            | "skipped"
                            | "truncated"
                            | "structured"
                    )
                ) {
                    return Err(Error::Invalid("nested operation remains unresolved; parent reconciliation cannot establish its outcome".into()));
                }
            }
        }
        c.nested_resume = None;
        let reported = read_tool_diagnostics(&self.owner, c.id, operation_id)?;
        record_tool_result(&mut c, &calls, index, operation_id, &outcome, reported)?;
        if c.abort_requested {
            if !self.conversation_tasks(c.id)?.is_empty() {
                return Err(Error::Invalid("owned task cleanup is still pending".into()));
            }
            self.append_tool_with_status(
                &mut c,
                &calls,
                index,
                operation_id,
                outcome.tool_content(),
            )?;
            for i in index + 1..calls.len() {
                self.append_skipped_with_status(&mut c, &calls, i, "aborted before dispatch")?;
            }
            c.abort_requested = false;
            finish_abort(&mut c);
        } else if c.extensions.is_empty() {
            self.append_tool_with_status(
                &mut c,
                &calls,
                index,
                operation_id,
                outcome.tool_content(),
            )?;
            c.phase = Phase::Tools {
                turns,
                calls,
                index: index + 1,
            };
        } else {
            let arguments = arguments.unwrap_or_else(|| {
                pi_rs_core::parse_tool_arguments(&calls[index].arguments)
                    .unwrap_or(serde_json::Value::Null)
            });
            c.phase = Phase::ToolFinished {
                turns,
                calls,
                index,
                operation_id,
                arguments,
                content: outcome.tool_content(),
            };
        }
        self.save(&c, Expected::Version(version))
    }
    pub fn blocked(&self, id: Uuid) -> Result<Option<(Uuid, String)>, Error> {
        let (c, _) = self.load(id)?;
        Ok(match c.phase {
            Phase::Blocked {
                operation_id,
                reason,
                ..
            } => Some((operation_id, reason)),
            Phase::Pending {
                operation_id,
                reference,
                ..
            } => Some((operation_id, format!("pending: {reference}"))),
            Phase::TaskPending {
                operation_id, task, ..
            } => Some((operation_id, format!("pending task: {task}"))),
            _ => None,
        })
    }
}
struct SummaryPlacementDriver {
    harness: Harness,
    summary_provider: Option<SummaryProviderIdentity>,
    cancel: CancellationToken,
}
impl SummaryPlacementDriver {
    /// Admit a placement against the selected version, or a later version
    /// proven to contain only admissible inbox changes since a busy snapshot.
    fn place_summary(&mut self, id: Uuid) -> Result<SummaryPlacement, Error> {
        use crate::summary_store::{State, SummaryStore};
        let provider = self
            .summary_provider
            .as_ref()
            .ok_or_else(|| Error::Invalid("summary provider identity is not configured".into()))?;
        for _ in 0..8 {
            let owner = self.harness.owner.clone();
            let store = SummaryStore::new(owner.store());
            let snap = store
                .get(id)
                .map_err(|e| self.summary_store_error(e))?
                .ok_or(Error::NotFound)?;
            if snap.operation.provider() != Some(provider) {
                return Err(Error::Invalid(
                    "summary provider identity is missing or does not match".into(),
                ));
            }
            let (result, pending) = match snap.operation.state() {
                State::Placed { .. } => return Ok(SummaryPlacement::Placed),
                State::Stale { .. } => return Ok(SummaryPlacement::Stale),
                State::Completed { result, .. } => (result, false),
                State::PlacementPending { result, .. } => (result, true),
                State::Aborting { .. } | State::Aborted => return Err(Error::Interrupted),
                _ => return Err(Error::Invalid("summary is not completed".into())),
            };
            let conversation = snap.operation.session_id();
            let key = Harness::key(conversation);
            let current = owner.store().get(&key)?;
            let observed = current
                .as_ref()
                .map_or(Expected::Missing, |entry| Expected::Version(entry.version));
            if pending {
                store
                    .placement_ticket(&snap)
                    .map_err(|e| self.summary_store_error(e))?;
                if let Some(entry) = current.as_ref() {
                    let raw: Conversation = serde_json::from_slice(&entry.value)?;
                    if raw
                        .pending_independent_summary
                        .as_ref()
                        .is_some_and(|marker| marker.id == id)
                    {
                        if matches!(raw.phase, Phase::Idle | Phase::FailedIdle)
                            && raw.active.is_none()
                            && !raw.abort_requested
                        {
                            let (mut state, version) =
                                self.harness.load_for_summary_placement(conversation)?;
                            if current.as_ref().map(|entry| entry.version) != Some(version) {
                                continue;
                            }
                            match self
                                .harness
                                .consume_independent_summary(&mut state, version)
                            {
                                Ok(())
                                | Err(Error::Storage(durable_store::Error::Conflict { .. })) => {
                                    continue;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        return Ok(SummaryPlacement::Queued);
                    }
                }
            } else if let Some(version) = current.as_ref().map(|entry| entry.version)
                && current
                    .as_ref()
                    .is_some_and(|entry| entry.value.len() <= SUMMARY_MAX_BATCH_BYTES)
            {
                let (state, loaded_version) =
                    self.harness.load_for_summary_placement(conversation)?;
                if loaded_version != version {
                    continue;
                }
                let admissions_proven = if snap.operation.placement_version() == Some(version) {
                    true
                } else if let Some(checkpoint) = snap.operation.busy_checkpoint() {
                    let base: Conversation = serde_json::from_value(checkpoint.clone())?;
                    admission_only(&base, &state)? || summary_progress_only(&base, &state)?
                } else {
                    false
                };
                let marker = if admissions_proven
                    && (matches!(
                        state.phase,
                        Phase::ModelRunning { .. }
                            | Phase::OnYield { .. }
                            | Phase::AfterTools { .. }
                            | Phase::YieldDecision { .. }
                            | Phase::YieldTasksWaiting { .. }
                            | Phase::HookTasksWaiting { turns: Some(_), .. }
                    ) && state.active.is_some()
                        || summary_tool_phase_eligible(&state)
                        || matches!(
                            state.phase,
                            Phase::Idle
                                | Phase::FailedIdle
                                | Phase::AfterRun { .. }
                                | Phase::HookTasksWaiting { turns: None, .. }
                        ) && state.active.is_none())
                    && state.pending_independent_summary.is_none()
                    && state.pending_resets.len() <= SUMMARY_MAX_HISTORY_RECORDS
                    && !state.abort_requested
                    && (if result.has_blocks() {
                        result.matches_blocks_prefix(
                            &state.messages,
                            &state.content_blocks,
                            state.context_generation,
                        )
                    } else {
                        state.content_blocks.is_empty()
                    })
                    && !result.is_stale(&state.messages, state.context_generation)
                {
                    let mut raw: serde_json::Value =
                        serde_json::from_slice(&current.as_ref().unwrap().value)?;
                    raw["pending_independent_summary"] =
                        serde_json::to_value(PendingIndependentSummary {
                            id,
                            submissions: state.submissions.len(),
                            writes: state.application_writes.len(),
                            resets: state.resets.len(),
                            resets_before: state
                                .pending_resets
                                .iter()
                                .map(|reset| reset.id)
                                .collect(),
                        })?;
                    let encoded = serde_json::to_vec(&raw)?;
                    if encoded.len() > SUMMARY_MAX_BATCH_BYTES {
                        return Err(Error::Invalid(
                            "independent summary admission exceeds limits".into(),
                        ));
                    }
                    Some(encoded)
                } else {
                    None
                };
                if state.is_working() && marker.is_none() {
                    // The original busy snapshot changed or is no longer eligible.
                } else if marker.is_some() || snap.operation.placement_version() == Some(version) {
                    let queued = marker.is_some();
                    match store.admit_placement_with_marker(&snap, key.clone(), version, marker) {
                        Ok(()) if queued => continue,
                        Ok(()) | Err(crate::summary_store::Error::Conflict) => continue,
                        Err(error) => return Err(self.summary_store_error(error)),
                    }
                }
            }
            let mut placement = None;
            if current.as_ref().map(|entry| entry.version) == snap.operation.placement_version()
                && snap.operation.placement_version().is_some()
            {
                let (mut state, version) = self.harness.load_for_summary_placement(conversation)?;
                if observed != Expected::Version(version) {
                    continue;
                }
                if !state.is_working()
                    && !state.abort_requested
                    && (if result.has_blocks() {
                        result.matches_blocks(
                            &state.messages,
                            &state.content_blocks,
                            state.context_generation,
                        )
                    } else {
                        state.content_blocks.is_empty()
                    })
                    && result.source_matches(&state.messages)
                    && !result.is_stale(&state.messages, state.context_generation)
                {
                    if result.has_blocks() {
                        let (system_end, first_kept) = result.retained_range();
                        state.content_blocks = project_compaction_blocks(
                            &state.content_blocks,
                            system_end,
                            first_kept,
                        )?;
                    }
                    state.messages =
                        result.apply_in_context(&state.messages, state.context_generation)?;
                    state.context_generation = Some(Uuid::new_v4());
                    placement = Some(self.harness.prepare_conversation_save_inner(
                        &state,
                        Expected::Version(version),
                        None,
                        &[],
                        &[],
                        Some(result),
                    )?);
                }
            }
            let placed = placement.is_some();
            let (checks, mutations) = store
                .placement_batch(&snap, placed)
                .map_err(|e| self.summary_store_error(e))?;
            let mut batch = placement.unwrap_or_else(|| PreparedConversationSave {
                owner: owner.clone(),
                checks: vec![Check {
                    key,
                    expected: observed,
                }],
                mutations: vec![],
            });
            batch.extend(PreparedConversationSave {
                owner,
                checks,
                mutations,
            })?;
            let bytes = batch
                .mutations
                .iter()
                .map(|m| match m {
                    Mutation::Put { key, value } => key.len().saturating_add(value.len()),
                    Mutation::Delete { key } => key.len(),
                })
                .fold(0usize, usize::saturating_add);
            if bytes > SUMMARY_MAX_BATCH_BYTES
                || batch.checks.len() > 64
                || batch.mutations.len() > 64
            {
                return Err(Error::Invalid(
                    "independent summary placement batch exceeds limits".into(),
                ));
            }
            match self.harness.commit_conversation_save(batch) {
                Ok(()) => {
                    return Ok(if placed {
                        SummaryPlacement::Placed
                    } else {
                        SummaryPlacement::Stale
                    });
                }
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(Error::Invalid(
            "independent summary placement conflicted repeatedly".into(),
        ))
    }
    fn summary_store_error(&self, error: crate::summary_store::Error) -> Error {
        if matches!(error, crate::summary_store::Error::Store(_)) {
            self.harness.storage_failed.store(true, Ordering::Release);
            self.cancel.cancel();
        }
        Error::Invalid(format!("summary: {error}"))
    }
}

/// A background scheduler. Dropping it does not cancel already spawned work;
/// those tasks retain scheduler ownership until they stop. Call `shutdown` to
/// wait for all admitted work and release ownership deterministically.
pub struct Scheduler {
    harness: Harness,
    tasks: std::collections::HashMap<Uuid, Vec<tokio::task::JoinHandle<Result<(), Error>>>>,
    cancel: CancellationToken,
    reporter_cancel: CancellationToken,
    reporter: Option<tokio::task::JoinHandle<()>>,
    reporter_error: Arc<std::sync::Mutex<Option<String>>>,
    worker_errors: Arc<std::sync::Mutex<HashMap<Uuid, String>>>,
    summary_tasks: HashMap<Uuid, SummaryTask>,
    summary_provider: Option<SummaryProviderIdentity>,
    summary_blocked: Vec<Uuid>,
}

/// A coalesced hint that persistent state or a local worker changed.
/// These two views are not an atomic wake decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulerChange {
    pub commit_sequence: u64,
    pub workers: Vec<crate::worker_activity::Worker>,
}

/// Subscription to the scheduler's local persistent and worker changes.
pub struct SchedulerChanges {
    commits: durable_store::CommitUpdates,
    workers: crate::worker_activity::WorkerUpdates,
    commits_closed: bool,
    workers_closed: bool,
}

impl SchedulerChanges {
    /// Read the latest values and mark them observed. They may have changed
    /// independently and do not prove durable idleness.
    pub fn current(&mut self) -> SchedulerChange {
        SchedulerChange {
            commit_sequence: self.commits.current(),
            workers: self.workers.current(),
        }
    }

    /// Wait for a change from either source. Slow readers can miss intermediate
    /// transitions and should inspect current durable state after each hint.
    pub async fn changed(&mut self) -> Option<SchedulerChange> {
        loop {
            if self.commits_closed && self.workers_closed {
                return None;
            }
            tokio::select! {
                result = self.commits.changed(), if !self.commits_closed => {
                    if result.is_err() {
                        self.commits_closed = true;
                        continue;
                    }
                }
                result = self.workers.changed(), if !self.workers_closed => {
                    if result.is_err() {
                        self.workers_closed = true;
                        continue;
                    }
                }
            }
            return Some(self.current());
        }
    }
}

struct SummaryTask {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<Result<(), crate::summary_worker::Error>>,
}

impl Scheduler {
    /// Observe live executors shared with the attached task driver.
    pub fn worker_activity(&self) -> crate::worker_activity::WorkerActivity {
        self.harness.worker_activity()
    }
    /// Subscribe before inspecting state to avoid missing local mutations or
    /// worker transitions. External SQLite connections are not observed.
    pub fn subscribe_changes(&self) -> Result<SchedulerChanges, Error> {
        Ok(SchedulerChanges {
            commits: self.harness.owner.store().subscribe_commits()?,
            workers: self.harness.activity.subscribe(),
            commits_closed: false,
            workers_closed: false,
        })
    }
    /// Wait for a persisted submission settlement. Dropping or cancelling the
    /// wait leaves the submission untouched. The conversation is polled every
    /// 100 ms; blocked work may require host intervention or a task driver.
    pub async fn wait_for_submission(
        &self,
        conversation: Uuid,
        submission: Uuid,
        cancel: &CancellationToken,
    ) -> Result<SubmissionSettlement, Error> {
        loop {
            if cancel.is_cancelled() {
                return Err(Error::WaitCancelled);
            }
            let state = self.harness.conversation(conversation)?;
            let result = if let Some(item) = state.submissions.iter().find(|s| s.id == submission) {
                match &item.status {
                    Status::Done { answer } => Some(match item.kind {
                        SubmissionKind::Input => SubmissionSettlement::Input {
                            answer: answer.clone(),
                            answer_id: state.answer_id(submission),
                        },
                        SubmissionKind::Continue => SubmissionSettlement::Continue {
                            answer: answer.clone(),
                            answer_id: state.answer_id(submission),
                        },
                    }),
                    Status::Unanswered { reason } => Some(SubmissionSettlement::Unanswered {
                        kind: item.kind,
                        reason: reason.clone(),
                    }),
                    Status::Queued | Status::Running => None,
                }
            } else if let Some(write) = state.application_writes.iter().find(|w| w.id == submission)
            {
                if write.withdrawn {
                    Some(SubmissionSettlement::WriteUnanswered {
                        reason: "aborted".into(),
                    })
                } else {
                    write.placed.then_some(SubmissionSettlement::Write)
                }
            } else {
                return Err(Error::SubmissionNotFound);
            };
            if cancel.is_cancelled() {
                return Err(Error::WaitCancelled);
            }
            if let Some(result) = result {
                return Ok(result);
            }
            if self.cancel.is_cancelled() {
                return Err(Error::Interrupted);
            }
            if let Some(error) = self
                .worker_errors
                .lock()
                .map_err(|_| Error::Failed)?
                .get(&conversation)
                .cloned()
            {
                return Err(Error::Invalid(format!("scheduler worker failed: {error}")));
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::WaitCancelled),
                _ = self.cancel.cancelled() => return Err(Error::Interrupted),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
    }

    /// Wait until the conversation and its foreground owned graph have no work.
    /// Cancelling or dropping the future does not abort work. Live generic tasks
    /// require a running task driver. State is polled every 100 ms.
    pub async fn wait_for_idle(&self, id: Uuid, cancel: &CancellationToken) -> Result<(), Error> {
        loop {
            if cancel.is_cancelled() {
                return Err(Error::WaitCancelled);
            }
            let (busy, tasks_pending) = self.foreground_busy(id)?;
            if cancel.is_cancelled() {
                return Err(Error::WaitCancelled);
            }
            if tasks_pending {
                self.check_idle_task_driver()?;
            }
            if !busy {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::WaitCancelled),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
    }

    fn check_idle_task_driver(&self) -> Result<(), Error> {
        let driver = self
            .harness
            .task_driver
            .lock()
            .map_err(|_| Error::Failed)?
            .as_ref()
            .and_then(|driver| driver.upgrade())
            .ok_or_else(|| {
                Error::Invalid("task driver is not running; idle work remains pending".into())
            })?;
        driver.check_running_and_wake()
    }

    fn foreground_busy(&self, root: Uuid) -> Result<(bool, bool), Error> {
        use crate::ownership_edge::Mode;
        use crate::task::TaskOwner;
        use std::collections::{HashMap, HashSet, VecDeque};
        let owner = &self.harness.owner;
        if self.harness.failed
            || self.harness.dirty.load(Ordering::Acquire)
            || self.harness.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let store = owner.store();
        let snapshot = store.list_prefixes(&[
            crate::task_store::PREFIX,
            crate::ownership_edge::PREFIX,
            b"runtime/conversation/",
            crate::summary_store::PREFIX,
        ])?;
        let mut records = Vec::new();
        let mut edges = Vec::new();
        let mut snapshots = HashMap::new();
        let mut summaries = Vec::new();
        for (key, entry) in snapshot {
            if key.starts_with(crate::task_store::PREFIX) {
                records.push(
                    crate::task_store::decode(&key, entry)
                        .map_err(|e| Error::Invalid(format!("task list: {e}")))?,
                );
            } else if key.starts_with(crate::ownership_edge::PREFIX) {
                edges.push(
                    crate::ownership_edge::decode(&key, entry)
                        .map_err(|e| Error::Invalid(format!("ownership edges: {e}")))?,
                );
            } else if key.starts_with(crate::summary_store::PREFIX) {
                summaries.push(
                    crate::summary_store::decode_entry(&key, entry)
                        .map_err(|e| Error::Invalid(format!("summary idle lookup: {e}")))?,
                );
            } else {
                snapshots.insert(key, entry);
            }
        }
        let by_id: HashMap<_, _> = records.iter().map(|r| (r.task.id(), r)).collect();
        let mut conversations = VecDeque::from([root]);
        let mut tasks = VecDeque::new();
        let mut seen_conversations = HashSet::new();
        let mut seen_tasks = HashSet::new();
        let mut busy = false;
        let mut tasks_pending = false;
        while !conversations.is_empty() || !tasks.is_empty() {
            while let Some(id) = conversations.pop_front() {
                if !seen_conversations.insert(id) {
                    continue;
                }
                let entry = snapshots.get(&Harness::key(id)).ok_or(Error::NotFound)?;
                let conversation: Conversation = serde_json::from_slice(&entry.value)?;
                busy |= conversation.is_working() || conversation.abort_requested;
                busy |= summaries.iter().any(|summary| {
                    summary.operation.auto_place()
                        && summary.operation.session_id() == id
                        && (summary.operation.is_live_owned()
                            || self
                                .summary_tasks
                                .get(&summary.operation.id())
                                .is_some_and(|task| !task.join.is_finished()))
                });
                for record in &records {
                    if record.task.owner() == &TaskOwner::Conversation(id)
                        && !record.task.is_background()
                    {
                        tasks.push_back(record.task.id());
                    }
                }
            }
            while let Some(id) = tasks.pop_front() {
                if !seen_tasks.insert(id) {
                    continue;
                }
                let record = by_id.get(&id).ok_or(Error::NotFound)?;
                tasks_pending |= !record.task.is_terminal();
                for child in &records {
                    if child.task.owner() == &TaskOwner::Task(id) && !child.task.is_background() {
                        tasks.push_back(child.task.id());
                    }
                }
                for edge in edges.iter().filter(|edge| edge.edge.task == id) {
                    if edge.edge.task_owner != *record.task.owner() {
                        return Err(Error::Invalid("ownership edge owner mismatch".into()));
                    }
                    if edge.edge.mode == Mode::Foreground {
                        conversations.push_back(edge.edge.child);
                    }
                }
            }
        }
        Ok((busy || tasks_pending, tasks_pending))
    }
    /// Must be called inside a Tokio runtime. Recovers and schedules all
    /// persisted conversations, including work admitted by an earlier owner.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self, Error> {
        Self::open_with_extensions(path, options, crate::extension::Registry::default())
    }
    /// Install the initial registry before recovered conversations are scheduled.
    pub fn open_with_extensions(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
    ) -> Result<Self, Error> {
        Self::open_configured(path, options, registry, None, None)
    }
    /// Install the provider adapter before recovering persisted conversations.
    pub fn open_with_provider_adapter(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        adapter: Arc<dyn crate::model::ProviderAdapter>,
    ) -> Result<Self, Error> {
        Self::open_configured(path, options, registry, None, Some(adapter))
    }
    /// Opt into bounded recovery of independent summaries. The identity is a
    /// host assertion about `options.model`, including the service that handles
    /// its usage receipts. It must be stable across restarts and changed when
    /// either provider's replay semantics change. Unbound or mismatched records
    /// remain blocked. The adapter, when present, is installed before recovery.
    pub fn open_with_summary_recovery(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
        provider: SummaryProviderIdentity,
    ) -> Result<Self, Error> {
        Self::open_configured_with_tasks(
            path,
            options,
            registry,
            None,
            adapter,
            None,
            Some(provider),
        )
        .map(|(scheduler, _)| scheduler)
    }

    /// Install lifecycle task handlers and the summary provider before any
    /// recovered conversation or summary worker is scheduled. Keep the returned
    /// driver alive and shut it down after the scheduler.
    pub fn open_with_summary_recovery_and_tasks(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
        provider: SummaryProviderIdentity,
        tasks: crate::task_scheduler::Registry,
    ) -> Result<(Self, crate::task_driver::Driver), Error> {
        let (scheduler, driver) = Self::open_configured_with_tasks(
            path,
            options,
            registry,
            None,
            adapter,
            Some(tasks),
            Some(provider),
        )?;
        Ok((
            scheduler,
            driver.ok_or_else(|| Error::Invalid("task driver was not installed".into()))?,
        ))
    }

    /// Nonterminal independent summaries skipped during startup because their
    /// persisted provider binding was absent or different.
    pub fn blocked_summary_recovery(&self) -> &[Uuid] {
        &self.summary_blocked
    }
    /// Open with parent-bound management; new children exclude the `subagent` tool.
    /// Other registered tool names are captured as an allowlist at open time.
    pub fn open_with_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        parent: Uuid,
        child_config: Config,
    ) -> Result<Self, Error> {
        let allowed = default_subagent_tools(&options);
        Self::open_configured(
            path,
            options,
            registry,
            Some((parent, child_config, Some(allowed))),
            None,
        )
    }
    /// Restore provider and parent-bound subagent management before recovery.
    pub fn open_with_provider_and_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        adapter: Arc<dyn crate::model::ProviderAdapter>,
        parent: Uuid,
        child_config: Config,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        validate_allowed_tools(&allowed_tools)?;
        Self::open_configured(
            path,
            options,
            registry,
            Some((parent, child_config, allowed_tools)),
            Some(adapter),
        )
    }
    /// Install management with an initial child allowlist before recovery.
    pub fn open_with_restricted_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        parent: Uuid,
        child_config: Config,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        validate_allowed_tools(&allowed_tools)?;
        Self::open_configured(
            path,
            options,
            registry,
            Some((parent, child_config, allowed_tools)),
            None,
        )
    }
    fn open_configured(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        management: Option<(Uuid, Config, Option<Vec<String>>)>,
        adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
    ) -> Result<Self, Error> {
        Self::open_configured_with_tasks(path, options, registry, management, adapter, None, None)
            .map(|(scheduler, _)| scheduler)
    }
    fn open_configured_with_tasks(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        management: Option<(Uuid, Config, Option<Vec<String>>)>,
        adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
        tasks: Option<crate::task_scheduler::Registry>,
        summary_provider: Option<SummaryProviderIdentity>,
    ) -> Result<(Self, Option<crate::task_driver::Driver>), Error> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("scheduler requires a Tokio runtime".into()))?;
        if summary_provider
            .as_ref()
            .is_some_and(|identity| !identity.valid())
        {
            return Err(Error::Invalid("invalid summary provider identity".into()));
        }
        let mut harness = Harness::open_paused(path, options)?;
        if let Some(adapter) = adapter {
            harness.install_provider_adapter(adapter)?;
        }
        *harness.extensions.write().map_err(|_| Error::Failed)? = registry;
        harness.summary_provider = summary_provider.clone();
        if let Some((parent, config, allowed_tools)) = management {
            harness.conversation(parent)?;
            let mut worker = harness.worker();
            worker.extensions =
                Arc::new(std::sync::RwLock::new(crate::extension::Registry::default()));
            harness.install_extension(crate::subagent_management::extension(
                worker,
                parent,
                config,
                allowed_tools,
            ))?;
        }
        let ids = harness.conversations()?;
        let (summaries, summary_blocked) = if let Some(identity) = &summary_provider {
            crate::summary_store::SummaryStore::new(harness.owner.store())
                .discover(identity)
                .map_err(|e| Error::Invalid(format!("summary discovery: {e}")))?
        } else {
            (Vec::new(), Vec::new())
        };
        let mut scheduler = Self {
            cancel: harness.owner.conversation_controls().root(),
            worker_errors: harness.owner.conversation_controls().worker_errors(),
            harness,
            tasks: HashMap::new(),
            summary_tasks: HashMap::new(),
            summary_provider,
            summary_blocked,
            reporter_cancel: CancellationToken::new(),
            reporter: None,
            reporter_error: Arc::new(std::sync::Mutex::new(None)),
        };
        // Attach and publish the task driver before any recovered worker is spawned.
        let driver = tasks
            .map(|registry| scheduler.start_tasks(registry))
            .transpose()?;
        for id in ids {
            scheduler.schedule(id);
        }
        for (id, conversation) in summaries {
            scheduler.spawn_summary(id, conversation);
        }
        scheduler.start_reporter();
        Ok((scheduler, driver))
    }
    /// Read the same durable extension error log exposed by the harness.
    pub fn extension_reports(&self) -> Result<Vec<ExtensionReportBatch>, Error> {
        self.harness.extension_reports()
    }
    /// Subscribe to the durable extension report log.
    pub fn subscribe_extension_reports(&self) -> Result<crate::report::Subscription<'_>, Error> {
        self.harness.subscribe_extension_reports()
    }
    /// Start an opt-in host-owned asynchronous report listener.
    pub fn dispatch_extension_reports<F, Fut>(
        &self,
        cursor: u64,
        poll: Duration,
        listener: F,
    ) -> Result<crate::report::Dispatcher, Error>
    where
        F: FnMut(crate::report::Delivery) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
    {
        self.harness
            .dispatch_extension_reports(cursor, poll, listener)
    }
    pub fn install_extension(&self, extension: crate::extension::Extension) -> Result<(), Error> {
        self.harness.install_extension(extension)
    }
    pub fn uninstall_extension(&self, name: &str) -> Result<(), Error> {
        self.harness.uninstall_extension(name)
    }
    pub fn select_extensions(&mut self, id: Uuid, names: Vec<String>) -> Result<(), Error> {
        self.harness.select_extensions(id, names)
    }
    pub fn select_provider_adapter(&mut self, id: Uuid, enabled: bool) -> Result<(), Error> {
        self.harness.select_provider_adapter(id, enabled)
    }
    pub fn select_tools(&mut self, id: Uuid, names: Option<Vec<String>>) -> Result<(), Error> {
        self.harness.select_tools(id, names)
    }
    /// Persist an exact model-facing selection for a settled conversation.
    pub fn select_model_tools(
        &mut self,
        id: Uuid,
        names: Option<Vec<String>>,
    ) -> Result<(), Error> {
        self.harness.select_model_tools(id, names)
    }
    pub fn select_tools_except(&mut self, id: Uuid, names: Vec<String>) -> Result<(), Error> {
        self.harness.select_tools_except(id, names)
    }
    pub fn select_model_tools_patch(
        &mut self,
        id: Uuid,
        remove: Vec<String>,
        add: Vec<String>,
    ) -> Result<(), Error> {
        self.harness.select_model_tools_patch(id, remove, add)
    }
    pub fn set_deferred_tools(&mut self, id: Uuid, names: Vec<String>) -> Result<(), Error> {
        self.harness.set_deferred_tools(id, names)
    }
    pub fn set_tool_callers(
        &mut self,
        id: Uuid,
        name: String,
        callers: Option<ToolCallers>,
    ) -> Result<(), Error> {
        self.harness.set_tool_callers(id, name, callers)
    }
    pub fn set_tool_output_schema(
        &mut self,
        id: Uuid,
        name: String,
        schema: Option<serde_json::Value>,
    ) -> Result<(), Error> {
        self.harness.set_tool_output_schema(id, name, schema)
    }
    pub fn set_queue_modes(&mut self, id: Uuid, modes: QueueModes) -> Result<(), Error> {
        self.harness.set_queue_modes(id, modes)
    }
    pub fn set_after_failed_run(&mut self, id: Uuid, policy: AfterFailedRun) -> Result<(), Error> {
        self.harness.set_after_failed_run(id, policy)
    }
    pub fn create_conversation(&mut self, config: Config) -> Result<Uuid, Error> {
        self.harness.create_conversation(config)
    }
    pub(crate) fn subagent_harness(&mut self) -> &mut Harness {
        &mut self.harness
    }
    pub fn fork(&mut self, id: Uuid) -> Result<Uuid, Error> {
        self.harness.fork(id)
    }
    pub fn fork_at(&mut self, id: Uuid, cursor: crate::observer::Cursor) -> Result<Uuid, Error> {
        self.harness.fork_at(id, cursor)
    }
    pub fn fork_at_position(&mut self, id: Uuid, position: ViewCursor) -> Result<Uuid, Error> {
        self.harness.fork_at_position(id, position)
    }
    pub fn fork_at_application_entry(&mut self, id: Uuid, entry_id: Uuid) -> Result<Uuid, Error> {
        self.harness.fork_at_application_entry(id, entry_id)
    }
    pub fn fork_by_entry_id(&mut self, id: Uuid, entry_id: Uuid) -> Result<Uuid, Error> {
        self.harness.fork_by_entry_id(id, entry_id)
    }
    pub fn conversation(&self, id: Uuid) -> Result<Conversation, Error> {
        self.harness.conversation(id)
    }
    pub fn document(
        &self,
        address: &crate::document::Address,
    ) -> Result<Option<crate::document::Document>, Error> {
        self.harness.document(address)
    }
    pub fn view(&self, id: Uuid, limit: std::num::NonZeroUsize) -> Result<ConversationView, Error> {
        self.harness.view(id, limit)
    }
    pub fn tool_progress_events(
        &self,
        attempt_id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.tool_progress_events(attempt_id, after, limit)
    }
    pub fn tool_attempts(
        &self,
        conversation_id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.tool_attempts(conversation_id, after, limit)
    }
    pub fn watch_committed_view(
        &self,
        id: Uuid,
        after: Option<CommittedViewCursor>,
        interval: Duration,
    ) -> Result<CommittedViewWatch, Error> {
        self.harness.watch_committed_view(id, after, interval)
    }
    pub fn watch_view(
        &self,
        id: Uuid,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<ViewWatch, Error> {
        self.harness.watch_view(id, limit, interval)
    }
    pub fn view_page(
        &self,
        id: Uuid,
        cursor: ViewCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<ViewPage, Error> {
        self.harness.view_page(id, cursor, limit)
    }
    pub fn view_positioned(
        &self,
        id: Uuid,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedViewPage, Error> {
        self.harness.view_positioned(id, limit)
    }
    pub fn view_positioned_page(
        &self,
        id: Uuid,
        position: ViewCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedViewPage, Error> {
        self.harness.view_positioned_page(id, position, limit)
    }
    pub fn send_signal(&self, signal: &crate::conversation_signal::Signal) -> Result<(), Error> {
        self.harness.send_signal(signal)
    }
    pub fn attach_tasks(
        &self,
        registry: crate::task_scheduler::Registry,
    ) -> Result<crate::task_scheduler::Scheduler, Error> {
        self.harness.attach_tasks(registry)
    }
    pub fn start_tasks(
        &self,
        registry: crate::task_scheduler::Registry,
    ) -> Result<crate::task_driver::Driver, Error> {
        self.harness.start_tasks(registry)
    }
    pub fn watch_merged_progress_handle(
        &self,
        conversation: Uuid,
        after: Option<MergedProgressCursor>,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<MergedProgressWatch, Error> {
        self.harness
            .watch_merged_progress_handle(conversation, after, limit, interval)
    }
    pub fn merged_progress_operations(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<IncrementalMergedPage, Error> {
        self.harness.merged_progress_operations(cursor, limit)
    }
    pub fn merged_progress_from_now(
        &self,
        conversation: Uuid,
    ) -> Result<MergedProgressCursor, Error> {
        self.harness.merged_progress_from_now(conversation)
    }
    pub fn merged_progress(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<MergedProgressPage, Error> {
        self.harness.merged_progress(cursor, limit)
    }
    pub async fn watch_merged_progress(
        &self,
        cursor: &MergedProgressCursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<MergedProgressPage, Error> {
        self.harness
            .watch_merged_progress(cursor, limit, interval)
            .await
    }
    pub fn progress_history_from_now(
        &self,
        conversation: Uuid,
    ) -> Result<ProgressHistoryCursor, Error> {
        self.harness.progress_history_from_now(conversation)
    }
    pub fn progress_history(
        &self,
        cursor: &ProgressHistoryCursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<ProgressHistoryPage, Error> {
        self.harness.progress_history(cursor, limit)
    }
    pub async fn watch_progress_history(
        &self,
        cursor: &ProgressHistoryCursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<ProgressHistoryPage, Error> {
        self.harness
            .watch_progress_history(cursor, limit, interval)
            .await
    }
    pub fn history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.history(id, after, limit)
    }
    pub fn history_head(&self, id: Uuid) -> Result<crate::observer::Cursor, Error> {
        self.harness.history_head(id)
    }
    pub fn history_through(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        through: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.history_through(id, after, through, limit)
    }
    pub fn reset(&mut self, id: Uuid, handoff: Option<String>) -> Result<Uuid, Error> {
        let marker = self.harness.reset(id, handoff)?;
        self.schedule(id);
        Ok(marker)
    }
    /// Admit a reset with ordered block occurrences and schedule placement.
    pub fn reset_with_blocks(
        &mut self,
        id: Uuid,
        handoff: Option<String>,
        additions: Vec<Vec<serde_json::Value>>,
    ) -> Result<(Uuid, Vec<Uuid>), Error> {
        let admitted = self.harness.reset_with_blocks(id, handoff, additions)?;
        self.schedule(id);
        Ok(admitted)
    }
    pub fn submit_write(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        kind: String,
        data: Option<serde_json::Value>,
    ) -> Result<Uuid, Error> {
        let write = self.harness.submit_write(id, request_id, kind, data)?;
        self.schedule(id);
        Ok(write)
    }
    pub async fn watch_history(
        &self,
        id: Uuid,
        after: crate::observer::Cursor,
        limit: std::num::NonZeroUsize,
        interval: Duration,
    ) -> Result<crate::observer::Page, Error> {
        self.harness.watch_history(id, after, limit, interval).await
    }
    pub fn request_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        let queued = self.harness.request_compaction(id, keep_recent_tokens)?;
        if queued {
            self.schedule(id);
        }
        Ok(queued)
    }
    /// Admit and schedule an idle manual compaction with a durable identity.
    pub fn request_manual_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
        instructions: Option<&str>,
    ) -> Result<Option<Uuid>, Error> {
        let operation =
            self.harness
                .request_manual_compaction(id, keep_recent_tokens, instructions)?;
        if operation.is_some() {
            self.schedule(id);
        }
        Ok(operation)
    }

    /// Start an independent compaction of an idle conversation with a caller-
    /// supplied unique operation ID. Returns false if there is nothing to compact.
    /// Retain the ID: a storage error can leave admission outcome unknown, and
    /// its status can be inspected after reopen. Requires recovery configuration.
    /// Selected before-compact hooks run against a persisted admission snapshot.
    /// A concurrent conversation change settles the result stale.
    /// Idle here refers to the conversation's own phase and queue; owned
    /// task and descendant activity is not included in this admission check.
    /// Conversation abort and foreground idle waits include this worker.
    pub fn request_idle_independent_compaction(
        &mut self,
        conversation: Uuid,
        id: Uuid,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        self.request_idle_independent_compaction_with_instructions(
            conversation,
            id,
            keep_recent_tokens,
            None,
        )
    }

    /// Admit an idle independent compaction with optional before-compact instructions.
    pub fn request_idle_independent_compaction_with_instructions(
        &mut self,
        conversation: Uuid,
        id: Uuid,
        keep_recent_tokens: usize,
        instructions: Option<&str>,
    ) -> Result<bool, Error> {
        if id.is_nil() {
            return Err(Error::Invalid("independent compaction ID is nil".into()));
        }
        if self.harness.failed
            || self.harness.dirty.load(Ordering::Acquire)
            || self.harness.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        if crate::summary_store::SummaryStore::new(self.harness.owner.store())
            .get(id)
            .map_err(|e| self.summary_store_error(e))?
            .is_some()
        {
            return Err(Error::Invalid(
                "independent compaction ID already exists".into(),
            ));
        }
        if !self.create_independent_summary_impl(
            id,
            conversation,
            None,
            keep_recent_tokens,
            (true, false),
            instructions,
        )? {
            return Ok(false);
        }
        self.spawn_summary(id, conversation);
        Ok(true)
    }

    /// Admit an independent summary during a model request or an eligible
    /// tool attempt in a proven round. Results settle at the next safe boundary.
    pub fn request_busy_independent_compaction(
        &mut self,
        conversation: Uuid,
        id: Uuid,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        if id.is_nil() {
            return Err(Error::Invalid("independent compaction ID is nil".into()));
        }
        if self.harness.failed
            || self.harness.dirty.load(Ordering::Acquire)
            || self.harness.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        if crate::summary_store::SummaryStore::new(self.harness.owner.store())
            .get(id)
            .map_err(|e| self.summary_store_error(e))?
            .is_some()
        {
            return Err(Error::Invalid(
                "independent compaction ID already exists".into(),
            ));
        }
        if !self.create_independent_summary_impl(
            id,
            conversation,
            None,
            keep_recent_tokens,
            (true, true),
            None,
        )? {
            return Ok(false);
        }
        self.spawn_summary(id, conversation);
        Ok(true)
    }

    /// Read an independent operation without driving it.
    pub fn independent_compaction_status(
        &self,
        id: Uuid,
    ) -> Result<IndependentCompactionStatus, Error> {
        use crate::summary_store::{State, SummaryStore};
        if self.harness.failed
            || self.harness.dirty.load(Ordering::Acquire)
            || self.harness.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let provider = self
            .summary_provider
            .as_ref()
            .ok_or_else(|| Error::Invalid("summary provider identity is not configured".into()))?;
        let snap = SummaryStore::new(self.harness.owner.store())
            .get(id)
            .map_err(|e| self.summary_store_error(e))?
            .ok_or(Error::NotFound)?;
        if !snap.operation.auto_place() || snap.operation.provider() != Some(provider) {
            return Err(Error::Invalid(
                "independent compaction identity is missing or does not match".into(),
            ));
        }
        Ok(match snap.operation.state() {
            State::Placed { .. } => IndependentCompactionStatus::Applied,
            State::Stale { .. } => IndependentCompactionStatus::Stale,
            State::Aborting { .. } => IndependentCompactionStatus::Aborting,
            State::Aborted => IndependentCompactionStatus::Aborted,
            State::Declined => IndependentCompactionStatus::Declined,
            _ => IndependentCompactionStatus::Running,
        })
    }

    /// Wait for this scheduler's worker, including placement. If an earlier
    /// wait observed a failure, explicitly reschedule or reopen to recover.
    pub async fn wait_independent_compaction(
        &mut self,
        id: Uuid,
    ) -> Result<IndependentCompactionStatus, Error> {
        let status = self.independent_compaction_status(id)?;
        if self.summary_tasks.contains_key(&id) {
            if let Err(error) = self.wait_summary(id).await {
                let status = self.independent_compaction_status(id)?;
                if matches!(
                    status,
                    IndependentCompactionStatus::Aborted
                        | IndependentCompactionStatus::Applied
                        | IndependentCompactionStatus::Stale
                        | IndependentCompactionStatus::Declined
                ) {
                    return Ok(status);
                }
                return Err(error);
            }
        } else if matches!(
            status,
            IndependentCompactionStatus::Running | IndependentCompactionStatus::Aborting
        ) {
            return Err(Error::Invalid(
                "independent compaction is not scheduled".into(),
            ));
        }
        self.independent_compaction_status(id)
    }

    /// Explicitly retry an interrupted independent operation under its fence.
    pub fn resume_independent_compaction(&mut self, id: Uuid) -> Result<(), Error> {
        if !matches!(
            self.independent_compaction_status(id)?,
            IndependentCompactionStatus::Running | IndependentCompactionStatus::Aborting
        ) {
            return Err(Error::Invalid(
                "independent compaction is already terminal".into(),
            ));
        }
        self.schedule_summary(id)
    }

    /// Persist abort, cancel and join the worker, then settle usage before
    /// returning. A placement that committed first remains terminal.
    pub async fn abort_independent_compaction(
        &mut self,
        id: Uuid,
    ) -> Result<IndependentCompactionStatus, Error> {
        self.independent_compaction_status(id)?;
        self.abort_summary(id).await?;
        self.independent_compaction_status(id)
    }
    /// Commits the admission before scheduling, including duplicate request IDs.
    pub fn submit(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
    ) -> Result<Uuid, Error> {
        let submission = self.harness.submit(id, request_id, content)?;
        self.schedule(id);
        Ok(submission)
    }
    /// Fit and commit user blocks before scheduling their run.
    pub async fn submit_fitted_user_blocks(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        blocks: Vec<serde_json::Value>,
        admission: &crate::images::UserImageAdmission,
        cancel: &CancellationToken,
    ) -> Result<crate::images::ImageAdmissionResult, Error> {
        let result = self
            .harness
            .submit_fitted_user_blocks(id, request_id, blocks, admission, cancel)
            .await?;
        self.schedule(id);
        Ok(result)
    }
    /// Fit and commit a reset, then schedule its placement.
    pub async fn reset_with_fitted_blocks(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        handoff: Option<String>,
        additions: Vec<Vec<serde_json::Value>>,
        admission: &crate::images::UserImageAdmission,
        cancel: &CancellationToken,
    ) -> Result<(Uuid, Vec<Uuid>, Vec<String>), Error> {
        let result = self
            .harness
            .reset_with_fitted_blocks(id, request_id, handoff, additions, admission, cancel)
            .await?;
        self.schedule(id);
        Ok(result)
    }
    /// Fit and schedule a steering or follow-up block input.
    pub async fn submit_fitted_user_blocks_with_options(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        blocks: Vec<serde_json::Value>,
        admission: &crate::images::UserImageAdmission,
        options: AdmissionOptions,
        cancel: &CancellationToken,
    ) -> Result<crate::images::ImageAdmissionResult, Error> {
        let result = self
            .harness
            .submit_fitted_user_blocks_with_options(
                id, request_id, blocks, admission, options, cancel,
            )
            .await?;
        self.schedule(id);
        Ok(result)
    }
    /// Commit a steering or follow-up input and schedule the conversation.
    pub fn submit_with_mode(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        mode: InputMode,
    ) -> Result<Uuid, Error> {
        let submission = self
            .harness
            .submit_with_mode(id, request_id, content, mode)?;
        self.schedule(id);
        Ok(submission)
    }
    /// Admit with policies and schedule only after a successful admission.
    pub fn submit_with_options(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        options: AdmissionOptions,
    ) -> Result<Uuid, Error> {
        let submission = self
            .harness
            .submit_with_options(id, request_id, content, options)?;
        self.schedule(id);
        Ok(submission)
    }
    /// Atomically admit input and document changes, then schedule the run.
    pub fn submit_with_documents(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        options: AdmissionOptions,
        change: impl FnMut(
            &mut crate::document::AdmissionDocuments<'_>,
        ) -> Result<(), crate::document::Error>,
    ) -> Result<Uuid, Error> {
        let submission = self
            .harness
            .submit_with_documents(id, request_id, content, options, change)?;
        self.schedule(id);
        Ok(submission)
    }
    /// Admit a continuation and schedule its persisted run.
    pub fn submit_continue(&mut self, id: Uuid, request_id: Option<String>) -> Result<Uuid, Error> {
        let submission = self.harness.submit_continue(id, request_id)?;
        self.schedule(id);
        Ok(submission)
    }
    pub fn withdraw_submission(
        &mut self,
        id: Uuid,
        submission: Uuid,
    ) -> Result<WithdrawResult, Error> {
        self.harness.withdraw_submission(id, submission)
    }
    pub fn blocked(&self, id: Uuid) -> Result<Option<(Uuid, String)>, Error> {
        self.harness.blocked(id)
    }
    pub fn nested_pending_jobs(
        &self,
        id: Uuid,
        root: Uuid,
    ) -> Result<Vec<crate::nested::PendingJob>, Error> {
        self.harness.nested_pending_jobs(id, root)
    }
    pub fn reconcile_nested_pending(
        &mut self,
        id: Uuid,
        root: Uuid,
        operation: Uuid,
        reference: &str,
        outcome: Outcome,
    ) -> Result<(), Error> {
        self.harness
            .reconcile_nested_pending(id, root, operation, reference, outcome)?;
        self.schedule(id);
        Ok(())
    }
    pub fn cancel_nested_pending(
        &mut self,
        id: Uuid,
        root: Uuid,
        operation: Uuid,
        reference: &str,
        reason: String,
    ) -> Result<(), Error> {
        self.harness
            .cancel_nested_pending(id, root, operation, reference, reason)?;
        self.schedule(id);
        Ok(())
    }
    /// Record a known tool outcome and resume the conversation.
    pub fn reconcile(
        &mut self,
        id: Uuid,
        operation_id: Uuid,
        outcome: Outcome,
    ) -> Result<(), Error> {
        self.harness.reconcile(id, operation_id, outcome)?;
        self.schedule(id);
        Ok(())
    }
    /// Release inputs held by the failed-run policy and schedule them.
    pub fn continue_queued(&mut self, id: Uuid) -> Result<(), Error> {
        self.harness.continue_queued(id)?;
        self.schedule(id);
        Ok(())
    }
    /// Persist intent before signalling every affected worker, then drive cleanup.
    pub fn abort(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_graph(Some(id), None).map(|_| ())
    }

    /// Persist and schedule abort across background ownership boundaries.
    pub fn abort_with_background(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_background_graph(id).map(|_| ())
    }

    fn abort_background_graph(
        &mut self,
        id: Uuid,
    ) -> Result<(Vec<Uuid>, Vec<Uuid>, ConversationAbortResult), Error> {
        let owner = self.harness.owner.clone();
        let mut tasks = Vec::new();
        let mut result = ConversationAbortResult::default();
        let affected = owner
            .conversation_controls()
            .fence(|| {
                let (conversations, reached, withdrawn) =
                    self.harness
                        .abort_owned_graph_impl(Some(id), None, None, true)?;
                tasks = reached;
                result = withdrawn;
                Ok(conversations)
            })
            .map_err(|error| match error {
                durable_store::controls::FenceError::Poisoned => Error::Failed,
                durable_store::controls::FenceError::Commit(error) => error,
            })?;
        for conversation in &affected {
            self.schedule(*conversation);
        }
        Ok((affected, tasks, result))
    }

    /// Abort a task and its foreground owned conversations.
    pub fn abort_task(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_graph(None, Some(id)).map(|_| ())
    }

    fn abort_graph(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
    ) -> Result<Vec<Uuid>, Error> {
        self.abort_graph_with_result(conversation, task)
            .map(|(affected, _)| affected)
    }

    fn abort_graph_with_result(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
    ) -> Result<(Vec<Uuid>, ConversationAbortResult), Error> {
        // Prevent controls from being created and fence every existing poll while
        // discovering and committing the graph. A child may be admitted right up
        // to the transaction, so fencing a previously read graph is insufficient.
        let owner = self.harness.owner.clone();
        let mut result = ConversationAbortResult::default();
        let affected = owner
            .conversation_controls()
            .fence(|| {
                let (affected, _, withdrawn) =
                    self.harness
                        .abort_owned_graph_impl(conversation, task, None, false)?;
                result = withdrawn;
                Ok(affected)
            })
            .map_err(|error| match error {
                durable_store::controls::FenceError::Poisoned => Error::Failed,
                durable_store::controls::FenceError::Commit(error) => error,
            })?;
        for id in &affected {
            self.schedule(*id);
        }
        Ok((affected, result))
    }
    /// Request abort and join workers for every affected foreground conversation.
    pub async fn abort_and_wait(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_and_wait_withdrawn(id).await.map(|_| ())
    }
    /// Abort and wait, returning the queued inputs withdrawn in the committed transaction.
    pub async fn abort_and_wait_withdrawn(
        &mut self,
        id: Uuid,
    ) -> Result<ConversationAbortResult, Error> {
        let (affected, result) = self.abort_graph_with_result(Some(id), None)?;
        let mut first_error = None;
        for child in &affected {
            if let Err(error) = self.wait(*child).await {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        for child in affected {
            if self.harness.conversation(child)?.abort_requested {
                return Err(Error::Invalid(format!(
                    "abort cleanup remains pending for conversation {child}"
                )));
            }
        }
        Ok(result)
    }
    /// Abort across background boundaries and wait for the reached work to settle.
    /// A task driver must be running for any nonterminal generic tasks.
    pub async fn abort_and_wait_with_background(&mut self, id: Uuid) -> Result<(), Error> {
        self.abort_and_wait_with_background_withdrawn(id)
            .await
            .map(|_| ())
    }
    /// Abort across background boundaries and return the withdrawn inputs after cleanup.
    pub async fn abort_and_wait_with_background_withdrawn(
        &mut self,
        id: Uuid,
    ) -> Result<ConversationAbortResult, Error> {
        let (conversations, tasks, result) = self.abort_background_graph(id)?;
        let mut first_error = None;
        for conversation in &conversations {
            loop {
                match tokio::time::timeout(Duration::from_millis(100), self.wait(*conversation))
                    .await
                {
                    Ok(Ok(())) => break,
                    Ok(Err(error)) => {
                        first_error.get_or_insert(error);
                        break;
                    }
                    Err(_) => {
                        self.check_abort_task_progress(&tasks)?;
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        for conversation in conversations {
            if self.harness.conversation(conversation)?.abort_requested {
                return Err(Error::Invalid(format!(
                    "abort cleanup remains pending for conversation {conversation}"
                )));
            }
        }
        loop {
            if !self.check_abort_task_progress(&tasks)? {
                return Ok(result);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn check_abort_task_progress(&self, tasks: &[Uuid]) -> Result<bool, Error> {
        let store = crate::task_store::TaskStore::new(self.harness.owner.store());
        let mut pending = false;
        for id in tasks {
            let task = store
                .load(*id)
                .map_err(|e| Error::Invalid(format!("task lookup: {e}")))?
                .ok_or(Error::NotFound)?;
            pending |= !task.task.is_terminal();
        }
        if pending {
            let driver = self
                .harness
                .task_driver
                .lock()
                .map_err(|_| Error::Failed)?
                .as_ref()
                .and_then(|driver| driver.upgrade());
            let Some(driver) = driver else {
                return Err(Error::Invalid(
                    "task driver is not running; abort cleanup remains pending".into(),
                ));
            };
            driver.check_running_and_wake()?;
        }
        Ok(pending)
    }
    /// Schedule a retry after an execution error or a dropped worker.
    pub fn schedule(&mut self, id: Uuid) {
        let control = self.harness.owner.conversation_controls().get(id);
        let lock = control.worker;
        let cancel = control.cancel;
        let mut worker = self.harness.worker();
        worker.execution_gate = Some(control.gate);
        let errors = self.worker_errors.clone();
        let activity = self.harness.activity.enter_at(
            crate::worker_activity::WorkerKind::Conversation,
            id,
            crate::worker_activity::WorkerPhase::Queued,
        );
        let task = tokio::spawn(async move {
            // A queued worker must not delay shutdown behind another owner.
            let mut guard = Some(tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                guard = lock.lock() => guard,
            });
            // A cancelled queued retry never took over the failed execution.
            if cancel.is_cancelled() {
                return Ok(());
            }
            let result = std::panic::AssertUnwindSafe(drive_scheduled(
                &mut worker,
                id,
                &cancel,
                &lock,
                &mut guard,
                Some(&errors),
                activity,
            ))
            .catch_unwind()
            .await;
            let failure = match &result {
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("worker panicked".into()),
                Ok(Ok(())) => None,
            };
            if let Some(failure) = failure {
                errors
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(id, failure);
            }
            match result {
                Ok(result) => result,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        });
        self.tasks.entry(id).or_default().push(task);
    }

    // The private test worker does not automatically place its result.
    #[allow(dead_code)]
    fn create_independent_summary(
        &self,
        id: Uuid,
        conversation: Uuid,
        max_output_tokens: u32,
        keep_recent_tokens: usize,
    ) -> Result<bool, Error> {
        self.create_independent_summary_impl(
            id,
            conversation,
            Some(max_output_tokens),
            keep_recent_tokens,
            (false, false),
            None,
        )
    }

    fn create_independent_summary_impl(
        &self,
        id: Uuid,
        conversation: Uuid,
        max_output_tokens: Option<u32>,
        keep_recent_tokens: usize,
        (auto_place, allow_busy): (bool, bool),
        instructions: Option<&str>,
    ) -> Result<bool, Error> {
        let provider = self
            .summary_provider
            .clone()
            .ok_or_else(|| Error::Invalid("summary provider identity is not configured".into()))?;
        if auto_place {
            crate::summary_store::SummaryStore::new(self.harness.owner.store())
                .ensure_creation_capacity()
                .map_err(|e| self.summary_store_error(e))?;
        }
        let (state, version) = self.harness.load_for_summary_placement(conversation)?;
        let busy_ok = allow_busy
            && (matches!(state.phase, Phase::ModelRunning { .. }) && state.active.is_some()
                || summary_tool_phase_eligible(&state))
            && state.pending_independent_summary.is_none()
            && state.pending_resets.is_empty()
            && !state.submissions.iter().any(|s| s.status == Status::Queued);
        if (state.is_working() && !busy_ok)
            || state.abort_requested
            || state.pending_independent_summary.is_some()
        {
            return Err(Error::Invalid(
                "conversation is not eligible for independent placement".into(),
            ));
        }
        if auto_place && !busy_ok && !matches!(state.phase, Phase::Idle) {
            return Err(Error::Invalid(
                "independent compaction requires an idle conversation".into(),
            ));
        }
        let generation = state
            .context_generation
            .ok_or_else(|| Error::Invalid("conversation lacks context identity".into()))?;
        let mut prepared_state = state.clone();
        prepared_state.config.max_output_tokens =
            max_output_tokens.unwrap_or(state.config.max_output_tokens);
        let Some(preparation) = self.harness.prepare_runtime_compaction(
            &prepared_state,
            keep_recent_tokens,
            instructions,
        )?
        else {
            return Ok(false);
        };
        let hook = if auto_place && !state.extensions.is_empty() {
            self.harness.selected_extensions(&state)?;
            Some(crate::summary_store::HookCheckpoint {
                extensions: state.extensions.clone(),
                snapshot: serde_json::to_value(&state)?,
            })
        } else {
            None
        };
        crate::summary_store::SummaryStore::new(self.harness.owner.store())
            .create_bound(
                id,
                conversation,
                preparation.bind_context(generation),
                (Harness::key(conversation), version),
                provider,
                (
                    auto_place,
                    hook,
                    if busy_ok {
                        Some(serde_json::to_value(&state)?)
                    } else {
                        None
                    },
                ),
            )
            .map_err(|e| self.summary_store_error(e))?;
        Ok(true)
    }

    #[cfg(test)]
    fn place_summary(&mut self, id: Uuid) -> Result<SummaryPlacement, Error> {
        SummaryPlacementDriver {
            harness: self.harness.worker(),
            summary_provider: self.summary_provider.clone(),
            cancel: self.cancel.clone(),
        }
        .place_summary(id)
    }

    #[allow(dead_code)]
    fn schedule_summary(&mut self, id: Uuid) -> Result<(), Error> {
        if self.harness.storage_failed.load(Ordering::Acquire) {
            return Err(Error::Failed);
        }
        if self.summary_tasks.contains_key(&id) {
            return Err(Error::Invalid("summary already scheduled".into()));
        }
        let provider = self
            .summary_provider
            .as_ref()
            .ok_or_else(|| Error::Invalid("summary provider identity is not configured".into()))?;
        let snap = crate::summary_store::SummaryStore::new(self.harness.owner.store())
            .get(id)
            .map_err(|e| self.summary_store_error(e))?
            .ok_or(Error::NotFound)?;
        if snap.operation.provider() != Some(provider) {
            return Err(Error::Invalid(
                "summary provider identity is missing or does not match".into(),
            ));
        }
        self.spawn_summary(id, snap.operation.session_id());
        Ok(())
    }

    fn spawn_summary(&mut self, id: Uuid, conversation: Uuid) {
        let cancel = self
            .harness
            .owner
            .conversation_controls()
            .get(conversation)
            .cancel
            .child_token();
        let owner = self.harness.owner.clone();
        let model = self.harness.options.model.clone();
        let worker_cancel = cancel.clone();
        let root = self.cancel.clone();
        let storage_failed = self.harness.storage_failed.clone();
        let mut placement = SummaryPlacementDriver {
            harness: self.harness.worker(),
            summary_provider: self.summary_provider.clone(),
            cancel: self.cancel.clone(),
        };
        let hook_harness = self.harness.worker();
        let provider = self
            .summary_provider
            .clone()
            .expect("summary recovery requires a provider identity");
        let activity = self
            .harness
            .activity
            .enter(crate::worker_activity::WorkerKind::Summary, id);
        let join = tokio::spawn(async move {
            let _activity = activity;
            let result = async {
                crate::summary_worker::run(
                    owner.clone(),
                    model,
                    id,
                    worker_cancel.clone(),
                    provider,
                    Some(&hook_harness),
                )
                .await?;
                if worker_cancel.is_cancelled() {
                    return Err(crate::summary_worker::Error::Interrupted);
                }
                let snap = crate::summary_store::SummaryStore::new(owner.store())
                    .get(id)?
                    .ok_or(crate::summary_store::Error::NotFound)?;
                if snap.operation.auto_place()
                    && matches!(
                        snap.operation.state(),
                        crate::summary_store::State::Completed { .. }
                            | crate::summary_store::State::PlacementPending { .. }
                    )
                {
                    placement.place_summary(id).map_err(|error| match error {
                        Error::Interrupted => crate::summary_worker::Error::Interrupted,
                        other => crate::summary_worker::Error::Placement(other.to_string()),
                    })?;
                }
                Ok(())
            }
            .await;
            let result = match result {
                Err(crate::summary_worker::Error::Store(
                    crate::summary_store::Error::Conflict | crate::summary_store::Error::State,
                )) => {
                    match crate::summary_store::SummaryStore::new(placement.harness.owner.store())
                        .get(id)
                    {
                        Ok(Some(snapshot))
                            if matches!(
                                snapshot.operation.state(),
                                crate::summary_store::State::Aborting { .. }
                                    | crate::summary_store::State::Aborted
                            ) =>
                        {
                            Ok(())
                        }
                        Ok(_) => Err(crate::summary_worker::Error::Store(
                            crate::summary_store::Error::Conflict,
                        )),
                        Err(error) => Err(crate::summary_worker::Error::Store(error)),
                    }
                }
                other => other,
            };
            if storage_failed.load(Ordering::Acquire)
                || matches!(
                    &result,
                    Err(crate::summary_worker::Error::Store(
                        crate::summary_store::Error::Store(_)
                    ))
                )
            {
                storage_failed.store(true, Ordering::Release);
                root.cancel();
            }
            result
        });
        self.summary_tasks.insert(id, SummaryTask { cancel, join });
    }

    #[allow(dead_code)]
    async fn wait_summary(&mut self, id: Uuid) -> Result<(), Error> {
        let Some(task) = self.summary_tasks.get_mut(&id) else {
            return Err(Error::NotFound);
        };
        let result = (&mut task.join).await;
        self.summary_tasks.remove(&id);
        result
            .map_err(|e| Error::Invalid(format!("summary task: {e}")))?
            .map_err(|e| Error::Invalid(e.to_string()))
    }

    #[allow(dead_code)]
    async fn abort_summary(&mut self, id: Uuid) -> Result<(), Error> {
        use crate::summary_store::{Error as StoreError, State, SummaryStore};
        if self.harness.storage_failed.load(Ordering::Acquire) {
            return Err(Error::Failed);
        }
        let provider = self
            .summary_provider
            .clone()
            .ok_or_else(|| Error::Invalid("summary provider identity is not configured".into()))?;
        let store = SummaryStore::new(self.harness.owner.store());
        let mut marked = false;
        for _ in 0..8 {
            let snap = store
                .get(id)
                .map_err(|e| self.summary_store_error(e))?
                .ok_or(Error::NotFound)?;
            if snap.operation.provider() != Some(&provider) {
                return Err(Error::Invalid(
                    "summary provider identity is missing or does not match".into(),
                ));
            }
            match snap.operation.state() {
                State::Aborting { .. } | State::Aborted => {
                    marked = true;
                    break;
                }
                State::Placed { .. } | State::Stale { .. } | State::Declined => {
                    return Err(Error::Invalid("summary already completed".into()));
                }
                _ => match store.begin_abort(id, snap.version) {
                    Ok(()) => {
                        marked = true;
                        break;
                    }
                    Err(StoreError::Conflict) => continue,
                    Err(e) => return Err(self.summary_store_error(e)),
                },
            }
        }
        if !marked {
            return Err(Error::Invalid("summary abort conflicted".into()));
        }
        let mut worker_error = None;
        if let Some(task) = self.summary_tasks.get_mut(&id) {
            task.cancel.cancel();
            match (&mut task.join).await {
                Ok(Ok(()))
                | Ok(Err(crate::summary_worker::Error::Interrupted))
                | Ok(Err(crate::summary_worker::Error::Store(
                    StoreError::Conflict | StoreError::State,
                ))) => (),
                Ok(Err(e)) => worker_error = Some(e.to_string()),
                Err(e) => worker_error = Some(format!("summary task: {e}")),
            }
            self.summary_tasks.remove(&id);
        }
        if self.harness.storage_failed.load(Ordering::Acquire) {
            return Err(Error::Failed);
        }
        // The prior task has joined before recovery takes the same fence.
        crate::summary_worker::run(
            self.harness.owner.clone(),
            self.harness.options.model.clone(),
            id,
            self.cancel.child_token(),
            provider,
            None,
        )
        .await
        .map_err(|e| match e {
            crate::summary_worker::Error::Store(error) => self.summary_store_error(error),
            other => Error::Invalid(other.to_string()),
        })?;
        if let Some(error) = worker_error {
            return Err(Error::Invalid(error));
        }
        Ok(())
    }

    fn summary_store_error(&self, error: crate::summary_store::Error) -> Error {
        if matches!(error, crate::summary_store::Error::Store(_)) {
            self.harness.storage_failed.store(true, Ordering::Release);
            self.cancel.cancel();
        }
        Error::Invalid(format!("summary: {error}"))
    }
    /// Most recent background reporter failure, if any. Reopen after storage errors.
    pub fn reporter_error(&self) -> Option<String> {
        self.reporter_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    fn start_reporter(&mut self) {
        let source = self.harness.worker();
        let cancel = self.reporter_cancel.clone();
        let errors = self.reporter_error.clone();
        let worker_errors = self.worker_errors.clone();
        self.reporter = Some(tokio::spawn(async move {
            let mut active: HashMap<Vec<u8>, tokio::task::JoinHandle<Result<(), Error>>> =
                HashMap::new();
            loop {
                let done: Vec<_> = active
                    .iter()
                    .filter(|(_, task)| task.is_finished())
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in done {
                    if let Some(task) = active.remove(&key) {
                        match task.await {
                            Ok(Ok(())) => (),
                            Ok(Err(Error::Interrupted)) if cancel.is_cancelled() => (),
                            Ok(Err(e)) => {
                                *errors.lock().unwrap_or_else(|e| e.into_inner()) =
                                    Some(e.to_string())
                            }
                            Err(e) => {
                                *errors.lock().unwrap_or_else(|e| e.into_inner()) =
                                    Some(e.to_string())
                            }
                        }
                    }
                }
                if cancel.is_cancelled() {
                    break;
                }
                match source.pending_reporters() {
                    Ok(pending) => {
                        for (key, reporter) in pending {
                            if active.contains_key(&key) {
                                continue;
                            }
                            let mut worker = source.worker();
                            let task_key = key.clone();
                            let worker_errors = worker_errors.clone();
                            let reporter_stop = cancel.clone();
                            active.insert(
                                key,
                                tokio::spawn(async move {
                                    let Some(child) = worker.reporter_child(&reporter)? else {
                                        return Ok(());
                                    };
                                    let activity = worker.activity.enter_at(
                                        crate::worker_activity::WorkerKind::Conversation,
                                        child,
                                        crate::worker_activity::WorkerPhase::Queued,
                                    );
                                    let lock =
                                        worker.owner.conversation_controls().get(child).worker;
                                    {
                                        let mut guard = Some(tokio::select! {
                                            biased;
                                            _ = reporter_stop.cancelled() => return Ok(()),
                                            guard = lock.lock() => guard,
                                        });
                                        let control =
                                            worker.owner.conversation_controls().get(child);
                                        worker.execution_gate = Some(control.gate);
                                        drive_scheduled(
                                            &mut worker,
                                            child,
                                            &control.cancel,
                                            &lock,
                                            &mut guard,
                                            Some(&worker_errors),
                                            activity,
                                        )
                                        .await?;
                                    }
                                    if let Some(parent) =
                                        worker.deliver_report(&task_key, &reporter)?
                                    {
                                        let activity = worker.activity.enter_at(
                                            crate::worker_activity::WorkerKind::Conversation,
                                            parent,
                                            crate::worker_activity::WorkerPhase::Queued,
                                        );
                                        let lock =
                                            worker.owner.conversation_controls().get(parent).worker;
                                        let mut guard = Some(tokio::select! {
                                            biased;
                                            _ = reporter_stop.cancelled() => return Ok(()),
                                            guard = lock.lock() => guard,
                                        });
                                        let control =
                                            worker.owner.conversation_controls().get(parent);
                                        worker.execution_gate = Some(control.gate);
                                        drive_scheduled(
                                            &mut worker,
                                            parent,
                                            &control.cancel,
                                            &lock,
                                            &mut guard,
                                            Some(&worker_errors),
                                            activity,
                                        )
                                        .await?;
                                    }
                                    Ok(())
                                }),
                            );
                        }
                    }
                    Err(e) => {
                        *errors.lock().unwrap_or_else(|e| e.into_inner()) = Some(e.to_string())
                    }
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                }
            }
            for (_, task) in active {
                let _ = task.await;
            }
        }));
    }
    /// Wait for the most recently scheduled execution of a conversation.
    pub async fn wait(&mut self, id: Uuid) -> Result<(), Error> {
        if let Some(tasks) = self.tasks.get_mut(&id) {
            while !tasks.is_empty() {
                let result = (&mut tasks[0])
                    .await
                    .map_err(|e| Error::Invalid(format!("scheduler task failed: {e}")))?;
                tasks.remove(0);
                result?;
            }
        }
        self.tasks.remove(&id);
        Ok(())
    }
    /// Stop workers at cancellation points and wait for them before returning.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.cancel.cancel();
        self.reporter_cancel.cancel();
        let mut error = None;
        for task in std::mem::take(&mut self.summary_tasks).into_values() {
            match task.join.await {
                Ok(Ok(())) | Ok(Err(crate::summary_worker::Error::Interrupted)) => (),
                Ok(Err(e)) => {
                    error.get_or_insert(Error::Invalid(e.to_string()));
                }
                Err(e) => {
                    error.get_or_insert(Error::Invalid(format!("summary task: {e}")));
                }
            }
        }
        if let Some(reporter) = self.reporter.take()
            && let Err(e) = reporter.await
        {
            error = Some(Error::Invalid(format!("reporter task failed: {e}")));
        }
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks.into_values().flatten() {
            match task.await {
                Ok(Ok(())) | Ok(Err(Error::Interrupted)) => (),
                Ok(Err(e)) => {
                    error.get_or_insert(e);
                }
                Err(e) => {
                    error.get_or_insert(Error::Invalid(format!("scheduler task failed: {e}")));
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}

/// Owns the conversation scheduler and generic task driver together.
///
/// Open inside a Tokio runtime. Extensions and task handlers must be registered
/// before opening so recovered work sees them. Configured constructors also
/// install provider adapters and parent-bound management before recovery. Dropping without
/// awaiting `shutdown` requests driver stop but does not wait for either worker.
/// A synchronous task handler must eventually return for shutdown to finish.
pub struct RuntimeOwner {
    // Dropped first, before the scheduler and driver when the owner is dropped.
    wake_subscriptions: WakeSubscriptions,
    scheduler: Scheduler,
    driver: crate::task_driver::Driver,
}

/// A stable observation of local workers and persisted task and conversation
/// readiness. Unknown conversations need further recovery or inspection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeWakeSnapshot {
    pub active: bool,
    pub at: Option<i64>,
    pub unknown_conversations: Vec<Uuid>,
}

/// Errors from combined wake observation or the host callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeSubscriptionError {
    Snapshot(String),
    Delivery(crate::wake_delivery::DeliveryError),
}

struct WakeSubscriptionInner {
    delivery: crate::wake_delivery::WakeDelivery,
    cancel: CancellationToken,
    kick: tokio::sync::Notify,
}
impl WakeSubscriptionInner {
    fn shutdown(&self) {
        self.cancel.cancel();
        self.delivery.shutdown();
    }
}

/// Owns a combined wake subscription. `kick` recomputes before attempting
/// delivery, so it can retry a failed callback or recover from a snapshot error.
/// Callbacks must not block; they may call `kick` or `shutdown` reentrantly.
/// Shutdown is nonblocking and a callback already claimed may still run.
pub struct WakeSubscription {
    inner: Arc<WakeSubscriptionInner>,
}
impl WakeSubscription {
    pub fn kick(&self) {
        if !self.inner.cancel.is_cancelled() {
            self.inner.kick.notify_one();
        }
    }
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}
impl Drop for WakeSubscription {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Default)]
struct WakeSubscriptions(std::sync::Mutex<Vec<std::sync::Weak<WakeSubscriptionInner>>>);
impl Drop for WakeSubscriptions {
    fn drop(&mut self) {
        for subscription in self
            .0
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            if let Some(subscription) = subscription.upgrade() {
                subscription.shutdown();
            }
        }
    }
}

fn conservative_wake(snapshot: RuntimeWakeSnapshot) -> crate::wake_delivery::Wake {
    crate::wake_delivery::Wake {
        active: snapshot.active || !snapshot.unknown_conversations.is_empty(),
        at: snapshot.at,
    }
}

async fn watch_wake(
    mut changes: SchedulerChanges,
    owner: std::sync::Weak<durable_store::owner::OwnedStore>,
    driver: std::sync::Weak<crate::task_driver::Shared>,
    activity: crate::worker_activity::WorkerActivity,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    on_error: Arc<dyn Fn(WakeSubscriptionError) + Send + Sync>,
    inner: Arc<WakeSubscriptionInner>,
) {
    loop {
        tokio::select! {
            biased;
            _ = inner.cancel.cancelled() => break,
            _ = inner.kick.notified() => {},
            change = changes.changed() => if change.is_none() { break },
        }
        if inner.cancel.is_cancelled() {
            break;
        }
        let (Some(owner), Some(driver)) = (owner.upgrade(), driver.upgrade()) else {
            break;
        };
        let snapshot = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime_wake_snapshot(&owner, &activity, &driver, clock())
        }))
        .unwrap_or_else(|_| Err(Error::Invalid("wake snapshot or clock panicked".into())));
        match snapshot {
            Ok(snapshot) => inner.delivery.publish(conservative_wake(snapshot)),
            Err(error) => {
                // A failed read cannot justify hibernation. Offer a conservative
                // wake and let a later change or kick reattempt the snapshot.
                inner.delivery.publish(crate::wake_delivery::Wake {
                    active: true,
                    at: None,
                });
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    on_error(WakeSubscriptionError::Snapshot(error.to_string()))
                }));
            }
        }
    }
    inner.shutdown();
}

fn runtime_wake_snapshot(
    owner: &durable_store::owner::OwnedStore,
    activity: &crate::worker_activity::WorkerActivity,
    driver: &crate::task_driver::Shared,
    now_millis: i64,
) -> Result<RuntimeWakeSnapshot, Error> {
    let store = owner.store();
    for _ in 0..16 {
        let (revision, workers) = activity.snapshot_with_revision();
        let sequence = store.commit_sequence()?;
        let snapshot = (|| {
            let task = driver
                .wake_snapshot(now_millis)
                .map_err(|e| Error::Invalid(format!("task wake: {e}")))?;
            let mut result = RuntimeWakeSnapshot {
                active: task.active
                    || workers
                        .iter()
                        .any(|worker| worker.phase != crate::worker_activity::WorkerPhase::Waiting),
                at: task.at,
                unknown_conversations: Vec::new(),
            };
            for (key, entry) in store.list(b"runtime/conversation/")? {
                let conversation: Conversation = serde_json::from_slice(&entry.value)?;
                if key != Harness::key(conversation.id) {
                    return Err(Error::Invalid("conversation key mismatch".into()));
                }
                match conversation_wake(store, conversation.id, now_millis)? {
                    ConversationWake::Ready => result.active = true,
                    ConversationWake::Parked { at: Some(at) } => {
                        result.at = Some(result.at.map_or(at, |old| old.min(at)));
                    }
                    ConversationWake::Parked { at: None } => (),
                    ConversationWake::Unknown => {
                        result.unknown_conversations.push(conversation.id);
                    }
                }
            }
            result.unknown_conversations.sort_unstable();
            Ok(result)
        })();
        let end_sequence = store.commit_sequence()?;
        let (end_revision, _) = activity.snapshot_with_revision();
        if sequence == end_sequence && revision == end_revision {
            return snapshot;
        }
    }
    Err(Error::Invalid("wake snapshot changed repeatedly".into()))
}

impl RuntimeOwner {
    pub fn open(
        path: impl AsRef<Path>,
        options: Options,
        extensions: crate::extension::Registry,
        tasks: crate::task_scheduler::Registry,
    ) -> Result<Self, Error> {
        Self::open_configured(path, options, extensions, tasks, None, None)
    }

    /// Install the provider adapter before recovering conversations and tasks.
    pub fn open_with_provider_adapter(
        path: impl AsRef<Path>,
        options: Options,
        extensions: crate::extension::Registry,
        tasks: crate::task_scheduler::Registry,
        adapter: Arc<dyn crate::model::ProviderAdapter>,
    ) -> Result<Self, Error> {
        Self::open_configured(path, options, extensions, tasks, None, Some(adapter))
    }

    /// Install parent-bound management with the default child tool allowlist.
    pub fn open_with_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        extensions: crate::extension::Registry,
        tasks: crate::task_scheduler::Registry,
        parent: Uuid,
        child_config: Config,
    ) -> Result<Self, Error> {
        let allowed = default_subagent_tools(&options);
        Self::open_configured(
            path,
            options,
            extensions,
            tasks,
            Some((parent, child_config, Some(allowed))),
            None,
        )
    }

    /// Install parent-bound management with an explicit child tool policy.
    pub fn open_with_restricted_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        extensions: crate::extension::Registry,
        tasks: crate::task_scheduler::Registry,
        parent: Uuid,
        child_config: Config,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        validate_allowed_tools(&allowed_tools)?;
        Self::open_configured(
            path,
            options,
            extensions,
            tasks,
            Some((parent, child_config, allowed_tools)),
            None,
        )
    }

    /// Install the adapter and parent-bound management with an explicit child tool policy.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_provider_and_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        extensions: crate::extension::Registry,
        tasks: crate::task_scheduler::Registry,
        adapter: Arc<dyn crate::model::ProviderAdapter>,
        parent: Uuid,
        child_config: Config,
        allowed_tools: Option<Vec<String>>,
    ) -> Result<Self, Error> {
        validate_allowed_tools(&allowed_tools)?;
        Self::open_configured(
            path,
            options,
            extensions,
            tasks,
            Some((parent, child_config, allowed_tools)),
            Some(adapter),
        )
    }

    fn open_configured(
        path: impl AsRef<Path>,
        options: Options,
        extensions: crate::extension::Registry,
        tasks: crate::task_scheduler::Registry,
        management: Option<(Uuid, Config, Option<Vec<String>>)>,
        adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
    ) -> Result<Self, Error> {
        let (scheduler, driver) = Scheduler::open_configured_with_tasks(
            path,
            options,
            extensions,
            management,
            adapter,
            Some(tasks),
            None,
        )?;
        Ok(Self {
            scheduler,
            driver: driver.expect("task registry supplied"),
            wake_subscriptions: WakeSubscriptions::default(),
        })
    }

    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }
    pub fn scheduler_mut(&mut self) -> &mut Scheduler {
        &mut self.scheduler
    }
    pub fn tasks(&self) -> &crate::task_driver::Driver {
        &self.driver
    }

    /// Inspect readiness without running user code. A stable local commit
    /// sequence and worker revision fence the individual reads. Unknown
    /// conversation states must not be interpreted as idle; this is not yet
    /// the upstream `onWake` callback.
    pub fn wake_snapshot(&self, now_millis: i64) -> Result<RuntimeWakeSnapshot, Error> {
        let driver = self
            .driver
            .weak()
            .upgrade()
            .ok_or_else(|| Error::Invalid("task driver stopped".into()))?;
        runtime_wake_snapshot(
            &self.scheduler.harness.owner,
            &self.scheduler.worker_activity(),
            &driver,
            now_millis,
        )
    }

    /// Subscribe to combined wake delivery. The clock returns Unix milliseconds.
    /// Subscribes before the initial snapshot, which must succeed. Later snapshot
    /// errors are reported and conservatively publish an active wake; unknown
    /// conversations also force active. Changes coalesce and the delivered value
    /// can be stale by the time the callback runs. Callbacks can run before this
    /// method returns on a multithreaded runtime and must not block.
    ///
    /// Keep the Tokio runtime alive. The host is responsible for arming a timer
    /// for `at` and kicking or causing a change when it fires. Changes through
    /// other SQLite connections do not notify this subscription. Dropping the
    /// returned handle or this owner stops it; an already claimed callback can
    /// still run after shutdown.
    pub fn subscribe_wake(
        &self,
        clock: impl Fn() -> i64 + Send + Sync + 'static,
        callback: impl Fn(crate::wake_delivery::Wake) -> Result<(), String> + Send + Sync + 'static,
        on_error: impl Fn(WakeSubscriptionError) + Send + Sync + 'static,
    ) -> Result<WakeSubscription, Error> {
        let changes = self.scheduler.subscribe_changes()?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|e| {
            Error::Invalid(format!("wake subscription requires a Tokio runtime: {e}"))
        })?;
        let clock = Arc::new(clock);
        let initial =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.wake_snapshot(clock())))
                .map_err(|_| Error::Invalid("wake snapshot or clock panicked".into()))??;
        let on_error = Arc::new(on_error);
        let callback_error = on_error.clone();
        let delivery = crate::wake_delivery::WakeDelivery::new(
            runtime.clone(),
            conservative_wake(initial),
            callback,
            move |error| callback_error(WakeSubscriptionError::Delivery(error)),
        );
        let inner = Arc::new(WakeSubscriptionInner {
            delivery,
            cancel: CancellationToken::new(),
            kick: tokio::sync::Notify::new(),
        });
        {
            let mut subscriptions = self.wake_subscriptions.0.lock().unwrap();
            subscriptions.retain(|entry| entry.strong_count() != 0);
            subscriptions.push(Arc::downgrade(&inner));
        }
        let watcher = inner.clone();
        let owner = Arc::downgrade(&self.scheduler.harness.owner);
        let driver = self.driver.weak();
        let activity = self.scheduler.worker_activity();
        runtime.spawn(async move {
            watch_wake(changes, owner, driver, activity, clock, on_error, watcher).await;
        });
        Ok(WakeSubscription { inner })
    }

    /// Cancel and join conversations while their task driver remains available,
    /// then stop and join the driver even if conversation shutdown failed.
    /// Await this future to completion for deterministic release of ownership.
    pub async fn shutdown(self) -> Result<(), Error> {
        let Self {
            scheduler,
            driver,
            wake_subscriptions,
        } = self;
        drop(wake_subscriptions);
        let conversations = scheduler.shutdown().await;
        let tasks = tokio::task::spawn_blocking(move || driver.shutdown()).await;
        match (conversations, tasks) {
            (Ok(()), Ok(Ok(()))) => Ok(()),
            (conversations, tasks) => Err(Error::Invalid(format!(
                "shutdown failed: conversations: {}; tasks: {}",
                conversations
                    .err()
                    .map_or_else(|| "ok".into(), |e| e.to_string()),
                match tasks {
                    Ok(Ok(())) => "ok".into(),
                    Ok(Err(e)) => e.to_string(),
                    Err(e) => e.to_string(),
                }
            ))),
        }
    }
}

/// Drive a conversation under its scheduler lock and cancellation fence.
pub(crate) async fn drive_scheduled<'a>(
    worker: &mut Harness,
    id: Uuid,
    cancel: &CancellationToken,
    lock: &'a tokio::sync::Mutex<()>,
    guard: &mut Option<tokio::sync::MutexGuard<'a, ()>>,
    errors: Option<&std::sync::Mutex<HashMap<Uuid, String>>>,
    activity: crate::worker_activity::Guard,
) -> Result<(), Error> {
    activity.set(crate::worker_activity::WorkerPhase::Running);
    let next_poll = worker.owner.conversation_controls().get(id).next_poll;
    if cancel.is_cancelled() {
        return Ok(());
    }
    // Only an owner that has actually acquired the worker lock supersedes a
    // previously reported failure. Queued and cancelled retries do not.
    if let Some(errors) = errors {
        errors.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
    }
    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        // Another owner may have failed while this worker was parked. Apply
        // the same recovery policy as a newly scheduled worker before driving.
        if let Err(error) = worker.recover_conversation(id) {
            let _gate = worker
                .execution_gate
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| Error::Failed)?;
            return if cancel.is_cancelled() {
                Ok(())
            } else {
                Err(error)
            };
        }
        if let Err(error) = worker.drive(id, cancel).await {
            let _gate = worker
                .execution_gate
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| Error::Failed)?;
            return if cancel.is_cancelled() {
                Ok(())
            } else {
                Err(error)
            };
        }
        let state = worker.conversation(id)?;
        if state.nested_suspended() && !state.abort_requested {
            // No parent dispatch remains alive after the suspension commit.
            // Never wait for the nested poll lock while holding the worker:
            // another driver may be finishing a poll and need this lock.
            drop(guard.take());
            let retry = poll_suspended_nested(worker, id, cancel).await?;
            if !retry || cancel.is_cancelled() {
                return Ok(());
            }
            activity.set(crate::worker_activity::WorkerPhase::Waiting);
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_secs(1)) => (),
            }
            *guard = Some(tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                guard = lock.lock() => guard,
            });
            activity.set(crate::worker_activity::WorkerPhase::Running);
            continue;
        }
        let should_poll = if state.abort_requested && !matches!(state.phase, Phase::Blocked { .. })
        {
            true
        } else {
            match &state.phase {
                Phase::Pending { calls, index, .. } => calls
                    .get(*index)
                    .map(|call| {
                        worker.resolved_tools(&state).map(|tools| {
                            tools
                                .get(&call.name)
                                .is_some_and(|tool| tool.tool.can_poll())
                        })
                    })
                    .transpose()?
                    .unwrap_or(false),
                Phase::TaskPending { .. }
                | Phase::RetryWait { .. }
                | Phase::ToolGateSleeping { .. }
                | Phase::HookTasksWaiting { .. }
                | Phase::YieldTasksWaiting { .. }
                | Phase::YieldDecision { .. }
                | Phase::OnYield { .. }
                | Phase::AfterTools { .. }
                | Phase::AfterRun { .. } => true,
                _ => false,
            }
        };
        if !should_poll {
            return Ok(());
        }
        // Every successful drive moves the shared periodic deadline. Explicit
        // scheduling still drives immediately, while existing parked drivers
        // share one poll per interval instead of each starting its own timer.
        let now = tokio::time::Instant::now();
        let interval = match state.phase {
            Phase::RetryWait { .. } => Duration::from_millis(
                state
                    .retry_state
                    .deadline_ms
                    .ok_or_else(|| Error::Invalid("retry deadline missing".into()))?
                    .saturating_sub(unix_ms()),
            ),
            _ => Duration::from_secs(1),
        };
        let deadline = now
            .checked_add(interval)
            .unwrap_or_else(|| now + Duration::from_secs(1));
        *next_poll.lock().unwrap_or_else(|e| e.into_inner()) = Some(deadline);
        loop {
            let deadline = next_poll
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .expect("deadline set while holding worker lock");
            // No dispatch future remains live at this persisted wait boundary.
            drop(guard.take());
            activity.set(crate::worker_activity::WorkerPhase::Waiting);
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep_until(deadline) => (),
            }
            *guard = Some(tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                guard = lock.lock() => guard,
            });
            activity.set(crate::worker_activity::WorkerPhase::Running);
            if cancel.is_cancelled() {
                return Ok(());
            }
            let deadline = next_poll
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .expect("deadline set while holding worker lock");
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        // This driver has taken over from any owner that failed while it was
        // parked. Publish that takeover before attempting recovery, just as a
        // newly scheduled driver does after acquiring the worker lock.
        if let Some(errors) = errors {
            errors.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
        }
    }
}

/// Poll accepted nested jobs without holding the conversation worker mutex.
/// The per-conversation poll mutex avoids overlapping poll rounds; the abort
/// gate fences every poll and every version-checked result commit.
async fn poll_suspended_nested(
    worker: &Harness,
    id: Uuid,
    cancel: &CancellationToken,
) -> Result<bool, Error> {
    let control = worker.owner.conversation_controls().get(id);
    let _poll = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Ok(false),
        guard = control.nested_poll.lock() => guard,
    };
    let (state, version) = worker.load(id)?;
    if state.abort_requested || !state.nested_suspended() {
        return Ok(false);
    }
    let Phase::Blocked { operation_id, .. } = &state.phase else {
        return Ok(false);
    };
    match fenced_poll(
        async {
            crate::nested::wake_before_gates(
                worker.owner.store(),
                *operation_id,
                id,
                Harness::key(id),
                version,
                (worker.clock)(),
            )
        },
        worker.execution_gate.as_ref(),
        cancel,
    )
    .await
    {
        Some(Ok(true)) | Some(Err(Error::Storage(durable_store::Error::Conflict { .. }))) => {
            return Ok(true);
        }
        Some(Ok(false)) => (),
        Some(Err(error)) => return Err(error),
        None => return Ok(false),
    }
    if crate::nested::has_staged_results(worker.owner.store(), *operation_id)? {
        if state.extensions.is_empty() {
            return Ok(false);
        }
        let host = crate::nested::NestedHost::new_with_schemas(
            worker.owner.clone(),
            *operation_id,
            worker
                .resolved_tools(&state)?
                .into_iter()
                .filter(|(name, reg)| {
                    caller_allowed(&state, name, false) && !crate::tool_search::is_search(&reg.tool)
                })
                .map(|(_, reg)| reg)
                .collect(),
            cancel.child_token(),
            Duration::from_secs(120),
            8,
            (
                state.tool_filters.output_schemas.clone(),
                Some(crate::nested::NestedHooks {
                    selected: worker.selected_extensions(&state)?,
                    snapshot: serde_json::to_value(&state)?,
                    conversation: id,
                    storage_failed: worker.storage_failed.clone(),
                    conversation_fence: Some((Harness::key(id), version)),
                }),
            ),
        )
        .map_err(|error| Error::Invalid(format!("nested hook host: {error}")))?;
        return match fenced_poll(
            host.finish_staged_results(),
            worker.execution_gate.as_ref(),
            cancel,
        )
        .await
        {
            Some(Ok(())) => Ok(true),
            None => Ok(false),
            Some(Err(error)) => {
                let (_, current) = worker.load(id)?;
                if current != version {
                    Ok(true)
                } else {
                    Err(Error::Invalid(error))
                }
            }
        };
    }
    let jobs = crate::nested::pending_polls(worker.owner.store(), *operation_id)?;
    if jobs.is_empty() {
        return worker.prepare_nested_resume(&mut state.clone());
    }
    let tools = worker.resolved_tools(&state)?;
    let mut polls = Vec::new();
    for job in jobs {
        let Some(reg) = tools.get(job.name()) else {
            continue;
        };
        if !caller_allowed(&state, job.name(), false)
            || reg.replay != job.policy()
            || !reg.tool.can_poll()
        {
            continue;
        }
        let tool = reg.tool.clone();
        let gate = worker.execution_gate.as_ref();
        let schemas = &state.tool_filters.output_schemas;
        let owner = &worker.owner;
        polls.push(async move {
            let polled = fenced_poll(
                std::panic::AssertUnwindSafe(crate::poll::poll_once(
                    tool.as_ref(), job.operation(), job.reference(), cancel, Duration::from_secs(120),
                )).catch_unwind(),
                gate,
                cancel,
            ).await;
            let outcome = match polled {
                Some(Ok(crate::poll::PollResult::Outcome(Outcome::Unknown(_)))) => return Ok(false),
                Some(Ok(crate::poll::PollResult::Outcome(outcome))) => outcome,
                Some(Err(panic)) => { crate::extension::discard_panic_payload(panic); return Ok(false); },
                _ => return Ok(false),
            };
            match fenced_poll(async move {
                job.commit(owner.store(), Harness::key(id), version, outcome, schemas)
            }, gate, cancel).await {
                Some(Ok(())) | Some(Err(Error::Storage(durable_store::Error::Conflict { .. }))) => Ok(true),
                Some(Err(error)) => Err(error),
                None => Ok(false),
            }
        });
    }
    let joined = futures_util::future::join_all(polls);
    tokio::pin!(joined);
    let mut gates_woken = false;
    let mut gate_scan = true;
    let mut gate_failure = None;
    loop {
        tokio::select! {
            results = &mut joined => {
                if let Some(error) = gate_failure { return Err(error); }
                let mut retry = gates_woken;
                for result in results { retry |= result?; }
                return Ok(retry);
            }
            _ = cancel.cancelled() => return Ok(gates_woken),
            _ = tokio::time::sleep(Duration::from_millis(100)), if gate_scan => {
                match fenced_poll(async {
                    crate::nested::wake_before_gates(worker.owner.store(), *operation_id, id, Harness::key(id), version, (worker.clock)())
                }, worker.execution_gate.as_ref(), cancel).await {
                    Some(Ok(true)) => gates_woken = true,
                    Some(Ok(false)) => (),
                    Some(Err(Error::Storage(durable_store::Error::Conflict { .. }))) => { gates_woken = true; gate_scan = false; },
                    Some(Err(error)) => { gate_failure = Some(error); gate_scan = false; },
                    None => return Ok(gates_woken),
                }
            }
        }
    }
}

// Serialize polling of effectful futures with the durable abort commit. Once
// the commit wins, no subsequent poll can initiate or resume dispatch.
async fn fenced_poll<F: std::future::Future>(
    future: F,
    gate: Option<&Arc<std::sync::Mutex<()>>>,
    cancel: &CancellationToken,
) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    tokio::select! {
        _ = cancel.cancelled() => None,
        result = std::future::poll_fn(|cx| {
            let _guard = gate.map(|gate| gate.lock().expect("conversation gate poisoned"));
            if cancel.is_cancelled() {
                return std::task::Poll::Ready(None);
            }
            // Drop the fence before propagating a panic: an effectful future
            // may panic while polled, but that must not poison the shared gate
            // and prevent a later owner from recovering the conversation.
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)));
            drop(_guard);
            match result {
                Ok(result) => result.map(Some),
                Err(payload) => std::panic::resume_unwind(payload),
            }
        }) => result,
    }
}

pub(crate) fn wall_time_millis() -> i64 {
    unix_ms().try_into().unwrap_or(i64::MAX)
}

pub(crate) fn signal_target_closed(
    bytes: &[u8],
    name: &str,
    now: i64,
) -> Result<bool, crate::conversation_signal::Error> {
    let c: serde_json::Value = serde_json::from_slice(bytes)?;
    let sleeping = c.get("phase").and_then(|p| p.get("ToolGateSleeping"));
    Ok(c.get("abort_requested")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || sleeping.is_some_and(|s| {
            s.get("until_millis")
                .and_then(|v| v.as_i64())
                .is_some_and(|d| now >= d)
                && s.get("signals")
                    .and_then(|v| v.as_array())
                    .is_some_and(|a| a.iter().any(|v| v.as_str() == Some(name)))
        }))
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn project_compaction_blocks(
    blocks: &std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks>,
    system_end: usize,
    first_kept: usize,
) -> Result<std::collections::BTreeMap<usize, crate::transcript::MessageContentBlocks>, Error> {
    if system_end > first_kept {
        return Err(crate::transcript::Error::Corrupt.into());
    }
    Ok(blocks
        .iter()
        .filter_map(|(&index, binding)| {
            if index < system_end {
                Some((index, binding.clone()))
            } else if index >= first_kept {
                Some((system_end + 1 + index - first_kept, binding.clone()))
            } else {
                None
            }
        })
        .collect())
}

fn start_overflow_compaction(
    harness: &Harness,
    c: &mut Conversation,
    turns: u32,
    reason: String,
) -> Result<(), Error> {
    if c.overflow_compaction_attempted {
        finish_failure(c, reason, FailureCause::ModelError);
        return Ok(());
    }
    c.overflow_compaction_attempted = true;
    c.auto_compaction_attempted = true;
    if let Some(preparation) = harness.prepare_runtime_compaction(c, 0, None)? {
        let generation = *c.context_generation.get_or_insert_with(Uuid::new_v4);
        c.compaction_return_turns = Some(turns);
        c.phase = Phase::CompactionHook {
            preparation: Box::new(preparation.bind_context(generation)),
            operation_id: Uuid::new_v4(),
            reason: crate::extension::CompactionReason::Overflow,
        };
    } else {
        finish_failure(c, reason, FailureCause::ModelError);
    }
    Ok(())
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        // The poller is otherwise perpetual and would keep the owner alive.
        self.reporter_cancel.cancel();
    }
}

// Prove the issued round and every completed result before selecting an active
// attempt. Unknown/blocked outcomes and legacy attempts stay fenced.
fn summary_tool_phase_eligible(state: &Conversation) -> bool {
    if state.active.is_none() || state.abort_requested {
        return false;
    }
    let (calls, index, operation) = match &state.phase {
        Phase::ToolRunning {
            calls,
            index,
            operation_id,
            replay_ready: false,
            dispatch_identity: Some(_),
            ..
        } => (calls, *index, *operation_id),
        Phase::Pending {
            calls,
            index,
            operation_id,
            reference,
            arguments: Some(_),
            ..
        } if !reference.is_empty() => (calls, *index, *operation_id),
        Phase::TaskPending {
            calls,
            index,
            operation_id,
            task,
            identity: Some(_),
            ..
        } if task == operation_id => (calls, *index, *operation_id),
        _ => return false,
    };
    let Some(start) = state.messages.len().checked_sub(index.saturating_add(1)) else {
        return false;
    };
    !operation.is_nil()
        && index < calls.len()
        && state
            .tool_attempt()
            .is_some_and(|attempt| !attempt.attempt_id.is_nil())
        && matches!(&state.messages[start], Message::Assistant { calls: issued, .. } if issued == calls)
        && calls[..index].iter().zip(&state.messages[start + 1..]).all(
            |(call, message)| matches!(message, Message::Tool { call_id, .. } if call_id == &call.id),
        )
}

// Prove that model progress has retained the selected ownership and inbox
// identities. The caller additionally checks the source prefix, block bindings
// and context generation, and fences the observed conversation version.
fn summary_progress_only(base: &Conversation, latest: &Conversation) -> Result<bool, Error> {
    if base.id != latest.id
        || base.context_generation != latest.context_generation
        || base.resets != latest.resets
        || base.fitted_reset_receipts != latest.fitted_reset_receipts
        || latest.submissions.len() < base.submissions.len()
        || latest.application_writes.len() < base.application_writes.len()
        || serde_json::to_value((
            &base.config,
            &base.extensions,
            &base.provider_adapter,
            &base.allowed_tools,
            &base.model_tools,
            &base.tool_filters,
        ))? != serde_json::to_value((
            &latest.config,
            &latest.extensions,
            &latest.provider_adapter,
            &latest.allowed_tools,
            &latest.model_tools,
            &latest.tool_filters,
        ))?
    {
        return Ok(false);
    }
    for (old, new) in base.submissions.iter().zip(&latest.submissions) {
        if old.id != new.id
            || old.kind != new.kind
            || old.request_id != new.request_id
            || old.content != new.content
            || old.image_notes != new.image_notes
            || old.input_blocks != new.input_blocks
            || base.input_modes.get(&old.id) != latest.input_modes.get(&old.id)
        {
            return Ok(false);
        }
    }
    for (old, new) in base
        .application_writes
        .iter()
        .zip(&latest.application_writes)
    {
        if old.id != new.id
            || old.request_id != new.request_id
            || old.kind != new.kind
            || old.data != new.data
            || old.after_reset != new.after_reset
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn admission_only(base: &Conversation, latest: &Conversation) -> Result<bool, Error> {
    let mut old = serde_json::to_value(base)?;
    let mut new = serde_json::to_value(latest)?;
    old.as_object_mut()
        .and_then(|v| v.remove("after_failed_run"));
    new.as_object_mut()
        .and_then(|v| v.remove("after_failed_run"));
    old.as_object_mut().and_then(|v| v.remove("queue_modes"));
    new.as_object_mut().and_then(|v| v.remove("queue_modes"));
    let old_submissions = old
        .as_object_mut()
        .and_then(|v| v.remove("submissions"))
        .ok_or_else(|| Error::Invalid("invalid baseline".into()))?;
    let new_submissions = new
        .as_object_mut()
        .and_then(|v| v.remove("submissions"))
        .ok_or_else(|| Error::Invalid("invalid snapshot".into()))?;
    let old_pending = old
        .as_object_mut()
        .and_then(|v| v.remove("pending_independent_summary"));
    let new_pending = new
        .as_object_mut()
        .and_then(|v| v.remove("pending_independent_summary"));
    let pending_ok = old_pending == new_pending || old_pending == Some(serde_json::Value::Null);
    old.as_object_mut()
        .and_then(|v| v.remove("fitted_reset_receipts"));
    new.as_object_mut()
        .and_then(|v| v.remove("fitted_reset_receipts"));
    let fitted_receipts_unchanged = base
        .fitted_reset_receipts
        .iter()
        .all(|(k, v)| latest.fitted_reset_receipts.get(k) == Some(v))
        && latest.fitted_reset_receipts.iter().all(|(k, v)| {
            base.fitted_reset_receipts.contains_key(k)
                || latest
                    .pending_resets
                    .iter()
                    .skip(base.pending_resets.len())
                    .any(|reset| reset.id == v.reset)
        });
    old.as_object_mut()
        .and_then(|v| v.remove("pending_reset_blocks"));
    new.as_object_mut()
        .and_then(|v| v.remove("pending_reset_blocks"));
    let reset_blocks_unchanged = base
        .pending_reset_blocks
        .iter()
        .all(|(id, blocks)| latest.pending_reset_blocks.get(id) == Some(blocks))
        && latest.pending_reset_blocks.iter().all(|(id, _)| {
            base.pending_reset_blocks.contains_key(id)
                || latest
                    .pending_resets
                    .iter()
                    .skip(base.pending_resets.len())
                    .any(|reset| reset.id == *id)
        });
    let old_resets = old.as_object_mut().and_then(|v| v.remove("pending_resets"));
    let new_resets = new.as_object_mut().and_then(|v| v.remove("pending_resets"));
    let resets_unchanged = match (old_resets, new_resets) {
        (Some(serde_json::Value::Array(old)), Some(serde_json::Value::Array(new))) => {
            new.starts_with(&old)
        }
        _ => false,
    };
    let old_writes = old
        .as_object_mut()
        .and_then(|v| v.remove("application_writes"));
    let new_writes = new
        .as_object_mut()
        .and_then(|v| v.remove("application_writes"));
    let writes_unchanged = match (old_writes, new_writes) {
        (Some(serde_json::Value::Array(old)), Some(serde_json::Value::Array(new))) => {
            new.len() >= old.len()
                && old.iter().zip(&new).all(|(a, b)| {
                    if a == b {
                        return true;
                    }
                    let mut a = a.clone();
                    let mut b = b.clone();
                    let old_withdrawn = a.as_object_mut().and_then(|v| v.remove("withdrawn"));
                    let new_withdrawn = b.as_object_mut().and_then(|v| v.remove("withdrawn"));
                    a == b
                        && old_withdrawn == Some(serde_json::json!(false))
                        && new_withdrawn == Some(serde_json::json!(true))
                        && a.get("placed") == Some(&serde_json::json!(false))
                })
        }
        _ => false,
    };
    let old_modes = old.as_object_mut().and_then(|v| v.remove("input_modes"));
    let new_modes = new.as_object_mut().and_then(|v| v.remove("input_modes"));
    let modes_unchanged = match (old_modes, new_modes) {
        (Some(serde_json::Value::Object(old)), Some(serde_json::Value::Object(new))) => {
            old.iter().all(|(key, value)| new.get(key) == Some(value))
        }
        _ => false,
    };
    let (Some(old_submissions), Some(new_submissions)) =
        (old_submissions.as_array(), new_submissions.as_array())
    else {
        return Err(Error::Invalid("invalid submissions".into()));
    };
    Ok(old == new
        && pending_ok
        && modes_unchanged
        && resets_unchanged
        && reset_blocks_unchanged
        && fitted_receipts_unchanged
        && writes_unchanged
        && new_submissions.len() >= old_submissions.len()
        && old_submissions.iter().zip(new_submissions).all(|(a, b)| {
            if a == b {
                return true;
            }
            let mut a = a.clone();
            let mut b = b.clone();
            let old_status = a.as_object_mut().and_then(|v| v.remove("status"));
            let new_status = b.as_object_mut().and_then(|v| v.remove("status"));
            a == b
                && old_status == Some(serde_json::json!("Queued"))
                && new_status == Some(serde_json::json!({"Unanswered": {"reason": "aborted"}}))
        }))
}

fn place_round_handoff(c: &mut Conversation) {
    for name in c
        .round_added_tools
        .iter()
        .flatten()
        .filter(|name| !name.is_empty())
    {
        c.tool_filters.excluded.retain(|excluded| excluded != name);
        c.tool_filters
            .model_excluded
            .retain(|excluded| excluded != name);
        if c.model_tools.is_none() && !c.tool_filters.model_add.contains(name) {
            c.tool_filters.model_add.push(name.clone());
        }
    }
    if let Some(allowed) = &mut c.allowed_tools {
        for name in c.round_added_tools.iter().flatten() {
            if !name.is_empty() && !allowed.contains(name) {
                allowed.push(name.clone());
            }
        }
    }
    if let Some(allowed) = &mut c.model_tools {
        for name in c.round_added_tools.iter().flatten() {
            if !name.is_empty() && !allowed.contains(name) {
                allowed.push(name.clone());
            }
        }
    }
    c.round_added_tools.clear();
    if let Some(text) = c.round_handoffs.iter().rev().find_map(|text| text.clone()) {
        place_reset(c, crate::reset::Reset::new(Uuid::new_v4(), Some(text)));
    }
    c.round_handoffs.clear();
}

fn validate_pending_reset_blocks(c: &Conversation) -> Result<(), Error> {
    if c.pending_reset_blocks.is_empty() {
        return Ok(());
    }
    let mut reset_ids = std::collections::HashSet::new();
    if c.pending_resets.iter().any(|reset| {
        reset.id.is_nil()
            || !reset_ids.insert(reset.id)
            || c.resets.iter().any(|placed| placed.id == reset.id)
    }) || c
        .pending_reset_blocks
        .keys()
        .any(|id| !reset_ids.contains(id))
    {
        return Err(Error::Invalid(
            "invalid pending reset block ordering".into(),
        ));
    }
    let mut ids: std::collections::HashSet<_> = c
        .content_blocks
        .values()
        .map(|binding| binding.message_id)
        .collect();
    for blocks in c.pending_reset_blocks.values() {
        if blocks.is_empty() {
            return Err(Error::Invalid("empty reset block additions".into()));
        }
        for binding in blocks {
            if binding.message_id.is_nil()
                || !ids.insert(binding.message_id)
                || crate::content::UserContent::from_yield(
                    serde_json::Value::Array(binding.blocks.clone()),
                    false,
                )
                .is_none()
            {
                return Err(Error::Invalid("invalid pending reset block binding".into()));
            }
        }
    }
    Ok(())
}

fn reset_baseline_digest(value: &serde_json::Value) -> Result<String, Error> {
    use sha2::{Digest, Sha256};
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}

// Derive reset cuts from a caller-authenticated baseline without writing storage.
// A future batch proof can compare each returned state to its committed cut.
#[derive(Clone, Copy)]
struct ResetDerivationStep {
    reset_id: Uuid,
    context_generation: Uuid,
    write_limit: usize,
}

fn derive_ordered_reset_states(
    baseline: &Conversation,
    steps: &[ResetDerivationStep],
) -> Result<Vec<Conversation>, Error> {
    if steps.is_empty() {
        return Err(Error::Invalid("empty reset derivation".into()));
    }
    validate_pending_reset_blocks(baseline)?;
    let mut state = baseline.clone();
    let mut generations = std::collections::HashSet::new();
    if let Some(generation) = state.context_generation {
        generations.insert(generation);
    }
    let mut cuts = Vec::with_capacity(steps.len());
    for step in steps {
        if step.context_generation.is_nil()
            || !generations.insert(step.context_generation)
            || state
                .pending_resets
                .first()
                .is_none_or(|reset| reset.id != step.reset_id)
        {
            return Err(Error::Invalid("invalid ordered reset derivation".into()));
        }
        let marker = state.pending_resets.remove(0);
        place_reset_before_writes(&mut state, marker, step.write_limit);
        state.context_generation = Some(step.context_generation);
        validate_pending_reset_blocks(&state)?;
        cuts.push(state.clone());
    }
    Ok(cuts)
}

// Each block-bearing reset needs its own durable transcript transition.
fn take_reset_batch(c: &mut Conversation) -> Vec<crate::reset::Reset> {
    let count = if c.pending_reset_blocks.is_empty() {
        c.pending_resets.len()
    } else {
        1
    };
    c.pending_resets.drain(..count).collect()
}

fn place_reset(c: &mut Conversation, marker: crate::reset::Reset) {
    place_reset_before_writes(c, marker, usize::MAX);
}

fn place_reset_before_writes(
    c: &mut Conversation,
    marker: crate::reset::Reset,
    write_limit: usize,
) {
    let additions = c
        .pending_reset_blocks
        .remove(&marker.id)
        .unwrap_or_default();
    {
        let proof = c.block_reset_proof.get_or_insert_with(|| BlockResetProof {
            previous_context: c.context_generation,
            previous_resets: c.resets.clone(),
            previous_messages: c.messages.clone(),
            previous_blocks: c.content_blocks.clone(),
            placed_resets: Vec::new(),
            messages: Vec::new(),
            blocks: Default::default(),
            overwritten_blocks: false,
        });
        proof.overwritten_blocks |= !proof.blocks.is_empty();
        proof.placed_resets.push(marker.clone());
    }
    c.content_blocks.clear();
    c.context_generation = Some(Uuid::new_v4());
    c.occurrence_epoch_proven = true;
    c.messages.clear();
    if let Some(message) = marker.message() {
        c.messages.push(message);
    }
    for binding in additions {
        let index = c.messages.len();
        c.messages.push(Message::User {
            content: String::new(),
        });
        c.content_blocks.insert(index, binding);
    }
    if let Some(proof) = &mut c.block_reset_proof {
        proof.messages = c.messages.clone();
        proof.blocks = c.content_blocks.clone();
    }
    for write in c.application_writes.iter_mut().take(write_limit) {
        if !write.withdrawn && write.after_reset == Some(marker.id) {
            write.placed = true;
        }
    }
    c.resets.push(marker);
    c.auto_compaction_attempted = false;
}

fn finish_manual_compaction(
    c: &mut Conversation,
    status: ManualCompactionStatus,
) -> Result<(), Error> {
    if let Some(id) = c.active_manual_compaction.take() {
        let operation = c
            .manual_compactions
            .iter_mut()
            .find(|operation| operation.id == id)
            .ok_or_else(|| Error::Invalid("missing active manual compaction".into()))?;
        if operation.status != ManualCompactionStatus::Running {
            return Err(Error::Invalid("manual compaction already settled".into()));
        }
        operation.status = status;
    }
    Ok(())
}

fn append_tool(
    c: &mut Conversation,
    calls: &[ToolCall],
    index: usize,
    content: String,
) -> Result<(), Error> {
    let call = calls
        .get(index)
        .ok_or_else(|| Error::Invalid("invalid tool index".into()))?;
    c.messages.push(Message::Tool {
        call_id: call.id.clone(),
        content,
    });
    c.auto_compaction_attempted = false;
    Ok(())
}
// Only still-queued inputs from the failed settlement reserve the automatic run.
// Withdrawal or abort can remove them without leaving the conversation busy.
fn pending_failure_continuation(c: &Conversation) -> bool {
    c.submissions
        .iter()
        .any(|s| s.status == Status::Queued && c.failure_continuation_inputs.contains(&s.id))
}

// Final boundaries select each mode independently; post-tool boundaries select only steers.
fn place_queued(c: &mut Conversation, final_boundary: bool) -> bool {
    let starting_run = c.active.is_none();
    let mut steer = false;
    let mut follow = false;
    let mut placed = false;
    let mut continued_failure = false;
    for submission in &mut c.submissions {
        if submission.status != Status::Queued {
            continue;
        }
        let mode = c
            .input_modes
            .get(&submission.id)
            .copied()
            .unwrap_or(InputMode::FollowUp);
        let (seen, setting) = match mode {
            InputMode::Steer => (&mut steer, c.queue_modes.steering_mode),
            InputMode::FollowUp if final_boundary => (&mut follow, c.queue_modes.follow_up_mode),
            InputMode::FollowUp => continue,
        };
        if *seen && setting == QueueMode::OneAtATime {
            continue;
        }
        *seen = true;
        placed = true;
        continued_failure |= c.failure_continuation_inputs.contains(&submission.id);
        submission.status = Status::Running;
        c.active.get_or_insert(submission.id);
        let message = Message::User {
            content: submission.content.clone(),
        };
        let index = c.messages.len();
        c.messages.push(message.clone());
        if let Some(binding) = &submission.input_blocks {
            c.content_blocks.insert(index, binding.clone());
        }
        if let Some(proof) = &mut c.block_reset_proof {
            proof.messages.push(message);
            if let Some(binding) = &submission.input_blocks {
                proof.blocks.insert(index, binding.clone());
            }
        }
    }
    if starting_run {
        if placed && !continued_failure {
            c.run_failure = None;
        }
        c.failure_continuation_inputs.clear();
    }
    placed
}

fn finish_answer(c: &mut Conversation, answer: String) {
    let answer_id = Uuid::new_v4();
    c.active = None;
    for s in &mut c.submissions {
        if s.status == Status::Running {
            c.answer_ids.insert(s.id, answer_id);
            s.status = Status::Done {
                answer: answer.clone(),
            };
        }
    }
    c.phase = Phase::Idle;
}
fn apply_failed_run_policy(c: &mut Conversation) {
    match c.after_failed_run {
        AfterFailedRun::Keep => {}
        AfterFailedRun::Continue => {
            // Ordered resets, summaries and writes need their existing boundary
            // processing before inputs can be placed in the transcript.
            if c.pending_resets.is_empty()
                && c.pending_independent_summary.is_none()
                && !c
                    .application_writes
                    .iter()
                    .any(|w| !w.placed && !w.withdrawn)
            {
                let failure = c.run_failure.clone();
                if place_queued(c, true) {
                    c.run_failure = failure;
                    c.auto_compaction_attempted = false;
                    c.retry_state = RetryState::default();
                    c.overflow_compaction_attempted = false;
                    c.phase = Phase::ModelReady { turns: 0 };
                } else {
                    c.phase = Phase::Idle;
                }
                return;
            }
            c.failure_continuation_inputs = c
                .submissions
                .iter()
                .filter(|s| s.status == Status::Queued)
                .map(|s| s.id)
                .collect();
            c.phase = Phase::Idle;
        }
        AfterFailedRun::Withdraw => {
            for s in &mut c.submissions {
                if s.status == Status::Queued {
                    s.status = Status::Unanswered {
                        reason: "run_failed".into(),
                    };
                }
            }
        }
    }
}
fn finish_abort(c: &mut Conversation) {
    finish_failure(c, "aborted".into(), FailureCause::Aborted);
    if c.submissions.iter().any(|s| s.status == Status::Queued) {
        c.phase = Phase::Idle;
    }
}
enum FailureCause {
    ModelError,
    Aborted,
    Reset,
    TurnLimit(u32),
    NothingToAnswer,
    RestartLimit(u32),
}

fn finish_failure(c: &mut Conversation, reason: String, cause: FailureCause) {
    c.failure_continuation_inputs.clear();
    let (classification, detail) = match cause {
        FailureCause::ModelError => ("model_error", Some(reason.clone())),
        FailureCause::Aborted => ("aborted", None),
        FailureCause::Reset => ("reset", None),
        FailureCause::NothingToAnswer => ("nothing_to_answer", None),
        FailureCause::TurnLimit(turns) => (
            "turn_limit",
            Some(format!(
                "{turns} {}",
                if turns == 1 { "turn" } else { "turns" }
            )),
        ),
        FailureCause::RestartLimit(restarts) => {
            let count = if restarts == 1 {
                "once".to_string()
            } else {
                format!("{restarts} times in a row")
            };
            (
                "restart_limit",
                Some(format!(
                    "The generation was interrupted {count} in its model request phase"
                )),
            )
        }
    };
    let inputs: Vec<_> = c
        .submissions
        .iter()
        .filter(|s| s.status == Status::Running)
        .map(|s| s.id)
        .collect();
    if !inputs.is_empty() {
        c.run_failure = Some(RunFailure {
            inputs,
            reason: classification.into(),
            detail,
        });
    }
    c.active = None;
    for s in &mut c.submissions {
        if s.status == Status::Running {
            s.status = Status::Unanswered {
                reason: reason.clone(),
            };
        }
    }
    c.phase = Phase::FailedIdle;
}

fn valid_schema(schema: &serde_json::Value, arguments: &serde_json::Value) -> bool {
    jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(arguments))
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use pi_rs_core::{ModelEvent, ModelRequest, ModelStream};
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore;

    #[test]
    fn failed_run_record_round_trips_with_settled_inputs() {
        let mut c = Harness::empty_conversation(Uuid::new_v4(), config());
        let id = Uuid::new_v4();
        c.admit_initial_input(id, "work".into());
        finish_failure(&mut c, "model failed".into(), FailureCause::ModelError);
        let saved = serde_json::to_vec(&c).unwrap();
        let restored: Conversation = serde_json::from_slice(&saved).unwrap();
        assert_eq!(
            restored.run_failure,
            Some(RunFailure {
                inputs: vec![id],
                reason: "model_error".into(),
                detail: Some("model failed".into()),
            })
        );
        assert!(
            matches!(&restored.submissions[0].status, Status::Unanswered { reason } if reason == "model failed")
        );
    }

    #[test]
    fn failed_continuation_marker_survives_serialization_and_cannot_attach_to_new_input() {
        let mut c = Harness::empty_conversation(Uuid::new_v4(), config());
        let first = Uuid::new_v4();
        let queued = Uuid::new_v4();
        c.admit_initial_input(first, "first".into());
        let mut next = c.submissions[0].clone();
        next.id = queued;
        next.status = Status::Queued;
        c.submissions.push(next);
        c.after_failed_run = AfterFailedRun::Continue;
        c.pending_resets
            .push(crate::reset::Reset::new(Uuid::new_v4(), None));
        finish_failure(&mut c, "failure".into(), FailureCause::ModelError);
        apply_failed_run_policy(&mut c);
        let mut restored: Conversation =
            serde_json::from_slice(&serde_json::to_vec(&c).unwrap()).unwrap();
        assert!(place_queued(&mut restored, true));
        assert_eq!(restored.run_failure.as_ref().unwrap().inputs, vec![first]);
        assert!(restored.failure_continuation_inputs.is_empty());
        c.submissions[1].status = Status::Unanswered {
            reason: "withdrawn".into(),
        };
        let mut unrelated = c.submissions[1].clone();
        unrelated.id = Uuid::new_v4();
        unrelated.status = Status::Queued;
        c.submissions.push(unrelated);
        assert!(place_queued(&mut c, true));
        assert!(c.run_failure.is_none());
        let legacy: RunFailure = serde_json::from_value(
            serde_json::json!({"inputs": [], "reason": "old provider message"}),
        )
        .unwrap();
        assert_eq!(legacy.reason, "old provider message");
        assert_eq!(legacy.detail, None);
    }

    #[test]
    fn tool_status_bindings_use_occurrences_and_reject_ambiguous_legacy_ids() {
        let mut c = Harness::empty_conversation(Uuid::new_v4(), config());
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        c.messages = vec![
            Message::Tool {
                call_id: "same".into(),
                content: String::new(),
            },
            Message::Tool {
                call_id: "same".into(),
                content: String::new(),
            },
        ];
        for (index, id, is_error) in [(0, first, true), (1, second, false)] {
            c.content_blocks.insert(
                index,
                crate::transcript::MessageContentBlocks {
                    message_id: id,
                    blocks: vec![],
                },
            );
            c.tool_results.push(ToolResultMetadata {
                operation_id: Uuid::new_v4(),
                call_id: "same".into(),
                tool: "test".into(),
                content_block_id: Some(id),
                is_error,
                redacted: false,
                metadata_truncated: false,
                schema_validated: true,
                details: None,
                structured_output: None,
                attachments: vec![],
                diagnostics: vec![],
            });
        }
        assert_eq!(
            tool_error_bindings(&c).unwrap(),
            [(1, true), (2, false)].into_iter().collect()
        );
        c.tool_results[0].content_block_id = None;
        assert!(tool_error_bindings(&c).is_err());
        c.messages.truncate(1);
        c.tool_results.truncate(1);
        c.content_blocks.clear();
        assert!(tool_error_bindings(&c).is_err());
        c.context_generation = Some(Uuid::new_v4());
        assert!(tool_error_bindings(&c).is_err());
        c.tool_results.clear();
        assert!(tool_error_bindings(&c).unwrap().is_empty());
    }

    struct GatedModel {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
        active: AtomicUsize,
        maximum: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Model for GatedModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("ok".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    fn options(model: Arc<GatedModel>) -> Options {
        Options {
            model,
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        }
    }

    #[test]
    fn fork_anchor_accepts_other_snapshots_in_same_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.submit(id, None, "active".into()).unwrap();
        let (reset, _) = h
            .reset_with_blocks(
                id,
                None,
                vec![vec![serde_json::json!({"type":"text", "text":"block"})]],
            )
            .unwrap();
        let head = h.history_head(id).unwrap().0;
        let root = Harness::key(id);
        let fork = format!("runtime/fork/{id}/{head:020}").into_bytes();
        let extra = format!("runtime/fork/{id}/intermediate").into_bytes();
        let mut event = b"durable/observer/".to_vec();
        event.extend_from_slice(id.as_bytes());
        event.extend_from_slice(b"/r");
        event.extend_from_slice(&head.to_be_bytes());
        let store = h.owner.store();
        let root_value = store.get(&root).unwrap().unwrap().value;
        let fork_value = store.get(&fork).unwrap().unwrap().value;
        let event_value = store.get(&event).unwrap().unwrap().value;
        store
            .commit(
                &[],
                &[
                    Mutation::Put {
                        key: root,
                        value: root_value,
                    },
                    Mutation::Put {
                        key: fork.clone(),
                        value: fork_value,
                    },
                    Mutation::Put {
                        key: event,
                        value: event_value,
                    },
                    Mutation::Put {
                        key: extra,
                        value: b"untrusted".to_vec(),
                    },
                ],
            )
            .unwrap();
        h.authenticate_fork_anchor(id, head).unwrap();
        let (_, version) = h.load(id).unwrap();
        // The anchored derivation still relies on the unique-snapshot lookup.
        // Until that lookup is replaced with authenticated cut selection it
        // cannot derive a baseline from a multi-snapshot commit.
        assert!(
            h.derive_anchored_reset_states(
                id,
                version,
                &[ResetDerivationStep {
                    reset_id: reset,
                    context_generation: Uuid::new_v4(),
                    write_limit: usize::MAX,
                }]
            )
            .is_err()
        );
        drop(h);
        let h = Harness::open_paused(&path, options(model)).unwrap();
        h.authenticate_fork_anchor(id, head).unwrap();
        // Rewriting the selected snapshot alone must still invalidate it.
        let value = h.owner.store().get(&fork).unwrap().unwrap().value;
        h.owner
            .store()
            .commit(&[], &[Mutation::Put { key: fork, value }])
            .unwrap();
        assert!(h.authenticate_fork_anchor(id, head).is_err());
    }

    #[test]
    fn new_fork_seed_does_not_revalidate_legacy_result() {
        let dir = tempfile::tempdir().unwrap();
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(placement_model())).unwrap();
        let mut state = Harness::empty_conversation(Uuid::new_v4(), config());
        state
            .tool_filters
            .output_schemas
            .insert("allowed".into(), serde_json::json!({"type":"integer"}));
        state.tool_results.push(ToolResultMetadata {
            operation_id: Uuid::new_v4(),
            call_id: "legacy".into(),
            tool: "allowed".into(),
            content_block_id: None,
            is_error: false,
            redacted: false,
            metadata_truncated: false,
            schema_validated: false,
            details: None,
            structured_output: None,
            attachments: Vec::new(),
            diagnostics: Vec::new(),
        });
        h.save(&state, Expected::Missing).unwrap();
        let saved = h.conversation(state.id).unwrap();
        assert!(!saved.tool_results[0].schema_validated);
        assert!(saved.tool_results[0].diagnostics.is_empty());
    }

    #[tokio::test]
    async fn shutdown_joins_summary_fence_holder_before_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut scheduler = Scheduler::open(&path, options(placement_model())).unwrap();
        let id = Uuid::new_v4();
        let owner = scheduler.harness.owner.clone();
        let cancel = scheduler.cancel.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = owner.summary_guard(id).await;
            started.send(()).unwrap();
            cancel.cancelled().await;
            Ok(())
        });
        scheduler.tasks.entry(id).or_default().push(task);
        ready.await.unwrap();
        assert!(matches!(OwnedStore::open(&path), Err(OwnerError::Busy)));
        scheduler.shutdown().await.unwrap();
        let reopened = Arc::new(OwnedStore::open(&path).unwrap());
        let _guard = tokio::time::timeout(Duration::from_secs(2), reopened.summary_guard(id))
            .await
            .unwrap();
    }

    #[test]
    fn runtime_block_save_reopens_and_rejects_lossy_operations() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let blocks = vec![
            json!({"type":"text","text":"a","textSignature":"opaque"}),
            json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            json!({"type":"text","text":"b"}),
        ];
        let occurrence = h.append_user_blocks(id, blocks.clone()).unwrap();
        let second = h.append_user_blocks(id, vec![]).unwrap();
        let limit = std::num::NonZeroUsize::new(32).unwrap();
        let history = h.history(id, crate::observer::Cursor(0), limit).unwrap();
        let snapshot = &history.records.last().unwrap().value;
        assert_eq!(
            snapshot["content_blocks"]["0"]["message_id"],
            json!(occurrence)
        );
        assert_eq!(snapshot["content_blocks"]["0"]["blocks"], json!(blocks));
        assert_eq!(snapshot["content_blocks"]["1"]["message_id"], json!(second));
        let through = h.history_head(id).unwrap();
        assert!(
            h.history_through(id, crate::observer::Cursor(0), through, limit)
                .is_ok()
        );
        let cursor = h.unified_history_start(id, false).unwrap();
        assert!(h.unified_history(&cursor, limit).is_ok());
        let c = h.conversation(id).unwrap();
        let binding = c.content_blocks.get(&0).unwrap();
        assert_eq!(binding.message_id, occurrence);
        assert_eq!(binding.blocks, blocks);
        assert_eq!(c.content_blocks[&1].message_id, second);
        let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
        let page = transcript
            .read_with_blocks(
                &transcript.snapshot().unwrap(),
                std::num::NonZeroUsize::new(8).unwrap(),
            )
            .unwrap();
        for record in page.records.iter().filter(|r| r.content_blocks.is_some()) {
            let binding = &record.content_blocks.as_ref().unwrap()[0];
            let value = record.value.as_object().unwrap().values().next().unwrap();
            assert!(
                value["ids"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(binding.message_id))
            );
        }
        let position = ViewCursor {
            cursor: transcript.snapshot().unwrap(),
            remaining: None,
        };
        let empty_fork = h.fork_at(id, crate::observer::Cursor(1)).unwrap();
        assert!(
            h.conversation(empty_fork)
                .unwrap()
                .content_blocks
                .is_empty()
        );
        let first_fork = h.fork_by_entry_id(id, occurrence).unwrap();
        let first_state = h.conversation(first_fork).unwrap();
        assert_eq!(first_state.messages.len(), 1);
        assert_eq!(first_state.content_blocks[&0].message_id, occurrence);
        assert_eq!(first_state.content_blocks[&0].blocks, blocks);
        let fork = h.fork(id).unwrap();
        let forked = h.conversation(fork).unwrap();
        assert_eq!(forked.content_blocks, c.content_blocks);
        let original_history = h
            .resolved_occurrences(id, c.transcript.clone().unwrap())
            .unwrap();
        let fork_history = h
            .resolved_occurrences(fork, forked.transcript.clone().unwrap())
            .unwrap();
        assert_eq!(original_history, fork_history);
        let inherited_fork = h.fork_by_entry_id(fork, occurrence).unwrap();
        assert_eq!(
            h.conversation(inherited_fork).unwrap().content_blocks[&0].blocks,
            blocks
        );
        let source = h.conversation(first_fork).unwrap();
        let historical = h.history(id, crate::observer::Cursor(0), limit).unwrap();
        let cut = historical
            .records
            .iter()
            .find(|r| {
                r.value["content_blocks"].get("0").is_some()
                    && r.value["messages"].as_array().is_some_and(|m| m.len() == 1)
            })
            .unwrap()
            .cursor;
        let by_cursor = h.fork_at(id, cut).unwrap();
        let cut_state = h.conversation(by_cursor).unwrap();
        assert_eq!(cut_state.content_blocks, source.content_blocks);
        assert_eq!(
            h.resolved_occurrences(by_cursor, cut_state.transcript.unwrap())
                .unwrap(),
            h.resolved_occurrences(first_fork, source.transcript.unwrap())
                .unwrap()
        );
        let added = h
            .append_user_blocks(fork, vec![json!({"type":"text", "text":"child only"})])
            .unwrap();
        assert_eq!(h.conversation(id).unwrap().content_blocks.len(), 2);
        assert_eq!(
            h.conversation(fork).unwrap().content_blocks[&2].message_id,
            added
        );
        assert!(h.fork_at_position(id, position).is_err());
        assert!(h.fork_at_application_entry(id, occurrence).is_err());
        let (mut changed, version) = h.load(id).unwrap();
        changed.phase = Phase::ModelReady { turns: 0 };
        h.save(&changed, Expected::Version(version)).unwrap();
        let reset = h.reset(id, None).unwrap();
        assert_eq!(h.conversation(id).unwrap().pending_resets[0].id, reset);
        let (mut changed, version) = h.load(id).unwrap();
        changed.messages.clear();
        assert!(matches!(
            h.save(&changed, Expected::Version(version)),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        drop(h);
        let h = Harness::open_paused(&path, options(model)).unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.content_blocks[&0].message_id, occurrence);
        assert_eq!(c.content_blocks[&0].blocks, blocks);
        assert_eq!(c.content_blocks[&1].message_id, second);
        assert!(c.content_blocks[&1].blocks.is_empty());
        let reopened_fork = h.conversation(fork).unwrap();
        assert_eq!(reopened_fork.content_blocks[&0], c.content_blocks[&0]);
        assert_eq!(reopened_fork.content_blocks[&1], c.content_blocks[&1]);
        assert_eq!(reopened_fork.content_blocks[&2].message_id, added);
        let parent_history = h
            .resolved_occurrences(id, c.transcript.clone().unwrap())
            .unwrap();
        let child_history = h
            .resolved_occurrences(fork, reopened_fork.transcript.clone().unwrap())
            .unwrap();
        assert_eq!(
            &child_history[..parent_history.len()],
            parent_history.as_slice()
        );
        assert_eq!(
            h.conversation(first_fork).unwrap().content_blocks[&0].message_id,
            occurrence
        );
        assert_eq!(
            h.conversation(inherited_fork).unwrap().content_blocks[&0].message_id,
            occurrence
        );
        assert_eq!(
            h.conversation(by_cursor).unwrap().content_blocks[&0].blocks,
            blocks
        );
        let page = h.history(id, crate::observer::Cursor(0), limit).unwrap();
        assert_eq!(
            page.records.last().unwrap().value["content_blocks"]["0"]["message_id"],
            json!(occurrence)
        );
    }

    #[test]
    fn block_reset_preserves_historical_occurrences_across_reopen() {
        use serde_json::json;
        for handoff in [None, Some(""), Some("summary")] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = placement_model();
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            let id = h.create_conversation(config()).unwrap();
            let blocks = vec![
                json!({"type":"text", "text":"before"}),
                json!({"type":"image", "data":"aGk=", "mimeType":"image/png"}),
                json!({"type":"text", "text":"after"}),
            ];
            let occurrence = h.append_user_blocks(id, blocks.clone()).unwrap();
            let before = h.conversation(id).unwrap();
            let histories = h
                .resolved_occurrences(id, before.transcript.clone().unwrap())
                .unwrap();
            let marker = h.reset(id, handoff.map(str::to_owned)).unwrap();
            let current = h.conversation(id).unwrap();
            assert!(current.content_blocks.is_empty());
            assert_eq!(current.resets.last().unwrap().id, marker);
            assert_eq!(
                current.messages,
                handoff
                    .map(|text| Message::User {
                        content: text.into()
                    })
                    .into_iter()
                    .collect::<Vec<_>>()
            );
            let old_fork = h.fork_by_entry_id(id, occurrence).unwrap();
            let old = h.conversation(old_fork).unwrap();
            assert_eq!(old.content_blocks[&0].blocks, blocks);
            assert_eq!(
                h.resolved_occurrences(old_fork, old.transcript.unwrap())
                    .unwrap(),
                histories
            );
            drop(h);
            let mut h = Harness::open_paused(&path, options(model)).unwrap();
            let current = h.conversation(id).unwrap();
            assert!(current.content_blocks.is_empty());
            assert_eq!(current.resets.last().unwrap().id, marker);
            let recovered = h.fork_by_entry_id(id, occurrence).unwrap();
            let recovered = h.conversation(recovered).unwrap();
            assert_eq!(recovered.content_blocks[&0].message_id, occurrence);
            assert_eq!(recovered.content_blocks[&0].blocks, blocks);
            assert_eq!(
                h.resolved_occurrences(recovered.id, recovered.transcript.unwrap())
                    .unwrap(),
                histories
            );
            let next = h
                .append_user_blocks(id, vec![json!({"type":"text", "text":"new context"})])
                .unwrap();
            let current = h.conversation(id).unwrap();
            let index = usize::from(handoff.is_some());
            assert_eq!(current.content_blocks[&index].message_id, next);
            assert_ne!(next, occurrence);
            let active = h
                .resolved_occurrences(id, current.transcript.unwrap())
                .unwrap();
            assert_eq!(active[index].1.as_ref().unwrap().id, next);
        }
    }

    #[test]
    fn block_reset_and_append_race_conflicts_without_losing_history() {
        use serde_json::json;
        for reset_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = placement_model();
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            let id = h.create_conversation(config()).unwrap();
            let first = h
                .append_user_blocks(id, vec![json!({"type":"text", "text":"first"})])
                .unwrap();
            let (mut planned, version) = h.load(id).unwrap();
            let mut added = None;
            if reset_first {
                let next = Uuid::new_v4();
                let index = planned.messages.len();
                planned.messages.push(Message::User {
                    content: String::new(),
                });
                planned.content_blocks.insert(
                    index,
                    crate::transcript::MessageContentBlocks {
                        message_id: next,
                        blocks: vec![json!({"type":"text", "text":"uncommitted"})],
                    },
                );
                added = Some(next);
            } else {
                place_reset(
                    &mut planned,
                    crate::reset::Reset::new(Uuid::new_v4(), Some("stale".into())),
                );
            }
            let stale = h
                .prepare_save(&planned, Expected::Version(version))
                .unwrap();
            if reset_first {
                h.reset(id, Some("winner".into())).unwrap();
            } else {
                added = Some(
                    h.append_user_blocks(id, vec![json!({"type":"text", "text":"second"})])
                        .unwrap(),
                );
            }
            assert!(matches!(
                h.commit_conversation_save(stale),
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
            ));
            if !reset_first {
                h.reset(id, Some("winner".into())).unwrap();
            }
            drop(h);
            let mut h = Harness::open_paused(&path, options(model)).unwrap();
            let c = h.conversation(id).unwrap();
            assert!(c.content_blocks.is_empty());
            assert_eq!(
                c.messages,
                vec![Message::User {
                    content: "winner".into()
                }]
            );
            let fork = h.fork_by_entry_id(id, first).unwrap();
            assert_eq!(
                h.conversation(fork).unwrap().content_blocks[&0].message_id,
                first
            );
            if reset_first {
                assert!(h.fork_by_entry_id(id, added.unwrap()).is_err());
            } else {
                let fork = h.fork_by_entry_id(id, added.unwrap()).unwrap();
                assert_eq!(h.conversation(fork).unwrap().content_blocks.len(), 2);
            }
        }
    }

    #[test]
    fn reset_block_placement_conflicts_with_concurrent_append() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(placement_model())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let original = h
            .append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
            .unwrap();
        let (mut planned, version) = h.load(id).unwrap();
        let reset = crate::reset::Reset::new(Uuid::new_v4(), Some("handoff".into()));
        let new_id = Uuid::new_v4();
        planned.pending_reset_blocks.insert(
            reset.id,
            vec![crate::transcript::MessageContentBlocks {
                message_id: new_id,
                blocks: vec![json!({"type":"text","text":"staged"})],
            }],
        );
        place_reset(&mut planned, reset.clone());
        let staged = h
            .prepare_save(&planned, Expected::Version(version))
            .unwrap();
        let winner = h
            .append_user_blocks(id, vec![json!({"type":"text","text":"winner"})])
            .unwrap();
        assert!(matches!(
            h.commit_conversation_save(staged),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        let c = h.conversation(id).unwrap();
        assert!(c.resets.iter().all(|marker| marker.id != reset.id));
        assert_eq!(c.content_blocks[&0].message_id, original);
        assert_eq!(c.content_blocks[&1].message_id, winner);
        assert!(
            c.content_blocks
                .values()
                .all(|binding| binding.message_id != new_id)
        );
    }

    #[test]
    fn queued_reset_blocks_cannot_be_mutated_dropped_or_overwritten() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.submit(id, None, "active".into()).unwrap();
        let (reset, ids) = h
            .reset_with_blocks(
                id,
                Some("handoff".into()),
                vec![vec![json!({"type":"text","text":"original"})]],
            )
            .unwrap();
        let (mut changed, version) = h.load(id).unwrap();
        changed.pending_reset_blocks.get_mut(&reset).unwrap()[0].blocks =
            vec![json!({"type":"text","text":"tampered"})];
        assert!(h.save(&changed, Expected::Version(version)).is_err());
        let (mut changed, version) = h.load(id).unwrap();
        changed.pending_reset_blocks.clear();
        assert!(h.save(&changed, Expected::Version(version)).is_err());
        let (mut changed, version) = h.load(id).unwrap();
        let marker = changed.pending_resets.remove(0);
        place_reset(&mut changed, marker);
        place_reset(&mut changed, crate::reset::Reset::new(Uuid::new_v4(), None));
        assert!(h.save(&changed, Expected::Version(version)).is_err());
        drop(h);
        let h = Harness::open_paused(&path, options(model)).unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.pending_reset_blocks[&reset][0].message_id, ids[0]);
        assert_eq!(
            c.pending_reset_blocks[&reset][0].blocks,
            vec![json!({"type":"text","text":"original"})]
        );
    }

    #[tokio::test]
    async fn queued_block_reset_stale_placement_loses_to_admission_and_abort() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.submit(id, None, "active".into()).unwrap();
        let (first, first_ids) = h
            .reset_with_blocks(id, None, vec![vec![json!({"type":"text","text":"first"})]])
            .unwrap();
        let (mut planned, version) = h.load(id).unwrap();
        let marker = planned.pending_resets.remove(0);
        place_reset(&mut planned, marker);
        let stale = h
            .prepare_save(&planned, Expected::Version(version))
            .unwrap();
        let (second, second_ids) = h
            .reset_with_blocks(id, None, vec![vec![json!({"type":"text","text":"second"})]])
            .unwrap();
        assert!(matches!(
            h.commit_conversation_save(stale),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        let (mut planned, version) = h.load(id).unwrap();
        let marker = planned.pending_resets.remove(0);
        place_reset(&mut planned, marker);
        let stale = h
            .prepare_save(&planned, Expected::Version(version))
            .unwrap();
        h.request_abort(id).unwrap();
        assert!(matches!(
            h.commit_conversation_save(stale),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        drop(h);
        let mut h = Harness::open_paused(&path, options(model)).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(c.content_blocks[&0].message_id, second_ids[0]);
        let fork = h.fork_by_entry_id(id, first_ids[0]).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().content_blocks[&0].message_id,
            first_ids[0]
        );
    }

    #[tokio::test]
    async fn queued_block_resets_recover_between_placement_commits() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let (id, first, second, first_id, second_id);
        {
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            id = h.create_conversation(config()).unwrap();
            h.submit(id, None, "active".into()).unwrap();
            let (r1, ids1) = h
                .reset_with_blocks(id, None, vec![vec![json!({"type":"text","text":"first"})]])
                .unwrap();
            let (r2, ids2) = h
                .reset_with_blocks(id, None, vec![vec![json!({"type":"text","text":"second"})]])
                .unwrap();
            (first, second, first_id, second_id) = (r1, r2, ids1[0], ids2[0]);
            let (mut c, version) = h.load(id).unwrap();
            let marker = c.pending_resets.remove(0);
            place_reset(&mut c, marker);
            h.save(&c, Expected::Version(version)).unwrap();
        }
        let mut h = Harness::open_paused(&path, options(model)).unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.content_blocks[&0].message_id, first_id);
        assert_eq!(c.pending_resets[0].id, second);
        h.request_abort(id).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(c.content_blocks[&0].message_id, second_id);
        let fork = h.fork_by_entry_id(id, first_id).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().content_blocks[&0].message_id,
            first_id
        );
    }

    #[test]
    fn block_reset_rejects_unproven_replacements_and_extra_messages() {
        let dir = tempfile::tempdir().unwrap();
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(placement_model())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.append_user_blocks(id, vec![]).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        place_reset(&mut state, crate::reset::Reset::new(Uuid::new_v4(), None));
        state.messages.push(Message::User {
            content: "unproven".into(),
        });
        assert!(matches!(
            h.save(&state, Expected::Version(version)),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        let (mut state, version) = h.load(id).unwrap();
        state.messages.clear();
        state.content_blocks.clear();
        state.context_generation = Some(Uuid::new_v4());
        state
            .resets
            .push(crate::reset::Reset::new(Uuid::new_v4(), None));
        assert!(matches!(
            h.save(&state, Expected::Version(version)),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        let (mut state, version) = h.load(id).unwrap();
        state
            .content_blocks
            .get_mut(&0)
            .unwrap()
            .blocks
            .push(serde_json::json!({"type":"text", "text":"forged"}));
        place_reset(&mut state, crate::reset::Reset::new(Uuid::new_v4(), None));
        assert!(matches!(
            h.save(&state, Expected::Version(version)),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        let (mut state, version) = h.load(id).unwrap();
        state
            .resets
            .push(crate::reset::Reset::new(Uuid::new_v4(), None));
        assert!(matches!(
            h.save(&state, Expected::Version(version)),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        let (mut state, version) = h.load(id).unwrap();
        place_reset(&mut state, crate::reset::Reset::new(Uuid::new_v4(), None));
        state.messages.push(Message::User {
            content: String::new(),
        });
        state.content_blocks.insert(
            0,
            crate::transcript::MessageContentBlocks {
                message_id: Uuid::new_v4(),
                blocks: vec![],
            },
        );
        assert!(matches!(
            h.save(&state, Expected::Version(version)),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        let (mut state, version) = h.load(id).unwrap();
        let marker = crate::reset::Reset::new(Uuid::new_v4(), None);
        let old_id = state.content_blocks[&0].message_id;
        state.pending_reset_blocks.insert(
            marker.id,
            vec![crate::transcript::MessageContentBlocks {
                message_id: old_id,
                blocks: vec![],
            }],
        );
        place_reset(&mut state, marker);
        assert!(h.save(&state, Expected::Version(version)).is_err());
        assert_eq!(h.conversation(id).unwrap().content_blocks.len(), 1);
    }

    #[test]
    fn interior_block_batch_fork_preserves_provenance_across_reopen() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let (id, child, ids, expected);
        {
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            ids = [Uuid::new_v4(), Uuid::new_v4()];
            for (index, occurrence) in ids.iter().enumerate() {
                c.messages.push(Message::User {
                    content: String::new(),
                });
                c.content_blocks.insert(
                    index,
                    crate::transcript::MessageContentBlocks {
                        message_id: *occurrence,
                        blocks: vec![
                            json!({"type":"text", "text":format!("{index}")}),
                            json!({"type":"image", "data":"aGk=", "mimeType":"image/png"}),
                            json!({"type":"text", "text":"tail", "textSignature":"opaque"}),
                        ],
                    },
                );
            }
            h.save(&c, Expected::Version(version)).unwrap();
            let source = h.conversation(id).unwrap();
            expected = h
                .resolved_occurrences(id, source.transcript.clone().unwrap())
                .unwrap();
            child = h
                .fork_at_position(
                    id,
                    ViewCursor {
                        cursor: source.transcript.unwrap(),
                        remaining: Some(1),
                    },
                )
                .unwrap();
            let forked = h.conversation(child).unwrap();
            assert_eq!(forked.messages.len(), 1);
            assert_eq!(forked.content_blocks[&0], c.content_blocks[&0]);
            assert_eq!(
                h.resolved_occurrences(child, forked.transcript.unwrap())
                    .unwrap(),
                expected[..1]
            );
            let via_id = h.fork_by_entry_id(id, ids[0]).unwrap();
            assert_eq!(
                h.conversation(via_id).unwrap().content_blocks[&0],
                c.content_blocks[&0]
            );
        }
        let mut h = Harness::open_paused(&path, options(model)).unwrap();
        let forked = h.conversation(child).unwrap();
        let marker = forked.transcript.clone().unwrap();
        assert_eq!(forked.content_blocks[&0].message_id, ids[0]);
        assert_eq!(
            h.resolved_occurrences(child, forked.transcript.unwrap())
                .unwrap(),
            expected[..1]
        );
        let descendant = h.fork_by_entry_id(child, ids[0]).unwrap();
        assert_eq!(
            h.conversation(descendant).unwrap().content_blocks[&0].message_id,
            ids[0]
        );
        assert!(h.fork_by_entry_id(child, ids[1]).is_err());
        let new_id = h
            .append_user_blocks(child, vec![json!({"type":"text", "text":"new"})])
            .unwrap();
        let appended = h.conversation(child).unwrap();
        assert_eq!(appended.content_blocks[&0].message_id, ids[0]);
        assert_eq!(appended.content_blocks[&1].message_id, new_id);
        assert_eq!(
            h.resolved_occurrences(child, appended.transcript.unwrap())
                .unwrap()[0],
            expected[0]
        );
        // A valid-looking but altered block on the child projection must not
        // override the immutable occurrence from its parent.
        let cursor = serde_json::to_value(marker).unwrap();
        let entry_id = Uuid::parse_str(cursor["next"]["id"].as_str().unwrap()).unwrap();
        let mut key = b"durable/transcript/entry/".to_vec();
        key.extend_from_slice(entry_id.as_bytes());
        let record = h.owner.store().get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        value["content_blocks"][0]["blocks"][0]["text"] = json!("tampered");
        h.owner
            .store()
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(record.version),
                }],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        assert!(h.conversation(child).is_err());
        drop(h);
        assert!(Harness::open_paused(&path, options(placement_model())).is_err());
    }

    #[test]
    fn interior_fork_before_first_block_preserves_plain_occurrence() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let (child, original);
        {
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            let id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.messages = vec![
                Message::User {
                    content: "plain".into(),
                },
                Message::User {
                    content: String::new(),
                },
                Message::User {
                    content: "later".into(),
                },
            ];
            c.content_blocks.insert(
                1,
                crate::transcript::MessageContentBlocks {
                    message_id: Uuid::new_v4(),
                    blocks: vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
                },
            );
            h.save(&c, Expected::Version(version)).unwrap();
            let source = h.conversation(id).unwrap();
            let histories = h
                .resolved_occurrences(id, source.transcript.clone().unwrap())
                .unwrap();
            original = histories[0].1.clone().unwrap();
            child = h
                .fork_at_position(
                    id,
                    ViewCursor {
                        cursor: source.transcript.unwrap(),
                        remaining: Some(1),
                    },
                )
                .unwrap();
            let forked = h.conversation(child).unwrap();
            assert!(forked.content_blocks.is_empty());
            assert_eq!(
                forked.messages,
                vec![Message::User {
                    content: "plain".into()
                }]
            );
            let page = h
                .view_positioned(child, std::num::NonZeroUsize::new(10).unwrap())
                .unwrap();
            assert_eq!(page.entries[0].history, Some(original.clone()));
            let nested = h
                .fork_at_position(child, page.entries[0].position.clone())
                .unwrap();
            assert_eq!(h.conversation(nested).unwrap().messages, forked.messages);
        }
        let mut h = Harness::open_paused(&path, options(model)).unwrap();
        let forked = h.conversation(child).unwrap();
        assert_eq!(
            h.resolved_occurrences(child, forked.transcript.unwrap())
                .unwrap()[0]
                .1,
            Some(original.clone())
        );
        let descendant = h.fork_by_entry_id(child, original.id).unwrap();
        assert_eq!(
            h.conversation(descendant).unwrap().messages,
            forked.messages
        );
    }

    #[test]
    fn interior_block_fork_rejects_unproven_and_incomplete_tool_cuts() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(placement_model())).unwrap();
        for unproven in [false, true] {
            let id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.messages.push(Message::User {
                content: String::new(),
            });
            let occurrence = Uuid::new_v4();
            c.content_blocks.insert(
                0,
                crate::transcript::MessageContentBlocks {
                    message_id: occurrence,
                    blocks: vec![json!({"type":"text","text":"keep"})],
                },
            );
            if unproven {
                c.allowed_tools = Some(vec![]);
                c.messages.push(Message::User {
                    content: "later".into(),
                });
            } else {
                c.messages.push(Message::Assistant {
                    content: String::new(),
                    calls: vec![ToolCall {
                        id: "call".into(),
                        name: "test".into(),
                        arguments: "{}".into(),
                    }],
                    replay: None,
                });
                c.messages.push(Message::Tool {
                    call_id: "call".into(),
                    content: "done".into(),
                });
                c.messages.push(Message::User {
                    content: "later".into(),
                });
            }
            h.save(&c, Expected::Version(version)).unwrap();
            let cursor = h.conversation(id).unwrap().transcript.unwrap();
            let count = if unproven { 1 } else { 2 };
            assert!(
                h.fork_at_position(
                    id,
                    ViewCursor {
                        cursor: cursor.clone(),
                        remaining: Some(count)
                    }
                )
                .is_err()
            );
            assert!(
                h.fork_at_position(
                    id,
                    ViewCursor {
                        cursor,
                        remaining: Some(0)
                    }
                )
                .is_err()
            );
            assert_eq!(
                h.conversation(id).unwrap().content_blocks[&0].message_id,
                occurrence
            );
        }
    }

    #[test]
    fn durable_yield_preserves_representable_array_shape() {
        let mut state = Harness::empty_conversation(Uuid::new_v4(), config());
        let blocks = vec![serde_json::json!({"type":"text", "text":"hello"})];
        Harness::append_yield(
            &mut state,
            crate::content::UserContent::Blocks(blocks.clone()),
            true,
        );
        assert_eq!(state.content_blocks[&0].blocks, blocks);
        assert!(matches!(&state.messages[0], Message::User { content } if content.is_empty()));
    }

    #[tokio::test]
    async fn block_history_tool_handoff_resets_and_preserves_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let occurrence = h
            .append_user_blocks(id, vec![serde_json::json!({"type":"text", "text":"kept"})])
            .unwrap();
        let (mut state, version) = h.load(id).unwrap();
        state.round_handoffs = vec![Some("handoff".into())];
        state.phase = Phase::AfterTools {
            turns: 1,
            calls: vec![],
            operation_id: Uuid::new_v4(),
        };
        h.save(&state, Expected::Version(version)).unwrap();
        model.release.add_permits(1);
        tokio::time::timeout(
            Duration::from_secs(5),
            h.resume_conversation(id, &CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options(model)).unwrap();
        let state = h.conversation(id).unwrap();
        assert!(matches!(state.phase, Phase::Idle));
        assert!(state.round_handoffs.is_empty());
        assert!(state.content_blocks.is_empty());
        assert!(matches!(&state.messages[0], Message::User { content } if content == "handoff"));
        let historical = h.fork_by_entry_id(id, occurrence).unwrap();
        assert_eq!(
            h.conversation(historical).unwrap().content_blocks[&0].message_id,
            occurrence
        );
    }

    #[test]
    fn staged_block_append_is_atomic_and_replayable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        let occurrence = Uuid::new_v4();
        state.messages.push(Message::User {
            content: String::new(),
        });
        state.content_blocks.insert(
            0,
            crate::transcript::MessageContentBlocks {
                message_id: occurrence,
                blocks: vec![serde_json::json!({"type":"text","text":"replay"})],
            },
        );
        let staged = h
            .prepare_conversation_save(&state, Expected::Version(version), None, &[], &[])
            .unwrap();
        drop(staged);
        drop(h);
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        assert!(h.conversation(id).unwrap().content_blocks.is_empty());
        let (_, version) = h.load(id).unwrap();
        h.save(&state, Expected::Version(version)).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options(model)).unwrap();
        assert_eq!(
            h.conversation(id).unwrap().content_blocks[&0].message_id,
            occurrence
        );
    }

    #[tokio::test]
    async fn block_history_never_reaches_core_model() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.append_user_blocks(id, vec![]).unwrap();
        assert!(matches!(
            h.submit(id, None, "next".into()),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        assert!(matches!(
            h.submit_continue(id, None),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        assert!(matches!(h.conversation(id).unwrap().phase, Phase::Idle));
        let (mut state, version) = h.load(id).unwrap();
        state.phase = Phase::ModelReady { turns: 0 };
        h.save(&state, Expected::Version(version)).unwrap();
        assert!(matches!(
            h.resume_conversation(id, &CancellationToken::new()).await,
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        assert_eq!(model.maximum.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn parked_driver_takes_over_after_another_driver_is_cancelled() {
        struct PendingTool(Arc<Semaphore>);
        #[async_trait::async_trait]
        impl Tool for PendingTool {
            fn spec(&self) -> pi_rs_core::ToolSpec {
                pi_rs_core::ToolSpec {
                    name: "job".into(),
                    description: String::new(),
                    parameters: serde_json::json!({"type": "object"}),
                }
            }
            fn can_poll(&self) -> bool {
                true
            }
            async fn invoke(&self, _: serde_json::Value, _: pi_rs_core::ToolContext) -> Outcome {
                panic!("pending operation must not be dispatched again")
            }
            async fn poll(&self, _: &str, _: pi_rs_core::ToolContext) -> Option<Outcome> {
                self.0.add_permits(1);
                Some(Outcome::Pending {
                    reference: "job".into(),
                })
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let polls = Arc::new(Semaphore::new(0));
        let mut opts = options(placement_model());
        opts.tools.push(RegisteredTool {
            tool: Arc::new(PendingTool(polls.clone())),
            replay: ReplayPolicy::Unsafe,
        });
        let mut harness = Harness::open_paused(dir.path().join("db"), opts).unwrap();
        let id = harness.create_conversation(config()).unwrap();
        let (mut state, version) = harness.load(id).unwrap();
        state.phase = Phase::Pending {
            turns: 0,
            calls: vec![ToolCall {
                id: "call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }],
            index: 0,
            operation_id: Uuid::new_v4(),
            reference: "job".into(),
            arguments: None,
        };
        harness.save(&state, Expected::Version(version)).unwrap();
        let control = harness.owner.conversation_controls().get(id);
        let start = |mut worker: Harness, cancel: CancellationToken| {
            let control = control.clone();
            tokio::spawn(async move {
                worker.execution_gate = Some(control.gate);
                let lock = control.worker;
                let mut guard = Some(lock.lock().await);
                let activity = worker
                    .activity
                    .enter(crate::worker_activity::WorkerKind::Conversation, id);
                drive_scheduled(&mut worker, id, &cancel, &lock, &mut guard, None, activity).await
            })
        };
        let first_cancel = CancellationToken::new();
        let first = start(harness.worker(), first_cancel.clone());
        polls.acquire().await.unwrap().forget();
        let second_cancel = CancellationToken::new();
        let second = start(harness.worker(), second_cancel.clone());
        polls.acquire().await.unwrap().forget();
        second_cancel.cancel();
        second.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(3), polls.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        first_cancel.cancel();
        first.await.unwrap().unwrap();

        // Cleanup drivers also supersede an earlier failed scheduler worker.
        let errors = harness.owner.conversation_controls().worker_errors();
        errors.lock().unwrap().insert(id, "worker panicked".into());
        let stop = CancellationToken::new();
        let mut cleanup = harness.worker();
        let cleanup_stop = stop.clone();
        let task =
            tokio::spawn(async move { cleanup.drive_owned_cleanup(id, &cleanup_stop).await });
        tokio::time::timeout(Duration::from_secs(2), polls.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(!errors.lock().unwrap().contains_key(&id));
        stop.cancel();
        task.await.unwrap().unwrap();

        // Cancellation before acquiring the lock cannot erase a failure.
        errors.lock().unwrap().insert(id, "worker panicked".into());
        let lock = control.worker.clone();
        let guard = lock.lock().await;
        let stop = CancellationToken::new();
        stop.cancel();
        let mut cleanup = harness.worker();
        cleanup.drive_owned_cleanup(id, &stop).await.unwrap();
        assert_eq!(
            errors.lock().unwrap().get(&id).map(String::as_str),
            Some("worker panicked")
        );
        drop(guard);
    }
    #[tokio::test]
    async fn cleanup_failure_is_visible_to_other_drivers() {
        let dir = tempfile::tempdir().unwrap();
        let mut harness =
            Harness::open_paused(dir.path().join("db"), options(placement_model())).unwrap();
        let id = Uuid::new_v4();
        let errors = harness.owner.conversation_controls().worker_errors();
        assert!(
            harness
                .drive_owned_cleanup(id, &CancellationToken::new())
                .await
                .is_err()
        );
        assert!(errors.lock().unwrap().contains_key(&id));
    }

    struct AutoBlockModel {
        window: Option<u32>,
        summaries: std::sync::Mutex<Vec<ModelRequest>>,
    }
    #[async_trait::async_trait]
    impl Model for AutoBlockModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            panic!("adapter required")
        }
        async fn context_window(&self, _: &str) -> Option<u32> {
            self.window
        }
        async fn stream_for_summary(
            &self,
            _: Uuid,
            _: Uuid,
            request: ModelRequest,
        ) -> Result<ModelStream, pi_rs_core::Error> {
            self.summaries.lock().unwrap().push(request);
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("checkpoint".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    struct AutoBlockAdapter {
        calls: AtomicUsize,
        overflow: bool,
        requests: std::sync::Mutex<Vec<crate::model::DurableRequest>>,
    }
    #[async_trait::async_trait]
    impl crate::model::ProviderAdapter for AutoBlockAdapter {
        fn id(&self) -> &str {
            "test/auto-block"
        }
        fn supports_durable_blocks(&self) -> bool {
            true
        }
        async fn prepare(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &ModelRequest,
        ) -> Result<serde_json::Value, pi_rs_core::Error> {
            Ok(serde_json::json!({}))
        }
        fn validate(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &ModelRequest,
            _: &serde_json::Value,
        ) -> Result<(), pi_rs_core::Error> {
            Ok(())
        }
        async fn prepare_durable(
            &self,
            _: &crate::model::ProviderAttempt,
            req: &crate::model::DurableRequest,
        ) -> Result<serde_json::Value, pi_rs_core::Error> {
            self.requests.lock().unwrap().push(req.clone());
            Ok(serde_json::json!({}))
        }
        fn validate_durable(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &crate::model::DurableRequest,
            _: &serde_json::Value,
        ) -> Result<(), pi_rs_core::Error> {
            Ok(())
        }
        async fn dispatch(
            &self,
            _: &crate::model::ProviderAttempt,
            _: &ModelRequest,
            _: serde_json::Value,
        ) -> Result<ModelStream, pi_rs_core::Error> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.overflow && n == 0 {
                return Err(pi_rs_core::Error::Model("context length exceeded".into()));
            }
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("answer".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    fn auto_options(model: Arc<AutoBlockModel>) -> Options {
        Options {
            model,
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    #[tokio::test]
    async fn auto_and_overflow_compact_block_history_then_dispatch_retained_blocks() {
        use serde_json::json;
        for overflow in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = Arc::new(AutoBlockModel {
                window: if overflow { None } else { Some(3000) },
                summaries: std::sync::Mutex::new(vec![]),
            });
            let adapter = Arc::new(AutoBlockAdapter {
                calls: AtomicUsize::new(0),
                overflow,
                requests: std::sync::Mutex::new(vec![]),
            });
            let (id, kept_id, history, kept);
            {
                let mut h = Harness::open_paused(&path, auto_options(model.clone())).unwrap();
                h.install_provider_adapter(adapter.clone()).unwrap();
                id = h.create_conversation(config()).unwrap();
                h.select_provider_adapter(id, true).unwrap();
                h.append_user_blocks(
                    id,
                    vec![
                        json!({"type":"text","text":"old"}),
                        json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    ],
                )
                .unwrap();
                kept = vec![
                    json!({"type":"text","text":"keep"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    json!({"type":"text","text":"tail","textSignature":"opaque"}),
                ];
                kept_id = h.append_user_blocks(id, kept.clone()).unwrap();
                let (mut c, version) = h.load(id).unwrap();
                history = h
                    .resolved_occurrences(id, c.transcript.clone().unwrap())
                    .unwrap();
                c.phase = Phase::ModelReady { turns: 0 };
                h.save(&c, Expected::Version(version)).unwrap();
            }
            {
                let mut h = Harness::open_paused(&path, auto_options(model.clone())).unwrap();
                h.install_provider_adapter(adapter.clone()).unwrap();
                h.resume_conversation(id, &CancellationToken::new())
                    .await
                    .unwrap();
                let c = h.conversation(id).unwrap();
                assert_eq!(c.content_blocks.len(), 1, "overflow={overflow}");
                let binding = c.content_blocks.values().next().unwrap();
                assert_eq!(binding.message_id, kept_id);
                assert_eq!(binding.blocks, kept);
                let after = h.resolved_occurrences(id, c.transcript.unwrap()).unwrap();
                assert!(after.iter().any(|entry| *entry == history[1]));
                assert_eq!(model.summaries.lock().unwrap().len(), 1);
                assert_eq!(
                    adapter.calls.load(Ordering::SeqCst),
                    if overflow { 2 } else { 1 }
                );
            }
            let mut h = Harness::open_paused(&path, auto_options(model.clone())).unwrap();
            h.install_provider_adapter(adapter.clone()).unwrap();
            let c = h.conversation(id).unwrap();
            assert_eq!(c.content_blocks.values().next().unwrap().blocks, kept);
            let requests = adapter.requests.lock().unwrap();
            assert!(
                requests
                    .last()
                    .unwrap()
                    .blocks
                    .values()
                    .any(|b| b.message_id == kept_id && b.blocks == kept)
            );
        }
    }

    #[tokio::test]
    async fn automatic_block_compaction_runs_from_public_submission() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(AutoBlockModel {
            window: Some(6000),
            summaries: std::sync::Mutex::new(vec![]),
        });
        let adapter = Arc::new(AutoBlockAdapter {
            calls: AtomicUsize::new(0),
            overflow: false,
            requests: std::sync::Mutex::new(vec![]),
        });
        let mut h =
            Harness::open_paused(dir.path().join("db"), auto_options(model.clone())).unwrap();
        h.install_provider_adapter(adapter.clone()).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.append_user_blocks(id, vec![json!({"type":"text","text":"x".repeat(15000)})])
            .unwrap();
        let kept = vec![
            json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            json!({"type":"text","text":"retained"}),
        ];
        let occurrence = h.append_user_blocks(id, kept.clone()).unwrap();
        let submission = h.submit(id, None, "latest".into()).unwrap();
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        assert!(
            c.submissions
                .iter()
                .any(|s| s.id == submission && matches!(s.status, Status::Done { .. }))
        );
        assert_eq!(model.summaries.lock().unwrap().len(), 1);
        assert!(
            c.content_blocks
                .values()
                .any(|b| b.message_id == occurrence && b.blocks == kept)
        );
        assert!(
            adapter
                .requests
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .blocks
                .values()
                .any(|b| b.message_id == occurrence && b.blocks == kept)
        );
    }

    #[tokio::test]
    async fn automatic_block_compaction_running_reopens_and_abort_prevents_replay() {
        use serde_json::json;
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = Arc::new(AutoBlockModel {
                window: None,
                summaries: std::sync::Mutex::new(vec![]),
            });
            let adapter = Arc::new(AutoBlockAdapter {
                calls: AtomicUsize::new(0),
                overflow: false,
                requests: std::sync::Mutex::new(vec![]),
            });
            let (id, kept_id, kept);
            {
                let mut h = Harness::open_paused(&path, auto_options(model.clone())).unwrap();
                h.install_provider_adapter(adapter.clone()).unwrap();
                id = h.create_conversation(config()).unwrap();
                h.select_provider_adapter(id, true).unwrap();
                h.append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
                    .unwrap();
                kept = vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})];
                kept_id = h.append_user_blocks(id, kept.clone()).unwrap();
                let (mut c, version) = h.load(id).unwrap();
                start_overflow_compaction(&h, &mut c, 0, "context length exceeded".into()).unwrap();
                let Phase::CompactionHook { preparation, .. } = c.phase else {
                    panic!("missing preparation");
                };
                c.phase = Phase::CompactionRunning {
                    preparation,
                    request_id: Uuid::new_v4(),
                    restarts: 0,
                };
                h.save(&c, Expected::Version(version)).unwrap();
                if abort {
                    h.request_abort(id).unwrap();
                }
            }
            let mut h = Harness::open_paused(&path, auto_options(model.clone())).unwrap();
            h.install_provider_adapter(adapter.clone()).unwrap();
            h.resume_conversation(id, &CancellationToken::new())
                .await
                .unwrap();
            let c = h.conversation(id).unwrap();
            assert_eq!(
                c.content_blocks.values().last().unwrap().message_id,
                kept_id
            );
            assert_eq!(c.content_blocks.values().last().unwrap().blocks, kept);
            assert_eq!(
                model.summaries.lock().unwrap().len(),
                if abort { 0 } else { 1 }
            );
            assert_eq!(
                adapter.calls.load(Ordering::SeqCst),
                if abort { 0 } else { 1 }
            );
            assert_eq!(c.content_blocks.len(), if abort { 2 } else { 1 });
        }
    }

    #[tokio::test]
    async fn automatic_block_placement_loses_abort_race() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(AutoBlockModel {
            window: None,
            summaries: std::sync::Mutex::new(vec![]),
        });
        let adapter = Arc::new(AutoBlockAdapter {
            calls: AtomicUsize::new(0),
            overflow: false,
            requests: std::sync::Mutex::new(vec![]),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), auto_options(model)).unwrap();
        h.install_provider_adapter(adapter.clone()).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
            .unwrap();
        let kept = h
            .append_user_blocks(
                id,
                vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
            )
            .unwrap();
        let (mut c, version) = h.load(id).unwrap();
        start_overflow_compaction(&h, &mut c, 0, "overflow".into()).unwrap();
        let Phase::CompactionHook { preparation, .. } = c.phase else {
            panic!("missing preparation");
        };
        let result = preparation
            .finish(CompletedGeneration {
                content: "checkpoint".into(),
                calls: vec![],
                usage: None,
                metadata: Default::default(),
            })
            .unwrap();
        c.phase = Phase::CompactionPlacement {
            result: Box::new(result),
            resets_before: None,
        };
        h.save(&c, Expected::Version(version)).unwrap();
        let (mut candidate, version) = h.load(id).unwrap();
        let Phase::CompactionPlacement { result, .. } = candidate.phase.clone() else {
            panic!("missing result");
        };
        let (system_end, first_kept) = result.retained_range();
        candidate.content_blocks =
            project_compaction_blocks(&candidate.content_blocks, system_end, first_kept).unwrap();
        candidate.messages = result
            .apply_in_context(&candidate.messages, candidate.context_generation)
            .unwrap();
        candidate.context_generation = Some(Uuid::new_v4());
        candidate.phase = Phase::ModelReady { turns: 0 };
        let prepared = h
            .prepare_save(&candidate, Expected::Version(version))
            .unwrap();
        h.request_abort(id).unwrap();
        assert!(matches!(
            h.commit_conversation_save(prepared),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.content_blocks.len(), 2);
        assert_eq!(c.content_blocks[&1].message_id, kept);
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    }
    struct LegacyCompactHook;
    #[async_trait::async_trait]
    impl crate::extension::Hook for LegacyCompactHook {}
    #[test]
    fn automatic_block_preparation_rejects_unproven_or_extension_contexts() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(AutoBlockModel {
            window: None,
            summaries: std::sync::Mutex::new(vec![]),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), auto_options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
            .unwrap();
        h.append_user_blocks(id, vec![json!({"type":"text","text":"new"})])
            .unwrap();
        let (c, _) = h.load(id).unwrap();
        let mut unsupported = c.clone();
        unsupported.occurrence_epoch_proven = false;
        assert!(matches!(
            h.prepare_runtime_compaction(&unsupported, 0, None),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
        h.install_extension(crate::extension::Extension {
            name: "unsupported".into(),
            hooks: vec![Arc::new(LegacyCompactHook)],
            ..Default::default()
        })
        .unwrap();
        let mut unsupported = c;
        unsupported.extensions.push("unsupported".into());
        assert!(matches!(
            h.prepare_runtime_compaction(&unsupported, 0, None),
            Err(Error::Transcript(
                crate::transcript::Error::UnsupportedContentBlocks
            ))
        ));
    }

    struct BlockCompactHook {
        decision: crate::extension::BeforeCompact,
        seen: Arc<std::sync::Mutex<Vec<crate::extension::CompactionInput>>>,
        entered: Arc<Semaphore>,
        wait: bool,
        memo_value: &'static str,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for BlockCompactHook {
        fn supports_compaction_blocks(&self) -> bool {
            true
        }
        async fn before_compact(
            &self,
            input: &crate::extension::CompactionInput,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            self.seen.lock().unwrap().push(input.clone());
            let value = ctx
                .memo
                .get_or_insert("block-compact", serde_json::json!(self.memo_value))
                .await?;
            assert_eq!(value, serde_json::json!("first"));
            self.entered.add_permits(1);
            if self.wait {
                std::future::pending::<()>().await;
            }
            Ok(self.decision.clone())
        }
    }
    fn block_compact_extension(
        hook: Arc<dyn crate::extension::Hook>,
    ) -> crate::extension::Extension {
        crate::extension::Extension {
            name: "block-hook".into(),
            hooks: vec![hook],
            ..Default::default()
        }
    }
    fn block_hook(
        decision: crate::extension::BeforeCompact,
        seen: Arc<std::sync::Mutex<Vec<crate::extension::CompactionInput>>>,
        entered: Arc<Semaphore>,
        wait: bool,
        memo_value: &'static str,
    ) -> Arc<dyn crate::extension::Hook> {
        Arc::new(BlockCompactHook {
            decision,
            seen,
            entered,
            wait,
            memo_value,
        })
    }
    #[tokio::test]
    async fn block_before_compact_decisions_preserve_hook_input_and_retained_history() {
        use crate::extension::BeforeCompact;
        use serde_json::json;
        for decision in [
            BeforeCompact::Pass,
            BeforeCompact::Summary("hook summary".into()),
            BeforeCompact::Decline,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
            let seen = Arc::new(std::sync::Mutex::new(vec![]));
            let entered = Arc::new(Semaphore::new(0));
            let (id, old_id, kept_id, original, before);
            {
                let mut h =
                    Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
                h.install_extension(block_compact_extension(block_hook(
                    decision.clone(),
                    seen.clone(),
                    entered.clone(),
                    false,
                    "first",
                )))
                .unwrap();
                id = h.create_conversation(config()).unwrap();
                h.select_extensions(id, vec!["block-hook".into()]).unwrap();
                original = vec![
                    json!({"type":"text","text":"first"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    json!({"type":"text","text":"last","textSignature":"opaque"}),
                ];
                old_id = h.append_user_blocks(id, original.clone()).unwrap();
                kept_id = h
                    .append_user_blocks(
                        id,
                        vec![json!({"type":"image","data":"Ymll","mimeType":"image/png"})],
                    )
                    .unwrap();
                let c = h.conversation(id).unwrap();
                before = h.resolved_occurrences(id, c.transcript.unwrap()).unwrap();
                h.request_manual_compaction(id, 0, Some("focus"))
                    .unwrap()
                    .unwrap();
                h.resume_conversation(id, &CancellationToken::new())
                    .await
                    .unwrap();
                let c = h.conversation(id).unwrap();
                assert_eq!(
                    c.manual_compactions[0].status,
                    if decision == BeforeCompact::Decline {
                        ManualCompactionStatus::Declined
                    } else {
                        ManualCompactionStatus::Applied
                    }
                );
                let history = h.resolved_occurrences(id, c.transcript.unwrap()).unwrap();
                assert_eq!(history[1], before[1]);
                assert_eq!(c.content_blocks[&1].message_id, kept_id);
                assert_eq!(
                    c.content_blocks.len(),
                    if decision == BeforeCompact::Decline {
                        2
                    } else {
                        1
                    }
                );
            }
            let h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            assert_eq!(
                h.conversation(id).unwrap().content_blocks[&1].message_id,
                kept_id
            );
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].content_blocks[&0].message_id, old_id);
            assert_eq!(seen[0].content_blocks[&0].blocks, original);
            assert_eq!(seen[0].instructions.as_deref(), Some("focus"));
            assert_eq!(
                model.0.lock().unwrap().len(),
                if decision == BeforeCompact::Pass {
                    1
                } else {
                    0
                }
            );
        }
    }
    #[tokio::test]
    async fn block_before_compact_runs_for_threshold_and_overflow() {
        use crate::extension::{BeforeCompact, CompactionReason};
        use serde_json::json;
        for overflow in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let model = Arc::new(AutoBlockModel {
                window: if overflow { None } else { Some(3000) },
                summaries: std::sync::Mutex::new(vec![]),
            });
            let adapter = Arc::new(AutoBlockAdapter {
                calls: AtomicUsize::new(0),
                overflow,
                requests: std::sync::Mutex::new(vec![]),
            });
            let seen = Arc::new(std::sync::Mutex::new(vec![]));
            let entered = Arc::new(Semaphore::new(0));
            let mut h =
                Harness::open_paused(dir.path().join("db"), auto_options(model.clone())).unwrap();
            h.install_provider_adapter(adapter.clone()).unwrap();
            h.install_extension(block_compact_extension(block_hook(
                BeforeCompact::Pass,
                seen.clone(),
                entered,
                false,
                "first",
            )))
            .unwrap();
            let id = h.create_conversation(config()).unwrap();
            h.select_provider_adapter(id, true).unwrap();
            h.select_extensions(id, vec!["block-hook".into()]).unwrap();
            let old = h
                .append_user_blocks(
                    id,
                    vec![
                        json!({"type":"text","text":"old"}),
                        json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    ],
                )
                .unwrap();
            let kept = h
                .append_user_blocks(
                    id,
                    vec![json!({"type":"image","data":"Ymll","mimeType":"image/png"})],
                )
                .unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.phase = Phase::ModelReady { turns: 0 };
            h.save(&c, Expected::Version(version)).unwrap();
            h.resume_conversation(id, &CancellationToken::new())
                .await
                .unwrap();
            let c = h.conversation(id).unwrap();
            assert_eq!(c.content_blocks.values().next().unwrap().message_id, kept);
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(
                seen[0].reason,
                if overflow {
                    CompactionReason::Overflow
                } else {
                    CompactionReason::Threshold
                }
            );
            assert_eq!(seen[0].content_blocks[&0].message_id, old);
            assert_eq!(model.summaries.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn block_before_compact_rechecks_capability_after_admission() {
        use crate::extension::BeforeCompact;
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
        let seen = Arc::new(std::sync::Mutex::new(vec![]));
        let entered = Arc::new(Semaphore::new(0));
        let (id, kept_id);
        {
            let mut h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            h.install_extension(block_compact_extension(block_hook(
                BeforeCompact::Pass,
                seen.clone(),
                entered.clone(),
                false,
                "first",
            )))
            .unwrap();
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["block-hook".into()]).unwrap();
            h.append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
                .unwrap();
            kept_id = h
                .append_user_blocks(
                    id,
                    vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
                )
                .unwrap();
            h.request_manual_compaction(id, 0, None).unwrap().unwrap();
            h.install_extension(block_compact_extension(Arc::new(LegacyCompactHook)))
                .unwrap();
            assert!(matches!(
                h.resume_conversation(id, &CancellationToken::new()).await,
                Err(Error::Transcript(
                    crate::transcript::Error::UnsupportedContentBlocks
                ))
            ));
            assert!(seen.lock().unwrap().is_empty());
        }
        let mut h = Harness::open_paused(&path, block_summary_options(model)).unwrap();
        h.install_extension(block_compact_extension(block_hook(
            BeforeCompact::Summary("safe".into()),
            seen.clone(),
            entered,
            false,
            "first",
        )))
        .unwrap();
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.content_blocks[&1].message_id, kept_id);
        assert_eq!(
            c.manual_compactions[0].status,
            ManualCompactionStatus::Applied
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn block_before_compact_replays_memo_after_reopen_or_honors_abort() {
        use crate::extension::BeforeCompact;
        use serde_json::json;
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
            let seen = Arc::new(std::sync::Mutex::new(vec![]));
            let entered = Arc::new(Semaphore::new(0));
            let (id, old_id, kept_id);
            {
                let mut h =
                    Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
                h.install_extension(block_compact_extension(block_hook(
                    BeforeCompact::Pass,
                    seen.clone(),
                    entered.clone(),
                    true,
                    "first",
                )))
                .unwrap();
                id = h.create_conversation(config()).unwrap();
                h.select_extensions(id, vec!["block-hook".into()]).unwrap();
                old_id = h
                    .append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
                    .unwrap();
                kept_id = h
                    .append_user_blocks(
                        id,
                        vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
                    )
                    .unwrap();
                h.request_manual_compaction(id, 0, None).unwrap().unwrap();
                let stop = CancellationToken::new();
                let trigger = stop.clone();
                let started = entered.clone();
                tokio::spawn(async move {
                    started.acquire().await.unwrap().forget();
                    trigger.cancel();
                });
                assert!(h.resume_conversation(id, &stop).await.is_err());
            }
            {
                let mut h =
                    Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
                h.install_extension(block_compact_extension(block_hook(
                    BeforeCompact::Summary("replayed".into()),
                    seen.clone(),
                    entered.clone(),
                    false,
                    "different",
                )))
                .unwrap();
                if abort {
                    h.request_abort(id).unwrap();
                }
                h.resume_conversation(id, &CancellationToken::new())
                    .await
                    .unwrap();
                let c = h.conversation(id).unwrap();
                assert_eq!(
                    c.manual_compactions[0].status,
                    if abort {
                        ManualCompactionStatus::Aborted
                    } else {
                        ManualCompactionStatus::Applied
                    }
                );
                assert_eq!(c.content_blocks[&1].message_id, kept_id);
                assert_eq!(c.content_blocks.len(), if abort { 2 } else { 1 });
            }
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), if abort { 1 } else { 2 });
            if !abort {
                assert_eq!(seen[0], seen[1]);
            }
            assert!(
                seen.iter()
                    .all(|input| input.content_blocks[&0].message_id == old_id)
            );
            assert!(model.0.lock().unwrap().is_empty());
        }
    }

    struct BlockSummaryModel(std::sync::Mutex<Vec<ModelRequest>>);
    #[async_trait::async_trait]
    impl Model for BlockSummaryModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            panic!("summary route required")
        }
        async fn stream_for_summary(
            &self,
            _: Uuid,
            _: Uuid,
            request: ModelRequest,
        ) -> Result<ModelStream, pi_rs_core::Error> {
            self.0.lock().unwrap().push(request);
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("checkpoint".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    fn block_summary_options(model: Arc<BlockSummaryModel>) -> Options {
        Options {
            model,
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    #[tokio::test]
    async fn manual_block_compaction_projects_summary_and_preserves_retained_provenance_after_reopen()
     {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
        let (id, kept_id, before_history, kept, fork);
        {
            let mut h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            id = h.create_conversation(config()).unwrap();
            h.append_user_blocks(
                id,
                vec![
                    json!({"type":"text","text":"old one"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    json!({"type":"text","text":"old two"}),
                ],
            )
            .unwrap();
            kept = vec![
                json!({"type":"text","text":"keep one"}),
                json!({"type":"image","data":"Ymll","mimeType":"image/png"}),
                json!({"type":"text","text":"keep two","textSignature":"opaque"}),
            ];
            kept_id = h.append_user_blocks(id, kept.clone()).unwrap();
            let c = h.conversation(id).unwrap();
            before_history = h.resolved_occurrences(id, c.transcript.unwrap()).unwrap();
            h.request_manual_compaction(id, 0, None).unwrap().unwrap();
        }
        {
            let mut h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            h.resume_conversation(id, &CancellationToken::new())
                .await
                .unwrap();
            let c = h.conversation(id).unwrap();
            assert_eq!(
                c.manual_compactions[0].status,
                ManualCompactionStatus::Applied
            );
            assert_eq!(c.content_blocks.len(), 1);
            assert_eq!(c.content_blocks[&1].message_id, kept_id);
            assert_eq!(c.content_blocks[&1].blocks, kept);
            let history = h.resolved_occurrences(id, c.transcript.unwrap()).unwrap();
            assert_eq!(history[1], before_history[1]);
            fork = h.fork(id).unwrap();
            let forked = h.conversation(fork).unwrap();
            assert_eq!(forked.content_blocks[&1].message_id, kept_id);
            assert_eq!(
                h.resolved_occurrences(fork, forked.transcript.unwrap())
                    .unwrap()[1],
                before_history[1]
            );
        }
        let h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
        assert_eq!(h.conversation(id).unwrap().content_blocks[&1].blocks, kept);
        assert_eq!(
            h.conversation(fork).unwrap().content_blocks[&1].blocks,
            kept
        );
        let calls = model.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(
            matches!(&calls[0].messages[1], Message::User { content } if content.contains("[User]: old one\nold two") && !content.contains("aGk=") && !content.contains("keep one"))
        );
    }

    fn block_placement(h: &mut Harness) -> (Uuid, Uuid, Uuid) {
        use serde_json::json;
        let id = h.create_conversation(config()).unwrap();
        h.append_user_blocks(id, vec![json!({"type":"text","text":"old"})])
            .unwrap();
        let kept = h
            .append_user_blocks(
                id,
                vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
            )
            .unwrap();
        let operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
        let (mut c, version) = h.load(id).unwrap();
        let Phase::CompactionHook { preparation, .. } = c.phase else {
            panic!("missing hook");
        };
        let result = preparation
            .finish(CompletedGeneration {
                content: "checkpoint".into(),
                calls: vec![],
                usage: None,
                metadata: Default::default(),
            })
            .unwrap();
        c.phase = Phase::CompactionPlacement {
            result: Box::new(result),
            resets_before: Some(vec![]),
        };
        h.save(&c, Expected::Version(version)).unwrap();
        (id, operation, kept)
    }
    #[tokio::test]
    async fn block_compaction_abort_conflicts_with_prepared_placement_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
        let (id, operation, kept);
        {
            let mut h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            (id, operation, kept) = block_placement(&mut h);
            let (mut candidate, version) = h.load(id).unwrap();
            let Phase::CompactionPlacement { result, .. } = candidate.phase.clone() else {
                panic!("missing result");
            };
            let mut stale = candidate.clone();
            stale.content_blocks.get_mut(&1).unwrap().blocks.clear();
            assert!(
                h.apply_manual_compaction_target(&stale, &result)
                    .unwrap()
                    .is_none()
            );
            let mut stale = candidate.clone();
            stale.context_generation = Some(Uuid::new_v4());
            assert!(
                h.apply_manual_compaction_target(&stale, &result)
                    .unwrap()
                    .is_none()
            );
            let (messages, (system_end, first_kept)) = h
                .apply_manual_compaction_target(&candidate, &result)
                .unwrap()
                .unwrap();
            candidate.content_blocks =
                project_compaction_blocks(&candidate.content_blocks, system_end, first_kept)
                    .unwrap();
            candidate.messages = messages;
            candidate.context_generation = Some(Uuid::new_v4());
            finish_manual_compaction(&mut candidate, ManualCompactionStatus::Applied).unwrap();
            candidate.phase = Phase::Idle;
            let mut forged = candidate.clone();
            forged.content_blocks.clear();
            assert!(h.prepare_save(&forged, Expected::Version(version)).is_err());
            let prepared = h
                .prepare_save(&candidate, Expected::Version(version))
                .unwrap();
            h.request_abort(id).unwrap();
            assert!(matches!(
                h.commit_conversation_save(prepared),
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
            ));
        }
        let mut h = Harness::open_paused(&path, block_summary_options(model)).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Aborted
            }
        );
        assert_eq!(c.content_blocks[&1].message_id, kept);
        assert_eq!(c.content_blocks.len(), 2);
    }
    #[tokio::test]
    async fn block_compaction_reset_before_result_is_stale_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
        let (id, operation);
        {
            let mut h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            (id, operation, _) = block_placement(&mut h);
            let reset = h.reset(id, Some("replacement".into())).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            let Phase::CompactionPlacement { resets_before, .. } = &mut c.phase else {
                panic!("missing result");
            };
            *resets_before = Some(vec![reset]);
            h.save(&c, Expected::Version(version)).unwrap();
        }
        let mut h = Harness::open_paused(&path, block_summary_options(model)).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Stale
            }
        );
        assert!(c.content_blocks.is_empty());
        assert_eq!(
            c.messages,
            vec![Message::User {
                content: "replacement".into()
            }]
        );
    }

    #[tokio::test]
    async fn block_compaction_queues_two_block_resets_before_stale_result() {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(BlockSummaryModel(std::sync::Mutex::new(vec![])));
        let (id, operation, first, second, first_id, second_id);
        {
            let mut h = Harness::open_paused(&path, block_summary_options(model.clone())).unwrap();
            (id, operation, _) = block_placement(&mut h);
            let (r1, ids1) = h
                .reset_with_blocks(id, None, vec![vec![json!({"type":"text","text":"first"})]])
                .unwrap();
            let (r2, ids2) = h
                .reset_with_blocks(id, None, vec![vec![json!({"type":"text","text":"second"})]])
                .unwrap();
            (first, second, first_id, second_id) = (r1, r2, ids1[0], ids2[0]);
            let (mut c, version) = h.load(id).unwrap();
            let Phase::CompactionPlacement { resets_before, .. } = &mut c.phase else {
                panic!("missing result")
            };
            *resets_before = Some(vec![first, second]);
            h.save(&c, Expected::Version(version)).unwrap();
        }
        let mut h = Harness::open_paused(&path, block_summary_options(model)).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Stale
            }
        );
        assert_eq!(
            c.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(c.content_blocks[&0].message_id, second_id);
        let fork = h.fork_by_entry_id(id, first_id).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().content_blocks[&0].message_id,
            first_id
        );
    }

    #[tokio::test]
    async fn merged_watch_tracks_source_capable_worker_but_not_reader_clones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = placement_model();
        let mut h = Harness::open_paused(&path, options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let mut worker = h.worker();
        let weak = Arc::downgrade(h.merged_source.as_ref().unwrap());
        let active_callback = weak.upgrade().unwrap();
        let mut watch = h
            .watch_merged_progress_handle(
                id,
                None,
                std::num::NonZeroUsize::MIN,
                Duration::from_millis(1),
            )
            .unwrap();
        let mut other_reader = h
            .watch_view(id, std::num::NonZeroUsize::MIN, Duration::from_millis(1))
            .unwrap();
        drop(h);
        assert!(
            tokio::time::timeout(Duration::from_millis(15), watch.next())
                .await
                .is_err()
        );
        worker.reset(id, Some("live".into())).unwrap();
        assert!(watch.next().await.unwrap().is_some());
        drop(worker);
        assert!(
            tokio::time::timeout(Duration::from_millis(15), watch.source_closed())
                .await
                .is_err()
        );
        drop(active_callback);
        assert!(weak.upgrade().is_none());
        assert!(
            tokio::time::timeout(Duration::from_secs(2), watch.next())
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        assert_eq!(watch.end_reason(), Some(&ViewWatchEnd::Ended));
        other_reader.close();
    }

    fn placement_model() -> Arc<GatedModel> {
        Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        })
    }
    fn prepare_manual_placement(h: &mut Harness) -> (Uuid, Uuid) {
        let id = h.create_conversation(config()).unwrap();
        let (mut c, version) = h.load(id).unwrap();
        c.messages = vec![
            Message::User {
                content: "old".into(),
            },
            Message::User {
                content: "new".into(),
            },
        ];
        h.save(&c, Expected::Version(version)).unwrap();
        let operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
        let (mut c, version) = h.load(id).unwrap();
        let Phase::CompactionHook { preparation, .. } = c.phase else {
            panic!("missing preparation")
        };
        let result = preparation
            .finish(CompletedGeneration {
                content: "checkpoint".into(),
                calls: vec![],
                usage: None,
                metadata: Default::default(),
            })
            .unwrap();
        c.phase = Phase::CompactionPlacement {
            result: Box::new(result),
            resets_before: Some(vec![]),
        };
        h.save(&c, Expected::Version(version)).unwrap();
        (id, operation)
    }
    #[test]
    fn manual_placement_process_child() {
        let Ok(path) = std::env::var("PI_TEST_PLACEMENT_DB") else {
            return;
        };
        let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
        let (id, operation) = prepare_manual_placement(&mut h);
        std::fs::write(format!("{path}.ready"), format!("{id} {operation}")).unwrap();
        loop {
            std::thread::park();
        }
    }
    #[tokio::test]
    async fn manual_placement_survives_process_death() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let ready = dir.path().join("db.ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::scheduler_tests::manual_placement_process_child",
            ])
            .env("PI_TEST_PLACEMENT_DB", &path)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !ready.exists() {
            if std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not commit placement")
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let ids = std::fs::read_to_string(ready).unwrap();
        let mut ids = ids
            .split_whitespace()
            .map(|id| Uuid::parse_str(id).unwrap());
        let id = ids.next().unwrap();
        let operation = ids.next().unwrap();
        let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Applied
            }
        );
        assert!(
            c.messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content.contains("checkpoint")))
        );
    }
    #[tokio::test]
    async fn manual_placement_settles_stale_or_abort_after_reopen() {
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (id, operation);
            {
                let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
                (id, operation) = prepare_manual_placement(&mut h);
                if abort {
                    h.request_abort(id).unwrap();
                } else {
                    let (mut c, version) = h.load(id).unwrap();
                    let old = c.messages.clone();
                    place_reset(&mut c, crate::reset::Reset::new(Uuid::new_v4(), None));
                    c.messages = old;
                    h.save(&c, Expected::Version(version)).unwrap();
                }
            }
            let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
            h.resume(&CancellationToken::new()).await.unwrap();
            let c = h.conversation(id).unwrap();
            assert_eq!(
                c.manual_compactions[0],
                ManualCompaction {
                    id: operation,
                    status: if abort {
                        ManualCompactionStatus::Aborted
                    } else {
                        ManualCompactionStatus::Stale
                    }
                }
            );
            assert_eq!(
                c.messages,
                vec![
                    Message::User {
                        content: "old".into()
                    },
                    Message::User {
                        content: "new".into()
                    }
                ]
            );
        }
    }
    #[tokio::test]
    async fn reset_after_aborted_compaction_preserves_pending_reset_and_write_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (id, operation, first, second, write);
        {
            let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
            (id, operation) = prepare_manual_placement(&mut h);
            first = h.reset(id, Some("first".into())).unwrap();
            write = h.submit_write(id, None, "note".into(), None).unwrap();
            h.request_abort(id).unwrap();
            let (c, version) = h.load(id).unwrap();
            assert!(
                h.settle_abort(c, version, &CancellationToken::new())
                    .await
                    .unwrap()
            );
            second = h.reset(id, Some("second".into())).unwrap();
        }
        let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.manual_compactions[0].id, operation);
        assert_eq!(
            c.manual_compactions[0].status,
            ManualCompactionStatus::Aborted
        );
        assert_eq!(
            c.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(
            c.messages,
            vec![Message::User {
                content: "second".into()
            }]
        );
        let entry = c
            .application_writes
            .iter()
            .find(|entry| entry.id == write)
            .unwrap();
        assert_eq!(entry.after_reset, Some(first));
        assert!(entry.placed);
    }
    #[tokio::test]
    async fn reset_legacy_placement_without_order_retains_previous_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (id, operation);
        {
            let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
            (id, operation) = prepare_manual_placement(&mut h);
            h.reset(id, Some("later".into())).unwrap();
            let (c, version) = h.load(id).unwrap();
            let mut value = serde_json::to_value(c).unwrap();
            value["phase"]["CompactionPlacement"]
                .as_object_mut()
                .unwrap()
                .remove("resets_before");
            let legacy: Conversation = serde_json::from_value(value).unwrap();
            h.save(&legacy, Expected::Version(version)).unwrap();
        }
        let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(c.manual_compactions[0].id, operation);
        assert_eq!(
            c.manual_compactions[0].status,
            ManualCompactionStatus::Applied
        );
        assert_eq!(
            c.messages,
            vec![Message::User {
                content: "later".into()
            }]
        );
    }
    #[tokio::test]
    async fn reset_after_manual_placement_admission_does_not_make_result_stale() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (id, operation);
        {
            let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
            (id, operation) = prepare_manual_placement(&mut h);
            h.reset(id, Some("later reset".into())).unwrap();
        }
        let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Applied
            }
        );
        assert_eq!(
            c.messages,
            vec![Message::User {
                content: "later reset".into()
            }]
        );
    }
    #[tokio::test]
    async fn reset_racing_manual_result_commit_is_ordered_before_result() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (id, operation);
        {
            let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
            id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.messages = vec![
                Message::User {
                    content: "old".into(),
                },
                Message::User {
                    content: "new".into(),
                },
            ];
            h.save(&c, Expected::Version(version)).unwrap();
            operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
            let mut result_worker = h.worker();
            let mut reset_worker = h.worker();
            let (mut snapshot, version) = result_worker.load(id).unwrap();
            let Phase::CompactionHook { preparation, .. } = snapshot.phase else {
                panic!("missing preparation")
            };
            let result = preparation
                .finish(CompletedGeneration {
                    content: "checkpoint".into(),
                    calls: vec![],
                    usage: None,
                    metadata: Default::default(),
                })
                .unwrap();
            snapshot.phase = Phase::CompactionPlacement {
                result: Box::new(result),
                resets_before: Some(vec![]),
            };
            let reset = reset_worker.reset(id, Some("first reset".into())).unwrap();
            result_worker
                .save(&snapshot, Expected::Version(version))
                .unwrap();
            let committed = result_worker.conversation(id).unwrap();
            assert!(
                matches!(committed.phase, Phase::CompactionPlacement { resets_before: Some(ref ids), .. } if ids == &vec![reset])
            );
        }
        let mut h = Harness::open_paused(&path, options(placement_model())).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Stale
            }
        );
        assert_eq!(
            c.messages,
            vec![Message::User {
                content: "first reset".into()
            }]
        );
    }
    #[tokio::test]
    async fn manual_placement_conflicts_with_concurrent_abort() {
        let dir = tempfile::tempdir().unwrap();
        let mut h =
            Harness::open_paused(dir.path().join("db"), options(placement_model())).unwrap();
        let (id, operation) = prepare_manual_placement(&mut h);
        let (mut candidate, version) = h.load(id).unwrap();
        let Phase::CompactionPlacement { result, .. } = candidate.phase.clone() else {
            panic!("missing placement")
        };
        candidate.messages = result
            .apply_in_context(&candidate.messages, candidate.context_generation)
            .unwrap();
        candidate.context_generation = Some(Uuid::new_v4());
        finish_manual_compaction(&mut candidate, ManualCompactionStatus::Applied).unwrap();
        candidate.phase = Phase::Idle;
        let prepared = h
            .prepare_save(&candidate, Expected::Version(version))
            .unwrap();
        h.request_abort(id).unwrap();
        assert!(matches!(
            h.commit_conversation_save(prepared),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        assert_eq!(
            c.manual_compactions[0],
            ManualCompaction {
                id: operation,
                status: ManualCompactionStatus::Aborted
            }
        );
        assert!(
            !c.messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content.contains("checkpoint")))
        );
    }
    #[test]
    fn compact_context_identity_survives_reopen_and_rejects_reset_aba() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let id;
        {
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.messages = vec![
                Message::User {
                    content: "old".into(),
                },
                Message::User {
                    content: "new".into(),
                },
            ];
            h.save(&c, Expected::Version(version)).unwrap();
            assert!(h.request_compaction(id, 0).unwrap());
        }
        let (result, original, generation);
        {
            let h = Harness::open_paused(&path, options(model.clone())).unwrap();
            let c = h.conversation(id).unwrap();
            generation = c.context_generation;
            original = c.messages.clone();
            let Phase::CompactionHook { preparation, .. } = c.phase else {
                panic!("missing preparation")
            };
            result = preparation
                .finish(CompletedGeneration {
                    content: "summary".into(),
                    calls: vec![],
                    usage: None,
                    metadata: Default::default(),
                })
                .unwrap();
            assert!(result.apply_in_context(&original, generation).is_ok());
        }
        {
            let mut h = Harness::open_paused(&path, options(model)).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            place_reset(&mut c, crate::reset::Reset::new(Uuid::new_v4(), None));
            // Recreating identical text after the reset must not revive the cut.
            c.messages = original.clone();
            h.save(&c, Expected::Version(version)).unwrap();
        }
        let h = Harness::open_paused(
            &path,
            options(Arc::new(GatedModel {
                started: Arc::new(Semaphore::new(0)),
                release: Arc::new(Semaphore::new(0)),
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
            })),
        )
        .unwrap();
        let c = h.conversation(id).unwrap();
        assert_ne!(c.context_generation, generation);
        assert_eq!(c.messages, original);
        assert!(
            result
                .apply_in_context(&c.messages, c.context_generation)
                .is_err()
        );
    }
    #[test]
    fn context_identity_fences_prepared_save_and_fork() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (mut original, version) = h.load(id).unwrap();
        original.messages = vec![Message::User {
            content: "same".into(),
        }];
        h.save(&original, Expected::Version(version)).unwrap();
        let child = h.fork(id).unwrap();
        let forked = h.conversation(child).unwrap();
        let (base, version) = h.load(id).unwrap();
        assert_eq!(base.messages, forked.messages);
        assert_ne!(base.context_generation, forked.context_generation);
        let mut changed_identity = base.clone();
        changed_identity.context_generation = forked.context_generation;
        assert!(!admission_only(&base, &changed_identity).unwrap());

        let mut stale = base.clone();
        stale.messages.push(Message::User {
            content: "stale".into(),
        });
        let prepared = h.prepare_save(&stale, Expected::Version(version)).unwrap();
        let mut reset = base.clone();
        place_reset(&mut reset, crate::reset::Reset::new(Uuid::new_v4(), None));
        reset.messages = base.messages.clone();
        assert!(!admission_only(&base, &reset).unwrap());
        h.save(&reset, Expected::Version(version)).unwrap();
        assert!(matches!(
            h.commit_conversation_save(prepared),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        let latest = h.conversation(id).unwrap();
        assert_eq!(latest.messages, base.messages);
        assert_eq!(latest.context_generation, reset.context_generation);
    }
    #[tokio::test]
    async fn applied_hook_compaction_rotates_context_identity_after_reopen() {
        struct SummaryHook;
        #[async_trait::async_trait]
        impl crate::extension::Hook for SummaryHook {
            async fn before_compact(
                &self,
                _: &crate::extension::CompactionInput,
                _: &crate::extension::Context<'_>,
            ) -> Result<crate::extension::BeforeCompact, String> {
                Ok(crate::extension::BeforeCompact::Summary("summary".into()))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let id;
        let before;
        {
            let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
            h.install_extension(crate::extension::Extension {
                name: "summary".into(),
                hooks: vec![Arc::new(SummaryHook)],
                ..Default::default()
            })
            .unwrap();
            id = h.create_conversation(config()).unwrap();
            h.select_extensions(id, vec!["summary".into()]).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.messages = vec![
                Message::User {
                    content: "old".into(),
                },
                Message::User {
                    content: "new".into(),
                },
            ];
            before = c.context_generation;
            h.save(&c, Expected::Version(version)).unwrap();
            assert!(h.request_compaction(id, 0).unwrap());
            h.resume(&CancellationToken::new()).await.unwrap();
        }
        let h = Harness::open_paused(&path, options(model)).unwrap();
        let c = h.conversation(id).unwrap();
        assert_ne!(c.context_generation, before);
        assert!(
            c.messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content.contains("summary")))
        );
    }
    struct GatedPanicModel {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }
    #[async_trait::async_trait]
    impl Model for GatedPanicModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            panic!("test worker failure")
        }
    }

    #[tokio::test]
    async fn queued_retry_does_not_hide_worker_failure() {
        let dir = tempfile::tempdir().unwrap();
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let mut opts = options(Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        }));
        opts.model = Arc::new(GatedPanicModel {
            started: started.clone(),
            release: release.clone(),
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), opts).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        let submission = scheduler
            .submit(id, Some("duplicate".into()), "hello".into())
            .unwrap();
        started.acquire().await.unwrap().forget();

        // Queue a lock holder before the duplicate submission schedules its retry.
        let lock = scheduler
            .harness
            .owner
            .conversation_controls()
            .get(id)
            .worker;
        let mut holder = Box::pin(lock.lock());
        assert!(matches!(
            futures_util::poll!(&mut holder),
            std::task::Poll::Pending
        ));
        assert_eq!(
            scheduler
                .submit(id, Some("duplicate".into()), "ignored".into())
                .unwrap(),
            submission
        );
        release.add_permits(1);
        let guard = holder.await;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.wait_for_submission(id, submission, &CancellationToken::new()),
        )
        .await
        .expect("failure must remain observable while retry is queued");
        assert!(
            matches!(result, Err(Error::Invalid(message)) if message.contains("worker panicked"))
        );
        scheduler.cancel.cancel();
        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), async {
            while scheduler.tasks[&id].iter().any(|task| !task.is_finished()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            scheduler
                .worker_errors
                .lock()
                .unwrap()
                .get(&id)
                .map(String::as_str),
            Some("worker panicked")
        );
        assert!(scheduler.shutdown().await.is_err());
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_queued_worker_lock() {
        let dir = tempfile::tempdir().unwrap();
        let mut scheduler =
            Scheduler::open(dir.path().join("db"), options(placement_model())).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        let lock = scheduler
            .harness
            .owner
            .conversation_controls()
            .get(id)
            .worker;
        let guard = lock.lock().await;
        scheduler.schedule(id);
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(2), scheduler.shutdown())
            .await
            .expect("shutdown waited for a queued worker")
            .unwrap();
        drop(guard);
    }

    #[tokio::test]
    async fn submission_wait_reports_scheduler_stop() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), options(model.clone())).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        let submission = scheduler.submit(id, None, "hello".into()).unwrap();
        model.started.acquire().await.unwrap().forget();
        let cancel = CancellationToken::new();
        let mut wait = Box::pin(scheduler.wait_for_submission(id, submission, &cancel));
        assert!(matches!(
            futures_util::poll!(&mut wait),
            std::task::Poll::Pending
        ));
        scheduler.cancel.cancel();
        assert!(matches!(wait.await, Err(Error::Interrupted)));
        scheduler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn idle_wait_includes_pending_reset_at_idle_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), options(model)).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        let (mut state, version) = scheduler.harness.load(id).unwrap();
        state
            .pending_resets
            .push(crate::reset::Reset::new(Uuid::new_v4(), None));
        scheduler
            .harness
            .save(&state, Expected::Version(version))
            .unwrap();
        let cancel = CancellationToken::new();
        let mut wait = Box::pin(scheduler.wait_for_idle(id, &cancel));
        assert!(matches!(
            futures_util::poll!(&mut wait),
            std::task::Poll::Pending
        ));
        cancel.cancel();
        assert!(matches!(wait.await, Err(Error::WaitCancelled)));
        scheduler.shutdown().await.unwrap();
    }

    #[test]
    fn idle_wait_does_not_block_executor_on_task_gate() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let path = dir.path().join("db");
        let cancel = CancellationToken::new();
        let cancellation = cancel.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let (owner_tx, owner_rx) = std::sync::mpsc::channel();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut scheduler = Scheduler::open(path, options(model)).unwrap();
                let id = scheduler.create_conversation(config()).unwrap();
                let (mut state, version) = scheduler.harness.load(id).unwrap();
                state
                    .pending_resets
                    .push(crate::reset::Reset::new(Uuid::new_v4(), None));
                scheduler
                    .harness
                    .save(&state, Expected::Version(version))
                    .unwrap();
                owner_tx.send(scheduler.harness.owner.clone()).unwrap();
                start_rx.recv().unwrap();
                let mut wait = Box::pin(scheduler.wait_for_idle(id, &cancellation));
                let pending = matches!(futures_util::poll!(&mut wait), std::task::Poll::Pending);
                tx.send(pending).unwrap();
                let result = (&mut wait).await;
                done_tx
                    .send(matches!(result, Err(Error::WaitCancelled)))
                    .unwrap();
                drop(wait);
                scheduler.shutdown().await.unwrap();
                result
            })
        });
        let owner = owner_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let gate = owner.task_gate().lock().unwrap();
        start_tx.send(()).unwrap();
        let observed = rx.recv_timeout(Duration::from_secs(2));
        cancel.cancel();
        let cancelled_while_locked = done_rx.recv_timeout(Duration::from_secs(2));
        drop(gate);
        let result = thread.join().unwrap();
        assert!(observed.unwrap(), "poll must not block on the task gate");
        assert!(cancelled_while_locked.unwrap());
        assert!(matches!(result, Err(Error::WaitCancelled)));
    }
    #[test]
    fn extension_report_replay_keeps_first_batch() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let path = dir.path().join("db");
        let conversation = Uuid::new_v4();
        let operation = Uuid::new_v4();
        let report = |error: &str| {
            vec![crate::extension::Report {
                extension: "x".into(),
                operation: "generation".into(),
                error: error.into(),
            }]
        };
        {
            let h = Harness::open_paused(&path, options(model.clone())).unwrap();
            h.record_extension_reports(conversation, operation, "AfterRun", report("first"))
                .unwrap();
        }
        let h = Harness::open_paused(&path, options(model)).unwrap();
        h.record_extension_reports(conversation, operation, "AfterRun", report("second"))
            .unwrap();
        let reports = h.extension_reports().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].reports[0].error, "first");
    }
    #[tokio::test]
    async fn blocked_panic_report_is_recovered_before_resume_or_reconciliation() {
        for reconcile in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let model = Arc::new(GatedModel {
                started: Arc::new(Semaphore::new(0)),
                release: Arc::new(Semaphore::new(0)),
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
            });
            let operation = Uuid::new_v4();
            let id;
            {
                let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
                id = h.create_conversation(config()).unwrap();
                let (mut c, version) = h.load(id).unwrap();
                c.phase = Phase::Blocked {
                    tool_panicked: true,
                    turns: 0,
                    index: 0,
                    operation_id: operation,
                    calls: vec![ToolCall {
                        id: "call".into(),
                        name: "tool".into(),
                        arguments: "{}".into(),
                    }],
                    reason: "uncertain".into(),
                    arguments: None,
                };
                h.save(&c, Expected::Version(version)).unwrap();
                assert!(h.extension_reports().unwrap().is_empty());
            }
            let mut h = Harness::open_paused(&path, options(model)).unwrap();
            if reconcile {
                h.reconcile(id, operation, Outcome::Succeeded("ok".into()))
                    .unwrap();
            } else {
                h.resume_conversation(id, &CancellationToken::new())
                    .await
                    .unwrap();
                h.resume_conversation(id, &CancellationToken::new())
                    .await
                    .unwrap();
            }
            let reports = h.extension_reports().unwrap();
            assert_eq!(reports.len(), 1);
            assert_eq!(reports[0].operation_id, operation);
            assert_eq!(reports[0].phase, "tool_execution_panic");
        }
    }

    #[test]
    fn indexed_panic_prevents_replay_before_blocked_phase_commit() {
        struct SafeTool;
        #[async_trait::async_trait]
        impl Tool for SafeTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "tool".into(),
                    description: String::new(),
                    parameters: serde_json::json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: serde_json::Value, _: pi_rs_core::ToolContext) -> Outcome {
                panic!("recovery must not dispatch")
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let opts = || {
            let mut opts = options(model.clone());
            opts.tools.push(RegisteredTool {
                tool: Arc::new(SafeTool),
                replay: ReplayPolicy::Safe,
            });
            opts
        };
        let operation = Uuid::new_v4();
        let id;
        let cursor;
        {
            let mut h = Harness::open_paused(&path, opts()).unwrap();
            id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.phase = Phase::ToolRunning {
                turns: 0,
                calls: vec![ToolCall {
                    id: "call".into(),
                    name: "tool".into(),
                    arguments: "{}".into(),
                }],
                index: 0,
                operation_id: operation,
                replay: ReplayPolicy::Safe,
                arguments: serde_json::json!({}),
                restarts: 0,
                replay_ready: false,
                dispatch_identity: Some(DispatchIdentity::Other),
            };
            h.save(&c, Expected::Version(version)).unwrap();
            h.record_tool_panic(id, operation).unwrap();
            cursor = h
                .subscribe_extension_reports()
                .unwrap()
                .read(0, 10)
                .unwrap()[0]
                .cursor;
        }
        for _ in 0..2 {
            let h = Harness::open_paused(&path, opts()).unwrap();
            assert!(matches!(
                h.load(id).unwrap().0.phase,
                Phase::Blocked { tool_panicked: true, operation_id, .. } if operation_id == operation
            ));
            let sub = h.subscribe_extension_reports().unwrap();
            assert!(sub.read(cursor, 10).unwrap().is_empty());
            assert_eq!(sub.read(0, 10).unwrap().len(), 1);
        }
    }

    #[test]
    fn dispatcher_missing_runtime_does_not_poison_harness() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        assert!(matches!(
            h.dispatch_extension_reports(0, Duration::from_secs(1), |_| async { Ok(()) }),
            Err(Error::Invalid(_))
        ));
        assert!(!h.storage_failed.load(Ordering::Acquire));
        assert!(
            h.subscribe_extension_reports()
                .unwrap()
                .read(0, 1)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn report_migration_storage_failure_poisons_harness() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let h = Harness::open_paused(&path, options(model)).unwrap();
        let conversation_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let batch = ExtensionReportBatch {
            conversation_id,
            operation_id,
            phase: "phase".into(),
            reports: vec![],
        };
        let key =
            format!("runtime/extension-report/{conversation_id}/{operation_id}/phase").into_bytes();
        h.owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&batch).unwrap(),
                }],
            )
            .unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TRIGGER fail_report_index BEFORE INSERT ON durable_store_entries BEGIN SELECT RAISE(FAIL, 'injected failure'); END;").unwrap();
        assert!(matches!(
            h.subscribe_extension_reports(),
            Err(Error::Storage(_))
        ));
        assert!(h.storage_failed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn report_subscription_drop_allows_scheduler_shutdown_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let scheduler = Scheduler::open(&path, options(model.clone())).unwrap();
        scheduler
            .harness
            .record_extension_reports(
                Uuid::new_v4(),
                Uuid::new_v4(),
                "phase",
                vec![crate::extension::Report {
                    extension: "test".into(),
                    operation: "hook".into(),
                    error: "error".into(),
                }],
            )
            .unwrap();
        let sub = scheduler.subscribe_extension_reports().unwrap();
        let cursor = sub.read(0, 1).unwrap()[0].cursor;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                sub.next(cursor, 1, Duration::from_secs(60))
            )
            .await
            .is_err()
        );
        scheduler.shutdown().await.unwrap();
        let h = Harness::open_paused(&path, options(model)).unwrap();
        assert_eq!(
            h.subscribe_extension_reports()
                .unwrap()
                .read(0, 1)
                .unwrap()
                .len(),
            1
        );
    }
    #[test]
    fn legacy_conversation_defaults_and_independent_boundary_selection() {
        let json = serde_json::json!({
            "id": Uuid::new_v4(), "config": config(), "messages": [],
            "submissions": [], "active": null, "phase": "Idle"
        });
        let mut c: Conversation = serde_json::from_value(json).unwrap();
        assert!(c.manual_compactions.is_empty());
        assert_eq!(c.active_manual_compaction, None);
        assert_eq!(c.queue_modes, QueueModes::default());
        c.queue_modes = QueueModes {
            steering_mode: QueueMode::All,
            follow_up_mode: QueueMode::OneAtATime,
        };
        for (content, mode) in [
            ("f1", InputMode::FollowUp),
            ("s1", InputMode::Steer),
            ("f2", InputMode::FollowUp),
            ("s2", InputMode::Steer),
        ] {
            let id = Uuid::new_v4();
            c.input_modes.insert(id, mode);
            c.submissions.push(Submission {
                id,
                kind: SubmissionKind::Input,
                request_id: None,
                content: content.into(),
                image_notes: Vec::new(),
                input_blocks: None,
                status: Status::Queued,
            });
        }
        assert!(place_queued(&mut c, true));
        let contents: Vec<_> = c
            .submissions
            .iter()
            .filter(|s| s.status == Status::Running)
            .map(|s| s.content.as_str())
            .collect();
        assert_eq!(contents, ["f1", "s1", "s2"]);
        assert_eq!(c.submissions[2].status, Status::Queued);
        let encoded = serde_json::to_value(c.queue_modes).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({"steeringMode":"all", "followUpMode":"one-at-a-time"})
        );
    }

    #[test]
    fn stale_worker_cannot_overwrite_manual_compaction_admission() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        state.messages = vec![
            Message::User {
                content: "old".into(),
            },
            Message::Assistant {
                content: "answer".into(),
                calls: vec![],
                replay: None,
            },
            Message::User {
                content: "new".into(),
            },
        ];
        h.save(&state, Expected::Version(version)).unwrap();
        let mut worker = h.worker();
        let (stale, version) = worker.load(id).unwrap();
        let operation = h.request_manual_compaction(id, 0, None).unwrap().unwrap();
        assert!(matches!(
            worker.save(&stale, Expected::Version(version)),
            Err(Error::Invalid(_))
        ));
        let saved = h.conversation(id).unwrap();
        assert_eq!(saved.active_manual_compaction, Some(operation));
        assert_eq!(
            saved.manual_compactions[0].status,
            ManualCompactionStatus::Running
        );
    }

    #[test]
    fn conflict_merge_rejects_non_admission_state_changes() {
        let base = Conversation {
            id: Uuid::new_v4(),
            config: config(),
            queue_modes: QueueModes::default(),
            after_failed_run: AfterFailedRun::default(),
            extensions: vec![],
            provider_adapter: None,
            allowed_tools: None,
            model_tools: None,
            tool_filters: ToolFilters::default(),
            tool_results: Vec::new(),
            pending_tool_result: None,
            messages: vec![],
            content_blocks: Default::default(),
            block_reset_proof: None,
            block_fork_cut: None,
            context_generation: Some(Uuid::new_v4()),
            occurrence_epoch_proven: true,
            transcript: None,
            submissions: vec![],
            manual_compactions: vec![],
            active_manual_compaction: None,
            aborted_generations: vec![],
            resets: vec![],
            pending_resets: vec![],
            pending_reset_blocks: Default::default(),
            fitted_reset_receipts: Default::default(),
            pending_independent_summary: None,
            application_writes: vec![],
            tool_states: HashMap::new(),
            round_terminates: Vec::new(),
            round_handoffs: Vec::new(),
            round_added_tools: Vec::new(),
            input_modes: HashMap::new(),
            answer_ids: HashMap::new(),
            auto_compaction_attempted: false,
            retry_state: RetryState::default(),
            overflow_compaction_attempted: false,
            compaction_return_turns: None,
            active: None,
            run_failure: None,
            failure_continuation_inputs: Vec::new(),
            tool_attempt: None,
            abort_requested: false,
            nested_resume: None,
            phase: Phase::Idle,
        };
        let mut latest = base.clone();
        latest.submissions.push(Submission {
            kind: SubmissionKind::Input,
            id: Uuid::new_v4(),
            request_id: None,
            content: "queued".into(),
            image_notes: Vec::new(),
            input_blocks: None,
            status: Status::Queued,
        });
        assert!(admission_only(&base, &latest).unwrap());
        let mut compaction = base.clone();
        let id = Uuid::new_v4();
        compaction.manual_compactions.push(ManualCompaction {
            id,
            status: ManualCompactionStatus::Running,
        });
        compaction.active_manual_compaction = Some(id);
        assert!(!admission_only(&base, &compaction).unwrap());
        let mut settled = compaction.clone();
        finish_manual_compaction(&mut settled, ManualCompactionStatus::Applied).unwrap();
        assert!(!admission_only(&compaction, &settled).unwrap());
        latest.phase = Phase::ModelReady { turns: 0 };
        assert!(!admission_only(&base, &latest).unwrap());
    }
    #[test]
    fn legacy_tool_phases_have_no_invented_attempt() {
        let mut conversation = Harness::empty_conversation(Uuid::new_v4(), config());
        let operation_id = Uuid::new_v4();
        let call = ToolCall {
            id: "call".into(),
            name: "tool".into(),
            arguments: "{}".into(),
        };
        for phase in [
            Phase::ToolRunning {
                turns: 0,
                calls: vec![call.clone()],
                index: 0,
                operation_id,
                replay: ReplayPolicy::Safe,
                arguments: serde_json::json!({}),
                restarts: 0,
                replay_ready: false,
                dispatch_identity: Some(DispatchIdentity::Other),
            },
            Phase::Pending {
                turns: 0,
                calls: vec![call.clone()],
                index: 0,
                operation_id,
                reference: "job".into(),
                arguments: None,
            },
            Phase::Blocked {
                tool_panicked: false,
                turns: 0,
                calls: vec![call.clone()],
                index: 0,
                operation_id,
                reason: "unknown".into(),
                arguments: None,
            },
        ] {
            conversation.phase = phase;
            let mut json = serde_json::to_value(&conversation).unwrap();
            json.as_object_mut().unwrap().remove("tool_attempt");
            let restored: Conversation = serde_json::from_value(json).unwrap();
            assert_eq!(restored.tool_attempt(), None);
        }
    }

    #[test]
    fn attempt_discovery_fences_competing_claims_and_lifecycle_saves() {
        use crate::observer::{Cursor, Observer};
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (base, version) = h.load(id).unwrap();
        let operation_id = Uuid::new_v4();
        let claim = |attempt_id| {
            let mut state = base.clone();
            state.tool_attempt = Some(ToolAttempt {
                operation_id,
                attempt_id,
            });
            state.phase = Phase::ToolRunning {
                turns: 0,
                calls: vec![],
                index: 0,
                operation_id,
                replay: ReplayPolicy::Safe,
                arguments: serde_json::json!({}),
                restarts: 0,
                replay_ready: false,
                dispatch_identity: Some(DispatchIdentity::Other),
            };
            state
        };
        let first = claim(Uuid::new_v4());
        let competing = claim(Uuid::new_v4());
        let winner = h.prepare_save(&first, Expected::Version(version)).unwrap();
        let loser = h
            .prepare_save(&competing, Expected::Version(version))
            .unwrap();
        h.commit_conversation_save(winner).unwrap();
        assert!(matches!(
            h.commit_conversation_save(loser),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        let limit = std::num::NonZeroUsize::new(10).unwrap();
        let attempts = || h.tool_attempts(id, Cursor(0), limit).unwrap().records;
        assert_eq!(attempts().len(), 1);
        assert_eq!(
            attempts()[0].value["attempt"],
            serde_json::json!(first.tool_attempt())
        );
        assert!(
            Observer::new(h.owner.store(), id)
                .read(Cursor(0), limit)
                .unwrap()
                .records
                .iter()
                .all(|r| r.value.get("attempt").is_none())
        );
        assert!(
            Observer::tool_progress(h.owner.store(), id)
                .read(Cursor(0), limit)
                .unwrap()
                .records
                .is_empty()
        );
        let (mut state, version) = h.load(id).unwrap();
        state.abort_requested = true;
        h.save(&state, Expected::Version(version)).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        state.phase = Phase::Idle;
        h.save(&state, Expected::Version(version)).unwrap();
        let page = h.tool_attempts(id, Cursor(0), limit).unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(
            page.records[0].value["attempt"],
            serde_json::json!(first.tool_attempt())
        );
    }

    #[test]
    fn legacy_running_attempt_is_not_backfilled_and_new_claim_is_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let mut legacy = h.conversation(id).unwrap();
        let operation_id = Uuid::new_v4();
        legacy.phase = Phase::ToolRunning {
            turns: 0,
            calls: vec![],
            index: 0,
            operation_id,
            replay: ReplayPolicy::Safe,
            arguments: serde_json::json!({}),
            restarts: 0,
            replay_ready: false,
            dispatch_identity: Some(DispatchIdentity::Other),
        };
        h.owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key: Harness::key(id),
                    value: serde_json::to_vec(&legacy).unwrap(),
                }],
            )
            .unwrap();
        let (state, version) = h.load(id).unwrap();
        h.save(&state, Expected::Version(version)).unwrap();
        let limit = std::num::NonZeroUsize::new(1).unwrap();
        assert!(
            h.tool_attempts(id, crate::observer::Cursor(0), limit)
                .unwrap()
                .records
                .is_empty()
        );
        let (mut state, version) = h.load(id).unwrap();
        let attempt = ToolAttempt {
            operation_id,
            attempt_id: Uuid::new_v4(),
        };
        state.tool_attempt = Some(attempt);
        let mut prepared = h
            .prepare_conversation_save(&state, Expected::Version(version), None, &[], &[])
            .unwrap();
        prepared.checks.push(Check {
            key: b"missing-guard".to_vec(),
            expected: Expected::Version(1),
        });
        assert!(h.commit_conversation_save(prepared).is_err());
        assert!(
            h.tool_attempts(id, crate::observer::Cursor(0), limit)
                .unwrap()
                .records
                .is_empty()
        );
        assert_eq!(h.conversation(id).unwrap().tool_attempt(), None);
        h.save(&state, Expected::Version(version)).unwrap();
        let page = h
            .tool_attempts(id, crate::observer::Cursor(0), limit)
            .unwrap();
        assert_eq!(page.records[0].value["attempt"], serde_json::json!(attempt));
    }

    #[test]
    fn task_marker_identity_is_required_and_bound_on_recovery() {
        use crate::task::{Task, TaskState};
        use crate::task_scheduler::{Handler, Step};
        use crate::task_tool::TaskTool;

        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let handler: Arc<dyn Handler> =
            Arc::new(|_: &Task| Ok(Step::Succeed(serde_json::json!(null))));
        let adapter = |kind: &str, version, description: &str| {
            Arc::new(TaskTool::new(
                pi_rs_core::ToolSpec {
                    name: "job".into(),
                    description: description.into(),
                    parameters: serde_json::json!({"type":"object"}),
                },
                kind,
                true,
                version,
                handler.clone(),
            ))
        };
        for (case, changed, legacy, should_block) in [
            ("same", Some(adapter("task", 1, "original")), false, false),
            ("kind", Some(adapter("other", 1, "original")), false, true),
            ("version", Some(adapter("task", 2, "original")), false, true),
            ("spec", Some(adapter("task", 1, "changed")), false, true),
            ("missing", None, false, true),
            ("legacy", Some(adapter("task", 1, "original")), true, true),
        ] {
            let path = dir.path().join(case);
            let original = adapter("task", 1, "original");
            let mut first = options(model.clone());
            first.tools = vec![original.register()];
            let mut h = Harness::open_paused(&path, first).unwrap();
            let id = h.create_conversation(config()).unwrap();
            let (mut c, version) = h.load(id).unwrap();
            c.phase = Phase::ToolRunning {
                turns: 0,
                calls: vec![ToolCall {
                    id: "call".into(),
                    name: "job".into(),
                    arguments: "{}".into(),
                }],
                index: 0,
                operation_id: Uuid::new_v4(),
                replay: ReplayPolicy::Safe,
                arguments: serde_json::json!({}),
                restarts: 0,
                replay_ready: false,
                dispatch_identity: if legacy {
                    None
                } else {
                    Some(DispatchIdentity::Task(original.identity().unwrap()))
                },
            };
            h.save(&c, Expected::Version(version)).unwrap();
            drop(h);
            let mut second = options(model.clone());
            second.tools = changed
                .map(|adapter| vec![adapter.register()])
                .unwrap_or_default();
            let h = Harness::open_paused(&path, second).unwrap();
            if should_block {
                assert!(
                    matches!(h.conversation(id).unwrap().phase, Phase::Blocked { .. }),
                    "{case}"
                );
                assert!(h.blocked(id).unwrap().unwrap().1.contains("identity"));
            } else {
                assert!(matches!(
                    h.conversation(id).unwrap().phase,
                    Phase::ToolRunning {
                        replay_ready: true,
                        ..
                    }
                ));
            }
            // Recovery must not create or run a scheduler task.
            assert!(
                crate::task_store::TaskStore::new(h.owner.store())
                    .list()
                    .unwrap()
                    .iter()
                    .all(|s| s.task.state() != &TaskState::Running)
            );
        }
    }

    #[test]
    fn legacy_pending_task_cannot_start_a_driver() {
        use crate::task::Task;
        use crate::task_scheduler::{Handler, Registry, Step};
        use crate::task_tool::TaskTool;
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let handler: Arc<dyn Handler> =
            Arc::new(|_: &Task| Ok(Step::Succeed(serde_json::json!(null))));
        let adapter = Arc::new(TaskTool::new(
            pi_rs_core::ToolSpec {
                name: "job".into(),
                description: "".into(),
                parameters: serde_json::json!({"type":"object"}),
            },
            "task",
            true,
            1,
            handler.clone(),
        ));
        let mut opts = options(model);
        opts.tools = vec![adapter.register()];
        let mut h = Harness::open_paused(dir.path().join("db"), opts).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (mut c, version) = h.load(id).unwrap();
        let operation_id = Uuid::new_v4();
        let task = adapter
            .task(id, operation_id, serde_json::json!({}), 5)
            .unwrap();
        c.phase = Phase::TaskPending {
            turns: 0,
            calls: vec![ToolCall {
                id: "call".into(),
                name: "job".into(),
                arguments: "{}".into(),
            }],
            index: 0,
            operation_id,
            arguments: serde_json::json!({}),
            task: operation_id,
            identity: None,
        };
        h.save_composed(&c, Expected::Version(version), None, &[task])
            .unwrap();
        let mut registry = Registry::default();
        registry.register_shared("task", true, handler);
        assert!(h.start_tasks(registry).is_err());
        assert!(matches!(
            h.conversation(id).unwrap().phase,
            Phase::TaskPending { .. }
        ));
    }

    #[test]
    fn task_id_collision_does_not_retry_admission_merge() {
        use crate::task::{Checkpoint, Task, TaskOwner};
        use crate::task_store::{Save, TaskStore};

        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(id),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::json!({}),
            },
            3,
        )
        .unwrap()
        .registered("job".into(), true);
        TaskStore::new(h.owner.store())
            .save(&[Save::Create(&task)])
            .unwrap();
        let (state, version) = h.load(id).unwrap();
        h.merge_admissions = true;
        assert!(matches!(
            h.save_composed(&state, Expected::Version(version), None, &[task]),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(h.load(id).unwrap().1, version);
    }

    #[test]
    fn conversation_abort_stops_at_persisted_background_task() {
        use crate::task::{Checkpoint, Task, TaskOwner};
        use crate::task_store::{Save, TaskStore};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let conversation = h.create_conversation(config()).unwrap();
        let make = |owner| {
            Task::new(
                Uuid::new_v4(),
                owner,
                Checkpoint {
                    phase: "start".into(),
                    data: serde_json::Value::Null,
                },
                3,
            )
            .unwrap()
        };
        let foreground = make(TaskOwner::Conversation(conversation));
        let background = make(TaskOwner::Conversation(conversation))
            .background()
            .unwrap();
        let child = make(TaskOwner::Task(background.id()));
        assert!(child.clone().background().is_err());
        TaskStore::new(h.owner.store())
            .save(&[
                Save::Create(&foreground),
                Save::Create(&background),
                Save::Create(&child),
            ])
            .unwrap();
        h.request_abort(conversation).unwrap();
        drop(h);

        let h = Harness::open_paused(&path, options(model)).unwrap();
        let store = TaskStore::new(h.owner.store());
        assert!(
            store
                .load(foreground.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        assert!(
            store
                .load(background.id())
                .unwrap()
                .unwrap()
                .task
                .is_background()
        );
        assert!(
            !store
                .load(background.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        assert!(
            !store
                .load(child.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        let mut tasks =
            crate::task_scheduler::Scheduler::attach(h.owner.clone(), Default::default()).unwrap();
        tasks.request_abort(background.id()).unwrap();
        assert!(
            store
                .load(background.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        assert!(
            store
                .load(child.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn transcript_observation_retains_abort_state_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        state.messages.push(Message::User {
            content: "before abort".into(),
        });
        state.abort_requested = true;
        h.save(&state, Expected::Version(version)).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options(model)).unwrap();
        assert!(h.conversation(id).unwrap().abort_requested);
        let history = h
            .history(
                id,
                crate::observer::Cursor(0),
                std::num::NonZeroUsize::new(100).unwrap(),
            )
            .unwrap();
        let event = &history.records.last().unwrap().value;
        assert_eq!(event["abort_requested"], true);
        assert_eq!(
            event["messages"],
            serde_json::to_value(&state.messages).unwrap()
        );
    }
    #[test]
    fn prepared_multiple_saves_are_atomic_and_migrate_legacy_transcripts() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model.clone())).unwrap();
        let first = h.create_conversation(config()).unwrap();
        let second = h.create_conversation(config()).unwrap();
        let mut legacy = h.conversation(first).unwrap();
        legacy.transcript = None;
        legacy.messages.push(Message::User {
            content: "legacy".into(),
        });
        let mut head = b"durable/transcript/head/".to_vec();
        head.extend_from_slice(first.as_bytes());
        h.owner
            .store()
            .commit(
                &[],
                &[
                    Mutation::Delete { key: head },
                    Mutation::Put {
                        key: Harness::key(first),
                        value: serde_json::to_vec(&legacy).unwrap(),
                    },
                ],
            )
            .unwrap();
        let request = Uuid::new_v4();
        let progress = crate::progress::Snapshot::load(h.owner.store(), request)
            .unwrap()
            .prepare_checkpoint()
            .unwrap();
        h.owner
            .store()
            .commit(&progress.checks, &progress.mutations)
            .unwrap();
        let (mut a, av) = h.load(first).unwrap();
        let (mut b, bv) = h.load(second).unwrap();
        a.messages.push(Message::User {
            content: "migrated".into(),
        });
        b.messages.push(Message::User {
            content: "second".into(),
        });
        b.phase = Phase::AbandonedModel {
            turns: 0,
            request_id: request,
            restarts: 0,
        };
        let batch = h
            .prepare_saves(&[(&a, Expected::Version(av)), (&b, Expected::Version(bv))])
            .unwrap();
        // A competing write invalidates just the second conversation's check.
        let key = Harness::key(second);
        let current = h.owner.store().get(&key).unwrap().unwrap();
        h.owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key,
                    value: current.value,
                }],
            )
            .unwrap();
        let snapshot = |h: &Harness| {
            h.owner
                .store()
                .list(b"")
                .unwrap()
                .into_iter()
                .map(|(k, v)| (k, v.value, v.version))
                .collect::<Vec<_>>()
        };
        let before = snapshot(&h);
        assert!(matches!(
            h.commit_conversation_save(batch),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(snapshot(&h), before);
        assert_eq!(h.conversation(first).unwrap().messages, legacy.messages);
        assert!(matches!(
            crate::progress::Snapshot::load(h.owner.store(), request)
                .unwrap()
                .state(),
            crate::progress::State::Running(_)
        ));
        let (mut a, av) = h.load(first).unwrap();
        let (mut b, bv) = h.load(second).unwrap();
        a.messages.push(Message::User {
            content: "migrated".into(),
        });
        b.messages.push(Message::User {
            content: "second".into(),
        });
        b.phase = Phase::AbandonedModel {
            turns: 0,
            request_id: request,
            restarts: 0,
        };
        assert!(matches!(
            h.prepare_saves(&[(&a, Expected::Version(av)), (&a, Expected::Version(av))]),
            Err(Error::Invalid(_))
        ));
        let batch = h
            .prepare_saves(&[(&a, Expected::Version(av)), (&b, Expected::Version(bv))])
            .unwrap();
        let progress = crate::progress::Snapshot::load(h.owner.store(), request)
            .unwrap()
            .prepare_checkpoint()
            .unwrap();
        h.owner
            .store()
            .commit(&progress.checks, &progress.mutations)
            .unwrap();
        let before = snapshot(&h);
        assert!(matches!(
            h.commit_conversation_save(batch),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(snapshot(&h), before);
        let batch = h
            .prepare_saves(&[(&a, Expected::Version(av)), (&b, Expected::Version(bv))])
            .unwrap();
        h.commit_conversation_save(batch).unwrap();
        assert!(matches!(
            crate::progress::Snapshot::load(h.owner.store(), request)
                .unwrap()
                .state(),
            crate::progress::State::Aborted(_)
        ));
        for state in [&a, &b] {
            let history = h
                .history(
                    state.id,
                    crate::observer::Cursor(0),
                    std::num::NonZeroUsize::new(100).unwrap(),
                )
                .unwrap();
            assert_eq!(
                history.records.last().unwrap().value["messages"],
                serde_json::to_value(&state.messages).unwrap()
            );
            let fork_prefix = format!("runtime/fork/{}/", state.id);
            assert_eq!(
                h.owner.store().list(fork_prefix.as_bytes()).unwrap().len(),
                2
            );
        }
        let fork = h.fork(first).unwrap();
        assert_eq!(h.conversation(fork).unwrap().messages, a.messages);
        drop(h);
        let h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        assert_eq!(h.conversation(fork).unwrap().messages, a.messages);
        assert_eq!(h.conversation(first).unwrap().messages, a.messages);
        assert_eq!(h.conversation(second).unwrap().messages, b.messages);
    }
    #[test]
    fn legacy_migration_is_atomic_and_snapshots_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let mut legacy = h.conversation(id).unwrap();
        legacy.transcript = None;
        legacy.messages.push(Message::User {
            content: "legacy".into(),
        });
        let store = h.owner.store();
        let head = store
            .list(b"durable/transcript/head/")
            .unwrap()
            .pop()
            .unwrap()
            .0;
        store
            .commit(
                &[],
                &[
                    Mutation::Delete { key: head },
                    Mutation::Put {
                        key: Harness::key(id),
                        value: serde_json::to_vec(&legacy).unwrap(),
                    },
                ],
            )
            .unwrap();
        let (mut state, version) = h.load(id).unwrap();
        assert_eq!(state.messages, legacy.messages);
        state.messages.push(Message::User {
            content: "new".into(),
        });
        let before = store_count(h.owner.store());
        assert!(matches!(
            h.save(&state, Expected::Version(version + 1)),
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(store_count(h.owner.store()), before);
        assert_eq!(h.conversation(id).unwrap().messages, legacy.messages);
        let (mut state, version) = h.load(id).unwrap();
        state.messages.push(Message::User {
            content: "new".into(),
        });
        h.save(&state, Expected::Version(version)).unwrap();
        let history = h
            .history(
                id,
                crate::observer::Cursor(0),
                std::num::NonZeroUsize::new(100).unwrap(),
            )
            .unwrap();
        assert_eq!(
            history.records.last().unwrap().value["messages"],
            serde_json::to_value(&state.messages).unwrap()
        );
        drop(h);
        let h = Harness::open_paused(&path, options(model)).unwrap();
        assert_eq!(h.conversation(id).unwrap().messages, state.messages);
    }
    #[test]
    fn snapshots_and_forks_share_message_storage() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut h = Harness::open_paused(dir.path().join("db"), options(model)).unwrap();
        let id = h.create_conversation(config()).unwrap();
        let (mut state, version) = h.load(id).unwrap();
        let message = "large-message-marker".repeat(1000);
        state.messages.push(Message::User {
            content: message.clone(),
        });
        h.save(&state, Expected::Version(version)).unwrap();
        for _ in 0..5 {
            let (state, version) = h.load(id).unwrap();
            h.save(&state, Expected::Version(version)).unwrap();
        }
        let entries = h
            .owner
            .store()
            .list(b"durable/transcript/entry/")
            .unwrap()
            .len();
        let child = h.fork(id).unwrap();
        assert_eq!(
            h.owner
                .store()
                .list(b"durable/transcript/entry/")
                .unwrap()
                .len(),
            entries
        );
        assert_eq!(h.conversation(child).unwrap().messages, state.messages);
        for prefix in [b"runtime/fork/".as_slice(), b"durable/observer/".as_slice()] {
            for (_, entry) in h.owner.store().list(prefix).unwrap() {
                assert!(
                    !entry
                        .value
                        .windows(message.len())
                        .any(|w| w == message.as_bytes())
                );
            }
        }
    }
    fn store_count(store: &durable_store::Store) -> usize {
        store.list(b"durable/transcript/").unwrap().len()
    }
    #[tokio::test]
    async fn concurrent_conversations_and_admission_during_execution() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut s = Scheduler::open(&path, options(model.clone())).unwrap();
        let a = s.create_conversation(config()).unwrap();
        let b = s.create_conversation(config()).unwrap();
        s.submit(a, None, "one".into()).unwrap();
        s.submit(b, None, "two".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), model.started.acquire_many(2))
            .await
            .unwrap()
            .unwrap()
            .forget();
        s.submit(a, Some("second".into()), "three".into()).unwrap();
        assert_eq!(model.maximum.load(Ordering::SeqCst), 2);
        model.release.add_permits(3);
        s.wait(a).await.unwrap();
        s.wait(b).await.unwrap();
        assert_eq!(s.conversation(a).unwrap().submissions.len(), 2);
        assert!(
            s.conversation(a)
                .unwrap()
                .submissions
                .iter()
                .all(|x| matches!(x.status, Status::Done { .. }))
        );
        assert_eq!(model.maximum.load(Ordering::SeqCst), 2);
        s.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn withdrawal_during_model_call_survives_worker_commit() {
        let dir = tempfile::tempdir().unwrap();
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), options(model.clone())).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        scheduler.submit(id, None, "first".into()).unwrap();
        model.started.acquire().await.unwrap().forget();
        let queued = scheduler
            .submit(id, Some("queued".into()), "withdraw me".into())
            .unwrap();
        assert_eq!(
            scheduler.withdraw_submission(id, queued).unwrap(),
            WithdrawResult::Aborted
        );
        model.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        let conversation = scheduler.conversation(id).unwrap();
        assert_eq!(
            conversation.submissions[1].status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
        assert!(
            !conversation
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { content } if content == "withdraw me"))
        );
        scheduler.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn restart_processes_abort_mark_before_recovery_can_redispatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut s = Scheduler::open(&path, options(model.clone())).unwrap();
        let id = s.create_conversation(config()).unwrap();
        s.submit(id, None, "first".into()).unwrap();
        model.started.acquire().await.unwrap().forget();
        {
            let control = s.harness.owner.conversation_controls().get(id);
            let _guard = control.gate.lock().unwrap();
            s.harness.request_abort(id).unwrap();
            s.cancel.cancel();
        }
        s.shutdown().await.unwrap();
        let h = Harness::open(&path, options(model.clone()), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
        assert!(model.started.try_acquire().is_err());
    }
    struct ToolModel;
    #[async_trait::async_trait]
    impl Model for ToolModel {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            Ok(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "call".into(),
                    name: "barrier".into(),
                    arguments: "{}".into(),
                }],
                usage: None,
            })])
            .boxed())
        }
    }
    #[tokio::test]
    async fn suspended_nested_poll_releases_actual_conversation_worker_lock() {
        struct Parent;
        #[async_trait::async_trait]
        impl crate::keyed_tool::KeyedTool for Parent {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "barrier".into(),
                    description: "".into(),
                    parameters: serde_json::json!({"type":"object"}),
                }
            }
            async fn invoke(
                &self,
                _: serde_json::Value,
                _: pi_rs_core::ToolContext,
                _: Option<&pi_rs_core::ExtensionState>,
                host: crate::keyed_tool::KeyedHost,
            ) -> pi_rs_core::ToolReply {
                assert!(matches!(
                    host.call_keyed_unpolled("child", "child", serde_json::json!({}))
                        .await,
                    Outcome::Pending { .. }
                ));
                host.suspend_pending().unwrap();
                Outcome::Succeeded("ignored".into()).into()
            }
        }
        struct Child(Arc<Semaphore>);
        #[async_trait::async_trait]
        impl Tool for Child {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: serde_json::json!({"type":"object"}),
                }
            }
            async fn invoke(&self, _: serde_json::Value, _: pi_rs_core::ToolContext) -> Outcome {
                Outcome::Pending {
                    reference: "job".into(),
                }
            }
            fn can_poll(&self) -> bool {
                true
            }
            async fn poll(&self, _: &str, _: pi_rs_core::ToolContext) -> Option<Outcome> {
                self.0.add_permits(1);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let entered = Arc::new(Semaphore::new(0));
        let parent = Arc::new(crate::keyed_tool::KeyedToolAdapter::new(Parent));
        let opts = Options {
            model: Arc::new(ToolModel),
            tools: vec![
                parent.register(ReplayPolicy::Safe),
                RegisteredTool {
                    tool: Arc::new(Child(entered.clone())),
                    replay: ReplayPolicy::Unsafe,
                },
            ],
            restart_limit: 5,
            max_turns: 5,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut scheduler = Scheduler::open(dir.path().join("db"), opts).unwrap();
        let id = scheduler.create_conversation(config()).unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let control = scheduler.harness.owner.conversation_controls().get(id);
        let guard = control
            .worker
            .try_lock()
            .expect("poll held conversation worker");
        drop(guard);
        scheduler.abort(id).unwrap();
        tokio::time::timeout(Duration::from_secs(5), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        scheduler.shutdown().await.unwrap();
    }
    struct BarrierTool {
        started: Arc<Semaphore>,
        count: Arc<AtomicUsize>,
    }

    struct ControlledUnknownTool {
        started: Arc<Semaphore>,
        release: Arc<Semaphore>,
    }
    #[async_trait::async_trait]
    impl Tool for ControlledUnknownTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "barrier".into(),
                description: "".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: pi_rs_core::ToolContext) -> Outcome {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            Outcome::Unknown("test handoff boundary".into())
        }
    }

    #[tokio::test]
    async fn live_tool_and_task_handler_have_no_existing_handoff_window() {
        use crate::executor_handoff::{self, Binding, ConversationLease};
        use crate::task::{Checkpoint, Task, TaskOwner};
        use crate::task_scheduler::{Registry, Step};
        use crate::task_store::TaskStore;
        use std::sync::mpsc;

        let dir = tempfile::tempdir().unwrap();
        let started = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let opts = Options {
            model: Arc::new(ToolModel),
            tools: vec![RegisteredTool {
                tool: Arc::new(ControlledUnknownTool {
                    started: started.clone(),
                    release: release.clone(),
                }),
                replay: ReplayPolicy::Safe,
            }],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        };
        let mut conversation = Scheduler::open(dir.path().join("db"), opts).unwrap();
        let id = conversation.create_conversation(config()).unwrap();
        conversation.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let tool_attempt = conversation
            .harness
            .conversation(id)
            .unwrap()
            .active_tool_attempt()
            .unwrap();
        let owner = conversation.harness.owner.clone();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (stop_tx, stop_rx) = mpsc::channel();
        let stop_rx = std::sync::Mutex::new(stop_rx);
        let mut registry = Registry::default();
        registry.register("actual-handler", true, move |task: &Task| {
            entered_tx.send(task.attempt_id().unwrap()).unwrap();
            stop_rx.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(serde_json::json!(null)))
        });
        let mut tasks = crate::task_scheduler::Scheduler::attach(owner.clone(), registry).unwrap();
        let task_id = Uuid::new_v4();
        let task = Task::new(
            task_id,
            TaskOwner::Conversation(id),
            Checkpoint {
                phase: "start".into(),
                data: serde_json::json!({}),
            },
            3,
        )
        .unwrap()
        .registered("actual-handler".into(), true);
        tasks.submit(&task).unwrap();
        let worker = std::thread::spawn(move || {
            tasks.tick(0).unwrap();
            tasks
        });
        let task_attempt = entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let binding = Binding {
            conversation: id,
            tool: tool_attempt,
            task: task_id,
            task_attempt,
        };
        let batch = executor_handoff::prepare_binding(owner.store(), &binding).unwrap();
        owner
            .store()
            .commit(&batch.checks, &batch.mutations)
            .unwrap();
        assert!(matches!(
            ConversationLease::acquire(owner.clone(), binding.clone()),
            Err(executor_handoff::Error::Busy)
        ));
        stop_tx.send(()).unwrap();
        let tasks = worker.join().unwrap();
        assert_eq!(
            TaskStore::new(owner.store())
                .load(task_id)
                .unwrap()
                .unwrap()
                .task
                .attempt_id(),
            None
        );
        assert_eq!(
            TaskStore::new(owner.store())
                .load(task_id)
                .unwrap()
                .unwrap()
                .task
                .retired_attempt_id(),
            Some(task_attempt)
        );
        assert!(matches!(
            ConversationLease::acquire(owner.clone(), binding.clone()),
            Err(executor_handoff::Error::Busy)
        ));
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(3), conversation.wait(id))
            .await
            .unwrap()
            .unwrap();
        assert!(conversation.harness.blocked(id).unwrap().is_some());
        assert_eq!(
            conversation
                .harness
                .conversation(id)
                .unwrap()
                .active_tool_attempt(),
            None
        );
        assert!(matches!(
            ConversationLease::acquire(owner.clone(), binding),
            Err(executor_handoff::Error::Stale)
        ));
        drop(tasks);
        drop(owner);
        conversation.shutdown().await.unwrap();
    }
    #[async_trait::async_trait]
    impl Tool for BarrierTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "barrier".into(),
                description: "".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: pi_rs_core::ToolContext) -> Outcome {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.started.add_permits(1);
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn restart_with_abort_mark_never_replays_dispatched_tool() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let started = Arc::new(Semaphore::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let opts = || Options {
            model: Arc::new(ToolModel),
            tools: vec![RegisteredTool {
                tool: Arc::new(BarrierTool {
                    started: started.clone(),
                    count: count.clone(),
                }),
                replay: ReplayPolicy::Safe,
            }],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        };
        let mut s = Scheduler::open(&path, opts()).unwrap();
        let id = s.create_conversation(config()).unwrap();
        s.submit(id, None, "work".into()).unwrap();
        started.acquire().await.unwrap().forget();
        {
            let control = s.harness.owner.conversation_controls().get(id);
            let _guard = control.gate.lock().unwrap();
            s.harness.request_abort(id).unwrap();
            s.cancel.cancel();
        }
        s.shutdown().await.unwrap();
        let h = Harness::open(&path, opts(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(h.blocked(id).unwrap().is_some());
        assert_eq!(
            h.conversation(id).unwrap().submissions[0].status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
    }
    struct FailingModel;
    #[async_trait::async_trait]
    impl Model for FailingModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            if request
                .messages
                .iter()
                .any(|message| matches!(message, Message::User { content } if content == "fail"))
            {
                return Err(pi_rs_core::Error::Model("deliberate failure".into()));
            }
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("ok".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
    #[tokio::test]
    async fn errors_do_not_stop_other_conversations() {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            model: Arc::new(FailingModel),
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        };
        let mut s = Scheduler::open(dir.path().join("db"), opts).unwrap();
        let a = s.create_conversation(config()).unwrap();
        let b = s.create_conversation(config()).unwrap();
        s.submit(a, None, "fail".into()).unwrap();
        s.submit(b, None, "ok".into()).unwrap();
        s.wait(b).await.unwrap();
        assert!(matches!(
            s.conversation(b).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        s.wait(a).await.unwrap();
        assert!(matches!(
            s.conversation(a).unwrap().submissions[0].status,
            Status::Unanswered { .. }
        ));
        s.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn dropped_wait_keeps_worker_and_owner_alive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(GatedModel {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let mut s = Scheduler::open(&path, options(model.clone())).unwrap();
        let id = s.create_conversation(config()).unwrap();
        s.submit(id, None, "one".into()).unwrap();
        model.started.acquire().await.unwrap().forget();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), s.wait(id))
                .await
                .is_err()
        );
        assert!(matches!(
            Harness::open_paused(&path, options(model.clone())),
            Err(Error::Owner(OwnerError::Busy))
        ));
        model.release.add_permits(1);
        s.wait(id).await.unwrap();
        assert!(matches!(
            s.conversation(id).unwrap().submissions[0].status,
            Status::Done { .. }
        ));
        s.shutdown().await.unwrap();
        assert!(Harness::open_paused(&path, options(model)).is_ok());
    }
}

#[cfg(test)]
#[path = "runtime_cas_tests.rs"]
mod runtime_cas_tests;

#[cfg(test)]
mod tool_allowlist_tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use pi_rs_core::{ModelEvent, ModelRequest, ModelStream, ToolContext};
    use std::sync::{Mutex, atomic::AtomicUsize};

    struct RecordingModel(Arc<Mutex<Vec<Vec<String>>>>);
    #[async_trait::async_trait]
    impl Model for RecordingModel {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
            self.0
                .lock()
                .unwrap()
                .push(request.tools.iter().map(|t| t.name.clone()).collect());
            let calls = if request
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. }))
            {
                vec![]
            } else {
                vec![ToolCall {
                    id: "stale".into(),
                    name: "blocked".into(),
                    arguments: "{}".into(),
                }]
            };
            Ok(stream::iter(vec![Ok(ModelEvent::Complete { calls, usage: None })]).boxed())
        }
    }
    struct CountingTool(&'static str, Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for CountingTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.0.into(),
                description: String::new(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            self.1.fetch_add(1, Ordering::SeqCst);
            Outcome::Succeeded("ran".into())
        }
    }
    fn opts(seen: Arc<Mutex<Vec<Vec<String>>>>, count: Arc<AtomicUsize>) -> Options {
        Options {
            model: Arc::new(RecordingModel(seen)),
            tools: ["allowed", "blocked"]
                .into_iter()
                .map(|name| RegisteredTool {
                    tool: Arc::new(CountingTool(name, count.clone())),
                    replay: ReplayPolicy::Safe,
                })
                .collect(),
            restart_limit: 3,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        }
    }

    #[test]
    fn additions_update_both_opt_in_lists_in_order() {
        let mut c = Harness::empty_conversation(Uuid::new_v4(), config());
        c.allowed_tools = Some(vec!["first".into()]);
        c.model_tools = Some(vec!["first".into()]);
        c.round_added_tools = vec![
            vec!["new".into(), "".into()],
            vec!["new".into(), "later".into()],
        ];
        place_round_handoff(&mut c);
        assert_eq!(
            c.allowed_tools,
            Some(vec!["first".into(), "new".into(), "later".into()])
        );
        assert_eq!(c.model_tools, c.allowed_tools);
        assert!(c.round_added_tools.is_empty());
    }

    #[tokio::test]
    async fn model_selection_filters_specs_and_model_calls_but_not_enabled_tools() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let seen = Arc::new(Mutex::new(vec![]));
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::open_paused(&path, opts(seen.clone(), count.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_tools(id, Some(vec!["allowed".into()])).unwrap();
        h.select_model_tools(id, Some(vec!["blocked".into(), "allowed".into()]))
            .unwrap();
        assert_eq!(
            h.ordered_tool_specs(&h.conversation(id).unwrap())
                .unwrap()
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            vec!["allowed"]
        );
        assert!(h.select_model_tools(id, Some(vec!["".into()])).is_err());
        assert!(
            h.select_model_tools(id, Some(vec!["allowed".into(), "allowed".into()]))
                .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, opts(seen.clone(), count.clone())).unwrap();
        assert_eq!(
            h.conversation(id).unwrap().model_tools,
            Some(vec!["blocked".into(), "allowed".into()])
        );
        h.select_tools(id, None).unwrap();
        h.select_model_tools(id, Some(vec!["allowed".into()]))
            .unwrap();
        assert_eq!(
            h.resolved_tools(&h.conversation(id).unwrap())
                .unwrap()
                .len(),
            2
        );
        h.submit_and_run(id, None, "work".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(
            seen.lock()
                .unwrap()
                .iter()
                .all(|names| names == &["allowed"])
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        h.select_model_tools(id, Some(vec![])).unwrap();
        assert!(
            h.ordered_tool_specs(&h.conversation(id).unwrap())
                .unwrap()
                .is_empty()
        );
        h.select_model_tools(id, None).unwrap();
        assert_eq!(
            h.ordered_tool_specs(&h.conversation(id).unwrap())
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn historical_fork_retains_model_selection_at_cut() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let opts = || opts(Arc::new(Mutex::new(vec![])), Arc::new(AtomicUsize::new(0)));
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_model_tools(id, Some(vec!["allowed".into()]))
            .unwrap();
        let cursor = h
            .history(
                id,
                crate::observer::Cursor(0),
                std::num::NonZeroUsize::new(100).unwrap(),
            )
            .unwrap()
            .next;
        h.select_model_tools(id, Some(vec!["blocked".into()]))
            .unwrap();
        let fork = h.fork_at(id, cursor).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, opts()).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().model_tools,
            Some(vec!["allowed".into()])
        );
        assert_eq!(
            h.conversation(id).unwrap().model_tools,
            Some(vec!["blocked".into()])
        );
    }

    #[tokio::test]
    async fn model_calls_respect_enabled_selection_without_extensions() {
        let dir = tempfile::tempdir().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::open_paused(
            dir.path().join("db"),
            opts(Arc::new(Mutex::new(vec![])), count.clone()),
        )
        .unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_tools(id, Some(vec!["allowed".into()])).unwrap();
        h.submit_and_run(id, None, "work".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn child_selection_persists_and_filters_specs_and_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let seen = Arc::new(Mutex::new(vec![]));
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::open_paused(&path, opts(seen.clone(), count.clone())).unwrap();
        let parent = h.create_conversation(config()).unwrap();
        let child = h.ensure_subagent(parent, "child", config()).unwrap().child;
        h.select_tools(child, Some(vec!["allowed".into(), "not-installed".into()]))
            .unwrap();
        assert_eq!(h.conversation(parent).unwrap().allowed_tools, None);
        let fork = h.fork(child).unwrap();
        assert_eq!(
            h.conversation(fork).unwrap().allowed_tools,
            h.conversation(child).unwrap().allowed_tools
        );
        h.select_tools(fork, Some(vec![])).unwrap();
        assert_ne!(
            h.conversation(fork).unwrap().allowed_tools,
            h.conversation(child).unwrap().allowed_tools
        );
        assert!(
            h.select_tools(child, Some(vec!["allowed".into(), "allowed".into()]))
                .is_err()
        );
        drop(h);
        let mut h = Harness::open_paused(&path, opts(seen.clone(), count.clone())).unwrap();
        assert_eq!(
            h.conversation(child).unwrap().allowed_tools,
            Some(vec!["allowed".into(), "not-installed".into()])
        );
        h.submit_and_run(child, None, "work".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(
            seen.lock()
                .unwrap()
                .iter()
                .all(|names| names == &["allowed"])
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(h.conversation(child).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool { content, .. } if content.contains("unavailable"))
        ));
        h.select_tools(child, Some(vec![])).unwrap();
        assert!(
            h.resolved_tools(&h.conversation(child).unwrap())
                .unwrap()
                .is_empty()
        );
        h.select_tools(child, None).unwrap();
        assert_eq!(
            h.resolved_tools(&h.conversation(child).unwrap())
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn stale_running_tool_is_not_replayed_when_hidden_from_model() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let seen = Arc::new(Mutex::new(vec![]));
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::open_paused(&path, opts(seen.clone(), count.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_tools(id, Some(vec!["blocked".into()])).unwrap();
        h.select_model_tools(id, Some(vec![])).unwrap();
        let (mut c, version) = h.load(id).unwrap();
        c.phase = Phase::ToolRunning {
            turns: 0,
            calls: vec![ToolCall {
                id: "call".into(),
                name: "blocked".into(),
                arguments: "{}".into(),
            }],
            index: 0,
            operation_id: Uuid::new_v4(),
            replay: ReplayPolicy::Safe,
            arguments: serde_json::json!({}),
            restarts: 0,
            replay_ready: false,
            dispatch_identity: Some(DispatchIdentity::Other),
        };
        h.save(&c, Expected::Version(version)).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, opts(seen, count.clone())).unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().phase,
            Phase::ToolFinished { .. }
        ));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stale_running_tool_is_not_replayed_when_disallowed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let seen = Arc::new(Mutex::new(vec![]));
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::open_paused(&path, opts(seen.clone(), count.clone())).unwrap();
        let id = h.create_conversation(config()).unwrap();
        h.select_tools(id, Some(vec![])).unwrap();
        let (mut c, version) = h.load(id).unwrap();
        c.phase = Phase::ToolRunning {
            turns: 0,
            calls: vec![ToolCall {
                id: "call".into(),
                name: "blocked".into(),
                arguments: "{}".into(),
            }],
            index: 0,
            operation_id: Uuid::new_v4(),
            replay: ReplayPolicy::Safe,
            arguments: serde_json::json!({}),
            restarts: 0,
            replay_ready: false,
            dispatch_identity: Some(DispatchIdentity::Other),
        };
        h.save(&c, Expected::Version(version)).unwrap();
        drop(h);
        let h = Harness::open_paused(&path, opts(seen, count.clone())).unwrap();
        assert!(matches!(
            h.conversation(id).unwrap().phase,
            Phase::ToolFinished { .. }
        ));
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod document_fork_retry_tests {
    use super::*;
    use crate::document::Documents;
    use durable_store::Store;

    #[test]
    fn retries_only_revision_conflicts() {
        let mut attempts = 0;
        retry_document_fork(|| {
            attempts += 1;
            if attempts < 3 {
                return Err(durable_store::Error::Conflict {
                    key: FORK_DOCUMENT_FENCE.to_vec(),
                }
                .into());
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(attempts, 3);
        attempts = 0;
        let result = retry_document_fork(|| {
            attempts += 1;
            Err(durable_store::Error::Conflict {
                key: b"other".to_vec(),
            }
            .into())
        });
        assert!(result.is_err());
        assert_eq!(attempts, 1);
    }

    #[test]
    fn unrelated_writes_bound_retries_and_leave_no_partial_child() {
        let store = Store::memory().unwrap();
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let docs = Documents::new(&store);
        let cut = docs.snapshot().unwrap();
        let target = b"test/fork-child".to_vec();
        let mut attempts = 0;
        let result = retry_document_fork(|| {
            attempts += 1;
            let mut batch = docs.fork_scope(parent, child, cut)?;
            batch.mutations.push(Mutation::Put {
                key: target.clone(),
                value: vec![1],
            });
            store.commit(
                &[],
                &[Mutation::Put {
                    key: b"durable/application-entries/unrelated".to_vec(),
                    value: vec![attempts as u8],
                }],
            )?;
            store.commit(&batch.checks, &batch.mutations)?;
            Ok(())
        });
        assert!(
            matches!(result, Err(Error::Storage(durable_store::Error::Conflict { key })) if key == FORK_DOCUMENT_FENCE)
        );
        assert_eq!(attempts, FORK_DOCUMENT_ATTEMPTS);
        assert!(store.get(&target).unwrap().is_none());
        retry_document_fork(|| {
            let batch = docs.fork_scope(parent, child, cut)?;
            store.commit(&batch.checks, &batch.mutations)?;
            Ok(())
        })
        .unwrap();
    }
}

#[cfg(test)]
#[path = "conversation_wake_tests.rs"]
mod conversation_wake_tests;
