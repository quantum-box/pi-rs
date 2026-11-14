//! Durable conversation execution with a single local scheduler owner.
//!
//! A committed phase is the authority for recovery. External calls never run
//! inside storage transactions. Independent conversations can run concurrently.
pub use crate::state::{AdmissionError, AdmissionOptions, InputMode};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Submission {
    #[serde(default)]
    pub kind: SubmissionKind,
    pub id: Uuid,
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    pub status: Status,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
enum ModelFailureDecision {
    Retry,
    Overflow,
    Stop,
}

#[derive(Serialize, Deserialize)]
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
}

type TranscriptChangeParts = (Vec<Message>, bool, Option<(Vec<Uuid>, u64, Uuid)>);

impl TranscriptChange {
    fn parts(self) -> Result<TranscriptChangeParts, Error> {
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
    pub messages: Vec<Message>,
    /// Identity of the active context since its last reset or compaction.
    /// Older persisted conversations acquire one at the next compaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_generation: Option<Uuid>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_attempt: Option<ToolAttempt>,
    #[serde(default)]
    pub abort_requested: bool,
    phase: Phase,
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

impl Conversation {
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

/// A pull-based watch of a bounded conversation view. `initial` is captured at
/// attachment. Later calls to `next` return the latest changed snapshot; commits
/// between polls may be coalesced. Dropping a pending `next` is cancellation-safe.
/// Call `close` (or drop the watch) to release its store ownership.
pub struct ViewWatch {
    pub initial: ConversationView,
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
            let view = reader.view(self.id, self.limit)?;
            let fingerprint = view_fingerprint(&view)?;
            if fingerprint != self.last {
                self.last = fingerprint;
                return Ok(Some(view));
            }
            tokio::time::sleep(self.interval).await;
        }
    }

    /// Stop watching and release the reader's store ownership. Idempotent.
    pub fn close(&mut self) {
        self.reader = None;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    conversation: Uuid,
    attempt: ToolAttempt,
    storage_failed: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl crate::keyed_tool::StreamSink for RuntimeStreamOutput {
    async fn record(&self, text: &str) -> pi_rs_core::ExtensionResult<()> {
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
    conversation: Uuid,
    attempt: ToolAttempt,
    storage_failed: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl crate::tool_progress::Sink for RuntimeToolProgress {
    async fn update(&self, partial: serde_json::Value) -> pi_rs_core::ExtensionResult<()> {
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
    options: Options,
    tools: HashMap<String, RegisteredTool>,
    provider_adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
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
        let mut harness = Self {
            owner: Arc::new(OwnedStore::open(path)?),
            options,
            tools,
            provider_adapter: None,
            extensions: Arc::new(std::sync::RwLock::new(crate::extension::Registry::default())),
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
            merge_admissions: false,
            recovering: false,
            storage_failed: Arc::new(AtomicBool::new(false)),
            loaded: std::sync::Mutex::new(HashMap::new()),
            execution_gate: None,
            task_driver: Arc::new(std::sync::Mutex::new(None)),
            clock: Arc::new(wall_time_millis),
        };
        harness.recover()?;
        Ok(harness)
    }
    pub(crate) fn worker(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            options: self.options.clone(),
            tools: self.tools.clone(),
            provider_adapter: self.provider_adapter.clone(),
            extensions: self.extensions.clone(),
            failed: false,
            dirty: Arc::new(AtomicBool::new(false)),
            merge_admissions: true,
            recovering: false,
            storage_failed: self.storage_failed.clone(),
            loaded: std::sync::Mutex::new(HashMap::new()),
            execution_gate: None,
            task_driver: self.task_driver.clone(),
            clock: self.clock.clone(),
        }
    }
    pub(crate) async fn drive_owned_cleanup(
        &mut self,
        id: Uuid,
        stop: &CancellationToken,
    ) -> Result<(), Error> {
        let lock = self.owner.conversation_controls().get(id).worker;
        let guard = tokio::select! {
            guard = lock.lock() => guard,
            _ = stop.cancelled() => return Ok(()),
        };
        let control = self.owner.conversation_controls().get(id);
        self.execution_gate = Some(control.gate);
        let mut guard = Some(guard);
        tokio::select! {
            result = drive_scheduled(self, id, &control.cleanup_cancel, &lock, &mut guard, None) => result,
            _ = stop.cancelled() => Ok(()),
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
        Ok(tools)
    }
    fn ordered_tool_specs(&self, c: &Conversation) -> Result<Vec<ToolSpec>, Error> {
        let mut tools = self.resolved_tools(c)?;
        if let Some(allowed) = c.model_tools.as_ref().or(c.allowed_tools.as_ref()) {
            Ok(allowed
                .iter()
                .filter_map(|name| tools.remove(name))
                .map(|tool| tool.tool.spec())
                .collect())
        } else {
            let mut specs: Vec<_> = tools.into_values().map(|tool| tool.tool.spec()).collect();
            specs.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(specs)
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
        self.provider_adapter = Some(adapter);
        Ok(())
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
        self.save(&c, Expected::Version(version))
    }
    /// Persist an exact model-facing tool selection. None offers all enabled tools.
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
    fn hydrate(&self, state: &mut Conversation) -> Result<(), Error> {
        if let Some(cursor) = &state.transcript {
            state.messages = self.read_transcript(state.id, cursor.clone())?;
        }
        Ok(())
    }
    fn read_transcript(
        &self,
        id: Uuid,
        mut cursor: crate::transcript::Cursor,
    ) -> Result<Vec<Message>, Error> {
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut parts = Vec::new();
        loop {
            let page = transcript.read(&cursor, std::num::NonZeroUsize::new(128).unwrap())?;
            for record in page.records {
                let change: TranscriptChange = serde_json::from_value(record.value)?;
                let (messages, base, _) = change.parts()?;
                parts.push(messages);
                if base {
                    return Ok(parts.into_iter().rev().flatten().collect());
                }
            }
            if !page.has_more {
                return Err(Error::Invalid("transcript has no base".into()));
            }
            cursor = page.next;
        }
    }
    fn expand_event(&self, id: Uuid, value: &mut serde_json::Value) -> Result<(), Error> {
        if let Some(reference) = value.get_mut("_transcript").map(std::mem::take) {
            let cursor = serde_json::from_value(reference)?;
            value["messages"] = serde_json::to_value(self.read_transcript(id, cursor)?)?;
            value
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("invalid observation".into()))?
                .remove("_transcript");
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
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
                    if self.merge_admissions =>
                {
                    let entry = self.owner.store().get(&key)?.ok_or(Error::NotFound)?;
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
                    // Settings changes can race external model/tool calls. Preserve the
                    // newest values; boundary commits retry selection separately.
                    state.queue_modes = latest.queue_modes;
                    state.after_failed_run = latest.after_failed_run;
                    for marker in latest.pending_resets.iter().skip(base.pending_resets.len()) {
                        state.pending_resets.push(marker.clone());
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
        if self.failed
            || self.dirty.load(Ordering::Acquire)
            || self.storage_failed.load(Ordering::Acquire)
        {
            return Err(Error::Failed);
        }
        let mut state = state.clone();
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
        let mut append = crate::observer::Observer::new(self.owner.store(), state.id)
            .prepare_append(&serde_json::json!({}))?;
        let commit = append.cursor.0;
        let create = old.and_then(|s| s.transcript.as_ref()).is_none();
        let change = match old {
            Some(previous) if !create && state.messages == previous.messages => None,
            Some(previous) if !create && state.messages.starts_with(&previous.messages) => Some(
                TranscriptChange::Append(state.messages[previous.messages.len()..].to_vec()),
            ),
            _ => Some(TranscriptChange::Replace(state.messages.clone())),
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
                    &state.allowed_tools, &state.model_tools, &state.resets, &state.application_writes, &state.tool_states,
                )), serde_json::to_value((
                    &previous.queue_modes, &previous.after_failed_run, &previous.extensions, &previous.provider_adapter,
                    &previous.allowed_tools, &previous.model_tools, &previous.resets, &previous.application_writes, &previous.tool_states,
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
                _ => unreachable!(),
            }
        });
        let prepared = if expected == Expected::Missing && state.transcript.is_some() {
            let source = state.transcript.as_ref().unwrap();
            if self.read_transcript(source.transcript_id(), source.clone())? != state.messages {
                return Err(Error::Invalid("fork transcript does not match seed".into()));
            }
            let (batch, cursor) =
                crate::transcript::Transcript::new(self.owner.store(), source.transcript_id())
                    .prepare_fork_at(source, state.id)?;
            state.transcript = Some(cursor);
            Some(batch)
        } else if let Some(change) = change {
            let (batch, cursor) = crate::transcript::Transcript::new(self.owner.store(), state.id)
                .prepare_write(&serde_json::to_value(change)?, create)?;
            state.transcript = Some(cursor);
            Some(batch)
        } else {
            state.transcript = old.and_then(|s| s.transcript.clone());
            None
        };
        let mut persisted = state.clone();
        persisted.messages.clear();
        let value = serde_json::to_vec(&persisted)?;
        let event = serde_json::json!({"_transcript": state.transcript, "submissions": state.submissions, "aborted_generations": state.aborted_generations, "resets": state.resets, "abort_requested": state.abort_requested, "application_writes": state.application_writes});
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
                }
            }
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
        match self.owner.store().commit(&batch.checks, &batch.mutations) {
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
            messages: vec![],
            context_generation: Some(Uuid::new_v4()),
            transcript: None,
            submissions: vec![],
            manual_compactions: vec![],
            active_manual_compaction: None,
            aborted_generations: vec![],
            resets: vec![],
            pending_resets: vec![],
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
            tool_attempt: None,
            abort_requested: false,
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
        if !matches!(state.phase, Phase::Idle | Phase::FailedIdle)
            || state.active.is_some()
            || !state.pending_resets.is_empty()
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
        {
            return Err(Error::Invalid(
                "fork requires a settled conversation".into(),
            ));
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
        if let Some((len, expected_len)) = prefix {
            let messages = value
                .get_mut("messages")
                .and_then(serde_json::Value::as_array_mut)
                .ok_or(crate::fork::Error::InvalidSnapshot)?;
            if messages.len() != expected_len || len > messages.len() {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            }
            messages.truncate(len);
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
            messages: point.messages,
            context_generation: Some(Uuid::new_v4()),
            transcript: if prefix.is_some() { None } else { transcript },
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
            tool_attempt: None,
            abort_requested: false,
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
        let page = transcript.read(&position.cursor, std::num::NonZeroUsize::MIN)?;
        let record = page
            .records
            .first()
            .ok_or(crate::fork::Error::InvalidCursor)?;
        let change: TranscriptChange = serde_json::from_value(record.value.clone())?;
        let stable_prefix = match &change {
            TranscriptChange::AppendV3 { stable_prefix, .. }
            | TranscriptChange::AppendV4 { stable_prefix, .. } => Some(*stable_prefix),
            _ => None,
        };
        let (messages, _, metadata) = change.parts()?;
        let (_, commit, origin) = metadata.ok_or(crate::fork::Error::InvalidSnapshot)?;
        if origin != id {
            return Err(crate::fork::Error::InvalidSnapshot.into());
        }
        let count = position
            .remaining
            .ok_or(crate::fork::Error::InvalidCursor)?;
        if messages.is_empty() || count == 0 || count > messages.len() {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let observer = crate::observer::Observer::new(self.owner.store(), id);
        let mut cursor = crate::observer::Cursor(0);
        loop {
            let page = observer.read(cursor, std::num::NonZeroUsize::new(128).unwrap())?;
            for record in &page.records {
                let reference = record.value.get("_transcript").cloned();
                let Some(reference) = reference else { continue };
                let reference: Option<crate::transcript::Cursor> =
                    serde_json::from_value(reference)?;
                if reference.as_ref() == Some(&position.cursor) {
                    if record.cursor.0 != commit {
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
                    return self.fork_at_cut(id, record.cursor, Some((total, expected_len)));
                }
            }
            if !page.has_more {
                break;
            }
            cursor = page.next;
        }
        Err(crate::fork::Error::InvalidSnapshot.into())
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
                let batch = transcript.read(&reference, std::num::NonZeroUsize::MIN)?;
                let Some(batch) = batch.records.first() else {
                    continue;
                };
                let (_, _, metadata) =
                    serde_json::from_value::<TranscriptChange>(batch.value.clone())?.parts()?;
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
            let page = transcript.read(&position, std::num::NonZeroUsize::MIN)?;
            let Some(batch) = page.records.first() else {
                return Err(crate::fork::Error::InvalidSnapshot.into());
            };
            let change: TranscriptChange = serde_json::from_value(batch.value.clone())?;
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
        self.load(id).map(|x| x.0)
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
            reader: Some(self.worker()),
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
                        history: metadata
                            .as_ref()
                            .map(|(ids, commit, origin)| MessageHistory {
                                id: ids[index],
                                commit: *commit,
                                origin: *origin,
                                commit_boundary: index + 1 == batch_len,
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
        Ok(crate::task_scheduler::Scheduler::attach(
            self.owner.clone(),
            registry,
        )?)
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
                && !state.submissions.iter().any(|s| s.status == Status::Queued)
            {
                place_reset(&mut state, marker.clone());
                state.phase = Phase::Idle;
            } else {
                state.pending_resets.push(marker.clone());
            }
            match self.save(&state, Expected::Version(version)) {
                Ok(()) => return Ok(marker.id),
                Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                Err(e) => return Err(e),
            }
        }
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
                placed: c.pending_resets.is_empty(),
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
    /// Start an idle manual compaction and return its durable operation ID.
    /// None means there is no older complete turn to compact.
    pub fn request_manual_compaction(
        &mut self,
        id: Uuid,
        keep_recent_tokens: usize,
        instructions: Option<&str>,
    ) -> Result<Option<Uuid>, Error> {
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle) || c.active.is_some() || c.abort_requested {
            return Err(Error::Invalid(
                "compaction requires an idle conversation".into(),
            ));
        }
        let Some(preparation) = crate::compact::prepare(
            &c.messages,
            c.config.model.clone(),
            c.config.max_output_tokens,
            keep_recent_tokens,
            instructions,
        )?
        else {
            return Ok(None);
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
                status: if place_now {
                    Status::Running
                } else {
                    Status::Queued
                },
            });
            c.input_modes.insert(sid, options.mode);
            if place_now {
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
            if c.active.is_some() || !matches!(c.phase, Phase::Idle | Phase::FailedIdle) {
                return Err(AdmissionError::Busy.into());
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
                status: Status::Running,
            });
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
            .map(|(conversations, _)| conversations)
    }

    fn abort_owned_graph_impl(
        &mut self,
        conversation: Option<Uuid>,
        task: Option<Uuid>,
        receipt: Option<(Vec<u8>, Vec<u8>)>,
        background: bool,
    ) -> Result<(Vec<Uuid>, Vec<Uuid>), Error> {
        use crate::ownership_edge::{EdgeStore, Mode};
        use crate::task::TaskOwner;
        use std::collections::{HashMap, HashSet, VecDeque};
        let owner = self.owner.clone();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Failed)?;
        loop {
            let store = owner.store();
            if let Some((key, _)) = &receipt
                && store.get(key)?.is_some()
            {
                return Ok((Vec::new(), Vec::new()));
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
                if !record.task.is_terminal() && !record.task.abort_requested() {
                    record
                        .task
                        .request_abort()
                        .map_err(|e| Error::Invalid(format!("task abort: {e:?}")))?;
                }
                updates.push(record);
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
            for (state, _) in &mut states {
                for submission in &mut state.submissions {
                    if submission.status == Status::Queued {
                        submission.status = Status::Unanswered {
                            reason: "aborted".into(),
                        };
                    }
                }
                state.abort_requested = true;
            }
            let refs: Vec<_> = states
                .iter()
                .map(|(s, v)| (s, Expected::Version(*v)))
                .collect();
            let mut batch = self.prepare_saves(&refs)?;
            let task_batch = crate::task_store::TaskStore::new(store)
                .prepare_save(
                    &updates
                        .iter()
                        .map(crate::task_store::Save::Update)
                        .collect::<Vec<_>>(),
                )
                .map_err(|e| Error::Invalid(format!("task batch: {e}")))?;
            batch.checks.extend(task_batch.checks);
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
            for (id, generation) in generation_updates {
                batch.mutations.push(Mutation::Put {
                    key: crate::subagent::generation_key(id),
                    value: serde_json::to_vec(&generation)?,
                });
            }
            batch.mutations.extend(task_batch.mutations);
            match self.commit_conversation_save(batch) {
                Ok(()) => {
                    return Ok((
                        conversations.into_iter().collect(),
                        tasks.into_iter().collect(),
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
                } => {
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
                            .filter(|call| {
                                c.allowed_tools
                                    .as_ref()
                                    .is_none_or(|names| names.contains(&call.name))
                                    && c.model_tools
                                        .as_ref()
                                        .is_none_or(|names| names.contains(&call.name))
                            })
                            .and_then(|x| self.tools.get(&x.name))
                            .map(|x| x.replay)
                    } else {
                        None
                    };
                    // A panic can be indexed before the blocked phase is committed,
                    // including while nested calls are being sealed. Never replay it.
                    let panic_key = format!(
                        "runtime/extension-report/{id}/{operation_id}/tool_execution_panic"
                    );
                    let panicked = self.owner.store().get(panic_key.as_bytes())?.is_some();
                    match if panicked {
                        ToolRecovery::Reconcile
                    } else {
                        recover_tool(
                            replay,
                            current,
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
                            }
                        }
                        ToolRecovery::Reconcile => {
                            c.phase = Phase::Blocked {
                                tool_panicked: panicked,
                                turns,
                                calls,
                                index,
                                operation_id,
                                reason: "interrupted; external outcome requires reconciliation"
                                    .into(),
                                arguments: Some(arguments),
                            }
                        }
                        ToolRecovery::Interrupted | ToolRecovery::RestartLimit => {
                            let reason = if n >= self.options.restart_limit {
                                "tool restart limit"
                            } else {
                                "tool interrupted and may have partially run"
                            };
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
    async fn settle_abort(
        &mut self,
        mut c: Conversation,
        version: u64,
        cancel: &CancellationToken,
    ) -> Result<bool, Error> {
        // Keep the conversation marked until all owned cleanup has finished.
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
                content,
                ..
            } => {
                // The effect already committed; retain its result even if an
                // after-tool hook was interrupted by the abort.
                append_tool(&mut c, &calls, index, content)?;
                for i in index + 1..calls.len() {
                    append_tool(
                        &mut c,
                        &calls,
                        i,
                        Outcome::Skipped("aborted before dispatch".into()).tool_content(),
                    )?;
                }
            }
            Phase::Tools { calls, index, .. }
            | Phase::ToolPreparing { calls, index, .. }
            | Phase::ToolGateSleeping { calls, index, .. } => {
                for i in index..calls.len() {
                    append_tool(
                        &mut c,
                        &calls,
                        i,
                        Outcome::Skipped("aborted before dispatch".into()).tool_content(),
                    )?;
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
        let snapshot = serde_json::to_value(c)?;
        let memo = self.extension_memo(operation_id)?;
        let context = crate::extension::Context {
            snapshot: &snapshot,
            memo: &memo,
        };
        let (_, continuation, tasks, reports) = fenced_poll(
            selected.lifecycle_generation(event, value, &context),
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
            match c.phase.clone() {
                Phase::Idle => {
                    if !c.pending_resets.is_empty() {
                        for marker in std::mem::take(&mut c.pending_resets) {
                            place_reset(&mut c, marker);
                        }
                        self.save(&c, Expected::Version(version))?;
                        continue;
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
                        let prior: Vec<_> = c.pending_resets.drain(..before.len()).collect();
                        for reset in prior {
                            place_reset(&mut c, reset);
                        }
                    }
                    if c.active_manual_compaction.is_some()
                        && result.is_stale(&c.messages, c.context_generation)
                    {
                        finish_manual_compaction(&mut c, ManualCompactionStatus::Stale)?;
                    } else {
                        c.messages = result.apply_in_context(&c.messages, c.context_generation)?;
                        c.context_generation = Some(Uuid::new_v4());
                        finish_manual_compaction(&mut c, ManualCompactionStatus::Applied)?;
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
                        finish_failure(&mut c, "nothing_to_answer".into());
                    } else if turns >= self.options.max_turns {
                        finish_failure(&mut c, "turn limit".into());
                    } else {
                        if !c.auto_compaction_attempted {
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
                                let estimated = pi_rs_core::estimate_request_tokens(&request);
                                if estimated >= window.saturating_sub(reserve) {
                                    c.auto_compaction_attempted = true;
                                    let message_tokens: u64 = c
                                        .messages
                                        .iter()
                                        .map(pi_rs_core::estimate_message_tokens)
                                        .sum();
                                    let overhead = estimated.saturating_sub(message_tokens);
                                    let keep =
                                        window.saturating_sub(reserve).saturating_sub(overhead) / 2;
                                    if let Some(preparation) = crate::compact::prepare(
                                        &c.messages,
                                        c.config.model.clone(),
                                        c.config.max_output_tokens,
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
                    if !c.extensions.is_empty() {
                        let selected = self.selected_extensions(&c)?;
                        let snapshot = serde_json::to_value(&c)?;
                        let memo = self.extension_memo(request_id)?;
                        let context = crate::extension::Context {
                            snapshot: &snapshot,
                            memo: &memo,
                        };
                        let value = serde_json::json!({"messages": request.messages});
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
                        request.messages =
                            serde_json::from_value(value.get("messages").cloned().ok_or_else(
                                || Error::Invalid("before-request hook omitted messages".into()),
                            )?)
                            .map_err(|e| {
                                Error::Invalid(format!("invalid before-request messages: {e}"))
                            })?;
                    }
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
                            adapter.prepare(&attempt, &request),
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
                        adapter.validate(&attempt, &request, &body)?;
                        Some((adapter, attempt, body))
                    } else {
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
                                let committed = snapshot.prepare_checkpoint().and_then(|batch| {
                                    store.commit(&batch.checks, &batch.mutations).map_err(crate::progress::Error::from)
                                }).and_then(|_| crate::progress::Snapshot::load(store, request_id));
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
                            start_overflow_compaction(&mut c, turns, reason)?
                        }
                        ModelFailureDecision::Stop => finish_failure(&mut c, reason),
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::RetryWait { turns } => {
                    match c.retry_state.poll(unix_ms()) {
                        Decision::WaitUntil(deadline) => {
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
                        finish_failure(&mut c, "model restart limit".into());
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
                        start_overflow_compaction(&mut c, turns, "model context overflow".into())?;
                        self.save(&c, Expected::Version(version))?;
                        continue;
                    }
                    c.retry_state = RetryState::default();
                    c.overflow_compaction_attempted = false;
                    if response.is_truncated() && !response.calls.is_empty() {
                        c.messages.push(response.assistant_message());
                        for index in 0..response.calls.len() {
                            append_tool(
                                &mut c,
                                &response.calls,
                                index,
                                Outcome::Skipped("tool call truncated before dispatch".into())
                                    .tool_content(),
                            )?;
                        }
                        finish_failure(&mut c, "truncated tool calls".into());
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
                    let continuation = match continuation {
                        Some(serde_json::Value::String(text)) => Some(text),
                        Some(_) => {
                            return Err(Error::Invalid(
                                "OnYield continuation must be a string".into(),
                            ));
                        }
                        None => None,
                    };
                    if let Some(text) = continuation.filter(|_| {
                        c.pending_resets.is_empty()
                            && !c.submissions.iter().any(|s| s.status == Status::Queued)
                    }) {
                        c.messages.push(Message::User { content: text });
                        c.auto_compaction_attempted = false;
                        c.phase = Phase::ModelReady { turns: 0 };
                    } else {
                        c.phase = Phase::Boundary {
                            turns,
                            answer: Some(answer),
                        };
                    }
                    // The inbox must be checked atomically with the decision.
                    let merge = self.merge_admissions;
                    self.merge_admissions = false;
                    let result = self.save(&c, Expected::Version(version));
                    self.merge_admissions = merge;
                    match result {
                        Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                        other => other?,
                    }
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
                            && let Some(text) = value.get("continue").and_then(|v| v.as_str())
                            && !text.trim().is_empty()
                        {
                            continuation = Some(text.to_owned());
                        }
                    }
                    if let Some(text) = continuation.filter(|_| {
                        c.pending_resets.is_empty()
                            && !c.submissions.iter().any(|s| s.status == Status::Queued)
                    }) {
                        c.messages.push(Message::User { content: text });
                        c.auto_compaction_attempted = false;
                        c.phase = Phase::ModelReady { turns: 0 };
                    } else {
                        c.phase = Phase::Boundary {
                            turns,
                            answer: Some(answer),
                        };
                    }
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
                        finish_failure(&mut c, "reset".into());
                        c.phase = Phase::Idle;
                        for marker in std::mem::take(&mut c.pending_resets) {
                            place_reset(&mut c, marker);
                        }
                        place_queued(&mut c, true);
                        if c.active.is_some() {
                            c.phase = Phase::ModelReady { turns: 0 };
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
                        if turns.is_some()
                            && matches!(task.state(), crate::task::TaskState::Succeeded(value)
                                if value.as_object().is_some_and(|object|
                                    object.len() == 1 && object.get("terminate") == Some(&serde_json::Value::Bool(true))))
                        {
                            terminate = true;
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
                        .filter(|_| {
                            c.model_tools
                                .as_ref()
                                .is_none_or(|names| names.contains(&calls[index].name))
                        })
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
                                }
                            }
                            Err(outcome) => {
                                append_tool(&mut c, &calls, index, outcome.tool_content())?;
                                c.phase = Phase::Tools {
                                    turns,
                                    calls,
                                    index: index + 1,
                                };
                            }
                        }
                    } else {
                        append_tool(&mut c, &calls, index, "tool unavailable".into())?;
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
                    let Some(reg) = tools.get(&call.name).filter(|_| {
                        c.model_tools
                            .as_ref()
                            .is_none_or(|names| names.contains(&call.name))
                    }) else {
                        let arguments = pi_rs_core::parse_tool_arguments(&call.arguments)
                            .unwrap_or(serde_json::Value::Null);
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
                        if (sleep.until_millis.is_none() && sleep.signals.is_empty())
                            || sleep.until_millis.is_some_and(|time| time < 0)
                            || sleep.signals.iter().any(String::is_empty)
                        {
                            return Err(Error::Invalid("invalid extension sleep".into()));
                        }
                        let wait = crate::conversation_signal::Inbox::new(self.owner.store())
                            .prepare_wait(id, &sleep.signals)?;
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
                            Err(Error::Storage(durable_store::Error::Conflict { .. })) => continue,
                            other => other?,
                        }
                        return Ok(());
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
                                }
                            }
                            Err(outcome) => {
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
                    replay_ready,
                    ..
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
                    let control =
                        Arc::new(std::sync::Mutex::new(crate::reset::ToolControl::default()));
                    let mut tool_panicked = false;
                    let reply = if let Some(reg) = tools.get(&call.name).filter(|_| {
                        c.model_tools
                            .as_ref()
                            .is_none_or(|names| names.contains(&call.name))
                    }) {
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
                                match crate::nested::NestedHost::new(
                                    self.owner.clone(),
                                    operation_id,
                                    tools.values().cloned().collect(),
                                    cancel.child_token(),
                                    Duration::from_secs(120),
                                    8,
                                ) {
                                    Ok(host) => {
                                        let scope = c.tool_attempt().map(|attempt| {
                                            crate::tool_progress::scoped_host(
                                                host.clone(),
                                                Arc::new(RuntimeToolProgress {
                                                    owner: self.owner.clone(),
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
                                                crate::keyed_tool::scope_with_control(
                                                    host.clone(),
                                                    reg.tool.clone(),
                                                    invocation.operation_id,
                                                    progress_host.clone(),
                                                    control.clone(),
                                                    c.tool_attempt().map(|attempt| {
                                                        Arc::new(RuntimeStreamOutput {
                                                            owner: Arc::downgrade(&self.owner),
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
                                            )
                                            .catch_unwind(),
                                            self.execution_gate.as_ref(),
                                            cancel,
                                        )
                                        .await;
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
                    if let Some(state) = reply.state {
                        c.tool_states.insert(call.name.clone(), state);
                    }
                    match reply.outcome {
                        Outcome::Unknown(reason) => {
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
                            // Older persisted rounds may predate either control vector.
                            c.round_terminates.resize(calls.len(), false);
                            c.round_handoffs.resize(calls.len(), None);
                            c.round_added_tools.resize(calls.len(), Vec::new());
                            let control = control.lock().expect("control mutex poisoned");
                            c.round_terminates[index] = control.terminate;
                            c.round_handoffs[index] = control.handoff.clone();
                            c.round_added_tools[index] = control.add_tools.clone();
                            c.phase = Phase::ToolFinished {
                                turns,
                                calls,
                                index,
                                operation_id,
                                arguments,
                                content: other.tool_content(),
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
                    let snapshot = serde_json::to_value(&c)?;
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
                    let content = result.as_str().unwrap_or(&content).to_owned();
                    append_tool(&mut c, &calls, index, content)?;
                    c.phase = Phase::Tools {
                        turns,
                        calls,
                        index: index + 1,
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
                        crate::poll::PollResult::Cancelled => return Err(Error::Interrupted),
                        crate::poll::PollResult::Unavailable
                        | crate::poll::PollResult::TimedOut => return Ok(()),
                    }
                    self.save(&c, Expected::Version(version))?;
                }
                Phase::FailedIdle if !c.pending_resets.is_empty() => {
                    for marker in std::mem::take(&mut c.pending_resets) {
                        place_reset(&mut c, marker);
                    }
                    c.phase = Phase::Idle;
                    self.save(&c, Expected::Version(version))?;
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
        if c.abort_requested {
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
        if c.abort_requested {
            if !self.conversation_tasks(c.id)?.is_empty() {
                return Err(Error::Invalid("owned task cleanup is still pending".into()));
            }
            append_tool(&mut c, &calls, index, outcome.tool_content())?;
            for i in index + 1..calls.len() {
                append_tool(
                    &mut c,
                    &calls,
                    i,
                    Outcome::Skipped("aborted before dispatch".into()).tool_content(),
                )?;
            }
            c.abort_requested = false;
            finish_abort(&mut c);
        } else if c.extensions.is_empty() {
            append_tool(&mut c, &calls, index, outcome.tool_content())?;
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
            _ => None,
        })
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
}

impl Scheduler {
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
        ])?;
        let mut records = Vec::new();
        let mut edges = Vec::new();
        let mut snapshots = HashMap::new();
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
                busy |= conversation.is_working();
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
        Self::open_configured_with_tasks(path, options, registry, management, adapter, None)
            .map(|(scheduler, _)| scheduler)
    }
    fn open_configured_with_tasks(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        management: Option<(Uuid, Config, Option<Vec<String>>)>,
        adapter: Option<Arc<dyn crate::model::ProviderAdapter>>,
        tasks: Option<crate::task_scheduler::Registry>,
    ) -> Result<(Self, Option<crate::task_driver::Driver>), Error> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("scheduler requires a Tokio runtime".into()))?;
        let mut harness = Harness::open_paused(path, options)?;
        if let Some(adapter) = adapter {
            harness.install_provider_adapter(adapter)?;
        }
        *harness.extensions.write().map_err(|_| Error::Failed)? = registry;
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
        let mut scheduler = Self {
            cancel: harness.owner.conversation_controls().root(),
            harness,
            tasks: HashMap::new(),
            reporter_cancel: CancellationToken::new(),
            reporter: None,
            reporter_error: Arc::new(std::sync::Mutex::new(None)),
            worker_errors: Arc::new(std::sync::Mutex::new(HashMap::new())),
        };
        // Attach and publish the task driver before any recovered worker is spawned.
        let driver = tasks
            .map(|registry| scheduler.start_tasks(registry))
            .transpose()?;
        for id in ids {
            scheduler.schedule(id);
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

    fn abort_background_graph(&mut self, id: Uuid) -> Result<(Vec<Uuid>, Vec<Uuid>), Error> {
        let owner = self.harness.owner.clone();
        let mut tasks = Vec::new();
        let affected = owner
            .conversation_controls()
            .fence(|| {
                let (conversations, reached) =
                    self.harness
                        .abort_owned_graph_impl(Some(id), None, None, true)?;
                tasks = reached;
                Ok(conversations)
            })
            .map_err(|error| match error {
                durable_store::controls::FenceError::Poisoned => Error::Failed,
                durable_store::controls::FenceError::Commit(error) => error,
            })?;
        for conversation in &affected {
            self.schedule(*conversation);
        }
        Ok((affected, tasks))
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
        // Prevent controls from being created and fence every existing poll while
        // discovering and committing the graph. A child may be admitted right up
        // to the transaction, so fencing a previously read graph is insufficient.
        let owner = self.harness.owner.clone();
        let affected = owner
            .conversation_controls()
            .fence(|| self.harness.abort_owned_graph(conversation, task))
            .map_err(|error| match error {
                durable_store::controls::FenceError::Poisoned => Error::Failed,
                durable_store::controls::FenceError::Commit(error) => error,
            })?;
        for id in &affected {
            self.schedule(*id);
        }
        Ok(affected)
    }
    /// Request abort and join workers for every affected foreground conversation.
    pub async fn abort_and_wait(&mut self, id: Uuid) -> Result<(), Error> {
        let affected = self.abort_graph(Some(id), None)?;
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
        Ok(())
    }
    /// Abort across background boundaries and wait for the reached work to settle.
    /// A task driver must be running for any nonterminal generic tasks.
    pub async fn abort_and_wait_with_background(&mut self, id: Uuid) -> Result<(), Error> {
        let (conversations, tasks) = self.abort_background_graph(id)?;
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
                return Ok(());
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
        let task = tokio::spawn(async move {
            let mut guard = Some(lock.lock().await);
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
                            active.insert(
                                key,
                                tokio::spawn(async move {
                                    let Some(child) = worker.reporter_child(&reporter)? else {
                                        return Ok(());
                                    };
                                    let lock =
                                        worker.owner.conversation_controls().get(child).worker;
                                    {
                                        let mut guard = Some(lock.lock().await);
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
                                        )
                                        .await?;
                                    }
                                    if let Some(parent) =
                                        worker.deliver_report(&task_key, &reporter)?
                                    {
                                        let lock =
                                            worker.owner.conversation_controls().get(parent).worker;
                                        let mut guard = Some(lock.lock().await);
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
    scheduler: Scheduler,
    driver: crate::task_driver::Driver,
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
        )?;
        Ok(Self {
            scheduler,
            driver: driver.expect("task registry supplied"),
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

    /// Cancel and join conversations while their task driver remains available,
    /// then stop and join the driver even if conversation shutdown failed.
    /// Await this future to completion for deterministic release of ownership.
    pub async fn shutdown(self) -> Result<(), Error> {
        let Self { scheduler, driver } = self;
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
) -> Result<(), Error> {
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
                Phase::ToolGateSleeping { .. }
                | Phase::HookTasksWaiting { .. }
                | Phase::YieldTasksWaiting { .. }
                | Phase::OnYield { .. }
                | Phase::AfterTools { .. }
                | Phase::AfterRun { .. } => true,
                _ => false,
            }
        };
        if !should_poll {
            return Ok(());
        }
        // `drive` has returned at a persisted wait boundary. No dispatch future
        // remains live, so another worker can safely inspect and drive it.
        drop(guard.take());
        tokio::select! { _ = cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(1)) => () }
        *guard = Some(tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            guard = lock.lock() => guard,
        });
        if cancel.is_cancelled() {
            return Ok(());
        }
        // This driver has taken over from any owner that failed while it was
        // parked. Publish that takeover before attempting recovery, just as a
        // newly scheduled driver does after acquiring the worker lock.
        if let Some(errors) = errors {
            errors.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
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

fn start_overflow_compaction(
    c: &mut Conversation,
    turns: u32,
    reason: String,
) -> Result<(), Error> {
    if c.overflow_compaction_attempted {
        finish_failure(c, reason);
        return Ok(());
    }
    c.overflow_compaction_attempted = true;
    c.auto_compaction_attempted = true;
    if let Some(preparation) = crate::compact::prepare(
        &c.messages,
        c.config.model.clone(),
        c.config.max_output_tokens,
        0,
        None,
    )? {
        let generation = *c.context_generation.get_or_insert_with(Uuid::new_v4);
        c.compaction_return_turns = Some(turns);
        c.phase = Phase::CompactionHook {
            preparation: Box::new(preparation.bind_context(generation)),
            operation_id: Uuid::new_v4(),
            reason: crate::extension::CompactionReason::Overflow,
        };
    } else {
        finish_failure(c, reason);
    }
    Ok(())
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        // The poller is otherwise perpetual and would keep the owner alive.
        self.reporter_cancel.cancel();
    }
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
        && modes_unchanged
        && resets_unchanged
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

fn place_reset(c: &mut Conversation, marker: crate::reset::Reset) {
    c.context_generation = Some(Uuid::new_v4());
    c.messages.clear();
    if let Some(message) = marker.message() {
        c.messages.push(message);
    }
    for write in &mut c.application_writes {
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
// Final boundaries select each mode independently; post-tool boundaries select only steers.
fn place_queued(c: &mut Conversation, final_boundary: bool) -> bool {
    let mut steer = false;
    let mut follow = false;
    let mut placed = false;
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
        submission.status = Status::Running;
        c.active.get_or_insert(submission.id);
        c.messages.push(Message::User {
            content: submission.content.clone(),
        });
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
        AfterFailedRun::Continue => c.phase = Phase::Idle,
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
    finish_failure(c, "aborted".into());
    if c.submissions.iter().any(|s| s.status == Status::Queued) {
        c.phase = Phase::Idle;
    }
}
fn finish_failure(c: &mut Conversation, reason: String) {
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
            messages: vec![],
            context_generation: Some(Uuid::new_v4()),
            transcript: None,
            submissions: vec![],
            manual_compactions: vec![],
            active_manual_compaction: None,
            aborted_generations: vec![],
            resets: vec![],
            pending_resets: vec![],
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
            tool_attempt: None,
            abort_requested: false,
            phase: Phase::Idle,
        };
        let mut latest = base.clone();
        latest.submissions.push(Submission {
            kind: SubmissionKind::Input,
            id: Uuid::new_v4(),
            request_id: None,
            content: "queued".into(),
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
