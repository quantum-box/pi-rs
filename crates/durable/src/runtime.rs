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
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error("session failed; reopen before further operations")]
    Failed,
    #[error("execution interrupted; reopen to recover")]
    Interrupted,
    #[error("invalid operation: {0}")]
    Invalid(String),
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
    /// The last reset preceding this write, if any.
    pub after_reset: Option<Uuid>,
}
/// Result of attempting to withdraw a single input.
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
    /// Installed extension names in execution order. Older conversations select none.
    #[serde(default)]
    pub extensions: Vec<String>,
    /// Stable adapter identity, if provider-body hooks are enabled.
    #[serde(default)]
    pub provider_adapter: Option<String>,
    /// Enabled tool names; None preserves the host default, Some([]) disables all.
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transcript: Option<crate::transcript::Cursor>,
    pub submissions: Vec<Submission>,
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

impl Conversation {
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

pub struct Harness {
    owner: Arc<OwnedStore>,
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
        match self.owner.store().commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&batch)?,
            }],
        ) {
            Ok(()) | Err(durable_store::Error::Conflict { .. }) => Ok(()),
            Err(e) => {
                self.storage_failed.store(true, Ordering::Release);
                Err(e.into())
            }
        }
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
        let _guard = tokio::select! {
            guard = lock.lock() => guard,
            _ = stop.cancelled() => return Ok(()),
        };
        let control = self.owner.conversation_controls().get(id);
        self.execution_gate = Some(control.gate);
        tokio::select! {
            result = drive_scheduled(self, id, &control.cancel) => result,
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
    fn key(id: Uuid) -> Vec<u8> {
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
        let mut expected = expected;
        loop {
            let mut prepared = if wake.is_none() && children.is_empty() && abort_tasks.is_empty() {
                self.prepare_saves(&[(&state, expected.clone())])?
            } else {
                self.prepare_conversation_save(
                    &state,
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
                    for marker in latest.pending_resets.iter().skip(base.pending_resets.len()) {
                        state.pending_resets.push(marker.clone());
                    }
                    for mut write in latest
                        .application_writes
                        .iter()
                        .skip(base.application_writes.len())
                        .cloned()
                    {
                        if write
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
                        &state.extensions,
                        &state.provider_adapter,
                        &state.allowed_tools,
                        &state.resets,
                        &state.application_writes,
                        &state.tool_states,
                    )), serde_json::to_value((
                            &previous.config,
                            &previous.queue_modes,
                            &previous.extensions,
                            &previous.provider_adapter,
                            &previous.allowed_tools,
                            &previous.resets,
                            &previous.application_writes,
                            &previous.tool_states,
                        ))), (Ok(a), Ok(b)) if a == b)
            })
            .map(|previous| previous.messages.len());
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
            extensions: vec![],
            provider_adapter: None,
            allowed_tools: None,
            messages: vec![],
            transcript: None,
            submissions: vec![],
            aborted_generations: vec![],
            resets: vec![],
            pending_resets: vec![],
            application_writes: vec![],
            tool_states: HashMap::new(),
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
        state
            .extensions
            .retain(|name| !name.starts_with(crate::subagent_management::EXTENSION_PREFIX));
        state.config = config;
        state.submissions.clear();
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
        let (mut state, _) = self.load(id)?;
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
        state.active = None;
        state.abort_requested = false;
        state.submissions.clear();
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
        self.save(&state, Expected::Missing)?;
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
            messages: point.messages,
            transcript: if prefix.is_some() { None } else { transcript },
            submissions: vec![],
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
        self.save(&state, Expected::Missing)?;
        Ok(state.id)
    }
    /// Fork at the commit that first exposed this complete message batch.
    /// Interior positions require metadata proving unchanged fork state across
    /// the append; incomplete tool transcripts and legacy metadata are rejected.
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
            TranscriptChange::AppendV3 { stable_prefix, .. } => Some(*stable_prefix),
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
    /// Interior messages require a proven stable-state append; legacy entries and
    /// entries originating in another conversation are rejected. Retained
    /// observations are scanned in bounded pages, including across resets.
    pub fn fork_by_entry_id(&mut self, id: Uuid, entry_id: Uuid) -> Result<Uuid, Error> {
        self.load(id)?;
        if entry_id.is_nil() {
            return Err(crate::fork::Error::InvalidCursor.into());
        }
        let observer = crate::observer::Observer::new(self.owner.store(), id);
        let transcript = crate::transcript::Transcript::new(self.owner.store(), id);
        let mut cursor = crate::observer::Cursor(0);
        let mut last_reference = None;
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
                if let Some(index) = ids.iter().position(|candidate| *candidate == entry_id) {
                    if origin != id || commit != record.cursor.0 {
                        return Err(crate::fork::Error::InvalidSnapshot.into());
                    }
                    return self.fork_at_position(
                        id,
                        ViewCursor {
                            cursor: reference,
                            remaining: Some(index + 1),
                        },
                    );
                }
            }
            if !page.has_more {
                break;
            }
            cursor = page.next;
        }
        Err(crate::fork::Error::InvalidCursor.into())
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
    /// Persist a reset; busy conversations place it at the next safe boundary.
    pub fn reset(&mut self, id: Uuid, handoff: Option<String>) -> Result<Uuid, Error> {
        let marker = crate::reset::Reset::new(Uuid::new_v4(), handoff);
        loop {
            let (mut state, version) = self.load(id)?;
            if matches!(state.phase, Phase::Idle | Phase::FailedIdle)
                && state.active.is_none()
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
        if kind.is_empty() || kind.starts_with("pi.") {
            return Err(Error::Invalid(
                "application entry kind must be nonempty and outside pi.".into(),
            ));
        }
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
        let (mut c, version) = self.load(id)?;
        if !matches!(c.phase, Phase::Idle) || c.active.is_some() {
            return Err(Error::Invalid(
                "compaction requires an idle conversation".into(),
            ));
        }
        let Some(preparation) = crate::compact::prepare(
            &c.messages,
            c.config.model.clone(),
            c.config.max_output_tokens,
            keep_recent_tokens,
            None,
        )?
        else {
            return Ok(false);
        };
        c.phase = Phase::CompactionHook {
            preparation: Box::new(preparation),
            operation_id: Uuid::new_v4(),
            reason: crate::extension::CompactionReason::Manual,
        };
        c.compaction_return_turns = None;
        self.save(&c, Expected::Version(version))?;
        Ok(true)
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
        self.submit_internal_with_options(id, request_id, content, options, &mut |_| Ok(()))
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
        self.submit_internal_with_options(id, request_id, content, options, &mut change)
    }
    pub(crate) fn submit_internal(
        &mut self,
        id: Uuid,
        request_id: Option<String>,
        content: String,
        mode: InputMode,
    ) -> Result<Uuid, Error> {
        self.submit_internal_with_options(
            id,
            request_id,
            content,
            AdmissionOptions {
                mode,
                ..Default::default()
            },
            &mut |_| Ok(()),
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
    ) -> Result<Uuid, Error> {
        loop {
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
            let batch = documents.into_batch();
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
    /// Withdraw an input only while it is still queued. Request IDs remain reserved.
    pub fn withdraw_submission(
        &mut self,
        id: Uuid,
        submission: Uuid,
    ) -> Result<WithdrawResult, Error> {
        loop {
            let (mut c, version) = self.load(id)?;
            let Some(item) = c.submissions.iter_mut().find(|item| item.id == submission) else {
                return Ok(WithdrawResult::NotFound);
            };
            match item.status {
                Status::Queued => {
                    item.status = Status::Unanswered {
                        reason: "aborted".into(),
                    }
                }
                Status::Running => return Ok(WithdrawResult::AlreadyPlaced),
                _ => return Ok(WithdrawResult::Settled),
            }
            match self.save(&c, Expected::Version(version)) {
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
        use crate::ownership_edge::{EdgeStore, Mode};
        use crate::task::TaskOwner;
        use std::collections::{HashMap, HashSet, VecDeque};
        let owner = self.owner.clone();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Failed)?;
        loop {
            let store = owner.store();
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
                        if r.task.owner() == &TaskOwner::Conversation(id) && !r.task.is_background()
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
                        if r.task.owner() == &TaskOwner::Task(id) && !r.task.is_background() {
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
                        if edge.edge.mode == Mode::Foreground {
                            cq.push_back(edge.edge.child);
                        }
                    }
                }
            }
            let mut updates = Vec::new();
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
            batch.checks.extend(edge_checks);
            batch.mutations.extend(task_batch.mutations);
            match self.commit_conversation_save(batch) {
                Ok(()) => return Ok(conversations.into_iter().collect()),
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
                            })
                            .and_then(|x| self.tools.get(&x.name))
                            .map(|x| x.replay)
                    } else {
                        None
                    };
                    match recover_tool(
                        replay,
                        current,
                        n,
                        self.options.restart_limit,
                        self.options.uncertain_policy,
                    ) {
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
    ) -> Result<Option<(Option<serde_json::Value>, Vec<crate::task::Task>)>, Error> {
        if c.extensions.is_empty() {
            return Ok(Some((None, vec![])));
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
            return Ok(Some((continuation, vec![])));
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
        }
        Ok(Some((continuation, children)))
    }
    async fn drive(&mut self, id: Uuid, cancel: &CancellationToken) -> Result<(), Error> {
        loop {
            if cancel.is_cancelled() {
                self.failed = true;
                return Err(Error::Interrupted);
            }
            let (mut c, version) = self.load(id)?;
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
                            c.messages = result.apply(&c.messages)?;
                            c.phase = c
                                .compaction_return_turns
                                .take()
                                .map_or(Phase::Idle, |turns| Phase::ModelReady { turns });
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
                    c.messages = result.apply(&c.messages)?;
                    c.phase = c
                        .compaction_return_turns
                        .take()
                        .map_or(Phase::Idle, |turns| Phase::ModelReady { turns });
                    self.save(&c, Expected::Version(version))?;
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
                                let specs: Vec<_> = self
                                    .resolved_tools(&c)?
                                    .values()
                                    .map(|tool| tool.tool.spec())
                                    .collect();
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
                                        c.compaction_return_turns = Some(turns);
                                        c.phase = Phase::CompactionHook {
                                            preparation: Box::new(preparation),
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
                    let mut specs: Vec<ToolSpec> = self
                        .resolved_tools(&c)?
                        .values()
                        .map(|t| t.tool.spec())
                        .collect();
                    specs.sort_by(|a, b| a.name.cmp(&b.name));
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
                    let Some((continuation, children)) = self
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
                        self.save_composed(&c, Expected::Version(version), None, &children)?;
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
                    let Some((_, children)) = self
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
                        Phase::Boundary {
                            turns,
                            answer: None,
                        }
                    } else {
                        Phase::HookTasksWaiting {
                            children: children.iter().map(|t| t.id()).collect(),
                            turns: Some(turns),
                        }
                    };
                    self.save_composed(&c, Expected::Version(version), None, &children)?;
                }
                Phase::AfterRun {
                    answer,
                    operation_id,
                } => {
                    let Some((_, children)) = self
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
                    self.save_composed(&c, Expected::Version(version), None, &children)?;
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
                    c.phase = if terminate {
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
                    } else if let Some(reg) = self.tools.get(&calls[index].name) {
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
                    let Some(reg) = tools.get(&call.name) else {
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
                    let reply = if let Some(reg) = tools.get(&call.name) {
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
                                            std::panic::AssertUnwindSafe(crate::tool::execute(
                                                reg.tool.as_ref(),
                                                &invocation,
                                                prepared,
                                                c.tool_states.get(&call.name),
                                                progress_host,
                                                cancel,
                                                Duration::from_secs(120),
                                            ))
                                            .catch_unwind(),
                                            self.execution_gate.as_ref(),
                                            cancel,
                                        )
                                        .await;
                                        drop(scope);
                                        match host.seal_and_wait().await {
                                            Ok(()) => match reply {
                                                Some(Ok(reply)) => reply,
                                                None => Outcome::Unknown(
                                                    "local cancellation after dispatch".into(),
                                                )
                                                .into(),
                                                Some(Err(_)) => Outcome::Unknown(
                                                    "parent tool panicked after dispatch".into(),
                                                )
                                                .into(),
                                            },
                                            Err(error) => Outcome::Unknown(format!(
                                                "nested tool requires reconciliation: {error}"
                                            ))
                                            .into(),
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
}

impl Scheduler {
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
    /// Open with a parent-bound, opt-in background subagent extension installed before recovery.
    pub fn open_with_subagent_management(
        path: impl AsRef<Path>,
        options: Options,
        registry: crate::extension::Registry,
        parent: Uuid,
        child_config: Config,
    ) -> Result<Self, Error> {
        Self::open_configured(
            path,
            options,
            registry,
            Some((parent, child_config, None)),
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
        };
        for id in ids {
            scheduler.schedule(id);
        }
        scheduler.start_reporter();
        Ok(scheduler)
    }
    /// Read the same durable extension error log exposed by the harness.
    pub fn extension_reports(&self) -> Result<Vec<ExtensionReportBatch>, Error> {
        self.harness.extension_reports()
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
    pub fn set_queue_modes(&mut self, id: Uuid, modes: QueueModes) -> Result<(), Error> {
        self.harness.set_queue_modes(id, modes)
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
    /// Schedule a retry after an execution error or a dropped worker.
    pub fn schedule(&mut self, id: Uuid) {
        let control = self.harness.owner.conversation_controls().get(id);
        let lock = control.worker;
        let cancel = control.cancel;
        let mut worker = self.harness.worker();
        worker.execution_gate = Some(control.gate);
        let task = tokio::spawn(async move {
            let _lock = lock.lock().await;
            drive_scheduled(&mut worker, id, &cancel).await
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
                            active.insert(
                                key,
                                tokio::spawn(async move {
                                    let child = worker.reporter_child(&reporter)?;
                                    let lock =
                                        worker.owner.conversation_controls().get(child).worker;
                                    {
                                        let _guard = lock.lock().await;
                                        let control =
                                            worker.owner.conversation_controls().get(child);
                                        worker.execution_gate = Some(control.gate);
                                        drive_scheduled(&mut worker, child, &control.cancel)
                                            .await?;
                                    }
                                    if let Some(parent) =
                                        worker.deliver_report(&task_key, &reporter)?
                                    {
                                        let lock =
                                            worker.owner.conversation_controls().get(parent).worker;
                                        let _guard = lock.lock().await;
                                        let control =
                                            worker.owner.conversation_controls().get(parent);
                                        worker.execution_gate = Some(control.gate);
                                        drive_scheduled(&mut worker, parent, &control.cancel)
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
        if let Some(reporter) = self.reporter.take() {
            reporter
                .await
                .map_err(|e| Error::Invalid(format!("reporter task failed: {e}")))?;
        }
        let tasks = std::mem::take(&mut self.tasks);
        let mut error = None;
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

/// Drive a conversation under its scheduler lock and cancellation fence.
pub(crate) async fn drive_scheduled(
    worker: &mut Harness,
    id: Uuid,
    cancel: &CancellationToken,
) -> Result<(), Error> {
    if cancel.is_cancelled() {
        return Ok(());
    }
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
    loop {
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
        tokio::select! { _ = cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(1)) => () }
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
            future.as_mut().poll(cx).map(Some)
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
        c.compaction_return_turns = Some(turns);
        c.phase = Phase::CompactionHook {
            preparation: Box::new(preparation),
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
            new.starts_with(&old)
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

fn place_reset(c: &mut Conversation, marker: crate::reset::Reset) {
    c.messages.clear();
    if let Some(message) = marker.message() {
        c.messages.push(message);
    }
    for write in &mut c.application_writes {
        if write.after_reset == Some(marker.id) {
            write.placed = true;
        }
    }
    c.resets.push(marker);
    c.auto_compaction_attempted = false;
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
    #[test]
    fn legacy_conversation_defaults_and_independent_boundary_selection() {
        let json = serde_json::json!({
            "id": Uuid::new_v4(), "config": config(), "messages": [],
            "submissions": [], "active": null, "phase": "Idle"
        });
        let mut c: Conversation = serde_json::from_value(json).unwrap();
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
    fn conflict_merge_rejects_non_admission_state_changes() {
        let base = Conversation {
            id: Uuid::new_v4(),
            config: config(),
            queue_modes: QueueModes::default(),
            extensions: vec![],
            provider_adapter: None,
            allowed_tools: None,
            messages: vec![],
            transcript: None,
            submissions: vec![],
            aborted_generations: vec![],
            resets: vec![],
            pending_resets: vec![],
            application_writes: vec![],
            tool_states: HashMap::new(),
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
