//! Standalone task-backed async tools with explicit hook policy. Checkpointable
//! tools have separate replay-safe and unsafe registrations. Conversation-
//! attached checkpointable execution supports explicitly registered replay-safe
//! task trees and explicitly selected replay-safe hooks for safe or unsafe Tools.

use crate::task::Task;
use pi_rs_core::{Outcome, Tool, ToolSpec};
use std::sync::Arc;

mod replay_safe;
pub(crate) use replay_safe::after_checkpoint_evidence;
pub(crate) use replay_safe::invalid_raw_checkpoint;
pub(crate) use replay_safe::validate_content_hook_result;
pub(crate) mod unsafe_park_proof;
pub(crate) use replay_safe::park_cancel_provenance_checks;
pub use replay_safe::{
    AbortAfterAuthorization, ReplaySafeAuthorization, RestartAfterAuthorization,
    UnsafeDurableAuthorization,
};
pub use unsafe_park_proof::{UnsafeDurableParkEvidence, UnsafeDurableReceipt};

/// Prepare matching conversation and driver registrations for an explicitly
/// replay-safe checkpointable Tool with no hooks. Add the returned adapter's
/// `register()` result to the Harness tool list and pass `registry` to the
/// task driver. The conversation must select no extensions. Unsafe tools,
/// hooks, task-owned conversations and external-job cancellation are unsupported.
pub fn register_conversation_replay_safe_durable_tool(
    registry: &mut crate::task_scheduler::Registry,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<crate::task_tool::TaskTool>>,
    _authorization: ReplaySafeAuthorization,
) -> Result<Arc<crate::task_tool::TaskTool>, String> {
    replay_safe::register_conversation_candidate(
        registry,
        tool,
        implementation,
        version,
        children,
        None,
        true,
        None,
    )
}

/// Register a conversation-owned non-replayable checkpointable tool without
/// hooks. Interrupted execution requires reconciliation; only a committed
/// park authorizes a later wake. All children must use matching conversation
/// checkpointable registrations with no selected hooks. Cancellation only
/// stops and joins local execution; no external cancellation API is provided.
/// Abort and interrupted execution require reconciliation, and joined receipts
/// remain available through `read_unsafe_durable_receipts`.
pub fn register_conversation_unsafe_durable_tool(
    registry: &mut crate::task_scheduler::Registry,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<crate::task_tool::TaskTool>>,
    _authorization: UnsafeDurableAuthorization,
) -> Result<Arc<crate::task_tool::TaskTool>, String> {
    replay_safe::register_conversation_candidate(
        registry,
        tool,
        implementation,
        version,
        children,
        None,
        false,
        None,
    )
}

/// Register an unsafe, unhooked conversation Tool and its external park-cancel
/// handler. `cancel_identity` must change whenever the handler implementation
/// or configuration changes and must be restored after reopening the store.
/// The exact Tool must first be registered with `nested_identity::register`;
/// its identity must cover both Tool and callback configuration, and every host
/// update must go through the returned `IdentityHandle`. The identity is held
/// through claim commit. Older registrations without it require reconciliation.
/// The handler is retained for a future cancellation integration; it is never
/// dispatched by this registration. Abort continues to require reconciliation.
#[allow(clippy::too_many_arguments)]
pub fn register_conversation_unsafe_durable_tool_with_park_cancel(
    registry: &mut crate::task_scheduler::Registry,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<crate::task_tool::TaskTool>>,
    cancel_identity: &str,
    cancel: Arc<dyn DurableParkCancel>,
    _authorization: UnsafeDurableAuthorization,
) -> Result<Arc<crate::task_tool::TaskTool>, String> {
    if cancel_identity.is_empty() || cancel_identity.len() > 4096 {
        return Err("invalid cancel identity".into());
    }
    replay_safe::register_conversation_candidate(
        registry,
        tool,
        implementation,
        version,
        children,
        None,
        false,
        Some((cancel_identity.to_owned(), cancel)),
    )
}

/// Host attestation that the selected before/after hooks are replay-safe,
/// cooperate with cancellation, and do not leave detached effects.
pub struct ReplaySafeConversationHooksAuthorization(());
impl ReplaySafeConversationHooksAuthorization {
    pub fn host_attests_hooks_replay_safe() -> Self {
        Self(())
    }
}

/// Register an ordered, nonempty selection of identified replay-safe hooks for conversation checkpointable
/// execution. Every checkpointable child must use the same selected registration.
/// Final bounded after results, including blocks, metadata and diagnostics, are
/// committed to the conversation without running the ordinary after chain again.
/// The supplied snapshot and Harness registry must share the exact registration.
#[allow(clippy::too_many_arguments)]
pub fn register_conversation_replay_safe_durable_tool_with_selected_hooks(
    registry: &mut crate::task_scheduler::Registry,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<crate::task_tool::TaskTool>>,
    snapshot: &crate::extension::Snapshot,
    names: &[String],
    _tool_authorization: ReplaySafeAuthorization,
    _hook_authorization: ReplaySafeConversationHooksAuthorization,
) -> Result<Arc<crate::task_tool::TaskTool>, String> {
    let selection = snapshot
        .stable_selection(names)
        .ok_or("extension selection is missing, duplicated, or unidentified")?;
    if names.is_empty() || selection.hook_count() == 0 {
        return Err("at least one selected hook is required".into());
    }
    let resolved = snapshot
        .resolve_stable(names, &selection)
        .ok_or("extension selection changed")?;
    let registration = resolved
        .registration_evidence()
        .ok_or("invalid hook registration evidence")?;
    replay_safe::register_conversation_candidate(
        registry,
        tool,
        implementation,
        version,
        children,
        Some((
            selection,
            registration,
            Arc::new(SelectedBeforeHooks(resolved)),
        )),
        true,
        None,
    )
}

/// Register an unsafe checkpointable conversation Tool with explicitly
/// replay-safe selected hooks. Tool execution remains non-replayable; only
/// separately proved before/after phases and committed parks may resume.
/// The host must restore the same Tool and hook registrations after restart.
/// External-job cancellation is not supported. Host-generated pre-dispatch
/// errors run after hooks only when an atomic no-dispatch proof establishes
/// that the task has no earlier unsafe intent or result evidence.
#[allow(clippy::too_many_arguments)]
pub fn register_conversation_unsafe_durable_tool_with_selected_hooks(
    registry: &mut crate::task_scheduler::Registry,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<crate::task_tool::TaskTool>>,
    snapshot: &crate::extension::Snapshot,
    names: &[String],
    _tool_authorization: UnsafeDurableAuthorization,
    _hook_authorization: ReplaySafeConversationHooksAuthorization,
) -> Result<Arc<crate::task_tool::TaskTool>, String> {
    let selection = snapshot
        .stable_selection(names)
        .ok_or("extension selection missing or unidentified")?;
    if names.is_empty() || selection.hook_count() == 0 {
        return Err("at least one selected hook is required".into());
    }
    let resolved = snapshot
        .resolve_stable(names, &selection)
        .ok_or("extension selection changed")?;
    let registration = resolved
        .registration_evidence()
        .ok_or("invalid hook registration evidence")?;
    replay_safe::register_conversation_candidate(
        registry,
        tool,
        implementation,
        version,
        children,
        Some((
            selection,
            registration,
            Arc::new(SelectedBeforeHooks(resolved)),
        )),
        false,
        None,
    )
}

/// Read joined unsafe outcomes retained even if abort won settlement. These
/// records are reconciliation evidence and do not authorize re-execution.
pub fn read_unsafe_durable_receipts(
    store: &durable_store::Store,
    task: uuid::Uuid,
) -> Result<Vec<UnsafeDurableReceipt>, String> {
    unsafe_park_proof::read_receipts(store, task)
}

/// Read returned Park proposals for reconciliation. A proposal does not
/// prove a park committed and must never be used to authorize a wake.
pub fn read_unsafe_durable_park_evidence(
    store: &durable_store::Store,
    task: uuid::Uuid,
) -> Result<Vec<UnsafeDurableParkEvidence>, String> {
    unsafe_park_proof::read_park_evidence(store, task)
}

const UNCERTAIN_PREFIX: &str = "tool outcome requires reconciliation: ";
const UNSUPPORTED_PREFIX: &str = "unsupported tool result: ";

/// A keyed task-backed leaf call. Inspect the typed result after the child wakes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AsyncTaskCall(crate::task_tool::NestedTaskCall);

/// Preserves the Tool outcome, including pending references and structured output.
#[derive(Debug, PartialEq)]
pub enum AsyncTaskResult {
    Outcome(Outcome),
    /// Ordered content and result metadata produced by block-aware after hooks.
    Rich(crate::extension::ToolBlockResult),
    /// The child requires reconciliation; its retained outcome, if known, is
    /// evidence rather than a finalized successful result.
    Uncertain(Outcome),
    /// The Tool outcome is known, but its after hook did not finish. It is
    /// evidence for reconciliation, not a finalized model-facing result.
    AfterIncomplete(Outcome),
    /// A host operation or state update was unsupported; the output is retained.
    Unsupported {
        reason: String,
        outcome: Outcome,
    },
    Failed(String),
    Cancelled,
}
impl AsyncTaskCall {
    pub fn task_id(&self) -> uuid::Uuid {
        self.0.task_id()
    }

    pub fn result(
        &self,
        parent: &Task,
        documents: &mut crate::document::TaskDocuments<'_>,
    ) -> Result<Option<AsyncTaskResult>, String> {
        use crate::task_tool::NestedTaskResult;
        let Some(result) = self.0.result(parent, documents)? else {
            return Ok(None);
        };
        if let Some(receipt) = documents.read_sealed_child_receipt(self.task_id())? {
            use crate::async_task_protocol::ReceiptRead;
            return Ok(Some(match receipt {
                ReceiptRead::AfterIncomplete(outcome) => AsyncTaskResult::AfterIncomplete(outcome),
                ReceiptRead::Final(outcome)
                    if matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) =>
                {
                    AsyncTaskResult::Uncertain(outcome)
                }
                ReceiptRead::Final(outcome) => AsyncTaskResult::Outcome(outcome),
            }));
        }
        let result = match result {
            NestedTaskResult::Succeeded(_) => {
                return Err("sealed tool result lacks a receipt".into());
            }
            NestedTaskResult::Failed(reason) => match reason.strip_prefix(UNCERTAIN_PREFIX) {
                Some(json) => {
                    let outcome: Outcome = serde_json::from_str(json)
                        .map_err(|_| "invalid stored uncertain tool outcome")?;
                    if !matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
                        return Err("invalid uncertain tool outcome kind".into());
                    }
                    AsyncTaskResult::Uncertain(outcome)
                }
                None => match reason.strip_prefix(UNSUPPORTED_PREFIX) {
                    Some(json) => {
                        let (reason, outcome): (String, Outcome) = serde_json::from_str(json)
                            .map_err(|_| "invalid stored unsupported tool outcome")?;
                        AsyncTaskResult::Unsupported { reason, outcome }
                    }
                    None => AsyncTaskResult::Failed(reason),
                },
            },
            NestedTaskResult::Cancelled => AsyncTaskResult::Cancelled,
        };
        Ok(Some(result))
    }
}

/// Durable result envelope for replay-safe block-hook task registrations.
/// Legacy task results remain directly encoded as `Outcome`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum DurableTaskResult {
    Rich {
        task_content_v1: crate::extension::ToolBlockResult,
    },
    Outcome(Outcome),
}

/// Read the finalized abort after-hook receipt after the task is cancelled.
/// Absence means no usable abort hook result was committed.
pub fn read_aborted_after_result(
    store: &durable_store::Store,
    task_id: uuid::Uuid,
) -> Result<Option<DurableTaskResult>, String> {
    let Some(task) = crate::task_store::TaskStore::new(store)
        .load(task_id)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    if !matches!(task.task.state(), crate::task::TaskState::Cancelled) {
        return Ok(None);
    }
    let key = format!("durable/replay-safe-async/{task_id}/abort-after");
    let Some(entry) = store.get(key.as_bytes()).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let (value, _) =
        crate::task_scheduler::abort_receipt_evidence(store, &task.task, &entry, false)?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| e.to_string())
}

pub(crate) fn validate_aborted_after_result(value: &serde_json::Value) -> Result<(), String> {
    if serde_json::to_vec(value).map_err(|e| e.to_string())?.len() > 60 * 1024 {
        return Err("abort result exceeds limit".into());
    }
    let DurableTaskResult::Rich {
        task_content_v1: result,
    } = serde_json::from_value(value.clone()).map_err(|e| format!("invalid abort result: {e}"))?
    else {
        return Err("invalid abort result".into());
    };
    let required = crate::runtime::ToolDiagnostic {
        severity: crate::runtime::DiagnosticSeverity::Error,
        code: Some("aborted".into()),
        message: "Tool was aborted".into(),
    };
    replay_safe::validate_content_hook_result(&result, Some(&required), false)
}

/// A committed replay-safe after checkpoint, retained only as reconciliation evidence.
/// This does not mean that the after hook completed or authorize a Tool retry.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplaySafeAfterEvidence {
    ToolOutcome(Outcome),
    HostError { code: String, outcome: Outcome },
}

/// Read a known raw outcome retained by a failed or cancelled replay-safe task.
/// Interrupted attempts without a committed after checkpoint remain unknown.
pub fn read_replay_safe_after_evidence(
    store: &durable_store::Store,
    task_id: uuid::Uuid,
) -> Result<Option<ReplaySafeAfterEvidence>, String> {
    let Some(record) = crate::task_store::TaskStore::new(store)
        .load(task_id)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let task = &record.task;
    if !matches!(
        task.state(),
        crate::task::TaskState::Failed(_) | crate::task::TaskState::Cancelled
    ) || !task.replay_safe()
        || !task.is_task_tool()
        || !task
            .kind()
            .is_some_and(|kind| kind.starts_with("pi-rs.replay-safe-async.v1."))
        || task.checkpoint().phase != "replay-safe-async-after"
    {
        return Ok(None);
    }
    replay_safe::after_checkpoint_evidence(&task.checkpoint().data).map(Some)
}

/// Read a restart-limit after receipt after the task has finalized as failed.
pub fn read_restart_after_result(
    store: &durable_store::Store,
    task_id: uuid::Uuid,
) -> Result<Option<DurableTaskResult>, String> {
    let Some(task) = crate::task_store::TaskStore::new(store)
        .load(task_id)
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    if !matches!(task.task.state(),crate::task::TaskState::Failed(reason) if reason == "restart limit")
        || !task.task.holds_restart_cleanup()
    {
        return Ok(None);
    }
    let key = format!("durable/replay-safe-async/{task_id}/restart-after");
    let Some(entry) = store.get(key.as_bytes()).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    let (value, _) = crate::task_scheduler::restart_receipt_evidence(store, &task.task, &entry)?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| e.to_string())
}
pub(crate) fn validate_restart_after_result(value: &serde_json::Value) -> Result<(), String> {
    if serde_json::to_vec(value).map_err(|e| e.to_string())?.len() > 60 * 1024 {
        return Err("restart result exceeds limit".into());
    }
    let DurableTaskResult::Rich {
        task_content_v1: result,
    } = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?
    else {
        return Err("invalid restart result".into());
    };
    let gate = crate::runtime::ToolDiagnostic {
        severity: crate::runtime::DiagnosticSeverity::Error,
        code: Some("restart_limit".into()),
        message: "Tool exceeded its restart limit; external effects may be unknown".into(),
    };
    replay_safe::validate_content_hook_result(&result, Some(&gate), false)
}

/// Evidence for a terminal sealed Tool task. Unknown and pending outcomes are
/// unresolved; an incomplete after result is not a final hook result.
#[derive(Clone, Debug, PartialEq)]
pub enum SealedOutcomeEvidence {
    NotDispatched,
    Uncertain(Outcome),
    AfterIncomplete(Outcome),
    Final(Outcome),
}

/// Read sealed dispatch and outcome evidence without authorizing another Tool call.
/// The task, identity, intent, attempt and receipt are checked in one read fence.
pub fn read_sealed_outcome_evidence(
    store: &durable_store::Store,
    task_id: uuid::Uuid,
) -> Result<Option<SealedOutcomeEvidence>, String> {
    for _ in 0..8 {
        let Some(record) = crate::task_store::TaskStore::new(store)
            .load(task_id)
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        if !record.task.is_terminal() || record.task.replay_safe() || !record.task.is_task_tool() {
            return Ok(None);
        }
        let (receipt, mut checks) =
            crate::async_task_protocol::prepare_receipt_read(store, task_id)
                .map_err(|e| e.to_string())?;
        checks.push(durable_store::Check {
            key: crate::task_store::key(task_id),
            expected: durable_store::Expected::Version(record.version),
        });
        match store.commit(&checks, &[]) {
            Ok(()) => {
                return Ok(Some(match receipt {
                    None => SealedOutcomeEvidence::NotDispatched,
                    Some(crate::async_task_protocol::ReceiptRead::AfterIncomplete(outcome)) => {
                        SealedOutcomeEvidence::AfterIncomplete(outcome)
                    }
                    Some(crate::async_task_protocol::ReceiptRead::Final(
                        outcome @ (Outcome::Unknown(_) | Outcome::Pending { .. }),
                    )) => SealedOutcomeEvidence::Uncertain(outcome),
                    Some(crate::async_task_protocol::ReceiptRead::Final(outcome)) => {
                        SealedOutcomeEvidence::Final(outcome)
                    }
                }));
            }
            Err(durable_store::Error::Conflict { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("sealed outcome evidence changed during read".into())
}

pub(crate) fn validate_interrupted_after_result(value: &serde_json::Value) -> Result<(), String> {
    if serde_json::to_vec(value).map_err(|e| e.to_string())?.len() > 60 * 1024 {
        return Err("interrupted result exceeds limit".into());
    }
    let DurableTaskResult::Rich {
        task_content_v1: result,
    } = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?
    else {
        return Err("invalid interrupted result".into());
    };
    let gate = crate::runtime::ToolDiagnostic {
        severity: crate::runtime::DiagnosticSeverity::Error,
        code: Some("interrupted".into()),
        message: "Tool was interrupted; external effects may be unknown".into(),
    };
    replay_safe::validate_content_hook_result(&result, Some(&gate), false)
}
/// Read supplemental interrupted-hook output only while the external outcome
/// remains unresolved. This never replaces or finalizes sealed Tool evidence.
pub fn read_interrupted_after_result(
    store: &durable_store::Store,
    task_id: uuid::Uuid,
) -> Result<Option<DurableTaskResult>, String> {
    for _ in 0..8 {
        let Some(task) = crate::task_store::TaskStore::new(store)
            .load(task_id)
            .map_err(|e| e.to_string())?
        else {
            return Ok(None);
        };
        if !matches!(task.task.state(),crate::task::TaskState::Failed(reason) if reason=="interrupted: execution outcome requires reconciliation")
            || !task.task.holds_interrupted_cleanup()
        {
            return Ok(None);
        }
        let identity = crate::async_task_protocol::interrupted_identity(store, task_id)
            .map_err(|e| e.to_string())?
            .ok_or("missing interrupted identity")?;
        if crate::async_task_protocol::interrupted_evidence(store, &task.task, &identity, false)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            return Ok(None);
        }
        let key = format!("durable/async-task/interrupted-after/{task_id}");
        let Some(entry) = store.get(key.as_bytes()).map_err(|e| e.to_string())? else {
            return Ok(None);
        };
        let (value, mut checks) = crate::task_scheduler::interrupted_receipt_evidence(
            store, &task.task, &identity, &entry,
        )?;
        checks.push(durable_store::Check {
            key: crate::task_store::key(task_id),
            expected: durable_store::Expected::Version(task.version),
        });
        match store.commit(&checks, &[]) {
            Ok(()) => {
                return serde_json::from_value(value)
                    .map(Some)
                    .map_err(|e| e.to_string());
            }
            Err(durable_store::Error::Conflict { .. }) => continue,
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("interrupted evidence changed during read".into())
}
struct InterruptedMemo(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl crate::extension::Memo for InterruptedMemo {
    async fn get(&self, _: &str) -> Result<Option<serde_json::Value>, String> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("memo unavailable during interrupted cleanup".into())
    }
    async fn get_or_insert(
        &self,
        _: &str,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("memo unavailable during interrupted cleanup".into())
    }
}

/// Read the durable pre-after Tool outcome for reconciliation after shutdown.
/// This is evidence only: it does not authorize dispatch or imply that the
/// after hook completed, and must not be used as a finalized tool result.
pub fn read_raw_outcome_for_reconciliation(
    store: &durable_store::Store,
    task_id: uuid::Uuid,
) -> Result<Option<Outcome>, String> {
    crate::async_task_protocol::read_raw_result(store, task_id)
        .map(|record| record.map(|(_, outcome)| outcome))
        .map_err(|error| error.to_string())
}

/// Why a durable Tool attempt was resumed after a committed park.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DurableWait {
    All(Vec<AsyncTaskCall>),
    Any(Vec<AsyncTaskCall>),
    #[cfg(test)]
    Signal(String),
}

/// A completed Tool outcome or an explicit durable park. A park is distinct
/// from `Outcome::Pending`, which names an external operation for reconciliation.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DurableToolStep {
    Complete(Outcome),
    Park {
        checkpoint: serde_json::Value,
        wait: DurableWait,
    },
}

/// Data supplied to one driver-owned execution attempt. `checkpoint` is absent
/// for the first attempt; `ended` contains direct children from the wake.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DurableToolResume {
    pub checkpoint: Option<serde_json::Value>,
    pub ended: Vec<uuid::Uuid>,
}

/// Task-owned nested admission and result access. Implementations must fence
/// every operation against the current parent reservation and abort state.
#[async_trait::async_trait]
pub trait DurableTaskHost: Send + Sync {
    async fn start_child(
        &self,
        key: &str,
        tool: &str,
        input: serde_json::Value,
    ) -> Result<AsyncTaskCall, String>;
    async fn child_result(&self, child: &AsyncTaskCall) -> Result<Option<AsyncTaskResult>, String>;
}

/// Explicit opt-in for Tools that can checkpoint and resume after a park.
/// Ordinary `Tool` implementations keep their existing behavior. Standalone
/// registration is available only with an explicit replay-safe attestation.
#[async_trait::async_trait]
pub trait DurableAsyncTool: Tool {
    async fn execute_durable(
        &self,
        arguments: serde_json::Value,
        context: pi_rs_core::ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String>;
}

/// Persisted callback replay policy. The initial protocol only permits a
/// single dispatch; a claimed callback is never automatically retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationCancelPolicy {
    AtMostOnce,
}

/// Explicitly registered cleanup for external work left by a parked tool.
/// Implementations must be cooperative, start no detached work, and cannot
/// admit nested tasks. A committed invocation claim is never replayed.
#[async_trait::async_trait]
pub trait DurableParkCancel: Send + Sync {
    /// Policy stored with a conversation claim; no callback is dispatched by
    /// merely recording this policy.
    fn conversation_cancel_policy(&self) -> ConversationCancelPolicy {
        ConversationCancelPolicy::AtMostOnce
    }
    async fn cancel(
        &self,
        arguments: serde_json::Value,
        checkpoint: serde_json::Value,
        context: pi_rs_core::ToolContext,
    ) -> Result<(), String>;
}

/// Callback execution evidence. Neither variant proves an external effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkCancelEvidence {
    AttemptedUnknown,
    CallbackReturned,
}

/// Read a terminal task's fenced callback evidence for reconciliation.
pub fn read_park_cancel_evidence(
    store: &durable_store::Store,
    task: uuid::Uuid,
) -> Result<Option<ParkCancelEvidence>, String> {
    Ok(
        crate::task_scheduler::read_park_cancel_claim(store, task)?.map(|finished| {
            if finished {
                ParkCancelEvidence::CallbackReturned
            } else {
                ParkCancelEvidence::AttemptedUnknown
            }
        }),
    )
}

/// The host guarantees that repeating this cancel with the same arguments and
/// checkpoint is idempotent, replay-safe, cooperative, and starts no detached work.
pub struct ParkCancelRetryAuthorization(());
impl ParkCancelRetryAuthorization {
    pub fn host_attests_cancel_replay_safe() -> Self {
        Self(())
    }
}

/// The host attests that this named tool is unavailable for this task and that
/// the supplied hook selection is the exact intended immutable selection.
pub struct UnavailableAfterAuthorization(());
impl UnavailableAfterAuthorization {
    pub fn host_attests_unavailable_and_selection() -> Self {
        Self(())
    }
}
struct UnavailablePlaceholder(ToolSpec);
#[async_trait::async_trait]
impl Tool for UnavailablePlaceholder {
    fn spec(&self) -> ToolSpec {
        self.0.clone()
    }
    async fn invoke(&self, _: serde_json::Value, _: pi_rs_core::ToolContext) -> Outcome {
        Outcome::Skipped("unavailable placeholder cannot execute".into())
    }
}
#[async_trait::async_trait]
impl DurableAsyncTool for UnavailablePlaceholder {
    async fn execute_durable(
        &self,
        _: serde_json::Value,
        _: pi_rs_core::ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        Err("unavailable placeholder cannot execute".into())
    }
}

/// The host attests that interrupted-after hooks are replay-safe, cooperative,
/// and start no detached work; external Tool effects remain uncertain.
pub struct InterruptedAfterAuthorization(());
impl InterruptedAfterAuthorization {
    pub fn host_attests_cleanup_safe() -> Self {
        Self(())
    }
}

/// The host explicitly attests that its standalone task application does not
/// require tool hooks for authorization or policy. This cannot establish facts
/// about external policy; callers are responsible for that assertion.
pub struct UnhookedAuthorization(());
impl UnhookedAuthorization {
    /// Explicitly attest that hooks are not required by this host's policy.
    pub fn standalone_only() -> Self {
        Self(())
    }
}

/// Owns a scheduler constructed without a conversation runtime. Its fields are
/// private, so a conversation-attached scheduler cannot be substituted.
/// Only explicitly registered sealed leaf tools can be dispatched.
pub struct StandaloneNoHooksHost {
    scheduler: crate::task_scheduler::Scheduler,
}
impl StandaloneNoHooksHost {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        registry: crate::task_scheduler::Registry,
        _authorization: UnhookedAuthorization,
    ) -> Result<Self, crate::task_scheduler::Error> {
        let scheduler = crate::task_scheduler::Scheduler::open(path, registry)?;
        Ok(Self { scheduler })
    }

    /// Register a sealed leaf Tool in this standalone host. The exact Tool
    /// allocation must have a matching `nested_identity` registration. The
    /// returned adapter stages task-owned child calls atomically.
    pub fn register_tool(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        let kind = kind.into();
        let (executor, identity) =
            self.scheduler
                .install_sealed_tool(kind.clone(), tool, implementation, version)?;
        let spec = identity.spec.clone();
        let handler: Arc<dyn crate::task_scheduler::Handler> = executor;
        let adapter = Arc::new(crate::task_tool::TaskTool::new(
            spec, kind, false, version, handler,
        ));
        Ok(Arc::new(AsyncTaskTool { adapter, identity }))
    }

    /// Fail closed until attempt-specific intents, park publication, and
    /// resumed executor ownership are wired into the driver.
    pub fn register_durable_tool(
        &mut self,
        _kind: impl Into<String>,
        _tool: Arc<dyn DurableAsyncTool>,
        _implementation: &str,
        _version: u32,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        Err("durable async Tool park/wake execution is not yet supported".into())
    }

    /// Register a checkpointable tool for a standalone scheduler. The host
    /// must attest that every attempt is pure or otherwise safe to replay.
    /// Children must be replay-safe TaskTools or explicitly registered unsafe
    /// checkpointable tools from this host. Their replay policies stay separate.
    pub fn register_replay_safe_durable_tool(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        _authorization: ReplaySafeAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        replay_safe::register(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            None,
            false,
            false,
            false,
            false,
            None,
        )
    }

    /// Create a standalone root before transferring this host to its driver.
    /// Arguments are normalized and checked by the registered tool at dispatch.
    pub fn start_replay_safe_root(
        &self,
        tool: &Arc<crate::task_tool::TaskTool>,
        input: serde_json::Value,
        restart_limit: u32,
    ) -> Result<uuid::Uuid, String> {
        self.scheduler
            .submit_checkpointable_async_root(tool, input, restart_limit, true)
    }

    /// Register an unsafe checkpointable Tool. Interruption of a running
    /// attempt requires reconciliation; only a committed park may resume.
    /// Children must be replay-safe TaskTools or explicitly registered unsafe
    /// checkpointable tools from this host.
    pub fn register_unsafe_durable_tool(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        _authorization: UnsafeDurableAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        replay_safe::register_unsafe(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            None,
        )
    }

    /// Admit an unsafe root belonging to this standalone host.
    pub fn start_unsafe_durable_root(
        &self,
        tool: &Arc<crate::task_tool::TaskTool>,
        input: serde_json::Value,
        restart_limit: u32,
    ) -> Result<uuid::Uuid, String> {
        self.scheduler
            .submit_checkpointable_async_root(tool, input, restart_limit, false)
    }

    #[cfg(test)]
    pub(crate) fn into_scheduler_for_test(self) -> crate::task_scheduler::Scheduler {
        self.scheduler
    }

    /// Transfer this standalone scheduler to its real task driver.
    /// Registered sealed tools execute only through this driver.
    pub fn into_driver(self) -> crate::task_driver::Driver {
        crate::task_driver::Driver::start(self.scheduler)
    }
}

/// A standalone host for replay-safe task hooks. The caller must change
/// `hook_identity` whenever hook code or configuration changes. Hook effects
/// other than task-fenced memo are unsupported. An opted-in after hook may be
/// retried after interruption; sleep and conversation state are unavailable.
pub struct StandaloneBeforeHookHost {
    scheduler: crate::task_scheduler::Scheduler,
}
impl StandaloneBeforeHookHost {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        registry: crate::task_scheduler::Registry,
    ) -> Result<Self, crate::task_scheduler::Error> {
        Ok(Self {
            scheduler: crate::task_scheduler::Scheduler::open(path, registry)?,
        })
    }
    pub fn register_tool(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        self.register_tool_inner(
            kind,
            tool,
            implementation,
            version,
            hook_identity,
            hook,
            false,
            false,
        )
    }
    /// Opt in to replay-safe after hooks. Hook code and configuration must be
    /// covered by `hook_identity`; hooks may use task-fenced memo but must not
    /// perform other side effects, because an interrupted after can be retried.
    pub fn register_tool_with_after(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        self.register_tool_inner(
            kind,
            tool,
            implementation,
            version,
            hook_identity,
            hook,
            true,
            false,
        )
    }
    /// Opt in to one fenced after invocation for an interrupted sealed Tool.
    #[allow(clippy::too_many_arguments)]
    pub fn register_tool_with_interrupted_after(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
        _authorization: InterruptedAfterAuthorization,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        self.register_tool_inner(
            kind,
            tool,
            implementation,
            version,
            hook_identity,
            hook,
            true,
            true,
        )
    }
    #[allow(clippy::too_many_arguments)] // Keep registration parameters explicit at this private boundary.
    fn register_tool_inner(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
        after: bool,
        interrupted_after: bool,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        let kind = kind.into();
        let (executor, identity) = self.scheduler.install_sealed_tool_with_hook(
            kind.clone(),
            tool,
            implementation,
            version,
            Some((hook_identity.into(), hook)),
            after,
            interrupted_after,
        )?;
        let handler: Arc<dyn crate::task_scheduler::Handler> = executor;
        let adapter = Arc::new(crate::task_tool::TaskTool::new(
            identity.spec.clone(),
            kind,
            false,
            version,
            handler,
        ));
        Ok(Arc::new(AsyncTaskTool { adapter, identity }))
    }
    /// Bind an exact, host-identified extension selection to this standalone
    /// task tool. The selection and its order are part of the durable identity.
    /// The selected hooks have the same replay and capability restrictions as
    /// `register_tool`; in particular this phase only invokes before hooks.
    pub fn register_selected_tool(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
    ) -> Result<Arc<AsyncTaskTool>, String> {
        self.register_selected_tool_inner(
            kind,
            tool,
            implementation,
            version,
            snapshot,
            names,
            false,
        )
    }
    /// Opt the exact ordered selection into replay-safe legacy text after hooks.
    pub fn register_selected_tool_with_after(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
    ) -> Result<Arc<AsyncTaskTool>, String> {
        self.register_selected_tool_inner(
            kind,
            tool,
            implementation,
            version,
            snapshot,
            names,
            true,
        )
    }
    /// Opt in to one restart-limit after cleanup, independently of abort hooks.
    #[allow(clippy::too_many_arguments)]
    pub fn register_replay_safe_durable_tool_with_restart_after(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        _authorization: ReplaySafeAuthorization,
        _cleanup_authorization: RestartAfterAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        self.register_content_hooks_inner(
            tool,
            implementation,
            version,
            children,
            snapshot,
            names,
            false,
            true,
        )
    }
    #[allow(clippy::too_many_arguments)] // Keep registration parameters explicit at this private boundary.
    fn register_selected_tool_inner(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        after: bool,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        let selection = snapshot
            .stable_selection(names)
            .ok_or("extension selection is missing, duplicated, or unidentified")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("extension selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        self.register_tool_inner(
            kind,
            tool,
            implementation,
            version,
            &identity,
            Arc::new(SelectedBeforeHooks(resolved)),
            after,
            false,
        )
    }
    /// Register a replay-safe durable Tool and one replay-safe hook. The hook
    /// identity covers code and configuration. Memo is task-fenced; hook
    /// parking is not available in this initial integration.
    #[allow(clippy::too_many_arguments)] // Explicit registration includes hook identity and replay authorization.
    pub fn register_replay_safe_durable_tool_with_hook(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
        _authorization: ReplaySafeAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        replay_safe::register(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            Some((hook_identity.into(), hook)),
            false,
            false,
            false,
            false,
            None,
        )
    }
    /// Register a stable ordered selection of replay-safe block-aware hooks.
    /// Hook identity declarations must cover implementation and configuration.
    #[allow(clippy::too_many_arguments)] // Definition, selection and replay policy are explicit.
    pub fn register_replay_safe_durable_tool_with_content_hooks(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        _authorization: ReplaySafeAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        self.register_content_hooks_inner(
            tool,
            implementation,
            version,
            children,
            snapshot,
            names,
            false,
            false,
        )
    }
    /// Opt in to abort after hooks whose host guarantees replay safety and bounded completion.
    /// Abort hooks have no memo access and must not spawn detached work.
    #[allow(clippy::too_many_arguments)]
    pub fn register_replay_safe_durable_tool_with_abort_after(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        _authorization: ReplaySafeAuthorization,
        _abort_authorization: AbortAfterAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        self.register_content_hooks_inner(
            tool,
            implementation,
            version,
            children,
            snapshot,
            names,
            true,
            false,
        )
    }
    /// Register one explicitly identified parked-tool cleanup, together with
    /// the selected abort-after hooks. The callback must not spawn detached work;
    /// `cancel_identity` must change when its implementation or configuration changes.
    #[allow(clippy::too_many_arguments)]
    pub fn register_replay_safe_durable_tool_with_park_cancel(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        cancel_identity: &str,
        cancel: Arc<dyn DurableParkCancel>,
        _authorization: ReplaySafeAuthorization,
        _cleanup: AbortAfterAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        if cancel_identity.is_empty() || cancel_identity.len() > 4096 {
            return Err("invalid cancel identity".into());
        }
        let selection = snapshot
            .stable_selection(names)
            .ok_or("invalid selection")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        replay_safe::register(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            Some((identity, Arc::new(SelectedBeforeHooks(resolved)))),
            true,
            true,
            false,
            false,
            Some((cancel_identity.to_owned(), cancel, false)),
        )
    }
    /// Opt in to bounded crash recovery for an idempotent parked cancel.
    /// The host guarantees repeat calls with the same arguments and checkpoint
    /// do not duplicate external effects. The task restart limit bounds attempts.
    #[allow(clippy::too_many_arguments)]
    pub fn register_replay_safe_durable_tool_with_retryable_park_cancel(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        cancel_identity: &str,
        cancel: Arc<dyn DurableParkCancel>,
        _authorization: ReplaySafeAuthorization,
        _cleanup: AbortAfterAuthorization,
        _retry: ParkCancelRetryAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        if cancel_identity.is_empty() || cancel_identity.len() > 4096 {
            return Err("invalid cancel identity".into());
        }
        let selection = snapshot
            .stable_selection(names)
            .ok_or("invalid selection")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        replay_safe::register(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            Some((identity, Arc::new(SelectedBeforeHooks(resolved)))),
            true,
            true,
            false,
            false,
            Some((cancel_identity.to_owned(), cancel, true)),
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn register_content_hooks_inner(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        abort_after: bool,
        restart_after: bool,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        let selection = snapshot
            .stable_selection(names)
            .ok_or("extension selection is missing, duplicated, or unidentified")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("extension selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        replay_safe::register(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            Some((identity, Arc::new(SelectedBeforeHooks(resolved)))),
            true,
            abort_after,
            restart_after,
            false,
            None,
        )
    }
    /// Register an explicitly unavailable name and its exact selected hooks.
    /// This never resolves or executes a Tool and must not replace an existing task definition.
    #[allow(clippy::too_many_arguments)]
    pub fn register_unavailable_tool_with_content_hooks(
        &mut self,
        spec: ToolSpec,
        placeholder_identity: &str,
        version: u32,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        _authorization: ReplaySafeAuthorization,
        _unavailable: UnavailableAfterAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        let selection = snapshot
            .stable_selection(names)
            .ok_or("extension selection is missing, duplicated, or unidentified")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("extension selection changed")?;
        if resolved.tool(&spec.name).is_some() {
            return Err("selected extension provides the supposedly unavailable tool".into());
        }
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        replay_safe::register(
            &mut self.scheduler,
            Arc::new(UnavailablePlaceholder(spec)),
            placeholder_identity,
            version,
            vec![],
            Some((identity, Arc::new(SelectedBeforeHooks(resolved)))),
            true,
            false,
            false,
            true,
            None,
        )
    }
    /// Register an unsafe checkpointable tool with replay-safe hooks and argument preparation.
    /// A committed park may resume; interruption during tool execution requires reconciliation.
    /// Parked external-job cancellation is not yet supported by `DurableAsyncTool`.
    #[allow(clippy::too_many_arguments)]
    pub fn register_unsafe_durable_tool_with_hook(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
        _unsafe_authorization: UnsafeDurableAuthorization,
        _hook_authorization: ReplaySafeAuthorization,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        replay_safe::register_unsafe(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            Some((hook_identity.into(), hook)),
        )
    }
    /// Opt an unsafe checkpointable Tool into a fenced parked cancel and
    /// selected abort hooks. External Tool effects always require reconciliation.
    /// The host must ensure the cancel callback is cooperative and starts no
    /// detached work, and change its identity whenever its behavior changes.
    /// `retry` requires an explicit host guarantee that repeated cancellation
    /// of the same arguments and checkpoint is idempotent.
    #[allow(clippy::too_many_arguments)]
    pub fn register_unsafe_durable_tool_with_park_cancel(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        cancel_identity: &str,
        cancel: Arc<dyn DurableParkCancel>,
        _unsafe: UnsafeDurableAuthorization,
        _hooks: ReplaySafeAuthorization,
        _cleanup: AbortAfterAuthorization,
        retry: Option<ParkCancelRetryAuthorization>,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        if cancel_identity.is_empty() || cancel_identity.len() > 4096 {
            return Err("invalid cancel identity".into());
        }
        let selection = snapshot
            .stable_selection(names)
            .ok_or("invalid selection")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        replay_safe::register_unsafe_park_cancel(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            (identity, Arc::new(SelectedBeforeHooks(resolved))),
            (cancel_identity.to_owned(), cancel, retry.is_some()),
            false,
        )
    }
    /// Opt in to cleanup for an interrupted woken unsafe execution. External
    /// effects remain uncertain; retry requires explicit idempotency attestation.
    /// This distinct registration does not cancel an unwoken park. The host must
    /// change `cancel_identity` when the callback or its configuration changes.
    /// Cleanup must be cooperative and must not spawn detached work.
    /// Before registration, register this exact Tool allocation with
    /// `nested_identity::register`. Its identity must cover Tool and callback
    /// implementation, configuration and tenant. All changes to either must
    /// go through the returned `IdentityHandle::update`; bypassing that host
    /// contract cannot be fenced. Updates are nonblocking and return busy while
    /// cleanup holds the lease. Busy recovery and pre-dispatch cleanup are
    /// deferred and retried; an identity mismatch remains fail-closed.
    /// The 30-second timeout cannot preempt blocking
    /// callback code, so a noncooperative callback can retain it longer.
    /// This contract has a distinct definition identity from earlier versions;
    /// older in-flight tasks require reconciliation or their original host.
    #[allow(clippy::too_many_arguments)]
    pub fn register_unsafe_durable_tool_with_woken_cancel(
        &mut self,
        tool: Arc<dyn DurableAsyncTool>,
        implementation: &str,
        version: u32,
        children: Vec<Arc<crate::task_tool::TaskTool>>,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
        cancel_identity: &str,
        cancel: Arc<dyn DurableParkCancel>,
        _unsafe: UnsafeDurableAuthorization,
        _hooks: ReplaySafeAuthorization,
        _cleanup: AbortAfterAuthorization,
        retry: Option<ParkCancelRetryAuthorization>,
    ) -> Result<Arc<crate::task_tool::TaskTool>, String> {
        if cancel_identity.is_empty() || cancel_identity.len() > 4096 {
            return Err("invalid cancel identity".into());
        }
        let selection = snapshot
            .stable_selection(names)
            .ok_or("invalid selection")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        replay_safe::register_unsafe_park_cancel(
            &mut self.scheduler,
            tool,
            implementation,
            version,
            children,
            (identity, Arc::new(SelectedBeforeHooks(resolved))),
            (cancel_identity.to_owned(), cancel, retry.is_some()),
            true,
        )
    }
    pub fn start_replay_safe_root(
        &self,
        tool: &Arc<crate::task_tool::TaskTool>,
        input: serde_json::Value,
        restart_limit: u32,
    ) -> Result<uuid::Uuid, String> {
        self.scheduler
            .submit_checkpointable_async_root(tool, input, restart_limit, true)
    }
    pub fn start_unsafe_durable_root(
        &self,
        tool: &Arc<crate::task_tool::TaskTool>,
        input: serde_json::Value,
        restart_limit: u32,
    ) -> Result<uuid::Uuid, String> {
        self.scheduler
            .submit_checkpointable_async_root(tool, input, restart_limit, false)
    }
    pub fn into_driver(self) -> crate::task_driver::Driver {
        crate::task_driver::Driver::start(self.scheduler)
    }
    #[cfg(test)]
    pub(crate) fn into_scheduler_for_test(self) -> crate::task_scheduler::Scheduler {
        self.scheduler
    }
}

struct SelectedBeforeHooks(crate::extension::Resolved);
#[async_trait::async_trait]
impl crate::extension::Hook for SelectedBeforeHooks {
    async fn before_tool(
        &self,
        call: &crate::extension::ToolCall,
        context: &crate::extension::Context<'_>,
    ) -> Result<crate::extension::BeforeTool, String> {
        let (next, decision, _reports) = self.0.before_tool(call.clone(), context).await;
        Ok(match decision {
            crate::extension::BeforeTool::Pass if next.arguments != call.arguments => {
                crate::extension::BeforeTool::Rewrite(next.arguments)
            }
            other => other,
        })
    }
    async fn after_tool(
        &self,
        call: &crate::extension::ToolCall,
        result: &serde_json::Value,
        context: &crate::extension::Context<'_>,
    ) -> Result<Option<serde_json::Value>, String> {
        let (value, _reports) = self.0.after_tool(call, result.clone(), context).await;
        Ok((value != *result).then_some(value))
    }
    async fn after_tool_blocks(
        &self,
        call: &crate::extension::ToolCall,
        result: &crate::extension::ToolBlockResult,
        nested: bool,
        context: &crate::extension::Context<'_>,
    ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
        let required = result.diagnostics.iter().find(|diagnostic| {
            matches!(
                diagnostic.code.as_deref(),
                Some(
                    "blocked"
                        | "invalid_arguments"
                        | "invalid_park"
                        | "aborted"
                        | "restart_limit"
                        | "tool_unavailable"
                        | "interrupted"
                )
            )
        });
        let value = self
            .0
            .after_tool_blocks_checked(
                call,
                result.clone(),
                nested,
                context,
                |candidate| replay_safe::validate_content_hook_result(candidate, required, true),
                |candidate| replay_safe::validate_content_hook_result(candidate, required, false),
            )
            .await?;
        Ok((value != *result).then_some(value))
    }
}

/// Persistable registration identity. The host-assigned implementation ID must
/// cover implementation and backend configuration; it is an assertion by the
/// host, not a cryptographic proof of executable code or tenant identity.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolIdentity {
    spec: ToolSpec,
    implementation: String,
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    before_hook: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    after_hook: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    interrupted_after: bool,
}
impl ToolIdentity {
    pub fn new(
        tool: &dyn Tool,
        implementation: impl Into<String>,
        version: u32,
    ) -> Result<Self, String> {
        let implementation = implementation.into();
        let spec = tool.spec();
        if implementation.is_empty()
            || implementation.len() > 4096
            || version == 0
            || spec.name.is_empty()
        {
            return Err("invalid tool identity".into());
        }
        Ok(Self {
            spec,
            implementation,
            version,
            before_hook: None,
            after_hook: false,
            interrupted_after: false,
        })
    }

    pub(crate) fn with_before_hook(mut self, id: String) -> Result<Self, String> {
        if id.trim().is_empty() || id.len() > 4096 {
            return Err("invalid before-hook identity".into());
        }
        self.before_hook = Some(id);
        Ok(self)
    }

    pub(crate) fn with_after_hook(mut self) -> Self {
        self.after_hook = true;
        self
    }
    pub(crate) fn has_after_hook(&self) -> bool {
        self.after_hook
    }

    pub(crate) fn with_interrupted_after(mut self) -> Self {
        self.interrupted_after = true;
        self
    }
    pub(crate) fn has_interrupted_after(&self) -> bool {
        self.interrupted_after
    }
    pub(crate) fn is_valid(&self) -> bool {
        (!self.interrupted_after || self.after_hook)
            && (!self.after_hook || self.before_hook.is_some())
            && !self.implementation.is_empty()
            && self.implementation.len() <= 4096
            && self.version != 0
            && !self.spec.name.is_empty()
            && self
                .before_hook
                .as_ref()
                .is_none_or(|id| !id.trim().is_empty() && id.len() <= 4096)
    }

    /// Compare the stored identity with the current definition. This check is
    /// only a building block and does not acquire an execution or abort fence.
    pub fn matches(&self, tool: &dyn Tool, implementation: &str, version: u32) -> bool {
        self.is_valid()
            && self.spec == tool.spec()
            && self.implementation == implementation
            && self.version == version
    }
}

/// Executor construction is crate-private: no caller can substitute a Handler
/// for the Tool whose identity is checked by the driver.
pub(crate) struct SealedExecutor {
    tool: Arc<dyn Tool>,
    identity: ToolIdentity,
    implementation: String,
    version: u32,
    before_hook: Option<Arc<dyn crate::extension::Hook>>,
}
impl SealedExecutor {
    pub(crate) fn new(
        tool: Arc<dyn Tool>,
        identity: ToolIdentity,
        implementation: String,
        version: u32,
        before_hook: Option<Arc<dyn crate::extension::Hook>>,
    ) -> Self {
        Self {
            tool,
            identity,
            implementation,
            version,
            before_hook,
        }
    }
    pub(crate) fn tool(&self) -> &Arc<dyn Tool> {
        &self.tool
    }

    pub(crate) fn has_before_hook(&self) -> bool {
        self.before_hook.is_some()
    }
    pub(crate) fn has_after_hook(&self) -> bool {
        self.identity.has_after_hook()
    }

    pub(crate) fn checkpoint_arguments(&self, task: &Task) -> Result<serde_json::Value, String> {
        if task.checkpoint().phase != "sealed.execute.v1" {
            return Err("unsupported sealed tool phase".into());
        }
        let data = &task.checkpoint().data;
        if data.get("identity")
            != Some(&serde_json::to_value(&self.identity).map_err(|e| e.to_string())?)
        {
            return Err("before-hook registration changed".into());
        }
        let args = data
            .get("arguments")
            .cloned()
            .ok_or("missing prepared arguments")?;
        self.validate_arguments(&args)?;
        Ok(args)
    }
    fn validate_arguments(&self, args: &serde_json::Value) -> Result<(), String> {
        if serde_json::to_vec(args).map_err(|e| e.to_string())?.len() > 16 * 1024 * 1024
            || !args.is_object()
            || !jsonschema::validator_for(&self.identity.spec.parameters)
                .is_ok_and(|v| v.is_valid(args))
        {
            return Err("arguments exceed 16 MiB or do not match tool schema".into());
        }
        Ok(())
    }
    pub(crate) fn preflight(
        &self,
        task: &Task,
        cancel: &pi_rs_core::CancellationToken,
        memo: &dyn crate::extension::Memo,
    ) -> Result<crate::task_scheduler::Step, String> {
        let hook = self
            .before_hook
            .as_ref()
            .ok_or("missing before hook")?
            .clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        runtime.block_on(async {
            let operation = async {
                let args = self.tool.prepare_arguments(task.input().clone()).await?;
                self.validate_arguments(&args)?;
                if !self.identity.matches(self.tool.as_ref(), &self.implementation, self.version) { return Err("tool identity changed".into()); }
                let mut registry = crate::extension::Registry::default();
                registry.install(crate::extension::Extension { name: "task-before".into(), hooks: vec![hook], ..Default::default() }).map_err(|e| e.to_string())?;
                let selected = registry.snapshot().resolve(None);
                let snapshot = serde_json::Value::Null;
                let context = crate::extension::Context { snapshot: &snapshot, memo };
                let (call, decision, _) = selected.before_tool(crate::extension::ToolCall { name: self.identity.spec.name.clone(), arguments: args }, &context).await;
                match decision {
                    crate::extension::BeforeTool::Pass => (),
                    crate::extension::BeforeTool::Block(reason) => return Ok(crate::task_scheduler::Step::Fail(format!("tool call blocked: {reason}"))),
                    crate::extension::BeforeTool::Sleep(_) => return Err("before-hook sleep is unsupported".into()),
                    crate::extension::BeforeTool::Rewrite(_) => return Err("unexpected hook decision".into()),
                }
                self.validate_arguments(&call.arguments)?;
                let checkpoint = crate::task::Checkpoint { phase: "sealed.execute.v1".into(), data: serde_json::json!({"identity": self.identity, "arguments": call.arguments}) };
                if serde_json::to_vec(&checkpoint).map_err(|e| e.to_string())?.len().saturating_add(1024) > 16 * 1024 * 1024 { return Err("prepared checkpoint exceeds 16 MiB".into()); }
                Ok(crate::task_scheduler::Step::Continue(checkpoint))
            };
            tokio::select! { biased; _ = cancel.cancelled() => Err("before-hook cancelled".into()), result = operation => result }
        })
    }

    /// Run the limited legacy text after hook. Non-text replacements are
    /// ignored as in the ordinary runtime; structured outcome replacement is
    /// rejected because this standalone path cannot preserve all channels.
    pub(crate) fn after(
        &self,
        task: &Task,
        outcome: Outcome,
        cancel: &pi_rs_core::CancellationToken,
        memo: &dyn crate::extension::Memo,
    ) -> Result<crate::task_scheduler::Step, String> {
        let args = self.checkpoint_arguments(task)?;
        let hook = self
            .before_hook
            .as_ref()
            .ok_or("missing after hook")?
            .clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let text = outcome.tool_content();
        let replacement = runtime.block_on(async {
            let call = crate::extension::ToolCall { name: self.identity.spec.name.clone(), arguments: args };
            let snapshot = serde_json::Value::Null;
            let context = crate::extension::Context { snapshot: &snapshot, memo };
            let mut registry = crate::extension::Registry::default();
            registry.install(crate::extension::Extension { name: "task-after".into(), hooks: vec![hook], ..Default::default() }).map_err(|e| e.to_string())?;
            let selected = registry.snapshot().resolve(None);
            tokio::select! { biased;
                _ = cancel.cancelled() => Err("after-hook cancelled".to_string()),
                result = selected.after_tool(&call, serde_json::Value::String(text.clone()), &context) => Ok(result.0),
            }
        })?;
        let outcome = match replacement.as_str() {
            Some(value) if value != text => match outcome {
                Outcome::Succeeded(_) => Outcome::Succeeded(value.to_owned()),
                Outcome::SucceededWithDetails { details, .. } => Outcome::SucceededWithDetails {
                    output: value.to_owned(),
                    details,
                },
                _ => return Err("after-hook replacement of non-text outcome is unsupported".into()),
            },
            _ => outcome,
        };
        if serde_json::to_vec(&outcome)
            .map_err(|e| e.to_string())?
            .len()
            > 60 * 1024
        {
            return Err("after-hook outcome exceeds receipt limit".into());
        }
        Ok(crate::task_scheduler::Step::Succeed(
            serde_json::to_value(outcome).map_err(|e| e.to_string())?,
        ))
    }

    /// Called only after the driver has committed its task-owned intent and
    /// acquired the registered identity lease. The worker directly polls and
    /// joins the Tool future. Arbitrary effects spawned by a Tool are outside
    /// that join and require destination-side idempotency or reconciliation.
    pub(crate) fn run(
        &self,
        task: &Task,
        intent: &crate::async_task_protocol::Intent,
        store: &durable_store::Store,
        cancellation: &pi_rs_core::CancellationToken,
        prepared: Option<serde_json::Value>,
    ) -> Result<crate::task_scheduler::Step, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tool runtime: {e}"))?;
        let outcome = runtime.block_on(async {
            if cancellation.is_cancelled() {
                return Outcome::Unknown("cancelled after logical dispatch".into());
            }
            let args = match prepared {
                Some(args) => args,
                None => match self.tool.prepare_arguments(task.input().clone()).await {
                    Ok(args) => args,
                    Err(error) => {
                        return Outcome::Unknown(format!(
                            "argument preparation failed after logical dispatch: {error}"
                        ));
                    }
                },
            };
            if !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
            {
                return Outcome::Unknown("tool identity changed during preparation".into());
            }
            let spec = self.tool.spec();
            if !args.is_object()
                || !jsonschema::validator_for(&spec.parameters).is_ok_and(|v| v.is_valid(&args))
            {
                return Outcome::Unknown(
                    "normalized arguments do not match the tool schema after dispatch".into(),
                );
            }
            let batch =
                match crate::async_task_protocol::prepare_arguments_record(store, intent, &args) {
                    Ok(batch) => batch,
                    Err(error) => {
                        return Outcome::Unknown(format!(
                            "normalized argument persistence failed: {error}"
                        ));
                    }
                };
            if let Err(error) = store.commit(&batch.checks, &batch.mutations) {
                return Outcome::Unknown(format!("normalized argument commit failed: {error}"));
            }
            if cancellation.is_cancelled() {
                return Outcome::Unknown("cancelled after logical dispatch".into());
            }
            let host = Arc::new(SealedLeafHost(std::sync::atomic::AtomicBool::new(false)));
            let reply = self
                .tool
                .invoke_transaction(
                    args,
                    pi_rs_core::ToolContext {
                        operation_id: task.id(),
                        cancellation: cancellation.clone(),
                    },
                    None,
                    host.clone(),
                )
                .await;
            if host.0.load(std::sync::atomic::Ordering::Acquire) || reply.state.is_some() {
                return Outcome::Unknown(format!(
                    "unsupported host operation or state update; returned reply: {}",
                    serde_json::to_string(&reply).unwrap_or_else(|_| "unserializable".into())
                ));
            }
            reply.outcome
        });
        Ok(crate::task_scheduler::Step::Succeed(
            serde_json::to_value(outcome).map_err(|e| e.to_string())?,
        ))
    }
}

impl crate::task_scheduler::Handler for SealedExecutor {
    fn run_abort_with_admission(
        &self,
        task: &Task,
        admission: &crate::task_scheduler::AdmissionHandle<'_>,
        cancellation: Option<&pi_rs_core::CancellationToken>,
    ) -> Result<crate::task_scheduler::Step, String> {
        let cancellation = cancellation.ok_or("missing interrupted cancellation")?;
        if cancellation.is_cancelled() {
            return Err("interrupted cleanup cancelled".into());
        }
        if !task.holds_interrupted_cleanup() || !self.identity.has_interrupted_after() {
            return Err("interrupted cleanup not authorized".into());
        }
        let _identity_lease =
            crate::nested_identity::checked_lease(&self.tool, &self.implementation)
                .map_err(|_| "sealed identity unavailable or changed")?;
        if let Some(value) = admission.interrupted_hook_receipt(self, &self.identity, None)? {
            validate_interrupted_after_result(&value)?;
            return Ok(crate::task_scheduler::Step::Succeed(
                serde_json::Value::Null,
            ));
        }
        let args = self.checkpoint_arguments(task)?;
        if serde_json::to_vec(&args).map_err(|e| e.to_string())?.len() > 60 * 1024 {
            return Err("interrupted hook arguments exceed limit".into());
        }
        let gate = crate::runtime::ToolDiagnostic {
            severity: crate::runtime::DiagnosticSeverity::Error,
            code: Some("interrupted".into()),
            message: "Tool was interrupted; external effects may be unknown".into(),
        };
        let initial = crate::extension::ToolBlockResult {
            output: vec![serde_json::json!({"type":"text","text":gate.message})],
            diagnostics: vec![gate.clone()],
            extra: [("isError".into(), serde_json::json!(true))]
                .into_iter()
                .collect(),
        };
        let memo = InterruptedMemo(std::sync::atomic::AtomicBool::new(false));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let result = rt.block_on(async {
            let call = crate::extension::ToolCall {
                name: self.identity.spec.name.clone(),
                arguments: args,
            };
            let snapshot = serde_json::Value::Null;
            let ctx = crate::extension::Context {
                snapshot: &snapshot,
                memo: &memo,
            };
            let mut registry = crate::extension::Registry::default();
            registry
                .install(crate::extension::Extension {
                    name: "interrupted".into(),
                    hooks: vec![self.before_hook.as_ref().ok_or("missing hook")?.clone()],
                    ..Default::default()
                })
                .map_err(|e| e.to_string())?;
            let selected=registry.snapshot().resolve(None);
            tokio::select! { biased;
                _=cancellation.cancelled()=>Err("interrupted cleanup cancelled".to_string()),
                result=selected.after_tool_blocks_checked(&call,initial,true,&ctx,
                    |candidate|replay_safe::validate_content_hook_result(candidate,Some(&gate),true),
                    |candidate|replay_safe::validate_content_hook_result(candidate,Some(&gate),false))=>result,
            }
        })?;
        if memo.0.load(std::sync::atomic::Ordering::Acquire) {
            return Err("interrupted hook used unavailable memo".into());
        }
        let value = serde_json::to_value(DurableTaskResult::Rich {
            task_content_v1: result,
        })
        .map_err(|e| e.to_string())?;
        validate_interrupted_after_result(&value)?;
        if cancellation.is_cancelled() {
            return Err("interrupted cleanup cancelled".into());
        }
        admission.interrupted_hook_receipt(self, &self.identity, Some(&value))?;
        #[cfg(test)]
        if let Some(dir) = std::env::var_os("PI_TEST_INTERRUPTED_AFTER_RECEIPT") {
            std::fs::write(
                std::path::PathBuf::from(dir).join("interrupted-receipt"),
                b"yes",
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(crate::task_scheduler::Step::Succeed(
            serde_json::Value::Null,
        ))
    }
    fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
        Err("sealed executor requires its driver-owned task reservation".into())
    }
}

struct SealedLeafHost(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl pi_rs_core::ToolHost for SealedLeafHost {
    fn tools(&self) -> Vec<ToolSpec> {
        vec![]
    }
    async fn report_progress(&self, _: serde_json::Value) -> pi_rs_core::ExtensionResult<()> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("progress is unavailable".into())
    }
    async fn ui(
        &self,
        _: pi_rs_core::UiRequest,
    ) -> pi_rs_core::ExtensionResult<pi_rs_core::UiResponse> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("UI is unavailable".into())
    }
    async fn call(&self, _: &str, _: serde_json::Value) -> Outcome {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Outcome::Skipped("nested calls are unavailable".into())
    }
}

/// Sealed task-backed Tool registered in a standalone no-hook host.
pub struct AsyncTaskTool {
    adapter: Arc<crate::task_tool::TaskTool>,
    identity: ToolIdentity,
}
impl AsyncTaskTool {
    /// Immediately admit a keyed sealed child using the caller's live task lease.
    /// The returned handle may be persisted in a subsequent checkpoint.
    pub fn start_child_now(
        &self,
        admission: &crate::task_scheduler::AdmissionHandle<'_>,
        key: &str,
        input: serde_json::Value,
    ) -> Result<AsyncTaskCall, String> {
        let (parent, child) = admission
            .admit_sealed_child(&self.adapter, &self.identity, key, input)
            .map_err(|error| error.to_string())?;
        Ok(AsyncTaskCall(crate::task_tool::NestedTaskCall::new(
            parent, child,
        )))
    }

    /// Stage a direct task-owned call. Its identity, admission and parent
    /// transition commit together; it runs only while the parent is active or
    /// durably waiting on this direct child.
    pub fn start_child(
        &self,
        parent: &Task,
        documents: &mut crate::document::TaskDocuments<'_>,
        key: &str,
        input: serde_json::Value,
    ) -> Result<AsyncTaskCall, String> {
        documents.require_standalone_for_sealed_child()?;
        let (identity, child) = self.adapter.sealed_child_candidate(parent, input)?;
        let id = documents.stage_task_tool_child(
            key,
            &identity,
            child.input(),
            child.clone(),
            self.adapter.handler(),
        )?;
        let handle = crate::task_tool::NestedTaskCall::new(parent.id(), id);
        documents.bind_sealed_child_identity(key, &self.identity)?;
        Ok(AsyncTaskCall(handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, TaskOwner};
    use crate::task_scheduler::Registry;
    use crate::task_store::{Save, TaskStore};
    use pi_rs_core::{ToolContext, ToolSpec};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use uuid::Uuid;

    struct TestTool {
        calls: Arc<AtomicUsize>,
        spec: &'static str,
    }
    #[async_trait::async_trait]
    impl Tool for TestTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.spec.into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Pending {
                reference: "remote-job".into(),
            }
        }
    }

    struct BeforeFixture {
        decision: u8,
        entered: Option<std::sync::mpsc::Sender<()>>,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for BeforeFixture {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(sender) = &self.entered {
                sender.send(()).unwrap();
            }
            match self.decision {
                0 => Ok(crate::extension::BeforeTool::Block("denied".into())),
                1 => Ok(crate::extension::BeforeTool::Rewrite(
                    json!({"rewritten": true}),
                )),
                2 => std::future::pending().await,
                3 => Ok(crate::extension::BeforeTool::Rewrite(json!({"bad": true}))),
                _ => Ok(crate::extension::BeforeTool::Rewrite(
                    json!({"huge": "x".repeat(16 * 1024 * 1024)}),
                )),
            }
        }
    }
    struct BeforeToolFixture(std::sync::mpsc::Sender<serde_json::Value>);
    #[async_trait::async_trait]
    impl Tool for BeforeToolFixture {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "before-fixture".into(),
                description: String::new(),
                parameters: json!({"type":"object", "properties":{"bad":false}}),
            }
        }
        async fn prepare_arguments(
            &self,
            mut args: serde_json::Value,
        ) -> pi_rs_core::ExtensionResult<serde_json::Value> {
            args["prepared"] = json!(true);
            Ok(args)
        }
        async fn invoke(&self, args: serde_json::Value, _: ToolContext) -> Outcome {
            self.0.send(args.clone()).unwrap();
            Outcome::Structured {
                data: args,
                attachments: vec![],
            }
        }
    }
    fn before_fixture_host(
        path: &std::path::Path,
        tool: Arc<dyn Tool>,
        hook: Arc<dyn crate::extension::Hook>,
        hook_id: &str,
    ) -> StandaloneBeforeHookHost {
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        let mut host = StandaloneBeforeHookHost::open(path, registry).unwrap();
        let adapter = host
            .register_tool("leaf", tool, "backend", 1, hook_id, hook)
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        host
    }
    fn before_parent() -> Task {
        Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("parent".into(), true)
        .into_task_tool()
    }
    fn wait_terminal(driver: &crate::task_driver::Driver, id: Uuid) -> Task {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let task = driver.get(id).unwrap().unwrap();
            if task.is_terminal() {
                return task;
            }
            assert!(
                std::time::Instant::now() < until,
                "task did not settle: {:?}",
                task.state()
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    #[test]
    fn before_hook_defers_while_parent_park_is_published() {
        use crate::task::WaitFor;
        use crate::task_scheduler::{Step, TickReservation};
        use std::sync::{OnceLock, mpsc};
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(BeforeFixture {
            decision: 1,
            entered: None,
            calls: calls.clone(),
        });
        let child_id = Arc::new(OnceLock::new());
        let handler_child = child_id.clone();
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, move |task: &Task| {
            if task.checkpoint().phase == "initial" {
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: json!(null),
                    },
                    condition: WaitFor::Tasks(vec![*handler_child.get().unwrap()]),
                    children: vec![],
                })
            } else {
                Ok(Step::Succeed(json!("done")))
            }
        }));
        let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
        let adapter = host
            .register_tool("leaf", tool, "backend", 1, "hook-v1", hook)
            .unwrap();
        let mut scheduler = host.into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let TickReservation::Invocation(parent_job) = scheduler.reserve_tick(0).unwrap() else {
            panic!("parent reservation")
        };
        let owner = scheduler.owner();
        let parent_snapshot = TaskStore::new(owner.store())
            .load(parent.id())
            .unwrap()
            .unwrap();
        let (_, child) = adapter
            .adapter
            .sealed_child_candidate(&parent_snapshot.task, json!({}))
            .unwrap();
        let (child, batch) = crate::async_task_protocol::prepare_child(
            owner.store(),
            &parent_snapshot,
            "child",
            &child,
            &adapter.identity,
        )
        .unwrap();
        owner
            .store()
            .commit(&batch.checks, &batch.mutations)
            .unwrap();
        child_id.set(child).unwrap();
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        crate::task_driver::before_execute_for_test(child, move |_| {
            reserved_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        let driver = crate::task_driver::Driver::start(scheduler);
        reserved_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let parent_result = parent_job.execute().unwrap();
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while driver.get(child).expect("healthy driver").unwrap().state()
            != &crate::task::TaskState::Ready
        {
            assert!(Instant::now() < deadline, "hooked child was not deferred");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(received.try_recv().is_err());
        assert!(driver.settle_for_test(parent_result).unwrap());
        let args = received
            .recv_timeout(Duration::from_secs(5))
            .expect("tool did not resume");
        assert_eq!(args, json!({"rewritten":true}));
        assert_eq!(
            wait_terminal(&driver, parent.id()).state(),
            &crate::task::TaskState::Succeeded(json!("done"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(received.try_recv().is_err());
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn before_driver_block_rewrite_and_reject_invalid() {
        for mode in [0, 1, 3, 4] {
            let dir = tempfile::tempdir().unwrap();
            let (sent, received) = std::sync::mpsc::channel();
            let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let hook = Arc::new(BeforeFixture {
                decision: mode,
                entered: None,
                calls: calls.clone(),
            });
            let scheduler = before_fixture_host(&dir.path().join("db"), tool, hook, "hook-v1")
                .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let task = wait_terminal(&driver, parent.id());
            if mode == 1 {
                assert_eq!(received.try_recv().unwrap(), json!({"rewritten":true}));
                assert_eq!(
                    task.state(),
                    &crate::task::TaskState::Succeeded(json!({"rewritten":true}))
                );
            } else {
                assert!(received.try_recv().is_err());
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let _ = driver.shutdown_with_pending().unwrap();
            if mode != 1 {
                let store = durable_store::Store::open(dir.path().join("db")).unwrap();
                let records = crate::task_store::TaskStore::new(&store).list().unwrap();
                let child = records
                    .iter()
                    .find(|r| r.task.owner() == &TaskOwner::Task(parent.id()))
                    .unwrap();
                assert_eq!(
                    read_sealed_outcome_evidence(&store, child.task.id()).unwrap(),
                    Some(SealedOutcomeEvidence::NotDispatched)
                );
            }
        }
    }
    #[test]
    fn before_driver_abort_cancels_hook_before_intent() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let (entered, waiting) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let hook = Arc::new(BeforeFixture {
            decision: 2,
            entered: Some(entered),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let scheduler = before_fixture_host(&dir.path().join("db"), tool, hook, "hook-v1")
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        waiting
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent.id()).unwrap();
        let task = wait_terminal(&driver, parent.id());
        // Neither fixture registers abort cleanup; the scheduler conservatively
        // orphans the parent while still preventing leaf dispatch.
        assert!(
            matches!(task.state(), crate::task::TaskState::Failed(reason) if reason.contains("orphaned")),
            "{:?}",
            task.state()
        );
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn before_driver_reopen_uses_committed_execute_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(BeforeFixture {
            decision: 1,
            entered: None,
            calls: calls.clone(),
        });
        let mut scheduler = before_fixture_host(&path, tool.clone(), hook.clone(), "hook-v1")
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let saved: AsyncTaskCall = serde_json::from_value(
            scheduler
                .get(parent.id())
                .unwrap()
                .unwrap()
                .checkpoint()
                .data
                .clone(),
        )
        .unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler
                .get(saved.task_id())
                .unwrap()
                .unwrap()
                .checkpoint()
                .phase,
            "sealed.execute.v1"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(scheduler);
        let driver = before_fixture_host(&path, tool, hook, "hook-v1").into_driver();
        let task = wait_terminal(&driver, parent.id());
        assert_eq!(
            task.state(),
            &crate::task::TaskState::Succeeded(json!({"rewritten":true}))
        );
        assert_eq!(received.try_recv().unwrap(), json!({"rewritten":true}));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
    }

    #[test]
    fn before_driver_reopen_rejects_changed_hook_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(BeforeFixture {
            decision: 1,
            entered: None,
            calls: calls.clone(),
        });
        let mut scheduler = before_fixture_host(&path, tool.clone(), hook.clone(), "hook-v1")
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(scheduler);
        let driver = before_fixture_host(&path, tool, hook, "hook-v2").into_driver();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match driver.get(parent.id()) {
                Err(error) => {
                    assert!(
                        error
                            .to_string()
                            .contains("before-hook registration changed")
                    );
                    break;
                }
                Ok(_) => {
                    assert!(
                        std::time::Instant::now() < until,
                        "driver did not reject changed hook"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        }
        assert!(received.try_recv().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending();
    }

    struct MemoHook {
        invalid: bool,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for MemoHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if self.invalid {
                let _ = ctx.memo.get_or_insert("", json!(1)).await;
                return Ok(crate::extension::BeforeTool::Pass);
            }
            let first = ctx.memo.get_or_insert("key", json!("first")).await?;
            let second = ctx.memo.get_or_insert("key", json!("second")).await?;
            let read = ctx.memo.get("key").await?;
            if first != json!("first") || second != first || read != Some(first) {
                return Err("memo did not retain first value".into());
            }
            Ok(crate::extension::BeforeTool::Rewrite(json!({"memo":true})))
        }
    }

    #[test]
    fn driver_fences_memo_and_rejects_ignored_memo_errors() {
        for invalid in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (sent, received) = std::sync::mpsc::channel();
            let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = before_fixture_host(
                &dir.path().join("db"),
                tool,
                Arc::new(MemoHook { invalid }),
                "memo-v1",
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let task = wait_terminal(&driver, parent.id());
            if invalid {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
                assert!(received.try_recv().is_err());
            } else {
                assert_eq!(received.try_recv().unwrap(), json!({"memo":true}));
                assert!(matches!(task.state(), crate::task::TaskState::Succeeded(_)));
            }
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct StaleMemoHook {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        result: std::sync::mpsc::Sender<bool>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for StaleMemoHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            let rejected = ctx.memo.get_or_insert("late", json!(1)).await.is_err();
            self.result.send(rejected).unwrap();
            Ok(crate::extension::BeforeTool::Pass)
        }
    }
    #[test]
    fn driver_rejects_memo_after_ancestor_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = StaleMemoHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        };
        let scheduler =
            before_fixture_host(&dir.path().join("db"), tool, Arc::new(hook), "memo-v1")
                .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent.id()).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        let _ = wait_terminal(&driver, parent.id());
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct AfterFixture {
        root: std::path::PathBuf,
        block: bool,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for AfterFixture {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            let proposed = if self.root.join("after-ready").exists() {
                "second"
            } else {
                "first"
            };
            let value = ctx.memo.get_or_insert("after", json!(proposed)).await?;
            if value != json!("first") {
                return Err("memo changed".into());
            }
            if self.block && !self.root.join("after-ready").exists() {
                std::fs::write(self.root.join("after-ready"), b"ready")
                    .map_err(|e| e.to_string())?;
                std::future::pending().await
            } else {
                Ok(None)
            }
        }
    }
    struct PersistentCountTool(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for PersistentCountTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "persistent-count".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.0.join("invocations"))
                .unwrap();
            writeln!(file, "invoked").unwrap();
            file.sync_all().unwrap();
            Outcome::Structured {
                data: json!({"done":true}),
                attachments: vec![],
            }
        }
    }
    fn after_fixture_host(
        path: &std::path::Path,
        tool: Arc<dyn Tool>,
        hook: Arc<dyn crate::extension::Hook>,
    ) -> StandaloneBeforeHookHost {
        after_fixture_host_with_id(path, tool, hook, "after-v1")
    }
    fn after_fixture_host_with_id(
        path: &std::path::Path,
        tool: Arc<dyn Tool>,
        hook: Arc<dyn crate::extension::Hook>,
        identity: &str,
    ) -> StandaloneBeforeHookHost {
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        let mut host = StandaloneBeforeHookHost::open(path, registry).unwrap();
        let adapter = host
            .register_tool_with_after("leaf", tool, "backend", 1, identity, hook)
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        host
    }
    #[test]
    fn after_driver_persists_result_and_runs_fenced_memo() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let scheduler = after_fixture_host(
            &dir.path().join("db"),
            tool,
            Arc::new(AfterFixture {
                root: dir.path().to_owned(),
                block: false,
            }),
        )
        .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let task = wait_terminal(&driver, parent.id());
        assert_eq!(
            task.state(),
            &crate::task::TaskState::Succeeded(json!({"done":true}))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct TextTool;
    #[async_trait::async_trait]
    impl Tool for TextTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "text".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded("original".into())
        }
    }
    struct RewriteAfter;
    #[async_trait::async_trait]
    impl crate::extension::Hook for RewriteAfter {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            result: &serde_json::Value,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            assert_eq!(result, &json!("original"));
            Ok(Some(json!("rewritten")))
        }
    }
    #[test]
    fn after_driver_returns_rewritten_text() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(TextTool);
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let scheduler = after_fixture_host(&dir.path().join("db"), tool, Arc::new(RewriteAfter))
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert_eq!(
            wait_terminal(&driver, parent.id()).state(),
            &crate::task::TaskState::Succeeded(json!("rewritten"))
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct AppendAfter(&'static str);
    #[async_trait::async_trait]
    impl crate::extension::Hook for AppendAfter {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            value: &serde_json::Value,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            Ok(Some(json!(format!(
                "{}{}",
                value.as_str().ok_or("nontext")?,
                self.0
            ))))
        }
    }
    #[test]
    fn after_driver_runs_selected_extensions_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(TextTool);
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let mut extensions = crate::extension::Registry::default();
        for (name, suffix) in [("one", "+one"), ("two", "+two")] {
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![Arc::new(AppendAfter(suffix))],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(1, name.into(), vec!["hook".into()])
                        .unwrap(),
                )
                .unwrap();
        }
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
        let adapter = host
            .register_selected_tool_with_after(
                "leaf",
                tool,
                "backend",
                1,
                &extensions.snapshot(),
                &["two".into(), "one".into()],
            )
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        let scheduler = host.into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert_eq!(
            wait_terminal(&driver, parent.id()).state(),
            &crate::task::TaskState::Succeeded(json!("original+two+one"))
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct DetailedTextTool;
    #[async_trait::async_trait]
    impl Tool for DetailedTextTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "details".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            Outcome::SucceededWithDetails {
                output: "original".into(),
                details: json!({"important":42}),
            }
        }
    }
    #[test]
    fn after_driver_text_rewrite_retains_details_in_final_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(DetailedTextTool);
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let mut scheduler =
            after_fixture_host(&dir.path().join("db"), tool, Arc::new(RewriteAfter))
                .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let parent_task = scheduler.get(parent.id()).unwrap().unwrap();
        let child: AsyncTaskCall =
            serde_json::from_value(parent_task.checkpoint().data.clone()).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert_eq!(
            wait_terminal(&driver, parent.id()).state(),
            &crate::task::TaskState::Succeeded(
                json!({"output":"rewritten", "details":{"important":42}})
            )
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        assert!(
            matches!(read_sealed_outcome_evidence(&store,child.task_id()).unwrap(),Some(SealedOutcomeEvidence::Final(Outcome::SucceededWithDetails {output,details})) if output=="rewritten" && details==json!({"important":42}))
        );
        assert!(
            matches!(crate::async_task_protocol::prepare_receipt_read(&store, child.task_id()).unwrap().0,
            Some(crate::async_task_protocol::ReceiptRead::Final(Outcome::SucceededWithDetails { output, details })) if output == "rewritten" && details == json!({"important":42}))
        );
    }
    #[test]
    fn after_driver_restart_limit_retains_raw_structured_result() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let hook = Arc::new(AfterFixture {
            root: dir.path().to_owned(),
            block: false,
        });
        let mut scheduler =
            after_fixture_host(&path, tool.clone(), hook.clone()).into_scheduler_for_test();
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            1,
        )
        .unwrap()
        .registered("parent".into(), true)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let parent_task = scheduler.get(parent.id()).unwrap().unwrap();
        let child: AsyncTaskCall =
            serde_json::from_value(parent_task.checkpoint().data.clone()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let crate::task_scheduler::TickReservation::Invocation(job) =
            scheduler.reserve_tick(0).unwrap()
        else {
            panic!("missing tool invocation")
        };
        let completed = job.execute().unwrap();
        drop(completed);
        drop(scheduler);
        let driver = after_fixture_host(&path, tool, hook).into_driver();
        assert!(matches!(
            wait_terminal(&driver, parent.id()).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            matches!(read_raw_outcome_for_reconciliation(&store, child.task_id()).unwrap(), Some(Outcome::Structured { data, .. }) if data == json!({"done":true}))
        );
        assert!(
            matches!(crate::async_task_protocol::prepare_receipt_read(&store, child.task_id()).unwrap().0,
            Some(crate::async_task_protocol::ReceiptRead::AfterIncomplete(Outcome::Structured { data, .. })) if data == json!({"done":true}))
        );
        assert!(
            matches!(read_sealed_outcome_evidence(&store,child.task_id()).unwrap(),Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Structured {data,..})) if data==json!({"done":true}))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    struct CountRewrite(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::extension::Hook for CountRewrite {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(json!("finished")))
        }
    }
    #[test]
    fn after_driver_reopen_after_final_receipt_does_not_repeat_hook() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let tool: Arc<dyn Tool> = Arc::new(TextTool);
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(CountRewrite(count.clone()));
        let mut scheduler =
            after_fixture_host(&path, tool.clone(), hook.clone()).into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.tick(0).unwrap());
        let crate::task_scheduler::TickReservation::Invocation(job) =
            scheduler.reserve_tick(0).unwrap()
        else {
            panic!("missing tool invocation")
        };
        let mut completed = job.execute().unwrap();
        completed.capture_marked_receipt().unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        drop(completed);
        drop(scheduler);
        let driver = after_fixture_host(&path, tool, hook).into_driver();
        assert_eq!(
            wait_terminal(&driver, parent.id()).state(),
            &crate::task::TaskState::Succeeded(json!("finished"))
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct ReceiptKillHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for ReceiptKillHook {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("after-invocations"))
                .map_err(|e| e.to_string())?;
            writeln!(file, "after").map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            Ok(None)
        }
    }

    #[test]
    fn after_driver_real_kill_after_receipt_before_settlement() {
        const ENV: &str = "PI_RS_AFTER_RECEIPT_KILL_DIR";
        if let Some(root) = std::env::var_os(ENV) {
            let root = std::path::PathBuf::from(root);
            let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(root.clone()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = after_fixture_host(
                &root.join("db"),
                tool,
                Arc::new(ReceiptKillHook(root.clone())),
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            std::fs::write(root.join("parent"), parent.id().to_string()).unwrap();
            let _driver = crate::task_driver::Driver::start(scheduler);
            loop {
                std::thread::park();
            }
        }
        struct ChildGuard(std::process::Child);
        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut child = ChildGuard(std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "async_task_tool::tests::after_driver_real_kill_after_receipt_before_settlement"])
            .env(ENV, dir.path()).spawn().unwrap());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("receipt-ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "fixture exited before receipt"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "receipt boundary timed out"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let parent: Uuid = std::fs::read_to_string(dir.path().join("parent"))
            .unwrap()
            .parse()
            .unwrap();
        let leaf: Uuid = std::fs::read_to_string(dir.path().join("receipt-ready"))
            .unwrap()
            .parse()
            .unwrap();
        {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let saved = crate::task_store::TaskStore::new(&store)
                .load(leaf)
                .unwrap()
                .unwrap();
            assert_eq!(saved.task.state(), &crate::task::TaskState::Running);
            assert!(
                matches!(crate::async_task_protocol::prepare_receipt_read(&store, leaf).unwrap().0,
                Some(crate::async_task_protocol::ReceiptRead::Final(Outcome::Structured { data, .. })) if data == json!({"done":true}))
            );
        }
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = after_fixture_host(
            &dir.path().join("db"),
            tool,
            Arc::new(ReceiptKillHook(dir.path().to_owned())),
        )
        .into_driver();
        assert_eq!(
            wait_terminal(&driver, parent).state(),
            &crate::task::TaskState::Succeeded(json!({"done":true}))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("after-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        driver.shutdown().unwrap();
    }

    #[test]
    fn after_driver_reopen_rejects_changed_hook_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let hook = Arc::new(AfterFixture {
            root: dir.path().to_owned(),
            block: false,
        });
        let mut scheduler =
            after_fixture_host(&path, tool.clone(), hook.clone()).into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.tick(0).unwrap());
        let crate::task_scheduler::TickReservation::Invocation(job) =
            scheduler.reserve_tick(0).unwrap()
        else {
            panic!("missing tool invocation")
        };
        let completed = job.execute().unwrap();
        drop(completed);
        drop(scheduler);
        let driver = after_fixture_host_with_id(&path, tool, hook, "after-v2").into_driver();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match driver.get(parent.id()) {
                Err(error) => {
                    assert!(error.to_string().contains("registration changed"));
                    break;
                }
                Ok(Some(task)) if task.is_terminal() => {
                    assert!(!matches!(
                        task.state(),
                        crate::task::TaskState::Succeeded(_)
                    ));
                    break;
                }
                _ => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        }
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending();
    }

    struct BadAfterMemo;
    #[async_trait::async_trait]
    impl crate::extension::Hook for BadAfterMemo {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            let _ = ctx.memo.get_or_insert("", json!(1)).await;
            Ok(Some(json!("should-not-publish")))
        }
    }
    #[test]
    fn after_driver_rejects_ignored_memo_error() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(TextTool);
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let scheduler = after_fixture_host(&dir.path().join("db"), tool, Arc::new(BadAfterMemo))
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let task = wait_terminal(&driver, parent.id());
        assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct AbortAfterHook {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        result: std::sync::mpsc::Sender<bool>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for AbortAfterHook {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            self.result
                .send(
                    ctx.memo
                        .get_or_insert("late-after", json!(1))
                        .await
                        .is_err(),
                )
                .unwrap();
            Ok(None)
        }
    }
    #[test]
    fn after_driver_abort_rejects_late_memo_and_does_not_repeat_tool() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = AbortAfterHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        };
        let scheduler = after_fixture_host(&dir.path().join("db"), tool, Arc::new(hook))
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let running_parent = driver.get(parent.id()).unwrap().unwrap();
        let child: AsyncTaskCall =
            serde_json::from_value(running_parent.checkpoint().data.clone()).unwrap();
        driver.request_abort(parent.id()).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        let task = wait_terminal(&driver, parent.id());
        assert!(!matches!(
            task.state(),
            crate::task::TaskState::Succeeded(_)
        ));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let outcome = read_raw_outcome_for_reconciliation(&store, child.task_id())
            .unwrap()
            .unwrap();
        assert!(
            matches!(outcome, Outcome::Structured { data, .. } if data == json!({"done":true}))
        );
        assert!(
            matches!(crate::async_task_protocol::prepare_receipt_read(&store, child.task_id()).unwrap().0,
            Some(crate::async_task_protocol::ReceiptRead::AfterIncomplete(Outcome::Structured { data, .. })) if data == json!({"done":true}))
        );
    }

    struct RecoveryBusyHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for RecoveryBusyHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if !self.0.join("resume").exists() {
                std::fs::write(self.0.join("unsafe-ready"), b"ready").unwrap();
                std::future::pending::<()>().await;
            }
            Ok(crate::extension::BeforeTool::Pass)
        }
    }

    fn recovery_busy_fixture(unsafe_hook: bool, driver_recovery: bool) {
        use crate::task_scheduler::Step;
        use std::time::{Duration, Instant};
        const ENV: &str = "PI_TEST_RECOVERY_BUSY_1120";
        if let Some(root) = std::env::var_os(ENV) {
            let root = std::path::PathBuf::from(root);
            let mut host =
                StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
            let tool = host
                .register_unsafe_durable_tool_with_hook(
                    Arc::new(HookRaceTool(Arc::new(AtomicUsize::new(0)))),
                    "unsafe-recovery",
                    1,
                    vec![],
                    "recovery-v1",
                    Arc::new(RecoveryBusyHook(root.clone())),
                    UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let id = host.start_unsafe_durable_root(&tool, json!({}), 5).unwrap();
            std::fs::write(root.join("task"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = if unsafe_hook {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "async_task_tool::tests::unsafe_hook_recovery_retries_writer_timeout",
                ])
                .env(ENV, dir.path())
                .spawn()
                .unwrap()
        } else {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "async_task_tool::tests::after_driver_kill_reopen_reuses_result_and_memo",
                ])
                .env("PI_RS_TASK_AFTER_KILL_1120", dir.path())
                .spawn()
                .unwrap()
        };
        let marker = dir.path().join(if unsafe_hook {
            "unsafe-ready"
        } else {
            "after-ready"
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        while !marker.exists() {
            assert!(process.try_wait().unwrap().is_none(), "kill worker exited");
            assert!(Instant::now() < deadline, "kill boundary timed out");
            std::thread::sleep(Duration::from_millis(5));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = TaskStore::new(&store);
        let parent: Uuid =
            std::fs::read_to_string(dir.path().join(if unsafe_hook { "task" } else { "parent" }))
                .unwrap()
                .parse()
                .unwrap();
        let target = if unsafe_hook {
            parent
        } else {
            tasks
                .list()
                .unwrap()
                .into_iter()
                .find(|r| r.task.owner() == &TaskOwner::Task(parent))
                .unwrap()
                .task
                .id()
        };
        assert_eq!(
            tasks.load(target).unwrap().unwrap().task.state(),
            &crate::task::TaskState::Running
        );
        let other = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("independent".into(), true);
        tasks.save(&[Save::Create(&other)]).unwrap();
        // A pending failure owns the target's conversation. Removing this
        // test edge after registration releases recovery only when the driver starts.
        let blocked_edge = if driver_recovery {
            let root = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("independent".into(), true);
            tasks.save(&[Save::Create(&root)]).unwrap();
            tasks
                .update(&tasks.load(root.id()).unwrap().unwrap(), |task| {
                    task.start()?;
                    task.hold_failure("pending abort".into())
                })
                .unwrap();
            let mut item = tasks.load(target).unwrap().unwrap().task;
            let conversation = loop {
                match *item.owner() {
                    TaskOwner::Conversation(id) => break id,
                    TaskOwner::Task(id) => item = tasks.load(id).unwrap().unwrap().task,
                }
            };
            let edge = crate::ownership_edge::Edge {
                task: root.id(),
                task_owner: root.owner().clone(),
                child: conversation,
                mode: crate::ownership_edge::Mode::Foreground,
            };
            let key = crate::ownership_edge::edge_key(conversation);
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key: key.clone(),
                        value: serde_json::to_vec(&edge).unwrap(),
                    }],
                )
                .unwrap();
            Some(key)
        } else {
            None
        };
        drop(store);
        std::fs::write(dir.path().join("resume"), b"resume").unwrap();
        let (writer_tx, writer_rx) = std::sync::mpsc::channel();
        let (unlock_tx, unlock_rx) = std::sync::mpsc::channel();
        let writer_path = path.clone();
        crate::task_scheduler::before_after_phase_recovery_for_test(target, move || {
            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            let writer = std::thread::spawn(move || {
                let conn = rusqlite::Connection::open(writer_path).unwrap();
                conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                locked_tx.send(()).unwrap();
                unlock_rx.recv_timeout(Duration::from_secs(20)).unwrap();
                conn.execute_batch("COMMIT").unwrap();
            });
            locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            writer_tx.send(writer).unwrap();
        });
        let (busy_tx, busy_rx) = std::sync::mpsc::channel();
        crate::task_scheduler::on_after_phase_recovery_busy_for_test(target, move || {
            busy_tx.send(()).unwrap();
            unlock_tx.send(()).unwrap();
        });
        let mut registry = Registry::default();
        assert!(
            registry.register("independent", true, |_: &Task| Ok(Step::Succeed(json!(
                "ok"
            ))))
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let driver = if unsafe_hook {
            let mut host = StandaloneBeforeHookHost::open(&path, registry).unwrap();
            host.register_unsafe_durable_tool_with_hook(
                Arc::new(HookRaceTool(calls.clone())),
                "unsafe-recovery",
                1,
                vec![],
                "recovery-v1",
                Arc::new(RecoveryBusyHook(dir.path().to_owned())),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
            if let Some(edge) = &blocked_edge {
                assert!(
                    busy_rx.try_recv().is_err(),
                    "recovery ran during registration"
                );
                let store = durable_store::Store::open(&path).unwrap();
                let item = store.get(edge).unwrap().unwrap();
                store
                    .commit(
                        &[durable_store::Check {
                            key: edge.clone(),
                            expected: durable_store::Expected::Version(item.version),
                        }],
                        &[durable_store::Mutation::Delete { key: edge.clone() }],
                    )
                    .unwrap();
            }
            host.into_driver()
        } else {
            let cell = Arc::new(std::sync::OnceLock::new());
            assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
            let mut host = StandaloneBeforeHookHost::open(&path, registry).unwrap();
            let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let adapter = host
                .register_tool_with_after(
                    "leaf",
                    tool,
                    "backend",
                    1,
                    "after-v1",
                    Arc::new(AfterFixture {
                        root: dir.path().to_owned(),
                        block: true,
                    }),
                )
                .unwrap();
            assert!(cell.set(adapter).is_ok());
            if let Some(edge) = &blocked_edge {
                assert!(
                    busy_rx.try_recv().is_err(),
                    "recovery ran during registration"
                );
                let store = durable_store::Store::open(&path).unwrap();
                let item = store.get(edge).unwrap().unwrap();
                store
                    .commit(
                        &[durable_store::Check {
                            key: edge.clone(),
                            expected: durable_store::Expected::Version(item.version),
                        }],
                        &[durable_store::Mutation::Delete { key: edge.clone() }],
                    )
                    .unwrap();
            }
            host.into_driver()
        };
        busy_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("writer timeout was not observed");
        writer_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .join()
            .unwrap();
        assert!(matches!(
            wait_terminal(&driver, parent).state(),
            crate::task::TaskState::Succeeded(_)
        ));
        assert_eq!(
            wait_terminal(&driver, other.id()).state(),
            &crate::task::TaskState::Succeeded(json!("ok"))
        );
        if unsafe_hook {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(
                std::fs::read_to_string(dir.path().join("invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn unsafe_hook_recovery_retries_writer_timeout() {
        recovery_busy_fixture(true, false);
    }

    #[test]
    fn sealed_after_recovery_retries_writer_timeout() {
        recovery_busy_fixture(false, false);
    }

    #[test]
    fn unsafe_hook_deferred_driver_recovery_retries_writer_timeout() {
        recovery_busy_fixture(true, true);
    }

    #[test]
    fn sealed_after_deferred_driver_recovery_retries_writer_timeout() {
        recovery_busy_fixture(false, true);
    }

    #[test]
    fn after_reopen_identity_busy_after_reservation_retries_without_redispatch() {
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "async_task_tool::tests::after_driver_kill_reopen_reuses_result_and_memo",
            ])
            .env("PI_RS_TASK_AFTER_KILL_1120", dir.path())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !dir.path().join("after-ready").exists() {
            assert!(process.try_wait().unwrap().is_none(), "kill worker exited");
            assert!(Instant::now() < deadline, "after hook did not start");
            std::thread::sleep(Duration::from_millis(5));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let parent: Uuid = std::fs::read_to_string(dir.path().join("parent"))
            .unwrap()
            .parse()
            .unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let child = crate::task_store::TaskStore::new(&store)
            .list()
            .unwrap()
            .into_iter()
            .find(|r| r.task.owner() == &TaskOwner::Task(parent))
            .unwrap()
            .task
            .id();
        assert!(
            crate::async_task_protocol::read_raw_result(&store, child)
                .unwrap()
                .is_some()
        );
        drop(store);
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let mut scheduler = after_fixture_host(
            &path,
            tool,
            Arc::new(AfterFixture {
                root: dir.path().to_owned(),
                block: true,
            }),
        )
        .into_scheduler_for_test();
        let mut registry = scheduler.registry_for_test();
        assert!(registry.register("independent", true, |_: &Task| Ok(
            crate::task_scheduler::Step::Succeed(json!("ok"))
        )));
        scheduler.replace_registry(registry).unwrap();
        let other = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("independent".into(), true);
        scheduler.submit(&other).unwrap();
        let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        crate::task_driver::before_execute_for_test(child, move |_| {
            reserved_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        let driver = crate::task_driver::Driver::start(scheduler);
        reserved_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (updating_tx, updating_rx) = std::sync::mpsc::channel();
        let (unlock_tx, unlock_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(binding.update(Some("backend".into()), move || {
                    updating_tx.send(()).unwrap();
                    unlock_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                }))
                .unwrap()
        });
        updating_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        release_tx.send(()).unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let task = driver
                .get(child)
                .expect("identity busy must not stop driver")
                .unwrap();
            let independent = driver.get(other.id()).unwrap().unwrap();
            if task.state() == &crate::task::TaskState::Ready && independent.is_terminal() {
                break;
            }
            assert!(
                Instant::now() < until,
                "busy after phase or independent graph did not progress"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        unlock_tx.send(()).unwrap();
        updater.join().unwrap();
        assert_eq!(
            wait_terminal(&driver, parent).state(),
            &crate::task::TaskState::Succeeded(json!({"done":true}))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn after_driver_kill_reopen_reuses_result_and_memo() {
        const ENV: &str = "PI_RS_TASK_AFTER_KILL_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(root.clone()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = after_fixture_host(
                &root.join("db"),
                tool,
                Arc::new(AfterFixture {
                    root: root.clone(),
                    block: true,
                }),
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            std::fs::write(root.join("parent"), parent.id().to_string()).unwrap();
            let _driver = crate::task_driver::Driver::start(scheduler);
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::after_driver_kill_reopen_reuses_result_and_memo")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("after-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("after hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let parent: Uuid = std::fs::read_to_string(dir.path().join("parent"))
            .unwrap()
            .parse()
            .unwrap();
        let tool: Arc<dyn Tool> = Arc::new(PersistentCountTool(dir.path().to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = after_fixture_host(
            &dir.path().join("db"),
            tool,
            Arc::new(AfterFixture {
                root: dir.path().to_owned(),
                block: true,
            }),
        )
        .into_driver();
        let task = wait_terminal(&driver, parent);
        assert_eq!(
            task.state(),
            &crate::task::TaskState::Succeeded(json!({"done":true}))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct KillBeforeHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for KillBeforeHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            ctx.memo
                .get_or_insert("kill-marker", json!("saved"))
                .await?;
            std::fs::write(self.0.join("hook-ready"), b"ready").map_err(|e| e.to_string())?;
            std::future::pending().await
        }
    }
    #[test]
    fn driver_kill_during_before_reopens_without_dispatch() {
        const ENV: &str = "PI_RS_TASK_BEFORE_KILL_1119";
        if let Ok(path) = std::env::var(ENV) {
            let path = std::path::PathBuf::from(path);
            let (sent, _received) = std::sync::mpsc::channel();
            let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = before_fixture_host(
                &path.join("db"),
                tool,
                Arc::new(KillBeforeHook(path.clone())),
                "kill-v1",
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            std::fs::write(path.join("parent"), parent.id().to_string()).unwrap();
            let _driver = crate::task_driver::Driver::start(scheduler);
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::driver_kill_during_before_reopens_without_dispatch")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("hook-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("before hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let parent: Uuid = std::fs::read_to_string(dir.path().join("parent"))
            .unwrap()
            .parse()
            .unwrap();
        {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let parent_task = TaskStore::new(&store).load(parent).unwrap().unwrap().task;
            let handle: AsyncTaskCall =
                serde_json::from_value(parent_task.checkpoint().data.clone()).unwrap();
            let key = format!("runtime/extension-memo/{}/kill-marker", handle.task_id());
            let stored = store.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&stored.value).unwrap(),
                json!("saved")
            );
        }
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = before_fixture_host(
            &dir.path().join("db"),
            tool,
            Arc::new(KillBeforeHook(dir.path().to_owned())),
            "kill-v1",
        )
        .into_driver();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match driver.get(parent) {
                Ok(Some(task)) if task.is_terminal() => break,
                Err(_) => break,
                _ => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "reopened task did not fail closed"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        }
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending();
    }

    #[test]
    fn selected_extensions_execute_in_driver_and_bind_order_to_identity() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut extensions = crate::extension::Registry::default();
        for (name, mode) in [("first", 1), ("second", 0)] {
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![Arc::new(BeforeFixture {
                            decision: mode,
                            entered: None,
                            calls: calls.clone(),
                        })],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(1, name.into(), vec!["hook".into()])
                        .unwrap(),
                )
                .unwrap();
        }
        let snapshot = extensions.snapshot();
        let names = vec!["first".into(), "second".into()];
        let mut registry = Registry::default();
        let cell = Arc::new(std::sync::OnceLock::new());
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
        assert!(
            host.register_selected_tool(
                "invalid",
                tool.clone(),
                "backend",
                1,
                &snapshot,
                &["missing".into()]
            )
            .is_err()
        );
        let adapter = host
            .register_selected_tool("leaf", tool, "backend", 1, &snapshot, &names)
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        let scheduler = host.into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert!(matches!(
            wait_terminal(&driver, parent.id()).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct DurableStub(Arc<AtomicUsize>);

    // This adapter deliberately exists only in unit tests. It exercises the
    // real driver without granting an external Tool a replay-safe policy.
    struct TestDurableHandler {
        tool: Arc<dyn DurableAsyncTool>,
        child: Option<Arc<crate::task_tool::TaskTool>>,
    }
    struct TestChildHost<'a, 'b> {
        task: Task,
        docs: std::sync::Mutex<&'a mut crate::document::TaskDocuments<'b>>,
        child: Option<Arc<crate::task_tool::TaskTool>>,
    }
    #[async_trait::async_trait]
    impl DurableTaskHost for TestChildHost<'_, '_> {
        async fn start_child(
            &self,
            key: &str,
            tool: &str,
            input: serde_json::Value,
        ) -> Result<AsyncTaskCall, String> {
            let child = self.child.as_ref().ok_or("test host has no child")?;
            if child.spec().name != tool {
                return Err("test child is not registered".into());
            }
            let mut docs = self.docs.lock().map_err(|e| e.to_string())?;
            child
                .start_child(&self.task, &mut docs, key, input)
                .map(AsyncTaskCall)
        }
        async fn child_result(
            &self,
            child: &AsyncTaskCall,
        ) -> Result<Option<AsyncTaskResult>, String> {
            let mut docs = self.docs.lock().map_err(|e| e.to_string())?;
            use crate::task_tool::NestedTaskResult;
            Ok(match child.0.result(&self.task, &mut docs)? {
                Some(NestedTaskResult::Succeeded(value)) => Some(AsyncTaskResult::Outcome(
                    serde_json::from_value(value)
                        .map_err(|e| format!("invalid child outcome: {e}"))?,
                )),
                Some(NestedTaskResult::Failed(reason)) => Some(AsyncTaskResult::Failed(reason)),
                Some(NestedTaskResult::Cancelled) => Some(AsyncTaskResult::Cancelled),
                None => None,
            })
        }
    }
    impl crate::task_scheduler::Handler for TestDurableHandler {
        fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
            Err("test executor requires admission".into())
        }
        fn run_with_admission(
            &self,
            task: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
            admission: &crate::task_scheduler::AdmissionHandle<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            use crate::task_scheduler::Step;
            let intent = admission
                .checkpointable_durable_intent(true, true, None)
                .map_err(|e| e.to_string())?;
            let resume = match task.checkpoint().phase.as_str() {
                "initial" => DurableToolResume {
                    checkpoint: None,
                    ended: vec![],
                },
                "test-park"
                    if task.delivered_signal().is_some() || task.ended_children().is_some() =>
                {
                    DurableToolResume {
                        checkpoint: Some(task.checkpoint().data.clone()),
                        ended: task.ended_children().unwrap_or_default().to_vec(),
                    }
                }
                _ => return Err("invalid test checkpoint or wake".into()),
            };
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .map_err(|e| e.to_string())?;
            let host = TestChildHost {
                task: task.clone(),
                docs: std::sync::Mutex::new(docs),
                child: self.child.clone(),
            };
            let result = runtime.block_on(self.tool.execute_durable(
                task.input().clone(),
                ToolContext {
                    operation_id: task.id(),
                    cancellation: pi_rs_core::CancellationToken::new(),
                },
                resume,
                &host,
            ))?;
            let docs = host.docs.into_inner().map_err(|e| e.to_string())?;
            docs.stage_checkpointable_durable_result(intent, &result)?;
            match result {
                DurableToolStep::Complete(outcome) => Ok(Step::Succeed(
                    serde_json::to_value(outcome).map_err(|e| e.to_string())?,
                )),
                DurableToolStep::Park {
                    checkpoint,
                    wait: DurableWait::Signal(signal),
                } if !signal.is_empty() => Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "test-park".into(),
                        data: checkpoint,
                    },
                    condition: crate::task::WaitFor::Signal(signal),
                    children: vec![],
                }),
                DurableToolStep::Park {
                    checkpoint,
                    wait: DurableWait::All(calls),
                } if !calls.is_empty() => Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "test-park".into(),
                        data: checkpoint,
                    },
                    condition: crate::task::WaitFor::Tasks(
                        calls.iter().map(AsyncTaskCall::task_id).collect(),
                    ),
                    children: vec![],
                }),
                DurableToolStep::Park {
                    checkpoint,
                    wait: DurableWait::Any(calls),
                } if !calls.is_empty() => Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "test-park".into(),
                        data: checkpoint,
                    },
                    condition: crate::task::WaitFor::TasksAny(
                        calls.iter().map(AsyncTaskCall::task_id).collect(),
                    ),
                    children: vec![],
                }),
                DurableToolStep::Park { .. } => Err("test host supports only signal waits".into()),
            }
        }
    }
    type ParkBarrier = Arc<(std::sync::Mutex<(bool, bool)>, std::sync::Condvar)>;
    struct PureParkTool {
        calls: Arc<AtomicUsize>,
        barrier: Option<ParkBarrier>,
        fail: bool,
    }

    #[test]
    fn reserved_unsafe_child_defers_if_unsafe_parent_starts_before_dispatch() {
        use crate::task_scheduler::{Step, TickReservation};
        use crate::task_store::{Save, TaskStore};
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        assert!(
            registry.register("unsafe-parent-test", false, |_: &Task| Ok(Step::Succeed(
                json!(null)
            )))
        );
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            registry,
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let child_tool = host
            .register_unsafe_durable_tool(
                Arc::new(PureParkTool {
                    calls: calls.clone(),
                    barrier: None,
                    fail: false,
                }),
                "reserved-child-v1",
                1,
                vec![],
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
            .unwrap();
        let mut scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("unsafe-parent-test".into(), false)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let tasks = TaskStore::new(owner.store());
        let parent_snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&parent_snapshot, |task| {
                task.start_attempt(Uuid::new_v4())?;
                task.wait(crate::task::WaitFor::Signal("hold".into()))
            })
            .unwrap();
        let waiting_parent = tasks.load(parent.id()).unwrap().unwrap().task;
        let (_, child) = child_tool
            .child_candidate(&waiting_parent, json!({}))
            .unwrap();
        tasks.save(&[Save::Create(&child)]).unwrap();
        let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
            panic!("child not reserved");
        };
        assert_eq!(job.task_id(), child.id());
        let parent_snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&parent_snapshot, |task| {
                task.wake()?;
                task.start_attempt(Uuid::new_v4())
            })
            .unwrap();
        let completed = job.execute().unwrap();
        assert!(matches!(
            completed.handler_result(),
            Some(Ok(Step::DeferUndispatched))
        ));
        scheduler.settle_invocation(completed).unwrap();
        assert!(matches!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            crate::task::TaskState::Ready
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            owner
                .store()
                .list(format!("durable/unsafe-park-async/{}/", child.id()).as_bytes())
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn intent_commit_conflict_defers_safe_and_unsafe_child() {
        use crate::task_scheduler::{INTENT_COMMIT_HOOK, Step, TickReservation};
        use crate::task_store::{Save, TaskStore};
        for (safe, abort, conflict_limit, existing) in [
            (true, false, 0, false),
            (false, false, 0, false),
            (true, true, 0, false),
            (false, true, 0, false),
            (true, false, 2, false),
            (false, false, 2, false),
            (true, false, 9, false),
            (false, false, 9, false),
            (true, false, 0, true),
            (false, false, 0, true),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            assert!(
                registry.register("unsafe-parent-test", false, |_: &Task| Ok(Step::Succeed(
                    json!(null)
                )))
            );
            let mut host = StandaloneNoHooksHost::open(
                dir.path().join("db"),
                registry,
                UnhookedAuthorization::standalone_only(),
            )
            .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
                calls: calls.clone(),
                barrier: None,
                fail: false,
            });
            let child_tool = if safe {
                host.register_replay_safe_durable_tool(
                    tool,
                    "intent-race",
                    1,
                    vec![],
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap()
            } else {
                host.register_unsafe_durable_tool(
                    tool,
                    "intent-race",
                    1,
                    vec![],
                    UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                )
                .unwrap()
            };
            let mut scheduler = host.into_scheduler_for_test();
            let owner = scheduler.owner();
            let parent = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("unsafe-parent-test".into(), false)
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let tasks = TaskStore::new(owner.store());
            let snapshot = tasks.load(parent.id()).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    task.start_attempt(Uuid::new_v4())?;
                    task.wait(crate::task::WaitFor::Signal("hold".into()))
                })
                .unwrap();
            let waiting = tasks.load(parent.id()).unwrap().unwrap().task;
            let (_, child) = child_tool.child_candidate(&waiting, json!({})).unwrap();
            tasks.save(&[Save::Create(&child)]).unwrap();
            let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
                panic!("child not reserved")
            };
            let parent_id = parent.id();
            let attempt = scheduler
                .get(child.id())
                .unwrap()
                .unwrap()
                .attempt_id()
                .unwrap();
            let namespace = if safe {
                "durable/replay-safe-async"
            } else {
                "durable/unsafe-park-async"
            };
            let intent_key = format!("{namespace}/{}/{attempt}/intent", child.id()).into_bytes();
            let conflicts = Arc::new(AtomicUsize::new(0));
            let hook_conflicts = conflicts.clone();
            *INTENT_COMMIT_HOOK.lock().unwrap() = Some((
                child.id(),
                Arc::new(move |store, write_intent| {
                    if existing {
                        if write_intent {
                            store
                                .commit(
                                    &[],
                                    &[durable_store::Mutation::Put {
                                        key: intent_key.clone(),
                                        value: b"prior intent".to_vec(),
                                    }],
                                )
                                .unwrap();
                        }
                        return;
                    }
                    if conflict_limit > 0
                        && hook_conflicts.fetch_add(1, Ordering::SeqCst) >= conflict_limit
                    {
                        return;
                    }
                    let tasks = TaskStore::new(store);
                    let snapshot = tasks.load(parent_id).unwrap().unwrap();
                    tasks
                        .update(&snapshot, |task| {
                            if conflict_limit > 0 {
                                Ok(())
                            } else if abort {
                                task.request_abort()
                            } else {
                                task.wake()?;
                                task.start_attempt(Uuid::new_v4())
                            }
                        })
                        .unwrap();
                }),
            ));
            let completed = job.execute().unwrap();
            *INTENT_COMMIT_HOOK.lock().unwrap() = None;
            if abort || existing {
                assert!(
                    matches!(completed.handler_result(), Some(Err(_))),
                    "abort or existing intent must not defer"
                );
                scheduler.settle_invocation(completed).unwrap();
            } else {
                assert!(
                    matches!(
                        completed.handler_result(),
                        Some(Ok(Step::DeferUndispatched))
                    ),
                    "safe={safe}"
                );
                scheduler.settle_invocation(completed).unwrap();
                assert!(matches!(
                    scheduler.get(child.id()).unwrap().unwrap().state(),
                    crate::task::TaskState::Ready
                ));
            }
            if conflict_limit > 0 {
                assert!(conflicts.load(Ordering::SeqCst) >= conflict_limit);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let namespace = if safe {
                "durable/replay-safe-async"
            } else {
                "durable/unsafe-park-async"
            };
            let evidence = owner
                .store()
                .list(format!("{namespace}/{}/", child.id()).as_bytes())
                .unwrap();
            assert_eq!(evidence.is_empty(), !existing);
        }
    }
    #[async_trait::async_trait]
    impl Tool for PureParkTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "pure-park".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for PureParkTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            resume: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(barrier) = &self.barrier {
                let (lock, wake) = &**barrier;
                let mut state = lock.lock().unwrap();
                state.0 = true;
                wake.notify_all();
                while !state.1 {
                    state = wake.wait(state).unwrap();
                }
            }
            if self.fail {
                return Err("test tool failure".into());
            }
            match resume.checkpoint {
                None => Ok(DurableToolStep::Park {
                    checkpoint: json!({"saved":42}),
                    wait: DurableWait::Signal("go".into()),
                }),
                Some(value) if value == json!({"saved":42}) && resume.ended.is_empty() => Ok(
                    DurableToolStep::Complete(Outcome::Succeeded("resumed".into())),
                ),
                _ => Err("invalid resumed state".into()),
            }
        }
    }
    fn pure_registry(tool: Arc<dyn DurableAsyncTool>) -> Registry {
        let mut registry = Registry::default();
        assert!(registry.register(
            "test-durable",
            true,
            TestDurableHandler { tool, child: None }
        ));
        assert!(registry.set_abort("test-durable", |_: &Task| Ok(
            crate::task_scheduler::Step::Succeed(json!(null))
        )));
        registry
    }

    struct ChainTool {
        name: &'static str,
        child: &'static str,
        calls: Arc<AtomicUsize>,
        any: bool,
    }
    #[async_trait::async_trait]
    impl Tool for ChainTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.name.into(),
                description: if self.name == "root"
                    && std::env::var_os("PI_TEST_PARK_CANCEL_SPEC_BARRIER").is_some_and(|dir| {
                        std::path::PathBuf::from(dir).join("spec-changed").exists()
                    }) {
                    "changed".into()
                } else {
                    String::new()
                },
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for ChainTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match resume.checkpoint {
                None => {
                    let call = host.start_child("child", self.child, json!({})).await?;
                    let wait = if self.any {
                        DurableWait::Any(vec![call.clone()])
                    } else {
                        DurableWait::All(vec![call.clone()])
                    };
                    Ok(DurableToolStep::Park {
                        checkpoint: json!({"call":call}),
                        wait,
                    })
                }
                Some(data) => {
                    if self.name == "root"
                        && let Some(dir) = std::env::var_os("PI_TEST_PARK_CANCEL_WAKE")
                    {
                        std::fs::write(std::path::PathBuf::from(dir).join("wake-running"), b"yes")
                            .map_err(|e| e.to_string())?;
                        context.cancellation.cancelled().await;
                        return Err("woken execution cancelled".into());
                    }
                    let call: AsyncTaskCall =
                        serde_json::from_value(data["call"].clone()).map_err(|e| e.to_string())?;
                    if resume.ended != vec![call.task_id()] {
                        return Err("wrong child wake".into());
                    }
                    match host.child_result(&call).await? {
                        Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value)))
                            if value == self.child => {}
                        other => return Err(format!("wrong child result: {other:?}")),
                    }
                    Ok(DurableToolStep::Complete(Outcome::Succeeded(
                        self.name.into(),
                    )))
                }
            }
        }
    }
    struct ChainLeaf;

    struct ParkEvidenceTool;
    #[async_trait::async_trait]
    impl Tool for ParkEvidenceTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "park-evidence".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for ParkEvidenceTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            let call = host
                .start_child("child", "proposal-child", json!({}))
                .await?;
            Ok(DurableToolStep::Park {
                checkpoint: json!({"remote_job":"external-park-42", "call":call}),
                wait: DurableWait::All(vec![call]),
            })
        }
    }
    #[test]
    fn unsafe_park_proposal_survives_abort_before_settlement_without_authorizing_resume() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let child_handler: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register_shared("proposal-child", true, child_handler.clone()));
        let child = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "proposal-child".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "proposal-child",
            true,
            1,
            child_handler,
        ));
        let mut host =
            StandaloneNoHooksHost::open(&path, registry, UnhookedAuthorization::standalone_only())
                .unwrap();
        let tool = host
            .register_unsafe_durable_tool(
                Arc::new(ParkEvidenceTool),
                "park-evidence-v1",
                1,
                vec![child],
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
            .unwrap();
        let id = host.start_unsafe_durable_root(&tool, json!({}), 3).unwrap();
        let (entered, receiver) = std::sync::mpsc::channel();
        let barrier = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let worker_barrier = barrier.clone();
        replay_safe::after_unsafe_evidence(id, move || {
            entered.send(()).unwrap();
            let (lock, wake) = &*worker_barrier;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        });
        let driver = host.into_driver();
        receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        driver.request_abort(id).unwrap();
        let (lock, wake) = &*barrier;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        wait_state(&driver, id, |s| {
            matches!(s, crate::task::TaskState::Cancelled)
        });
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let evidence = read_unsafe_durable_park_evidence(&store, id).unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].checkpoint["remote_job"], "external-park-42");
        assert!(matches!(&evidence[0].wait, DurableWait::All(children) if children.len() == 1));
        assert!(
            store
                .get(format!("durable/unsafe-park-async/{id}/park").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            !crate::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .iter()
                .any(|task| task.task.owner() == &TaskOwner::Task(id))
        );
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
    }
    #[test]
    fn unsafe_hook_park_proposal_survives_abort_before_settlement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let child_handler: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register_shared("proposal-child", true, child_handler.clone()));
        let child = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "proposal-child".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "proposal-child",
            true,
            1,
            child_handler,
        ));
        let mut host = StandaloneBeforeHookHost::open(&path, registry).unwrap();
        let tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(ParkEvidenceTool),
                "park-evidence-v1",
                1,
                vec![child],
                "hook-v1",
                Arc::new(HookRace {
                    phase: "unused",
                    fired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    change_parent: Arc::new(std::sync::OnceLock::new()),
                }),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_unsafe_durable_root(&tool, json!({}), 3).unwrap();
        let (entered, receiver) = std::sync::mpsc::channel();
        let barrier = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let worker_barrier = barrier.clone();
        replay_safe::after_unsafe_evidence(id, move || {
            entered.send(()).unwrap();
            let (lock, wake) = &*worker_barrier;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        });
        let driver = host.into_driver();
        receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        driver.request_abort(id).unwrap();
        let (lock, wake) = &*barrier;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        wait_state(&driver, id, |s| {
            matches!(s, crate::task::TaskState::Cancelled)
        });
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let evidence = read_unsafe_durable_park_evidence(&store, id).unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].checkpoint["remote_job"], "external-park-42");
        assert!(matches!(&evidence[0].wait, DurableWait::All(children) if children.len() == 1));
        assert!(
            store
                .get(format!("durable/unsafe-park-async/{id}/park").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            !crate::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .iter()
                .any(|task| task.task.owner() == &TaskOwner::Task(id))
        );
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
    }
    impl crate::task_scheduler::Handler for ChainLeaf {
        fn run(&self, task: &Task) -> Result<crate::task_scheduler::Step, String> {
            use crate::task_scheduler::Step;
            match task.checkpoint().phase.as_str() {
                "initial" => Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "leaf-wait".into(),
                        data: json!({}),
                    },
                    condition: crate::task::WaitFor::Signal("release".into()),
                    children: vec![],
                }),
                "leaf-wait" if task.delivered_signal().is_some() => Ok(Step::Succeed(
                    serde_json::to_value(Outcome::Succeeded("leaf".into())).unwrap(),
                )),
                _ => Err("invalid leaf wake".into()),
            }
        }
    }
    fn chain_registry(root_calls: Arc<AtomicUsize>, middle_calls: Arc<AtomicUsize>) -> Registry {
        use crate::task_scheduler::Handler;
        let leaf: Arc<dyn Handler> = Arc::new(ChainLeaf);
        let spec = |name: &str| ToolSpec {
            name: name.into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let leaf_tool = Arc::new(crate::task_tool::TaskTool::new(
            spec("leaf"),
            "chain-leaf",
            true,
            1,
            leaf.clone(),
        ));
        let middle: Arc<dyn Handler> = Arc::new(TestDurableHandler {
            tool: Arc::new(ChainTool {
                name: "middle",
                child: "leaf",
                calls: middle_calls,
                any: false,
            }),
            child: Some(leaf_tool),
        });
        let middle_tool = Arc::new(crate::task_tool::TaskTool::new(
            spec("middle"),
            "chain-middle",
            true,
            1,
            middle.clone(),
        ));
        let root: Arc<dyn Handler> = Arc::new(TestDurableHandler {
            tool: Arc::new(ChainTool {
                name: "root",
                child: "middle",
                calls: root_calls,
                any: false,
            }),
            child: Some(middle_tool),
        });
        let mut registry = Registry::default();
        for (kind, handler) in [
            ("test-durable", root),
            ("chain-middle", middle),
            ("chain-leaf", leaf),
        ] {
            assert!(registry.register_shared(kind, true, handler));
            assert!(
                registry.set_abort(kind, |_: &Task| Ok(crate::task_scheduler::Step::Succeed(
                    json!(null)
                )))
            );
        }
        registry
    }
    fn chain_parked(driver: &crate::task_driver::Driver, root: Uuid) -> (Uuid, Uuid) {
        let parent = wait_state(driver, root, |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(crate::task::WaitFor::Tasks(_))
            )
        });
        let middle: AsyncTaskCall =
            serde_json::from_value(parent.checkpoint().data["call"].clone()).unwrap();
        let middle_task = wait_state(driver, middle.task_id(), |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(crate::task::WaitFor::Tasks(_))
            )
        });
        let leaf: AsyncTaskCall =
            serde_json::from_value(middle_task.checkpoint().data["call"].clone()).unwrap();
        wait_state(
            driver,
            leaf.task_id(),
            |s| matches!(s, crate::task::TaskState::Waiting(crate::task::WaitFor::Signal(name)) if name == "release"),
        );
        (middle.task_id(), leaf.task_id())
    }

    struct ProductionOuter(Arc<std::sync::OnceLock<Arc<crate::task_tool::TaskTool>>>);
    impl crate::task_scheduler::Handler for ProductionOuter {
        fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
            Err("documents required".into())
        }
        fn run_with_documents(
            &self,
            task: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            use crate::task_scheduler::Step;
            use crate::task_tool::{NestedTaskCall, NestedTaskResult};
            match task.checkpoint().phase.as_str() {
                "initial" => {
                    let call = self.0.get().ok_or("missing root tool")?.start_child(
                        task,
                        docs,
                        "root",
                        json!({}),
                    )?;
                    Ok(Step::Wait {
                        condition: crate::task::WaitFor::Tasks(vec![call.task_id()]),
                        checkpoint: Checkpoint {
                            phase: "root".into(),
                            data: json!({"call":call}),
                        },
                        children: vec![],
                    })
                }
                "root" => {
                    let call: NestedTaskCall =
                        serde_json::from_value(task.checkpoint().data["call"].clone())
                            .map_err(|e| e.to_string())?;
                    match call.result(task, docs)? {
                        Some(NestedTaskResult::Succeeded(value)) => Ok(Step::Succeed(value)),
                        other => Err(format!("root failed: {other:?}")),
                    }
                }
                _ => Err("invalid outer checkpoint".into()),
            }
        }
    }
    fn production_chain_scheduler(
        path: &std::path::Path,
        any: bool,
        root_calls: Arc<AtomicUsize>,
        middle_calls: Arc<AtomicUsize>,
    ) -> crate::task_scheduler::Scheduler {
        use crate::task_scheduler::Handler;
        let root_slot = Arc::new(std::sync::OnceLock::new());
        let outer: Arc<dyn Handler> = Arc::new(ProductionOuter(root_slot.clone()));
        let leaf: Arc<dyn Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        for (kind, handler) in [
            ("production-outer", outer),
            ("production-leaf", leaf.clone()),
        ] {
            assert!(registry.register_shared(kind, true, handler));
            assert!(
                registry.set_abort(kind, |_: &Task| Ok(crate::task_scheduler::Step::Succeed(
                    json!(null)
                )))
            );
        }
        let mut host =
            StandaloneNoHooksHost::open(path, registry, UnhookedAuthorization::standalone_only())
                .unwrap();
        let leaf_tool = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "production-leaf",
            true,
            1,
            leaf,
        ));
        let middle: Arc<dyn DurableAsyncTool> = Arc::new(ChainTool {
            name: "middle",
            child: "leaf",
            calls: middle_calls,
            any,
        });
        let middle_tool = host
            .register_replay_safe_durable_tool(
                middle,
                "pure-middle-v1",
                1,
                vec![leaf_tool],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let root: Arc<dyn DurableAsyncTool> = Arc::new(ChainTool {
            name: "root",
            child: "middle",
            calls: root_calls,
            any,
        });
        let root_tool = host
            .register_replay_safe_durable_tool(
                root,
                "pure-root-v1",
                1,
                vec![middle_tool],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        assert!(root_slot.set(root_tool).is_ok());
        host.into_scheduler_for_test()
    }
    fn production_outer_task() -> Task {
        Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("production-outer".into(), true)
        .into_task_tool()
    }
    fn production_chain_parked(
        driver: &crate::task_driver::Driver,
        outer: Uuid,
    ) -> (Uuid, Uuid, Uuid) {
        let task = wait_state(driver, outer, |s| {
            matches!(s, crate::task::TaskState::Waiting(_))
        });
        let root: crate::task_tool::NestedTaskCall =
            serde_json::from_value(task.checkpoint().data["call"].clone()).unwrap();
        let root_task = wait_state(driver, root.task_id(), |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(
                    crate::task::WaitFor::Tasks(_) | crate::task::WaitFor::TasksAny(_)
                )
            )
        });
        let middle: AsyncTaskCall =
            serde_json::from_value(root_task.checkpoint().data["checkpoint"]["call"].clone())
                .unwrap();
        let mid_task = wait_state(driver, middle.task_id(), |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(
                    crate::task::WaitFor::Tasks(_) | crate::task::WaitFor::TasksAny(_)
                )
            )
        });
        let leaf: AsyncTaskCall =
            serde_json::from_value(mid_task.checkpoint().data["checkpoint"]["call"].clone())
                .unwrap();
        wait_state(driver, leaf.task_id(), |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(crate::task::WaitFor::Signal(_))
            )
        });
        (root.task_id(), middle.task_id(), leaf.task_id())
    }
    struct HookRace {
        phase: &'static str,
        fired: Arc<std::sync::atomic::AtomicBool>,
        change_parent: Arc<std::sync::OnceLock<Arc<dyn Fn() + Send + Sync>>>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for HookRace {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if self.phase == "before" {
                self.race(ctx).await;
            }
            Ok(crate::extension::BeforeTool::Pass)
        }
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            if self.phase == "after" {
                self.race(ctx).await;
            }
            Ok(None)
        }
    }
    impl HookRace {
        async fn race(&self, ctx: &crate::extension::Context<'_>) {
            if !self.fired.swap(true, Ordering::SeqCst) {
                ctx.memo
                    .get_or_insert("race", json!("saved"))
                    .await
                    .unwrap();
                self.change_parent.get().unwrap()();
                // Hooks may ignore a memo error; the executor must still defer.
                let _ = ctx.memo.get("race").await;
            } else {
                assert_eq!(ctx.memo.get("race").await.unwrap(), Some(json!("saved")));
            }
        }
    }
    struct HookRaceTool(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for HookRaceTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "hook-race".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for HookRaceTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
        }
    }
    #[test]
    fn hooked_child_defers_memo_races_with_running_unsafe_parent() {
        use crate::task_scheduler::{Step, TickReservation};
        use crate::task_store::{Save, TaskStore};
        for (phase, safe) in [
            ("before", true),
            ("after", true),
            ("before", false),
            ("after", false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            assert!(
                registry.register("unsafe-parent-test", false, |_: &Task| Ok(Step::Succeed(
                    json!(null)
                )))
            );
            let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
            let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let change_parent: Arc<std::sync::OnceLock<Arc<dyn Fn() + Send + Sync>>> =
                Arc::new(std::sync::OnceLock::new());
            let calls = Arc::new(AtomicUsize::new(0));
            let tool: Arc<dyn DurableAsyncTool> = Arc::new(HookRaceTool(calls.clone()));
            let hook: Arc<dyn crate::extension::Hook> = Arc::new(HookRace {
                phase,
                fired: fired.clone(),
                change_parent: change_parent.clone(),
            });
            let child_tool = if safe {
                host.register_replay_safe_durable_tool_with_hook(
                    tool,
                    "hook-race-v1",
                    1,
                    vec![],
                    "hook-v1",
                    hook,
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap()
            } else {
                host.register_unsafe_durable_tool_with_hook(
                    tool,
                    "hook-race-v1",
                    1,
                    vec![],
                    "hook-v1",
                    hook,
                    UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap()
            };
            let mut scheduler = host.into_scheduler_for_test();
            let owner = scheduler.owner();
            let parent = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                10,
            )
            .unwrap()
            .registered("unsafe-parent-test".into(), false)
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let tasks = TaskStore::new(owner.store());
            let snapshot = tasks.load(parent.id()).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    task.start_attempt(Uuid::new_v4())?;
                    task.wait(crate::task::WaitFor::Signal("hold".into()))
                })
                .unwrap();
            let waiting = tasks.load(parent.id()).unwrap().unwrap().task;
            let (_, child) = child_tool.child_candidate(&waiting, json!({})).unwrap();
            tasks.save(&[Save::Create(&child)]).unwrap();
            let parent_id = parent.id();
            let race_owner = owner.clone();
            assert!(
                change_parent
                    .set(Arc::new(move || {
                        let tasks = TaskStore::new(race_owner.store());
                        let snapshot = tasks.load(parent_id).unwrap().unwrap();
                        tasks
                            .update(&snapshot, |task| {
                                task.wake()?;
                                task.start_attempt(Uuid::new_v4())
                            })
                            .unwrap();
                    }))
                    .is_ok()
            );
            let mut deferred = false;
            for _ in 0..8 {
                let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
                    panic!("child not reserved in {phase}")
                };
                assert_eq!(job.task_id(), child.id());
                let completed = job.execute().unwrap();
                let is_defer = matches!(
                    completed.handler_result(),
                    Some(Ok(Step::DeferUndispatched))
                );
                scheduler.settle_invocation(completed).unwrap();
                if is_defer {
                    deferred = true;
                    let snapshot = tasks.load(parent.id()).unwrap().unwrap();
                    tasks
                        .update(&snapshot, |task| {
                            task.wait(crate::task::WaitFor::Signal("hold".into()))
                        })
                        .unwrap();
                }
                if matches!(
                    scheduler.get(child.id()).unwrap().unwrap().state(),
                    crate::task::TaskState::Succeeded(_)
                ) {
                    break;
                }
            }
            assert!(fired.load(Ordering::SeqCst) && deferred, "{phase}");
            assert!(
                matches!(
                    scheduler.get(child.id()).unwrap().unwrap().state(),
                    crate::task::TaskState::Succeeded(_)
                ),
                "{phase}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{phase}");
        }
    }
    struct RichAfterMemoRace {
        fired: Arc<std::sync::atomic::AtomicBool>,
        ignored_error: Arc<std::sync::atomic::AtomicBool>,
        change_parent: Arc<std::sync::OnceLock<Arc<dyn Fn() + Send + Sync>>>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for RichAfterMemoRace {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if !self.fired.swap(true, Ordering::SeqCst) {
                ctx.memo.get_or_insert("rich-race", json!("saved")).await?;
                self.change_parent.get().unwrap()();
                let error = ctx.memo.get("rich-race").await;
                self.ignored_error.store(error.is_err(), Ordering::SeqCst);
            } else if ctx.memo.get("rich-race").await? != Some(json!("saved")) {
                return Err("memo was not preserved".into());
            }
            let mut result = input.clone();
            result.output = vec![json!({"type":"text", "text":"rich-after-resumed"})];
            Ok(Some(result))
        }
    }
    #[test]
    fn rich_after_child_defers_ignored_memo_error_with_running_unsafe_parent() {
        use crate::task_scheduler::{Step, TickReservation};
        use crate::task_store::{Save, TaskStore};
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        assert!(
            registry.register("unsafe-parent-test", false, |_: &Task| Ok(Step::Succeed(
                json!(null)
            )))
        );
        let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ignored_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let change_parent: Arc<std::sync::OnceLock<Arc<dyn Fn() + Send + Sync>>> =
            Arc::new(std::sync::OnceLock::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "rich-race".into(),
                    hooks: vec![Arc::new(RichAfterMemoRace {
                        fired: fired.clone(),
                        ignored_error: ignored_error.clone(),
                        change_parent: change_parent.clone(),
                    })],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "rich-race-v1".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let child_tool = host
            .register_replay_safe_durable_tool_with_content_hooks(
                Arc::new(HookRaceTool(calls.clone())),
                "hook-race-v1",
                1,
                vec![],
                &extensions.snapshot(),
                &["rich-race".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let mut scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            10,
        )
        .unwrap()
        .registered("unsafe-parent-test".into(), false)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let tasks = TaskStore::new(owner.store());
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| {
                task.start_attempt(Uuid::new_v4())?;
                task.wait(crate::task::WaitFor::Signal("hold".into()))
            })
            .unwrap();
        let waiting = tasks.load(parent.id()).unwrap().unwrap().task;
        let (_, child) = child_tool.child_candidate(&waiting, json!({})).unwrap();
        tasks.save(&[Save::Create(&child)]).unwrap();
        let parent_id = parent.id();
        let race_owner = owner.clone();
        assert!(
            change_parent
                .set(Arc::new(move || {
                    let tasks = TaskStore::new(race_owner.store());
                    let snapshot = tasks.load(parent_id).unwrap().unwrap();
                    tasks
                        .update(&snapshot, |task| {
                            task.wake()?;
                            task.start_attempt(Uuid::new_v4())
                        })
                        .unwrap();
                }))
                .is_ok()
        );
        let mut deferred = false;
        for _ in 0..8 {
            let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
                panic!("child not reserved")
            };
            assert_eq!(job.task_id(), child.id());
            let completed = job.execute().unwrap();
            let is_defer = matches!(
                completed.handler_result(),
                Some(Ok(Step::DeferUndispatched))
            );
            if is_defer {
                assert!(ignored_error.load(Ordering::SeqCst));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
            scheduler.settle_invocation(completed).unwrap();
            if is_defer {
                deferred = true;
                let parked_child = scheduler.get(child.id()).unwrap().unwrap();
                assert!(matches!(
                    parked_child.state(),
                    crate::task::TaskState::Ready
                ));
                assert_eq!(parked_child.checkpoint().phase, "replay-safe-async-after");
                let snapshot = tasks.load(parent.id()).unwrap().unwrap();
                tasks
                    .update(&snapshot, |task| {
                        task.wait(crate::task::WaitFor::Signal("hold".into()))
                    })
                    .unwrap();
            }
            if scheduler.get(child.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert!(fired.load(Ordering::SeqCst) && ignored_error.load(Ordering::SeqCst) && deferred);
        let task = scheduler.get(child.id()).unwrap().unwrap();
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected child state {:?}", task.state())
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("expected rich result")
        };
        assert_eq!(result.output[0]["text"], "rich-after-resumed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unsafe_hook_raw_receipt_survives_abort_before_after_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(HookRaceTool(calls.clone())),
                "unsafe-raw-before-after",
                1,
                vec![],
                "hook-v1",
                Arc::new(HookRace {
                    phase: "unused",
                    fired,
                    change_parent: Arc::new(std::sync::OnceLock::new()),
                }),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_unsafe_durable_root(&tool, json!({}), 5).unwrap();
        let (entered, receiver) = std::sync::mpsc::channel();
        let barrier = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let worker_barrier = barrier.clone();
        replay_safe::after_unsafe_evidence(id, move || {
            entered.send(()).unwrap();
            let (lock, wake) = &*worker_barrier;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        });
        let driver = host.into_driver();
        receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        driver.request_abort(id).unwrap();
        let (lock, wake) = &*barrier;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        wait_state(&driver, id, |s| {
            matches!(s, crate::task::TaskState::Cancelled)
        });
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let receipts = read_unsafe_durable_receipts(&store, id).unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].outcome, Outcome::Succeeded("done".into()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let prefix = format!("durable/unsafe-park-async/{id}/");
        let evidence = store.list(prefix.as_bytes()).unwrap();
        assert!(
            !evidence
                .iter()
                .any(|(key, _)| key.ends_with(b"/result") || key.ends_with(b"/hook-final"))
        );
    }
    #[test]
    fn unsafe_hook_kill_after_receipt_before_checkpoint_requires_reconciliation() {
        fn open(
            path: &std::path::Path,
            calls: Arc<AtomicUsize>,
        ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
            let mut host = StandaloneBeforeHookHost::open(path, Registry::default()).unwrap();
            let tool = host
                .register_unsafe_durable_tool_with_hook(
                    Arc::new(HookRaceTool(calls)),
                    "unsafe-raw-kill",
                    1,
                    vec![],
                    "hook-v1",
                    Arc::new(HookRace {
                        phase: "unused",
                        fired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                        change_parent: Arc::new(std::sync::OnceLock::new()),
                    }),
                    UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            (host, tool)
        }
        if let Some(dir) = std::env::var_os("PI_UNSAFE_HOOK_RAW_KILL") {
            let dir = std::path::PathBuf::from(dir);
            let (host, tool) = open(&dir.join("db"), Arc::new(AtomicUsize::new(0)));
            let id = host.start_unsafe_durable_root(&tool, json!({}), 5).unwrap();
            replay_safe::after_unsafe_evidence(id, move || {
                std::fs::write(dir.join("raw.tmp"), id.to_string()).unwrap();
                std::fs::rename(dir.join("raw.tmp"), dir.join("raw-ready")).unwrap();
                loop {
                    std::thread::park();
                }
            });
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "async_task_tool::tests::unsafe_hook_kill_after_receipt_before_checkpoint_requires_reconciliation"])
            .env("PI_UNSAFE_HOOK_RAW_KILL",dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !dir.path().join("raw-ready").exists() {
            if std::time::Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
                let _ = process.kill();
                let _ = process.wait();
                panic!("raw receipt boundary not reached");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let id: Uuid = std::fs::read_to_string(dir.path().join("raw-ready"))
            .unwrap()
            .parse()
            .unwrap();
        process.kill().unwrap();
        process.wait().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, _) = open(&dir.path().join("db"), calls.clone());
        let driver = host.into_driver();
        let failed = wait_state(&driver, id, |s| {
            matches!(s, crate::task::TaskState::Failed(_))
        });
        assert!(
            matches!(failed.state(), crate::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        driver.shutdown().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let receipts = read_unsafe_durable_receipts(&store, id).unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].outcome, Outcome::Succeeded("done".into()));
        let evidence = store
            .list(format!("durable/unsafe-park-async/{id}/").as_bytes())
            .unwrap();
        assert!(
            !evidence
                .iter()
                .any(|(key, _)| key.ends_with(b"/result") || key.ends_with(b"/hook-final"))
        );
    }
    #[test]
    fn unsafe_after_hook_settlement_conflict_retries_without_tool_dispatch() {
        use crate::task_scheduler::TickReservation;
        let dir = tempfile::tempdir().unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(HookRaceTool(calls.clone())),
                "unsafe-after-conflict",
                1,
                vec![],
                "hook-v1",
                Arc::new(HookRace {
                    phase: "unused",
                    fired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    change_parent: Arc::new(std::sync::OnceLock::new()),
                }),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_unsafe_durable_root(&tool, json!({}), 5).unwrap();
        let mut scheduler = host.into_scheduler_for_test();
        while scheduler.get(id).unwrap().unwrap().checkpoint().phase != "replay-safe-async-after" {
            assert!(scheduler.tick(0).unwrap());
        }
        let TickReservation::Invocation(job) = scheduler.reserve_tick(0).unwrap() else {
            panic!("after hook not reserved")
        };
        let completed = job.execute().unwrap();
        let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let once = changed.clone();
        scheduler.set_result_commit_hook(Arc::new(move |_, store| {
            if !once.swap(true, Ordering::SeqCst) {
                store
                    .commit(
                        &[],
                        &[durable_store::Mutation::Put {
                            key: format!("durable/unsafe-park-async/{id}/unrelated").into_bytes(),
                            value: b"race".to_vec(),
                        }],
                    )
                    .unwrap();
            }
        }));
        scheduler.settle_invocation(completed).unwrap();
        assert!(changed.load(Ordering::SeqCst));
        assert!(matches!(
            scheduler.get(id).unwrap().unwrap().state(),
            crate::task::TaskState::Ready
        ));
        assert!(scheduler.tick(0).unwrap());
        assert!(matches!(
            scheduler.get(id).unwrap().unwrap().state(),
            crate::task::TaskState::Succeeded(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn unsafe_hook_block_fails_closed_before_dispatch() {
        struct Block;
        #[async_trait::async_trait]
        impl crate::extension::Hook for Block {
            async fn before_tool(
                &self,
                _: &crate::extension::ToolCall,
                _: &crate::extension::Context<'_>,
            ) -> Result<crate::extension::BeforeTool, String> {
                Ok(crate::extension::BeforeTool::Block("denied".into()))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(HookRaceTool(calls.clone())),
                "unsafe-block",
                1,
                vec![],
                "hook-v1",
                Arc::new(Block),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_unsafe_durable_root(&tool, json!({}), 5).unwrap();
        let driver = host.into_driver();
        wait_state(&driver, id, |s| {
            matches!(s, crate::task::TaskState::Failed(_))
        });
        driver.shutdown().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
        // A blocked before hook records proof of non-dispatch. It must not
        // create an intent, park proposal, receipt, or final outcome.
        let records = store
            .list(format!("durable/unsafe-park-async/{id}/").as_bytes())
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].0,
            format!("durable/unsafe-park-async/{id}/no-dispatch").as_bytes()
        );
        let proof: serde_json::Value = serde_json::from_slice(&records[0].1.value).unwrap();
        assert_eq!(proof["task"], id.to_string());
        assert_eq!(proof["version"], 1);
        assert!(proof["before_attempt"].as_str().is_some());
        assert_eq!(proof["checkpoint"]["phase"], "replay-safe-async-after");
    }
    struct DurableHooks {
        before: Arc<AtomicUsize>,
        after: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for DurableHooks {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if ctx.memo.get_or_insert("phase", json!("prepared")).await? != json!("prepared") {
                return Err("memo changed".into());
            }
            self.before.fetch_add(1, Ordering::SeqCst);
            Ok(crate::extension::BeforeTool::Rewrite(json!({"bound":true})))
        }
        async fn after_tool(
            &self,
            call: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            if call.arguments != json!({"bound":true}) {
                return Err("rewritten arguments changed".into());
            }
            if ctx.memo.get("phase").await? != Some(json!("prepared")) {
                return Err("memo missing after wake".into());
            }
            self.after.fetch_add(1, Ordering::SeqCst);
            if call.name == "root"
                && let Some(root) = std::env::var_os("PI_RS_HOOKED_AFTER_CUT_1120")
            {
                std::fs::write(std::path::PathBuf::from(root).join("after-cut"), b"ready")
                    .map_err(|e| e.to_string())?;
                std::future::pending::<()>().await;
            }
            Ok(None)
        }
    }
    struct HookedChainTool(ChainTool);
    #[async_trait::async_trait]
    impl Tool for HookedChainTool {
        fn spec(&self) -> ToolSpec {
            self.0.spec()
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for HookedChainTool {
        async fn execute_durable(
            &self,
            args: serde_json::Value,
            context: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            if args != json!({"bound":true}) {
                return Err("rewritten arguments lost across wake".into());
            }
            self.0.execute_durable(args, context, resume, host).await
        }
    }
    fn hooked_chain_scheduler(
        path: &std::path::Path,
        any: bool,
        root_calls: Arc<AtomicUsize>,
        middle_calls: Arc<AtomicUsize>,
        before: Arc<AtomicUsize>,
        after: Arc<AtomicUsize>,
    ) -> crate::task_scheduler::Scheduler {
        use crate::task_scheduler::Handler;
        let root_slot = Arc::new(std::sync::OnceLock::new());
        let outer: Arc<dyn Handler> = Arc::new(ProductionOuter(root_slot.clone()));
        let leaf: Arc<dyn Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        for (kind, handler) in [
            ("production-outer", outer),
            ("production-leaf", leaf.clone()),
        ] {
            assert!(registry.register_shared(kind, true, handler));
            assert!(
                registry.set_abort(kind, |_: &Task| Ok(crate::task_scheduler::Step::Succeed(
                    json!(null)
                )))
            );
        }
        let mut host = StandaloneBeforeHookHost::open(path, registry).unwrap();
        let leaf_tool = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "production-leaf",
            true,
            1,
            leaf,
        ));
        let middle: Arc<dyn DurableAsyncTool> = Arc::new(HookedChainTool(ChainTool {
            name: "middle",
            child: "leaf",
            calls: middle_calls,
            any,
        }));
        let middle_tool = host
            .register_replay_safe_durable_tool_with_hook(
                middle,
                "pure-middle-v1",
                1,
                vec![leaf_tool],
                "middle-hook-v1",
                Arc::new(DurableHooks {
                    before: before.clone(),
                    after: after.clone(),
                }),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let root: Arc<dyn DurableAsyncTool> = Arc::new(HookedChainTool(ChainTool {
            name: "root",
            child: "middle",
            calls: root_calls,
            any,
        }));
        let root_tool = host
            .register_replay_safe_durable_tool_with_hook(
                root,
                "pure-root-v1",
                1,
                vec![middle_tool],
                "root-hook-v1",
                Arc::new(DurableHooks { before, after }),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        assert!(root_slot.set(root_tool).is_ok());
        host.into_scheduler_for_test()
    }
    #[test]
    fn hooked_durable_multilevel_park_wake_runs_hooks_once() {
        for any in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let before = Arc::new(AtomicUsize::new(0));
            let after = Arc::new(AtomicUsize::new(0));
            let root_calls = Arc::new(AtomicUsize::new(0));
            let middle_calls = Arc::new(AtomicUsize::new(0));
            let scheduler = hooked_chain_scheduler(
                &dir.path().join("db"),
                any,
                root_calls.clone(),
                middle_calls.clone(),
                before.clone(),
                after.clone(),
            );
            let parent = production_outer_task();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let (_, _, leaf) = production_chain_parked(&driver, parent.id());
            assert_eq!(before.load(Ordering::SeqCst), 2);
            assert_eq!(after.load(Ordering::SeqCst), 0);
            release_chain(&driver, parent.id(), leaf);
            assert_eq!(root_calls.load(Ordering::SeqCst), 2);
            assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
            assert_eq!(before.load(Ordering::SeqCst), 2);
            assert_eq!(after.load(Ordering::SeqCst), 2);
            driver.shutdown().unwrap();
        }
    }

    #[test]
    fn hooked_durable_multilevel_park_survives_process_kill() {
        const ENV: &str = "PI_RS_HOOKED_PARK_KILL_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let scheduler = hooked_chain_scheduler(
                &root.join("db"),
                false,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            );
            let parent = production_outer_task();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let (_, _, leaf) = production_chain_parked(&driver, parent.id());
            std::fs::write(root.join("parked"), format!("{} {}", parent.id(), leaf)).unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::hooked_durable_multilevel_park_survives_process_kill")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("parked").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("hooked durable chain did not park");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let saved = std::fs::read_to_string(dir.path().join("parked")).unwrap();
        let mut fields = saved.split_whitespace();
        let parent: Uuid = fields.next().unwrap().parse().unwrap();
        let leaf: Uuid = fields.next().unwrap().parse().unwrap();
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let root_calls = Arc::new(AtomicUsize::new(0));
        let middle_calls = Arc::new(AtomicUsize::new(0));
        let scheduler = hooked_chain_scheduler(
            &dir.path().join("db"),
            false,
            root_calls.clone(),
            middle_calls.clone(),
            before.clone(),
            after.clone(),
        );
        let driver = crate::task_driver::Driver::start(scheduler);
        let (_, _, recovered_leaf) = production_chain_parked(&driver, parent);
        assert_eq!(leaf, recovered_leaf);
        assert_eq!(before.load(Ordering::SeqCst), 0);
        assert_eq!(after.load(Ordering::SeqCst), 0);
        release_chain(&driver, parent, leaf);
        assert_eq!(before.load(Ordering::SeqCst), 0);
        assert_eq!(after.load(Ordering::SeqCst), 2);
        assert_eq!(root_calls.load(Ordering::SeqCst), 1);
        assert_eq!(middle_calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
    }

    #[test]
    fn hooked_durable_after_kill_reopens_without_tool_reexecution() {
        const ENV: &str = "PI_RS_HOOKED_AFTER_WORKER_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let scheduler = hooked_chain_scheduler(
                &root.join("db"),
                false,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            );
            let parent = production_outer_task();
            scheduler.submit(&parent).unwrap();
            std::fs::write(root.join("parent"), parent.id().to_string()).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let (_, _, leaf) = production_chain_parked(&driver, parent.id());
            driver
                .signal(&crate::task_store::Signal::new(
                    Uuid::new_v4(),
                    leaf,
                    "release".into(),
                    json!(null),
                ))
                .unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::hooked_durable_after_kill_reopens_without_tool_reexecution").env(ENV, dir.path()).env("PI_RS_HOOKED_AFTER_CUT_1120", dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("after-cut").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("after hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let parent: Uuid = std::fs::read_to_string(dir.path().join("parent"))
            .unwrap()
            .parse()
            .unwrap();
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let root_calls = Arc::new(AtomicUsize::new(0));
        let middle_calls = Arc::new(AtomicUsize::new(0));
        let scheduler = hooked_chain_scheduler(
            &dir.path().join("db"),
            false,
            root_calls.clone(),
            middle_calls.clone(),
            before.clone(),
            after.clone(),
        );
        let driver = crate::task_driver::Driver::start(scheduler);
        let task = wait_state(&driver, parent, |s| {
            matches!(s, crate::task::TaskState::Succeeded(_))
        });
        assert_eq!(
            task.state(),
            &crate::task::TaskState::Succeeded(
                serde_json::to_value(Outcome::Succeeded("root".into())).unwrap()
            )
        );
        assert_eq!(before.load(Ordering::SeqCst), 0);
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert_eq!(root_calls.load(Ordering::SeqCst), 0);
        assert_eq!(middle_calls.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
    }

    struct ImmediateDurable(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for ImmediateDurable {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "immediate".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for ImmediateDurable {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
        }
    }
    #[test]
    fn hooked_durable_after_abort_rejects_stale_memo() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = Arc::new(AbortAfterHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        });
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(calls.clone())),
                "immediate-v1",
                1,
                vec![],
                "hook-v1",
                hook,
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        let task = wait_terminal(&driver, id);
        assert!(matches!(task.state(), crate::task::TaskState::Cancelled));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct StrictDurable(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for StrictDurable {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "strict".into(),
                description: String::new(),
                parameters: json!({"type":"object", "properties":{"bad":false}}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for StrictDurable {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
        }
    }
    #[test]
    fn hooked_durable_rejects_invalid_and_oversize_before_rewrite() {
        for decision in [3, 4] {
            let dir = tempfile::tempdir().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut host =
                StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
            let hook = Arc::new(BeforeFixture {
                decision,
                entered: None,
                calls: Arc::new(AtomicUsize::new(0)),
            });
            let tool = host
                .register_replay_safe_durable_tool_with_hook(
                    Arc::new(StrictDurable(calls.clone())),
                    "strict-v1",
                    1,
                    vec![],
                    "hook-v1",
                    hook,
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            if decision == 3 {
                assert!(
                    matches!(task.state(), crate::task::TaskState::Succeeded(value) if matches!(serde_json::from_value::<Outcome>(value.clone()), Ok(Outcome::Failed(reason)) if reason.contains("Invalid tool arguments")))
                );
            } else {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct PoisonedMemoHook {
        before_phase: bool,
        observed: std::sync::mpsc::Sender<(bool, bool, bool)>,
    }
    impl PoisonedMemoHook {
        async fn check(&self, ctx: &crate::extension::Context<'_>) {
            assert_eq!(
                ctx.memo.get_or_insert("existing", json!(42)).await.unwrap(),
                json!(42)
            );
            let first = ctx.memo.get("").await.is_err();
            let read = ctx.memo.get("existing").await.is_err();
            let write = ctx
                .memo
                .get_or_insert("valid", json!("must-not-persist"))
                .await
                .is_err();
            self.observed.send((first, read, write)).unwrap();
        }
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for PoisonedMemoHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if self.before_phase {
                self.check(ctx).await;
            }
            Ok(crate::extension::BeforeTool::Pass)
        }
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            if !self.before_phase {
                self.check(ctx).await;
            }
            Ok(None)
        }
    }
    #[test]
    fn hooked_durable_memo_failure_rejects_later_access_in_both_phases() {
        for before_phase in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let calls = Arc::new(AtomicUsize::new(0));
            let (observed, received) = std::sync::mpsc::channel();
            let mut host = StandaloneBeforeHookHost::open(&path, Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_hook(
                    Arc::new(ImmediateDurable(calls.clone())),
                    "immediate-v1",
                    1,
                    vec![],
                    "hook-v1",
                    Arc::new(PoisonedMemoHook {
                        before_phase,
                        observed,
                    }),
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            assert_eq!(
                received
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap(),
                (true, true, true)
            );
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Failed(_)
            ));
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!before_phase));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            let key = format!("runtime/extension-memo/{id}/valid");
            assert!(store.get(key.as_bytes()).unwrap().is_none());
        }
    }

    struct ConcurrentPoisonedMemoHook {
        before_phase: bool,
        observed: std::sync::mpsc::Sender<(bool, bool)>,
    }
    impl ConcurrentPoisonedMemoHook {
        fn check(&self, ctx: &crate::extension::Context<'_>) {
            let barrier = std::sync::Barrier::new(2);
            let (failed_tx, failed_rx) = std::sync::mpsc::sync_channel(0);
            let (failed, write) = std::thread::scope(|scope| {
                let first = scope.spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    barrier.wait();
                    let failed = runtime.block_on(ctx.memo.get("")).is_err();
                    failed_tx.send(failed).unwrap();
                    failed
                });
                let barrier_ref = &barrier;
                let later = scope.spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    barrier_ref.wait();
                    // The write begins on another live thread only after the
                    // failing access returned and its failure was observed.
                    assert!(
                        failed_rx
                            .recv_timeout(std::time::Duration::from_secs(5))
                            .unwrap()
                    );
                    runtime
                        .block_on(
                            ctx.memo
                                .get_or_insert("concurrent-write", json!("forbidden")),
                        )
                        .is_err()
                });
                (first.join().unwrap(), later.join().unwrap())
            });
            self.observed.send((failed, write)).unwrap();
        }
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for ConcurrentPoisonedMemoHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if self.before_phase {
                self.check(ctx);
            }
            Ok(crate::extension::BeforeTool::Pass)
        }
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            if !self.before_phase {
                self.check(ctx);
            }
            Ok(None)
        }
    }
    #[test]
    fn hooked_durable_concurrent_write_after_observed_memo_failure_is_rejected() {
        for before_phase in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let calls = Arc::new(AtomicUsize::new(0));
            let (observed, received) = std::sync::mpsc::channel();
            let mut host = StandaloneBeforeHookHost::open(&path, Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_hook(
                    Arc::new(ImmediateDurable(calls.clone())),
                    "immediate-v1",
                    1,
                    vec![],
                    "concurrent-hook-v1",
                    Arc::new(ConcurrentPoisonedMemoHook {
                        before_phase,
                        observed,
                    }),
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            assert_eq!(
                received
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap(),
                (true, true)
            );
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Failed(_)
            ));
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!before_phase));
            driver.shutdown().unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            let key = format!("runtime/extension-memo/{id}/concurrent-write");
            assert!(store.get(key.as_bytes()).unwrap().is_none());
        }
    }

    #[test]
    fn hooked_durable_after_ignored_memo_error_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(calls.clone())),
                "immediate-v1",
                1,
                vec![],
                "hook-v1",
                Arc::new(BadAfterMemo),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    #[test]
    fn hooked_durable_before_abort_rejects_stale_memo_and_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = Arc::new(StaleMemoHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        });
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(calls.clone())),
                "immediate-v1",
                1,
                vec![],
                "hook-v1",
                hook,
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct HostResultHook {
        mode: u8,
        before_calls: Arc<AtomicUsize>,
        observed: std::sync::mpsc::Sender<(String, bool)>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for HostResultHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            self.before_calls.fetch_add(1, Ordering::SeqCst);
            Ok(match self.mode {
                0 => crate::extension::BeforeTool::Block("denied".into()),
                1 => crate::extension::BeforeTool::Rewrite(json!({"bad":true})),
                3 => crate::extension::BeforeTool::Block("漢".repeat(2048)),
                _ => crate::extension::BeforeTool::Pass,
            })
        }
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            result: &crate::extension::ToolResult,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            if self.mode == 3 {
                let diagnostic = result.diagnostics.first().ok_or("missing diagnostic")?;
                assert!(diagnostic.message.starts_with("Tool call blocked: 漢"));
                assert!(diagnostic.message.len() <= 4096);
            }
            let code = result
                .diagnostics
                .first()
                .and_then(|d| d.code.clone())
                .ok_or("missing diagnostic")?;
            self.observed.send((code, result.is_error)).unwrap();
            ctx.memo.get_or_insert("host-after", json!(42)).await?;
            let mut result = result.clone();
            result.output = "after observed host error".into();
            Ok(Some(result))
        }
    }
    #[test]
    fn hooked_durable_blocked_and_invalid_run_after_without_tool_dispatch() {
        for mode in [0, 1, 2, 3] {
            let dir = tempfile::tempdir().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let before_calls = Arc::new(AtomicUsize::new(0));
            let (observed, received) = std::sync::mpsc::channel();
            let hook = Arc::new(HostResultHook {
                mode,
                before_calls: before_calls.clone(),
                observed,
            });
            let mut host =
                StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_hook(
                    Arc::new(StrictDurable(calls.clone())),
                    "strict-v1",
                    1,
                    vec![],
                    "host-result-v1",
                    hook,
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let input = if mode == 2 {
                json!({"bad":true})
            } else {
                json!({})
            };
            let id = host.start_replay_safe_root(&tool, input, 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            assert!(
                matches!(task.state(), crate::task::TaskState::Succeeded(value) if matches!(serde_json::from_value::<Outcome>(value.clone()), Ok(Outcome::Failed(reason)) if reason == "after observed host error"))
            );
            assert_eq!(
                received
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap(),
                (
                    if mode == 0 || mode == 3 {
                        "blocked".into()
                    } else {
                        "invalid_arguments".into()
                    },
                    true
                )
            );
            assert_eq!(
                before_calls.load(Ordering::SeqCst),
                if mode == 2 { 0 } else { 1 }
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct PrepareFailureTool(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for PrepareFailureTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "prepare-failure".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn prepare_arguments(
            &self,
            _: serde_json::Value,
        ) -> pi_rs_core::ExtensionResult<serde_json::Value> {
            Err("repair failed".into())
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for PrepareFailureTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "unexpected".into(),
            )))
        }
    }
    #[test]
    fn host_invalid_preparation_runs_after_without_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let before_calls = Arc::new(AtomicUsize::new(0));
        let (observed, received) = std::sync::mpsc::channel();
        let hook = Arc::new(HostResultHook {
            mode: 2,
            before_calls: before_calls.clone(),
            observed,
        });
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(PrepareFailureTool(calls.clone())),
                "prepare-failure-v1",
                1,
                vec![],
                "host-result-v1",
                hook,
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver, id).state(), crate::task::TaskState::Succeeded(value) if matches!(serde_json::from_value::<Outcome>(value.clone()), Ok(Outcome::Failed(_))))
        );
        assert_eq!(
            received
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            ("invalid_arguments".into(), true)
        );
        assert_eq!(before_calls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct BlockedAbortHook(AbortAfterHook);
    #[async_trait::async_trait]
    impl crate::extension::Hook for BlockedAbortHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            Ok(crate::extension::BeforeTool::Block("denied".into()))
        }
        async fn after_tool(
            &self,
            call: &crate::extension::ToolCall,
            value: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            crate::extension::Hook::after_tool(&self.0, call, value, ctx).await
        }
    }
    #[test]
    fn host_blocked_after_abort_fences_memo_and_never_dispatches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = Arc::new(BlockedAbortHook(AbortAfterHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        }));
        let mut host = StandaloneBeforeHookHost::open(&path, Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(calls.clone())),
                "immediate-v1",
                1,
                vec![],
                "blocked-abort-v1",
                hook,
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            entered_rx.try_recv().is_err(),
            "after hook must not restart in abort cleanup"
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/late-after").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    struct RecoverHostHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for RecoverHostHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            Ok(crate::extension::BeforeTool::Block("denied".into()))
        }
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            result: &crate::extension::ToolResult,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            if !result.is_error
                || result.diagnostics.first().and_then(|d| d.code.as_deref()) != Some("blocked")
            {
                return Err("missing blocked result".into());
            }
            let exists = self.0.join("hook-ready").exists();
            let memo = ctx
                .memo
                .get_or_insert(
                    "host-recovery",
                    json!(if exists { "second" } else { "first" }),
                )
                .await?;
            if memo != json!("first") {
                return Err("memo changed after restart".into());
            }
            if !exists {
                std::fs::write(self.0.join("hook-ready"), b"ready").map_err(|e| e.to_string())?;
                std::future::pending().await
            } else {
                Ok(None)
            }
        }
    }
    struct FileDurable(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for FileDurable {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "file-durable".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for FileDurable {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            std::fs::write(self.0.join("tool-dispatched"), b"incorrect")
                .map_err(|e| e.to_string())?;
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "unexpected".into(),
            )))
        }
    }
    fn recovery_host_result(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(FileDurable(root.to_owned())),
                "file-v1",
                1,
                vec![],
                "recover-host-v1",
                Arc::new(RecoverHostHook(root.to_owned())),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, tool)
    }
    #[test]
    fn host_blocked_after_kill_reopens_without_dispatch() {
        const ENV: &str = "PI_RS_HOST_BLOCK_AFTER_KILL_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = recovery_host_result(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::host_blocked_after_kill_reopens_without_dispatch")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("hook-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("host result hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = recovery_host_result(dir.path());
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        assert!(
            matches!(task.state(), crate::task::TaskState::Succeeded(value) if matches!(serde_json::from_value::<Outcome>(value.clone()), Ok(Outcome::Failed(reason)) if reason.contains("blocked")))
        );
        assert!(!dir.path().join("tool-dispatched").exists());
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct HostMetadataRewrite;
    #[async_trait::async_trait]
    impl crate::extension::Hook for HostMetadataRewrite {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            Ok(crate::extension::BeforeTool::Block("denied".into()))
        }
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            result: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            let mut changed = result.clone();
            changed.is_error = false;
            Ok(Some(changed))
        }
    }
    #[test]
    fn host_blocked_after_cannot_suppress_error_status() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(calls.clone())),
                "immediate-v1",
                1,
                vec![],
                "block-policy-v1",
                Arc::new(HostMetadataRewrite),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct OrderedContentHook {
        stage: u8,
        block: bool,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for OrderedContentHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if self.block && self.stage == 1 {
                Ok(crate::extension::BeforeTool::Block("policy".into()))
            } else {
                Ok(crate::extension::BeforeTool::Pass)
            }
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let mut out = input.clone();
            if self.stage == 1 {
                if self.block
                    && (!out
                        .diagnostics
                        .iter()
                        .any(|d| d.code.as_deref() == Some("blocked"))
                        || out.extra.get("isError") != Some(&json!(true)))
                {
                    return Err("missing gate diagnosis".into());
                }
                ctx.memo
                    .get_or_insert("content-hook", json!("first"))
                    .await?;
                out.output = vec![
                    json!({"type":"text","text":"first"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                ];
                out.extra.insert("details".into(), json!({"private":42}));
                out.extra
                    .insert("structuredOutput".into(), json!({"stage":1}));
                out.diagnostics.push(crate::runtime::ToolDiagnostic {
                    severity: crate::runtime::DiagnosticSeverity::Info,
                    message: "stage one".into(),
                    code: Some("stage_one".into()),
                });
            } else {
                if out.output.len() != 2
                    || out.extra.get("details") != Some(&json!({"private":42}))
                    || out.extra.get("structuredOutput") != Some(&json!({"stage":1}))
                    || !out
                        .diagnostics
                        .iter()
                        .any(|d| d.code.as_deref() == Some("stage_one"))
                    || ctx.memo.get("content-hook").await? != Some(json!("first"))
                {
                    return Err("prior hook result not preserved".into());
                }
                out.output.push(json!({"type":"text","text":"second"}));
                out.extra
                    .insert("structuredOutput".into(), json!({"stage":2}));
            }
            Ok(Some(out))
        }
    }
    fn content_host(
        path: &std::path::Path,
        tool: Arc<dyn DurableAsyncTool>,
        block: bool,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut extensions = crate::extension::Registry::default();
        for (name, stage) in [("first", 1), ("second", 2)] {
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![Arc::new(OrderedContentHook { stage, block })],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{name}-v1"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut host = StandaloneBeforeHookHost::open(path, Registry::default()).unwrap();
        let adapter = host
            .register_replay_safe_durable_tool_with_content_hooks(
                tool,
                "immediate-v1",
                1,
                vec![],
                &extensions.snapshot(),
                &["first".into(), "second".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, adapter)
    }
    #[test]
    fn durable_content_hooks_chain_and_persist_all_channels_after_reopen() {
        for block in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let calls = Arc::new(AtomicUsize::new(0));
            let (host, tool) =
                content_host(&path, Arc::new(ImmediateDurable(calls.clone())), block);
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            let crate::task::TaskState::Succeeded(value) = task.state() else {
                panic!("unexpected task state {:?}", task.state())
            };
            let result: DurableTaskResult = serde_json::from_value(value.clone()).unwrap();
            let DurableTaskResult::Rich {
                task_content_v1: result,
            } = result
            else {
                panic!("missing rich result")
            };
            assert_eq!(
                result.output,
                vec![
                    json!({"type":"text","text":"first"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                    json!({"type":"text","text":"second"})
                ]
            );
            assert_eq!(result.extra.get("details"), Some(&json!({"private":42})));
            assert_eq!(
                result.extra.get("structuredOutput"),
                Some(&json!({"stage":2}))
            );
            assert_eq!(result.extra.get("isError"), Some(&json!(block)));
            assert!(
                result
                    .diagnostics
                    .iter()
                    .any(|d| d.code.as_deref() == Some("stage_one"))
            );
            if block {
                assert!(
                    result
                        .diagnostics
                        .iter()
                        .any(|d| d.code.as_deref() == Some("blocked"))
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!block));
            let _ = driver.shutdown_with_pending().unwrap();
            let (host, _) = content_host(&path, Arc::new(ImmediateDurable(calls.clone())), block);
            let driver = host.into_driver();
            let restored = driver.get(id).unwrap().unwrap();
            assert_eq!(restored.state(), task.state());
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!block));
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct NoopDurableHook;
    #[async_trait::async_trait]
    impl crate::extension::Hook for NoopDurableHook {}
    struct RichParentTool;
    #[async_trait::async_trait]
    impl Tool for RichParentTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "rich-parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for RichParentTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            match resume.checkpoint {
                None => {
                    let child = host.start_child("child", "immediate", json!({})).await?;
                    Ok(DurableToolStep::Park {
                        checkpoint: json!({"child":child}),
                        wait: DurableWait::All(vec![child]),
                    })
                }
                Some(checkpoint) => {
                    let child: AsyncTaskCall = serde_json::from_value(checkpoint["child"].clone())
                        .map_err(|e| e.to_string())?;
                    match host.child_result(&child).await? {
                        Some(AsyncTaskResult::Rich(result))
                            if result.output.len() == 3
                                && result.extra.get("structuredOutput")
                                    == Some(&json!({"stage":2}))
                                && result
                                    .diagnostics
                                    .iter()
                                    .any(|d| d.code.as_deref() == Some("stage_one")) =>
                        {
                            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                                "rich child received".into(),
                            )))
                        }
                        other => Err(format!("unexpected child result: {other:?}")),
                    }
                }
            }
        }
    }
    #[test]
    fn durable_content_nested_result_delivers_rich_channels_after_park() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (mut host, child) = content_host(
            &dir.path().join("db"),
            Arc::new(ImmediateDurable(calls.clone())),
            false,
        );
        let parent = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(RichParentTool),
                "rich-parent-v1",
                1,
                vec![child],
                "parent-hook-v1",
                Arc::new(NoopDurableHook),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&parent, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver, id).state(), crate::task::TaskState::Succeeded(value) if serde_json::from_value::<Outcome>(value.clone()).ok() == Some(Outcome::Succeeded("rich child received".into())))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn durable_content_invalid_arguments_preserve_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, tool) = content_host(
            &dir.path().join("db"),
            Arc::new(StrictDurable(calls.clone())),
            false,
        );
        let id = host
            .start_replay_safe_root(&tool, json!({"bad":true}), 4)
            .unwrap();
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected state {:?}", task.state())
        };
        assert!(
            matches!(serde_json::from_value::<DurableTaskResult>(value.clone()), Ok(DurableTaskResult::Rich { task_content_v1: result }) if result.extra.get("isError") == Some(&json!(true)) && result.diagnostics.iter().any(|d| d.code.as_deref() == Some("invalid_arguments")) && result.output.len() == 3)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct CombinedHooks {
        stage: usize,
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for CombinedHooks {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            self.events
                .lock()
                .unwrap()
                .push(format!("result{}", self.stage));
            let mut out = input.clone();
            if self.stage == 1 {
                out.output = "rich-one".into();
                out.details = Some(json!({"stage": 1}));
                out.structured_output = Some(json!({"value": 42}));
                out.diagnostics.push(crate::runtime::ToolDiagnostic {
                    severity: crate::runtime::DiagnosticSeverity::Info,
                    code: Some("rich".into()),
                    message: "preserved".into(),
                });
            } else if input.output != "block-one"
                || input.structured_output != Some(json!({"value":42}))
                || input.details != Some(json!({"stage":1}))
                || input.diagnostics.len() != 1
            {
                return Err("previous channels lost".into());
            }
            Ok(Some(out))
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            self.events
                .lock()
                .unwrap()
                .push(format!("blocks{}", self.stage));
            let mut out = input.clone();
            if self.stage == 1 {
                out.output = vec![json!({"type":"text","text":"block-one"})];
            } else {
                out.output
                    .push(json!({"type":"image","data":"aGk=","mimeType":"image/png"}));
            }
            Ok(Some(out))
        }
    }
    fn combined_content_host(
        path: &std::path::Path,
        first: Arc<dyn crate::extension::Hook>,
        second: Arc<dyn crate::extension::Hook>,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        for (name, hook) in [("first", first), ("second", second)] {
            registry
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![hook],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{name}-v1"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut host = StandaloneBeforeHookHost::open(path, Registry::default()).unwrap();
        let adapter = host
            .register_replay_safe_durable_tool_with_content_hooks(
                Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
                "combined-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["first".into(), "second".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, adapter)
    }
    #[test]
    fn durable_combined_hooks_run_in_selection_order_and_keep_channels() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let first = Arc::new(CombinedHooks {
            stage: 1,
            events: events.clone(),
        });
        let second = Arc::new(CombinedHooks {
            stage: 2,
            events: events.clone(),
        });
        let (host, adapter) = combined_content_host(&path, first.clone(), second.clone());
        let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected {:?}", task.state())
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("expected rich")
        };
        assert_eq!(
            *events.lock().unwrap(),
            vec!["result1", "blocks1", "result2", "blocks2"]
        );
        assert_eq!(result.output.len(), 2);
        assert_eq!(result.extra["structuredOutput"], json!({"value":42}));
        assert_eq!(result.extra["details"], json!({"stage":1}));
        assert_eq!(result.diagnostics[0].message, "preserved");
        let _ = driver.shutdown_with_pending().unwrap();
        let (host, _) = combined_content_host(&path, first, second);
        let driver = host.into_driver();
        assert_eq!(driver.get(id).unwrap().unwrap().state(), task.state());
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct AddAttachmentMetadata(serde_json::Value);
    #[async_trait::async_trait]
    impl crate::extension::Hook for AddAttachmentMetadata {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let mut out = input.clone();
            out.extra.insert("attachments".into(), self.0.clone());
            Ok(Some(out))
        }
    }
    struct RewriteTextWithAttachments(bool);
    #[async_trait::async_trait]
    impl crate::extension::Hook for RewriteTextWithAttachments {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            let mut out = input.clone();
            out.output = "rewritten".into();
            if self.0 {
                out.attachments.clear();
            }
            Ok(Some(out))
        }
    }
    #[test]
    fn durable_combined_text_bridge_preserves_attachment_representation() {
        for (attachments, change) in [
            (json!([]), false),
            (
                json!([{"id":"asset", "media_type":"image/png", "extra":{"origin":"original"}}]),
                false,
            ),
            (
                json!([{"id":"asset", "media_type":"image/png", "extra":{"origin":"original"}}]),
                true,
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (host, tool) = combined_content_host(
                &path,
                Arc::new(AddAttachmentMetadata(attachments.clone())),
                Arc::new(RewriteTextWithAttachments(change)),
            );
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            if change {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            } else {
                let crate::task::TaskState::Succeeded(value) = task.state() else {
                    panic!("unexpected {:?}", task.state())
                };
                let DurableTaskResult::Rich {
                    task_content_v1: result,
                } = serde_json::from_value(value.clone()).unwrap()
                else {
                    panic!("expected rich result")
                };
                assert_eq!(result.output[0]["text"], "rewritten");
                assert_eq!(result.extra.get("attachments"), Some(&attachments));
            }
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct UnchangedThenShrink(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::extension::Hook for UnchangedThenShrink {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            assert_eq!(
                input.structured_output.as_ref().unwrap()["payload"]
                    .as_str()
                    .unwrap()
                    .len(),
                60_000
            );
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(input.clone()))
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let mut out = input.clone();
            out.output = vec![json!({"type":"text", "text":"trimmed"})];
            Ok(Some(out))
        }
    }
    #[test]
    fn durable_combined_oversized_initial_unchanged_result_reaches_shrinking_block_hook() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("structured"), b"yes").unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let (host, tool) = single_content_host(
            &dir.path().join("db"),
            Arc::new(CountFileTool(dir.path().into())),
            Arc::new(UnchangedThenShrink(count.clone())),
        );
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected {:?}", task.state())
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("expected rich result")
        };
        assert_eq!(result.output[0]["text"], "trimmed");
        assert_eq!(
            result.extra["structuredOutput"]["payload"]
                .as_str()
                .unwrap()
                .len(),
            60_000
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct InvalidRichGate;
    #[async_trait::async_trait]
    impl crate::extension::Hook for InvalidRichGate {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            Ok(crate::extension::BeforeTool::Block("denied".into()))
        }
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            let mut out = input.clone();
            out.diagnostics.clear();
            Ok(Some(out))
        }
    }
    #[test]
    fn durable_combined_invalid_rich_replacement_stops_later_memo() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, adapter) = combined_content_host(
            &path,
            Arc::new(InvalidRichGate),
            Arc::new(LateContentMemo(calls.clone())),
        );
        let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/must-not-run").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    struct OversizeRich;
    #[async_trait::async_trait]
    impl crate::extension::Hook for OversizeRich {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            let mut result = input.clone();
            let block = crate::extension::ToolBlockResult {
                output: vec![json!({"type":"text","text":""})],
                diagnostics: result.diagnostics.clone(),
                extra: [("isError".into(), json!(false))].into_iter().collect(),
            };
            let overhead = serde_json::to_vec(&block).unwrap().len();
            result.output = "x".repeat(60 * 1024 - overhead);
            Ok(Some(result))
        }
    }
    #[test]
    fn durable_combined_envelope_limit_precedes_next_hook_and_memo() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, adapter) = combined_content_host(
            &path,
            Arc::new(OversizeRich),
            Arc::new(LateContentMemo(calls.clone())),
        );
        let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/must-not-run").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    struct MakeImage;
    #[async_trait::async_trait]
    impl crate::extension::Hook for MakeImage {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let mut out = input.clone();
            out.output
                .push(json!({"type":"image","data":"aGk=","mimeType":"image/png"}));
            Ok(Some(out))
        }
    }
    struct ObserveImage(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::extension::Hook for ObserveImage {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            Err("image result cannot be represented".into())
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if input.output.len() != 2 {
                return Err("image lost".into());
            }
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    #[test]
    fn durable_combined_image_skips_lossy_text_hook() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, adapter) = combined_content_host(
            &dir.path().join("db"),
            Arc::new(MakeImage),
            Arc::new(ObserveImage(calls.clone())),
        );
        let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Succeeded(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct InvalidContentHook;
    #[async_trait::async_trait]
    impl crate::extension::Hook for InvalidContentHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let mut result = input.clone();
            result.output = vec![json!({"type":"image","data":"invalid","mimeType":"image/png"})];
            Ok(Some(result))
        }
    }
    #[test]
    fn durable_content_rejects_invalid_image() {
        let dir = tempfile::tempdir().unwrap();
        let (host, tool) = single_content_host(
            &dir.path().join("db"),
            Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
            Arc::new(InvalidContentHook),
        );
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct OriginalChannelsTool {
        structured: bool,
    }
    #[async_trait::async_trait]
    impl Tool for OriginalChannelsTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "original-channels".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for OriginalChannelsTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            Ok(DurableToolStep::Complete(if self.structured {
                Outcome::Structured {
                    data: json!({"source":42}),
                    attachments: vec![pi_rs_core::AttachmentRef {
                        id: "source-file".into(),
                        media_type: "text/plain".into(),
                    }],
                }
            } else {
                Outcome::SucceededWithDetails {
                    output: "original".into(),
                    details: serde_json::Value::Null,
                }
            }))
        }
    }
    struct PreserveOriginalHook(bool);
    #[async_trait::async_trait]
    impl crate::extension::Hook for PreserveOriginalHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if self.0 {
                if input.extra.get("structuredOutput") != Some(&json!({"source":42}))
                    || input
                        .extra
                        .get("attachments")
                        .and_then(|v| v.as_array())
                        .is_none_or(|v| v.len() != 1)
                {
                    return Err("original structured channels missing".into());
                }
            } else if input.extra.get("details") != Some(&serde_json::Value::Null) {
                return Err("explicit null details missing".into());
            }
            let mut out = input.clone();
            out.output = vec![
                json!({"type":"text","text":"a"}),
                json!({"type":"text","text":"b"}),
            ];
            Ok(Some(out))
        }
    }
    #[test]
    fn durable_content_preserves_original_details_and_structured_attachments() {
        for structured in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (host, tool) = single_content_host(
                &dir.path().join("db"),
                Arc::new(OriginalChannelsTool { structured }),
                Arc::new(PreserveOriginalHook(structured)),
            );
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            let crate::task::TaskState::Succeeded(value) = task.state() else {
                panic!("unexpected {:?}", task.state())
            };
            let DurableTaskResult::Rich {
                task_content_v1: result,
            } = serde_json::from_value(value.clone()).unwrap()
            else {
                panic!("expected rich result")
            };
            assert_eq!(result.output.len(), 2);
            if structured {
                assert_eq!(
                    result.extra.get("structuredOutput"),
                    Some(&json!({"source":42}))
                );
                assert_eq!(result.extra["attachments"][0]["id"], "source-file");
            } else {
                assert_eq!(result.extra.get("details"), Some(&serde_json::Value::Null));
            }
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct FailedResultTool;
    #[async_trait::async_trait]
    impl Tool for FailedResultTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "failed-result".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for FailedResultTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            Ok(DurableToolStep::Complete(Outcome::Failed(
                "tool failure".into(),
            )))
        }
    }
    #[test]
    fn durable_content_tool_failure_produces_durable_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (host, tool) =
            single_content_host(&path, Arc::new(FailedResultTool), Arc::new(NoopDurableHook));
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected {:?}", task.state())
        };
        assert!(
            matches!(serde_json::from_value::<DurableTaskResult>(value.clone()), Ok(DurableTaskResult::Rich { task_content_v1: result }) if result.extra.get("isError") == Some(&json!(true)) && result.diagnostics.iter().any(|d| d.code.as_deref() == Some("tool_failed") && d.message == "tool failure"))
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let (host, _) =
            single_content_host(&path, Arc::new(FailedResultTool), Arc::new(NoopDurableHook));
        let driver = host.into_driver();
        assert_eq!(driver.get(id).unwrap().unwrap().state(), task.state());
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct LongBlock;
    #[async_trait::async_trait]
    impl crate::extension::Hook for LongBlock {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            Ok(crate::extension::BeforeTool::Block("あ".repeat(2000)))
        }
    }
    struct LongFailure(bool);
    #[async_trait::async_trait]
    impl Tool for LongFailure {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "long-failure".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for LongFailure {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            Ok(DurableToolStep::Complete(if self.0 {
                Outcome::Skipped("あ".repeat(2000))
            } else {
                Outcome::Failed("あ".repeat(2000))
            }))
        }
    }
    #[test]
    fn durable_content_long_utf8_host_and_tool_diagnostics_survive() {
        for mode in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let (tool, hook): (Arc<dyn DurableAsyncTool>, Arc<dyn crate::extension::Hook>) =
                if mode == 0 {
                    (
                        Arc::new(ImmediateDurable(calls.clone())),
                        Arc::new(LongBlock),
                    )
                } else {
                    (Arc::new(LongFailure(mode == 2)), Arc::new(NoopDurableHook))
                };
            let (host, adapter) = single_content_host(&dir.path().join("db"), tool, hook);
            let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            let crate::task::TaskState::Succeeded(value) = task.state() else {
                panic!("unexpected {:?}", task.state())
            };
            let DurableTaskResult::Rich {
                task_content_v1: result,
            } = serde_json::from_value(value.clone()).unwrap()
            else {
                panic!("expected rich result")
            };
            let diagnostic = &result.diagnostics[0];
            assert_eq!(
                diagnostic.code.as_deref(),
                Some(match mode {
                    0 => "blocked",
                    1 => "tool_failed",
                    _ => "tool_skipped",
                })
            );
            if mode == 0 {
                // Host-generated reasons are capped at 2048 UTF-8 bytes before
                // adding the diagnostic prefix; tool reasons have a 4096-byte cap.
                assert!((2048..=2080).contains(&diagnostic.message.len()));
            } else {
                assert!((4093..=4096).contains(&diagnostic.message.len()));
            }
            assert!(result.output[0]["text"].as_str().unwrap().len() > diagnostic.message.len());
            assert_eq!(result.extra.get("isError"), Some(&json!(true)));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct LateContentMemo(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::extension::Hook for LateContentMemo {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            ctx.memo.get_or_insert("must-not-run", json!(true)).await?;
            Ok(None)
        }
    }
    #[test]
    fn durable_content_invalid_first_replacement_stops_later_hook_and_memo() {
        for blocked in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let calls = Arc::new(AtomicUsize::new(0));
            let later = Arc::new(AtomicUsize::new(0));
            let first: Arc<dyn crate::extension::Hook> = if blocked {
                Arc::new(SuppressGate)
            } else {
                Arc::new(InvalidContentHook)
            };
            let mut extensions = crate::extension::Registry::default();
            for (name, hook) in [
                ("first", first),
                (
                    "second",
                    Arc::new(LateContentMemo(later.clone())) as Arc<dyn crate::extension::Hook>,
                ),
            ] {
                extensions
                    .install_identified(
                        crate::extension::Extension {
                            name: name.into(),
                            hooks: vec![hook],
                            ..Default::default()
                        },
                        crate::extension::ExtensionIdentity::new(
                            1,
                            format!("{name}-v1"),
                            vec!["hook".into()],
                        )
                        .unwrap(),
                    )
                    .unwrap();
            }
            let mut host = StandaloneBeforeHookHost::open(&path, Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_content_hooks(
                    Arc::new(ImmediateDurable(calls.clone())),
                    "immediate-v1",
                    1,
                    vec![],
                    &extensions.snapshot(),
                    &["first".into(), "second".into()],
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Failed(_)
            ));
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(!blocked));
            assert_eq!(later.load(Ordering::SeqCst), 0);
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            assert!(
                store
                    .get(format!("runtime/extension-memo/{id}/must-not-run").as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
    }

    struct EnvelopeBoundaryHook(usize);
    #[async_trait::async_trait]
    impl crate::extension::Hook for EnvelopeBoundaryHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let mut output = input.clone();
            output.output = vec![json!({"type":"text", "text":""})];
            let empty_envelope = serde_json::to_vec(&DurableTaskResult::Rich {
                task_content_v1: output.clone(),
            })
            .map_err(|e| e.to_string())?
            .len();
            output.output[0]["text"] = json!("x".repeat(self.0 - empty_envelope));
            let raw_size = serde_json::to_vec(&output)
                .map_err(|e| e.to_string())?
                .len();
            let envelope_size = serde_json::to_vec(&DurableTaskResult::Rich {
                task_content_v1: output.clone(),
            })
            .map_err(|e| e.to_string())?
            .len();
            assert!(raw_size <= 60 * 1024);
            assert_eq!(envelope_size, self.0);
            Ok(Some(output))
        }
    }

    #[test]
    fn durable_content_envelope_boundary_fences_later_hook_and_memo() {
        for oversized in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let calls = Arc::new(AtomicUsize::new(0));
            let later = Arc::new(AtomicUsize::new(0));
            let mut extensions = crate::extension::Registry::default();
            for (name, hook) in [
                (
                    "first",
                    Arc::new(EnvelopeBoundaryHook(60 * 1024 + usize::from(oversized)))
                        as Arc<dyn crate::extension::Hook>,
                ),
                (
                    "second",
                    Arc::new(LateContentMemo(later.clone())) as Arc<dyn crate::extension::Hook>,
                ),
            ] {
                extensions
                    .install_identified(
                        crate::extension::Extension {
                            name: name.into(),
                            hooks: vec![hook],
                            ..Default::default()
                        },
                        crate::extension::ExtensionIdentity::new(
                            1,
                            format!("{name}-v1"),
                            vec!["hook".into()],
                        )
                        .unwrap(),
                    )
                    .unwrap();
            }
            let mut host = StandaloneBeforeHookHost::open(&path, Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_content_hooks(
                    Arc::new(ImmediateDurable(calls.clone())),
                    "immediate-v1",
                    1,
                    vec![],
                    &extensions.snapshot(),
                    &["first".into(), "second".into()],
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            if oversized {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            } else {
                let crate::task::TaskState::Succeeded(value) = task.state() else {
                    panic!("boundary result failed: {:?}", task.state());
                };
                assert_eq!(serde_json::to_vec(value).unwrap().len(), 60 * 1024);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(later.load(Ordering::SeqCst), usize::from(!oversized));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            let memo = store
                .get(format!("runtime/extension-memo/{id}/must-not-run").as_bytes())
                .unwrap();
            assert_eq!(memo.is_some(), !oversized);
        }
    }

    struct ContentPause(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for ContentPause {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            if !self.0.join("rich-phase").exists() {
                return Ok(None);
            }
            use std::io::Write;
            ctx.memo
                .get_or_insert("rich-phase", json!("stable"))
                .await?;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.0.join("rich-count"))
                .map_err(|e| e.to_string())?;
            writeln!(file, "run").map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            let mut out = input.clone();
            out.output = "rich-before-block".into();
            out.structured_output = Some(json!({"phase":1}));
            Ok(Some(out))
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let exists = self.0.join("content-ready").exists();
            if self.0.join("rich-phase").exists()
                && (input.output[0]["text"] != json!("rich-before-block")
                    || input.extra.get("structuredOutput") != Some(&json!({"phase":1})))
            {
                return Err("result phase was not preserved".into());
            }
            if self.0.join("structured").exists()
                && input
                    .extra
                    .get("structuredOutput")
                    .and_then(|v| v.get("payload"))
                    .and_then(|v| v.as_str())
                    .map(str::len)
                    != Some(60_000)
            {
                return Err("large structured result missing".into());
            }
            if ctx
                .memo
                .get_or_insert("resume", json!(if exists { "second" } else { "first" }))
                .await?
                != json!("first")
            {
                return Err("memo changed".into());
            }
            if !exists {
                std::fs::write(self.0.join("content-ready"), b"ready")
                    .map_err(|e| e.to_string())?;
                std::future::pending().await
            } else {
                let mut out = input.clone();
                out.output = vec![
                    json!({"type":"text","text":"restored"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                ];
                out.extra.insert("details".into(), json!({"resumed":true}));
                Ok(Some(out))
            }
        }
    }
    struct CountFileTool(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for CountFileTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "content-file".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for CountFileTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.0.join("tool-count"))
                .map_err(|e| e.to_string())?;
            writeln!(file, "run").map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            Ok(DurableToolStep::Complete(
                if self.0.join("structured").exists() {
                    Outcome::Structured {
                        data: json!({"payload": "x".repeat(60_000)}),
                        attachments: vec![],
                    }
                } else {
                    Outcome::Succeeded("raw".into())
                },
            ))
        }
    }
    fn content_pause_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "pause".into(),
                    hooks: vec![Arc::new(ContentPause(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "pause-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let adapter = host
            .register_replay_safe_durable_tool_with_content_hooks(
                Arc::new(CountFileTool(root.to_owned())),
                "file-v1",
                1,
                vec![],
                &extensions.snapshot(),
                &["pause".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, adapter)
    }
    #[test]
    fn durable_content_after_kill_reopens_without_tool_reexecution() {
        const ENV: &str = "PI_RS_CONTENT_AFTER_KILL_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = content_pause_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::durable_content_after_kill_reopens_without_tool_reexecution").env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("content-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("content hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = content_pause_host(dir.path());
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected task state {:?}", task.state())
        };
        assert!(
            matches!(serde_json::from_value::<DurableTaskResult>(value.clone()), Ok(DurableTaskResult::Rich { task_content_v1: result }) if result.output.len() == 2 && result.extra.get("details") == Some(&json!({"resumed":true})))
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn durable_large_structured_after_kill_reopens_without_tool_reexecution() {
        const ENV: &str = "PI_RS_LARGE_CONTENT_AFTER_KILL_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = content_pause_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("structured"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::durable_large_structured_after_kill_reopens_without_tool_reexecution").env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("content-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("large structured hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = content_pause_host(dir.path());
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected task state {:?}", task.state())
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("expected rich result")
        };
        assert_eq!(
            result.extra["structuredOutput"]["payload"]
                .as_str()
                .unwrap()
                .len(),
            60_000
        );
        assert_eq!(result.extra.get("details"), Some(&json!({"resumed":true})));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct CountLargeNoop(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl crate::extension::Hook for CountLargeNoop {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            result: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            assert_eq!(
                result.extra["structuredOutput"]["payload"]
                    .as_str()
                    .unwrap()
                    .len(),
                60_000
            );
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }
    #[test]
    fn durable_large_structured_noop_runs_before_final_size_rejection() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("structured"), b"yes").unwrap();
        let seen = Arc::new(AtomicUsize::new(0));
        let (host, tool) = single_content_host(
            &dir.path().join("db"),
            Arc::new(CountFileTool(dir.path().to_owned())),
            Arc::new(CountLargeNoop(seen.clone())),
        );
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn durable_combined_result_then_blocks_recover_after_kill() {
        const ENV: &str = "PI_RS_COMBINED_RESULT_KILL_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, adapter) = content_pause_host(&root);
            let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("rich-phase"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::durable_combined_result_then_blocks_recover_after_kill")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("content-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = content_pause_host(dir.path());
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected {:?}", task.state())
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("expected rich")
        };
        assert_eq!(result.extra["structuredOutput"], json!({"phase":1}));
        assert_eq!(result.output.len(), 2);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("rich-count"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn durable_host_restart_limit_does_not_invoke_unfenced_recovery_hook() {
        const ENV: &str = "PI_RS_HOST_RESTART_LIMIT_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, adapter) = content_pause_host(&root);
            let id = host.start_replay_safe_root(&adapter, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("rich-phase"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::durable_host_restart_limit_does_not_invoke_unfenced_recovery_hook").env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("content-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("after hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = content_pause_host(dir.path());
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        assert!(
            matches!(task.state(), crate::task::TaskState::Failed(reason) if reason == "restart limit")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("rich-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let key = format!("runtime/extension-memo/{id}/rich-phase");
        assert!(store.get(key.as_bytes()).unwrap().is_some());
    }
    #[test]
    fn durable_host_missing_definition_blocks_hooks_until_restored() {
        const ENV: &str = "PI_RS_HOST_MISSING_DEFINITION_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, adapter) = content_pause_host(&root);
            let id = host.start_replay_safe_root(&adapter, json!({}), 4).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("rich-phase"), b"yes").unwrap();
        let mut process=std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::durable_host_missing_definition_blocks_hooks_until_restored").env(ENV,dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("content-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let driver = host.into_driver();
        assert!(!driver.get(id).unwrap().unwrap().is_terminal());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("rich-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let (host, _) = content_pause_host(dir.path());
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Succeeded(_)
        ));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("rich-count"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct AbortResultTool(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for AbortResultTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "abort-result-tool".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for AbortResultTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            std::fs::write(self.0.join("tool-start"), b"ready").map_err(|e| e.to_string())?;
            context.cancellation.cancelled().await;
            Err("cancelled".into())
        }
    }
    struct AbortResultHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for AbortResultHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if !input
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("aborted"))
            {
                return Err("missing aborted diagnosis".into());
            }
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("abort-hook-count"))
                .map_err(|e| e.to_string())?;
            writeln!(f, "hook").map_err(|e| e.to_string())?;
            f.sync_all().map_err(|e| e.to_string())?;
            if self.0.join("memo-test").exists() {
                let _ = ctx.memo.get_or_insert("abort-write", json!(true)).await;
            }
            if self.0.join("pause-always").exists()
                || (self.0.join("pause-hook").exists() && !self.0.join("abort-hook-ready").exists())
            {
                std::fs::write(self.0.join("abort-hook-ready"), b"ready")
                    .map_err(|e| e.to_string())?;
                std::future::pending::<()>().await;
            }
            let mut out = input.clone();
            out.output = vec![json!({"type":"text","text":"aborted after hook"})];
            out.extra.insert("details".into(), json!({"cleanup":true}));
            Ok(Some(out))
        }
    }
    fn abort_result_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        abort_result_host_identity(root, "abort-hook-v1")
    }
    fn abort_result_host_identity(
        root: &std::path::Path,
        hook_identity: &str,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "abort-hook".into(),
                    hooks: vec![Arc::new(AbortResultHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    hook_identity.into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(AbortResultTool(root.to_owned())),
                "abort-tool-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["abort-hook".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, tool)
    }
    fn wait_file(path: &std::path::Path) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {}",
                path.display()
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    fn ordinary_rich_abort_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "abort-hook".into(),
                    hooks: vec![Arc::new(AbortResultHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "abort-hook-v1".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_content_hooks(
                Arc::new(AbortResultTool(root.to_owned())),
                "abort-tool-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["abort-hook".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, tool)
    }
    #[test]
    fn ordinary_rich_abort_reopen_does_not_run_or_persist_abort_hook() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (host, tool) = ordinary_rich_abort_host(root);
        let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let driver = host.into_driver();
        wait_file(&root.join("tool-start"));
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("abort-hook-count").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_aborted_after_result(&store, id).unwrap().is_none());
        assert!(
            read_replay_safe_after_evidence(&store, id)
                .unwrap()
                .is_none()
        );
        drop(store);
        let (host, _) = ordinary_rich_abort_host(root);
        let driver = host.into_driver();
        assert!(matches!(
            driver.get(id).unwrap().unwrap().state(),
            crate::task::TaskState::Cancelled
        ));
        assert!(!root.join("abort-hook-count").exists());
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct MutableAbortTool {
        changed: Arc<std::sync::atomic::AtomicBool>,
        root: std::path::PathBuf,
    }
    #[async_trait::async_trait]
    impl Tool for MutableAbortTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "mutable-abort-tool".into(),
                description: if self.changed.load(Ordering::SeqCst) {
                    "changed"
                } else {
                    "original"
                }
                .into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for MutableAbortTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            std::fs::write(self.root.join("tool-start"), b"ready").map_err(|e| e.to_string())?;
            context.cancellation.cancelled().await;
            Err("cancelled".into())
        }
    }
    struct PausedIdentityAbortHook {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for PausedIdentityAbortHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            self.entered.send(()).map_err(|e| e.to_string())?;
            self.release
                .lock()
                .map_err(|e| e.to_string())?
                .recv()
                .map_err(|e| e.to_string())?;
            let mut result = input.clone();
            result.output = vec![json!({"type":"text", "text":"cleanup completed"})];
            Ok(Some(result))
        }
    }
    #[test]
    fn aborted_after_rechecks_tool_identity_after_hook_before_receipt() {
        for mutate in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (entered, entered_rx) = std::sync::mpsc::channel();
            let (release, release_rx) = std::sync::mpsc::channel();
            let mut extensions = crate::extension::Registry::default();
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: "identity-hook".into(),
                        hooks: vec![Arc::new(PausedIdentityAbortHook {
                            entered,
                            release: std::sync::Mutex::new(release_rx),
                        })],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        "identity-hook-v1".into(),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
            let mut host =
                StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_abort_after(
                    Arc::new(MutableAbortTool {
                        changed: changed.clone(),
                        root: dir.path().into(),
                    }),
                    "mutable-abort-v1",
                    1,
                    vec![],
                    &extensions.snapshot(),
                    &["identity-hook".into()],
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                    AbortAfterAuthorization::host_attests_cleanup_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            let driver = host.into_driver();
            wait_file(&dir.path().join("tool-start"));
            driver.request_abort(id).unwrap();
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            if mutate {
                changed.store(true, Ordering::SeqCst);
            }
            release.send(()).unwrap();
            let task = wait_terminal(&driver, id);
            if mutate {
                assert!(
                    matches!(task.state(), crate::task::TaskState::Failed(reason) if reason.contains("identity changed during execution")),
                    "{:?}",
                    task.state()
                );
            } else {
                assert!(
                    matches!(task.state(), crate::task::TaskState::Cancelled),
                    "{:?}",
                    task.state()
                );
            }
            driver.shutdown().unwrap();
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let receipt = store
                .get(format!("durable/replay-safe-async/{id}/abort-after").as_bytes())
                .unwrap();
            assert_eq!(receipt.is_some(), !mutate);
            assert_eq!(
                read_aborted_after_result(&store, id).unwrap().is_some(),
                !mutate
            );
        }
    }

    #[test]
    fn aborted_after_result_is_durable_and_memo_is_denied() {
        for memo in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            if memo {
                std::fs::write(dir.path().join("memo-test"), b"yes").unwrap();
            }
            let (host, tool) = abort_result_host(dir.path());
            let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
            let driver = host.into_driver();
            wait_file(&dir.path().join("tool-start"));
            driver.request_abort(id).unwrap();
            let task = wait_terminal(&driver, id);
            if memo {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            } else {
                assert!(matches!(task.state(), crate::task::TaskState::Cancelled));
            }
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            assert!(
                store
                    .get(format!("runtime/extension-memo/{id}/abort-write").as_bytes())
                    .unwrap()
                    .is_none()
            );
            let result = read_aborted_after_result(&store, id).unwrap();
            if memo {
                assert!(result.is_none());
            } else {
                let Some(DurableTaskResult::Rich {
                    task_content_v1: result,
                }) = result
                else {
                    panic!("missing abort result")
                };
                assert_eq!(result.output[0]["text"], json!("aborted after hook"));
                assert_eq!(result.extra["details"], json!({"cleanup":true}));
                assert!(
                    result
                        .diagnostics
                        .iter()
                        .any(|d| d.code.as_deref() == Some("aborted"))
                );
            }
        }
    }
    #[test]
    fn aborted_after_kill_reopen_retries_or_reuses_receipt() {
        const ENV: &str = "PI_RS_ABORT_AFTER_TEST_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = abort_result_host(&root);
            let limit = if root.join("receipt-mode").exists() {
                1
            } else {
                4
            };
            let id = host
                .start_replay_safe_root(&tool, json!({}), limit)
                .unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let driver = host.into_driver();
            wait_file(&root.join("tool-start"));
            driver.request_abort(id).unwrap();
            loop {
                std::thread::park();
            }
        }
        for receipt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            if receipt {
                std::fs::write(dir.path().join("receipt-mode"), b"yes").unwrap();
            } else {
                std::fs::write(dir.path().join("pause-hook"), b"yes").unwrap();
            }
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("async_task_tool::tests::aborted_after_kill_reopen_retries_or_reuses_receipt")
                .env(ENV, dir.path());
            if receipt {
                command.env("PI_TEST_ABORT_AFTER_RECEIPT", dir.path());
            }
            let mut process = command.spawn().unwrap();
            wait_file(&dir.path().join(if receipt {
                "abort-receipt"
            } else {
                "abort-hook-ready"
            }));
            process.kill().unwrap();
            process.wait().unwrap();
            let id: Uuid = std::fs::read_to_string(dir.path().join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let (host, _) = abort_result_host(dir.path());
            let driver = host.into_driver();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Cancelled
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            assert!(read_aborted_after_result(&store, id).unwrap().is_some());
            assert_eq!(
                std::fs::read_to_string(dir.path().join("abort-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                if receipt { 1 } else { 2 }
            );
        }
    }
    #[test]
    fn aborted_after_rejects_stale_cleanup_lease_in_driver() {
        const ENV: &str = "PI_RS_ABORT_STALE_WORKER_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = abort_result_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
            let driver = host.into_driver();
            wait_file(&root.join("tool-start"));
            driver.request_abort(id).unwrap();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Cancelled
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::aborted_after_rejects_stale_cleanup_lease_in_driver")
            .env(ENV, dir.path())
            .env("PI_TEST_ABORT_STALE_LEASE", dir.path())
            .status()
            .unwrap();
        assert!(status.success());
        assert!(dir.path().join("stale-rejected").exists());
    }
    #[test]
    fn aborted_after_repeated_kills_stop_at_cleanup_budget() {
        const ENV: &str = "PI_RS_ABORT_BUDGET_TEST_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = abort_result_host(&root);
            if !root.join("task-id").exists() {
                let id = host.start_replay_safe_root(&tool, json!({}), 2).unwrap();
                std::fs::write(root.join("task-id"), id.to_string()).unwrap();
                let driver = host.into_driver();
                wait_file(&root.join("tool-start"));
                driver.request_abort(id).unwrap();
                loop {
                    std::thread::park();
                }
            }
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("pause-always"), b"yes").unwrap();
        for count in 1..=2 {
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::aborted_after_repeated_kills_stop_at_cleanup_budget")
                .env(ENV, root)
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while std::fs::read_to_string(root.join("abort-hook-count"))
                .ok()
                .map(|s| s.lines().count())
                .unwrap_or(0)
                < count
            {
                if std::time::Instant::now() >= deadline {
                    let _ = process.kill();
                    let _ = process.wait();
                    panic!("abort attempt did not start");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            process.kill().unwrap();
            process.wait().unwrap();
        }
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = abort_result_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="abort restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_aborted_after_result(&store, id).unwrap().is_none());
    }
    fn interrupted_saved_abort_receipt(root: &std::path::Path) -> Uuid {
        std::fs::write(root.join("receipt-mode"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::aborted_after_kill_reopen_retries_or_reuses_receipt")
            .env("PI_RS_ABORT_AFTER_TEST_1120", root)
            .env("PI_TEST_ABORT_AFTER_RECEIPT", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("abort-receipt"));
        process.kill().unwrap();
        process.wait().unwrap();
        std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap()
    }
    #[test]
    fn aborted_after_receipt_recovery_rejects_deleted_or_changed_evidence() {
        for (intent, replace) in [(false, false), (false, true), (true, false), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let id = interrupted_saved_abort_receipt(dir.path());
            let (host, _) = abort_result_host(dir.path());
            let mut scheduler = host.into_scheduler_for_test();
            let fired = Arc::new(AtomicUsize::new(0));
            let observed = fired.clone();
            scheduler.set_reservation_commit_hook(Arc::new(move |_, store| {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    let receipt_key =
                        format!("durable/replay-safe-async/{id}/abort-after").into_bytes();
                    let key = if intent {
                        let receipt = store.get(&receipt_key).unwrap().unwrap();
                        let value: serde_json::Value =
                            serde_json::from_slice(&receipt.value).unwrap();
                        format!(
                            "durable/replay-safe-async/{id}/abort-intent/{}",
                            value["intent"]["attempt"].as_str().unwrap()
                        )
                        .into_bytes()
                    } else {
                        receipt_key
                    };
                    let mutation = if replace {
                        durable_store::Mutation::Put {
                            key,
                            value: b"{}".to_vec(),
                        }
                    } else {
                        durable_store::Mutation::Delete { key }
                    };
                    store.commit(&[], &[mutation]).unwrap();
                }
            }));
            let driver = crate::task_driver::Driver::start(scheduler);
            let task = wait_terminal(&driver, id);
            assert!(
                matches!(task.state(),crate::task::TaskState::Failed(reason) if reason=="abort restart limit")
            );
            assert!(fired.load(Ordering::SeqCst) >= 1);
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }
    #[test]
    fn aborted_after_receipt_recovery_requires_matching_registration() {
        for changed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let id = interrupted_saved_abort_receipt(dir.path());
            let host = if changed {
                abort_result_host_identity(dir.path(), "changed-hook-v2").0
            } else {
                StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap()
            };
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            assert!(
                matches!(task.state(),crate::task::TaskState::Failed(reason) if reason=="abort restart limit")
            );
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }
    struct SuppressAbortDiagnostic;
    #[async_trait::async_trait]
    impl crate::extension::Hook for SuppressAbortDiagnostic {
        async fn after_tool_result(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolResult,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolResult>, String> {
            let mut out = input.clone();
            out.diagnostics.clear();
            Ok(Some(out))
        }
    }
    #[test]
    fn aborted_after_invalid_replacement_stops_later_hook() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::extension::Registry::default();
        for (name, hook) in [
            (
                "first",
                Arc::new(SuppressAbortDiagnostic) as Arc<dyn crate::extension::Hook>,
            ),
            (
                "second",
                Arc::new(LateContentMemo(calls.clone())) as Arc<dyn crate::extension::Hook>,
            ),
        ] {
            registry
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![hook],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{name}-v1"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(AbortResultTool(root.to_owned())),
                "tool-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["first".into(), "second".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let driver = host.into_driver();
        wait_file(&root.join("tool-start"));
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_aborted_after_result(&store, id).unwrap().is_none());
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/must-not-run").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    struct HoldCleanupHook {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for HoldCleanupHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.send(()).map_err(|e| e.to_string())?;
            self.release
                .lock()
                .unwrap()
                .recv()
                .map_err(|e| e.to_string())?;
            Ok(None)
        }
    }
    #[test]
    fn aborted_after_duplicate_abort_does_not_repeat_hook() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (entered, rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "hold".into(),
                    hooks: vec![Arc::new(HoldCleanupHook {
                        entered,
                        release: std::sync::Mutex::new(release_rx),
                        calls: calls.clone(),
                    })],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "hold-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(AbortResultTool(root.to_owned())),
                "tool-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["hold".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let driver = host.into_driver();
        wait_file(&root.join("tool-start"));
        driver.request_abort(id).unwrap();
        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_aborted_after_result(&store, id).unwrap().is_some());
    }
    struct AbortParentTool;
    #[async_trait::async_trait]
    impl Tool for AbortParentTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "abort-parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for AbortParentTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            let child = host
                .start_child("child", "abort-result-tool", json!({}))
                .await?;
            Ok(DurableToolStep::Park {
                checkpoint: json!({}),
                wait: DurableWait::All(vec![child]),
            })
        }
    }
    struct CleanupOrder(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for CleanupOrder {
        async fn after_tool_blocks(
            &self,
            call: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if input
                .diagnostics
                .iter()
                .any(|d| matches!(d.code.as_deref(), Some("aborted" | "restart_limit")))
            {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("order"))
                    .map_err(|e| e.to_string())?;
                writeln!(f, "{}", call.name).map_err(|e| e.to_string())?;
                f.sync_all().map_err(|e| e.to_string())?;
            }
            Ok(None)
        }
    }
    #[test]
    fn aborted_after_waits_for_child_cleanup_before_parent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "order".into(),
                    hooks: vec![Arc::new(CleanupOrder(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "order-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let snapshot = registry.snapshot();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let child = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(AbortResultTool(root.to_owned())),
                "child-v1",
                1,
                vec![],
                &snapshot,
                &["order".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let parent = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(AbortParentTool),
                "parent-v1",
                1,
                vec![child],
                &snapshot,
                &["order".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&parent, json!({}), 4).unwrap();
        let driver = host.into_driver();
        wait_file(&root.join("tool-start"));
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("order"))
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec!["abort-result-tool", "abort-parent"]
        );
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let records = crate::task_store::TaskStore::new(&store).list().unwrap();
        let child = records
            .iter()
            .find(|r| r.task.owner() == &crate::task::TaskOwner::Task(id))
            .unwrap();
        assert!(matches!(
            child.task.state(),
            crate::task::TaskState::Cancelled
        ));
        assert!(read_aborted_after_result(&store, id).unwrap().is_some());
        assert!(
            read_aborted_after_result(&store, child.task.id())
                .unwrap()
                .is_some()
        );
    }
    #[test]
    fn replay_safe_restart_limit_keeps_structured_raw_outcome_for_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("structured"), b"yes").unwrap();
        let mut process=std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::durable_host_restart_limit_does_not_invoke_unfenced_recovery_hook").env("PI_RS_HOST_RESTART_LIMIT_1120",root).spawn().unwrap();
        wait_file(&root.join("content-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = content_pause_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let Some(ReplaySafeAfterEvidence::ToolOutcome(Outcome::Structured { data, attachments })) =
            read_replay_safe_after_evidence(&store, id).unwrap()
        else {
            panic!("missing structured raw outcome")
        };
        assert_eq!(data["payload"].as_str().unwrap().len(), 60_000);
        assert!(attachments.is_empty());
        assert_eq!(
            std::fs::read_to_string(root.join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    struct CrashUncertainTool(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for CrashUncertainTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "crash-uncertain".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for CrashUncertainTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.0.join("uncertain-invocations"))
                .map_err(|e| e.to_string())?;
            writeln!(f, "invoked").map_err(|e| e.to_string())?;
            f.sync_all().map_err(|e| e.to_string())?;
            std::fs::write(self.0.join("uncertain-ready"), b"ready").map_err(|e| e.to_string())?;
            std::future::pending().await
        }
    }
    fn uncertain_recovery_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        single_content_host(
            &root.join("db"),
            Arc::new(CrashUncertainTool(root.to_owned())),
            Arc::new(NoopDurableHook),
        )
    }
    #[test]
    fn replay_safe_interrupted_execution_never_invents_known_outcome() {
        const ENV: &str = "PI_RS_UNCERTAIN_PRECHECKPOINT_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = uncertain_recovery_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process=std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::replay_safe_interrupted_execution_never_invents_known_outcome").env(ENV,root).spawn().unwrap();
        wait_file(&root.join("uncertain-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = uncertain_recovery_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            read_replay_safe_after_evidence(&store, id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            std::fs::read_to_string(root.join("uncertain-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    struct MutableRestartTool(Arc<std::sync::atomic::AtomicBool>);
    #[async_trait::async_trait]
    impl Tool for MutableRestartTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "mutable-restart".into(),
                description: if self.0.load(Ordering::SeqCst) {
                    "changed"
                } else {
                    "original"
                }
                .into(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for MutableRestartTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            Err("must not execute".into())
        }
    }
    struct PausedRestartHook {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for PausedRestartHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.send(()).map_err(|e| e.to_string())?;
            self.release
                .lock()
                .map_err(|e| e.to_string())?
                .recv()
                .map_err(|e| e.to_string())?;
            let mut out = input.clone();
            out.output = vec![json!({"type":"text", "text":"restart cleaned"})];
            Ok(Some(out))
        }
    }
    #[test]
    fn restart_cleanup_respects_identity_and_abort_precedence() {
        use crate::task_store::TaskStore;
        for case in [
            "control",
            "mutation",
            "abort-before",
            "abort-after-drain-mark",
            "abort-during",
            "abort-after-receipt",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let calls = Arc::new(AtomicUsize::new(0));
            let (entered, entered_rx) = std::sync::mpsc::channel();
            let (release, release_rx) = std::sync::mpsc::channel();
            let mut extensions = crate::extension::Registry::default();
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: "restart-race".into(),
                        hooks: vec![Arc::new(PausedRestartHook {
                            entered,
                            release: std::sync::Mutex::new(release_rx),
                            calls: calls.clone(),
                        })],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        "restart-race-v1".into(),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
            let mut host =
                StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
            let tool = host
                .register_replay_safe_durable_tool_with_restart_after(
                    Arc::new(MutableRestartTool(changed.clone())),
                    "mutable-restart-v1",
                    1,
                    vec![],
                    &extensions.snapshot(),
                    &["restart-race".into()],
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                    RestartAfterAuthorization::host_attests_cleanup_safe(),
                )
                .unwrap();
            let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
            let mut scheduler = host.into_scheduler_for_test();
            let owner = scheduler.owner();
            let tasks = TaskStore::new(owner.store());
            let snapshot = tasks.load(id).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    task.start_attempt(Uuid::new_v4())?;
                    task.hold_restart_failure()
                })
                .unwrap();
            if case == "abort-after-drain-mark" {
                let snapshot = tasks.load(id).unwrap().unwrap();
                tasks
                    .update(&snapshot, |task| task.request_restart_internal_abort())
                    .unwrap();
            }
            if matches!(case, "abort-before" | "abort-after-drain-mark") {
                scheduler.request_abort(id).unwrap();
            }
            let (receipt_entered, receipt_entered_rx) = std::sync::mpsc::channel();
            let (receipt_release, receipt_release_rx) = std::sync::mpsc::channel();
            if case == "abort-after-receipt" {
                replay_safe::after_restart_receipt(id, move || {
                    receipt_entered.send(()).unwrap();
                    receipt_release_rx.recv().unwrap();
                });
            }
            let driver = crate::task_driver::Driver::start(scheduler);
            if !matches!(case, "abort-before" | "abort-after-drain-mark") {
                entered_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("restart hook did not enter");
                if case == "mutation" {
                    changed.store(true, Ordering::SeqCst);
                }
                if case == "abort-during" {
                    driver.request_abort(id).unwrap();
                }
                release.send(()).unwrap();
                if case == "abort-after-receipt" {
                    receipt_entered_rx
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .expect("receipt was not committed");
                    driver.request_abort(id).unwrap();
                    receipt_release.send(()).unwrap();
                }
            }
            let task = wait_terminal(&driver, id);
            assert!(
                matches!(task.state(), crate::task::TaskState::Failed(reason) if reason == "restart limit"),
                "{case}: {:?}",
                task.state()
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                usize::from(!matches!(case, "abort-before" | "abort-after-drain-mark")),
                "{case}"
            );
            driver.shutdown().unwrap();
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let result = read_restart_after_result(&store, id).unwrap();
            assert_eq!(result.is_some(), case == "control", "{case}");
            if case != "control" {
                assert!(
                    store
                        .get(format!("durable/replay-safe-async/{id}/restart-after").as_bytes())
                        .unwrap()
                        .is_none(),
                    "{case}"
                );
            }
        }
    }

    struct RestartCleanupHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for RestartCleanupHook {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if !input
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("restart_limit"))
            {
                return Err("missing restart diagnostic".into());
            }
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("restart-hook-count"))
                .map_err(|e| e.to_string())?;
            writeln!(f, "hook").map_err(|e| e.to_string())?;
            f.sync_all().map_err(|e| e.to_string())?;
            if self.0.join("restart-memo").exists() {
                let _ = ctx.memo.get_or_insert("restart-write", json!(true)).await;
            }
            if self.0.join("restart-pause").exists() {
                std::fs::write(self.0.join("restart-hook-ready"), b"ready")
                    .map_err(|e| e.to_string())?;
                std::future::pending::<()>().await;
            }
            let mut out = input.clone();
            out.output = vec![json!({"type":"text","text":"restart after hook"})];
            Ok(Some(out))
        }
    }
    fn restart_cleanup_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        restart_cleanup_host_identity(root, "restart-v1")
    }
    fn restart_cleanup_host_identity(
        root: &std::path::Path,
        hook_identity: &str,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "restart".into(),
                    hooks: vec![Arc::new(RestartCleanupHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    hook_identity.into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(CrashUncertainTool(root.to_owned())),
                "uncertain-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["restart".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, tool)
    }
    fn restart_kill_child(root: &std::path::Path) {
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::restart_after_process_fixture")
            .env("PI_RS_RESTART_FIXTURE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("uncertain-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
    }
    #[test]
    fn restart_after_process_fixture() {
        let Ok(root) = std::env::var("PI_RS_RESTART_FIXTURE") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let (host, tool) = restart_cleanup_host(&root);
        let id = host.start_replay_safe_root(&tool, json!({}), 1).unwrap();
        std::fs::write(root.join("task-id"), id.to_string()).unwrap();
        let _driver = host.into_driver();
        loop {
            std::thread::park();
        }
    }
    #[test]
    fn restart_after_retains_failed_state_and_unknown_effects() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        restart_kill_child(root);
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = restart_cleanup_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            read_replay_safe_after_evidence(&store, id)
                .unwrap()
                .is_none()
        );
        let Some(DurableTaskResult::Rich {
            task_content_v1: result,
        }) = read_restart_after_result(&store, id).unwrap()
        else {
            panic!("missing restart result")
        };
        assert!(
            result.diagnostics.iter().any(
                |d| d.code.as_deref() == Some("restart_limit") && d.message.contains("unknown")
            )
        );
        assert_eq!(result.extra["isError"], json!(true));
        assert_eq!(
            std::fs::read_to_string(root.join("uncertain-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("restart-hook-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn restart_after_resume_fixture() {
        let Ok(root) = std::env::var("PI_RS_RESTART_RESUME_FIXTURE") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let (host, _) = restart_cleanup_host(&root);
        let driver = host.into_driver();
        if root.join("restart-pause").exists() {
            wait_file(&root.join("restart-hook-ready"));
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                matches!(driver.get(id).unwrap().unwrap().state(), crate::task::TaskState::CompletingFailed(reason) if reason=="restart limit")
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
            assert_eq!(
                std::fs::read_to_string(root.join("restart-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            std::fs::write(root.join("failed-state-verified"), b"yes").unwrap();
        }
        loop {
            std::thread::park();
        }
    }
    #[test]
    fn restart_after_kill_during_hook_or_after_receipt_is_bounded() {
        for receipt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            restart_kill_child(root);
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            if !receipt {
                std::fs::write(root.join("restart-pause"), b"yes").unwrap();
            }
            let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
            cmd.arg("--exact")
                .arg("async_task_tool::tests::restart_after_resume_fixture")
                .env("PI_RS_RESTART_RESUME_FIXTURE", root);
            if receipt {
                cmd.env("PI_TEST_RESTART_AFTER_RECEIPT", root);
            }
            let mut process = cmd.spawn().unwrap();
            wait_file(&root.join(if receipt {
                "restart-receipt"
            } else {
                "restart-hook-ready"
            }));
            if !receipt {
                wait_file(&root.join("failed-state-verified"));
            }
            process.kill().unwrap();
            process.wait().unwrap();
            let (host, _) = restart_cleanup_host(root);
            let driver = host.into_driver();
            assert!(
                matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
            );
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_restart_after_result(&store, id).unwrap().is_some(),
                receipt
            );
            assert!(
                read_replay_safe_after_evidence(&store, id)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                std::fs::read_to_string(root.join("restart-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(root.join("uncertain-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn restart_after_rejects_memo_access() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("restart-memo"), b"yes").unwrap();
        restart_kill_child(root);
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = restart_cleanup_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_restart_after_result(&store, id).unwrap().is_none());
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/restart-write").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    fn saved_restart_receipt(root: &std::path::Path) -> Uuid {
        restart_kill_child(root);
        let id = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::restart_after_resume_fixture")
            .env("PI_RS_RESTART_RESUME_FIXTURE", root)
            .env("PI_TEST_RESTART_AFTER_RECEIPT", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("restart-receipt"));
        process.kill().unwrap();
        process.wait().unwrap();
        id
    }
    #[test]
    fn restart_after_receipt_recovery_checks_concurrent_evidence_changes() {
        for (intent, replace) in [(false, false), (false, true), (true, false), (true, true)] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let id = saved_restart_receipt(root);
            let (host, _) = restart_cleanup_host(root);
            let mut scheduler = host.into_scheduler_for_test();
            let fired = Arc::new(AtomicUsize::new(0));
            let counter = fired.clone();
            scheduler.set_reservation_commit_hook(Arc::new(move |_, store| {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    let receipt_key =
                        format!("durable/replay-safe-async/{id}/restart-after").into_bytes();
                    let key = if intent {
                        let entry = store.get(&receipt_key).unwrap().unwrap();
                        let value: serde_json::Value =
                            serde_json::from_slice(&entry.value).unwrap();
                        format!(
                            "durable/replay-safe-async/{id}/restart-intent/{}",
                            value["intent"]["attempt"].as_str().unwrap()
                        )
                        .into_bytes()
                    } else {
                        receipt_key
                    };
                    let mutation = if replace {
                        durable_store::Mutation::Put {
                            key,
                            value: b"{}".to_vec(),
                        }
                    } else {
                        durable_store::Mutation::Delete { key }
                    };
                    store.commit(&[], &[mutation]).unwrap();
                }
            }));
            let driver = crate::task_driver::Driver::start(scheduler);
            assert!(
                matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
            );
            let _ = driver.shutdown_with_pending().unwrap();
            assert!(fired.load(Ordering::SeqCst) > 0);
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert!(read_restart_after_result(&store, id).unwrap().is_none());
        }
    }
    #[test]
    fn restart_after_receipt_recovery_requires_matching_registration() {
        for changed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let id = saved_restart_receipt(root);
            let host = if changed {
                restart_cleanup_host_identity(root, "changed-v2").0
            } else {
                StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap()
            };
            let driver = host.into_driver();
            assert!(
                matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
            );
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert!(read_restart_after_result(&store, id).unwrap().is_none());
        }
    }
    #[test]
    fn restart_after_rejects_stale_lease_on_driver() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        restart_kill_child(root);
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::restart_after_resume_fixture")
            .env("PI_RS_RESTART_RESUME_FIXTURE", root)
            .env("PI_TEST_RESTART_STALE_LEASE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("restart-stale-rejected"));
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap(); // fixture keeps the driver alive after settlement
        process.kill().unwrap();
        process.wait().unwrap();
        let (host, _) = restart_cleanup_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn restart_after_opt_in_does_not_enable_abort_after() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "restart".into(),
                    hooks: vec![Arc::new(RestartCleanupHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "restart-v1".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(AbortResultTool(root.to_owned())),
                "tool-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["restart".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let driver = host.into_driver();
        wait_file(&root.join("tool-start"));
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("restart-hook-count").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_aborted_after_result(&store, id).unwrap().is_none());
        assert!(read_restart_after_result(&store, id).unwrap().is_none());
    }
    #[test]
    fn restart_after_invalid_replacement_blocks_later_hook() {
        const ENV: &str = "PI_RS_RESTART_INVALID_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool, _) = restart_invalid_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::restart_after_invalid_replacement_blocks_later_hook")
            .env(ENV, root)
            .spawn()
            .unwrap();
        wait_file(&root.join("uncertain-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _, calls) = restart_invalid_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_restart_after_result(&store, id).unwrap().is_none());
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/must-not-run").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    fn restart_invalid_host(
        root: &std::path::Path,
    ) -> (
        StandaloneBeforeHookHost,
        Arc<crate::task_tool::TaskTool>,
        Arc<AtomicUsize>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = crate::extension::Registry::default();
        for (name, hook) in [
            (
                "first",
                Arc::new(SuppressAbortDiagnostic) as Arc<dyn crate::extension::Hook>,
            ),
            (
                "second",
                Arc::new(LateContentMemo(calls.clone())) as Arc<dyn crate::extension::Hook>,
            ),
        ] {
            registry
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![hook],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{name}-v1"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(CrashUncertainTool(root.to_owned())),
                "uncertain-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["first".into(), "second".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, tool, calls)
    }
    struct AbortParkedChild(Option<std::path::PathBuf>);
    #[async_trait::async_trait]
    impl Tool for AbortParkedChild {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "slow-parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for AbortParkedChild {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            _: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            if let Some(root) = &self.0 {
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(root.join("slow-attempts"))
                    .map_err(|e| e.to_string())?;
                writeln!(file, "attempt").map_err(|e| e.to_string())?;
                file.sync_all().map_err(|e| e.to_string())?;
            }
            let child = host
                .start_child("grand", "abort-result-tool", json!({}))
                .await?;
            if let Some(root) = &self.0 {
                std::fs::write(root.join("slow-staged"), b"yes").map_err(|e| e.to_string())?;
                while !root.join("parent-woken").exists() {
                    tokio::select! {
                        _ = context.cancellation.cancelled() => return Err("cancelled".into()),
                        _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => (),
                    }
                }
            }
            Ok(DurableToolStep::Park {
                checkpoint: json!({}),
                wait: DurableWait::All(vec![child]),
            })
        }
    }
    struct RestartParentWithChild(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for RestartParentWithChild {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "restart-parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for RestartParentWithChild {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            if resume.checkpoint.is_none() {
                let slow = host.start_child("child", "slow-parent", json!({})).await?;
                let fast = host.start_child("fast", "immediate", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"phase":1}),
                    wait: DurableWait::Any(vec![slow, fast]),
                })
            } else {
                std::fs::write(self.0.join("parent-woken"), b"yes").map_err(|e| e.to_string())?;
                if self.0.join("force-early-wake").exists() {
                    context.cancellation.cancelled().await;
                    return Err("cancelled".into());
                }
                std::future::pending().await
            }
        }
    }
    // Ensure the Any wake happens only after the grandchild has started.
    // This makes the intended child-first cleanup state independent of dispatch order.
    struct RestartGatedFast(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for RestartGatedFast {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "immediate".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for RestartGatedFast {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            let gate = if self.0.join("force-early-wake").exists() {
                "slow-staged"
            } else {
                "tool-start"
            };
            while !self.0.join(gate).exists() {
                tokio::select! {
                    _ = context.cancellation.cancelled() => return Err("cancelled".into()),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => (),
                }
            }
            Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
        }
    }
    fn restart_child_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "order".into(),
                    hooks: vec![Arc::new(CleanupOrder(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "order-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let snapshot = registry.snapshot();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let grand = host
            .register_replay_safe_durable_tool_with_content_hooks(
                Arc::new(AbortResultTool(root.to_owned())),
                "grand-v1",
                1,
                vec![],
                &snapshot,
                &["order".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let child_tool: Arc<dyn DurableAsyncTool> = Arc::new(AbortParkedChild(
            root.join("force-early-wake")
                .exists()
                .then(|| root.to_owned()),
        ));
        let child = if root.join("force-unsafe-child").exists() {
            host.register_unsafe_durable_tool_with_hook(
                child_tool,
                "child-v1",
                1,
                vec![grand],
                "unsafe-child-hook",
                Arc::new(NoopDurableHook),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
        } else {
            host.register_replay_safe_durable_tool_with_abort_after(
                child_tool,
                "child-v1",
                1,
                vec![grand],
                &snapshot,
                &["order".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
        }
        .unwrap();
        let fast = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(RestartGatedFast(root.to_owned())),
                "fast-v1",
                1,
                vec![],
                "fast-hook",
                Arc::new(NoopDurableHook),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let parent = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(RestartParentWithChild(root.to_owned())),
                "parent-v1",
                1,
                vec![child, fast],
                &snapshot,
                &["order".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, parent)
    }
    #[test]
    fn any_wake_during_child_admission_keeps_descendant_progressing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("force-early-wake"), b"yes").unwrap();
        let (host, tool) = restart_child_host(root);
        let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let driver = crate::task_driver::Driver::start(scheduler);
        wait_file(&root.join("slow-staged"));
        wait_file(&root.join("parent-woken"));
        wait_file(&root.join("tool-start"));
        assert!(
            std::fs::read_to_string(root.join("slow-attempts"))
                .unwrap()
                .lines()
                .count()
                >= 2,
            "conflicting child transition must be retried"
        );
        let tasks = crate::task_store::TaskStore::new(owner.store())
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 4, "the stale child admission must not commit");
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        driver.shutdown().unwrap();
    }
    #[test]
    fn any_wake_with_unsafe_child_conflict_requires_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("force-early-wake"), b"yes").unwrap();
        std::fs::write(root.join("force-unsafe-child"), b"yes").unwrap();
        let (host, tool) = restart_child_host(root);
        let id = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let driver = crate::task_driver::Driver::start(scheduler);
        wait_file(&root.join("slow-staged"));
        wait_file(&root.join("parent-woken"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let child = loop {
            let tasks = crate::task_store::TaskStore::new(owner.store())
                .list()
                .unwrap();
            if let Some(child) = tasks.iter().find(|item| {
                item.task.owner() == &TaskOwner::Task(id)
                    && !item.task.replay_safe()
                    && item.task.is_terminal()
            }) {
                assert_eq!(
                    tasks.len(),
                    3,
                    "stale unsafe child admission must not commit"
                );
                break child.task.clone();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "unsafe child did not settle"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert!(
            matches!(child.state(), crate::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert_eq!(
            std::fs::read_to_string(root.join("slow-attempts"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(!root.join("tool-start").exists());
        assert_eq!(
            read_unsafe_durable_park_evidence(owner.store(), child.id())
                .unwrap()
                .len(),
            1
        );
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        driver.shutdown().unwrap();
    }
    #[test]
    fn restart_after_waits_for_owned_child_abort_cleanup() {
        const ENV: &str = "PI_RS_RESTART_CHILD_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = restart_child_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        struct KillChild(std::process::Child);
        impl Drop for KillChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut process = KillChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::restart_after_waits_for_owned_child_abort_cleanup")
                .env(ENV, root)
                .spawn()
                .unwrap(),
        );
        wait_file(&root.join("tool-start"));
        wait_file(&root.join("parent-woken"));
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = restart_child_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("order"))
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec!["slow-parent", "restart-parent"]
        );
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_restart_after_result(&store, id).unwrap().is_some());
        assert!(
            read_replay_safe_after_evidence(&store, id)
                .unwrap()
                .is_none()
        );
    }
    struct InterruptedUnsafeRestartChild(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for InterruptedUnsafeRestartChild {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "slow-parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for InterruptedUnsafeRestartChild {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("unsafe-effects"))
                .map_err(|e| e.to_string())?;
            writeln!(file, "effect").map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            std::fs::write(self.0.join("tool-start"), b"ready").map_err(|e| e.to_string())?;
            std::future::pending().await
        }
    }
    fn restart_unsafe_child_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "restart".into(),
                    hooks: vec![Arc::new(RestartCleanupHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "restart-v1".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        // Use the real unsafe registration on the same scheduler as the hook-enabled parent.
        let child = replay_safe::register_unsafe(
            &mut host.scheduler,
            Arc::new(InterruptedUnsafeRestartChild(root.to_owned())),
            "interrupted-unsafe-child-v1",
            1,
            vec![],
            None,
        )
        .unwrap();
        let fast = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
                "fast-v1",
                1,
                vec![],
                "fast-hook",
                Arc::new(NoopDurableHook),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let parent = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(RestartParentWithChild(root.to_owned())),
                "parent-v1",
                1,
                vec![child, fast],
                &registry.snapshot(),
                &["restart".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, parent)
    }
    #[test]
    fn restart_cleanup_abort_after_receipt_preserves_unsafe_descendant_evidence() {
        use crate::task_store::TaskStore;
        const ENV: &str = "PI_RS_RESTART_UNSAFE_CHILD_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, parent) = restart_unsafe_child_host(&root);
            let id = host.start_replay_safe_root(&parent, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::restart_cleanup_abort_after_receipt_preserves_unsafe_descendant_evidence")
            .env(ENV, root).spawn().unwrap();
        wait_file(&root.join("tool-start"));
        wait_file(&root.join("parent-woken"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = restart_unsafe_child_host(root);
        let scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let tasks = TaskStore::new(owner.store());
        let child = tasks
            .list()
            .unwrap()
            .into_iter()
            .find(|item| {
                item.task.owner() == &crate::task::TaskOwner::Task(id) && !item.task.replay_safe()
            })
            .expect("unsafe child was not admitted")
            .task
            .id();
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        replay_safe::after_restart_receipt(id, move || {
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let driver = crate::task_driver::Driver::start(scheduler);
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("restart receipt was not committed");
        assert!(
            owner
                .store()
                .get(format!("durable/replay-safe-async/{id}/restart-after").as_bytes())
                .unwrap()
                .is_some()
        );
        let unsafe_task = tasks.load(child).unwrap().unwrap().task;
        assert!(matches!(
            unsafe_task.state(),
            crate::task::TaskState::Failed(_) | crate::task::TaskState::Cancelled
        ));
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        let task = wait_terminal(&driver, id);
        assert!(
            matches!(task.state(), crate::task::TaskState::Failed(reason) if reason == "restart limit")
        );
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_restart_after_result(&store, id).unwrap().is_none());
        let intents = store
            .list(format!("durable/unsafe-park-async/{child}/").as_bytes())
            .unwrap();
        let intent_records: Vec<_> = intents
            .iter()
            .filter(|(key, _)| key.ends_with(b"/intent"))
            .collect();
        assert_eq!(intent_records.len(), 1);
        let intent_task: crate::task::Task =
            serde_json::from_slice(&intent_records[0].1.value).unwrap();
        assert_eq!(intent_task.id(), child);
        assert!(matches!(
            intent_task.state(),
            crate::task::TaskState::Running
        ));
        assert!(!intent_task.replay_safe());
        assert!(
            read_unsafe_durable_receipts(&store, child)
                .unwrap()
                .is_empty()
        );
        assert!(
            read_unsafe_durable_park_evidence(&store, child)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            std::fs::read_to_string(root.join("unsafe-effects"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("restart-hook-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    fn cancel_combined_host(
        root: &std::path::Path,
        sealed: Arc<dyn Tool>,
    ) -> (
        StandaloneBeforeHookHost,
        Vec<Arc<crate::task_tool::TaskTool>>,
    ) {
        let cell = Arc::new(std::sync::OnceLock::new());
        let leaf: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        assert!(registry.register_shared("park-cancel-leaf", true, leaf.clone()));
        let marker = root.join("child-aborted");
        assert!(registry.set_abort("park-cancel-leaf", move |_: &Task| {
            std::fs::write(&marker, b"yes").map_err(|e| e.to_string())?;
            Ok(crate::task_scheduler::Step::Succeed(json!(null)))
        }));
        let mut extensions = crate::extension::Registry::default();
        for (name, hook) in [
            (
                "abort",
                Arc::new(AbortResultHook(root.to_owned())) as Arc<dyn crate::extension::Hook>,
            ),
            (
                "restart",
                Arc::new(RestartCleanupHook(root.to_owned())) as Arc<dyn crate::extension::Hook>,
            ),
        ] {
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![hook],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{name}-v1"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut host = StandaloneBeforeHookHost::open(root.join("db"), registry).unwrap();
        let sealed_adapter = host
            .register_tool_with_interrupted_after(
                "leaf",
                sealed,
                "backend",
                1,
                "interrupted-v1",
                Arc::new(InterruptedAfterFixture(root.to_owned())),
                InterruptedAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        assert!(cell.set(sealed_adapter).is_ok());
        let child = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "park-cancel-leaf",
            true,
            1,
            leaf,
        ));
        let parked = host
            .register_replay_safe_durable_tool_with_park_cancel(
                Arc::new(ChainTool {
                    name: "root",
                    child: "leaf",
                    calls: Arc::new(AtomicUsize::new(0)),
                    any: false,
                }),
                "park-v1",
                1,
                vec![child.clone()],
                &extensions.snapshot(),
                &["abort".into()],
                "cancel-v1",
                Arc::new(ParkCancelFixture(root.to_owned())),
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let retryable = host
            .register_replay_safe_durable_tool_with_retryable_park_cancel(
                Arc::new(ChainTool {
                    name: "root",
                    child: "leaf",
                    calls: Arc::new(AtomicUsize::new(0)),
                    any: false,
                }),
                "park-v1",
                1,
                vec![child.clone()],
                &extensions.snapshot(),
                &["abort".into()],
                "cancel-v1",
                Arc::new(ParkCancelFixture(root.to_owned())),
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
                ParkCancelRetryAuthorization::host_attests_cancel_replay_safe(),
            )
            .unwrap();
        let abort_only = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(ChainTool {
                    name: "root",
                    child: "leaf",
                    calls: Arc::new(AtomicUsize::new(0)),
                    any: false,
                }),
                "park-v1",
                1,
                vec![child],
                &extensions.snapshot(),
                &["abort".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let unsafe_tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
                "unsafe-v1",
                1,
                vec![],
                "unsafe-hook-v1",
                Arc::new(UnsafePolicyHook),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let restart = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(CrashUncertainTool(root.to_owned())),
                "restart-v1",
                1,
                vec![],
                &extensions.snapshot(),
                &["restart".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (
            host,
            vec![parked, unsafe_tool, restart, retryable, abort_only],
        )
    }

    #[test]
    fn cancel_unsafe_hook_restart_and_interrupted_coexist_after_process_loss() {
        const ENV: &str = "PI_CANCEL_COMBINED_RECOVERY_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let sealed: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.clone()));
            let binding =
                crate::nested_identity::register(&sealed, Some("backend".into())).unwrap();
            let (host, tools) = cancel_combined_host(&root, sealed);
            let parked = host
                .start_replay_safe_root(&tools[0], json!({"token":"prepared"}), 4)
                .unwrap();
            let unsafe_id = host
                .start_unsafe_durable_root(&tools[1], json!({}), 4)
                .unwrap();
            let restart = host
                .start_replay_safe_root(&tools[2], json!({}), 1)
                .unwrap();
            let scheduler = host.into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_park_cancel(&driver, parked);
            assert!(matches!(
                wait_terminal(&driver, unsafe_id).state(),
                crate::task::TaskState::Succeeded(_)
            ));
            wait_file(&root.join("uncertain-ready"));
            wait_file(&root.join("unsafe-ready"));
            let task = driver.get(parent.id()).unwrap().unwrap();
            let child: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data.clone()).unwrap();
            std::fs::write(
                root.join("combined-ids"),
                serde_json::to_vec(&vec![
                    parked,
                    unsafe_id,
                    restart,
                    parent.id(),
                    child.task_id(),
                ])
                .unwrap(),
            )
            .unwrap();
            loop {
                std::hint::black_box((&driver, &binding));
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        struct KillChild(std::process::Child);
        impl Drop for KillChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut process = KillChild(std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_tool::tests::cancel_unsafe_hook_restart_and_interrupted_coexist_after_process_loss")
            .env(ENV, root).spawn().unwrap());
        wait_file(&root.join("combined-ids"));
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let ids: Vec<Uuid> =
            serde_json::from_slice(&std::fs::read(root.join("combined-ids")).unwrap()).unwrap();
        let sealed: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&sealed, Some("backend".into())).unwrap();
        let (host, tools) = cancel_combined_host(root, sealed);
        let kinds: std::collections::HashSet<_> = tools
            .iter()
            .map(|tool| tool.identity().unwrap().kind)
            .collect();
        assert_eq!(kinds.len(), 5);
        let driver = host.into_driver();
        driver.request_abort(ids[0]).unwrap();
        assert!(matches!(
            wait_terminal(&driver, ids[0]).state(),
            crate::task::TaskState::Cancelled
        ));
        assert!(
            matches!(wait_terminal(&driver, ids[2]).state(), crate::task::TaskState::Failed(reason) if reason == "restart limit")
        );
        assert!(matches!(
            wait_terminal(&driver, ids[3]).state(),
            crate::task::TaskState::Failed(_)
        ));
        let unsafe_task = driver.get(ids[1]).unwrap().unwrap();
        assert!(
            matches!(unsafe_task.state(), crate::task::TaskState::Succeeded(value) if serde_json::from_value::<Outcome>(value.clone()).unwrap() == Outcome::Succeeded("unsafe-rewritten".into()))
        );
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, ids[0]).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert!(read_aborted_after_result(&store, ids[0]).unwrap().is_some());
        assert!(read_restart_after_result(&store, ids[2]).unwrap().is_some());
        assert!(
            read_interrupted_after_result(&store, ids[4])
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            read_sealed_outcome_evidence(&store, ids[4]).unwrap(),
            Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
        ));
        assert_eq!(
            read_unsafe_durable_receipts(&store, ids[1]).unwrap().len(),
            1
        );
        for id in [ids[1], ids[2], ids[4]] {
            assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
        }
        for name in [
            "cancel-count",
            "abort-hook-count",
            "restart-hook-count",
            "interrupted-hook-count",
            "unsafe-invocations",
            "uncertain-invocations",
        ] {
            assert_eq!(
                std::fs::read_to_string(root.join(name))
                    .unwrap()
                    .lines()
                    .count(),
                1,
                "{name}"
            );
        }
    }

    fn conversation_candidate_combined_host(
        root: &std::path::Path,
        sealed: Arc<dyn Tool>,
        candidate_calls: Arc<AtomicUsize>,
    ) -> (
        StandaloneBeforeHookHost,
        Arc<crate::task_tool::TaskTool>,
        Arc<crate::task_tool::TaskTool>,
        Arc<crate::task_tool::TaskTool>,
        String,
    ) {
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        let leaf: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        assert!(registry.register_shared("cross-cancel-leaf", true, leaf.clone()));
        let marker = root.join("child-aborted");
        assert!(registry.set_abort("cross-cancel-leaf", move |_: &Task| {
            std::fs::write(&marker, b"yes").map_err(|e| e.to_string())?;
            Ok(crate::task_scheduler::Step::Succeed(json!(null)))
        }));
        let candidate = register_conversation_replay_safe_durable_tool(
            &mut registry,
            Arc::new(ImmediateDurable(candidate_calls)),
            "shared-tool-v1",
            1,
            vec![],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
        let mut host = StandaloneBeforeHookHost::open(root.join("db"), registry).unwrap();
        let unsafe_tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
                "shared-tool-v1",
                1,
                vec![],
                "unsafe-hook-v1",
                Arc::new(UnsafePolicyHook),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "cancel".into(),
                    hooks: vec![Arc::new(AbortResultHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "cross-cancel-v1".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let child = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "cross-cancel-leaf",
            true,
            1,
            leaf,
        ));
        let cancel_tool = host
            .register_replay_safe_durable_tool_with_park_cancel(
                Arc::new(ChainTool {
                    name: "root",
                    child: "leaf",
                    calls: Arc::new(AtomicUsize::new(0)),
                    any: false,
                }),
                "cross-cancel-v1",
                1,
                vec![child],
                &extensions.snapshot(),
                &["cancel".into()],
                "cancel-v1",
                Arc::new(ParkCancelFixture(root.to_owned())),
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let sealed_adapter = host
            .register_tool_with_interrupted_after(
                "leaf",
                sealed,
                "backend",
                1,
                "interrupted-v1",
                Arc::new(InterruptedAfterFixture(root.to_owned())),
                InterruptedAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let sealed_kind = sealed_adapter.adapter.identity().unwrap().kind;
        assert!(cell.set(sealed_adapter).is_ok());
        (host, candidate, unsafe_tool, cancel_tool, sealed_kind)
    }

    fn assert_conversation_candidate_disabled(
        candidate: &crate::task_tool::TaskTool,
        calls: &AtomicUsize,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let outcome = runtime.block_on(candidate.invoke(
            json!({}),
            ToolContext {
                operation_id: Uuid::new_v4(),
                cancellation: pi_rs_core::CancellationToken::new(),
            },
        ));
        assert!(matches!(outcome, Outcome::Skipped(reason) if reason.contains("not enabled")));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn conversation_candidate_unsafe_hook_and_interrupted_cleanup_recover_together() {
        const ENV: &str = "PI_CONVERSATION_CANDIDATE_COMBINED_1120";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let sealed: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.clone()));
            let binding =
                crate::nested_identity::register(&sealed, Some("backend".into())).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let (host, candidate, unsafe_tool, cancel_tool, sealed_kind) =
                conversation_candidate_combined_host(&root, sealed, calls.clone());
            assert_conversation_candidate_disabled(&candidate, &calls);
            let kinds = vec![
                candidate.identity().unwrap().kind,
                unsafe_tool.identity().unwrap().kind,
                cancel_tool.identity().unwrap().kind,
                sealed_kind,
            ];
            assert_eq!(
                kinds.iter().collect::<std::collections::HashSet<_>>().len(),
                4
            );
            let cancel_id = host
                .start_replay_safe_root(&cancel_tool, json!({"token":"prepared"}), 4)
                .unwrap();
            let unsafe_id = host
                .start_unsafe_durable_root(&unsafe_tool, json!({}), 4)
                .unwrap();
            let scheduler = host.into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            assert!(matches!(
                wait_terminal(&driver, unsafe_id).state(),
                crate::task::TaskState::Succeeded(_)
            ));
            wait_park_cancel(&driver, cancel_id);
            wait_file(&root.join("unsafe-ready"));
            let task = driver.get(parent.id()).unwrap().unwrap();
            let child: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data.clone()).unwrap();
            std::fs::write(
                root.join("combined-ids"),
                serde_json::to_vec(&(unsafe_id, parent.id(), child.task_id(), cancel_id, kinds))
                    .unwrap(),
            )
            .unwrap();
            loop {
                std::hint::black_box((&driver, &binding));
                std::thread::park();
            }
        }
        struct KillChild(std::process::Child);
        impl Drop for KillChild {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = KillChild(std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_tool::tests::conversation_candidate_unsafe_hook_and_interrupted_cleanup_recover_together")
            .env(ENV, root).spawn().unwrap());
        wait_file(&root.join("combined-ids"));
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let (unsafe_id, parent, child, cancel_id, old_kinds): (
            Uuid,
            Uuid,
            Uuid,
            Uuid,
            Vec<String>,
        ) = serde_json::from_slice(&std::fs::read(root.join("combined-ids")).unwrap()).unwrap();
        let sealed: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&sealed, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, candidate, unsafe_tool, cancel_tool, sealed_kind) =
            conversation_candidate_combined_host(root, sealed, calls.clone());
        assert_eq!(
            old_kinds,
            vec![
                candidate.identity().unwrap().kind,
                unsafe_tool.identity().unwrap().kind,
                cancel_tool.identity().unwrap().kind,
                sealed_kind
            ]
        );
        assert_conversation_candidate_disabled(&candidate, &calls);
        let driver = host.into_driver();
        driver.request_abort(cancel_id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, cancel_id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert!(matches!(
            wait_terminal(&driver, parent).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert!(
            matches!(driver.get(unsafe_id).unwrap().unwrap().state(), crate::task::TaskState::Succeeded(value) if serde_json::from_value::<Outcome>(value.clone()).unwrap() == Outcome::Succeeded("unsafe-rewritten".into()))
        );
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            crate::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .iter()
                .all(|record| record.task.kind() != Some(old_kinds[0].as_str()))
        );
        assert_eq!(
            read_park_cancel_evidence(&store, cancel_id).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert!(
            read_aborted_after_result(&store, cancel_id)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            read_sealed_outcome_evidence(&store, child).unwrap(),
            Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
        ));
        assert!(
            read_interrupted_after_result(&store, child)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            read_unsafe_durable_receipts(&store, unsafe_id)
                .unwrap()
                .len(),
            1
        );
        for name in [
            "unsafe-invocations",
            "interrupted-hook-count",
            "cancel-count",
            "abort-hook-count",
        ] {
            assert_eq!(
                std::fs::read_to_string(root.join(name))
                    .unwrap()
                    .lines()
                    .count(),
                1,
                "{name}"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    struct UnsafePolicyHook;
    #[async_trait::async_trait]
    impl crate::extension::Hook for UnsafePolicyHook {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            Ok(Some(json!("unsafe-rewritten")))
        }
    }
    fn combined_policy_host(
        root: &std::path::Path,
    ) -> (
        StandaloneBeforeHookHost,
        Vec<Arc<crate::task_tool::TaskTool>>,
    ) {
        let mut extensions = crate::extension::Registry::default();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hooks: Vec<(&str, Arc<dyn crate::extension::Hook>)> = vec![
            (
                "rich-first",
                Arc::new(CombinedHooks {
                    stage: 1,
                    events: events.clone(),
                }),
            ),
            ("rich-second", Arc::new(CombinedHooks { stage: 2, events })),
            ("abort", Arc::new(AbortResultHook(root.to_owned()))),
            ("restart", Arc::new(RestartCleanupHook(root.to_owned()))),
        ];
        for (name, hook) in hooks {
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![hook],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{name}-v1"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let snapshot = extensions.snapshot();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let unsafe_tool = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
                "shared-tool-v1",
                1,
                vec![],
                "unsafe-hook-v1",
                Arc::new(UnsafePolicyHook),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let rich = host
            .register_replay_safe_durable_tool_with_content_hooks(
                Arc::new(ImmediateDurable(Arc::new(AtomicUsize::new(0)))),
                "shared-tool-v1",
                1,
                vec![],
                &snapshot,
                &["rich-first".into(), "rich-second".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let abort = host
            .register_replay_safe_durable_tool_with_abort_after(
                Arc::new(AbortResultTool(root.to_owned())),
                "abort-v1",
                1,
                vec![],
                &snapshot,
                &["abort".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        let restart = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(CrashUncertainTool(root.to_owned())),
                "restart-v1",
                1,
                vec![],
                &snapshot,
                &["restart".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, vec![unsafe_tool, rich, abort, restart])
    }
    #[test]
    fn unsafe_hook_rich_abort_and_restart_coexist_across_recovery() {
        const ENV: &str = "PI_RS_COMBINED_UNSAFE_HOOK_POLICIES";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tools) = combined_policy_host(&root);
            let ids = vec![
                host.start_unsafe_durable_root(&tools[0], json!({}), 4)
                    .unwrap(),
                host.start_replay_safe_root(&tools[1], json!({}), 4)
                    .unwrap(),
                host.start_replay_safe_root(&tools[2], json!({}), 4)
                    .unwrap(),
                host.start_replay_safe_root(&tools[3], json!({}), 1)
                    .unwrap(),
            ];
            std::fs::write(root.join("policy-ids"), serde_json::to_vec(&ids).unwrap()).unwrap();
            let driver = host.into_driver();
            for id in &ids[..2] {
                assert!(matches!(
                    wait_terminal(&driver, *id).state(),
                    crate::task::TaskState::Succeeded(_)
                ));
            }
            wait_file(&root.join("tool-start"));
            wait_file(&root.join("uncertain-ready"));
            driver.request_abort(ids[2]).unwrap();
            assert!(matches!(
                wait_terminal(&driver, ids[2]).state(),
                crate::task::TaskState::Cancelled
            ));
            std::fs::write(root.join("policies-ready"), b"ready").unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_tool::tests::unsafe_hook_rich_abort_and_restart_coexist_across_recovery")
            .env(ENV, root).spawn().unwrap();
        wait_file(&root.join("policies-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let ids: Vec<Uuid> =
            serde_json::from_slice(&std::fs::read(root.join("policy-ids")).unwrap()).unwrap();
        let (host, _) = combined_policy_host(root);
        let driver = host.into_driver();
        let unsafe_task = driver.get(ids[0]).unwrap().unwrap();
        let rich_task = driver.get(ids[1]).unwrap().unwrap();
        let abort_task = driver.get(ids[2]).unwrap().unwrap();
        let restart_task = wait_terminal(&driver, ids[3]);
        let kinds: std::collections::HashSet<_> =
            [&unsafe_task, &rich_task, &abort_task, &restart_task]
                .iter()
                .map(|task| task.kind().unwrap().to_owned())
                .collect();
        assert_eq!(kinds.len(), 4);
        assert!(
            matches!(unsafe_task.state(), crate::task::TaskState::Succeeded(value) if serde_json::from_value::<Outcome>(value.clone()).unwrap() == Outcome::Succeeded("unsafe-rewritten".into()))
        );
        let crate::task::TaskState::Succeeded(rich_value) = rich_task.state() else {
            panic!("rich task failed")
        };
        let DurableTaskResult::Rich {
            task_content_v1: rich,
        } = serde_json::from_value(rich_value.clone()).unwrap()
        else {
            panic!("missing rich result")
        };
        assert_eq!(rich.output.len(), 2);
        assert_eq!(
            rich.extra.get("structuredOutput"),
            Some(&json!({"value":42}))
        );
        assert!(matches!(
            abort_task.state(),
            crate::task::TaskState::Cancelled
        ));
        assert!(
            matches!(restart_task.state(), crate::task::TaskState::Failed(reason) if reason == "restart limit")
        );
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let unsafe_receipts = read_unsafe_durable_receipts(&store, ids[0]).unwrap();
        assert_eq!(unsafe_receipts.len(), 1);
        assert_eq!(
            unsafe_receipts[0].outcome,
            Outcome::Succeeded("done".into())
        );
        for id in [ids[0], ids[1], ids[3]] {
            assert!(read_aborted_after_result(&store, id).unwrap().is_none());
        }
        for id in [ids[0], ids[1], ids[2]] {
            assert!(read_restart_after_result(&store, id).unwrap().is_none());
        }
        assert!(read_aborted_after_result(&store, ids[2]).unwrap().is_some());
        assert!(read_restart_after_result(&store, ids[3]).unwrap().is_some());
        assert_eq!(
            std::fs::read_to_string(root.join("abort-hook-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("restart-hook-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("uncertain-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    fn known_restart_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "pause".into(),
                    hooks: vec![Arc::new(ContentPause(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "pause-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(CountFileTool(root.to_owned())),
                "known-v1",
                1,
                vec![],
                &registry.snapshot(),
                &["pause".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, tool)
    }
    #[test]
    fn restart_after_does_not_replace_committed_tool_outcome() {
        const ENV: &str = "PI_RS_RESTART_KNOWN_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = known_restart_host(&root);
            let id = host.start_replay_safe_root(&tool, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::restart_after_does_not_replace_committed_tool_outcome")
            .env(ENV, root)
            .spawn()
            .unwrap();
        wait_file(&root.join("content-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = known_restart_host(root);
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_replay_safe_after_evidence(&store, id).unwrap(),
            Some(ReplaySafeAfterEvidence::ToolOutcome(Outcome::Succeeded(
                "raw".into()
            )))
        );
        assert!(read_restart_after_result(&store, id).unwrap().is_none());
        assert_eq!(
            std::fs::read_to_string(root.join("tool-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    struct UnavailableFixture {
        root: std::path::PathBuf,
        stage: usize,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for UnavailableFixture {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            std::fs::write(self.root.join("unexpected-before"), b"bad")
                .map_err(|e| e.to_string())?;
            Ok(crate::extension::BeforeTool::Pass)
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if !input
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("tool_unavailable"))
                || input.extra.get("isError") != Some(&json!(true))
            {
                return Err("missing unavailable error".into());
            }
            if self.stage == 1 {
                let value = ctx
                    .memo
                    .get_or_insert("unavailable", json!("first"))
                    .await?;
                if value != json!("first") {
                    return Err("memo changed".into());
                }
                if self.root.join("bad-unavailable").exists() {
                    let mut out = input.clone();
                    out.extra
                        .insert("structuredOutput".into(), json!({"bad":true}));
                    return Ok(Some(out));
                }
                if self.root.join("pause-unavailable").exists()
                    && !self.root.join("unavailable-ready").exists()
                {
                    std::fs::write(self.root.join("unavailable-ready"), b"ready")
                        .map_err(|e| e.to_string())?;
                    std::future::pending::<()>().await;
                }
                let mut out = input.clone();
                out.output = vec![
                    json!({"type":"text","text":"unavailable"}),
                    json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
                ];
                out.extra.insert("details".into(), json!({"stage":1}));
                Ok(Some(out))
            } else {
                if input.output.len() != 2
                    || input.extra.get("details") != Some(&json!({"stage":1}))
                    || ctx.memo.get("unavailable").await? != Some(json!("first"))
                {
                    return Err("previous result lost".into());
                }
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.root.join("unavailable-second"))
                    .map_err(|e| e.to_string())?;
                writeln!(file, "called").map_err(|e| e.to_string())?;
                file.sync_all().map_err(|e| e.to_string())?;
                Ok(None)
            }
        }
    }
    fn unavailable_host(
        root: &std::path::Path,
        identity: &str,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut registry = crate::extension::Registry::default();
        for (name, stage) in [("first", 1), ("second", 2)] {
            registry
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![Arc::new(UnavailableFixture {
                            root: root.to_owned(),
                            stage,
                        })],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(
                        1,
                        format!("{identity}-{name}"),
                        vec!["hook".into()],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_unavailable_tool_with_content_hooks(
                ToolSpec {
                    name: "missing".into(),
                    description: String::new(),
                    parameters: json!({"type":"object","required":["required"]}),
                },
                "missing-placeholder-v1",
                1,
                &registry.snapshot(),
                &["first".into(), "second".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                UnavailableAfterAuthorization::host_attests_unavailable_and_selection(),
            )
            .unwrap();
        (host, tool)
    }
    struct CombinedUnavailableRestartHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for CombinedUnavailableRestartHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            use std::io::Write;
            writeln!(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("combined-before"))
                    .map_err(|e| e.to_string())?,
                "called"
            )
            .map_err(|e| e.to_string())?;
            Ok(crate::extension::BeforeTool::Pass)
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let code = input
                .diagnostics
                .iter()
                .find_map(|d| d.code.as_deref())
                .ok_or("missing diagnostic")?;
            if !matches!(code, "tool_unavailable" | "restart_limit") {
                return Err("unexpected hook".into());
            }
            use std::io::Write;
            writeln!(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("combined-after"))
                    .map_err(|e| e.to_string())?,
                "{code}"
            )
            .map_err(|e| e.to_string())?;
            let mut result = input.clone();
            result.output = vec![json!({"type":"text", "text":format!("handled {code}")})];
            Ok(Some(result))
        }
    }
    fn combined_unavailable_restart_host(
        root: &std::path::Path,
    ) -> (
        StandaloneBeforeHookHost,
        Arc<crate::task_tool::TaskTool>,
        Arc<crate::task_tool::TaskTool>,
    ) {
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "combined".into(),
                    hooks: vec![Arc::new(CombinedUnavailableRestartHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "combined-hooks-v1".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let snapshot = extensions.snapshot();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let missing = host
            .register_unavailable_tool_with_content_hooks(
                CrashUncertainTool(root.to_owned()).spec(),
                "combined-v1",
                1,
                &snapshot,
                &["combined".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                UnavailableAfterAuthorization::host_attests_unavailable_and_selection(),
            )
            .unwrap();
        let restart = host
            .register_replay_safe_durable_tool_with_restart_after(
                Arc::new(CrashUncertainTool(root.to_owned())),
                "combined-v1",
                1,
                vec![],
                &snapshot,
                &["combined".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                RestartAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        (host, missing, restart)
    }
    struct CombinedUnsafePending(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for CombinedUnsafePending {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "unsafe-pending".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for CombinedUnsafePending {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Pending {
                reference: "remote-combined".into(),
            }))
        }
    }
    #[test]
    fn unavailable_restart_and_unsafe_registrations_coexist_on_real_drivers() {
        const ENV: &str = "PI_RS_COMBINED_UNAVAILABLE_RESTART_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, missing, restart) = combined_unavailable_restart_host(&root);
            let missing_id = host.start_replay_safe_root(&missing, json!({}), 3).unwrap();
            let restart_id = host.start_replay_safe_root(&restart, json!({}), 1).unwrap();
            std::fs::write(
                root.join("combined-ids"),
                format!("{missing_id}\n{restart_id}\n"),
            )
            .unwrap();
            let driver = host.into_driver();
            let task = wait_terminal(&driver, missing_id);
            assert!(matches!(task.state(), crate::task::TaskState::Succeeded(_)));
            std::fs::write(root.join("combined-unavailable-done"), b"done").unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_tool::tests::unavailable_restart_and_unsafe_registrations_coexist_on_real_drivers")
            .env(ENV, root).spawn().unwrap();
        wait_file(&root.join("combined-unavailable-done"));
        wait_file(&root.join("uncertain-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let ids: Vec<Uuid> = std::fs::read_to_string(root.join("combined-ids"))
            .unwrap()
            .lines()
            .map(|s| s.parse().unwrap())
            .collect();
        let (host, _, _) = combined_unavailable_restart_host(root);
        let hook_driver = host.into_driver();
        let missing = hook_driver.get(ids[0]).unwrap().unwrap();
        let failed = wait_terminal(&hook_driver, ids[1]);
        assert_ne!(
            missing.kind(),
            failed.kind(),
            "same spec and hook selection require distinct policy identities"
        );
        assert!(
            matches!(failed.state(), crate::task::TaskState::Failed(reason) if reason == "restart limit")
        );
        let crate::task::TaskState::Succeeded(value) = missing.state() else {
            panic!("unavailable did not finish")
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("unavailable result not rich")
        };
        assert_eq!(
            result.output,
            vec![json!({"type":"text","text":"handled tool_unavailable"})]
        );
        assert!(
            result
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("tool_unavailable"))
        );
        assert_eq!(result.extra.get("isError"), Some(&json!(true)));
        let unsafe_calls = Arc::new(AtomicUsize::new(0));
        let mut unsafe_host = StandaloneNoHooksHost::open(
            root.join("unsafe-db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let unsafe_tool = unsafe_host
            .register_unsafe_durable_tool(
                Arc::new(CombinedUnsafePending(unsafe_calls.clone())),
                "unsafe-v1",
                1,
                vec![],
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
            .unwrap();
        let unsafe_id = unsafe_host
            .start_unsafe_durable_root(&unsafe_tool, json!({}), 3)
            .unwrap();
        let unsafe_driver = unsafe_host.into_driver();
        let unsafe_task = wait_terminal(&unsafe_driver, unsafe_id);
        assert!(matches!(
            unsafe_task.state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(unsafe_calls.load(Ordering::SeqCst), 1);
        unsafe_driver.shutdown().unwrap();
        hook_driver.shutdown().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            store
                .list(format!("durable/replay-safe-async/{}/", ids[0]).as_bytes())
                .unwrap()
                .is_empty()
        );
        let Some(DurableTaskResult::Rich {
            task_content_v1: restart_result,
        }) = read_restart_after_result(&store, ids[1]).unwrap()
        else {
            panic!("missing restart cleanup receipt");
        };
        assert_eq!(
            restart_result.output,
            vec![json!({"type":"text", "text":"handled restart_limit"})]
        );
        assert!(
            restart_result
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("restart_limit"))
        );
        assert_eq!(
            std::fs::read_to_string(root.join("combined-before"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let after = std::fs::read_to_string(root.join("combined-after")).unwrap();
        assert_eq!(
            after
                .lines()
                .filter(|line| *line == "tool_unavailable")
                .count(),
            1
        );
        assert_eq!(
            after
                .lines()
                .filter(|line| *line == "restart_limit")
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("uncertain-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let unsafe_store = durable_store::Store::open(root.join("unsafe-db")).unwrap();
        let evidence = read_unsafe_durable_receipts(&unsafe_store, unsafe_id).unwrap();
        assert_eq!(evidence.len(), 1);
        assert_eq!(
            evidence[0].outcome,
            Outcome::Pending {
                reference: "remote-combined".into()
            }
        );
    }

    #[test]
    fn unavailable_after_kill_reopen_preserves_selection_memo_and_diagnostic() {
        const ENV: &str = "PI_RS_UNAVAILABLE_AFTER_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = unavailable_host(&root, "v1");
            let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("pause-unavailable"), b"yes").unwrap();
        let mut process=std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::unavailable_after_kill_reopen_preserves_selection_memo_and_diagnostic").env(ENV,root).spawn().unwrap();
        wait_file(&root.join("unavailable-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = unavailable_host(root, "v2");
        let driver = host.into_driver();
        assert!(!driver.get(id).unwrap().unwrap().is_terminal());
        assert!(!root.join("unavailable-second").exists());
        let _ = driver.shutdown_with_pending().unwrap();
        let (host, _) = unavailable_host(root, "v1");
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        let crate::task::TaskState::Succeeded(value) = task.state() else {
            panic!("unexpected {:?}", task.state())
        };
        let DurableTaskResult::Rich {
            task_content_v1: result,
        } = serde_json::from_value(value.clone()).unwrap()
        else {
            panic!("missing rich result")
        };
        assert_eq!(result.output.len(), 2);
        assert!(
            result
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("tool_unavailable"))
        );
        assert!(root.join("unavailable-second").exists());
        assert!(!root.join("unexpected-before").exists());
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            store
                .list(format!("durable/replay-safe-async/{id}/").as_bytes())
                .unwrap()
                .is_empty()
        );
        drop(store);
        let (host, _) = unavailable_host(root, "v1");
        let driver = host.into_driver();
        assert_eq!(driver.get(id).unwrap().unwrap().state(), task.state());
        let _ = driver.shutdown_with_pending().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("unavailable-second"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn unavailable_after_rejects_unsupported_metadata_before_later_hook() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("bad-unavailable"), b"yes").unwrap();
        let (host, tool) = unavailable_host(root, "v1");
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("unavailable-second").exists());
        assert!(!root.join("unexpected-before").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            matches!(read_replay_safe_after_evidence(&store,id).unwrap(),Some(ReplaySafeAfterEvidence::HostError {code,..}) if code=="tool_unavailable")
        );
    }
    #[test]
    fn unavailable_after_abort_fences_late_memo() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (entered, rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "hook".into(),
                    hooks: vec![Arc::new(AbortAfterHook {
                        entered,
                        release: std::sync::Mutex::new(release_rx),
                        result,
                    })],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "hook-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(root.join("db"), Registry::default()).unwrap();
        let tool = host
            .register_unavailable_tool_with_content_hooks(
                ToolSpec {
                    name: "missing".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                },
                "missing-v1",
                1,
                &registry.snapshot(),
                &["hook".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                UnavailableAfterAuthorization::host_attests_unavailable_and_selection(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/late-after").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            matches!(read_replay_safe_after_evidence(&store,id).unwrap(),Some(ReplaySafeAfterEvidence::HostError{code,..}) if code=="tool_unavailable")
        );
    }
    #[test]
    fn unavailable_after_restart_limit_retains_host_evidence() {
        const ENV: &str = "PI_RS_UNAVAILABLE_LIMIT_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = unavailable_host(&root, "v1");
            let id = host.start_replay_safe_root(&tool, json!({}), 1).unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let _driver = host.into_driver();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("pause-unavailable"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unavailable_after_restart_limit_retains_host_evidence")
            .env(ENV, root)
            .spawn()
            .unwrap();
        wait_file(&root.join("unavailable-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = unavailable_host(root, "v1");
        let driver = host.into_driver();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason=="restart limit")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("unavailable-second").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(
            matches!(read_replay_safe_after_evidence(&store,id).unwrap(),Some(ReplaySafeAfterEvidence::HostError{code,..}) if code=="tool_unavailable")
        );
    }
    #[test]
    fn unavailable_registration_rejects_selected_matching_tool() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(TextTool);
        let mut registry = crate::extension::Registry::default();
        registry
            .install_identified(
                crate::extension::Extension {
                    name: "hook".into(),
                    tools: vec![tool.clone()],
                    hooks: vec![Arc::new(NoopDurableHook)],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "hook-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        assert!(
            host.register_unavailable_tool_with_content_hooks(
                tool.spec(),
                "missing-v1",
                1,
                &registry.snapshot(),
                &["hook".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
                UnavailableAfterAuthorization::host_attests_unavailable_and_selection()
            )
            .is_err()
        );
    }
    struct UnsafeCrashFixture(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for UnsafeCrashFixture {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "unsafe-crash".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(self.0.join("unsafe-invocations"))
                .unwrap();
            writeln!(f, "run").unwrap();
            f.sync_all().unwrap();
            std::fs::write(self.0.join("unsafe-ready"), b"yes").unwrap();
            std::future::pending().await
        }
    }
    struct UnsafeAfterProbe(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for UnsafeAfterProbe {
        async fn after_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &serde_json::Value,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            std::fs::write(self.0.join("unsafe-after"), b"called").map_err(|e| e.to_string())?;
            Ok(Some(json!("pretend success")))
        }
    }
    #[test]
    fn unsafe_interrupted_after_stays_unknown_and_does_not_run_unfenced_hook() {
        const ENV: &str = "PI_RS_UNSAFE_AFTER_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.clone()));
            let binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = after_fixture_host(
                &root.join("db"),
                tool,
                Arc::new(UnsafeAfterProbe(root.clone())),
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_file(&root.join("unsafe-ready"));
            let parent_task = driver.get(parent.id()).unwrap().unwrap();
            let child: AsyncTaskCall =
                serde_json::from_value(parent_task.checkpoint().data.clone()).unwrap();
            std::fs::write(root.join("parent-id"), parent.id().to_string()).unwrap();
            std::fs::write(root.join("child-id"), child.task_id().to_string()).unwrap();
            loop {
                std::hint::black_box((&driver, &binding));
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process=std::process::Command::new(std::env::current_exe().unwrap()).arg("--exact").arg("async_task_tool::tests::unsafe_interrupted_after_stays_unknown_and_does_not_run_unfenced_hook").env(ENV,root).spawn().unwrap();
        wait_file(&root.join("child-id"));
        process.kill().unwrap();
        process.wait().unwrap();
        let child: Uuid = std::fs::read_to_string(root.join("child-id"))
            .unwrap()
            .parse()
            .unwrap();
        let parent: Uuid = std::fs::read_to_string(root.join("parent-id"))
            .unwrap()
            .parse()
            .unwrap();
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = after_fixture_host(
            &root.join("db"),
            tool,
            Arc::new(UnsafeAfterProbe(root.to_owned())),
        )
        .into_driver();
        assert!(matches!(
            wait_terminal(&driver, parent).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(matches!(
            read_sealed_outcome_evidence(&store, child).unwrap(),
            Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
        ));
        assert!(!root.join("unsafe-after").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("unsafe-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    struct InterruptedAfterFixture(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for InterruptedAfterFixture {
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            if !input
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("interrupted") && d.message.contains("unknown"))
                || input.extra.get("isError") != Some(&json!(true))
            {
                return Err("missing interrupted status".into());
            }
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("interrupted-hook-count"))
                .map_err(|e| e.to_string())?;
            writeln!(file, "hook").map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            if self.0.join("interrupted-memo").exists() {
                let _ = ctx
                    .memo
                    .get_or_insert("interrupted-write", json!(true))
                    .await;
            }
            if self.0.join("interrupted-pause").exists() {
                std::fs::write(self.0.join("interrupted-ready"), b"yes")
                    .map_err(|e| e.to_string())?;
                while !self.0.join("interrupted-release").exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
            let mut out = input.clone();
            out.output = vec![json!({"type":"text","text":"interrupted after hook"})];
            Ok(Some(out))
        }
    }
    fn interrupted_host(
        root: &std::path::Path,
        tool: Arc<dyn Tool>,
        hook_identity: &str,
    ) -> StandaloneBeforeHookHost {
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone(), false)));
        assert!(registry.register("independent-wake", true, |_: &Task| {
            Ok(crate::task_scheduler::Step::Succeed(json!("woken")))
        }));
        let mut host = StandaloneBeforeHookHost::open(root.join("db"), registry).unwrap();
        let adapter = host
            .register_tool_with_interrupted_after(
                "leaf",
                tool,
                "backend",
                1,
                hook_identity,
                Arc::new(InterruptedAfterFixture(root.to_owned())),
                InterruptedAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        host
    }
    #[test]
    fn interrupted_after_fixture() {
        let Ok(root) = std::env::var("PI_RS_INTERRUPTED_FIXTURE") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.clone()));
        let binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let scheduler = interrupted_host(&root, tool, "hook-v1").into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        wait_file(&root.join("unsafe-ready"));
        let task = driver.get(parent.id()).unwrap().unwrap();
        let child: AsyncTaskCall = serde_json::from_value(task.checkpoint().data.clone()).unwrap();
        std::fs::write(root.join("parent-id"), parent.id().to_string()).unwrap();
        std::fs::write(root.join("child-id"), child.task_id().to_string()).unwrap();
        loop {
            std::hint::black_box((&driver, &binding));
            std::thread::park();
        }
    }
    fn interrupted_kill_tool(root: &std::path::Path) -> (Uuid, Uuid) {
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::interrupted_after_fixture")
            .env("PI_RS_INTERRUPTED_FIXTURE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("child-id"));
        process.kill().unwrap();
        process.wait().unwrap();
        (
            std::fs::read_to_string(root.join("parent-id"))
                .unwrap()
                .parse()
                .unwrap(),
            std::fs::read_to_string(root.join("child-id"))
                .unwrap()
                .parse()
                .unwrap(),
        )
    }
    struct UnsafeHookedSealedAncestor(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for UnsafeHookedSealedAncestor {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "unsafe-hooked-ancestor".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for UnsafeHookedSealedAncestor {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("ancestor-effects"))
                .map_err(|e| e.to_string())?;
            writeln!(
                f,
                "{}",
                if resume.checkpoint.is_none() {
                    "initial"
                } else {
                    "resume"
                }
            )
            .map_err(|e| e.to_string())?;
            f.sync_all().map_err(|e| e.to_string())?;
            if let Some(data) = resume.checkpoint {
                let bridge: AsyncTaskCall =
                    serde_json::from_value(data["bridge"].clone()).map_err(|e| e.to_string())?;
                if resume.ended.contains(&bridge.task_id()) {
                    return Ok(DurableToolStep::Complete(Outcome::Failed(
                        "bridge finished".into(),
                    )));
                }
                std::fs::write(self.0.join("ancestor-busy"), b"yes").map_err(|e| e.to_string())?;
                loop {
                    if context.cancellation.is_cancelled() {
                        return Err("ancestor aborted".into());
                    }
                    if self.0.join("ancestor-release").exists() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Ok(DurableToolStep::Park {
                    checkpoint: data,
                    wait: DurableWait::All(vec![bridge]),
                })
            } else {
                let bridge = host
                    .start_child("bridge", "sealed-bridge", json!({}))
                    .await?;
                let gate = host.start_child("gate", "ancestor-gate", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"bridge":bridge, "gate":gate}),
                    wait: DurableWait::Any(vec![bridge, gate]),
                })
            }
        }
    }
    fn interrupted_unsafe_hooked_host(
        root: &std::path::Path,
    ) -> (
        StandaloneBeforeHookHost,
        Arc<crate::task_tool::TaskTool>,
        crate::nested_identity::IdentityHandle,
    ) {
        let sealed: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let binding = crate::nested_identity::register(&sealed, Some("backend".into())).unwrap();
        let bridge_cell = Arc::new(std::sync::OnceLock::new());
        let bridge_handler: Arc<dyn crate::task_scheduler::Handler> =
            Arc::new(CheckpointParent(bridge_cell.clone(), false));
        let gate_handler: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register_shared("sealed-bridge", true, bridge_handler.clone()));
        assert!(registry.register_shared("ancestor-gate", true, gate_handler.clone()));
        let bridge = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "sealed-bridge".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "sealed-bridge",
            true,
            1,
            bridge_handler,
        ));
        let gate = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "ancestor-gate".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "ancestor-gate",
            true,
            1,
            gate_handler,
        ));
        let mut host = StandaloneBeforeHookHost::open(root.join("db"), registry).unwrap();
        let adapter = host
            .register_tool_with_interrupted_after(
                "sealed-leaf",
                sealed.clone(),
                "backend",
                1,
                "interrupted-hook-v1",
                Arc::new(InterruptedAfterFixture(root.to_owned())),
                InterruptedAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
        assert!(bridge_cell.set(adapter).is_ok());
        let ancestor = host
            .register_unsafe_durable_tool_with_hook(
                Arc::new(UnsafeHookedSealedAncestor(root.to_owned())),
                "unsafe-ancestor-v1",
                1,
                vec![bridge, gate],
                "ancestor-hook-v1",
                Arc::new(NoopDurableHook),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, ancestor, binding)
    }
    #[test]
    fn interrupted_sealed_child_late_receipt_under_busy_unsafe_hooked_ancestor() {
        const ENV: &str = "PI_RS_INTERRUPTED_UNSAFE_ANCESTOR_COMBINED";
        if let Ok(path) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(path);
            let (host, tool, binding) = interrupted_unsafe_hooked_host(&root);
            let id = host.start_unsafe_durable_root(&tool, json!({}), 5).unwrap();
            let driver = host.into_driver();
            let task = wait_state(&driver, id, |state| {
                matches!(state, crate::task::TaskState::Waiting(_))
            });
            let bridge: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["bridge"].clone())
                    .unwrap();
            let gate: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["gate"].clone())
                    .unwrap();
            let bridge_task = wait_state(&driver, bridge.task_id(), |state| {
                matches!(state, crate::task::TaskState::Waiting(_))
            });
            let sealed: AsyncTaskCall =
                serde_json::from_value(bridge_task.checkpoint().data.clone()).unwrap();
            wait_file(&root.join("unsafe-ready"));
            wait_state(&driver, gate.task_id(), |state| {
                matches!(state, crate::task::TaskState::Waiting(_))
            });
            std::fs::write(
                root.join("combined-ids"),
                serde_json::to_vec(&(id, bridge.task_id(), gate.task_id(), sealed.task_id()))
                    .unwrap(),
            )
            .unwrap();
            loop {
                std::hint::black_box((&driver, &binding));
                std::thread::park();
            }
        }
        for abort_ancestor in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact").arg("async_task_tool::tests::interrupted_sealed_child_late_receipt_under_busy_unsafe_hooked_ancestor")
                .env(ENV, root).spawn().unwrap();
            wait_file(&root.join("combined-ids"));
            process.kill().unwrap();
            process.wait().unwrap();
            let (id, _bridge, gate, child): (Uuid, Uuid, Uuid, Uuid) =
                serde_json::from_slice(&std::fs::read(root.join("combined-ids")).unwrap()).unwrap();
            std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            let (host, _, _binding) = interrupted_unsafe_hooked_host(root);
            let scheduler = host.into_scheduler_for_test();
            let owner = scheduler.owner();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_file(&root.join("interrupted-ready"));
            driver
                .signal(&crate::task_store::Signal::new(
                    Uuid::new_v4(),
                    gate,
                    "release".into(),
                    json!(null),
                ))
                .unwrap();
            wait_file(&root.join("ancestor-busy"));
            let intent = crate::async_task_protocol::load_intent(owner.store(), child).unwrap();
            let batch = crate::async_task_protocol::prepare_raw_result(
                owner.store(),
                &intent,
                &Outcome::Succeeded("late raw".into()),
            )
            .unwrap();
            owner
                .store()
                .commit(&batch.checks, &batch.mutations)
                .unwrap();
            if abort_ancestor {
                driver.request_abort(id).unwrap();
            }
            std::fs::write(root.join("interrupted-release"), b"yes").unwrap();
            if !abort_ancestor {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                loop {
                    let task = driver.get(child).unwrap().unwrap();
                    if task.holds_interrupted_cleanup() && task.attempt_id().is_none() {
                        break;
                    }
                    assert!(std::time::Instant::now() < deadline, "child did not defer");
                    std::thread::yield_now();
                }
                std::fs::write(root.join("ancestor-release"), b"yes").unwrap();
            }
            let task = wait_terminal(&driver, child);
            if abort_ancestor {
                assert!(matches!(task.state(), crate::task::TaskState::Cancelled));
            } else {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            }
            assert!(
                matches!(read_sealed_outcome_evidence(owner.store(), child).unwrap(), Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(value))) if value == "late raw")
            );
            assert!(
                read_interrupted_after_result(owner.store(), child)
                    .unwrap()
                    .is_none()
            );
            let ancestor = wait_terminal(&driver, id);
            if abort_ancestor {
                assert!(matches!(
                    ancestor.state(),
                    crate::task::TaskState::Cancelled
                ));
            } else {
                assert!(
                    !matches!(ancestor.state(), crate::task::TaskState::Succeeded(value) if serde_json::from_value::<Outcome>(value.clone()).is_ok_and(|outcome| matches!(outcome, Outcome::Succeeded(_))))
                );
            }
            let _ = driver.shutdown_with_pending().unwrap();
            assert_eq!(
                std::fs::read_to_string(root.join("unsafe-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            let effects = std::fs::read_to_string(root.join("ancestor-effects")).unwrap();
            let effects: Vec<_> = effects.lines().collect();
            assert_eq!(
                effects,
                if abort_ancestor {
                    vec!["initial", "resume"]
                } else {
                    vec!["initial", "resume", "resume"]
                }
            );
        }
    }

    struct InterruptedBusyParent {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        child: Uuid,
        runs: AtomicUsize,
    }
    impl crate::task_scheduler::Handler for InterruptedBusyParent {
        fn run(&self, task: &Task) -> Result<crate::task_scheduler::Step, String> {
            if self.runs.fetch_add(1, Ordering::SeqCst) != 0 {
                return Ok(crate::task_scheduler::Step::Fail("child finished".into()));
            }
            self.entered.send(()).map_err(|e| e.to_string())?;
            self.release
                .lock()
                .unwrap()
                .recv()
                .map_err(|e| e.to_string())?;
            Ok(crate::task_scheduler::Step::Wait {
                checkpoint: task.checkpoint().clone(),
                condition: crate::task::WaitFor::Tasks(vec![self.child]),
                children: vec![],
            })
        }
    }
    #[test]
    fn interrupted_cleanup_waits_for_independent_uncertain_graph() {
        use crate::task_store::{Save, TaskStore};
        for case in ["repair", "late-raw", "shutdown", "waits", "waits-busy"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (_, child) = interrupted_kill_tool(root);
            std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
            let owner = scheduler.owner();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_file(&root.join("interrupted-ready"));
            let tasks = TaskStore::new(owner.store());
            let mut unrelated = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("parent".into(), true);
            unrelated.start_attempt(Uuid::new_v4()).unwrap();
            unrelated.hold_failure("unrelated failure".into()).unwrap();
            let edge = crate::ownership_edge::edge_key(Uuid::new_v4());
            let edge_value = if case.starts_with("waits") {
                let child = Uuid::from_slice(&edge[crate::ownership_edge::PREFIX.len()..]).unwrap();
                serde_json::to_vec(&crate::ownership_edge::Edge {
                    task: unrelated.id(),
                    task_owner: unrelated.owner().clone(),
                    child,
                    mode: crate::ownership_edge::Mode::Foreground,
                })
                .unwrap()
            } else {
                b"invalid edge".to_vec()
            };
            {
                let _gate = owner.task_gate().lock().unwrap();
                tasks.save(&[Save::Create(&unrelated)]).unwrap();
                owner
                    .store()
                    .commit(
                        &[durable_store::Check {
                            key: edge.clone(),
                            expected: durable_store::Expected::Missing,
                        }],
                        &[durable_store::Mutation::Put {
                            key: edge.clone(),
                            value: edge_value,
                        }],
                    )
                    .unwrap();
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !driver.has_pending_task(child) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "interrupted settlement was not retained"
                );
                assert!(!driver.get(child).unwrap().unwrap().is_terminal());
                std::thread::yield_now();
            }
            let held = driver.get(child).unwrap().unwrap();
            assert!(held.holds_interrupted_cleanup());
            assert!(!held.abort_requested());
            assert!(
                read_interrupted_after_result(owner.store(), child)
                    .unwrap()
                    .is_none()
            );
            if case.starts_with("waits") {
                let gate = owner.task_gate().lock().unwrap();
                let mut waiters = Vec::new();
                for condition in [
                    crate::task::WaitFor::Signal("go".into()),
                    crate::task::WaitFor::DeadlineMillis(crate::runtime::wall_time_millis() - 1),
                ] {
                    let mut task = Task::new(
                        Uuid::new_v4(),
                        TaskOwner::Conversation(Uuid::new_v4()),
                        Checkpoint {
                            phase: "initial".into(),
                            data: json!(null),
                        },
                        3,
                    )
                    .unwrap()
                    .registered("independent-wake".into(), true);
                    task.start().unwrap();
                    task.wait(condition).unwrap();
                    tasks.save(&[Save::Create(&task)]).unwrap();
                    waiters.push(task.id());
                }
                let mut nested_parent = Task::new(
                    Uuid::new_v4(),
                    TaskOwner::Conversation(Uuid::new_v4()),
                    Checkpoint {
                        phase: "initial".into(),
                        data: json!(null),
                    },
                    3,
                )
                .unwrap()
                .registered("independent-wake".into(), true);
                nested_parent.start().unwrap();
                nested_parent
                    .wait(crate::task::WaitFor::Signal("never".into()))
                    .unwrap();
                let mut nested = Task::new(
                    Uuid::new_v4(),
                    TaskOwner::Task(nested_parent.id()),
                    Checkpoint {
                        phase: "initial".into(),
                        data: json!(null),
                    },
                    3,
                )
                .unwrap()
                .registered("independent-wake".into(), true);
                nested.start().unwrap();
                nested
                    .wait(crate::task::WaitFor::DeadlineMillis(
                        crate::runtime::wall_time_millis() - 1,
                    ))
                    .unwrap();
                tasks
                    .save(&[Save::Create(&nested_parent), Save::Create(&nested)])
                    .unwrap();
                waiters.push(nested.id());
                let mut maintenance = Task::new(
                    Uuid::new_v4(),
                    TaskOwner::Conversation(Uuid::new_v4()),
                    Checkpoint {
                        phase: "initial".into(),
                        data: json!(null),
                    },
                    3,
                )
                .unwrap()
                .registered("independent-wake".into(), true);
                maintenance.start().unwrap();
                maintenance.hold_success(json!("maintenance")).unwrap();
                tasks.save(&[Save::Create(&maintenance)]).unwrap();
                let (busy_tx, busy_rx) = std::sync::mpsc::channel();
                let (writer_tx, writer_rx) = std::sync::mpsc::channel();
                if case == "waits-busy" {
                    let (unlock_tx, unlock_rx) = std::sync::mpsc::channel();
                    let path = root.join("db");
                    crate::task_scheduler::before_independent_wake_for_test(
                        waiters[0],
                        move || {
                            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
                            let writer = std::thread::spawn(move || {
                                let db = rusqlite::Connection::open(path).unwrap();
                                db.execute_batch("BEGIN IMMEDIATE").unwrap();
                                locked_tx.send(()).unwrap();
                                unlock_rx
                                    .recv_timeout(std::time::Duration::from_secs(20))
                                    .unwrap();
                                db.execute_batch("COMMIT").unwrap();
                            });
                            locked_rx
                                .recv_timeout(std::time::Duration::from_secs(5))
                                .unwrap();
                            writer_tx.send(writer).unwrap();
                        },
                    );
                    crate::task_scheduler::on_independent_wake_busy_for_test(
                        waiters[0],
                        move || {
                            busy_tx.send(()).unwrap();
                            unlock_tx.send(()).unwrap();
                        },
                    );
                }
                tasks
                    .send_signal(&crate::task_store::Signal::new(
                        Uuid::new_v4(),
                        waiters[0],
                        "go".into(),
                        json!("signal"),
                    ))
                    .unwrap();
                drop(gate);
                if case == "waits-busy" {
                    busy_rx
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .expect("wake commit did not hit writer busy");
                    writer_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap()
                        .join()
                        .unwrap();
                }
                for id in waiters {
                    assert_eq!(
                        wait_terminal(&driver, id).state(),
                        &crate::task::TaskState::Succeeded(json!("woken"))
                    );
                }
                assert_eq!(
                    wait_terminal(&driver, maintenance.id()).state(),
                    &crate::task::TaskState::Succeeded(json!("maintenance"))
                );
                assert!(
                    driver
                        .get(child)
                        .unwrap()
                        .unwrap()
                        .holds_interrupted_cleanup()
                );
                assert!(
                    read_interrupted_after_result(owner.store(), child)
                        .unwrap()
                        .is_none()
                );
                assert!(owner.store().get(&edge).unwrap().is_some());
            }
            if case == "shutdown" {
                let report = driver.shutdown_with_pending().unwrap();
                assert!(report.failure().is_none());
                assert_eq!(report.pending().len(), 1);
                assert_eq!(report.pending()[0].task_id(), child);
                assert!(report.pending()[0].completed().is_some());
                continue;
            }
            if case == "late-raw" {
                let intent = crate::async_task_protocol::load_intent(owner.store(), child).unwrap();
                let batch = crate::async_task_protocol::prepare_raw_result(
                    owner.store(),
                    &intent,
                    &Outcome::Succeeded("late result".into()),
                )
                .unwrap();
                owner
                    .store()
                    .commit(&batch.checks, &batch.mutations)
                    .unwrap();
            }
            {
                let _gate = owner.task_gate().lock().unwrap();
                let entry = owner.store().get(&edge).unwrap().unwrap();
                owner
                    .store()
                    .commit(
                        &[durable_store::Check {
                            key: edge.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Delete { key: edge }],
                    )
                    .unwrap();
            }
            let task = wait_terminal(&driver, child);
            assert!(!task.abort_requested());
            if case == "late-raw" {
                assert!(
                    matches!(task.state(), crate::task::TaskState::Failed(reason) if reason == "sealed outcome requires reconciliation")
                );
                assert!(
                    matches!(read_sealed_outcome_evidence(owner.store(), child).unwrap(), Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(value))) if value == "late result")
                );
            } else {
                assert!(
                    matches!(task.state(), crate::task::TaskState::Failed(reason) if reason == "interrupted: execution outcome requires reconciliation")
                );
                assert!(matches!(
                    read_sealed_outcome_evidence(owner.store(), child).unwrap(),
                    Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
                ));
            }
            assert!(
                read_interrupted_after_result(owner.store(), child)
                    .unwrap()
                    .is_none()
            );
            let _ = driver.shutdown_with_pending().unwrap();
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(root.join("unsafe-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }

    #[test]
    fn pending_uncertain_cleanup_kill_reopen_retains_evidence() {
        use crate::task_store::{Save, TaskStore};
        use std::time::{Duration, Instant};
        const ENV: &str = "PI_TEST_PENDING_WAKE_KILL_1120";
        if let Some(path) = std::env::var_os(ENV) {
            let root = std::path::PathBuf::from(path);
            let (_, child) = interrupted_kill_tool(&root);
            std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.clone()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = interrupted_host(&root, tool, "hook-v1").into_scheduler_for_test();
            let owner = scheduler.owner();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_file(&root.join("interrupted-ready"));
            let mut unrelated = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("parent".into(), true);
            unrelated.start_attempt(Uuid::new_v4()).unwrap();
            unrelated.hold_failure("unrelated failure".into()).unwrap();
            let edge_child = Uuid::new_v4();
            let edge = crate::ownership_edge::edge_key(edge_child);
            let value = serde_json::to_vec(&crate::ownership_edge::Edge {
                task: unrelated.id(),
                task_owner: unrelated.owner().clone(),
                child: edge_child,
                mode: crate::ownership_edge::Mode::Foreground,
            })
            .unwrap();
            {
                let _gate = owner.task_gate().lock().unwrap();
                TaskStore::new(owner.store())
                    .save(&[Save::Create(&unrelated)])
                    .unwrap();
                owner
                    .store()
                    .commit(
                        &[durable_store::Check {
                            key: edge.clone(),
                            expected: durable_store::Expected::Missing,
                        }],
                        &[durable_store::Mutation::Put {
                            key: edge.clone(),
                            value,
                        }],
                    )
                    .unwrap();
            }
            let until = Instant::now() + Duration::from_secs(10);
            while !driver.has_pending_task(child) {
                assert!(Instant::now() < until, "cleanup did not retain its lease");
                std::thread::yield_now();
            }
            assert!(
                read_interrupted_after_result(owner.store(), child)
                    .unwrap()
                    .is_none()
            );
            std::fs::write(
                root.join("pending-kill"),
                serde_json::to_vec(&(child, edge)).unwrap(),
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "async_task_tool::tests::pending_uncertain_cleanup_kill_reopen_retains_evidence",
            ])
            .env(ENV, root)
            .spawn()
            .unwrap();
        let until = Instant::now() + Duration::from_secs(30);
        while !root.join("pending-kill").exists() {
            assert!(
                process.try_wait().unwrap().is_none(),
                "pending worker exited"
            );
            assert!(Instant::now() < until, "pending worker timed out");
            std::thread::sleep(Duration::from_millis(5));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let (child, edge): (Uuid, Vec<u8>) =
            serde_json::from_slice(&std::fs::read(root.join("pending-kill")).unwrap()).unwrap();
        std::fs::write(root.join("interrupted-release"), b"release").unwrap();
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
        let owner = scheduler.owner();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert!(
            read_interrupted_after_result(owner.store(), child)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            std::fs::read_to_string(root.join("unsafe-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        {
            let _gate = owner.task_gate().lock().unwrap();
            let entry = owner.store().get(&edge).unwrap().unwrap();
            owner
                .store()
                .commit(
                    &[durable_store::Check {
                        key: edge.clone(),
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[durable_store::Mutation::Delete { key: edge }],
                )
                .unwrap();
        }
        let result = wait_terminal(&driver, child);
        assert!(matches!(result.state(), crate::task::TaskState::Failed(_)));
        assert!(matches!(
            read_sealed_outcome_evidence(owner.store(), child).unwrap(),
            Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
        ));
        assert_eq!(
            std::fs::read_to_string(root.join("unsafe-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn interrupted_cleanup_parent_failure_cascades_before_settlement() {
        use crate::task_store::TaskStore;
        for after_receipt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (parent, child) = interrupted_kill_tool(root);
            if !after_receipt {
                std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            }
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let mut scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
            let invocation = loop {
                match scheduler.reserve_tick(0).unwrap() {
                    crate::task_scheduler::TickReservation::Invocation(invocation) => {
                        break invocation;
                    }
                    crate::task_scheduler::TickReservation::Progress(true) => continue,
                    _ => panic!("expected interrupted cleanup reservation"),
                }
            };
            assert_eq!(invocation.task_id(), child);
            let worker = std::thread::spawn(move || invocation.execute().unwrap());
            let mut worker = Some(worker);
            let mut completed = if after_receipt {
                Some(worker.take().unwrap().join().unwrap())
            } else {
                wait_file(&root.join("interrupted-ready"));
                None
            };
            let owner = scheduler.owner();
            let tasks = TaskStore::new(owner.store());
            if after_receipt {
                assert!(
                    owner
                        .store()
                        .get(format!("durable/async-task/interrupted-after/{child}").as_bytes())
                        .unwrap()
                        .is_some()
                );
            }
            let snapshot = tasks.load(parent).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    task.wake()?;
                    task.start_attempt(Uuid::new_v4())?;
                    task.hold_failure("parent failed while hook was running".into())
                })
                .unwrap();
            assert!(!tasks.load(child).unwrap().unwrap().task.abort_requested());
            if !after_receipt {
                std::fs::write(root.join("interrupted-release"), b"yes").unwrap();
                completed = Some(worker.take().unwrap().join().unwrap());
            }
            let mut completed = completed.unwrap();
            let mut retries = 0;
            loop {
                retries += 1;
                assert!(retries <= 10, "cleanup settlement did not converge");
                match scheduler.settle_invocation(completed) {
                    Ok(_) => break,
                    Err(crate::task_scheduler::SettlementError::Pending { completed: pending }) => {
                        completed = *pending
                    }
                    other => panic!("cleanup settlement failed: {other:?}"),
                }
            }
            let child_task = tasks.load(child).unwrap().unwrap().task;
            assert!(matches!(
                child_task.state(),
                crate::task::TaskState::Cancelled
            ));
            assert!(child_task.abort_requested());
            assert!(
                read_interrupted_after_result(owner.store(), child)
                    .unwrap()
                    .is_none()
            );
            assert!(
                owner
                    .store()
                    .get(format!("durable/async-task/interrupted-after/{child}").as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                read_sealed_outcome_evidence(owner.store(), child).unwrap(),
                Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
            ));
            let driver = crate::task_driver::Driver::start(scheduler);
            assert!(
                matches!(wait_terminal(&driver, parent).state(), crate::task::TaskState::Failed(reason) if reason == "parent failed while hook was running")
            );
            driver.shutdown().unwrap();
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(root.join("unsafe-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }

    #[test]
    fn interrupted_cleanup_defers_while_unsafe_ancestor_is_running() {
        use crate::task_store::TaskStore;
        for race_during_hook in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (parent, child) = interrupted_kill_tool(root);
            if race_during_hook {
                std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            }
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let (entered, entered_rx) = std::sync::mpsc::channel();
            let (release, release_rx) = std::sync::mpsc::channel();
            let mut registry = Registry::default();
            assert!(registry.register(
                "parent",
                false,
                InterruptedBusyParent {
                    entered,
                    release: std::sync::Mutex::new(release_rx),
                    child,
                    runs: AtomicUsize::new(0),
                }
            ));
            let mut host = StandaloneBeforeHookHost::open(root.join("db"), registry).unwrap();
            host.register_tool_with_interrupted_after(
                "leaf",
                tool,
                "backend",
                1,
                "hook-v1",
                Arc::new(InterruptedAfterFixture(root.to_owned())),
                InterruptedAfterAuthorization::host_attests_cleanup_safe(),
            )
            .unwrap();
            let scheduler = host.into_scheduler_for_test();
            let owner = scheduler.owner();
            let tasks = TaskStore::new(owner.store());
            let snapshot = tasks.load(parent).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    *task = task.clone().registered("parent".into(), false);
                    if !race_during_hook {
                        task.wake()?;
                    }
                    Ok(())
                })
                .unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            if race_during_hook {
                wait_file(&root.join("interrupted-ready"));
                let snapshot = tasks.load(parent).unwrap().unwrap();
                tasks.update(&snapshot, |task| task.wake()).unwrap();
            }
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("unsafe parent did not start");
            if race_during_hook {
                std::fs::write(root.join("interrupted-release"), b"yes").unwrap();
                std::fs::remove_file(root.join("interrupted-pause")).unwrap();
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let task = driver.get(child).unwrap().unwrap();
                if task.holds_interrupted_cleanup() && task.attempt_id().is_none() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "cleanup did not defer: {:?}",
                    task.state()
                );
                std::thread::yield_now();
            }
            assert!(
                driver.get(parent).unwrap().unwrap().state() == &crate::task::TaskState::Running
            );
            if !race_during_hook {
                assert!(!root.join("interrupted-hook-count").exists());
            }
            release.send(()).unwrap();
            assert!(matches!(
                wait_terminal(&driver, child).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert!(
                read_interrupted_after_result(&store, child)
                    .unwrap()
                    .is_some()
            );
            assert!(matches!(
                read_sealed_outcome_evidence(&store, child).unwrap(),
                Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
            ));
            let count = std::fs::read_to_string(root.join("interrupted-hook-count"))
                .unwrap()
                .lines()
                .count();
            assert_eq!(count, if race_during_hook { 2 } else { 1 });
            assert_eq!(
                std::fs::read_to_string(root.join("unsafe-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }

    #[test]
    fn interrupted_after_keeps_external_outcome_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (parent, child) = interrupted_kill_tool(root);
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = interrupted_host(root, tool, "hook-v1").into_driver();
        assert!(matches!(
            wait_terminal(&driver, parent).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(matches!(
            read_sealed_outcome_evidence(&store, child).unwrap(),
            Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
        ));
        let Some(DurableTaskResult::Rich {
            task_content_v1: result,
        }) = read_interrupted_after_result(&store, child).unwrap()
        else {
            panic!(
                "missing interrupted receipt: child={:?}, keys={:?}, hook={:?}",
                crate::task_store::TaskStore::new(&store)
                    .load(child)
                    .unwrap(),
                store
                    .list(b"durable/async-task/")
                    .unwrap()
                    .iter()
                    .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
                    .collect::<Vec<_>>(),
                std::fs::read_to_string(root.join("interrupted-hook-count"))
            )
        };
        assert!(
            result
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("interrupted") && d.message.contains("unknown"))
        );
        assert_eq!(result.extra["isError"], json!(true));
        assert_eq!(
            std::fs::read_to_string(root.join("unsafe-invocations"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.join("interrupted-hook-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn interrupted_after_abort_and_memo_fail_closed() {
        for abort in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (_, child) = interrupted_kill_tool(root);
            std::fs::write(
                root.join(if abort {
                    "interrupted-pause"
                } else {
                    "interrupted-memo"
                }),
                b"yes",
            )
            .unwrap();
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let driver = interrupted_host(root, tool, "hook-v1").into_driver();
            if abort {
                wait_file(&root.join("interrupted-ready"));
                driver.request_abort(child).unwrap();
            }
            let task = wait_terminal(&driver, child);
            if abort {
                assert!(matches!(task.state(), crate::task::TaskState::Cancelled));
            } else {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            }
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert!(matches!(
                read_sealed_outcome_evidence(&store, child).unwrap(),
                Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
            ));
            assert!(
                read_interrupted_after_result(&store, child)
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .list(format!("runtime/extension-memo/{child}/").as_bytes())
                    .unwrap()
                    .is_empty()
            );
        }
    }
    #[test]
    fn interrupted_after_waits_for_matching_registration() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (parent, child) = interrupted_kill_tool(root);
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell, false)));
        let driver = StandaloneBeforeHookHost::open(root.join("db"), registry)
            .unwrap()
            .into_driver();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!driver.get(child).unwrap().unwrap().is_terminal());
        let _ = driver.shutdown_with_pending().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = interrupted_host(root, tool, "hook-v2").into_driver();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!driver.get(child).unwrap().unwrap().is_terminal());
        assert!(!root.join("interrupted-hook-count").exists());
        let _ = driver.shutdown_with_pending().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = interrupted_host(root, tool, "hook-v1").into_driver();
        assert!(matches!(
            wait_terminal(&driver, parent).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn interrupted_after_late_raw_and_final_receipts_win() {
        for final_receipt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (_, child) = interrupted_kill_tool(root);
            std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
            let owner = scheduler.owner();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_file(&root.join("interrupted-ready"));
            let intent = crate::async_task_protocol::load_intent(owner.store(), child).unwrap();
            let outcome = Outcome::Succeeded("late evidence".into());
            let batch = if final_receipt {
                crate::async_task_protocol::prepare_receipt(owner.store(), &intent, &outcome)
            } else {
                crate::async_task_protocol::prepare_raw_result(owner.store(), &intent, &outcome)
            }
            .unwrap();
            owner
                .store()
                .commit(&batch.checks, &batch.mutations)
                .unwrap();
            std::fs::write(root.join("interrupted-release"), b"yes").unwrap();
            assert!(matches!(
                wait_terminal(&driver, child).state(),
                crate::task::TaskState::Failed(_)
            ));
            let evidence = read_sealed_outcome_evidence(owner.store(), child).unwrap();
            if final_receipt {
                assert!(matches!(
                    evidence,
                    Some(SealedOutcomeEvidence::Final(Outcome::Succeeded(_)))
                ));
            } else {
                assert!(matches!(
                    evidence,
                    Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(
                        _
                    )))
                ));
            }
            assert!(
                read_interrupted_after_result(owner.store(), child)
                    .unwrap()
                    .is_none()
            );
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }
    #[test]
    fn interrupted_after_recovery_fixture() {
        let Ok(root) = std::env::var("PI_RS_INTERRUPTED_RECOVERY_FIXTURE") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.clone()));
        let binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = interrupted_host(&root, tool, "hook-v1").into_driver();
        loop {
            std::hint::black_box((&driver, &binding));
            std::thread::park();
        }
    }
    #[test]
    fn interrupted_after_crash_before_and_after_receipt() {
        for after_receipt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (_, child) = interrupted_kill_tool(root);
            if !after_receipt {
                std::fs::write(root.join("interrupted-pause"), b"yes").unwrap();
            }
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("async_task_tool::tests::interrupted_after_recovery_fixture")
                .env("PI_RS_INTERRUPTED_RECOVERY_FIXTURE", root);
            if after_receipt {
                command.env("PI_TEST_INTERRUPTED_AFTER_RECEIPT", root);
            }
            let mut process = command.spawn().unwrap();
            wait_file(&root.join(if after_receipt {
                "interrupted-receipt"
            } else {
                "interrupted-ready"
            }));
            process.kill().unwrap();
            process.wait().unwrap();
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let driver = interrupted_host(root, tool, "hook-v1").into_driver();
            assert!(matches!(
                wait_terminal(&driver, child).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert!(matches!(
                read_sealed_outcome_evidence(&store, child).unwrap(),
                Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
            ));
            assert_eq!(
                read_interrupted_after_result(&store, child)
                    .unwrap()
                    .is_some(),
                after_receipt
            );
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn interrupted_after_receipt_arrives_during_failure_commit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (_, child) = interrupted_kill_tool(root);
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::interrupted_after_recovery_fixture")
            .env("PI_RS_INTERRUPTED_RECOVERY_FIXTURE", root)
            .env("PI_TEST_INTERRUPTED_AFTER_RECEIPT", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("interrupted-receipt"));
        process.kill().unwrap();
        process.wait().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let mut scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
        let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let marker = injected.clone();
        scheduler.set_reservation_commit_hook(Arc::new(move |_, store| {
            if !marker.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let intent = crate::async_task_protocol::load_intent(store, child).unwrap();
                let batch = crate::async_task_protocol::prepare_raw_result(
                    store,
                    &intent,
                    &Outcome::Succeeded("racing raw".into()),
                )
                .unwrap();
                store.commit(&batch.checks, &batch.mutations).unwrap();
            }
        }));
        let driver = crate::task_driver::Driver::start(scheduler);
        assert!(matches!(
            wait_terminal(&driver, child).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(matches!(
            read_sealed_outcome_evidence(&store, child).unwrap(),
            Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(
                _
            )))
        ));
        assert!(
            read_interrupted_after_result(&store, child)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            std::fs::read_to_string(root.join("interrupted-hook-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn interrupted_after_crash_before_intent_is_not_dispatched() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::interrupted_after_fixture")
            .env("PI_RS_INTERRUPTED_FIXTURE", root)
            .env("PI_TEST_INTERRUPTED_BEFORE_INTENT", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("before-intent"));
        let child: Uuid = std::fs::read_to_string(root.join("before-intent"))
            .unwrap()
            .parse()
            .unwrap();
        process.kill().unwrap();
        process.wait().unwrap();
        let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = interrupted_host(root, tool, "hook-v1").into_driver();
        assert!(
            matches!(wait_terminal(&driver, child).state(), crate::task::TaskState::Failed(reason) if reason == "interrupted before dispatch")
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(matches!(
            read_sealed_outcome_evidence(&store, child).unwrap(),
            Some(SealedOutcomeEvidence::NotDispatched)
        ));
        assert!(
            read_interrupted_after_result(&store, child)
                .unwrap()
                .is_none()
        );
        assert!(!root.join("interrupted-hook-count").exists());
        assert!(!root.join("unsafe-invocations").exists());
    }
    #[test]
    fn interrupted_after_live_settlement_rechecks_graph_and_receipt_conflicts() {
        for receipt_race in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (parent, child) = interrupted_kill_tool(root);
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let mut scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
            let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let marker = injected.clone();
            scheduler.set_reservation_commit_hook(Arc::new(move |_, store| {
                if marker.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                if receipt_race {
                    let intent = crate::async_task_protocol::load_intent(store, child).unwrap();
                    let batch = crate::async_task_protocol::prepare_raw_result(
                        store,
                        &intent,
                        &Outcome::Succeeded("live racing raw".into()),
                    )
                    .unwrap();
                    store.commit(&batch.checks, &batch.mutations).unwrap();
                } else {
                    let key = crate::task_store::key(parent);
                    let entry = store.get(&key).unwrap().unwrap();
                    store
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Version(entry.version),
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: entry.value,
                            }],
                        )
                        .unwrap();
                }
            }));
            let driver = crate::task_driver::Driver::start(scheduler);
            assert!(matches!(
                wait_terminal(&driver, child).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
            let store = durable_store::Store::open(root.join("db")).unwrap();
            if receipt_race {
                assert!(matches!(
                    read_sealed_outcome_evidence(&store, child).unwrap(),
                    Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(
                        _
                    )))
                ));
                assert!(
                    read_interrupted_after_result(&store, child)
                        .unwrap()
                        .is_none()
                );
            } else {
                assert!(matches!(
                    read_sealed_outcome_evidence(&store, child).unwrap(),
                    Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
                ));
                assert!(
                    read_interrupted_after_result(&store, child)
                        .unwrap()
                        .is_some()
                );
            }
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn interrupted_after_late_receipt_between_evidence_reads_retries() {
        for final_receipt in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (_, child) = interrupted_kill_tool(root);
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let mut scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
            let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let marker = injected.clone();
            scheduler.set_interrupted_receipt_read_hook(Arc::new(move |_, store| {
                if marker.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let intent = crate::async_task_protocol::load_intent(store, child).unwrap();
                let outcome = Outcome::Succeeded("between reads".into());
                let batch = if final_receipt {
                    crate::async_task_protocol::prepare_receipt(store, &intent, &outcome)
                } else {
                    crate::async_task_protocol::prepare_raw_result(store, &intent, &outcome)
                }
                .unwrap();
                store.commit(&batch.checks, &batch.mutations).unwrap();
            }));
            let driver = crate::task_driver::Driver::start(scheduler);
            assert!(matches!(
                wait_terminal(&driver, child).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
            let store = durable_store::Store::open(root.join("db")).unwrap();
            let evidence = read_sealed_outcome_evidence(&store, child).unwrap();
            if final_receipt {
                assert!(
                    matches!(evidence, Some(SealedOutcomeEvidence::Final(Outcome::Succeeded(ref s))) if s == "between reads")
                );
            } else {
                assert!(
                    matches!(evidence, Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(ref s))) if s == "between reads")
                );
            }
            assert!(
                read_interrupted_after_result(&store, child)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(root.join("unsafe-invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn interrupted_after_blocking_settlement_retains_conflicted_invocation() {
        for receipt_race in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (parent, child) = interrupted_kill_tool(root);
            let tool: Arc<dyn Tool> = Arc::new(UnsafeCrashFixture(root.to_owned()));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let mut scheduler = interrupted_host(root, tool, "hook-v1").into_scheduler_for_test();
            let invocation = loop {
                match scheduler.reserve_tick(0).unwrap() {
                    crate::task_scheduler::TickReservation::Invocation(invocation) => {
                        break invocation;
                    }
                    crate::task_scheduler::TickReservation::Progress(true) => continue,
                    _ => panic!("expected interrupted cleanup reservation"),
                }
            };
            let completed = invocation.execute().unwrap();
            let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let marker = injected.clone();
            scheduler.set_reservation_commit_hook(Arc::new(move |_, store| {
                if marker.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                if receipt_race {
                    let intent = crate::async_task_protocol::load_intent(store, child).unwrap();
                    let batch = crate::async_task_protocol::prepare_raw_result(
                        store,
                        &intent,
                        &Outcome::Succeeded("blocking racing raw".into()),
                    )
                    .unwrap();
                    store.commit(&batch.checks, &batch.mutations).unwrap();
                } else {
                    let key = crate::task_store::key(parent);
                    let entry = store.get(&key).unwrap().unwrap();
                    store
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Version(entry.version),
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: entry.value,
                            }],
                        )
                        .unwrap();
                }
            }));
            let completed = match scheduler.settle_invocation(completed) {
                Err(crate::task_scheduler::SettlementError::Pending { completed }) => *completed,
                other => panic!("expected retained invocation, got {other:?}"),
            };
            assert!(scheduler.settle_invocation(completed).unwrap());
            assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
            let owner = scheduler.owner();
            if receipt_race {
                assert!(matches!(
                    read_sealed_outcome_evidence(owner.store(), child).unwrap(),
                    Some(SealedOutcomeEvidence::AfterIncomplete(Outcome::Succeeded(
                        _
                    )))
                ));
                assert!(
                    read_interrupted_after_result(owner.store(), child)
                        .unwrap()
                        .is_none()
                );
            } else {
                assert!(matches!(
                    read_sealed_outcome_evidence(owner.store(), child).unwrap(),
                    Some(SealedOutcomeEvidence::Uncertain(Outcome::Unknown(_)))
                ));
                assert!(
                    read_interrupted_after_result(owner.store(), child)
                        .unwrap()
                        .is_some()
                );
            }
            assert_eq!(
                std::fs::read_to_string(root.join("interrupted-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    struct ParkCancelFixture(std::path::PathBuf);
    #[async_trait::async_trait]
    impl DurableParkCancel for ParkCancelFixture {
        async fn cancel(
            &self,
            args: serde_json::Value,
            checkpoint: serde_json::Value,
            _: ToolContext,
        ) -> Result<(), String> {
            if args != json!({"token":"prepared"})
                || checkpoint.get("call").is_none()
                || (!self.0.join("child-aborted").exists() && !self.0.join("woken-case").exists())
            {
                return Err("bad cancel input or child ordering".into());
            }
            if self.0.join("require-child-aborted").exists()
                && !self.0.join("child-aborted").exists()
            {
                return Err("owned child has not aborted".into());
            }
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("cancel-count"))
                .map_err(|e| e.to_string())?;
            writeln!(file, "cancel").map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            if self.0.join("fail-cancel").exists() {
                return Err("external outcome unknown".into());
            }
            if self.0.join("reenter-update").exists() {
                let handle = WOKEN_TEST_IDENTITIES
                    .get()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .get(&self.0)
                    .unwrap()
                    .clone();
                let result = handle.update(Some("changed".into()), || {}).await;
                std::fs::write(
                    self.0.join("reenter-result"),
                    if result.is_err() { "busy" } else { "updated" },
                )
                .unwrap();
            }
            if self.0.join("pause-cancel").exists() {
                std::fs::write(self.0.join("cancel-ready"), b"yes").map_err(|e| e.to_string())?;
                while !self.0.join("cancel-release").exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                std::fs::write(self.0.join("cancel-returning"), b"yes")
                    .map_err(|e| e.to_string())?;
            }
            Ok(())
        }
    }
    fn park_cancel_host(
        root: &std::path::Path,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        park_cancel_host_with_retry(root, false)
    }
    fn park_cancel_host_with_retry(
        root: &std::path::Path,
        retry: bool,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        use crate::task_scheduler::Handler;
        let leaf: Arc<dyn Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register_shared("park-cancel-leaf", true, leaf.clone()));
        let child_marker = root.join("child-aborted");
        assert!(registry.set_abort("park-cancel-leaf", move |_: &Task| {
            std::fs::write(&child_marker, b"yes").map_err(|e| e.to_string())?;
            Ok(crate::task_scheduler::Step::Succeed(json!(null)))
        }));
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "hook".into(),
                    hooks: vec![Arc::new(AbortResultHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "park-cancel-hook".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let mut host = StandaloneBeforeHookHost::open(root.join("db"), registry).unwrap();
        let child = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "park-cancel-leaf",
            true,
            1,
            leaf,
        ));
        let definition: Arc<dyn DurableAsyncTool> = Arc::new(ChainTool {
            name: "root",
            child: "leaf",
            calls: Arc::new(AtomicUsize::new(0)),
            any: false,
        });
        let callback: Arc<dyn DurableParkCancel> = Arc::new(ParkCancelFixture(root.to_owned()));
        let tool = if retry {
            host.register_replay_safe_durable_tool_with_retryable_park_cancel(
                definition,
                "park-cancel-tool-v1",
                1,
                vec![child],
                &extensions.snapshot(),
                &["hook".into()],
                "park-cancel-v1",
                callback,
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
                ParkCancelRetryAuthorization::host_attests_cancel_replay_safe(),
            )
        } else {
            host.register_replay_safe_durable_tool_with_park_cancel(
                definition,
                "park-cancel-tool-v1",
                1,
                vec![child],
                &extensions.snapshot(),
                &["hook".into()],
                "park-cancel-v1",
                callback,
                ReplaySafeAuthorization::host_attests_replay_safe(),
                AbortAfterAuthorization::host_attests_cleanup_safe(),
            )
        }
        .unwrap();
        (host, tool)
    }
    fn unsafe_park_cancel_host(
        root: &std::path::Path,
        retry: bool,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        unsafe_park_cancel_host_inner(root, retry, None, false)
    }
    type UnsafeCancelOuterSlot = Arc<std::sync::OnceLock<Arc<crate::task_tool::TaskTool>>>;
    static WOKEN_TEST_IDENTITIES: std::sync::OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<std::path::PathBuf, crate::nested_identity::IdentityHandle>,
        >,
    > = std::sync::OnceLock::new();
    struct UnsafeCancelOuter(UnsafeCancelOuterSlot);
    impl crate::task_scheduler::Handler for UnsafeCancelOuter {
        fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
            Err("documents required".into())
        }
        fn run_with_documents(
            &self,
            task: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            let call = self.0.get().ok_or("missing child")?.start_child(
                task,
                docs,
                "nested",
                json!({"token":"prepared"}),
            )?;
            Ok(crate::task_scheduler::Step::Wait {
                checkpoint: Checkpoint {
                    phase: "nested".into(),
                    data: json!({"call":call}),
                },
                condition: crate::task::WaitFor::Tasks(vec![call.task_id()]),
                children: vec![],
            })
        }
    }
    struct WokenAnyTool(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Tool for WokenAnyTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "root".into(),
                description: if self.0.join("any-spec-changed").exists() {
                    "changed".into()
                } else {
                    String::new()
                },
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for WokenAnyTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            if resume.checkpoint.is_none() {
                let first = host.start_child("first", "leaf", json!({})).await?;
                let second = host.start_child("second", "leaf", json!({})).await?;
                return Ok(DurableToolStep::Park {
                    checkpoint: json!({"call":first,"other":second}),
                    wait: DurableWait::Any(vec![first, second]),
                });
            }
            std::fs::write(self.0.join("wake-running"), b"yes").map_err(|e| e.to_string())?;
            context.cancellation.cancelled().await;
            Err("woken execution cancelled".into())
        }
    }
    fn unsafe_park_cancel_host_inner(
        root: &std::path::Path,
        retry: bool,
        outer: Option<(UnsafeCancelOuterSlot, bool)>,
        woken: bool,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        use crate::task_scheduler::Handler;
        let leaf: Arc<dyn Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register_shared("park-cancel-leaf", true, leaf.clone()));
        let child_marker = root.join("child-aborted");
        assert!(registry.set_abort("park-cancel-leaf", move |_: &Task| {
            std::fs::write(&child_marker, b"yes").map_err(|e| e.to_string())?;
            Ok(crate::task_scheduler::Step::Succeed(json!(null)))
        }));
        if let Some((slot, replay_safe)) = outer {
            assert!(registry.register_shared(
                "unsafe-cancel-outer",
                replay_safe,
                Arc::new(UnsafeCancelOuter(slot))
            ));
            assert!(registry.set_abort("unsafe-cancel-outer", |_: &Task| Ok(
                crate::task_scheduler::Step::Succeed(json!(null))
            )));
        }
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "hook".into(),
                    hooks: vec![Arc::new(AbortResultHook(root.to_owned()))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "park-cancel-hook".into(),
                    vec!["hook".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let db = if root.join("separate-identity-db").exists() {
            "identity-db"
        } else {
            "db"
        };
        let mut host = StandaloneBeforeHookHost::open(root.join(db), registry).unwrap();
        let child = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "park-cancel-leaf",
            true,
            1,
            leaf,
        ));
        let definition: Arc<dyn DurableAsyncTool> = if root.join("any-case").exists() {
            Arc::new(WokenAnyTool(root.to_owned()))
        } else {
            Arc::new(ChainTool {
                name: "root",
                child: "leaf",
                calls: Arc::new(AtomicUsize::new(0)),
                any: false,
            })
        };
        let callback: Arc<dyn DurableParkCancel> = Arc::new(ParkCancelFixture(root.to_owned()));
        if woken {
            let identity_tool: Arc<dyn Tool> = definition.clone();
            let handle = crate::nested_identity::register(
                &identity_tool,
                Some("test-woken-cancel-v1".into()),
            )
            .unwrap();
            if root.join("capture-identity-handle").exists() {
                WOKEN_TEST_IDENTITIES
                    .get_or_init(Default::default)
                    .lock()
                    .unwrap()
                    .insert(root.to_owned(), handle);
            }
        }
        let register = if woken {
            StandaloneBeforeHookHost::register_unsafe_durable_tool_with_woken_cancel
        } else {
            StandaloneBeforeHookHost::register_unsafe_durable_tool_with_park_cancel
        };
        let tool = register(
            &mut host,
            definition,
            "park-cancel-tool-v1",
            1,
            vec![child],
            &extensions.snapshot(),
            &["hook".into()],
            "park-cancel-v1",
            callback,
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeAuthorization::host_attests_replay_safe(),
            AbortAfterAuthorization::host_attests_cleanup_safe(),
            retry.then(ParkCancelRetryAuthorization::host_attests_cancel_replay_safe),
        )
        .unwrap();
        (host, tool)
    }
    fn wait_park_cancel(driver: &crate::task_driver::Driver, id: Uuid) {
        let root = wait_state(driver, id, |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(
                    crate::task::WaitFor::Tasks(_) | crate::task::WaitFor::TasksAny(_)
                )
            )
        });
        let child: AsyncTaskCall =
            serde_json::from_value(root.checkpoint().data["checkpoint"]["call"].clone()).unwrap();
        wait_state(driver, child.task_id(), |s| {
            matches!(
                s,
                crate::task::TaskState::Waiting(crate::task::WaitFor::Signal(_))
            )
        });
    }
    #[test]
    fn parked_cancel_child_first_and_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (host, tool) = park_cancel_host(root);
        let id = host
            .start_replay_safe_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn parked_cancel_unknown_checkpoint_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (host, tool) = park_cancel_host(root);
        let id = host
            .start_replay_safe_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let tasks = crate::task_store::TaskStore::new(&store);
        let mut task = tasks.load(id).unwrap().unwrap();
        let mut checkpoint = task.task.checkpoint().clone();
        checkpoint.data["version"] = json!(99);
        task.task.migrate(
            task.task.definition_version(),
            task.task.input().clone(),
            checkpoint,
        );
        let batch = tasks
            .prepare_save(&[crate::task_store::Save::Update(&task)])
            .unwrap();
        store.commit(&batch.checks, &batch.mutations).unwrap();
        drop(store);
        let (host, _) = park_cancel_host(root);
        let driver = host.into_driver();
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("cancel-count").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
    }
    #[test]
    fn parked_cancel_rejects_valid_checkpoint_substitution() {
        for retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (host, tool) = park_cancel_host_with_retry(root, retry);
            let id = host
                .start_replay_safe_root(&tool, json!({"token":"prepared"}), 3)
                .unwrap();
            let driver = host.into_driver();
            wait_park_cancel(&driver, id);
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            let tasks = crate::task_store::TaskStore::new(&store);
            let mut task = tasks.load(id).unwrap().unwrap();
            let mut checkpoint = task.task.checkpoint().clone();
            checkpoint.data["checkpoint"]["injected"] = json!("valid but different");
            task.task.migrate(
                task.task.definition_version(),
                task.task.input().clone(),
                checkpoint,
            );
            let batch = tasks
                .prepare_save(&[crate::task_store::Save::Update(&task)])
                .unwrap();
            store.commit(&batch.checks, &batch.mutations).unwrap();
            drop(store);
            let (host, _) = park_cancel_host_with_retry(root, retry);
            let driver = host.into_driver();
            driver.request_abort(id).unwrap();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            assert!(!root.join("cancel-count").exists());
        }
    }
    #[test]
    fn parked_cancel_kill_and_reopen_does_not_duplicate() {
        const ENV: &str = "PI_TEST_PARK_CANCEL_FIXTURE";
        if let Ok(root) = std::env::var(ENV) {
            let root = std::path::PathBuf::from(root);
            let (host, tool) = park_cancel_host(&root);
            let id = host
                .start_replay_safe_root(&tool, json!({"token":"prepared"}), 3)
                .unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            let driver = host.into_driver();
            wait_park_cancel(&driver, id);
            driver.request_abort(id).unwrap();
            loop {
                std::hint::black_box(&driver);
                std::thread::park();
            }
        }
        for cut in ["claim", "running", "returned"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            if cut == "running" {
                std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
            }
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("async_task_tool::tests::parked_cancel_kill_and_reopen_does_not_duplicate")
                .env(ENV, root);
            if cut == "returned" {
                command.env("PI_TEST_PARK_CANCEL_AFTER_RETURN", root);
            } else if cut == "claim" {
                command.env("PI_TEST_PARK_CANCEL_AFTER_CLAIM", root);
            }
            let mut process = command.spawn().unwrap();
            wait_file(&root.join(match cut {
                "claim" => "cancel-claimed",
                "returned" => "cancel-returned",
                _ => "cancel-ready",
            }));
            process.kill().unwrap();
            process.wait().unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let (host, _) = park_cancel_host(root);
            let driver = host.into_driver();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Cancelled
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(if cut == "returned" {
                    ParkCancelEvidence::CallbackReturned
                } else {
                    ParkCancelEvidence::AttemptedUnknown
                })
            );
            if cut == "claim" {
                assert!(!root.join("cancel-count").exists());
            } else {
                assert_eq!(
                    std::fs::read_to_string(root.join("cancel-count"))
                        .unwrap()
                        .lines()
                        .count(),
                    1
                );
            }
        }
    }
    #[test]
    fn parked_cancel_deleted_claim_cannot_authorize_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::parked_cancel_kill_and_reopen_does_not_duplicate")
            .env("PI_TEST_PARK_CANCEL_FIXTURE", root)
            .env("PI_TEST_ABORT_AFTER_RECEIPT", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("abort-receipt"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let key = crate::task_scheduler::park_cancel_key(id);
        let entry = store.get(&key).unwrap().unwrap();
        store
            .commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Delete { key }],
            )
            .unwrap();
        drop(store);
        let (host, _) = park_cancel_host(root);
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        assert!(!matches!(task.state(), crate::task::TaskState::Cancelled));
        let _ = driver.shutdown_with_pending().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn retryable_park_cancel_fixture() {
        let Ok(root) = std::env::var("PI_RETRY_PARK_CANCEL") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let (host, tool) = park_cancel_host_with_retry(&root, true);
        let first = !root.join("task-id").exists();
        let id = if first {
            let id = host
                .start_replay_safe_root(&tool, json!({"token":"prepared"}), 3)
                .unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            id
        } else {
            std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap()
        };
        let driver = host.into_driver();
        if first {
            wait_park_cancel(&driver, id);
            if std::env::var_os("PI_TEST_PARK_CANCEL_WAKE").is_some() {
                let task = driver.get(id).unwrap().unwrap();
                let child: AsyncTaskCall =
                    serde_json::from_value(task.checkpoint().data["checkpoint"]["call"].clone())
                        .unwrap();
                driver
                    .signal(&crate::task_store::Signal::new(
                        Uuid::new_v4(),
                        child.task_id(),
                        "release".into(),
                        json!(null),
                    ))
                    .unwrap();
                wait_file(&root.join("wake-running"));
            }
            driver.request_abort(id).unwrap();
        }
        if std::env::var_os("PI_TEST_PARK_CANCEL_WAKE").is_some() {
            let task = wait_terminal(&driver, id);
            std::fs::write(root.join("wake-terminal"), format!("{:?}", task.state())).unwrap();
        }
        if std::env::var_os("PI_TEST_PARK_CANCEL_SPEC_BARRIER").is_some() {
            let task = wait_terminal(&driver, id);
            std::fs::write(root.join("spec-terminal"), format!("{:?}", task.state())).unwrap();
        }
        loop {
            std::hint::black_box(&driver);
            std::thread::park();
        }
    }
    fn kill_retryable_park_cancel(root: &std::path::Path) {
        let _ = std::fs::remove_file(root.join("cancel-ready"));
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::retryable_park_cancel_fixture")
            .env("PI_RETRY_PARK_CANCEL", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("cancel-ready"));
        process.kill().unwrap();
        process.wait().unwrap();
    }
    #[test]
    fn retryable_park_cancel_reopens_and_stops_at_budget() {
        for exhausted in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
            for _ in 0..if exhausted { 3 } else { 2 } {
                kill_retryable_park_cancel(root);
            }
            std::fs::remove_file(root.join("pause-cancel")).unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let (host, _) = park_cancel_host_with_retry(root, true);
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            if exhausted {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            } else {
                assert!(matches!(task.state(), crate::task::TaskState::Cancelled));
            }
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(if exhausted {
                    ParkCancelEvidence::AttemptedUnknown
                } else {
                    ParkCancelEvidence::CallbackReturned
                })
            );
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                3
            );
            let archive = store
                .list(format!("durable/replay-safe-async/{id}/park-cancel-attempt/").as_bytes())
                .unwrap();
            assert_eq!(archive.len(), 2);
        }
    }
    #[test]
    fn retryable_park_cancel_reuses_completed_callback_after_kill() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::retryable_park_cancel_fixture")
            .env("PI_RETRY_PARK_CANCEL", root)
            .env("PI_TEST_PARK_CANCEL_AFTER_RETURN", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("cancel-returned"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = park_cancel_host_with_retry(root, true);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn retryable_park_cancel_does_not_cancel_old_epoch_after_wake() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::retryable_park_cancel_fixture")
            .env("PI_RETRY_PARK_CANCEL", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-terminal"));
        assert!(!root.join("cancel-count").exists());
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
    }
    #[test]
    fn retryable_park_cancel_rechecks_spec_at_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::retryable_park_cancel_fixture")
            .env("PI_RETRY_PARK_CANCEL", root)
            .env("PI_TEST_PARK_CANCEL_SPEC_BARRIER", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("spec-barrier"));
        std::fs::write(root.join("spec-changed"), b"yes").unwrap();
        std::fs::write(root.join("spec-release"), b"yes").unwrap();
        wait_file(&root.join("spec-terminal"));
        assert!(
            std::fs::read_to_string(root.join("spec-terminal"))
                .unwrap()
                .contains("Failed")
        );
        assert!(!root.join("cancel-count").exists());
        process.kill().unwrap();
        process.wait().unwrap();
    }
    #[test]
    fn retryable_park_cancel_rejects_changed_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
        kill_retryable_park_cancel(root);
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let key = crate::task_scheduler::park_cancel_key(id);
        let entry = store.get(&key).unwrap().unwrap();
        let mut claim: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        claim["attempt"] = json!(Uuid::new_v4());
        store
            .commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&claim).unwrap(),
                }],
            )
            .unwrap();
        drop(store);
        let (host, _) = park_cancel_host_with_retry(root, true);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    /// Constructs the discriminator-8, version-1 checkpoint written by a587.
    #[test]
    fn legacy_park_cancel_fixture() {
        let Ok(root) = std::env::var("PI_TEST_LEGACY_PARK_CANCEL") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let (host, tool) = park_cancel_host(&root);
        let id = host
            .start_replay_safe_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        std::fs::write(root.join("legacy-task-id"), id.to_string()).unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        std::fs::write(root.join("legacy-parked"), b"yes").unwrap();
        loop {
            std::hint::black_box(&driver);
            std::thread::park();
        }
    }
    #[test]
    fn legacy_park_cancel_reopen_requires_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::legacy_park_cancel_fixture")
            .env("PI_TEST_LEGACY_PARK_CANCEL", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("legacy-parked"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("legacy-task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let previous = crate::task_store::TaskStore::new(&store)
            .load(id)
            .unwrap()
            .unwrap()
            .task;
        assert_eq!(previous.checkpoint().data["version"], json!(1));
        assert!(previous.checkpoint().data.get("park_attempt").is_none());
        let old_kind = previous.kind().unwrap().to_owned();
        let prefix = format!("durable/replay-safe-async/{id}/");
        let old_evidence = store.list(prefix.as_bytes()).unwrap();
        assert!(
            old_evidence
                .iter()
                .any(|(key, _)| key.ends_with(b"/intent/result"))
        );
        drop(store);
        let (host, tool) = park_cancel_host(root);
        let current_kind = tool.identity().unwrap().kind;
        assert_ne!(current_kind, old_kind);
        let driver = host.into_driver();
        assert!(matches!(
            driver.get(id).unwrap().unwrap().state(),
            crate::task::TaskState::Waiting(_)
        ));
        driver.request_abort(id).unwrap();
        let ended = wait_terminal(&driver, id);
        assert!(
            matches!(ended.state(), crate::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert_eq!(ended.checkpoint(), previous.checkpoint());
        assert!(!root.join("cancel-count").exists());
        assert!(!root.join("abort-hook-count").exists());
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
        for (key, old) in old_evidence {
            assert_eq!(store.get(&key).unwrap().unwrap().value, old.value);
        }
        drop(store);
        let (host, retry_tool) = park_cancel_host_with_retry(root, true);
        assert_ne!(retry_tool.identity().unwrap().kind, old_kind);
        assert_ne!(retry_tool.identity().unwrap().kind, current_kind);
        drop(host);
    }
    #[test]
    fn actual_cross_version_park_cancel_stores_preserve_evidence() {
        let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/park-cancel-cross-version");
        for generation in ["a587", "7f5"] {
            for boundary in ["parked", "claimed", "receipted"] {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path();
                let name = format!("{generation}-{boundary}");
                std::fs::copy(fixtures.join(format!("{name}.sqlite3")), root.join("db")).unwrap();
                let id: Uuid = std::fs::read_to_string(fixtures.join(format!("{name}.task-id")))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                let store = durable_store::Store::open(root.join("db")).unwrap();
                let before = crate::task_store::TaskStore::new(&store)
                    .load(id)
                    .unwrap()
                    .unwrap()
                    .task;
                assert_eq!(before.checkpoint().phase, "replay-safe-async-wait");
                assert_eq!(
                    before.checkpoint().data["version"],
                    if generation == "a587" {
                        json!(1)
                    } else {
                        json!(2)
                    }
                );
                assert_eq!(
                    before.checkpoint().data.get("park_attempt").is_some(),
                    generation == "7f5"
                );
                assert_eq!(before.abort_requested(), boundary != "parked");
                let prefix = format!("durable/replay-safe-async/{id}/");
                let records = store.list(prefix.as_bytes()).unwrap();
                assert!(
                    records
                        .iter()
                        .any(|(key, _)| key.ends_with(b"/intent/result"))
                );
                assert_eq!(
                    records
                        .iter()
                        .any(|(key, _)| key.ends_with(b"/park-cancel-claim")),
                    boundary != "parked"
                );
                assert_eq!(
                    records
                        .iter()
                        .any(|(key, _)| key.ends_with(b"/abort-after")),
                    boundary == "receipted"
                );
                drop(store);
                let (host, tool) = park_cancel_host(root);
                assert_ne!(before.kind().unwrap(), tool.identity().unwrap().kind);
                let driver = host.into_driver();
                if boundary == "parked" {
                    driver.request_abort(id).unwrap();
                }
                let ended = wait_terminal(&driver, id);
                assert!(
                    matches!(ended.state(),crate::task::TaskState::Failed(reason) if reason.contains("reconciliation")),
                    "{name}: {:?}",
                    ended.state()
                );
                assert_eq!(ended.checkpoint(), before.checkpoint());
                assert!(!root.join("cancel-count").exists(), "{name}");
                assert!(!root.join("abort-hook-count").exists(), "{name}");
                let _ = driver.shutdown_with_pending().unwrap();
                let store = durable_store::Store::open(root.join("db")).unwrap();
                for (key, old) in records {
                    assert_eq!(store.get(&key).unwrap().unwrap().value, old.value, "{name}");
                }
                let evidence = read_park_cancel_evidence(&store, id).unwrap();
                assert_eq!(
                    evidence,
                    match boundary {
                        "parked" => None,
                        "claimed" => Some(ParkCancelEvidence::AttemptedUnknown),
                        _ => Some(ParkCancelEvidence::CallbackReturned),
                    },
                    "{name}"
                );
                assert!(read_aborted_after_result(&store, id).unwrap().is_none());
            }
        }
    }
    #[test]
    fn unsafe_park_cancel_child_first_preserves_evidence() {
        for retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let (host, tool) = unsafe_park_cancel_host(root, retry);
            let id = host
                .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
                .unwrap();
            let driver = host.into_driver();
            wait_park_cancel(&driver, id);
            driver.request_abort(id).unwrap();
            let ended = wait_terminal(&driver, id);
            assert!(
                matches!(ended.state(), crate::task::TaskState::Cancelled),
                "{:?}",
                ended.state()
            );
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(ParkCancelEvidence::CallbackReturned)
            );
            assert_eq!(
                read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
                1
            );
            assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn unsafe_park_cancel_process_fixture() {
        let Ok(root) = std::env::var("PI_UNSAFE_PARK_CANCEL_FIXTURE") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let retry = root.join("retry").exists();
        let (host, tool) = unsafe_park_cancel_host(&root, retry);
        let first = !root.join("task-id").exists();
        let id = if first {
            let id = host
                .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
                .unwrap();
            std::fs::write(root.join("task-id"), id.to_string()).unwrap();
            id
        } else {
            std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap()
        };
        let driver = host.into_driver();
        if first {
            wait_park_cancel(&driver, id);
        }
        if first && std::env::var_os("PI_TEST_PARK_CANCEL_WAKE").is_some() {
            let task = driver.get(id).unwrap().unwrap();
            let child: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["call"].clone())
                    .unwrap();
            driver
                .signal(&crate::task_store::Signal::new(
                    Uuid::new_v4(),
                    child.task_id(),
                    "release".into(),
                    json!(null),
                ))
                .unwrap();
            wait_file(&root.join("wake-running"));
        }
        if first {
            driver.request_abort(id).unwrap();
        }
        if std::env::var_os("PI_TEST_PARK_CANCEL_WAKE").is_some() {
            let task = wait_terminal(&driver, id);
            std::fs::write(root.join("wake-terminal"), format!("{:?}", task.state())).unwrap();
        }
        if std::env::var_os("PI_TEST_PARK_CANCEL_SPEC_BARRIER").is_some() {
            let task = wait_terminal(&driver, id);
            std::fs::write(root.join("spec-terminal"), format!("{:?}", task.state())).unwrap();
        }
        loop {
            std::hint::black_box(&driver);
            std::thread::park();
        }
    }
    #[test]
    fn unsafe_park_cancel_kill_reopen_obeys_retry_policy() {
        for retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            if retry {
                std::fs::write(root.join("retry"), b"yes").unwrap();
            }
            std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
                .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("cancel-ready"));
            process.kill().unwrap();
            process.wait().unwrap();
            std::fs::remove_file(root.join("pause-cancel")).unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let before_store = durable_store::Store::open(root.join("db")).unwrap();
            let before = read_unsafe_durable_park_evidence(&before_store, id).unwrap();
            let receipts = read_unsafe_durable_receipts(&before_store, id).unwrap();
            drop(before_store);
            let (host, _) = unsafe_park_cancel_host(root, retry);
            let driver = host.into_driver();
            let ended = wait_terminal(&driver, id);
            if retry {
                assert!(matches!(ended.state(), crate::task::TaskState::Cancelled));
            } else {
                assert!(
                    matches!(ended.state(),crate::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
                );
            }
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_unsafe_durable_park_evidence(&store, id).unwrap(),
                before
            );
            assert_eq!(read_unsafe_durable_receipts(&store, id).unwrap(), receipts);
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(if retry {
                    ParkCancelEvidence::CallbackReturned
                } else {
                    ParkCancelEvidence::AttemptedUnknown
                })
            );
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                if retry { 2 } else { 1 }
            );
        }
    }
    #[test]
    fn unsafe_park_cancel_rejects_old_woken_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
            .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-terminal"));
        assert!(!root.join("cancel-count").exists());
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
        assert_eq!(
            read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
            1
        );
    }
    #[test]
    fn unsafe_park_cancel_reuses_abort_receipt_after_kill() {
        for retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            if retry {
                std::fs::write(root.join("retry"), b"yes").unwrap();
            }
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
                .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
                .env("PI_TEST_ABORT_AFTER_RECEIPT", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("abort-receipt"));
            process.kill().unwrap();
            process.wait().unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let (host, _) = unsafe_park_cancel_host(root, retry);
            let driver = host.into_driver();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Cancelled
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert!(read_aborted_after_result(&store, id).unwrap().is_some());
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(ParkCancelEvidence::CallbackReturned)
            );
            assert_eq!(
                read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
                1
            );
            assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            assert_eq!(
                std::fs::read_to_string(root.join("abort-hook-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn unsafe_park_cancel_rejects_existing_tool_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (host, tool) = unsafe_park_cancel_host(root, false);
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let task = crate::task_store::TaskStore::new(&store)
            .load(id)
            .unwrap()
            .unwrap()
            .task;
        let attempt: Uuid =
            serde_json::from_value(task.checkpoint().data["park_attempt"].clone()).unwrap();
        let intent_key = format!("durable/unsafe-park-async/{id}/{attempt}/intent").into_bytes();
        let entry = store.get(&intent_key).unwrap().unwrap();
        let intent: Task = serde_json::from_slice(&entry.value).unwrap();
        let (checks, mutations) = unsafe_park_proof::prepare_receipt(
            &store,
            &intent,
            &intent_key,
            &Outcome::Succeeded("known external outcome".into()),
        )
        .unwrap();
        store.commit(&checks, &mutations).unwrap();
        let before = read_unsafe_durable_receipts(&store, id).unwrap();
        drop(store);
        let (host, _) = unsafe_park_cancel_host(root, false);
        let driver = host.into_driver();
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(read_unsafe_durable_receipts(&store, id).unwrap(), before);
        assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
        assert!(!root.join("cancel-count").exists());
    }
    #[test]
    fn unsafe_park_cancel_runs_after_nested_children_on_ancestor_abort() {
        for parent_safe in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let slot: UnsafeCancelOuterSlot = Arc::new(std::sync::OnceLock::new());
            let (host, tool) = unsafe_park_cancel_host_inner(
                root,
                false,
                Some((slot.clone(), parent_safe)),
                false,
            );
            assert!(slot.set(tool).is_ok());
            let scheduler = host.into_scheduler_for_test();
            let parent = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("unsafe-cancel-outer".into(), parent_safe)
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let waiting = wait_state(&driver, parent.id(), |s| {
                matches!(s, crate::task::TaskState::Waiting(_))
            });
            let child: crate::task_tool::NestedTaskCall =
                serde_json::from_value(waiting.checkpoint().data["call"].clone()).unwrap();
            wait_park_cancel(&driver, child.task_id());
            driver.request_abort(parent.id()).unwrap();
            assert!(matches!(
                wait_terminal(&driver, parent.id()).state(),
                crate::task::TaskState::Cancelled
            ));
            assert!(matches!(
                wait_terminal(&driver, child.task_id()).state(),
                crate::task::TaskState::Cancelled
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, child.task_id()).unwrap(),
                Some(ParkCancelEvidence::CallbackReturned)
            );
            assert_eq!(
                read_unsafe_durable_park_evidence(&store, child.task_id())
                    .unwrap()
                    .len(),
                1
            );
            assert!(
                read_unsafe_durable_receipts(&store, child.task_id())
                    .unwrap()
                    .is_empty()
            );
        }
    }
    #[test]
    fn unsafe_park_cancel_retry_budget_preserves_uncertainty() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("retry"), b"yes").unwrap();
        std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
        for _ in 0..3 {
            let _ = std::fs::remove_file(root.join("cancel-ready"));
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
                .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("cancel-ready"));
            process.kill().unwrap();
            process.wait().unwrap();
        }
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = unsafe_park_cancel_host(root, true);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::AttemptedUnknown)
        );
        assert_eq!(
            read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
            1
        );
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            3
        );
    }
    #[test]
    fn unsafe_park_cancel_rejects_changed_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (host, tool) = unsafe_park_cancel_host(root, false);
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let tasks = crate::task_store::TaskStore::new(&store);
        let mut task = tasks.load(id).unwrap().unwrap();
        let mut checkpoint = task.task.checkpoint().clone();
        checkpoint.data["checkpoint"]["changed"] = json!(true);
        task.task.migrate(
            task.task.definition_version(),
            task.task.input().clone(),
            checkpoint,
        );
        let batch = tasks
            .prepare_save(&[crate::task_store::Save::Update(&task)])
            .unwrap();
        store.commit(&batch.checks, &batch.mutations).unwrap();
        drop(store);
        let (host, _) = unsafe_park_cancel_host(root, false);
        let driver = host.into_driver();
        driver.request_abort(id).unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("cancel-count").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
            1
        );
    }
    #[test]
    fn unsafe_park_cancel_rechecks_late_receipt_before_callback() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
            .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_SPEC_BARRIER", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("spec-barrier"));
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let task = crate::task_store::TaskStore::new(&store)
            .load(id)
            .unwrap()
            .unwrap()
            .task;
        let attempt: Uuid =
            serde_json::from_value(task.checkpoint().data["park_attempt"].clone()).unwrap();
        let key = format!("durable/unsafe-park-async/{id}/{attempt}/intent").into_bytes();
        let entry = store.get(&key).unwrap().unwrap();
        let intent: Task = serde_json::from_slice(&entry.value).unwrap();
        let (checks, mutations) = unsafe_park_proof::prepare_receipt(
            &store,
            &intent,
            &key,
            &Outcome::Succeeded("late evidence".into()),
        )
        .unwrap();
        store.commit(&checks, &mutations).unwrap();
        let before = read_unsafe_durable_receipts(&store, id).unwrap();
        drop(store);
        std::fs::write(root.join("spec-release"), b"yes").unwrap();
        wait_file(&root.join("spec-terminal"));
        assert!(
            std::fs::read_to_string(root.join("spec-terminal"))
                .unwrap()
                .contains("Failed")
        );
        assert!(!root.join("cancel-count").exists());
        process.kill().unwrap();
        process.wait().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(read_unsafe_durable_receipts(&store, id).unwrap(), before);
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::AttemptedUnknown)
        );
    }
    #[test]
    fn unsafe_park_cancel_missing_or_changed_definition_does_not_reexecute() {
        for changed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
                .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("cancel-ready"));
            process.kill().unwrap();
            process.wait().unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let driver = if changed {
                unsafe_park_cancel_host(root, true).0.into_driver()
            } else {
                StandaloneBeforeHookHost::open(root.join("db"), Registry::default())
                    .unwrap()
                    .into_driver()
            };
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(ParkCancelEvidence::AttemptedUnknown)
            );
            assert_eq!(
                read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
                1
            );
            assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn unsafe_park_cancel_never_uses_uncommitted_proposal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let (host, tool) = unsafe_park_cancel_host(root, false);
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let (entered, receiver) = std::sync::mpsc::channel();
        let barrier = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let worker_barrier = barrier.clone();
        replay_safe::after_unsafe_evidence(id, move || {
            entered.send(()).unwrap();
            let (lock, wake) = &*worker_barrier;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        });
        let driver = host.into_driver();
        receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        driver.request_abort(id).unwrap();
        let (lock, wake) = &*barrier;
        *lock.lock().unwrap() = true;
        wake.notify_all();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
            1
        );
        assert!(
            store
                .get(format!("durable/unsafe-park-async/{id}/park").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(read_park_cancel_evidence(&store, id).unwrap().is_none());
        assert!(!root.join("cancel-count").exists());
    }
    #[test]
    fn unsafe_park_cancel_callback_failure_requires_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("fail-cancel"), b"yes").unwrap();
        let (host, tool) = unsafe_park_cancel_host(root, false);
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        driver.request_abort(id).unwrap();
        assert!(
            matches!(wait_terminal(&driver,id).state(),crate::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::AttemptedUnknown)
        );
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
        assert!(!root.join("abort-hook-count").exists());
    }
    #[test]
    fn unsafe_park_cancel_late_receipt_while_callback_paused_requires_reconciliation() {
        for retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
            let (host, tool) = unsafe_park_cancel_host(root, retry);
            let id = host
                .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
                .unwrap();
            let scheduler = host.into_scheduler_for_test();
            let owner = scheduler.owner();
            let driver = crate::task_driver::Driver::start(scheduler);
            wait_park_cancel(&driver, id);
            driver.request_abort(id).unwrap();
            wait_file(&root.join("cancel-ready"));
            assert!(!root.join("cancel-returning").exists());
            let store = owner.store();
            let task = crate::task_store::TaskStore::new(store)
                .load(id)
                .unwrap()
                .unwrap()
                .task;
            let attempt: Uuid =
                serde_json::from_value(task.checkpoint().data["park_attempt"].clone()).unwrap();
            let key = format!("durable/unsafe-park-async/{id}/{attempt}/intent").into_bytes();
            let entry = store.get(&key).unwrap().unwrap();
            let intent: Task = serde_json::from_slice(&entry.value).unwrap();
            let expected = Outcome::Succeeded("joined late Tool outcome".into());
            let (checks, mutations) =
                unsafe_park_proof::prepare_receipt(store, &intent, &key, &expected).unwrap();
            store.commit(&checks, &mutations).unwrap();
            let receipt = read_unsafe_durable_receipts(store, id).unwrap();
            assert_eq!(receipt.len(), 1);
            assert_eq!(receipt[0].outcome, expected);
            std::fs::write(root.join("cancel-release"), b"yes").unwrap();
            wait_file(&root.join("cancel-returning"));
            let ended = wait_terminal(&driver, id);
            assert!(
                matches!(ended.state(), crate::task::TaskState::Failed(reason) if reason.contains("reconciliation")),
                "retry={retry}: {:?}",
                ended.state()
            );
            let _ = driver.shutdown_with_pending().unwrap();
            assert_eq!(read_unsafe_durable_receipts(store, id).unwrap(), receipt);
            assert_eq!(
                read_park_cancel_evidence(store, id).unwrap(),
                Some(ParkCancelEvidence::AttemptedUnknown)
            );
            assert!(read_aborted_after_result(store, id).unwrap().is_none());
            assert!(!root.join("abort-hook-count").exists());
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn unsafe_park_cancel_late_receipt_after_callback_wins_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_park_cancel_process_fixture")
            .env("PI_UNSAFE_PARK_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_AFTER_RETURN", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("cancel-returned"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let task = crate::task_store::TaskStore::new(&store)
            .load(id)
            .unwrap()
            .unwrap()
            .task;
        let attempt: Uuid =
            serde_json::from_value(task.checkpoint().data["park_attempt"].clone()).unwrap();
        let key = format!("durable/unsafe-park-async/{id}/{attempt}/intent").into_bytes();
        let entry = store.get(&key).unwrap().unwrap();
        let intent: Task = serde_json::from_slice(&entry.value).unwrap();
        let (checks, mutations) = unsafe_park_proof::prepare_receipt(
            &store,
            &intent,
            &key,
            &Outcome::Succeeded("late known result".into()),
        )
        .unwrap();
        store.commit(&checks, &mutations).unwrap();
        let before = read_unsafe_durable_receipts(&store, id).unwrap();
        drop(store);
        let (host, _) = unsafe_park_cancel_host(root, false);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(read_unsafe_durable_receipts(&store, id).unwrap(), before);
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert!(!root.join("abort-hook-count").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn unsafe_woken_cancel_process_fixture() {
        let Ok(root) = std::env::var("PI_UNSAFE_WOKEN_CANCEL_FIXTURE") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let (host, tool) =
            unsafe_park_cancel_host_inner(&root, root.join("retry").exists(), None, true);
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        std::fs::write(root.join("task-id"), id.to_string()).unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        let task = driver.get(id).unwrap().unwrap();
        let child: AsyncTaskCall =
            serde_json::from_value(task.checkpoint().data["checkpoint"]["call"].clone()).unwrap();
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                child.task_id(),
                "release".into(),
                json!(null),
            ))
            .unwrap();
        wait_file(&root.join("wake-running"));
        if root.join("abort-woken").exists() {
            driver.request_abort(id).unwrap();
            let task = wait_terminal(&driver, id);
            std::fs::write(root.join("woken-terminal"), format!("{:?}", task.state())).unwrap();
        }
        loop {
            std::hint::black_box(&driver);
            std::thread::park();
        }
    }
    #[test]
    fn unsafe_woken_cancel_running_abort_and_crash_reopen() {
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            std::fs::write(root.join("woken-case"), b"yes").unwrap();
            if abort {
                std::fs::write(root.join("abort-woken"), b"yes").unwrap();
            }
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
                .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
                .env("PI_TEST_PARK_CANCEL_WAKE", root)
                .spawn()
                .unwrap();
            wait_file(&root.join(if abort {
                "woken-terminal"
            } else {
                "wake-running"
            }));
            process.kill().unwrap();
            process.wait().unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let (host, _) = unsafe_park_cancel_host_inner(root, false, None, true);
            let driver = host.into_driver();
            let task = wait_terminal(&driver, id);
            if abort {
                assert!(
                    matches!(task.state(), crate::task::TaskState::Cancelled),
                    "{:?}",
                    task.state()
                );
            } else {
                assert!(
                    matches!(task.state(),crate::task::TaskState::Failed(reason) if reason.contains("reconciliation")),
                    "{:?}",
                    task.state()
                );
            }
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(ParkCancelEvidence::CallbackReturned)
            );
            assert_eq!(
                read_unsafe_durable_park_evidence(&store, id).unwrap().len(),
                1
            );
            assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
        }
    }
    #[test]
    fn woken_cancel_identity_lease_rejects_reentry_and_concurrent_update() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for marker in [
            "woken-case",
            "any-case",
            "capture-identity-handle",
            "pause-cancel",
            "reenter-update",
        ] {
            std::fs::write(root.join(marker), b"yes").unwrap();
        }
        let (host, tool) = unsafe_park_cancel_host_inner(root, false, None, true);
        let handle = WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(root)
            .unwrap();
        // Keep the handle visible to the callback for the reentrant update.
        WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .insert(root.to_owned(), handle.clone());
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let reservation_blocked = Arc::new(AtomicBool::new(false));
        let observed_reservation = reservation_blocked.clone();
        let reservation_handle = handle.clone();
        crate::task_scheduler::after_woken_abort_reservation_for_test(id, move || {
            use futures_util::FutureExt;
            let result = reservation_handle
                .update(Some("changed".into()), || {
                    panic!("changed after reservation")
                })
                .now_or_never()
                .expect("update must not wait");
            observed_reservation.store(result.is_err(), Ordering::SeqCst);
        });
        let dispatch_blocked = Arc::new(AtomicBool::new(false));
        let observed = dispatch_blocked.clone();
        let dispatch_handle = handle.clone();
        replay_safe::before_woken_cancel_dispatch(id, move || {
            use futures_util::FutureExt;
            let result = dispatch_handle
                .update(Some("changed".into()), || panic!("changed before callback"))
                .now_or_never()
                .expect("update must not wait");
            observed.store(result.is_err(), Ordering::SeqCst);
        });
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        let task = driver.get(id).unwrap().unwrap();
        let child: AsyncTaskCall =
            serde_json::from_value(task.checkpoint().data["checkpoint"]["call"].clone()).unwrap();
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                child.task_id(),
                "release".into(),
                json!(null),
            ))
            .unwrap();
        wait_file(&root.join("wake-running"));
        driver.request_abort(id).unwrap();
        wait_file(&root.join("cancel-ready"));
        assert!(reservation_blocked.load(Ordering::SeqCst));
        assert!(dispatch_blocked.load(Ordering::SeqCst));
        assert_eq!(std::fs::read(root.join("reenter-result")).unwrap(), b"busy");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let start = std::time::Instant::now();
        let changed = Arc::new(AtomicBool::new(false));
        let flag = changed.clone();
        assert!(
            runtime
                .block_on(handle.update(Some("changed".into()), move || {
                    flag.store(true, Ordering::SeqCst)
                }))
                .is_err()
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        assert!(!changed.load(Ordering::SeqCst));
        assert!(driver.get(id).unwrap().is_some());
        std::fs::write(root.join("cancel-release"), b"yes").unwrap();
        wait_terminal(&driver, id);
        runtime
            .block_on(handle.update(Some("changed".into()), || {}))
            .unwrap();
        let _ = driver.shutdown_with_pending().unwrap();
        WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(root);
    }
    #[test]
    fn woken_cancel_registration_requires_identity_binding() {
        let dir = tempfile::tempdir().unwrap();
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(WokenAnyTool(dir.path().to_owned()));
        let result = host.register_unsafe_durable_tool_with_woken_cancel(
            tool,
            "tool-v1",
            1,
            vec![],
            &crate::extension::Registry::default().snapshot(),
            &[],
            "cancel-v1",
            Arc::new(ParkCancelFixture(dir.path().to_owned())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeAuthorization::host_attests_replay_safe(),
            AbortAfterAuthorization::host_attests_cleanup_safe(),
            None,
        );
        assert!(
            matches!(result, Err(error) if error.contains("requires a registered identity lease"))
        );
    }
    #[test]
    fn unsafe_woken_cancel_late_receipt_before_callback_is_reconcilable() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("woken-case"), b"yes").unwrap();
        std::fs::write(root.join("abort-woken"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .env("PI_TEST_PARK_CANCEL_SPEC_BARRIER", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("spec-barrier"));
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let prefix = format!("durable/unsafe-park-async/{id}/");
        let mut target = None;
        for (key, entry) in store.list(prefix.as_bytes()).unwrap() {
            if !key.ends_with(b"/intent") {
                continue;
            }
            let task: Task = serde_json::from_slice(&entry.value).unwrap();
            if task.ended_children().is_some() {
                target = Some((key, task));
            }
        }
        let (key, intent) = target.expect("woken intent");
        let (checks, mutations) = unsafe_park_proof::prepare_receipt(
            &store,
            &intent,
            &key,
            &Outcome::Succeeded("external evidence".into()),
        )
        .unwrap();
        store.commit(&checks, &mutations).unwrap();
        let expected = read_unsafe_durable_receipts(&store, id).unwrap();
        drop(store);
        std::fs::write(root.join("spec-release"), b"yes").unwrap();
        wait_file(&root.join("woken-terminal"));
        assert!(
            std::fs::read_to_string(root.join("woken-terminal"))
                .unwrap()
                .contains("Failed")
        );
        assert!(!root.join("cancel-count").exists());
        process.kill().unwrap();
        process.wait().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(read_unsafe_durable_receipts(&store, id).unwrap(), expected);
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::AttemptedUnknown)
        );
    }
    #[test]
    fn unsafe_woken_cancel_late_receipt_after_callback_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("woken-case"), b"yes").unwrap();
        std::fs::write(root.join("abort-woken"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .env("PI_TEST_PARK_CANCEL_AFTER_RETURN", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("cancel-returned"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let prefix = format!("durable/unsafe-park-async/{id}/");
        let mut target = None;
        for (key, entry) in store.list(prefix.as_bytes()).unwrap() {
            if !key.ends_with(b"/intent") {
                continue;
            }
            let task: Task = serde_json::from_slice(&entry.value).unwrap();
            if task.ended_children().is_some() {
                target = Some((key, task));
            }
        }
        let (key, intent) = target.expect("woken intent");
        let (checks, mutations) = unsafe_park_proof::prepare_receipt(
            &store,
            &intent,
            &key,
            &Outcome::Succeeded("external result".into()),
        )
        .unwrap();
        store.commit(&checks, &mutations).unwrap();
        let expected = read_unsafe_durable_receipts(&store, id).unwrap();
        drop(store);
        let (host, _) = unsafe_park_cancel_host_inner(root, false, None, true);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(read_unsafe_durable_receipts(&store, id).unwrap(), expected);
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert!(!root.join("abort-hook-count").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn unsafe_woken_cancel_rejects_changed_attempt_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("woken-case"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-running"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        let prefix = format!("durable/unsafe-park-async/{id}/");
        for (key, entry) in store.list(prefix.as_bytes()).unwrap() {
            if !key.ends_with(b"/intent") {
                continue;
            }
            let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            if value.get("ended_children").is_some() {
                value["attempt_id"] = json!(Uuid::new_v4());
                store
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: serde_json::to_vec(&value).unwrap(),
                        }],
                    )
                    .unwrap();
                break;
            }
        }
        drop(store);
        let (host, _) = unsafe_park_cancel_host_inner(root, false, None, true);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(!root.join("cancel-count").exists());
    }
    #[test]
    fn unsafe_woken_cancel_resume_fixture() {
        let Ok(root) = std::env::var("PI_UNSAFE_WOKEN_CANCEL_RESUME") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let (host, _) =
            unsafe_park_cancel_host_inner(&root, root.join("retry").exists(), None, true);
        let driver = host.into_driver();
        loop {
            std::hint::black_box(&driver);
            std::thread::park();
        }
    }
    #[test]
    fn unsafe_woken_cancel_cleanup_kill_reopen_obeys_retry_policy() {
        for retry in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            std::fs::write(root.join("woken-case"), b"yes").unwrap();
            if retry {
                std::fs::write(root.join("retry"), b"yes").unwrap();
            }
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
                .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
                .env("PI_TEST_PARK_CANCEL_WAKE", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("wake-running"));
            process.kill().unwrap();
            process.wait().unwrap();
            std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_woken_cancel_resume_fixture")
                .env("PI_UNSAFE_WOKEN_CANCEL_RESUME", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("cancel-ready"));
            process.kill().unwrap();
            process.wait().unwrap();
            std::fs::remove_file(root.join("pause-cancel")).unwrap();
            let id: Uuid = std::fs::read_to_string(root.join("task-id"))
                .unwrap()
                .parse()
                .unwrap();
            let (host, _) = unsafe_park_cancel_host_inner(root, retry, None, true);
            let driver = host.into_driver();
            assert!(matches!(
                wait_terminal(&driver, id).state(),
                crate::task::TaskState::Failed(_)
            ));
            let _ = driver.shutdown_with_pending().unwrap();
            let store = durable_store::Store::open(root.join("db")).unwrap();
            assert_eq!(
                read_park_cancel_evidence(&store, id).unwrap(),
                Some(if retry {
                    ParkCancelEvidence::CallbackReturned
                } else {
                    ParkCancelEvidence::AttemptedUnknown
                })
            );
            assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.join("cancel-count"))
                    .unwrap()
                    .lines()
                    .count(),
                if retry { 2 } else { 1 }
            );
        }
    }
    #[test]
    fn unsafe_woken_cancel_retry_budget_is_finite() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("woken-case"), b"yes").unwrap();
        std::fs::write(root.join("retry"), b"yes").unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-running"));
        process.kill().unwrap();
        process.wait().unwrap();
        std::fs::write(root.join("pause-cancel"), b"yes").unwrap();
        for _ in 0..3 {
            let _ = std::fs::remove_file(root.join("cancel-ready"));
            let mut process = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("async_task_tool::tests::unsafe_woken_cancel_resume_fixture")
                .env("PI_UNSAFE_WOKEN_CANCEL_RESUME", root)
                .spawn()
                .unwrap();
            wait_file(&root.join("cancel-ready"));
            process.kill().unwrap();
            process.wait().unwrap();
        }
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = unsafe_park_cancel_host_inner(root, true, None, true);
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::AttemptedUnknown)
        );
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            3
        );
    }
    #[test]
    fn unsafe_woken_crash_drains_live_any_child_before_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for marker in ["woken-case", "any-case", "require-child-aborted"] {
            std::fs::write(root.join(marker), b"yes").unwrap();
        }
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-running"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        let (host, _) = unsafe_park_cancel_host_inner(root, false, None, true);
        let driver = host.into_driver();
        let task = wait_terminal(&driver, id);
        assert!(
            matches!(task.state(),crate::task::TaskState::Failed(reason) if reason.contains("reconciliation")),
            "{:?}",
            task.state()
        );
        let _ = driver.shutdown_with_pending().unwrap();
        assert!(root.join("child-aborted").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(
            read_park_cancel_evidence(&store, id).unwrap(),
            Some(ParkCancelEvidence::CallbackReturned)
        );
        assert!(read_unsafe_durable_receipts(&store, id).unwrap().is_empty());
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn woken_recovery_identity_change_does_not_abort_live_any_child() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for marker in [
            "woken-case",
            "any-case",
            "capture-identity-handle",
            "require-child-aborted",
        ] {
            std::fs::write(root.join(marker), b"yes").unwrap();
        }
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-running"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        std::fs::write(root.join("separate-identity-db"), b"yes").unwrap();
        let (host, _) = unsafe_park_cancel_host_inner(root, false, None, true);
        let registry = host.into_scheduler_for_test().registry_for_test();
        let handle = WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(root)
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(handle.update(Some("test-woken-cancel-v1".into()), || {
                std::fs::write(root.join("any-spec-changed"), b"yes").unwrap();
            }))
            .unwrap();
        let scheduler =
            crate::task_scheduler::Scheduler::open(root.join("db"), registry.clone()).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        std::thread::sleep(std::time::Duration::from_millis(500));
        let task = driver.get(id).unwrap().unwrap();
        assert_eq!(task.state(), &crate::task::TaskState::Running);
        let child: AsyncTaskCall =
            serde_json::from_value(task.checkpoint().data["checkpoint"]["other"].clone()).unwrap();
        assert!(
            !driver
                .get(child.task_id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        assert!(!root.join("child-aborted").exists());
        assert!(!root.join("cancel-count").exists());
        let _ = driver.shutdown_with_pending().unwrap();
        runtime
            .block_on(handle.update(Some("test-woken-cancel-v1".into()), || {
                std::fs::remove_file(root.join("any-spec-changed")).unwrap()
            }))
            .unwrap();
        let blocked = Arc::new(AtomicBool::new(false));
        let observed = blocked.clone();
        crate::task_scheduler::before_woken_recovery_commit_for_test(id, move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(handle.update(Some("other".into()), || {
                panic!("identity changed during recovery commit")
            }));
            observed.store(result.is_err(), Ordering::SeqCst);
        });
        let scheduler = crate::task_scheduler::Scheduler::open(root.join("db"), registry).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert!(root.join("child-aborted").exists());
        assert!(root.join("cancel-count").exists());
        assert!(blocked.load(Ordering::SeqCst));
        let _ = driver.shutdown_with_pending().unwrap();
        WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(root);
    }
    // Hold the host's update in progress without changing its identity. The
    // rendezvous ensures the driver observes an actual write-lease conflict.
    fn hold_woken_identity_update(
        handle: crate::nested_identity::IdentityHandle,
    ) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime
                .block_on(handle.update(Some("test-woken-cancel-v1".into()), || {
                    entered_tx.send(()).unwrap();
                    let _ = release_rx.recv_timeout(std::time::Duration::from_secs(10));
                }))
                .unwrap();
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        (release_tx, thread)
    }
    #[test]
    fn woken_recovery_busy_is_retried_by_driver_after_update() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for marker in [
            "woken-case",
            "any-case",
            "capture-identity-handle",
            "require-child-aborted",
        ] {
            std::fs::write(root.join(marker), b"yes").unwrap();
        }
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::unsafe_woken_cancel_process_fixture")
            .env("PI_UNSAFE_WOKEN_CANCEL_FIXTURE", root)
            .env("PI_TEST_PARK_CANCEL_WAKE", root)
            .spawn()
            .unwrap();
        wait_file(&root.join("wake-running"));
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(root.join("task-id"))
            .unwrap()
            .parse()
            .unwrap();
        std::fs::write(root.join("separate-identity-db"), b"yes").unwrap();
        let (host, _) = unsafe_park_cancel_host_inner(root, false, None, true);
        let registry = host.into_scheduler_for_test().registry_for_test();
        let handle = WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(root)
            .unwrap();
        let (release, update) = hold_woken_identity_update(handle);
        let (busy_tx, busy_rx) = std::sync::mpsc::channel();
        crate::task_scheduler::on_woken_identity_busy_for_test(id, move || {
            busy_tx.send(()).unwrap();
        });
        let scheduler = crate::task_scheduler::Scheduler::open(root.join("db"), registry).unwrap();
        busy_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert_eq!(
            driver.get(id).unwrap().unwrap().state(),
            &crate::task::TaskState::Running
        );
        assert!(!root.join("cancel-count").exists());
        release.send(()).unwrap();
        update.join().unwrap();
        assert!(
            matches!(wait_terminal(&driver, id).state(), crate::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert!(root.join("child-aborted").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn woken_abort_busy_after_child_cleanup_defers_without_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for marker in [
            "woken-case",
            "any-case",
            "capture-identity-handle",
            "require-child-aborted",
        ] {
            std::fs::write(root.join(marker), b"yes").unwrap();
        }
        let (host, tool) = unsafe_park_cancel_host_inner(root, false, None, true);
        let handle = WOKEN_TEST_IDENTITIES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(root)
            .unwrap();
        let id = host
            .start_unsafe_durable_root(&tool, json!({"token":"prepared"}), 3)
            .unwrap();
        let driver = host.into_driver();
        wait_park_cancel(&driver, id);
        let task = driver.get(id).unwrap().unwrap();
        let child: AsyncTaskCall =
            serde_json::from_value(task.checkpoint().data["checkpoint"]["call"].clone()).unwrap();
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                child.task_id(),
                "release".into(),
                json!(null),
            ))
            .unwrap();
        wait_file(&root.join("wake-running"));
        let (release, update) = hold_woken_identity_update(handle);
        let (busy_tx, busy_rx) = std::sync::mpsc::channel();
        crate::task_scheduler::on_woken_identity_busy_for_test(id, move || {
            busy_tx.send(()).unwrap();
        });
        driver.request_abort(id).unwrap();
        busy_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(root.join("child-aborted").exists());
        let task = driver.get(id).unwrap().unwrap();
        assert!(task.abort_requested());
        assert_eq!(task.state(), &crate::task::TaskState::Ready);
        assert!(!root.join("cancel-count").exists());
        let store = durable_store::Store::open(root.join("db")).unwrap();
        assert_eq!(read_park_cancel_evidence(&store, id).unwrap(), None);
        drop(store);
        release.send(()).unwrap();
        update.join().unwrap();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(
            std::fs::read_to_string(root.join("cancel-count"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let _ = driver.shutdown_with_pending().unwrap();
    }
    struct SuppressGate;
    #[async_trait::async_trait]
    impl crate::extension::Hook for SuppressGate {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            Ok(crate::extension::BeforeTool::Block("denied".into()))
        }
        async fn after_tool_blocks(
            &self,
            _: &crate::extension::ToolCall,
            input: &crate::extension::ToolBlockResult,
            _: bool,
            _: &crate::extension::Context<'_>,
        ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
            let mut out = input.clone();
            out.diagnostics.clear();
            Ok(Some(out))
        }
    }
    fn single_content_host(
        path: &std::path::Path,
        tool: Arc<dyn DurableAsyncTool>,
        hook: Arc<dyn crate::extension::Hook>,
    ) -> (StandaloneBeforeHookHost, Arc<crate::task_tool::TaskTool>) {
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "hook".into(),
                    hooks: vec![hook],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(1, "hook-v1".into(), vec!["hook".into()])
                    .unwrap(),
            )
            .unwrap();
        let mut host = StandaloneBeforeHookHost::open(path, Registry::default()).unwrap();
        let adapter = host
            .register_replay_safe_durable_tool_with_content_hooks(
                tool,
                "single-v1",
                1,
                vec![],
                &extensions.snapshot(),
                &["hook".into()],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, adapter)
    }
    #[test]
    fn durable_content_host_block_cannot_suppress_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (host, tool) = single_content_host(
            &dir.path().join("db"),
            Arc::new(ImmediateDurable(calls.clone())),
            Arc::new(SuppressGate),
        );
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        assert!(
            matches!(read_replay_safe_after_evidence(&store,id).unwrap(),Some(ReplaySafeAfterEvidence::HostError {code,outcome:Outcome::Failed(reason)}) if code=="blocked" && reason.contains("denied"))
        );
    }
    #[test]
    fn durable_content_abort_fences_late_memo() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = Arc::new(AbortAfterHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        });
        let (host, tool) = single_content_host(
            &dir.path().join("db"),
            Arc::new(ImmediateDurable(calls.clone())),
            hook,
        );
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        assert_eq!(
            read_replay_safe_after_evidence(&store, id).unwrap(),
            Some(ReplaySafeAfterEvidence::ToolOutcome(Outcome::Succeeded(
                "done".into()
            )))
        );
    }

    struct CountAbortHook {
        inner: AbortAfterHook,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for CountAbortHook {
        async fn after_tool(
            &self,
            call: &crate::extension::ToolCall,
            value: &serde_json::Value,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<Option<serde_json::Value>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.after_tool(call, value, ctx).await
        }
    }
    #[test]
    fn durable_host_abort_does_not_run_cleanup_after_hook_or_write_stale_memo() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hooks = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(CountAbortHook {
            inner: AbortAfterHook {
                entered,
                release: std::sync::Mutex::new(release_rx),
                result,
            },
            calls: hooks.clone(),
        });
        let (host, tool) = single_content_host(
            &path,
            Arc::new(ImmediateDurable(calls.clone())),
            hook.clone(),
        );
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(id).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            store
                .get(format!("runtime/extension-memo/{id}/late-after").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(read_aborted_after_result(&store, id).unwrap().is_none());
        drop(store);
        let (host, _) = single_content_host(&path, Arc::new(ImmediateDurable(calls)), hook);
        let driver = host.into_driver();
        assert!(matches!(
            driver.get(id).unwrap().unwrap().state(),
            crate::task::TaskState::Cancelled
        ));
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn hooked_durable_before_block_never_dispatches_tool() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(BeforeFixture {
            decision: 0,
            entered: None,
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let mut host =
            StandaloneBeforeHookHost::open(dir.path().join("db"), Registry::default()).unwrap();
        let tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(ImmediateDurable(calls.clone())),
                "immediate-v1",
                1,
                vec![],
                "hook-v1",
                hook,
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let id = host.start_replay_safe_root(&tool, json!({}), 4).unwrap();
        let driver = host.into_driver();
        assert!(matches!(
            wait_terminal(&driver, id).state(),
            crate::task::TaskState::Succeeded(value) if matches!(serde_json::from_value::<Outcome>(value.clone()), Ok(Outcome::Failed(reason)) if reason.contains("blocked"))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
    }

    #[test]
    fn production_replay_safe_async_children_all_and_any_wake() {
        for any in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let root_calls = Arc::new(AtomicUsize::new(0));
            let middle_calls = Arc::new(AtomicUsize::new(0));
            let scheduler = production_chain_scheduler(
                &dir.path().join("db"),
                any,
                root_calls.clone(),
                middle_calls.clone(),
            );
            let task = production_outer_task();
            scheduler.submit(&task).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let (_, _, leaf) = production_chain_parked(&driver, task.id());
            release_chain(&driver, task.id(), leaf);
            assert_eq!(root_calls.load(Ordering::SeqCst), 2);
            assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
            driver.shutdown().unwrap();
        }
    }

    #[test]
    fn production_replay_safe_registration_rejects_unsafe_and_ambiguous_children() {
        let dir = tempfile::tempdir().unwrap();
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let handler: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        let spec = ToolSpec {
            name: "leaf".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        };
        let unsafe_child = Arc::new(crate::task_tool::TaskTool::new(
            spec.clone(),
            "unsafe",
            false,
            1,
            handler.clone(),
        ));
        let safe_child = Arc::new(crate::task_tool::TaskTool::new(
            spec, "safe", true, 1, handler,
        ));
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
            calls: Arc::new(AtomicUsize::new(0)),
            barrier: None,
            fail: false,
        });
        assert!(
            host.register_replay_safe_durable_tool(
                tool.clone(),
                "impl",
                1,
                vec![unsafe_child],
                ReplaySafeAuthorization::host_attests_replay_safe()
            )
            .is_err()
        );
        assert!(
            host.register_replay_safe_durable_tool(
                tool.clone(),
                "impl",
                1,
                vec![safe_child.clone(), safe_child],
                ReplaySafeAuthorization::host_attests_replay_safe()
            )
            .is_err()
        );
        assert!(
            host.register_replay_safe_durable_tool(
                tool,
                "",
                1,
                vec![],
                ReplaySafeAuthorization::host_attests_replay_safe()
            )
            .is_err()
        );
        host.into_driver().shutdown().unwrap();
    }

    struct TwoChildTool {
        any: bool,
        calls: Arc<AtomicUsize>,
        fail_after_wake: bool,
    }
    #[async_trait::async_trait]
    impl Tool for TwoChildTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "two-children".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for TwoChildTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            resume: DurableToolResume,
            host: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match resume.checkpoint {
                None => {
                    let a = host.start_child("a", "leaf", json!({"id":"a"})).await?;
                    let b = host.start_child("b", "leaf", json!({"id":"b"})).await?;
                    let calls = vec![a, b];
                    let wait = if self.any {
                        DurableWait::Any(calls.clone())
                    } else {
                        DurableWait::All(calls.clone())
                    };
                    Ok(DurableToolStep::Park {
                        checkpoint: json!({"calls":calls}),
                        wait,
                    })
                }
                Some(value) => {
                    if self.fail_after_wake {
                        return Err("failure after wake".into());
                    }
                    let calls: Vec<AsyncTaskCall> = serde_json::from_value(value["calls"].clone())
                        .map_err(|e| e.to_string())?;
                    if resume.ended.len() != if self.any { 1 } else { 2 } {
                        return Err("unexpected ended children".into());
                    }
                    for id in resume.ended {
                        let call = calls
                            .iter()
                            .find(|call| call.task_id() == id)
                            .ok_or("unknown ended child")?;
                        if !matches!(host.child_result(call).await?, Some(AsyncTaskResult::Outcome(Outcome::Succeeded(value))) if value == "leaf")
                        {
                            return Err("bad child result".into());
                        }
                    }
                    Ok(DurableToolStep::Complete(Outcome::Succeeded("root".into())))
                }
            }
        }
    }
    #[test]
    fn hooked_durable_any_wake_retains_remaining_child() {
        let dir = tempfile::tempdir().unwrap();
        let slot = Arc::new(std::sync::OnceLock::new());
        let leaf: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
        let mut registry = Registry::default();
        assert!(registry.register("production-outer", true, ProductionOuter(slot.clone())));
        assert!(registry.register_shared("production-leaf", true, leaf.clone()));
        for kind in ["production-outer", "production-leaf"] {
            assert!(
                registry.set_abort(kind, |_: &Task| Ok(crate::task_scheduler::Step::Succeed(
                    json!(null)
                )))
            );
        }
        let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
        let leaf_tool = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "production-leaf",
            true,
            1,
            leaf,
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let root_tool = host
            .register_replay_safe_durable_tool_with_hook(
                Arc::new(TwoChildTool {
                    any: true,
                    calls: calls.clone(),
                    fail_after_wake: false,
                }),
                "two-v1",
                1,
                vec![leaf_tool],
                "hook-v1",
                Arc::new(DurableHooks {
                    before: before.clone(),
                    after: after.clone(),
                }),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        assert!(slot.set(root_tool).is_ok());
        let scheduler = host.into_scheduler_for_test();
        let outer = production_outer_task();
        scheduler.submit(&outer).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let outer_wait = wait_state(&driver, outer.id(), |s| {
            matches!(s, crate::task::TaskState::Waiting(_))
        });
        let root: crate::task_tool::NestedTaskCall =
            serde_json::from_value(outer_wait.checkpoint().data["call"].clone()).unwrap();
        let root_wait = wait_state(&driver, root.task_id(), |s| {
            matches!(s, crate::task::TaskState::Waiting(_))
        });
        let children: Vec<AsyncTaskCall> =
            serde_json::from_value(root_wait.checkpoint().data["checkpoint"]["calls"].clone())
                .unwrap();
        for child in &children {
            wait_state(&driver, child.task_id(), |s| {
                matches!(s, crate::task::TaskState::Waiting(_))
            });
        }
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                children[0].task_id(),
                "release".into(),
                json!(null),
            ))
            .unwrap();
        wait_state(&driver, root.task_id(), |s| {
            matches!(s, crate::task::TaskState::Completing(_))
        });
        assert!(matches!(
            driver.get(children[1].task_id()).unwrap().unwrap().state(),
            crate::task::TaskState::Waiting(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(before.load(Ordering::SeqCst), 1);
        assert_eq!(after.load(Ordering::SeqCst), 1);
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                children[1].task_id(),
                "release".into(),
                json!(null),
            ))
            .unwrap();
        wait_state(&driver, outer.id(), |s| {
            matches!(s, crate::task::TaskState::Succeeded(_))
        });
        driver.shutdown().unwrap();
    }

    #[test]
    fn production_all_and_any_freeze_ended_children_with_two_children() {
        for any in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let slot = Arc::new(std::sync::OnceLock::new());
            let leaf: Arc<dyn crate::task_scheduler::Handler> = Arc::new(ChainLeaf);
            let mut registry = Registry::default();
            assert!(registry.register("production-outer", true, ProductionOuter(slot.clone())));
            assert!(registry.register_shared("production-leaf", true, leaf.clone()));
            for kind in ["production-outer", "production-leaf"] {
                assert!(registry.set_abort(kind, |_: &Task| Ok(
                    crate::task_scheduler::Step::Succeed(json!(null))
                )));
            }
            let mut host = StandaloneNoHooksHost::open(
                dir.path().join("db"),
                registry,
                UnhookedAuthorization::standalone_only(),
            )
            .unwrap();
            let leaf_tool = Arc::new(crate::task_tool::TaskTool::new(
                ToolSpec {
                    name: "leaf".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                },
                "production-leaf",
                true,
                1,
                leaf,
            ));
            let calls = Arc::new(AtomicUsize::new(0));
            let root_tool = host
                .register_replay_safe_durable_tool(
                    Arc::new(TwoChildTool {
                        any,
                        calls: calls.clone(),
                        fail_after_wake: false,
                    }),
                    "two-v1",
                    1,
                    vec![leaf_tool],
                    ReplaySafeAuthorization::host_attests_replay_safe(),
                )
                .unwrap();
            assert!(slot.set(root_tool).is_ok());
            let scheduler = host.into_scheduler_for_test();
            let outer = production_outer_task();
            scheduler.submit(&outer).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let outer_wait = wait_state(&driver, outer.id(), |s| {
                matches!(s, crate::task::TaskState::Waiting(_))
            });
            let root: crate::task_tool::NestedTaskCall =
                serde_json::from_value(outer_wait.checkpoint().data["call"].clone()).unwrap();
            let root_wait = wait_state(&driver, root.task_id(), |s| {
                matches!(s, crate::task::TaskState::Waiting(_))
            });
            let children: Vec<AsyncTaskCall> =
                serde_json::from_value(root_wait.checkpoint().data["checkpoint"]["calls"].clone())
                    .unwrap();
            for child in &children {
                wait_state(&driver, child.task_id(), |s| {
                    matches!(s, crate::task::TaskState::Waiting(_))
                });
            }
            driver
                .signal(&crate::task_store::Signal::new(
                    Uuid::new_v4(),
                    children[0].task_id(),
                    "release".into(),
                    json!(null),
                ))
                .unwrap();
            wait_state(&driver, children[0].task_id(), |s| {
                matches!(s, crate::task::TaskState::Succeeded(_))
            });
            if any {
                wait_state(&driver, root.task_id(), |s| {
                    matches!(s, crate::task::TaskState::Completing(_))
                });
                assert!(matches!(
                    driver.get(children[1].task_id()).unwrap().unwrap().state(),
                    crate::task::TaskState::Waiting(_)
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            } else {
                assert!(matches!(
                    driver.get(root.task_id()).unwrap().unwrap().state(),
                    crate::task::TaskState::Waiting(_)
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
            driver
                .signal(&crate::task_store::Signal::new(
                    Uuid::new_v4(),
                    children[1].task_id(),
                    "release".into(),
                    json!(null),
                ))
                .unwrap();
            let completed = wait_state(&driver, outer.id(), |s| {
                matches!(s, crate::task::TaskState::Succeeded(_))
            });
            assert_eq!(
                completed.state(),
                &crate::task::TaskState::Succeeded(
                    serde_json::to_value(Outcome::Succeeded("root".into())).unwrap()
                )
            );
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            driver.shutdown().unwrap();
        }
    }

    struct NamedGate;
    impl crate::task_scheduler::Handler for NamedGate {
        fn run(&self, task: &Task) -> Result<crate::task_scheduler::Step, String> {
            use crate::task_scheduler::Step;
            match task.checkpoint().phase.as_str() {
                "initial" => Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "gate".into(),
                        data: json!(null),
                    },
                    condition: crate::task::WaitFor::Signal("release".into()),
                    children: vec![],
                }),
                "gate" if task.delivered_signal().is_some() => Ok(Step::Succeed(
                    serde_json::to_value(Outcome::Succeeded("gate".into())).unwrap(),
                )),
                _ => Err("bad gate wake".into()),
            }
        }
    }
    fn completing_chain_host_with_failure(
        path: &std::path::Path,
        fail_after_wake: bool,
    ) -> (StandaloneNoHooksHost, Arc<crate::task_tool::TaskTool>) {
        let gate: Arc<dyn crate::task_scheduler::Handler> = Arc::new(NamedGate);
        let mut registry = Registry::default();
        assert!(registry.register_shared("completing-gate", true, gate.clone()));
        assert!(registry.set_abort("completing-gate", |_: &Task| Ok(
            crate::task_scheduler::Step::Succeed(json!(null))
        )));
        let mut host =
            StandaloneNoHooksHost::open(path, registry, UnhookedAuthorization::standalone_only())
                .unwrap();
        let gate_tool = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "gate".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "completing-gate",
            true,
            1,
            gate,
        ));
        let leaf = host
            .register_replay_safe_durable_tool(
                Arc::new(ChainTool {
                    name: "leaf",
                    child: "gate",
                    calls: Arc::new(AtomicUsize::new(0)),
                    any: false,
                }),
                "completing-leaf-v1",
                1,
                vec![gate_tool],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        let root = host
            .register_replay_safe_durable_tool(
                Arc::new(TwoChildTool {
                    any: true,
                    calls: Arc::new(AtomicUsize::new(0)),
                    fail_after_wake,
                }),
                "completing-root-v1",
                1,
                vec![leaf],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        (host, root)
    }
    fn completing_chain_host(
        path: &std::path::Path,
    ) -> (StandaloneNoHooksHost, Arc<crate::task_tool::TaskTool>) {
        completing_chain_host_with_failure(path, false)
    }
    fn completing_chain_parked(
        driver: &crate::task_driver::Driver,
        root: Uuid,
    ) -> (Vec<Uuid>, Vec<Uuid>) {
        let root_task = wait_state(driver, root, |s| {
            matches!(s, crate::task::TaskState::Waiting(_))
        });
        let calls: Vec<AsyncTaskCall> =
            serde_json::from_value(root_task.checkpoint().data["checkpoint"]["calls"].clone())
                .unwrap();
        let mut children = vec![];
        let mut gates = vec![];
        for call in calls {
            let task = wait_state(driver, call.task_id(), |s| {
                matches!(s, crate::task::TaskState::Waiting(_))
            });
            let gate: AsyncTaskCall =
                serde_json::from_value(task.checkpoint().data["checkpoint"]["call"].clone())
                    .unwrap();
            wait_state(driver, gate.task_id(), |s| {
                matches!(s, crate::task::TaskState::Waiting(_))
            });
            children.push(call.task_id());
            gates.push(gate.task_id());
        }
        (children, gates)
    }
    fn signal_gate(driver: &crate::task_driver::Driver, id: Uuid) {
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                id,
                "release".into(),
                json!(null),
            ))
            .unwrap();
    }
    #[test]
    fn completing_any_parent_preserves_durable_child_across_reopen_and_abort() {
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (host, root_tool) = completing_chain_host(&path);
            let root = host
                .start_replay_safe_root(&root_tool, json!({}), 3)
                .unwrap();
            let driver = host.into_driver();
            let (children, gates) = completing_chain_parked(&driver, root);
            signal_gate(&driver, gates[0]);
            wait_state(&driver, root, |s| {
                matches!(s, crate::task::TaskState::Completing(_))
            });
            assert!(matches!(
                driver.get(children[1]).unwrap().unwrap().state(),
                crate::task::TaskState::Waiting(_)
            ));
            driver.shutdown().unwrap();
            let (host, _) = completing_chain_host(&path);
            let driver = host.into_driver();
            if abort {
                driver.request_abort(root).unwrap();
                for id in [children[1], gates[1]] {
                    wait_state(&driver, id, |s| {
                        matches!(s, crate::task::TaskState::Cancelled)
                    });
                }
                let completed = wait_state(&driver, root, |s| {
                    matches!(s, crate::task::TaskState::Succeeded(_))
                });
                assert!(completed.abort_requested());
            } else {
                signal_gate(&driver, gates[1]);
                wait_state(&driver, children[1], |s| {
                    matches!(s, crate::task::TaskState::Succeeded(_))
                });
                wait_state(&driver, root, |s| {
                    matches!(s, crate::task::TaskState::Succeeded(_))
                });
            }
            driver.shutdown().unwrap();
        }
    }
    #[test]
    fn failure_after_any_wake_cancels_remaining_durable_child() {
        let dir = tempfile::tempdir().unwrap();
        let (host, tool) = completing_chain_host_with_failure(&dir.path().join("db"), true);
        let root = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let driver = host.into_driver();
        let (children, gates) = completing_chain_parked(&driver, root);
        signal_gate(&driver, gates[0]);
        wait_state(
            &driver,
            root,
            |s| matches!(s, crate::task::TaskState::Failed(reason) if reason.contains("failure after wake")),
        );
        for id in [children[1], gates[1]] {
            wait_state(&driver, id, |s| {
                matches!(s, crate::task::TaskState::Cancelled)
            });
        }
        driver.shutdown().unwrap();
    }

    #[test]
    fn completing_any_kill_fixture() {
        let Some(dir) = std::env::var_os("PI_COMPLETING_ANY_KILL_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let (host, tool) = completing_chain_host(&dir.join("db"));
        let root = host.start_replay_safe_root(&tool, json!({}), 3).unwrap();
        let driver = host.into_driver();
        let (children, gates) = completing_chain_parked(&driver, root);
        signal_gate(&driver, gates[0]);
        wait_state(&driver, root, |s| {
            matches!(s, crate::task::TaskState::Completing(_))
        });
        std::fs::write(
            dir.join("ready.tmp"),
            serde_json::to_vec(&(root, children[1], gates[1])).unwrap(),
        )
        .unwrap();
        std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
        loop {
            std::thread::park();
        }
    }
    #[test]
    fn completing_any_kill_reopen_finishes_remaining_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::completing_any_kill_fixture")
            .env("PI_COMPLETING_ANY_KILL_DIR", dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
                let _ = process.kill();
                let _ = process.wait();
                panic!("completing fixture did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let (root, child, gate): (Uuid, Uuid, Uuid) =
            serde_json::from_slice(&std::fs::read(dir.path().join("ready")).unwrap()).unwrap();
        let (host, _) = completing_chain_host(&dir.path().join("db"));
        let driver = host.into_driver();
        assert!(matches!(
            driver.get(root).unwrap().unwrap().state(),
            crate::task::TaskState::Completing(_)
        ));
        signal_gate(&driver, gate);
        wait_state(&driver, child, |s| {
            matches!(s, crate::task::TaskState::Succeeded(_))
        });
        wait_state(&driver, root, |s| {
            matches!(s, crate::task::TaskState::Succeeded(_))
        });
        driver.shutdown().unwrap();
    }

    struct CancellationTool(std::sync::mpsc::Sender<()>);
    #[async_trait::async_trait]
    impl Tool for CancellationTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "cancel-test".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for CancellationTool {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            context: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.send(()).map_err(|e| e.to_string())?;
            context.cancellation.cancelled().await;
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "cancel observed".into(),
            )))
        }
    }
    #[test]
    fn production_replay_safe_async_abort_cancels_live_tool_and_discards_result() {
        let dir = tempfile::tempdir().unwrap();
        let slot = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("production-outer", true, ProductionOuter(slot.clone())));
        assert!(registry.set_abort("production-outer", |_: &Task| Ok(
            crate::task_scheduler::Step::Succeed(json!(null))
        )));
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            registry,
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let tool = host
            .register_replay_safe_durable_tool(
                Arc::new(CancellationTool(tx)),
                "cancel-v1",
                1,
                vec![],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap();
        assert!(slot.set(tool).is_ok());
        let scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let outer = production_outer_task();
        scheduler.submit(&outer).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        let parked = wait_state(&driver, outer.id(), |s| {
            matches!(s, crate::task::TaskState::Waiting(_))
        });
        let call: crate::task_tool::NestedTaskCall =
            serde_json::from_value(parked.checkpoint().data["call"].clone()).unwrap();
        driver.request_abort(outer.id()).unwrap();
        wait_state(&driver, outer.id(), |s| {
            matches!(s, crate::task::TaskState::Cancelled)
        });
        wait_state(&driver, call.task_id(), |s| {
            matches!(s, crate::task::TaskState::Cancelled)
        });
        let records = owner
            .store()
            .list(format!("durable/replay-safe-async/{}/", call.task_id()).as_bytes())
            .unwrap();
        assert_eq!(
            records.len(),
            1,
            "aborted execution must not publish a result"
        );
        driver.shutdown().unwrap();
    }

    #[test]
    fn production_replay_safe_process_fixture() {
        let Some(dir) = std::env::var_os("PI_TEST_REPLAY_SAFE_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let scheduler = production_chain_scheduler(
            &dir.join("db"),
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let task = production_outer_task();
        scheduler.submit(&task).unwrap();
        std::fs::write(dir.join("outer"), task.id().to_string()).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        if std::env::var_os("PI_TEST_REPLAY_SAFE_CUT").is_none() {
            let ids = production_chain_parked(&driver, task.id());
            std::fs::write(dir.join("cut.tmp"), serde_json::to_vec(&ids).unwrap()).unwrap();
            std::fs::rename(dir.join("cut.tmp"), dir.join("cut")).unwrap();
        }
        loop {
            std::thread::park();
        }
    }
    fn kill_production_fixture(dir: &std::path::Path, cut: Option<&str>) -> (Uuid, Vec<u8>) {
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.arg("--exact")
            .arg("async_task_tool::tests::production_replay_safe_process_fixture")
            .env("PI_TEST_REPLAY_SAFE_DIR", dir);
        if let Some(cut) = cut {
            cmd.env("PI_TEST_REPLAY_SAFE_CUT", cut);
        }
        let mut process = cmd.spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.join("cut").exists() {
            if std::time::Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
                let _ = process.kill();
                let _ = process.wait();
                panic!("production fixture did not reach boundary");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        (
            std::fs::read_to_string(dir.join("outer"))
                .unwrap()
                .parse()
                .unwrap(),
            std::fs::read(dir.join("cut")).unwrap(),
        )
    }
    #[test]
    fn production_replay_safe_kill_before_park_commit_replays_without_orphan_child() {
        for cut in ["intent", "result"] {
            let dir = tempfile::tempdir().unwrap();
            let (outer, root_data) = kill_production_fixture(dir.path(), Some(cut));
            let root: Uuid = std::str::from_utf8(&root_data).unwrap().parse().unwrap();
            let root_calls = Arc::new(AtomicUsize::new(0));
            let middle_calls = Arc::new(AtomicUsize::new(0));
            let scheduler = production_chain_scheduler(
                &dir.path().join("db"),
                false,
                root_calls.clone(),
                middle_calls.clone(),
            );
            let owner = scheduler.owner();
            let records = owner
                .store()
                .list(format!("durable/replay-safe-async/{root}/").as_bytes())
                .unwrap();
            assert_eq!(records.len(), 1, "uncommitted result must not persist");
            assert!(
                !TaskStore::new(owner.store())
                    .list()
                    .unwrap()
                    .iter()
                    .any(|s| s.task.owner() == &TaskOwner::Task(root)),
                "staged child must not persist before park"
            );
            let driver = crate::task_driver::Driver::start(scheduler);
            let (resumed_root, _, leaf) = production_chain_parked(&driver, outer);
            assert_eq!(resumed_root, root);
            release_chain(&driver, outer, leaf);
            assert_eq!(root_calls.load(Ordering::SeqCst), 2);
            assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
            let records = owner
                .store()
                .list(format!("durable/replay-safe-async/{root}/").as_bytes())
                .unwrap();
            assert_eq!(
                records.len(),
                5,
                "old intent plus two new attempt/result pairs"
            );
            driver.shutdown().unwrap();
        }
    }
    #[test]
    fn production_replay_safe_parked_kill_reopen_and_abort() {
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (outer, data) = kill_production_fixture(dir.path(), None);
            let (root, middle, leaf): (Uuid, Uuid, Uuid) = serde_json::from_slice(&data).unwrap();
            let root_calls = Arc::new(AtomicUsize::new(0));
            let middle_calls = Arc::new(AtomicUsize::new(0));
            let scheduler = production_chain_scheduler(
                &dir.path().join("db"),
                false,
                root_calls.clone(),
                middle_calls.clone(),
            );
            let driver = crate::task_driver::Driver::start(scheduler);
            if abort {
                driver.request_abort(outer).unwrap();
                for id in [outer, root, middle, leaf] {
                    wait_state(&driver, id, |s| {
                        matches!(s, crate::task::TaskState::Cancelled)
                    });
                }
                assert_eq!(root_calls.load(Ordering::SeqCst), 0);
                assert_eq!(middle_calls.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(
                    production_chain_parked(&driver, outer),
                    (root, middle, leaf)
                );
                release_chain(&driver, outer, leaf);
                assert_eq!(root_calls.load(Ordering::SeqCst), 1);
                assert_eq!(middle_calls.load(Ordering::SeqCst), 1);
            }
            driver.shutdown().unwrap();
        }
    }
    fn release_chain(driver: &crate::task_driver::Driver, root: Uuid, leaf: Uuid) {
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                leaf,
                "release".into(),
                json!(null),
            ))
            .unwrap();
        let task = wait_state(driver, root, |s| {
            matches!(s, crate::task::TaskState::Succeeded(_))
        });
        assert_eq!(
            task.state(),
            &crate::task::TaskState::Succeeded(
                serde_json::to_value(Outcome::Succeeded("root".into())).unwrap()
            )
        );
    }
    #[test]
    fn multilevel_async_tools_admit_park_and_wake_in_real_driver() {
        let dir = tempfile::tempdir().unwrap();
        let root_calls = Arc::new(AtomicUsize::new(0));
        let middle_calls = Arc::new(AtomicUsize::new(0));
        let scheduler = crate::task_scheduler::Scheduler::open(
            dir.path().join("db"),
            chain_registry(root_calls.clone(), middle_calls.clone()),
        )
        .unwrap();
        let task = test_task();
        scheduler.submit(&task).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let (_, leaf) = chain_parked(&driver, task.id());
        release_chain(&driver, task.id(), leaf);
        assert_eq!(root_calls.load(Ordering::SeqCst), 2);
        assert_eq!(middle_calls.load(Ordering::SeqCst), 2);
        driver.shutdown().unwrap();
    }

    #[test]
    fn multilevel_async_tools_kill_fixture() {
        let Some(dir) = std::env::var_os("PI_TEST_ASYNC_CHAIN_KILL_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let scheduler = crate::task_scheduler::Scheduler::open(
            dir.join("db"),
            chain_registry(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .unwrap();
        let task = test_task();
        scheduler.submit(&task).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let (middle, leaf) = chain_parked(&driver, task.id());
        std::fs::write(
            dir.join("ready.tmp"),
            serde_json::to_vec(&(task.id(), middle, leaf)).unwrap(),
        )
        .unwrap();
        std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
        loop {
            std::thread::park();
        }
    }
    fn kill_parked_chain(dir: &std::path::Path) -> (Uuid, Uuid, Uuid) {
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::multilevel_async_tools_kill_fixture")
            .env("PI_TEST_ASYNC_CHAIN_KILL_DIR", dir)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.join("ready").exists() {
            if std::time::Instant::now() >= deadline || process.try_wait().unwrap().is_some() {
                let _ = process.kill();
                let _ = process.wait();
                panic!("multilevel fixture did not park");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        serde_json::from_slice(&std::fs::read(dir.join("ready")).unwrap()).unwrap()
    }
    #[test]
    fn multilevel_async_tools_kill_reopen_resumes_once_per_level() {
        let dir = tempfile::tempdir().unwrap();
        let (root, middle, leaf) = kill_parked_chain(dir.path());
        let root_calls = Arc::new(AtomicUsize::new(0));
        let middle_calls = Arc::new(AtomicUsize::new(0));
        let scheduler = crate::task_scheduler::Scheduler::open(
            dir.path().join("db"),
            chain_registry(root_calls.clone(), middle_calls.clone()),
        )
        .unwrap();
        let owner = scheduler.owner();
        for id in [root, middle] {
            assert_eq!(
                owner
                    .store()
                    .list(format!("test/durable-async/{id}/").as_bytes())
                    .unwrap()
                    .len(),
                2
            );
        }
        let driver = crate::task_driver::Driver::start(scheduler);
        assert_eq!(chain_parked(&driver, root), (middle, leaf));
        release_chain(&driver, root, leaf);
        assert_eq!(root_calls.load(Ordering::SeqCst), 1);
        assert_eq!(middle_calls.load(Ordering::SeqCst), 1);
        for id in [root, middle] {
            assert_eq!(
                owner
                    .store()
                    .list(format!("test/durable-async/{id}/").as_bytes())
                    .unwrap()
                    .len(),
                4
            );
        }
        driver.shutdown().unwrap();
    }
    #[test]
    fn multilevel_async_tools_abort_after_reopen_drains_descendants() {
        let dir = tempfile::tempdir().unwrap();
        let (root, middle, leaf) = kill_parked_chain(dir.path());
        let root_calls = Arc::new(AtomicUsize::new(0));
        let middle_calls = Arc::new(AtomicUsize::new(0));
        let scheduler = crate::task_scheduler::Scheduler::open(
            dir.path().join("db"),
            chain_registry(root_calls.clone(), middle_calls.clone()),
        )
        .unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        driver.request_abort(root).unwrap();
        for id in [root, middle, leaf] {
            wait_state(&driver, id, |s| {
                matches!(s, crate::task::TaskState::Cancelled)
            });
        }
        assert_eq!(root_calls.load(Ordering::SeqCst), 0);
        assert_eq!(middle_calls.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
    }
    fn test_task() -> Task {
        Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("test-durable".into(), true)
        .into_task_tool()
    }
    fn wait_state(
        driver: &crate::task_driver::Driver,
        id: Uuid,
        expected: impl Fn(&crate::task::TaskState) -> bool,
    ) -> Task {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let task = driver.get(id).unwrap().unwrap();
            if expected(task.state()) {
                return task;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "task did not reach expected state: {:?}",
                task.state()
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    #[test]
    fn pure_durable_tool_real_driver_parks_reopens_and_wakes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
            calls: calls.clone(),
            barrier: None,
            fail: false,
        });
        let scheduler =
            crate::task_scheduler::Scheduler::open(&path, pure_registry(tool.clone())).unwrap();
        let owner = scheduler.owner();
        let task = test_task();
        scheduler.submit(&task).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let parked = wait_state(
            &driver,
            task.id(),
            |state| matches!(state, crate::task::TaskState::Waiting(crate::task::WaitFor::Signal(name)) if name == "go"),
        );
        assert_eq!(parked.checkpoint().data, json!({"saved":42}));
        let prefix = format!("test/durable-async/{}/", task.id());
        let before = owner.store().list(prefix.as_bytes()).unwrap();
        assert_eq!(before.len(), 2);
        driver.shutdown().unwrap();
        drop(owner);
        let scheduler = crate::task_scheduler::Scheduler::open(&path, pure_registry(tool)).unwrap();
        let owner = scheduler.owner();
        let driver = crate::task_driver::Driver::start(scheduler);
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                task.id(),
                "go".into(),
                json!(null),
            ))
            .unwrap();
        let finished = wait_state(&driver, task.id(), |state| {
            matches!(state, crate::task::TaskState::Succeeded(_))
        });
        assert_eq!(
            finished.state(),
            &crate::task::TaskState::Succeeded(
                serde_json::to_value(Outcome::Succeeded("resumed".into())).unwrap()
            )
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let records = owner.store().list(prefix.as_bytes()).unwrap();
        assert_eq!(records.len(), 4);
        assert!(before.iter().all(|entry| records.contains(entry)));
        driver.shutdown().unwrap();
    }

    #[test]
    fn pure_durable_tool_abort_discards_park_result() {
        let dir = tempfile::tempdir().unwrap();
        let barrier = Arc::new((
            std::sync::Mutex::new((false, false)),
            std::sync::Condvar::new(),
        ));
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
            calls: Arc::new(AtomicUsize::new(0)),
            barrier: Some(barrier.clone()),
            fail: false,
        });
        let scheduler =
            crate::task_scheduler::Scheduler::open(dir.path().join("db"), pure_registry(tool))
                .unwrap();
        let owner = scheduler.owner();
        let task = test_task();
        scheduler.submit(&task).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        let (lock, wake) = &*barrier;
        let mut state = lock.lock().unwrap();
        while !state.0 {
            state = wake
                .wait_timeout(state, std::time::Duration::from_secs(10))
                .unwrap()
                .0;
            assert!(state.0, "tool did not start");
        }
        drop(state);
        driver.request_abort(task.id()).unwrap();
        let mut state = lock.lock().unwrap();
        state.1 = true;
        wake.notify_all();
        drop(state);
        wait_state(&driver, task.id(), |state| {
            matches!(state, crate::task::TaskState::Cancelled)
        });
        let prefix = format!("test/durable-async/{}/", task.id());
        let records = owner.store().list(prefix.as_bytes()).unwrap();
        assert_eq!(
            records.len(),
            1,
            "abort must discard the proposed park result"
        );
        assert!(!String::from_utf8_lossy(&records[0].0).ends_with("/result"));
        driver.shutdown().unwrap();
    }

    #[test]
    fn pure_durable_tool_failure_never_publishes_park() {
        let dir = tempfile::tempdir().unwrap();
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
            calls: Arc::new(AtomicUsize::new(0)),
            barrier: None,
            fail: true,
        });
        let scheduler =
            crate::task_scheduler::Scheduler::open(dir.path().join("db"), pure_registry(tool))
                .unwrap();
        let owner = scheduler.owner();
        let task = test_task();
        scheduler.submit(&task).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        wait_state(
            &driver,
            task.id(),
            |state| matches!(state, crate::task::TaskState::Failed(reason) if reason.contains("test tool failure")),
        );
        let prefix = format!("test/durable-async/{}/", task.id());
        let records = owner.store().list(prefix.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);
        assert!(!String::from_utf8_lossy(&records[0].0).ends_with("/result"));
        driver.shutdown().unwrap();
    }

    #[test]
    fn pure_durable_tool_kill_process_fixture() {
        let Some(dir) = std::env::var_os("PI_TEST_PURE_DURABLE_KILL_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
            calls: Arc::new(AtomicUsize::new(0)),
            barrier: None,
            fail: false,
        });
        let scheduler =
            crate::task_scheduler::Scheduler::open(dir.join("db"), pure_registry(tool)).unwrap();
        let task = test_task();
        scheduler.submit(&task).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        wait_state(&driver, task.id(), |state| {
            matches!(state, crate::task::TaskState::Waiting(_))
        });
        std::fs::write(dir.join("ready.tmp"), task.id().to_string()).unwrap();
        std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn pure_durable_tool_kill_reopen_wakes_new_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::pure_durable_tool_kill_process_fixture")
            .env("PI_TEST_PURE_DURABLE_KILL_DIR", dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("park fixture did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(PureParkTool {
            calls: calls.clone(),
            barrier: None,
            fail: false,
        });
        let scheduler =
            crate::task_scheduler::Scheduler::open(dir.path().join("db"), pure_registry(tool))
                .unwrap();
        let owner = scheduler.owner();
        let prefix = format!("test/durable-async/{id}/");
        assert_eq!(owner.store().list(prefix.as_bytes()).unwrap().len(), 2);
        let driver = crate::task_driver::Driver::start(scheduler);
        driver
            .signal(&crate::task_store::Signal::new(
                Uuid::new_v4(),
                id,
                "go".into(),
                json!(null),
            ))
            .unwrap();
        let finished = wait_state(&driver, id, |state| {
            matches!(state, crate::task::TaskState::Succeeded(_))
        });
        assert_eq!(
            finished.state(),
            &crate::task::TaskState::Succeeded(
                serde_json::to_value(Outcome::Succeeded("resumed".into())).unwrap()
            )
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(owner.store().list(prefix.as_bytes()).unwrap().len(), 4);
        driver.shutdown().unwrap();
    }
    #[async_trait::async_trait]
    impl Tool for DurableStub {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "durable".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            self.0.fetch_add(1, Ordering::SeqCst);
            Outcome::Succeeded("unexpected".into())
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for DurableStub {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "unexpected".into(),
            )))
        }
    }
    #[test]
    fn durable_contract_distinguishes_park_from_pending_and_rejects_registration() {
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let handle: AsyncTaskCall =
            serde_json::from_value(json!({"parent":parent,"task_id":child})).unwrap();
        let park = DurableToolStep::Park {
            checkpoint: json!({"phase":1}),
            wait: DurableWait::Any(vec![handle]),
        };
        let encoded = serde_json::to_value(&park).unwrap();
        assert_eq!(
            serde_json::from_value::<DurableToolStep>(encoded).unwrap(),
            park
        );
        assert_ne!(
            park,
            DurableToolStep::Complete(Outcome::Pending {
                reference: "remote".into()
            })
        );
        let dir = tempfile::tempdir().unwrap();
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn DurableAsyncTool> = Arc::new(DurableStub(calls.clone()));
        assert!(
            host.register_durable_tool("durable", tool, "backend", 1)
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        host.into_driver().shutdown().unwrap();
    }

    #[test]
    fn serialized_handle_survives_checkpoint_round_trip() {
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let handle: AsyncTaskCall =
            serde_json::from_value(json!({"parent": parent, "task_id": child})).unwrap();
        let checkpoint = serde_json::to_value(&handle).unwrap();
        let restored: AsyncTaskCall = serde_json::from_value(checkpoint).unwrap();
        assert_eq!(restored, handle);
        assert_eq!(restored.task_id(), child);
    }

    #[test]
    fn driver_blocks_persisted_unavailable_tasks_for_all_tool_specs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("host-db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        for spec in ["original", "changed"] {
            let tool = Arc::new(TestTool {
                calls: calls.clone(),
                spec,
            });
            assert!(
                host.register_tool("async.disabled", tool, "backend", 1)
                    .is_err()
            );
        }
        let id = Uuid::new_v4();
        let task = Task::new(
            id,
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "execute".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("async.disabled".into(), false, 1, json!({}))
        .unwrap()
        .into_task_tool();
        let store = durable_store::Store::open(&path).unwrap();
        TaskStore::new(&store).save(&[Save::Create(&task)]).unwrap();
        drop(store);
        let host = StandaloneNoHooksHost::open(
            &path,
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let driver = host.into_driver();
        let blocked = driver.blocked_tasks().unwrap();
        assert!(blocked.iter().any(|task| task.id == id));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let restored = TaskStore::new(&store).load(id).unwrap().unwrap().task;
        assert!(!restored.is_terminal());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    struct CheckpointParent(Arc<std::sync::OnceLock<Arc<AsyncTaskTool>>>, bool);
    impl crate::task_scheduler::Handler for CheckpointParent {
        fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
            unreachable!()
        }
        fn run_with_admission(
            &self,
            parent: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
            admission: &crate::task_scheduler::AdmissionHandle<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            if self.1 && parent.checkpoint().phase == "initial" {
                let handle = self.0.get().ok_or("missing tool")?.start_child_now(
                    admission,
                    "child",
                    json!({}),
                )?;
                let repeated = self.0.get().ok_or("missing tool")?.start_child_now(
                    admission,
                    "child",
                    json!({}),
                )?;
                if repeated != handle {
                    return Err("keyed child did not reconnect".into());
                }
                if self
                    .0
                    .get()
                    .ok_or("missing tool")?
                    .start_child_now(admission, "child", json!({"changed":true}))
                    .is_ok()
                {
                    return Err("keyed child accepted changed input".into());
                }
                let id = handle.task_id();
                return Ok(crate::task_scheduler::Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: serde_json::to_value(handle).map_err(|e| e.to_string())?,
                    },
                    condition: crate::task::WaitFor::Tasks(vec![id]),
                    children: vec![],
                });
            }
            self.run_with_documents(parent, docs)
        }
        fn run_with_documents(
            &self,
            parent: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            use crate::task_scheduler::Step;
            if parent.checkpoint().phase == "initial" {
                let handle = self.0.get().ok_or("missing tool")?.start_child(
                    parent,
                    docs,
                    "child",
                    json!({}),
                )?;
                let id = handle.task_id();
                return Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: serde_json::to_value(&handle).map_err(|e| e.to_string())?,
                    },
                    condition: crate::task::WaitFor::Tasks(vec![id]),
                    children: vec![],
                });
            }
            let handle: AsyncTaskCall = serde_json::from_value(parent.checkpoint().data.clone())
                .map_err(|e| e.to_string())?;
            match handle.result(parent, docs)? {
                Some(AsyncTaskResult::Outcome(Outcome::Structured { data, .. })) => {
                    Ok(Step::Succeed(data))
                }
                Some(AsyncTaskResult::Outcome(Outcome::Succeeded(output))) => {
                    Ok(Step::Succeed(json!(output)))
                }
                Some(AsyncTaskResult::Outcome(Outcome::SucceededWithDetails {
                    output,
                    details,
                })) => Ok(Step::Succeed(json!({"output":output, "details":details}))),
                other => Err(format!("unexpected child outcome: {other:?}")),
            }
        }
    }

    fn driver_checkpoint_reopen(mode: u8) {
        use std::time::{Duration, Instant};
        struct ReplyTool {
            changed: Arc<std::sync::atomic::AtomicBool>,
            calls: Arc<AtomicUsize>,
            normalize: bool,
        }
        #[async_trait::async_trait]
        impl Tool for ReplyTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "fixture".into(),
                    description: String::new(),
                    parameters: if self.changed.load(Ordering::SeqCst) {
                        json!({"type":"object", "required":["different"]})
                    } else if self.normalize {
                        json!({"type":"object", "required":["normalized"]})
                    } else {
                        json!({"type":"object"})
                    },
                }
            }
            async fn prepare_arguments(
                &self,
                mut args: serde_json::Value,
            ) -> pi_rs_core::ExtensionResult<serde_json::Value> {
                if self.normalize {
                    args["normalized"] = json!(true);
                }
                Ok(args)
            }
            async fn invoke(&self, args: serde_json::Value, _: ToolContext) -> Outcome {
                if self.normalize {
                    assert_eq!(args["normalized"], json!(true));
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                Outcome::Structured {
                    data: json!({"answer":42}),
                    attachments: vec![],
                }
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(ReplyTool {
            changed: changed.clone(),
            calls: calls.clone(),
            normalize: mode >= 4,
        });
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let make_host = || {
            let cell = Arc::new(std::sync::OnceLock::new());
            let mut registry = Registry::default();
            assert!(registry.register(
                "parent",
                true,
                CheckpointParent(cell.clone(), mode == 3 || mode == 5)
            ));
            let mut host = StandaloneNoHooksHost::open(
                &path,
                registry,
                UnhookedAuthorization::standalone_only(),
            )
            .unwrap();
            let adapter = host
                .register_tool("fixture", tool.clone(), "backend", 1)
                .unwrap();
            assert!(cell.set(adapter).is_ok());
            host
        };
        let mut scheduler = make_host().into_scheduler_for_test();
        let id = Uuid::new_v4();
        let parent = Task::new(
            id,
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("parent".into(), true)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(id).unwrap().unwrap().checkpoint().phase,
            "waiting"
        );
        drop(scheduler);
        if mode == 2 {
            let cell = Arc::new(std::sync::OnceLock::new());
            let mut registry = Registry::default();
            assert!(registry.register("parent", true, CheckpointParent(cell, false)));
            let forged_calls = Arc::new(AtomicUsize::new(0));
            let observed = forged_calls.clone();
            assert!(registry.register("fixture", false, move |_: &Task| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(crate::task_scheduler::Step::Succeed(json!(null)))
            }));
            let driver = StandaloneNoHooksHost::open(
                &path,
                registry,
                UnhookedAuthorization::standalone_only(),
            )
            .unwrap()
            .into_driver();
            let deadline = Instant::now() + Duration::from_secs(5);
            while driver.get(id).is_ok() {
                assert!(
                    Instant::now() < deadline,
                    "replacement handler was not rejected"
                );
                std::thread::yield_now();
            }
            assert_eq!(forged_calls.load(Ordering::SeqCst), 0);
            assert!(driver.shutdown_with_pending().unwrap().failure().is_some());
            return;
        }
        changed.store(mode == 1, Ordering::SeqCst);
        let driver = make_host().into_driver();
        if mode == 1 {
            let deadline = Instant::now() + Duration::from_secs(5);
            while driver.get(id).is_ok() {
                assert!(Instant::now() < deadline, "changed spec was not rejected");
                std::thread::yield_now();
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(driver.shutdown_with_pending().unwrap().failure().is_some());
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let task = driver.get(id).unwrap().unwrap();
            if task.is_terminal() {
                assert_eq!(
                    task.state(),
                    &crate::task::TaskState::Succeeded(json!({"answer":42}))
                );
                break;
            }
            assert!(Instant::now() < deadline, "driver did not settle parent");
            std::thread::yield_now();
        }
        driver.shutdown().unwrap();
    }
    #[test]
    fn driver_restores_checkpoint_handle_and_reads_child_after_reopen() {
        driver_checkpoint_reopen(0);
    }
    #[test]
    fn driver_rejects_changed_tool_spec_after_reopen() {
        driver_checkpoint_reopen(1);
    }
    #[test]
    fn driver_rejects_replacement_handler_after_reopen() {
        driver_checkpoint_reopen(2);
    }
    #[test]
    fn immediate_sealed_child_handle_survives_checkpoint_and_reopen() {
        driver_checkpoint_reopen(3);
    }
    #[test]
    fn staged_sealed_child_normalizes_before_schema_validation_after_reopen() {
        driver_checkpoint_reopen(4);
    }
    #[test]
    fn immediate_sealed_child_normalizes_before_schema_validation_after_reopen() {
        driver_checkpoint_reopen(5);
    }
    struct ChangedSchema;
    #[async_trait::async_trait]
    impl Tool for ChangedSchema {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "original".into(),
                description: String::new(),
                parameters: json!({"type":"object", "required":["new"]}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }

    #[test]
    fn stored_tool_identity_detects_spec_implementation_and_version_changes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let original = TestTool {
            calls: calls.clone(),
            spec: "original",
        };
        let changed = TestTool {
            calls,
            spec: "changed",
        };
        let identity = ToolIdentity::new(&original, "backend-tenant-v1", 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let key = b"test/tool-identity".to_vec();
        store
            .commit(
                &[],
                &[durable_store::Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&identity).unwrap(),
                }],
            )
            .unwrap();
        drop(store);
        let store = durable_store::Store::open(&path).unwrap();
        let restored: ToolIdentity =
            serde_json::from_slice(&store.get(&key).unwrap().unwrap().value).unwrap();
        assert!(restored.matches(&original, "backend-tenant-v1", 1));
        assert!(!restored.matches(&changed, "backend-tenant-v1", 1));
        assert!(!restored.matches(&ChangedSchema, "backend-tenant-v1", 1));
        assert!(!restored.matches(&original, "other-tenant", 1));
        assert!(!restored.matches(&original, "backend-tenant-v1", 2));
        assert!(ToolIdentity::new(&original, "", 1).is_err());
        assert!(ToolIdentity::new(&original, "valid", 0).is_err());
    }

    #[test]
    fn unhooked_host_drives_only_its_standalone_scheduler() {
        let dir = tempfile::tempdir().unwrap();
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let tool = Arc::new(TestTool {
            calls: Arc::new(AtomicUsize::new(0)),
            spec: "fixture",
        });
        assert!(host.register_tool("fixture", tool, "backend", 1).is_err());
        host.into_driver().shutdown().unwrap();
    }
    #[test]
    fn unhooked_host_cannot_attach_to_a_live_conversation_runtime() {
        struct ModelStub;
        #[async_trait::async_trait]
        impl pi_rs_core::Model for ModelStub {
            async fn stream(
                &self,
                _: pi_rs_core::ModelRequest,
            ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
                unreachable!()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let harness = crate::runtime::Harness::open_paused(
            &path,
            crate::runtime::Options {
                model: Arc::new(ModelStub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
            },
        )
        .unwrap();
        assert!(
            StandaloneNoHooksHost::open(
                &path,
                Registry::default(),
                UnhookedAuthorization::standalone_only()
            )
            .is_err()
        );
        drop(harness);
        let host = StandaloneNoHooksHost::open(
            &path,
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        host.into_driver().shutdown().unwrap();
    }
    #[test]
    fn production_registration_requires_matching_host_tool_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(TestTool {
            calls: calls.clone(),
            spec: "leaf",
        });
        assert!(
            host.register_tool("leaf", tool.clone(), "backend", 1)
                .is_err()
        );
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        assert!(
            host.register_tool("leaf", tool.clone(), "other", 1)
                .is_err()
        );
        host.register_tool("leaf", tool.clone(), "backend", 1)
            .unwrap();
        assert!(host.register_tool("leaf", tool, "backend", 1).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        host.into_driver().shutdown().unwrap();
    }
}
