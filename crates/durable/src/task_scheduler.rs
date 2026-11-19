//! Explicitly driven scheduler for generic tasks, separate from the conversation runtime.
//!
//! The scheduler holds the store's exclusive cooperative owner lock. `tick` runs
//! at most one invocation synchronously; callers periodically call it again,
//! including after signals and deadlines. Dropping between ticks is a clean stop.
//! Interrupted invocations replay only when both recorded and current policies
//! allow it. External effects cannot be made atomic with this
//! store: handlers must be pure or use destination-side idempotency/reconciliation.
//! A storage error other than a recovered checked-document conflict, or a panic,
//! during an invocation requires dropping this scheduler
//! and reopening it; never assume an unsuccessful commit did not happen.
//!
//! This scheduler supports direct-child all, any and fail-fast waits. It has no
//! background tasks, combined waits, or conversation integration.
//! Cancellation changes durable state only and cannot undo external effects or
//! interrupt a synchronous handler. Direct TaskStore writers must be fenced.
//! Success is retained while ordinary owned work finishes.
//!
//! `reserve_tick` and `settle_invocation` expose a lower-level invocation split;
//! callers must serialize scheduler operations and manage the detached leases.
//! This does not make the existing task driver concurrent or supply a coordinator
//! for blocked settlement, failure publication, or bounded worker admission.

use crate::task::{Checkpoint, Task, TaskOwner, TaskState, TransitionError, WaitFor};
use crate::task_store::{self, Save, Signal, TaskStore, WakeResult};
use durable_store::owner::{OwnedStore, OwnerError};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
use uuid::Uuid;

/// A handler computes one transition. For effects, use a stable idempotency key
/// derived from task ID and checkpoint, and reconcile uncertain results externally.
pub trait Handler: Send + Sync {
    fn run(&self, task: &Task) -> Result<Step, String>;
    /// Stage checked documents alongside a successful step. The default keeps
    /// existing handlers unchanged. Conflict retries require replay-safe registration.
    fn run_with_documents(
        &self,
        task: &Task,
        _documents: &mut crate::document::TaskDocuments<'_>,
    ) -> Result<Step, String> {
        self.run(task)
    }
    /// Run with an admission capability borrowed from this invocation's live
    /// executor lease. Existing handlers continue through `run_with_documents`.
    /// The capability cannot be retained after this call or used by an abort
    /// handler. Newly admitted children are durable even if this step fails.
    fn run_with_admission(
        &self,
        task: &Task,
        documents: &mut crate::document::TaskDocuments<'_>,
        _admission: &AdmissionHandle<'_>,
    ) -> Result<Step, String> {
        self.run_with_documents(task, documents)
    }
    /// The default preserves existing handlers. A cancellation-aware adapter
    /// must join its own executor before returning.
    fn run_with_cancellation(
        &self,
        task: &Task,
        documents: &mut crate::document::TaskDocuments<'_>,
        _cancellation: &pi_rs_core::CancellationToken,
    ) -> Result<Step, String> {
        self.run_with_documents(task, documents)
    }
}
impl<F> Handler for F
where
    F: Fn(&Task) -> Result<Step, String> + Send + Sync,
{
    fn run(&self, task: &Task) -> Result<Step, String> {
        self(task)
    }
}

#[derive(Clone)]
pub enum Step {
    Continue(Checkpoint),
    Wait {
        checkpoint: Checkpoint,
        condition: WaitFor,
        children: Vec<Task>,
    },
    Succeed(Value),
    Fail(String),
}

pub type Migration =
    dyn Fn(&Value, &Checkpoint, u32) -> Result<(Value, Checkpoint), String> + Send + Sync;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockedReason {
    MissingTask,
    TaskTooOld,
    MigrationFailed(String),
}

fn orphan_diagnostic(reason: &BlockedReason) -> String {
    match reason {
        BlockedReason::MissingTask => "orphaned: missing task definition".into(),
        BlockedReason::TaskTooOld => {
            "orphaned: task definition is older than stored version".into()
        }
        BlockedReason::MigrationFailed(error) => format!("orphaned: migration failed: {error}"),
    }
}

/// A live task whose stored definition cannot currently execute.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockedTask {
    pub id: Uuid,
    pub kind: Option<String>,
    pub owner: TaskOwner,
    pub reason: BlockedReason,
}

/// Read-only view of generic task scheduling at a supplied Unix-millisecond clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WakeSnapshot {
    /// Ready executable work, or a durable deadline that is already due.
    pub active: bool,
    /// Earliest future durable task deadline, in Unix milliseconds.
    pub at: Option<i64>,
    /// Live tasks whose definitions cannot currently execute.
    pub blocked: Vec<BlockedTask>,
}

#[derive(Clone)]
struct ReceiptCapture {
    identity: crate::async_task_tool::ToolIdentity,
    executor: Arc<crate::async_task_tool::SealedExecutor>,
    implementation: String,
    version: u32,
    #[cfg(test)]
    before_intent: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    busy_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    after_intent: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Clone)]
struct Registration {
    identity: Arc<()>,
    version: u32,
    migrate: Option<Arc<Migration>>,
    replay_safe: bool,
    handler: Arc<dyn Handler>,
    abort: Option<Arc<dyn Handler>>,
    receipt_capture: Option<Arc<ReceiptCapture>>,
}
/// Clones preserve definition identity for migration retry decisions.
#[derive(Clone, Default)]
pub struct Registry {
    entries: HashMap<String, Registration>,
}
impl Registry {
    pub(crate) fn matches_task_tool(
        &self,
        identity: &crate::task_tool::Identity,
        handler: &Arc<dyn Handler>,
    ) -> bool {
        self.entries.get(&identity.kind).is_some_and(|r| {
            r.version == identity.definition_version
                && r.replay_safe == identity.replay_safe
                && Arc::ptr_eq(&r.handler, handler)
        })
    }
    pub fn register_shared(
        &mut self,
        kind: impl Into<String>,
        replay_safe: bool,
        handler: Arc<dyn Handler>,
    ) -> bool {
        self.register_shared_versioned(kind, 1, replay_safe, handler)
    }
    /// Register a shared handler with its stable, host-assigned definition version.
    pub fn register_shared_versioned(
        &mut self,
        kind: impl Into<String>,
        version: u32,
        replay_safe: bool,
        handler: Arc<dyn Handler>,
    ) -> bool {
        if version == 0 {
            return false;
        }
        use std::collections::hash_map::Entry;
        match self.entries.entry(kind.into()) {
            Entry::Vacant(v) => {
                v.insert(Registration {
                    identity: Arc::new(()),
                    version,
                    migrate: None,
                    replay_safe,
                    handler,
                    abort: None,
                    receipt_capture: None,
                });
                true
            }
            Entry::Occupied(_) => false,
        }
    }
    pub fn register(
        &mut self,
        kind: impl Into<String>,
        replay_safe: bool,
        handler: impl Handler + 'static,
    ) -> bool {
        use std::collections::hash_map::Entry;
        match self.entries.entry(kind.into()) {
            Entry::Vacant(v) => {
                v.insert(Registration {
                    identity: Arc::new(()),
                    version: 1,
                    migrate: None,
                    replay_safe,
                    handler: Arc::new(handler),
                    abort: None,
                    receipt_capture: None,
                });
                true
            }
            Entry::Occupied(_) => false,
        }
    }
    /// Register a versioned definition. One migration handles any supported older version.
    pub fn register_versioned(
        &mut self,
        kind: impl Into<String>,
        version: u32,
        replay_safe: bool,
        handler: impl Handler + 'static,
        migrate: Option<Arc<Migration>>,
    ) -> bool {
        let kind = kind.into();
        if version == 0 || !self.register(kind.clone(), replay_safe, handler) {
            return false;
        }
        let entry = self.entries.get_mut(&kind).unwrap();
        entry.version = version;
        entry.migrate = migrate;
        true
    }
    /// Add an abort handler to an already registered definition.
    pub fn set_abort(&mut self, kind: &str, abort: impl Handler + 'static) -> bool {
        let Some(entry) = self.entries.get_mut(kind) else {
            return false;
        };
        entry.identity = Arc::new(());
        entry.abort = Some(Arc::new(abort));
        true
    }
    /// Register a separate cleanup invocation. Abort handlers must be replay-safe:
    /// their external effects require idempotency across a crash before commit.
    pub fn register_with_abort(
        &mut self,
        kind: impl Into<String>,
        replay_safe: bool,
        handler: impl Handler + 'static,
        abort: impl Handler + 'static,
    ) -> bool {
        let kind = kind.into();
        if !self.register(kind.clone(), replay_safe, handler) {
            return false;
        }
        self.entries.get_mut(&kind).unwrap().abort = Some(Arc::new(abort));
        true
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Owner(#[from] OwnerError),
    #[error(transparent)]
    Store(#[from] task_store::Error),
    #[error(transparent)]
    Document(#[from] crate::document::Error),
    #[error("task transition: {0:?}")]
    Transition(TransitionError),
    #[error("unknown or mismatched task registration")]
    Registration,
    #[error("untrusted task-owned tool inheritance; host review required")]
    UntrustedToolInheritance,
    #[error("invalid ownership or child creation")]
    Ownership,
    #[error("a pending ownership abort must settle task {0}")]
    PendingAbort(Uuid),
    #[error("foreground owned conversation abort is not supported")]
    ForegroundAbortUnsupported,
    #[error("task has live owned children")]
    LiveChildren,
    #[error("handler failed with an uncertain outcome: {0}")]
    Handler(String),
    #[error("a task scheduler is already attached to this owner")]
    AlreadyAttached,
    #[error("scheduler has active detached invocations")]
    Busy,
    #[error("scheduler is no longer accepting reservations")]
    Stopped,
    #[error("scheduler must be reopened after an invocation or commit failure")]
    Poisoned,
    #[cfg(test)]
    #[error("test settlement yield")]
    TestYield,
}

// The owner lock prevents another OwnedStore for the same database, but Arc
// clones share that lock. Fence recovery and execution across those clones too.
fn attachments() -> &'static Mutex<HashSet<usize>> {
    static ATTACHMENTS: OnceLock<Mutex<HashSet<usize>>> = OnceLock::new();
    ATTACHMENTS.get_or_init(|| Mutex::new(HashSet::new()))
}

struct Attachment {
    owner: Arc<OwnedStore>,
}
impl Attachment {
    fn acquire(owner: Arc<OwnedStore>) -> Result<Self, Error> {
        let key = Arc::as_ptr(&owner) as usize;
        let mut active = attachments().lock().unwrap_or_else(|e| e.into_inner());
        if !active.insert(key) {
            return Err(Error::AlreadyAttached);
        }
        drop(active);
        Ok(Self { owner })
    }
}
impl Drop for Attachment {
    fn drop(&mut self) {
        let key = Arc::as_ptr(&self.owner) as usize;
        attachments()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
    }
}

#[derive(Default)]
struct InvocationState {
    attempts: HashSet<Uuid>,
    // A task ID alone can be reused by a later reservation. Bind each live
    // executor to the durable attempt that was reserved for it.
    leases: HashMap<Uuid, (Uuid, Option<Uuid>)>,
    poisoned: bool,
    handler_failed: bool,
    failed_handlers: HashSet<Uuid>,
    published: std::collections::VecDeque<(Uuid, bool)>,
}

// A stored Running state alone can be an interrupted invocation after reopen.
// Only the scheduler that owns its still-unpublished lease may dispatch below it.
fn live_task_tool_parent(task: &Task, invocations: &Mutex<InvocationState>) -> bool {
    if !task.is_task_tool() || task.state() != &TaskState::Running {
        return false;
    }
    let Ok(state) = invocations.lock() else {
        return false;
    };
    !state.poisoned
        && state.attempts.contains(&task.id())
        && !state.failed_handlers.contains(&task.id())
        && !state.published.iter().any(|(id, _)| *id == task.id())
}

/// Shared publication gate used by the driver without taking the scheduler mutex.
#[derive(Clone)]
pub(crate) struct FailurePublication {
    fence: Arc<Mutex<()>>,
    state: Arc<Mutex<InvocationState>>,
}

impl FailurePublication {
    // Held through admission commit. Publishers update this ledger under the
    // same mutex. Ledger holders never wait for the task or conversation gates.
    pub(crate) fn admission(&self) -> Result<AdmissionPublication<'_>, Error> {
        Ok(AdmissionPublication {
            state: self.state.lock().map_err(|_| Error::Poisoned)?,
        })
    }
    #[cfg(test)]
    pub(crate) fn test_set_published(&self, task: Uuid, present: bool) {
        let _fence = self.fence.lock().unwrap();
        let mut state = self.state.lock().unwrap();
        state.published.retain(|(id, _)| *id != task);
        if present {
            state.published.push_back((task, true));
        }
    }
    /// Cleanup errors and panics may follow an unknown durable commit. Publish
    /// poison and the driver stop under the same fence as task settlement.
    pub(crate) fn indeterminate(&self, publish_stop: impl FnOnce()) {
        let _guard = self.fence.lock().unwrap_or_else(|e| e.into_inner());
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .poisoned = true;
        publish_stop();
    }

    pub(crate) fn try_lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, ()>, std::sync::TryLockError<std::sync::MutexGuard<'_, ()>>>
    {
        self.fence.try_lock()
    }
}

pub(crate) struct AdmissionPublication<'a> {
    state: std::sync::MutexGuard<'a, InvocationState>,
}
impl AdmissionPublication<'_> {
    pub(crate) fn permits_admission(
        &self,
        owner: &OwnedStore,
        conversation: Uuid,
        parent: Option<&Task>,
    ) -> bool {
        let state = &self.state;
        if state.poisoned {
            return false;
        }
        let pending: Vec<_> = state
            .failed_handlers
            .iter()
            .copied()
            .chain(
                state
                    .published
                    .iter()
                    .filter_map(|(id, abort)| abort.then_some(*id)),
            )
            .collect();
        if pending.is_empty() {
            return true;
        }
        let edges = crate::ownership_edge::EdgeStore::new(owner.store());
        let root = match parent {
            Some(task) => Scheduler::foreground_root(owner, task),
            None => match edges.get(conversation) {
                Ok(None) => return true,
                Ok(Some(edge)) if edge.edge.mode == crate::ownership_edge::Mode::Background => {
                    return true;
                }
                Ok(Some(edge)) => TaskStore::new(owner.store())
                    .load(edge.edge.task)
                    .ok()
                    .flatten()
                    .and_then(|record| {
                        if edge.edge.task_owner != *record.task.owner() {
                            None
                        } else {
                            Scheduler::foreground_root(owner, &record.task)
                        }
                    }),
                Err(_) => None,
            },
        };
        let Some(root) = root else {
            return false;
        };
        pending.iter().all(|id| {
            TaskStore::new(owner.store())
                .load(*id)
                .ok()
                .flatten()
                .and_then(|record| Scheduler::foreground_root(owner, &record.task))
                .is_some_and(|failed_root| failed_root != root)
        })
    }
}

// Retain the attachment and activity until settlement or abandonment. Dropping
// an unretired invocation forces recovery rather than guessing its outcome.
/// The in-process guard for a reserved invocation.
pub struct InvocationLease {
    attachment: Arc<Attachment>,
    state: Arc<Mutex<InvocationState>>,
    failure_fence: Arc<Mutex<()>>,
    scheduler_poison: Arc<std::sync::atomic::AtomicBool>,
    id: Uuid,
    token: Uuid,
    retired: bool,
    _activity: crate::worker_activity::Guard,
}
impl Drop for InvocationLease {
    fn drop(&mut self) {
        let _fence = self.failure_fence.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .leases
            .get(&self.id)
            .is_some_and(|(token, _)| *token == self.token)
        {
            state.leases.remove(&self.id);
            state.attempts.remove(&self.id);
            state.published.retain(|(id, _)| *id != self.id);
        }
        if !self.retired {
            state.poisoned = true;
        }
    }
}

/// A scheduling pass either did maintenance or reserved a detached invocation.
pub enum TickReservation {
    Progress(bool),
    Invocation(Box<DetachedInvocation>),
    /// Graph maintenance must be attempted outside scheduler and failure locks.
    Maintenance(PendingMaintenance),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum MaintenanceIntent {
    Nested(Uuid),
    Failed(Uuid),
    FailFast(Uuid),
}

/// A graph intent retained when a nonblocking reservation needs cleanup.
pub struct PendingMaintenance {
    attachment: Arc<Attachment>,
    fence: Arc<Mutex<()>>,
    state: Arc<Mutex<InvocationState>>,
    scheduler_poison: Arc<std::sync::atomic::AtomicBool>,
    intent: MaintenanceIntent,
}

impl PendingMaintenance {
    /// Try the poll fences before the failure fence; busy makes no mutation.
    /// Call without the scheduler mutex or a task or poll gate.
    pub fn try_cleanup(&self, harness: &mut crate::runtime::Harness) -> Result<bool, Error> {
        let owner = &self.attachment.owner;
        if !Arc::ptr_eq(owner, &harness.owner) {
            return Err(Error::Ownership);
        }
        match owner.conversation_controls().try_fence(|| {
            let _guard = match self.fence.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => return Err(Error::Busy),
                Err(std::sync::TryLockError::Poisoned(_)) => return Err(Error::Poisoned),
            };
            {
                let state = self.state.lock().map_err(|_| Error::Poisoned)?;
                if state.poisoned
                    || self
                        .scheduler_poison
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    return Err(Error::Poisoned);
                }
                if state.handler_failed {
                    return Err(Error::Busy);
                }
            }
            match harness.abort_maintenance_checked(self.intent) {
                Ok(affected) => Ok(affected.unwrap_or_default()),
                Err(error) => {
                    self.state.lock().map_err(|_| Error::Poisoned)?.poisoned = true;
                    Err(Error::Handler(format!("maintenance abort: {error}")))
                }
            }
        }) {
            Ok(Some(_)) => Ok(true),
            Ok(None) | Err(durable_store::controls::FenceError::Commit(Error::Busy)) => Ok(false),
            Err(durable_store::controls::FenceError::Poisoned) => Err(Error::Poisoned),
            Err(durable_store::controls::FenceError::Commit(error)) => Err(error),
        }
    }
}

#[cfg(test)]
type BoundaryHooks = Mutex<std::collections::HashMap<Uuid, Box<dyn FnOnce() + Send>>>;
#[cfg(test)]
fn release_hooks() -> &'static BoundaryHooks {
    static HOOKS: std::sync::OnceLock<BoundaryHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(crate) fn after_ready_for_test(id: Uuid, hook: impl FnOnce() + Send + 'static) {
    release_hooks().lock().unwrap().insert(id, Box::new(hook));
}

/// An exclusive in-process reservation. Dropping it without settlement poisons
/// the scheduler; its attachment remains held until the invocation is dropped.
pub struct DetachedInvocation {
    running: task_store::StoredTask,
    handler: Arc<dyn Handler>,
    abort: bool,
    validator: Option<crate::runtime::Harness>,
    lease: InvocationLease,
    definitions: Arc<std::sync::RwLock<WakeDefinitions>>,
    activity: crate::worker_activity::WorkerActivity,
    receipt_capture: Option<Arc<ReceiptCapture>>,
    unhooked_cancellation: Option<pi_rs_core::CancellationToken>,
    sealed_registration: bool,
}

#[derive(Clone)]
enum InvocationResult {
    Handler(
        Result<Step, String>,
        Box<crate::document::DetachedTaskDocuments>,
    ),
    Abort(Option<String>),
}

/// Completed handler output, still fenced by its exclusive reservation.
pub struct CompletedInvocation {
    running: task_store::StoredTask,
    result: InvocationResult,
    lease: InvocationLease,
    receipt_capture: bool,
    // Pin the exact host registration until receipt persistence or abandonment.
    _identity_lease: Option<crate::nested_identity::IdentityLease>,
}

impl CompletedInvocation {
    /// Read a marked handler's retained outcome when receipt capture failed.
    /// This is evidence for reconciliation, not a retry authorization.
    pub fn retained_tool_outcome(&self) -> Option<pi_rs_core::Outcome> {
        if !self.receipt_capture {
            return None;
        }
        let InvocationResult::Handler(Ok(Step::Succeed(value)), _) = &self.result else {
            return None;
        };
        let outcome: pi_rs_core::Outcome = serde_json::from_value(value.clone()).ok()?;
        Some(outcome)
    }

    /// Preserve a marked task's uncertain result before the driver settles it.
    pub(crate) fn capture_marked_receipt(
        &mut self,
    ) -> Result<(), crate::async_task_protocol::Error> {
        use crate::async_task_protocol::Error as ProtocolError;
        if !self.receipt_capture {
            return Ok(());
        }
        let intent = crate::async_task_protocol::load_intent(
            self.lease.attachment.owner.store(),
            self.running.task.id(),
        )?;
        if !intent.matches_attempt(&self.running.task)
            && !crate::async_task_protocol::read_raw_result(
                self.lease.attachment.owner.store(),
                self.running.task.id(),
            )?
            .is_some_and(|(raw, _)| raw == intent)
        {
            return Err(ProtocolError::Invalid);
        }
        let InvocationResult::Handler(Ok(Step::Succeed(value)), _) = &self.result else {
            return Err(ProtocolError::Invalid);
        };
        let outcome: pi_rs_core::Outcome = serde_json::from_value(value.clone())?;
        self.persist_unhooked_receipt(&intent)?;
        self.receipt_capture = false;
        if matches!(
            outcome,
            pi_rs_core::Outcome::Pending { .. } | pi_rs_core::Outcome::Unknown(_)
        ) && let InvocationResult::Handler(result, _) = &mut self.result
        {
            *result = Ok(Step::Fail(
                "uncertain tool outcome retained for reconciliation".into(),
            ));
        }
        Ok(())
    }

    /// Persist an outcome taken from this joined handler's exact
    /// return value while its invocation lease is still held. It does not prove
    /// that a handler joined any executor it may have spawned.
    pub(crate) fn persist_unhooked_receipt(
        &self,
        intent: &crate::async_task_protocol::Intent,
    ) -> Result<(), crate::async_task_protocol::Error> {
        use crate::async_task_protocol::Error as ProtocolError;
        if self.lease.retired
            || (!intent.matches_attempt(&self.running.task)
                && !crate::async_task_protocol::read_raw_result(
                    self.lease.attachment.owner.store(),
                    self.running.task.id(),
                )?
                .is_some_and(|(raw, _)| raw == *intent))
        {
            return Err(ProtocolError::Invalid);
        }
        let InvocationResult::Handler(Ok(Step::Succeed(value)), _) = &self.result else {
            return Err(ProtocolError::Invalid);
        };
        let outcome: pi_rs_core::Outcome = serde_json::from_value(value.clone())?;
        let _failure = self
            .lease
            .failure_fence
            .lock()
            .map_err(|_| ProtocolError::Invalid)?;
        let owner = &self.lease.attachment.owner;
        let _gate = owner
            .task_gate()
            .lock()
            .map_err(|_| ProtocolError::Invalid)?;
        let state = self
            .lease
            .state
            .lock()
            .map_err(|_| ProtocolError::Invalid)?;
        if !state.attempts.contains(&self.running.task.id())
            || state.leases.get(&self.lease.id)
                != Some(&(self.lease.token, self.running.task.attempt_id()))
        {
            return Err(ProtocolError::Invalid);
        }
        let batch = crate::async_task_protocol::prepare_receipt(owner.store(), intent, &outcome)?;
        owner.store().commit(&batch.checks, &batch.mutations)?;
        Ok(())
    }
}

// Own the exact checked transaction and the observations needed to revalidate
// it after graph-only conflicts. The invocation lease remains with its caller.
/// A checked transaction and its graph revalidation state.
struct TaskNotificationAssembly {
    original: crate::document::Transaction,
    settlements: Vec<crate::runtime::SettlementNotification>,
    reservation_only: HashMap<Uuid, Vec<crate::runtime::SubmissionNotify>>,
    reservation_wait: HashMap<Uuid, Vec<task_store::SignalKey>>,
    reservation_destinations: Vec<crate::runtime::SubmissionNotify>,
    reservation_conversations_only: bool,
    clocks: Vec<Option<(i64, std::time::Instant)>>,
    retries: usize,
}

impl TaskNotificationAssembly {
    fn refresh_reservation_conversations(
        &mut self,
        store: &durable_store::Store,
    ) -> Result<bool, crate::document::Error> {
        if !self.reservation_conversations_only {
            return Ok(false);
        }
        let Some(old) = self.original.checks().iter().find_map(|check| {
            if check.key == b"runtime/conversation/"
                && let durable_store::Expected::PrefixUnchangedSince(boundary) = check.expected
            {
                return Some(boundary);
            }
            None
        }) else {
            return Ok(false);
        };
        // Capture the boundary before scanning. A concurrent conversation put
        // after it will still conflict at the final atomic commit.
        let boundary = store.put_version()?;
        for notify in &self.reservation_destinations {
            match crate::runtime::check_submission_destination(store, notify) {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(crate::document::Error::Store(error)) => {
                    return Err(error.into());
                }
                Err(crate::document::Error::Signal(task_store::Error::Store(error))) => {
                    return Err(error.into());
                }
                Err(_) => return Ok(false),
            }
        }
        Ok(self
            .original
            .refresh_reservation_conversations(old, boundary))
    }

    fn refresh_reservation_revision(
        &mut self,
        store: &durable_store::Store,
        key: &[u8],
    ) -> Result<bool, crate::document::Error> {
        let Some((task, destinations)) = self
            .reservation_only
            .iter()
            .find(|(task, _)| task_store::signal_revision_key(**task) == key)
        else {
            return Ok(false);
        };
        let Some(old) = self
            .original
            .checks()
            .iter()
            .find(|check| check.key == key)
            .map(|check| check.expected.clone())
        else {
            return Ok(false);
        };
        let revision = store.get(key)?;
        let tasks = task_store::TaskStore::new(store);
        if let Some(keys) = self.reservation_wait.get(task) {
            match tasks.validate_typed_wait_keys(*task, keys) {
                Ok(()) => {}
                Err(task_store::Error::Store(error)) => return Err(error.into()),
                Err(_) => return Ok(false),
            }
        }
        for notify in destinations {
            match tasks.read_typed_signal_state(
                *task,
                &crate::runtime::submission_settled_signal(),
                &notify.key,
            ) {
                Ok(None) => {}
                Ok(Some(_)) => return Ok(false),
                Err(task_store::Error::Store(error)) => {
                    return Err(error.into());
                }
                // Ambiguous or malformed receipts cannot prove the key free.
                Err(_) => return Ok(false),
            }
        }
        let expected = revision.map_or(durable_store::Expected::Missing, |entry| {
            durable_store::Expected::Version(entry.version)
        });
        Ok(self
            .original
            .refresh_reservation_revision(key, &old, expected))
    }

    fn assemble(
        &mut self,
        store: &durable_store::Store,
        harness: &crate::runtime::Harness,
    ) -> Result<crate::document::Transaction, crate::document::Error> {
        let mut samples = Vec::with_capacity(self.settlements.len());
        for clock in &mut self.clocks {
            let sample = harness.task_submission_clock();
            let (anchor, started) =
                clock.get_or_insert_with(|| (sample, std::time::Instant::now()));
            let elapsed = started.elapsed().as_millis().min(i64::MAX as u128) as i64;
            let projected = anchor.saturating_add(elapsed);
            let now = sample.max(projected);
            if sample > projected {
                *anchor = sample;
                *started = std::time::Instant::now();
            }
            samples.push(now);
        }
        self.original
            .clone()
            .assemble_submission_notifications(store, &self.settlements, &samples)
    }
}

pub struct PreparedSettlement {
    attachment: Arc<Attachment>,
    transaction: crate::document::Transaction,
    notifications: Option<TaskNotificationAssembly>,
    running: task_store::StoredTask,
    candidate: task_store::StoredTask,
    wait_targets: Vec<Uuid>,
    staged_children: HashMap<Uuid, Task>,
    conflict_keys: HashSet<Vec<u8>>,
    graph_version: u64,
    attempts: usize,
    commit_uncertain: bool,
}

/// Owned settlement state at an invocation or prepared transaction boundary.
#[allow(clippy::large_enum_variant)]
pub enum PendingSettlement {
    Unprepared(CompletedInvocation),
    Prepared {
        lease: InvocationLease,
        settlement: PreparedSettlement,
    },
    PreparationConflict {
        lease: InvocationLease,
        conflict: PreparationConflict,
    },
    Failure {
        lease: InvocationLease,
        settlement: FailureSettlement,
    },
}

/// A definitively uncommitted document-preparation conflict awaiting abort revalidation.
pub struct PreparationConflict {
    attachment: Arc<Attachment>,
    running: task_store::StoredTask,
}

#[allow(clippy::large_enum_variant)]
enum DeferredSettlement {
    Unprepared(
        task_store::StoredTask,
        Result<Step, String>,
        crate::document::DetachedTaskDocuments,
    ),
    Prepared(PreparedSettlement),
    IndeterminatePrepared(PreparedSettlement),
    IndeterminateFailure(FailureSettlement),
    PreparationConflict(PreparationConflict),
    Failure(FailureSettlement),
}

impl std::fmt::Debug for PendingSettlement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unprepared(completed) => f
                .debug_tuple("Unprepared")
                .field(&completed.task_id())
                .finish(),
            Self::Prepared { lease, .. } => f.debug_tuple("Prepared").field(&lease.id).finish(),
            Self::PreparationConflict { lease, .. } => f
                .debug_tuple("PreparationConflict")
                .field(&lease.id)
                .finish(),
            Self::Failure { lease, .. } => f.debug_tuple("Failure").field(&lease.id).finish(),
        }
    }
}

impl PendingSettlement {
    // Distinguish durable ownership uncertainty from a temporarily busy poll
    // fence during graceful driver shutdown. This only observes the graph;
    // the returned settlement retains its lease and is always revalidated.
    pub(crate) fn waits_for_uncertain_graph(&self, include_background: bool) -> bool {
        let owner = &self.lease().attachment.owner;
        let Ok(_gate) = owner.task_gate().lock() else {
            return false;
        };
        matches!(
            pending_abort_root_for_running(
                &TaskStore::new(owner.store()),
                self.task_id(),
                include_background,
            ),
            Err(Error::Busy)
        )
    }

    /// Borrow the original completed invocation without releasing its lease.
    pub fn completed(&self) -> Option<&CompletedInvocation> {
        match self {
            Self::Unprepared(completed) => Some(completed),
            _ => None,
        }
    }

    /// The task whose result and exclusive lease remain owned here.
    pub fn task_id(&self) -> Uuid {
        self.lease().id
    }

    /// The reserved attempt identity, retained for external reconciliation.
    pub fn attempt_id(&self) -> Option<Uuid> {
        match self {
            Self::Unprepared(completed) => completed.running.task.attempt_id(),
            Self::Prepared { settlement, .. } => settlement.running.task.attempt_id(),
            Self::PreparationConflict { conflict, .. } => conflict.running.task.attempt_id(),
            Self::Failure { settlement, .. } => settlement.running.task.attempt_id(),
        }
    }

    /// Read the durable task while this outcome still holds its attachment.
    /// This is an observation, not authorization to retry an uncertain commit.
    pub fn observed_task(&self) -> Result<Option<Task>, Error> {
        Ok(TaskStore::new(self.lease().attachment.owner.store())
            .load(self.task_id())?
            .map(|stored| stored.task))
    }

    /// Read a durable key for reconciliation without releasing the lease.
    pub fn read_durable(
        &self,
        key: &[u8],
    ) -> Result<Option<durable_store::Entry>, durable_store::Error> {
        self.lease().attachment.owner.store().get(key)
    }

    /// The preserved reason for an owned failure settlement, if present.
    pub fn failure_reason(&self) -> Option<&str> {
        match self {
            Self::Failure { settlement, .. } => Some(&settlement.reason),
            _ => None,
        }
    }

    /// The task transition candidate, where preparation reached that boundary.
    pub fn candidate_task(&self) -> Option<&Task> {
        match self {
            Self::Prepared { settlement, .. } => Some(&settlement.candidate.task),
            Self::Failure { settlement, .. } => settlement
                .candidate
                .as_ref()
                .map(|candidate| &candidate.task.task),
            _ => None,
        }
    }

    fn lease(&self) -> &InvocationLease {
        match self {
            Self::Unprepared(completed) => &completed.lease,
            Self::Prepared { lease, .. } => lease,
            Self::PreparationConflict { lease, .. } | Self::Failure { lease, .. } => lease,
        }
    }

    /// Attempt the checked graph abort without waiting for poll or failure
    /// fences. The caller must not hold the scheduler mutex or a task/poll gate.
    /// `false` means a fence was busy; retain this value and retry after a wake.
    pub fn try_cleanup(&self, harness: &mut crate::runtime::Harness) -> Result<bool, Error> {
        let lease = self.lease();
        let owner = &lease.attachment.owner;
        if let Self::Prepared { settlement, .. } = self
            && (lease.id != settlement.running.task.id()
                || !Arc::ptr_eq(&lease.attachment, &settlement.attachment))
        {
            return Err(Error::Ownership);
        }
        if let Self::PreparationConflict { conflict, .. } = self
            && (lease.id != conflict.running.task.id()
                || !Arc::ptr_eq(&lease.attachment, &conflict.attachment))
        {
            return Err(Error::Ownership);
        }
        if let Self::Failure { settlement, .. } = self
            && (lease.id != settlement.running.task.id()
                || !Arc::ptr_eq(&lease.attachment, &settlement.attachment))
        {
            return Err(Error::Ownership);
        }
        if !Arc::ptr_eq(owner, &harness.owner) {
            return Err(Error::Ownership);
        }
        let result = owner.conversation_controls().try_fence(|| {
            let _failure = match lease.failure_fence.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => return Err(Error::Busy),
                Err(std::sync::TryLockError::Poisoned(_)) => return Err(Error::Poisoned),
            };
            {
                let state = lease.state.lock().map_err(|_| Error::Poisoned)?;
                if state.poisoned
                    || lease
                        .scheduler_poison
                        .load(std::sync::atomic::Ordering::Acquire)
                {
                    return Err(Error::Poisoned);
                }
                if state.handler_failed && !state.failed_handlers.contains(&lease.id) {
                    return Err(Error::Busy);
                }
            }
            match harness.abort_pending_task_graph(lease.id) {
                Ok(affected) => Ok(affected.unwrap_or_default()),
                Err(crate::runtime::Error::TaskScheduler(Error::Busy)) => Err(Error::Busy),
                Err(error) => {
                    lease.state.lock().map_err(|_| Error::Poisoned)?.poisoned = true;
                    Err(Error::Handler(format!("checked pending abort: {error}")))
                }
            }
        });
        match result {
            Ok(Some(_)) => Ok(true),
            Ok(None) | Err(durable_store::controls::FenceError::Commit(Error::Busy)) => Ok(false),
            Err(durable_store::controls::FenceError::Poisoned) => Err(Error::Poisoned),
            Err(durable_store::controls::FenceError::Commit(error)) => Err(error),
        }
    }
}

enum PreparedRevalidation {
    Ready,
    BlockedOrder,
    PendingAbort(Uuid),
    Aborted(Box<task_store::StoredTask>),
    Stale,
}

impl PreparedSettlement {
    /// Revalidate under the owner's task gate. The graph boundary precedes all
    /// reads, so later graph writes remain fenced by the checked transaction.
    fn revalidate(
        &mut self,
        scheduler: &Scheduler,
        owner: &Arc<OwnedStore>,
        _gate: &std::sync::MutexGuard<'_, ()>,
    ) -> Result<PreparedRevalidation, Error> {
        self.graph_version = owner
            .store()
            .put_version()
            .map_err(task_store::Error::from)?;
        if scheduler.has_prior_outcome(owner, &self.running.task)
            || !scheduler.separate_from_failures(owner, &self.running.task)
        {
            return Ok(PreparedRevalidation::BlockedOrder);
        }
        if let Some(root) = pending_abort_root_for_running(
            &scheduler.ts(),
            self.running.task.id(),
            scheduler.validator.is_none(),
        )? {
            return Ok(PreparedRevalidation::PendingAbort(root));
        }
        let latest = scheduler
            .ts()
            .load(self.running.task.id())?
            .ok_or(Error::Ownership)?;
        if latest.task.abort_requested() {
            return Ok(PreparedRevalidation::Aborted(Box::new(latest)));
        }
        if latest.task != self.running.task {
            return Ok(PreparedRevalidation::Stale);
        }
        let mut candidate = self.candidate.task.clone();
        if matches!(candidate.state(), TaskState::Succeeded(_))
            && has_live_owned(owner.store(), candidate.id(), &scheduler.ts().list()?)?
        {
            candidate
                .hold_prepared_success()
                .map_err(Error::Transition)?;
        }
        if latest.version != self.running.version || candidate != self.candidate.task {
            self.transaction.rebase_task_candidate(
                self.running.task.id(),
                self.running.version,
                latest.version,
                &self.candidate.task,
                &candidate,
            )?;
            if let Some(notifications) = &mut self.notifications {
                notifications.original.rebase_task_candidate(
                    self.running.task.id(),
                    self.running.version,
                    latest.version,
                    &self.candidate.task,
                    &candidate,
                )?;
            }
            self.running = latest;
            self.candidate.task = candidate;
        }
        let refs: HashMap<_, _> = self
            .staged_children
            .iter()
            .map(|(id, task)| (*id, task))
            .collect();
        if !self.wait_targets.is_empty() {
            match validate_wait_graph(
                owner.store(),
                self.candidate.task.id(),
                &self.wait_targets,
                &refs,
            ) {
                Ok(()) => (),
                Err(Error::Ownership) => return Ok(PreparedRevalidation::Stale),
                Err(error) => return Err(error),
            }
        }
        if !refs.is_empty() {
            let targets: Vec<_> = refs.keys().copied().collect();
            match validate_wait_graph(owner.store(), self.candidate.task.id(), &targets, &refs) {
                Ok(()) => (),
                Err(Error::Ownership) => return Ok(PreparedRevalidation::Stale),
                Err(error) => return Err(error),
            }
        }
        Ok(PreparedRevalidation::Ready)
    }
}

// Failure settlement retains its original outcome and the most recently
// prepared checked write until that write commits or is superseded.
/// An owned failure outcome and its checked candidate.
pub struct FailureSettlement {
    attachment: Arc<Attachment>,
    running: task_store::StoredTask,
    reason: String,
    completed: bool,
    check_pending: bool,
    attempt: usize,
    candidate: Option<FailureCandidate>,
}

struct FailureCandidate {
    task: task_store::StoredTask,
    batch: task_store::OwnedBatch,
    graph_version: u64,
}

impl std::fmt::Debug for CompletedInvocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompletedInvocation")
            .field("task_id", &self.running.task.id())
            .finish_non_exhaustive()
    }
}

/// An invocation rejected before settlement remains owned by the caller.
/// `Handler` means the handler returned an error, not that its external effects
/// were rolled back. Indeterminate errors do not prove a commit failed; where
/// available, `IndeterminatePrepared` retains the exact candidate and lease for
/// reconciliation. Nonblocking owned paths retain the original input or the
/// failure candidate in `IndeterminateOwned`.
#[derive(Debug)]
pub enum SettlementError {
    #[cfg(test)]
    Yielded {
        pending: Box<PendingSettlement>,
    },
    /// The poll or failure fence was busy before preparation. The caller
    /// retains the completed invocation and its exclusive lease.
    Pending {
        completed: Box<CompletedInvocation>,
    },
    /// Abort intent was found after preparation; the transaction and lease
    /// remain with the caller.
    PendingPrepared {
        pending: Box<PendingSettlement>,
    },
    /// Prepared state could not be resumed; the caller still owns it.
    RejectedPrepared {
        error: Error,
        pending: Box<PendingSettlement>,
    },
    Rejected {
        error: Error,
        completed: Box<CompletedInvocation>,
    },
    Handler(Error),
    /// A settlement transition may have committed. Retain the prepared
    /// candidate for reconciliation; do not blindly retry it.
    IndeterminatePrepared {
        error: Error,
        pending: Box<PendingSettlement>,
    },
    /// An uncertain settlement retaining its original input or failure candidate.
    /// Inspect durable state before choosing how to reconcile it.
    IndeterminateOwned {
        error: Error,
        pending: Box<PendingSettlement>,
    },
    Indeterminate(Error),
}

impl CompletedInvocation {
    /// Inspect the original handler result without invoking or releasing it.
    pub fn handler_result(&self) -> Option<&Result<Step, String>> {
        match &self.result {
            InvocationResult::Handler(result, _) => Some(result),
            _ => None,
        }
    }
    /// Inspect the original staged effects without preparing or committing them.
    pub fn staged_documents(&self) -> Option<&crate::document::DetachedTaskDocuments> {
        match &self.result {
            InvocationResult::Handler(_, documents) => Some(documents),
            _ => None,
        }
    }
    /// Inspect an abort-handler result; `None` means this was not an abort handler.
    pub fn abort_outcome(&self) -> Option<Option<&str>> {
        match &self.result {
            InvocationResult::Abort(outcome) => Some(outcome.as_deref()),
            _ => None,
        }
    }
    pub fn task_id(&self) -> Uuid {
        self.running.task.id()
    }
}

/// Authority to admit keyed TaskTool children during a single live invocation.
///
/// It borrows the executor reservation and cannot outlive the handler call.
/// It does not park the parent or reserve worker capacity for a child.
///
/// ```compile_fail
/// use pi_rs_durable::task_scheduler::AdmissionHandle;
/// fn escape<'a>(handle: &'a AdmissionHandle<'a>) -> &'static AdmissionHandle<'static> {
///     handle
/// }
/// ```
pub struct AdmissionHandle<'a> {
    running: &'a task_store::StoredTask,
    lease: &'a InvocationLease,
    abort: bool,
    definitions: &'a std::sync::RwLock<WakeDefinitions>,
    activity: &'a crate::worker_activity::WorkerActivity,
    standalone: bool,
}

impl DetachedInvocation {
    pub(crate) fn cancellation_registration_key(&self) -> (Uuid, Uuid) {
        (self.running.task.id(), self.lease.token)
    }
    pub(crate) fn unhooked_cancellation(&self) -> Option<pi_rs_core::CancellationToken> {
        self.unhooked_cancellation.clone()
    }
    pub fn task_id(&self) -> Uuid {
        self.running.task.id()
    }

    #[cfg(test)]
    pub(crate) fn admit_task_tool_child(
        &self,
        tool: &crate::task_tool::TaskTool,
        key: &str,
        input: Value,
    ) -> Result<Uuid, Error> {
        AdmissionHandle {
            running: &self.running,
            lease: &self.lease,
            abort: self.abort,
            definitions: &self.definitions,
            activity: &self.activity,
            standalone: self.validator.is_none(),
        }
        .admit_task_tool_child(tool, key, input)
    }
}

impl AdmissionHandle<'_> {
    /// Atomically admit or reconnect a keyed TaskTool child. The parent must be
    /// a live, non-aborted TaskTool attempt with this exact executor lease.
    /// A changed definition or input rejects reuse of an existing key.
    /// A storage error can be uncertain; stop and reopen before retrying.
    pub fn admit_task_tool_child(
        &self,
        tool: &crate::task_tool::TaskTool,
        key: &str,
        input: Value,
    ) -> Result<Uuid, Error> {
        self.admit_child(tool, None, key, input)
    }

    pub(crate) fn admit_sealed_child(
        &self,
        tool: &crate::task_tool::TaskTool,
        sealed: &crate::async_task_tool::ToolIdentity,
        key: &str,
        input: Value,
    ) -> Result<(Uuid, Uuid), Error> {
        if !self.standalone {
            return Err(Error::Registration);
        }
        let parent = self.running.task.id();
        self.admit_child(tool, Some(sealed), key, input)
            .map(|child| (parent, child))
    }

    fn admit_child(
        &self,
        tool: &crate::task_tool::TaskTool,
        sealed: Option<&crate::async_task_tool::ToolIdentity>,
        key: &str,
        input: Value,
    ) -> Result<Uuid, Error> {
        let Some(attempt) = self.running.task.attempt_id() else {
            return Err(Error::Ownership);
        };
        if self.abort || self.lease.retired || !self.running.task.is_task_tool() {
            return Err(Error::Ownership);
        }
        let (identity, child) = if sealed.is_some() {
            tool.sealed_child_candidate(&self.running.task, input)
        } else {
            tool.child_candidate(&self.running.task, input)
        }
        .map_err(Error::Handler)?;
        let owner = &self.lease.attachment.owner;
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        let state = self.lease.state.lock().map_err(|_| Error::Poisoned)?;
        if state.poisoned
            || self
                .lease
                .scheduler_poison
                .load(std::sync::atomic::Ordering::Acquire)
            || !state.attempts.contains(&self.lease.id)
            || state.leases.get(&self.lease.id) != Some(&(self.lease.token, Some(attempt)))
            || state.failed_handlers.contains(&self.lease.id)
            || state.published.iter().any(|(id, _)| *id == self.lease.id)
        {
            return Err(Error::Ownership);
        }
        let publication = AdmissionPublication { state };
        let tasks = TaskStore::new(owner.store());
        let parent = tasks.load(self.lease.id)?.ok_or(Error::Ownership)?;
        if parent.task != self.running.task
            || parent.task.attempt_id() != Some(attempt)
            || parent.task.abort_requested()
            || parent.task.state() != &TaskState::Running
            || !publication.permits_admission(owner, Uuid::nil(), Some(&parent.task))
        {
            return Err(Error::Ownership);
        }
        let definitions = self.definitions.read().map_err(|_| Error::Poisoned)?;
        let tool_identity = tool.identity().ok_or(Error::Registration)?;
        if !definitions
            .registry
            .matches_task_tool(&tool_identity, &tool.handler())
        {
            return Err(Error::Registration);
        }
        if let Some(identity) = sealed {
            let registration = definitions
                .registry
                .entries
                .get(&tool_identity.kind)
                .ok_or(Error::Registration)?;
            if registration
                .receipt_capture
                .as_ref()
                .is_none_or(|capture| &capture.identity != identity)
            {
                return Err(Error::Registration);
            }
        }
        if pending_abort_root_for_running(&tasks, parent.task.id(), false)?.is_some() {
            return Err(Error::Ownership);
        }
        let prepared = crate::keyed_child::prepare(
            owner.store(),
            &parent,
            key,
            &identity,
            child.input(),
            &child,
        )
        .map_err(|e| Error::Handler(e.to_string()))?;
        let mut transaction = crate::document::Transaction::new();
        transaction.append(crate::task_store::OwnedBatch {
            checks: prepared.checks,
            mutations: prepared.mutations,
        })?;
        if let Some(identity) = sealed {
            let identity_batch = crate::async_task_protocol::prepare_child_identity(
                owner.store(),
                prepared.child,
                identity,
            )
            .map_err(|error| Error::Handler(error.to_string()))?;
            transaction.append(identity_batch)?;
        }
        let checks = crate::ownership_edge::owner_chain_admission_checks(
            owner.store(),
            parent.task.id(),
            child.id(),
        )
        .map_err(|_| Error::Ownership)?;
        transaction.append(crate::task_store::OwnedBatch {
            checks,
            mutations: vec![],
        })?;
        if let Err(error) = transaction.commit(owner.store()) {
            self.lease
                .scheduler_poison
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        self.activity.notify();
        Ok(prepared.child)
    }
}

/// Memo writes are committed under the same attempt, lease, ancestor and
/// registration checks as dispatch. A failed operation poisons the handle,
/// including when a hook ignores its error.
struct TaskHookMemo<'a> {
    invocation: &'a DetachedInvocation,
    after: bool,
    failed: std::sync::atomic::AtomicBool,
}
impl TaskHookMemo<'_> {
    fn access(
        &self,
        key: &str,
        value: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, String> {
        use std::sync::atomic::Ordering;
        if self.failed.load(Ordering::Acquire) {
            return Err("memo is unavailable".into());
        }
        let operation = || -> Result<Option<serde_json::Value>, String> {
            if key.is_empty() || key.len() > 4096 {
                return Err("invalid memo key".into());
            }
            let invocation = self.invocation;
            let capture = invocation
                .receipt_capture
                .as_ref()
                .ok_or("missing capture")?;
            let store = invocation.lease.attachment.owner.store();
            let storage_key = format!(
                "runtime/extension-memo/{}/{}",
                invocation.running.task.id(),
                key
            )
            .into_bytes();
            let entry = store.get(&storage_key).map_err(|e| e.to_string())?;
            let expected = entry
                .as_ref()
                .map_or(durable_store::Expected::Missing, |e| {
                    durable_store::Expected::Version(e.version)
                });
            let check = durable_store::Check {
                key: storage_key.clone(),
                expected,
            };
            let (result, mutations) = match (entry, value) {
                (Some(entry), _) => (
                    Some(serde_json::from_slice(&entry.value).map_err(|e| e.to_string())?),
                    vec![],
                ),
                (None, Some(value)) => {
                    let bytes = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
                    if bytes.len() > 65536 {
                        return Err("memo value exceeds 64 KiB".into());
                    }
                    (
                        Some(value),
                        vec![durable_store::Mutation::Put {
                            key: storage_key,
                            value: bytes,
                        }],
                    )
                }
                (None, None) => (None, vec![]),
            };
            invocation
                .checked_tool_transaction(
                    &capture.identity,
                    capture.executor.tool().as_ref(),
                    &capture.implementation,
                    capture.version,
                    (false, self.after),
                    (&[check], &mutations),
                )
                .map_err(|e| e.to_string())?;
            Ok(result)
        };
        let result = operation();
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
}
#[async_trait::async_trait]
impl crate::extension::Memo for TaskHookMemo<'_> {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, String> {
        self.access(key, None)
    }
    async fn get_or_insert(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.access(key, Some(value))?
            .ok_or_else(|| "memo insertion failed".into())
    }
}

impl DetachedInvocation {
    /// Record a standalone intent while both task reservations and the graph
    /// gate are held. The commit can be a logical dispatch start only when the
    /// caller also owns, cancels, and joins the authenticated executor. Effects
    /// may physically begin after a later abort.
    pub(crate) fn commit_unhooked_intent(
        &self,
        identity: &crate::async_task_tool::ToolIdentity,
        tool: &dyn pi_rs_core::Tool,
        implementation: &str,
        version: u32,
    ) -> Result<crate::async_task_protocol::Intent, crate::async_task_protocol::Error> {
        self.check_or_commit_unhooked_intent(identity, tool, implementation, version, true)
    }

    fn check_or_commit_unhooked_intent(
        &self,
        identity: &crate::async_task_tool::ToolIdentity,
        tool: &dyn pi_rs_core::Tool,
        implementation: &str,
        version: u32,
        commit: bool,
    ) -> Result<crate::async_task_protocol::Intent, crate::async_task_protocol::Error> {
        self.checked_tool_transaction(identity, tool, implementation, version, (commit, false), (&[], &[]))
    }

    fn checked_tool_transaction(
        &self,
        identity: &crate::async_task_tool::ToolIdentity,
        tool: &dyn pi_rs_core::Tool,
        implementation: &str,
        version: u32,
        phase: (bool, bool),
        extra: (&[durable_store::Check], &[durable_store::Mutation]),
    ) -> Result<crate::async_task_protocol::Intent, crate::async_task_protocol::Error> {
        use crate::async_task_protocol::Error as ProtocolError;
        let (commit, after) = phase;
        if self.abort
            || self.validator.is_some()
            || !self.running.task.is_task_tool()
            || self.running.task.replay_safe()
            || !identity.matches(tool, implementation, version)
        {
            return Err(ProtocolError::Invalid);
        }
        let TaskOwner::Task(parent_id) = self.running.task.owner() else {
            return Err(ProtocolError::Invalid);
        };
        let _failure = self
            .lease
            .failure_fence
            .lock()
            .map_err(|_| ProtocolError::Invalid)?;
        let owner = &self.lease.attachment.owner;
        let _gate = owner
            .task_gate()
            .lock()
            .map_err(|_| ProtocolError::Invalid)?;
        let state = self
            .lease
            .state
            .lock()
            .map_err(|_| ProtocolError::Invalid)?;
        if self
            .unhooked_cancellation
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
        {
            return Err(ProtocolError::Invalid);
        }
        let parent = task_store::TaskStore::new(owner.store())
            .load(*parent_id)?
            .ok_or(ProtocolError::Invalid)?;
        let parent_live = match parent.task.state() {
            TaskState::Running => {
                state.attempts.contains(parent_id)
                    && parent.task.attempt_id().is_some()
                    && !state.published.iter().any(|(id, _)| *id == *parent_id)
            }
            TaskState::Waiting(
                WaitFor::Tasks(ids) | WaitFor::TasksAny(ids) | WaitFor::TasksFailFast(ids),
            ) => parent.task.attempt_id().is_none() && ids.contains(&self.running.task.id()),
            _ => false,
        };
        if state.poisoned
            || state.handler_failed
            || self.lease.retired
            || !state.attempts.contains(&self.running.task.id())
            || state.leases.get(&self.lease.id)
                != Some(&(self.lease.token, self.running.task.attempt_id()))
            || state
                .published
                .iter()
                .any(|(id, _)| *id == self.running.task.id())
            || !parent.task.is_task_tool()
            || !parent_live
            || parent.task.abort_requested()
        {
            return Err(ProtocolError::Invalid);
        }
        if pending_abort_root_for_running(
            &task_store::TaskStore::new(owner.store()),
            self.running.task.id(),
            true,
        )
        .map_err(|_| ProtocolError::Invalid)?
        .is_some()
        {
            return Err(ProtocolError::Invalid);
        }
        let (intent, batch) = if after {
            crate::async_task_protocol::prepare_after_phase(owner.store(), &self.running, identity)?
        } else {
            crate::async_task_protocol::prepare_intent(owner.store(), &self.running, identity)?
        };
        let definitions = self
            .definitions
            .read()
            .map_err(|_| ProtocolError::Invalid)?;
        let registered = self
            .running
            .task
            .kind()
            .and_then(|kind| definitions.registry.entries.get(kind))
            .and_then(|entry| entry.receipt_capture.as_ref())
            .ok_or(ProtocolError::Invalid)?;
        if !self
            .receipt_capture
            .as_ref()
            .is_some_and(|captured| Arc::ptr_eq(captured, registered))
        {
            return Err(ProtocolError::Invalid);
        }
        let mut checks = batch.checks;
        checks.extend_from_slice(extra.0);
        let mut mutations = if commit { batch.mutations } else { Vec::new() };
        mutations.extend_from_slice(extra.1);
        owner.store().commit(&checks, &mutations)?;
        Ok(intent)
    }

    fn persist_raw_tool_result(
        &self,
        intent: &crate::async_task_protocol::Intent,
        outcome: &pi_rs_core::Outcome,
    ) -> Result<(), String> {
        if !intent.matches_attempt(&self.running.task) || self.lease.retired {
            return Err("stale tool attempt".into());
        }
        let _failure = self.lease.failure_fence.lock().map_err(|e| e.to_string())?;
        let owner = &self.lease.attachment.owner;
        let _gate = owner.task_gate().lock().map_err(|e| e.to_string())?;
        let state = self.lease.state.lock().map_err(|e| e.to_string())?;
        if !state.attempts.contains(&self.running.task.id())
            || state.leases.get(&self.lease.id)
                != Some(&(self.lease.token, self.running.task.attempt_id()))
        {
            return Err("stale tool lease".into());
        }
        let batch = crate::async_task_protocol::prepare_raw_result(owner.store(), intent, outcome)
            .map_err(|e| e.to_string())?;
        owner
            .store()
            .commit(&batch.checks, &batch.mutations)
            .map_err(|e| e.to_string())
    }

    fn release_undispatched(mut self) -> Result<CompletedInvocation, Error> {
        let owner = &self.lease.attachment.owner;
        {
            let _failure = self
                .lease
                .failure_fence
                .lock()
                .map_err(|_| Error::Poisoned)?;
            let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
            let tasks = task_store::TaskStore::new(owner.store());
            let mut current = tasks
                .load(self.running.task.id())?
                .ok_or(Error::Ownership)?;
            if current.task.state() != &TaskState::Running
                || current.task.attempt_id() != self.running.task.attempt_id()
                || current.task.attempt_id().is_none()
            {
                return Err(Error::Ownership);
            }
            current.task.yield_ready().map_err(Error::Transition)?;
            let mut batch = tasks.prepare_save(&[task_store::Save::Update(&current)])?;
            batch
                .checks
                .push(crate::async_task_protocol::undispatched_intent_check(
                    current.task.id(),
                ));
            let mut live = self.lease.state.lock().map_err(|_| Error::Poisoned)?;
            if !live
                .leases
                .get(&self.lease.id)
                .is_some_and(|(token, attempt)| {
                    *token == self.lease.token && *attempt == self.running.task.attempt_id()
                })
            {
                return Err(Error::Ownership);
            }
            owner
                .store()
                .commit(&batch.checks, &batch.mutations)
                .map_err(task_store::Error::from)?;
            #[cfg(test)]
            {
                let hook = release_hooks().lock().unwrap().remove(&self.lease.id);
                if let Some(hook) = hook {
                    hook();
                }
            }
            live.leases.remove(&self.lease.id);
            live.attempts.remove(&self.lease.id);
            live.published.retain(|(id, _)| *id != self.lease.id);
            self.lease.retired = true;
        }
        Err(Error::Stopped)
    }

    fn abort_precedes_unhooked_dispatch(&self) -> Result<bool, Error> {
        let owner = &self.lease.attachment.owner;
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        let tasks = task_store::TaskStore::new(owner.store());
        Ok(tasks
            .load(self.running.task.id())?
            .is_some_and(|task| task.task.abort_requested())
            || pending_abort_root_for_running(&tasks, self.running.task.id(), true)?.is_some())
    }

    /// Run without holding the scheduler mutex. An unwinding TaskTool handler
    /// panic becomes a published failure so its owned graph can be cleaned up.
    /// Generic handler panics and abort handler panics retain their semantics.
    pub fn execute(self) -> Result<CompletedInvocation, Error> {
        if !self.sealed_registration
            && crate::async_task_protocol::has_sealed_identity(
                self.lease.attachment.owner.store(),
                self.running.task.id(),
            )
            .map_err(|error| Error::Handler(format!("sealed identity lookup failed: {error}")))?
        {
            return Err(Error::Handler(
                "sealed task requires its registered executor".into(),
            ));
        }
        let identity_lease = if let Some(capture) = &self.receipt_capture {
            match crate::nested_identity::checked_lease(
                capture.executor.tool(),
                &capture.implementation,
            ) {
                Ok(lease) => Some(lease),
                Err(crate::nested_identity::LeaseFailure::Busy) => {
                    #[cfg(test)]
                    if let Some(hook) = &capture.busy_hook {
                        hook();
                    }
                    return self.release_undispatched();
                }
                Err(crate::nested_identity::LeaseFailure::Mismatch) => {
                    if self
                        .unhooked_cancellation
                        .as_ref()
                        .is_some_and(|token| token.is_cancelled())
                        || self.abort_precedes_unhooked_dispatch()?
                    {
                        return self.release_undispatched();
                    }
                    return Err(Error::Handler(
                        "unhooked tool identity unavailable or changed".into(),
                    ));
                }
            }
        } else {
            None
        };
        let preflight = self.receipt_capture.as_ref().is_some_and(|capture| {
            capture.executor.has_before_hook() && self.running.task.checkpoint().phase == "initial"
        });
        if preflight {
            let capture = self.receipt_capture.as_ref().expect("preflight capture");
            if self
                .check_or_commit_unhooked_intent(
                    &capture.identity,
                    capture.executor.tool().as_ref(),
                    &capture.implementation,
                    capture.version,
                    false,
                )
                .is_err()
            {
                if self
                    .unhooked_cancellation
                    .as_ref()
                    .is_some_and(|token| token.is_cancelled())
                    || self.abort_precedes_unhooked_dispatch()?
                {
                    return self.release_undispatched();
                }
                return Err(Error::Handler("preflight authorization rejected".into()));
            }
        }
        let prepared = match &self.receipt_capture {
            Some(capture) if capture.executor.has_before_hook() && !preflight => Some(
                capture
                    .executor
                    .checkpoint_arguments(&self.running.task)
                    .map_err(Error::Handler)?,
            ),
            _ => None,
        };
        let raw_result = if !preflight
            && !self.abort
            && self
                .receipt_capture
                .as_ref()
                .is_some_and(|c| c.executor.has_after_hook())
        {
            crate::async_task_protocol::read_raw_result(
                self.lease.attachment.owner.store(),
                self.running.task.id(),
            )
            .map_err(|e| Error::Handler(format!("raw result rejected: {e}")))?
        } else {
            None
        };
        let intent = if preflight {
            None
        } else if let Some((intent, _)) = &raw_result {
            Some(intent.clone())
        } else if let Some(capture) = &self.receipt_capture {
            #[cfg(test)]
            if let Some(before) = &capture.before_intent {
                before();
            }
            let intent = match self.commit_unhooked_intent(
                &capture.identity,
                capture.executor.tool().as_ref(),
                &capture.implementation,
                capture.version,
            ) {
                Ok(intent) => Some(intent),
                Err(crate::async_task_protocol::Error::Invalid)
                    if self
                        .unhooked_cancellation
                        .as_ref()
                        .is_some_and(|token| token.is_cancelled()) =>
                {
                    return self.release_undispatched();
                }
                Err(crate::async_task_protocol::Error::Invalid)
                    if self.abort_precedes_unhooked_dispatch()? =>
                {
                    None
                }
                Err(error) => {
                    return Err(Error::Handler(format!("unhooked intent rejected: {error}")));
                }
            };
            #[cfg(test)]
            if intent.is_some()
                && let Some(after) = &capture.after_intent
            {
                after();
            }
            intent
        } else {
            None
        };
        let preflight_result = if preflight {
            let capture = self.receipt_capture.as_ref().expect("preflight capture");
            let cancel = self
                .unhooked_cancellation
                .as_ref()
                .expect("preflight cancellation");
            let memo = TaskHookMemo {
                invocation: &self,
                after: false,
                failed: std::sync::atomic::AtomicBool::new(false),
            };
            Some(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || {
                    let result = capture
                        .executor
                        .preflight(&self.running.task, cancel, &memo);
                    if memo.failed.load(std::sync::atomic::Ordering::Acquire) {
                        Err("task-fenced memo failed".into())
                    } else {
                        result
                    }
                },
            )))
        } else if self
            .receipt_capture
            .as_ref()
            .is_some_and(|c| c.executor.has_after_hook())
            && intent.is_some()
            && !self.abort
        {
            let capture = self.receipt_capture.as_ref().unwrap();
            let intent_ref = intent.as_ref().unwrap();
            let cancel = self.unhooked_cancellation.as_ref().unwrap();
            let memo = TaskHookMemo {
                invocation: &self,
                after: true,
                failed: std::sync::atomic::AtomicBool::new(false),
            };
            Some(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || {
                    let outcome = if let Some((_, outcome)) = raw_result {
                        outcome
                    } else {
                        let result = capture.executor.run(
                            &self.running.task,
                            intent_ref,
                            self.lease.attachment.owner.store(),
                            cancel,
                            prepared.clone(),
                        )?;
                        let Step::Succeed(value) = result else {
                            return Err("invalid tool result".into());
                        };
                        let outcome: pi_rs_core::Outcome =
                            serde_json::from_value(value).map_err(|e| e.to_string())?;
                        if matches!(
                            outcome,
                            pi_rs_core::Outcome::Unknown(_) | pi_rs_core::Outcome::Pending { .. }
                        ) {
                            return Ok(Step::Succeed(
                                serde_json::to_value(outcome).map_err(|e| e.to_string())?,
                            ));
                        }
                        self.persist_raw_tool_result(intent_ref, &outcome)?;
                        outcome
                    };
                    self.checked_tool_transaction(
                        &capture.identity,
                        capture.executor.tool().as_ref(),
                        &capture.implementation,
                        capture.version,
                        false,
                        true,
                        &[],
                        &[],
                    )
                    .map_err(|e| format!("after authorization rejected: {e}"))?;
                    // A previous attempt may have committed the final receipt
                    // before dying during settlement. Do not invoke after twice.
                    if let Some(final_outcome) = crate::async_task_protocol::read_receipt(
                        self.lease.attachment.owner.store(),
                        intent_ref,
                    )
                    .map_err(|e| e.to_string())?
                    {
                        return Ok(Step::Succeed(
                            serde_json::to_value(final_outcome).map_err(|e| e.to_string())?,
                        ));
                    }
                    let result = capture
                        .executor
                        .after(&self.running.task, outcome, cancel, &memo);
                    if memo.failed.load(std::sync::atomic::Ordering::Acquire) {
                        Err("task-fenced memo failed".into())
                    } else {
                        self.checked_tool_transaction(
                            &capture.identity,
                            capture.executor.tool().as_ref(),
                            &capture.implementation,
                            capture.version,
                            false,
                            true,
                            &[],
                            &[],
                        )
                        .map_err(|e| format!("after completion rejected: {e}"))?;
                        result
                    }
                },
            )))
        } else {
            None
        };
        let after_enabled = self
            .receipt_capture
            .as_ref()
            .is_some_and(|c| c.executor.has_after_hook())
            && !preflight;
        let after_success = preflight_result
            .as_ref()
            .is_some_and(|r| matches!(r, Ok(Ok(Step::Succeed(_)))));
        let Self {
            running,
            handler,
            abort,
            validator,
            lease,
            definitions,
            activity,
            receipt_capture,
            unhooked_cancellation,
            sealed_registration: _,
        } = self;
        let capture_enabled =
            receipt_capture.is_some() && intent.is_some() && (!after_enabled || after_success);
        let result = if abort {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.run(&running.task)
            }));
            let outcome = match result {
                Ok(Ok(Step::Succeed(_))) => None,
                Ok(Ok(Step::Fail(reason))) => Some(reason),
                Ok(Ok(_)) => Some("abort handler must return Succeed or Fail".into()),
                Ok(Err(reason)) => Some(format!("abort handler failed: {reason}")),
                Err(payload) => {
                    crate::extension::discard_panic_payload(payload);
                    Some("abort handler panicked".into())
                }
            };
            InvocationResult::Abort(outcome)
        } else {
            let owner = &lease.attachment.owner;
            let mut documents = crate::document::TaskDocuments::new(owner.store(), &running.task)?;
            documents.set_input_validator(validator.as_ref());
            let result = preflight_result.unwrap_or_else(|| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match (&receipt_capture, &intent, &unhooked_cancellation) {
                        (Some(capture), Some(intent), Some(cancel)) => capture.executor.run(
                            &running.task,
                            intent,
                            owner.store(),
                            cancel,
                            prepared,
                        ),
                        (Some(_), None, Some(_)) => {
                            Ok(Step::Fail("aborted before dispatch".into()))
                        }
                        (None, None, None) => {
                            let admission = AdmissionHandle {
                                running: &running,
                                lease: &lease,
                                abort,
                                definitions: &definitions,
                                activity: &activity,
                                standalone: validator.is_none(),
                            };
                            handler.run_with_admission(&running.task, &mut documents, &admission)
                        }
                        _ => Err("invalid unhooked executor state".into()),
                    }
                }))
            });
            let result = match result {
                Ok(result) => result,
                Err(payload) if running.task.is_task_tool() => {
                    crate::extension::discard_panic_payload(payload);
                    // Nothing staged by a panicking handler is a valid result.
                    // In particular, a staged input conflict must not turn the
                    // panic into a replayable Continue or hide its cleanup.
                    documents = documents.discard_staged();
                    Err("task handler panicked; execution outcome requires reconciliation".into())
                }
                Err(payload) => {
                    let mut lease = lease;
                    {
                        let _fence = lease
                            .failure_fence
                            .lock()
                            .unwrap_or_else(|e| e.into_inner());
                        let mut state = lease.state.lock().unwrap_or_else(|e| e.into_inner());
                        state.handler_failed = true;
                        state.failed_handlers.insert(running.task.id());
                        lease.retired = true;
                    }
                    drop(lease);
                    std::panic::resume_unwind(payload);
                }
            };
            let documents = documents.detach(owner.clone(), &running)?;
            // A checked input proof invalidates the entire attempt even when
            // a handler stringifies or ignores the document error.
            let result = if documents.task_input_conflict.is_some()
                || documents.task_input_storage.is_some()
            {
                Ok(Step::Continue(running.task.checkpoint().clone()))
            } else {
                result
            };
            if result.is_err() && !running.task.is_task_tool() {
                let _fence = lease.failure_fence.lock().map_err(|_| Error::Poisoned)?;
                let mut state = lease.state.lock().map_err(|_| Error::Poisoned)?;
                state.handler_failed = true;
                state.failed_handlers.insert(running.task.id());
            }
            InvocationResult::Handler(result, Box::new(documents))
        };
        // Publish completion under the same fence as all task commits. A
        // later dependent result cannot overtake this still-owned outcome.
        {
            let _fence = lease.failure_fence.lock().map_err(|_| Error::Poisoned)?;
            let mut state = lease.state.lock().map_err(|_| Error::Poisoned)?;
            let abort_intent = match &result {
                InvocationResult::Abort(_) => true,
                InvocationResult::Handler(_, documents)
                    if documents.task_input_storage.is_some() =>
                {
                    true
                }
                // A non-replay-safe input conflict will settle as an owned
                // failure, so publish its fence before dependent work can run.
                InvocationResult::Handler(_, documents)
                    if documents.task_input_conflict.is_some() && !running.task.replay_safe() =>
                {
                    true
                }
                InvocationResult::Handler(Err(_), _) => true,
                InvocationResult::Handler(Ok(Step::Fail(_)), _) => true,
                InvocationResult::Handler(Ok(Step::Succeed(_)), _) => running.task.is_task_tool(),
                _ => false,
            };
            state.published.push_back((running.task.id(), abort_intent));
        }
        Ok(CompletedInvocation {
            running,
            result,
            lease,
            receipt_capture: capture_enabled,
            _identity_lease: identity_lease,
        })
    }
}

#[derive(Clone)]
struct WakeDefinitions {
    registry: Registry,
    failed_migrations: HashMap<Uuid, (Arc<()>, String)>,
}

/// Task wake reader independent of the mutex held across handler execution.
#[derive(Clone)]
pub(crate) struct WakeReader {
    owner: Arc<OwnedStore>,
    definitions: Arc<std::sync::RwLock<WakeDefinitions>>,
    live_invocations: Arc<Mutex<InvocationState>>,
}
impl WakeReader {
    pub(crate) fn snapshot(&self, now_millis: i64) -> Result<WakeSnapshot, Error> {
        let _gate = self.owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        let definitions = self.definitions.read().map_err(|_| Error::Poisoned)?;
        let ts = TaskStore::new(self.owner.store());
        let tasks = ts.list()?;
        let mut result = WakeSnapshot {
            active: false,
            at: None,
            blocked: Vec::new(),
        };
        for item in &tasks {
            let task = &item.task;
            if task.is_terminal() {
                continue;
            }
            let reason = definitions.blocked_reason(task);
            if let Some(reason) = reason.clone() {
                result.blocked.push(BlockedTask {
                    id: task.id(),
                    kind: task.kind().map(str::to_owned),
                    owner: task.owner().clone(),
                    reason,
                });
            }
            match task.state() {
                TaskState::Completing(_) | TaskState::CompletingFailed(_)
                    if task.is_task_tool() && has_unmarked_nested(task.id(), &tasks) =>
                {
                    result.active = true
                }
                TaskState::CompletingFailed(_) if !task.abort_requested() => result.active = true,
                TaskState::Completing(_) | TaskState::CompletingFailed(_)
                    if !has_live_owned(self.owner.store(), task.id(), &tasks)? =>
                {
                    result.active = true
                }
                TaskState::Ready if task.abort_requested() => {
                    if !tasks.iter().any(|child| {
                        child.task.owner() == &TaskOwner::Task(task.id())
                            && !child.task.is_terminal()
                    }) {
                        result.active = true;
                    }
                }
                TaskState::Ready
                    if reason.is_none()
                        && definitions.ancestors_available(
                            self.owner.store(),
                            task,
                            &tasks,
                            &self.live_invocations,
                        ) =>
                {
                    result.active = true
                }
                TaskState::Running if task.abort_requested() || reason.is_none() => {
                    result.active = true
                }
                TaskState::Waiting(WaitFor::TasksFailFast(ids))
                    if !task.abort_requested()
                        && ids.iter().any(|id| {
                            tasks.iter().any(|child| {
                                child.task.id() == *id
                                    && matches!(
                                        child.task.state(),
                                        TaskState::Failed(_)
                                            | TaskState::CompletingFailed(_)
                                            | TaskState::Cancelled
                                    )
                            })
                        })
                        && ids.iter().any(|id| {
                            tasks.iter().any(|child| {
                                child.task.id() == *id
                                    && !child.task.is_terminal()
                                    && !child.task.abort_requested()
                                    && !matches!(child.task.state(), TaskState::CompletingFailed(_))
                            })
                        }) =>
                {
                    result.active = true;
                }
                TaskState::Waiting(wait) => {
                    let deadline = match wait {
                        WaitFor::DeadlineMillis(at) => Some(*at),
                        WaitFor::Sleep { until_millis, .. }
                        | WaitFor::TypedSleep { until_millis, .. } => *until_millis,
                        _ => None,
                    };
                    let ready = ts.wait_ready(item, now_millis)?;
                    if ready
                        && ((reason.is_none()
                            && definitions.ancestors_available(
                                self.owner.store(),
                                task,
                                &tasks,
                                &self.live_invocations,
                            ))
                            || (task.abort_requested()
                                && !tasks.iter().any(|child| {
                                    child.task.owner() == &TaskOwner::Task(task.id())
                                        && !child.task.is_terminal()
                                })))
                    {
                        result.active = true;
                    } else if !ready && let Some(at) = deadline.filter(|at| *at > now_millis) {
                        result.at = Some(result.at.map_or(at, |old| old.min(at)));
                    }
                }
                _ => {}
            }
        }
        Ok(result)
    }
}
// Follow task ownership through foreground conversation edges as well. A root
// conversation may be absent in standalone task stores; an owned conversation
// must have a valid record and a consistent edge. Invalid graphs fail closed.
fn task_ancestors_available(
    store: &durable_store::Store,
    task: &Task,
    tasks: &[task_store::StoredTask],
    mut blocked: impl FnMut(&Task) -> bool,
) -> bool {
    let edges = crate::ownership_edge::EdgeStore::new(store);
    let mut current = task;
    let mut seen_tasks = HashSet::from([task.id()]);
    let mut seen_conversations = HashSet::new();
    let mut past_conversation = false;
    loop {
        let (parent_id, through_conversation) = match current.owner() {
            TaskOwner::Task(id) => (*id, false),
            TaskOwner::Conversation(id) => {
                if current.is_background() {
                    return true;
                }
                if !seen_conversations.insert(*id) {
                    return false;
                }
                let Ok(edge) = edges.get(*id) else {
                    return false;
                };
                let key = format!("runtime/conversation/{id}");
                let Ok(entry) = store.get(key.as_bytes()) else {
                    return false;
                };
                match entry {
                    Some(entry) => {
                        let Ok(conversation) =
                            serde_json::from_slice::<crate::runtime::Conversation>(&entry.value)
                        else {
                            return false;
                        };
                        if conversation.id != *id || conversation.abort_requested {
                            return false;
                        }
                    }
                    None if edge.is_some() => return false,
                    None => (),
                }
                let Some(edge) = edge else {
                    return true;
                };
                if edge.edge.mode == crate::ownership_edge::Mode::Background {
                    return true;
                }
                let Some(parent) = tasks.iter().find(|entry| entry.task.id() == edge.edge.task)
                else {
                    return false;
                };
                if edge.edge.task_owner != *parent.task.owner() {
                    return false;
                }
                (edge.edge.task, true)
            }
        };
        if !seen_tasks.insert(parent_id) {
            return false;
        }
        let Some(parent) = tasks.iter().find(|entry| entry.task.id() == parent_id) else {
            return false;
        };
        // A successfully finished task remains an ownership anchor for work
        // admitted later in its foreground conversation. Keep walking above it.
        past_conversation |= through_conversation;
        let successful_anchor = past_conversation
            && matches!(parent.task.state(), TaskState::Succeeded(_))
            && !parent.task.abort_requested();
        if !successful_anchor && blocked(&parent.task) {
            return false;
        }
        current = &parent.task;
    }
}

impl WakeDefinitions {
    fn blocked_reason(&self, task: &Task) -> Option<BlockedReason> {
        if task.is_completing() {
            return None;
        }
        let Some(reg) = task.kind().and_then(|kind| self.registry.entries.get(kind)) else {
            return Some(BlockedReason::MissingTask);
        };
        if task.definition_version() > reg.version {
            return Some(BlockedReason::TaskTooOld);
        }
        if task.definition_version() < reg.version {
            if let Some((identity, error)) = self.failed_migrations.get(&task.id())
                && Arc::ptr_eq(identity, &reg.identity)
            {
                return Some(BlockedReason::MigrationFailed(error.clone()));
            }
            if reg.migrate.is_none() {
                return Some(BlockedReason::MigrationFailed("missing migration".into()));
            }
        }
        None
    }
    fn ancestors_available(
        &self,
        store: &durable_store::Store,
        task: &Task,
        tasks: &[task_store::StoredTask],
        live: &Mutex<InvocationState>,
    ) -> bool {
        task_ancestors_available(store, task, tasks, |parent| {
            parent.abort_requested()
                || parent.is_terminal()
                || matches!(parent.state(), TaskState::CompletingFailed(_))
                || (parent.state() == &TaskState::Running && !live_task_tool_parent(parent, live))
                || self.blocked_reason(parent).is_some()
        })
    }
}

#[cfg(test)]
type CommitHook = Arc<dyn Fn(usize, &durable_store::Store) + Send + Sync>;

pub struct Scheduler {
    attachment: Arc<Attachment>,
    live_invocations: Arc<Mutex<InvocationState>>,
    failure_fence: Arc<Mutex<()>>,
    wake_reader: WakeReader,
    registry: Registry,
    validator: Option<crate::runtime::Harness>,
    cleanup_jobs: tokio::task::JoinSet<(Uuid, Result<bool, crate::runtime::Error>)>,
    cleanup_running: HashSet<Uuid>,
    cleanup_cursor: Option<Uuid>,
    cleanup_timeout: std::time::Duration,
    cleanup_failures: HashMap<Uuid, String>,
    poisoned: Arc<std::sync::atomic::AtomicBool>,
    reservations_stopped: bool,
    reservation_stop: Arc<std::sync::atomic::AtomicBool>,
    failed_migrations: HashMap<Uuid, (Arc<()>, String)>,
    // Interrupted invocations held until pending fail-fast marks have landed.
    deferred_recovery: HashSet<Uuid>,
    #[cfg(test)]
    result_commit_hook: Option<CommitHook>,
    #[cfg(test)]
    after_order_hook: Option<CommitHook>,
    #[cfg(test)]
    reservation_commit_hook: Option<CommitHook>,
    #[cfg(test)]
    after_result_load_hook: Option<CommitHook>,
    #[cfg(test)]
    preparation_conflict_hook: Option<CommitHook>,
    #[cfg(test)]
    failure_commit_hook: Option<CommitHook>,
    #[cfg(test)]
    failure_committed_hook: Option<CommitHook>,
    #[cfg(test)]
    inject_batch_prepare_conflict: bool,
    #[cfg(test)]
    inject_post_commit_error: bool,
    #[cfg(test)]
    inject_preparation_error: u8,
    #[cfg(test)]
    inject_failure_commit_error: u8,
    #[cfg(test)]
    inject_abort_commit_error: u8,
    #[cfg(test)]
    inject_aborted_yield_error: u8,
    #[cfg(test)]
    yield_before_preparation: bool,
    #[cfg(test)]
    yield_after_graph_conflict: bool,
    #[cfg(test)]
    yielded_prepared: Option<PreparedSettlement>,
    activity: crate::worker_activity::WorkerActivity,
}
impl Scheduler {
    fn make_invocation(
        &self,
        running: task_store::StoredTask,
        handler: Arc<dyn Handler>,
        abort: bool,
    ) -> Result<DetachedInvocation, Error> {
        let id = running.task.id();
        let registered_capture = self.registered(&running.task)?.receipt_capture.clone();
        let sealed_registration = registered_capture.is_some();
        let receipt_capture = if abort { None } else { registered_capture };
        let mut state = self.live_invocations.lock().map_err(|_| Error::Poisoned)?;
        if state.poisoned || !state.attempts.insert(id) {
            return Err(Error::Poisoned);
        }
        let token = Uuid::new_v4();
        state.leases.insert(id, (token, running.task.attempt_id()));
        drop(state);
        let unhooked_cancellation = receipt_capture
            .as_ref()
            .map(|_| pi_rs_core::CancellationToken::new());
        Ok(DetachedInvocation {
            running,
            handler,
            abort,
            sealed_registration,
            validator: self.validator.as_ref().map(|harness| harness.worker()),
            definitions: self.wake_reader.definitions.clone(),
            activity: self.activity.clone(),
            receipt_capture,
            unhooked_cancellation,
            lease: InvocationLease {
                attachment: self.attachment.clone(),
                state: self.live_invocations.clone(),
                failure_fence: self.failure_fence.clone(),
                scheduler_poison: self.poisoned.clone(),
                id,
                token,
                retired: false,
                _activity: self
                    .activity
                    .enter(crate::worker_activity::WorkerKind::Task, id),
            },
        })
    }

    /// Settle a completed invocation against its original scheduler and attempt.
    /// The caller must serialize this operation with other scheduler operations.
    /// A rejected invocation retains its lease and must remain owned until its
    /// caller chooses how to handle it. Failure publication and the complete
    /// settlement are serialized by the same fence.
    pub fn settle_invocation(
        &mut self,
        completed: CompletedInvocation,
    ) -> Result<bool, SettlementError> {
        let multiple = match self.live_invocations.lock() {
            Ok(state) => state.attempts.len() > 1,
            Err(_) => {
                return Err(SettlementError::Rejected {
                    error: Error::Poisoned,
                    completed: Box::new(completed),
                });
            }
        };
        self.settle_invocation_inner(completed, multiple)
    }

    fn settle_invocation_inner(
        &mut self,
        completed: CompletedInvocation,
        nonblocking_abort: bool,
    ) -> Result<bool, SettlementError> {
        if completed.receipt_capture {
            return Err(SettlementError::Rejected {
                error: Error::Handler("tool receipt capture is required before settlement".into()),
                completed: Box::new(completed),
            });
        }
        let fence = self.failure_fence.clone();
        let guard = if nonblocking_abort {
            match fence.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => {
                    return Err(SettlementError::Pending {
                        completed: Box::new(completed),
                    });
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(SettlementError::Rejected {
                        error: Error::Poisoned,
                        completed: Box::new(completed),
                    });
                }
            }
        } else {
            match fence.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    return Err(SettlementError::Rejected {
                        error: Error::Poisoned,
                        completed: Box::new(completed),
                    });
                }
            }
        };
        let health = self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)
            .and_then(|state| {
                if self.poisoned.load(std::sync::atomic::Ordering::Acquire) || state.poisoned {
                    Err(Error::Poisoned)
                } else {
                    Ok(())
                }
            });
        if let Err(error) = health {
            return Err(SettlementError::Rejected {
                error,
                completed: Box::new(completed),
            });
        }
        if !Arc::ptr_eq(&self.live_invocations, &completed.lease.state) {
            return Err(SettlementError::Rejected {
                error: Error::Ownership,
                completed: Box::new(completed),
            });
        }
        let owner = self.owner();
        let task_gate = match owner.task_gate().lock() {
            Ok(gate) => gate,
            Err(_) => {
                return Err(SettlementError::Rejected {
                    error: Error::Poisoned,
                    completed: Box::new(completed),
                });
            }
        };
        let ordering_graph_version = match owner.store().put_version() {
            Ok(version) => version,
            Err(error) => {
                return Err(SettlementError::Rejected {
                    error: task_store::Error::from(error).into(),
                    completed: Box::new(completed),
                });
            }
        };
        if (matches!(&completed.result, InvocationResult::Abort(_))
            && self.has_global_prior(completed.task_id()))
            || self.has_prior_outcome(&owner, &completed.running.task)
        {
            return Err(if nonblocking_abort {
                SettlementError::Pending {
                    completed: Box::new(completed),
                }
            } else {
                SettlementError::Rejected {
                    error: Error::Busy,
                    completed: Box::new(completed),
                }
            });
        }
        let after_other_failure = {
            let state = self
                .live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            state.handler_failed && !state.failed_handlers.contains(&completed.task_id())
        };
        if after_other_failure {
            // Until failed-handler recovery is committed, a descendant could be
            // a fail-fast target. Only drain a task rooted in an independent
            // conversation; hold the task gate through the checked commit.
            let independent = self.separate_from_failures(&owner, &completed.running.task);
            if !independent {
                return Err(SettlementError::Rejected {
                    error: Error::Busy,
                    completed: Box::new(completed),
                });
            }
        }
        #[cfg(test)]
        if self.yield_before_preparation {
            drop(task_gate);
            drop(guard);
            return Err(SettlementError::Yielded {
                pending: Box::new(PendingSettlement::Unprepared(completed)),
            });
        }
        // Preserve the exact handler input until all preparation and commit
        // paths have either succeeded or returned a more precise owned candidate.
        let backup = if nonblocking_abort {
            Some((completed.running.clone(), completed.result.clone()))
        } else {
            None
        };
        let CompletedInvocation {
            running,
            result,
            mut lease,
            receipt_capture,
            _identity_lease: _,
        } = completed;
        let mut reported_handler_failure = false;
        let mut deferred = None;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match result {
            InvocationResult::Handler(result, documents) => self.settle_handler_result(
                running,
                result,
                *documents,
                &mut reported_handler_failure,
                task_gate,
                nonblocking_abort,
                ordering_graph_version,
                &mut deferred,
            ),
            InvocationResult::Abort(outcome) => {
                let _gate = task_gate;
                #[cfg(test)]
                if self.inject_abort_commit_error == 1 {
                    return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
                }
                match outcome {
                    None => self.ts().update(&running, |task| task.cancel())?,
                    Some(reason) => self.ts().update(&running, |task| task.fail(reason))?,
                }
                #[cfg(test)]
                if self.inject_abort_commit_error == 2 {
                    return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
                }
                Ok(true)
            }
        }));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                self.live_invocations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .poisoned = true;
                drop(guard);
                if let Some((running, result)) = backup {
                    crate::extension::discard_panic_payload(payload);
                    return Err(SettlementError::IndeterminateOwned {
                        error: Error::Handler("settlement panicked".into()),
                        pending: Box::new(PendingSettlement::Unprepared(CompletedInvocation {
                            running,
                            result,
                            lease,
                            receipt_capture,
                            _identity_lease: None,
                        })),
                    });
                }
                lease.retired = true;
                drop(lease);
                std::panic::resume_unwind(payload);
            }
        };
        if let Some(deferred) = deferred {
            if matches!(
                deferred,
                DeferredSettlement::IndeterminatePrepared(_)
                    | DeferredSettlement::IndeterminateFailure(_)
            ) {
                self.live_invocations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .poisoned = true;
            }
            drop(guard);
            return Err(match deferred {
                DeferredSettlement::Unprepared(running, result, documents) => {
                    SettlementError::Pending {
                        completed: Box::new(CompletedInvocation {
                            running,
                            result: InvocationResult::Handler(result, Box::new(documents)),
                            lease,
                            receipt_capture,
                            _identity_lease: None,
                        }),
                    }
                }
                DeferredSettlement::Prepared(settlement) => SettlementError::PendingPrepared {
                    pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                },
                DeferredSettlement::IndeterminatePrepared(settlement) => {
                    SettlementError::IndeterminatePrepared {
                        error: result.expect_err("indeterminate commit must have an error"),
                        pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                    }
                }
                DeferredSettlement::IndeterminateFailure(settlement) => {
                    SettlementError::IndeterminateOwned {
                        error: result.expect_err("indeterminate failure must have an error"),
                        pending: Box::new(PendingSettlement::Failure { lease, settlement }),
                    }
                }
                DeferredSettlement::PreparationConflict(conflict) => {
                    SettlementError::PendingPrepared {
                        pending: Box::new(PendingSettlement::PreparationConflict {
                            lease,
                            conflict,
                        }),
                    }
                }
                DeferredSettlement::Failure(settlement) => SettlementError::PendingPrepared {
                    pending: Box::new(PendingSettlement::Failure { lease, settlement }),
                },
            });
        }
        #[cfg(test)]
        if matches!(result, Err(Error::TestYield)) {
            let settlement = self.yielded_prepared.take().expect("prepared yield state");
            drop(guard);
            return Err(SettlementError::Yielded {
                pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
            });
        }
        if result.is_err()
            && !reported_handler_failure
            && let Some((running, result_backup)) = backup
        {
            self.live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .poisoned = true;
            let error = result.expect_err("indeterminate settlement must have an error");
            drop(guard);
            return Err(SettlementError::IndeterminateOwned {
                error,
                pending: Box::new(PendingSettlement::Unprepared(CompletedInvocation {
                    running,
                    result: result_backup,
                    lease,
                    receipt_capture,
                    _identity_lease: None,
                })),
            });
        }
        if result.is_ok() {
            let mut state = self
                .live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.failed_handlers.remove(&lease.id) {
                state.handler_failed = !state.failed_handlers.is_empty();
            }
        }
        if result.is_err() && !reported_handler_failure {
            self.live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .poisoned = true;
        }
        // Failure has already been published under the fence, or the attempt
        // completed. Do not publish it again when dropping the lease.
        lease.retired = true;
        drop(guard);
        drop(lease);
        result.map_err(|error| {
            if reported_handler_failure {
                SettlementError::Handler(error)
            } else {
                SettlementError::Indeterminate(error)
            }
        })
    }

    /// Try the pre-preparation abort fence without waiting for poll or failure
    /// gates. A pending result owns the invocation and must be retried by the
    /// caller. The task gate and later serial settlement paths may still block;
    /// in particular, a new abort discovered after this check uses serial cleanup.
    pub fn try_settle_invocation(
        &mut self,
        completed: CompletedInvocation,
    ) -> Result<bool, SettlementError> {
        let fence = self.failure_fence.clone();
        let guard = match fence.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(SettlementError::Pending {
                    completed: Box::new(completed),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(SettlementError::Rejected {
                    error: Error::Poisoned,
                    completed: Box::new(completed),
                });
            }
        };
        if !Arc::ptr_eq(&self.live_invocations, &completed.lease.state) {
            return Err(SettlementError::Rejected {
                error: Error::Ownership,
                completed: Box::new(completed),
            });
        }
        let health = self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)
            .and_then(|state| {
                if self.poisoned.load(std::sync::atomic::Ordering::Acquire) || state.poisoned {
                    Err(Error::Poisoned)
                } else {
                    Ok(state.handler_failed
                        && !state.failed_handlers.contains(&completed.task_id()))
                }
            });
        let after_other_failure = match health {
            Ok(value) => value,
            Err(error) => {
                return Err(SettlementError::Rejected {
                    error,
                    completed: Box::new(completed),
                });
            }
        };
        let owner = self.owner();
        let gate = match owner.task_gate().lock() {
            Ok(gate) => gate,
            Err(_) => {
                return Err(SettlementError::Rejected {
                    error: Error::Poisoned,
                    completed: Box::new(completed),
                });
            }
        };
        if (matches!(&completed.result, InvocationResult::Abort(_))
            && self.has_global_prior(completed.task_id()))
            || self.has_prior_outcome(&owner, &completed.running.task)
        {
            return Err(SettlementError::Pending {
                completed: Box::new(completed),
            });
        }
        if after_other_failure {
            let independent = self.separate_from_failures(&owner, &completed.running.task);
            if !independent {
                return Err(SettlementError::Rejected {
                    error: Error::Busy,
                    completed: Box::new(completed),
                });
            }
        }
        if matches!(&completed.result, InvocationResult::Abort(_)) {
            drop(gate);
            drop(guard);
            return self.settle_invocation_inner(completed, true);
        }
        let pending = (|| {
            let current = self
                .ts()
                .load(completed.task_id())?
                .ok_or(Error::Ownership)?;
            if current.version != completed.running.version && current.task.abort_requested() {
                return Ok(None);
            }
            if current.task != completed.running.task {
                return Err(task_store::Error::Store(durable_store::Error::Conflict {
                    key: task_store::key(completed.task_id()),
                })
                .into());
            }
            pending_abort_root_for_running(
                &self.ts(),
                completed.task_id(),
                self.validator.is_none(),
            )
        })();
        drop(gate);
        drop(guard);
        let pending = match pending {
            Ok(pending) => pending,
            Err(Error::Busy) => {
                return Err(SettlementError::Pending {
                    completed: Box::new(completed),
                });
            }
            Err(error) => {
                return Err(SettlementError::Rejected {
                    error,
                    completed: Box::new(completed),
                });
            }
        };
        if pending.is_some()
            && let Some(validator) = self.validator.as_ref()
        {
            let mut worker = validator.worker();
            let state = self.live_invocations.clone();
            let scheduler_poison = self.poisoned.clone();
            let running = completed.task_id();
            let result = owner.conversation_controls().try_fence(|| {
                let _failure = match fence.try_lock() {
                    Ok(guard) => guard,
                    Err(std::sync::TryLockError::WouldBlock) => return Err(Error::Busy),
                    Err(std::sync::TryLockError::Poisoned(_)) => return Err(Error::Poisoned),
                };
                {
                    let state = state.lock().map_err(|_| Error::Poisoned)?;
                    if state.poisoned || scheduler_poison.load(std::sync::atomic::Ordering::Acquire)
                    {
                        return Err(Error::Poisoned);
                    }
                    if state.handler_failed && !state.failed_handlers.contains(&running) {
                        return Err(Error::Busy);
                    }
                }
                match worker.abort_pending_task_graph(running) {
                    Ok(affected) => Ok(affected.unwrap_or_default()),
                    Err(crate::runtime::Error::TaskScheduler(Error::Busy)) => Err(Error::Busy),
                    Err(error) => {
                        state.lock().map_err(|_| Error::Poisoned)?.poisoned = true;
                        Err(Error::Handler(format!("checked pending abort: {error}")))
                    }
                }
            });
            match result {
                Ok(Some(_)) => (),
                Ok(None) | Err(durable_store::controls::FenceError::Commit(Error::Busy)) => {
                    return Err(SettlementError::Pending {
                        completed: Box::new(completed),
                    });
                }
                Err(durable_store::controls::FenceError::Poisoned) => {
                    return Err(SettlementError::Rejected {
                        error: Error::Poisoned,
                        completed: Box::new(completed),
                    });
                }
                Err(durable_store::controls::FenceError::Commit(error)) => {
                    return Err(SettlementError::Rejected {
                        error,
                        completed: Box::new(completed),
                    });
                }
            }
        }
        self.settle_invocation_inner(completed, true)
    }

    /// Resume an owned pending state after cleanup. The checked transaction is
    /// revalidated under the task gate before its first resumed commit.
    pub fn try_resume_pending(
        &mut self,
        pending: PendingSettlement,
    ) -> Result<bool, SettlementError> {
        let (mut lease, mut settlement) = match pending {
            PendingSettlement::Unprepared(completed) => {
                return self.try_settle_invocation(completed);
            }
            PendingSettlement::Prepared { lease, settlement } => (lease, settlement),
            other @ (PendingSettlement::PreparationConflict { .. }
            | PendingSettlement::Failure { .. }) => {
                return self.try_resume_failure_or_conflict(other);
            }
        };
        let fence = self.failure_fence.clone();
        let guard = match fence.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(SettlementError::PendingPrepared {
                    pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(SettlementError::RejectedPrepared {
                    error: Error::Poisoned,
                    pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                });
            }
        };
        let health = if !Arc::ptr_eq(&self.live_invocations, &lease.state)
            || !Arc::ptr_eq(&lease.attachment, &settlement.attachment)
            || lease.id != settlement.running.task.id()
        {
            Err(Error::Ownership)
        } else {
            self.live_invocations
                .lock()
                .map_err(|_| Error::Poisoned)
                .and_then(|state| {
                    if self.poisoned.load(std::sync::atomic::Ordering::Acquire) || state.poisoned {
                        Err(Error::Poisoned)
                    } else {
                        Ok(state.handler_failed && !state.failed_handlers.contains(&lease.id))
                    }
                })
        };
        let after_other_failure = match health {
            Ok(value) => value,
            Err(error) => {
                return Err(SettlementError::RejectedPrepared {
                    error,
                    pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                });
            }
        };
        let owner = self.owner();
        let gate = match owner.task_gate().lock() {
            Ok(gate) => gate,
            Err(_) => {
                return Err(SettlementError::RejectedPrepared {
                    error: Error::Poisoned,
                    pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                });
            }
        };
        if self.has_prior_outcome(&owner, &settlement.running.task) {
            return Err(SettlementError::PendingPrepared {
                pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
            });
        }
        if after_other_failure && !self.separate_from_failures(&owner, &settlement.running.task) {
            return Err(SettlementError::RejectedPrepared {
                error: Error::Busy,
                pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
            });
        }
        let mut deferred = None;
        let mut precommit_rejection = false;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let revalidation = match settlement.revalidate(self, &owner, &gate) {
                Ok(value) => value,
                Err(error) => {
                    precommit_rejection = true;
                    return Err(error);
                }
            };
            match revalidation {
                PreparedRevalidation::Ready => self.commit_prepared_settlement(
                    &mut settlement,
                    &owner,
                    gate,
                    true,
                    &mut deferred,
                ),
                PreparedRevalidation::PendingAbort(root) => Err(Error::PendingAbort(root)),
                PreparedRevalidation::BlockedOrder => Err(Error::Busy),
                PreparedRevalidation::Aborted(latest) => {
                    self.yield_aborted_running(&latest)?;
                    self.poisoned
                        .store(false, std::sync::atomic::Ordering::Release);
                    Ok(true)
                }
                PreparedRevalidation::Stale => {
                    precommit_rejection = true;
                    Err(task_store::Error::Store(durable_store::Error::Conflict {
                        key: task_store::key(settlement.running.task.id()),
                    })
                    .into())
                }
            }
        }));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                self.live_invocations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .poisoned = true;
                drop(guard);
                crate::extension::discard_panic_payload(payload);
                return Err(SettlementError::IndeterminateOwned {
                    error: Error::Handler("settlement panicked".into()),
                    pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
                });
            }
        };
        if matches!(result, Err(Error::PendingAbort(_) | Error::Busy)) {
            drop(guard);
            return Err(SettlementError::PendingPrepared {
                pending: Box::new(match deferred {
                    Some(DeferredSettlement::Failure(settlement)) => {
                        PendingSettlement::Failure { lease, settlement }
                    }
                    _ => PendingSettlement::Prepared { lease, settlement },
                }),
            });
        }
        if precommit_rejection {
            let error = result.expect_err("revalidation rejection must contain an error");
            drop(guard);
            return Err(SettlementError::RejectedPrepared {
                error,
                pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
            });
        }
        if let Some(DeferredSettlement::IndeterminateFailure(failure)) = deferred {
            self.live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .poisoned = true;
            let error = result.expect_err("indeterminate failure must have an error");
            drop(guard);
            return Err(SettlementError::IndeterminateOwned {
                error,
                pending: Box::new(PendingSettlement::Failure {
                    lease,
                    settlement: failure,
                }),
            });
        }
        if result.is_err() && settlement.commit_uncertain {
            self.live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .poisoned = true;
            let error = result.expect_err("indeterminate commit must have an error");
            drop(guard);
            return Err(SettlementError::IndeterminatePrepared {
                error,
                pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
            });
        }
        if result.is_err() {
            self.live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .poisoned = true;
            let error = result.expect_err("indeterminate settlement must have an error");
            drop(guard);
            return Err(SettlementError::IndeterminateOwned {
                error,
                pending: Box::new(PendingSettlement::Prepared { lease, settlement }),
            });
        } else {
            let mut state = self
                .live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.failed_handlers.remove(&lease.id) {
                state.handler_failed = !state.failed_handlers.is_empty();
            }
        }
        lease.retired = true;
        drop(guard);
        drop(lease);
        result.map_err(SettlementError::Indeterminate)
    }

    fn try_resume_failure_or_conflict(
        &mut self,
        pending: PendingSettlement,
    ) -> Result<bool, SettlementError> {
        let (mut lease, mut failure, mut conflict) = match pending {
            PendingSettlement::Failure { lease, settlement } => (lease, Some(settlement), None),
            PendingSettlement::PreparationConflict { lease, conflict } => {
                (lease, None, Some(conflict))
            }
            _ => unreachable!(),
        };
        macro_rules! pending {
            () => {
                if let Some(settlement) = failure.take() {
                    PendingSettlement::Failure { lease, settlement }
                } else {
                    PendingSettlement::PreparationConflict {
                        lease,
                        conflict: conflict.unwrap(),
                    }
                }
            };
        }
        let fence = self.failure_fence.clone();
        let guard = match fence.try_lock() {
            Ok(g) => g,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(SettlementError::PendingPrepared {
                    pending: Box::new(pending!()),
                });
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(SettlementError::RejectedPrepared {
                    error: Error::Poisoned,
                    pending: Box::new(pending!()),
                });
            }
        };
        let (attachment, running) = if let Some(ref f) = failure {
            (&f.attachment, &f.running)
        } else {
            let c = conflict.as_ref().unwrap();
            (&c.attachment, &c.running)
        };
        let health = if !Arc::ptr_eq(&self.live_invocations, &lease.state)
            || !Arc::ptr_eq(&lease.attachment, attachment)
            || lease.id != running.task.id()
        {
            Err(Error::Ownership)
        } else {
            self.live_invocations
                .lock()
                .map_err(|_| Error::Poisoned)
                .and_then(|state| {
                    if self.poisoned.load(std::sync::atomic::Ordering::Acquire) || state.poisoned {
                        Err(Error::Poisoned)
                    } else {
                        Ok(state.handler_failed && !state.failed_handlers.contains(&lease.id))
                    }
                })
        };
        let after_other_failure = match health {
            Ok(value) => value,
            Err(error) => {
                drop(guard);
                return Err(SettlementError::RejectedPrepared {
                    error,
                    pending: Box::new(pending!()),
                });
            }
        };
        let owner = self.owner();
        let gate = match owner.task_gate().lock() {
            Ok(g) => g,
            Err(_) => {
                drop(guard);
                return Err(SettlementError::RejectedPrepared {
                    error: Error::Poisoned,
                    pending: Box::new(pending!()),
                });
            }
        };
        if self.has_prior_outcome(&owner, &running.task) {
            drop(gate);
            drop(guard);
            return Err(SettlementError::PendingPrepared {
                pending: Box::new(pending!()),
            });
        }
        if after_other_failure && !self.separate_from_failures(&owner, &running.task) {
            drop(gate);
            drop(guard);
            return Err(SettlementError::RejectedPrepared {
                error: Error::Busy,
                pending: Box::new(pending!()),
            });
        }
        let mut rejected = false;
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<bool, Error> {
                let running = if let Some(ref f) = failure {
                    &f.running
                } else {
                    &conflict.as_ref().unwrap().running
                };
                let current = self.ts().load(lease.id)?.ok_or(Error::Ownership)?;
                if current.version != running.version && current.task.abort_requested() {
                    self.ts().update(&current, |task| task.yield_ready())?;
                    return Ok(true);
                }
                if current.task != running.task {
                    rejected = true;
                    return Err(task_store::Error::Store(durable_store::Error::Conflict {
                        key: task_store::key(lease.id),
                    })
                    .into());
                }
                if let Some(ref mut settlement) = failure {
                    settlement.running = current.clone();
                } else if let Some(ref mut conflict) = conflict {
                    conflict.running = current.clone();
                }
                if failure.is_none() {
                    if let Some(root) = pending_abort_root_for_running(
                        &self.ts(),
                        lease.id,
                        self.validator.is_none(),
                    )? {
                        return Err(Error::PendingAbort(root));
                    }
                    if current.task.replay_safe() && self.registered(&current.task)?.replay_safe {
                        self.ts().update(&current, |task| task.yield_ready())?;
                        return Ok(true);
                    }
                    failure = Some(FailureSettlement {
                        attachment: self.attachment.clone(),
                        running: current,
                        reason: "document conflict: execution outcome requires reconciliation"
                            .into(),
                        completed: false,
                        check_pending: true,
                        attempt: 0,
                        candidate: None,
                    });
                }
                self.commit_failure_settlement(failure.as_mut().unwrap(), || {})?;
                Ok(true)
            }));
        drop(gate);
        let result = match result {
            Ok(r) => r,
            Err(payload) => {
                self.live_invocations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .poisoned = true;
                drop(guard);
                crate::extension::discard_panic_payload(payload);
                return Err(SettlementError::IndeterminateOwned {
                    error: Error::Handler("settlement panicked".into()),
                    pending: Box::new(pending!()),
                });
            }
        };
        if matches!(result, Err(Error::PendingAbort(_) | Error::Busy)) {
            drop(guard);
            return Err(SettlementError::PendingPrepared {
                pending: Box::new(pending!()),
            });
        }
        if rejected {
            let error = result.unwrap_err();
            drop(guard);
            return Err(SettlementError::RejectedPrepared {
                error,
                pending: Box::new(pending!()),
            });
        }
        if result.is_err() {
            self.live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .poisoned = true;
            let error = result.expect_err("indeterminate settlement must have an error");
            drop(guard);
            return Err(SettlementError::IndeterminateOwned {
                error,
                pending: Box::new(pending!()),
            });
        } else {
            let mut state = self
                .live_invocations
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.failed_handlers.remove(&lease.id) {
                state.handler_failed = !state.failed_handlers.is_empty();
            }
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
        }
        lease.retired = true;
        drop(guard);
        drop(lease);
        result.map_err(SettlementError::Indeterminate)
    }

    // Resolve the top-level foreground task through Task -> conversation ->
    // Task ownership, stopping at background boundaries. Legacy cycles use
    // a stable representative; missing or inconsistent anchors fail closed.
    fn foreground_root(owner: &OwnedStore, task: &Task) -> Option<Uuid> {
        let store = TaskStore::new(owner.store());
        let edges = crate::ownership_edge::EdgeStore::new(owner.store());
        let mut current = task.clone();
        let mut path = Vec::new();
        loop {
            if let Some(start) = path.iter().position(|id| *id == current.id()) {
                return path[start..].iter().copied().min();
            }
            path.push(current.id());
            if current.is_background() {
                return Some(current.id());
            }
            match current.owner() {
                TaskOwner::Task(id) => current = store.load(*id).ok()??.task,
                TaskOwner::Conversation(id) => match edges.get(*id).ok()? {
                    None => return Some(current.id()),
                    Some(edge) => {
                        let key = format!("runtime/conversation/{id}");
                        let entry = owner.store().get(key.as_bytes()).ok()??;
                        let conversation: crate::runtime::Conversation =
                            serde_json::from_slice(&entry.value).ok()?;
                        if conversation.id != *id {
                            return None;
                        }
                        let parent = store.load(edge.edge.task).ok()??.task;
                        if edge.edge.task_owner != *parent.owner() {
                            return None;
                        }
                        if edge.edge.mode == crate::ownership_edge::Mode::Background {
                            return Some(current.id());
                        }
                        current = parent;
                    }
                },
            }
        }
    }
    fn foreground_ancestors_available(
        &self,
        owner: &OwnedStore,
        task: &Task,
    ) -> Result<bool, Error> {
        let store = TaskStore::new(owner.store());
        let edges = crate::ownership_edge::EdgeStore::new(owner.store());
        let mut current = task.clone();
        let mut first = true;
        let mut through_conversation = false;
        let mut seen_tasks = HashSet::new();
        let mut seen_conversations = HashSet::new();
        loop {
            if !seen_tasks.insert(current.id()) {
                return Ok(false);
            }
            let successful_anchor = through_conversation
                && matches!(current.state(), TaskState::Succeeded(_))
                && !current.abort_requested();
            if !first
                && !successful_anchor
                && (current.abort_requested()
                    || current.is_terminal()
                    || matches!(current.state(), TaskState::CompletingFailed(_))
                    || (current.state() == &TaskState::Running
                        && !live_task_tool_parent(&current, &self.live_invocations))
                    || self.blocked_reason(&current).is_some())
            {
                return Ok(false);
            }
            first = false;
            let background = current.is_background();
            match current.owner() {
                TaskOwner::Task(id) => {
                    if background {
                        return Ok(false);
                    }
                    let Some(parent) = store.load(*id)? else {
                        return Ok(false);
                    };
                    current = parent.task;
                }
                TaskOwner::Conversation(id) => {
                    if background {
                        return Ok(true);
                    }
                    if !seen_conversations.insert(*id) {
                        return Ok(false);
                    }
                    let edge = edges
                        .get(*id)
                        .map_err(|e| Error::Handler(format!("ownership edge: {e}")))?;
                    let key = format!("runtime/conversation/{id}");
                    match owner
                        .store()
                        .get(key.as_bytes())
                        .map_err(task_store::Error::from)?
                    {
                        Some(entry) => {
                            let conversation: crate::runtime::Conversation =
                                serde_json::from_slice(&entry.value)
                                    .map_err(task_store::Error::from)?;
                            if conversation.id != *id {
                                return Err(Error::Ownership);
                            }
                            if conversation.abort_requested {
                                return Ok(false);
                            }
                        }
                        None if edge.is_some() => return Err(Error::Ownership),
                        None => (),
                    }
                    match edge {
                        None => return Ok(true),
                        Some(edge) if edge.edge.mode == crate::ownership_edge::Mode::Background => {
                            return Ok(true);
                        }
                        Some(edge) => {
                            let Some(parent) = store.load(edge.edge.task)? else {
                                return Err(Error::Ownership);
                            };
                            if edge.edge.task_owner != *parent.task.owner() {
                                return Err(Error::Ownership);
                            }
                            current = parent.task;
                            through_conversation = true;
                        }
                    }
                }
            }
        }
    }

    fn separate_from_failures(&self, owner: &OwnedStore, task: &Task) -> bool {
        let failures = self
            .live_invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .failed_handlers
            .clone();
        if failures.iter().all(|id| *id == task.id()) {
            return true;
        }
        let Some(root) = Self::foreground_root(owner, task) else {
            return false;
        };
        failures.iter().filter(|id| **id != task.id()).all(|id| {
            self.ts()
                .load(*id)
                .ok()
                .flatten()
                .and_then(|record| Self::foreground_root(owner, &record.task))
                .is_some_and(|other| other != root)
        })
    }
    pub(crate) fn has_published_outcomes(&self) -> Result<bool, Error> {
        Ok(!self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)?
            .published
            .is_empty())
    }
    fn has_global_prior(&self, id: Uuid) -> bool {
        let state = self
            .live_invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state
            .published
            .iter()
            .find(|(_, priority)| *priority)
            .or_else(|| state.published.front())
            .is_some_and(|(first, _)| *first != id)
    }
    fn has_prior_outcome(&self, owner: &OwnedStore, task: &Task) -> bool {
        let published = self
            .live_invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .published
            .clone();
        if published.iter().all(|(id, _)| *id == task.id()) {
            return false;
        }
        let Some(root) = Self::foreground_root(owner, task) else {
            return true;
        };
        let mut related = Vec::new();
        for (id, priority) in published {
            let other = if id == task.id() {
                Some(root)
            } else {
                self.ts()
                    .load(id)
                    .ok()
                    .flatten()
                    .and_then(|record| Self::foreground_root(owner, &record.task))
            };
            match other {
                Some(other) if other == root => related.push((id, priority)),
                Some(_) => (),
                None => return true,
            }
        }
        related
            .iter()
            .find(|(_, priority)| *priority)
            .or_else(|| related.first())
            .is_some_and(|(id, _)| *id != task.id())
    }
    fn has_related_published(&self, owner: &OwnedStore, task: &Task) -> bool {
        let published = self
            .live_invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .published
            .clone();
        if published.is_empty() {
            return false;
        }
        let Some(root) = Self::foreground_root(owner, task) else {
            return true;
        };
        published.iter().any(|(id, _)| {
            self.ts()
                .load(*id)
                .ok()
                .flatten()
                .and_then(|record| Self::foreground_root(owner, &record.task))
                .is_none_or(|other| other == root)
        })
    }

    /// Stop admitting new reservations while allowing existing results to drain.
    pub fn stop_reservations(&mut self) {
        self.reservations_stopped = true;
        self.reservation_stop
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn has_live_invocations(&self) -> Result<bool, Error> {
        Ok(!self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)?
            .attempts
            .is_empty())
    }

    #[cfg(test)]
    pub(crate) fn set_result_commit_hook(&mut self, hook: CommitHook) {
        self.result_commit_hook = Some(hook);
    }

    #[cfg(all(test, unix))]
    pub(crate) fn set_failure_boundary_hook(&mut self, after_commit: bool, hook: CommitHook) {
        if after_commit {
            self.failure_committed_hook = Some(hook);
        } else {
            self.failure_commit_hook = Some(hook);
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_failure_commit_error_for_test(&mut self, boundary: u8) {
        self.inject_failure_commit_error = boundary;
    }

    #[cfg(test)]
    pub(crate) fn set_reservation_commit_hook(&mut self, hook: CommitHook) {
        self.reservation_commit_hook = Some(hook);
    }

    pub(crate) fn reservation_stop_token(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.reservation_stop.clone()
    }

    // A busy identity is transient: leave the task Ready and let the driver
    // revisit it on a later scheduling pass without consuming a worker.
    fn sealed_identity_not_busy(&self, task: &Task) -> bool {
        let Some(capture) = self
            .registered(task)
            .ok()
            .and_then(|entry| entry.receipt_capture.as_ref())
        else {
            return true;
        };
        !matches!(
            crate::nested_identity::checked_lease(capture.executor.tool(), &capture.implementation),
            Err(crate::nested_identity::LeaseFailure::Busy)
        )
    }

    /// Reserve a foreground-independent task without running graph maintenance.
    /// Every ownership ancestor is checked under the task gate.
    pub fn reserve_independent_root(&mut self) -> Result<Option<Box<DetachedInvocation>>, Error> {
        let fence = self.failure_fence.clone();
        let _failure = fence.lock().map_err(|_| Error::Poisoned)?;
        self.check()?;
        if self.reservations_stopped
            || self
                .reservation_stop
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Stopped);
        }
        let owner = self.owner();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        let graph_version = owner
            .store()
            .put_version()
            .map_err(task_store::Error::from)?;
        let tasks = self.ts().list()?;
        for snapshot in &tasks {
            if snapshot.task.state() != &TaskState::Ready
                || snapshot.task.abort_requested()
                || self.blocked_reason(&snapshot.task).is_some()
                || !self.sealed_identity_not_busy(&snapshot.task)
                || !self.foreground_ancestors_available(&owner, &snapshot.task)?
            {
                continue;
            }
            if self.has_related_published(&owner, &snapshot.task) {
                continue;
            }
            if pending_abort_root_for_running(
                &self.ts(),
                snapshot.task.id(),
                self.validator.is_none(),
            )?
            .is_some()
            {
                continue;
            }
            let handler = self.registered(&snapshot.task)?.handler.clone();
            if !self.reserve_resolved(snapshot, Some(graph_version))? {
                return Ok(None);
            }
            let running = self
                .ts()
                .load(snapshot.task.id())?
                .ok_or(Error::Ownership)?;
            let invocation = self.make_invocation(running, handler, false)?;
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(Some(Box::new(invocation)));
        }
        Ok(None)
    }

    /// Observe live handler invocations without acquiring the scheduler lock.
    pub fn worker_activity(&self) -> crate::worker_activity::WorkerActivity {
        self.activity.clone()
    }
    pub(crate) fn wake_reader(&self) -> WakeReader {
        self.wake_reader.clone()
    }
    fn publish_wake_definitions(&self) -> Result<(), Error> {
        let mut view = self
            .wake_reader
            .definitions
            .write()
            .map_err(|_| Error::Poisoned)?;
        view.registry = self.registry.clone();
        view.failed_migrations.clone_from(&self.failed_migrations);
        Ok(())
    }
    pub(crate) fn owner(&self) -> Arc<OwnedStore> {
        self.attachment.owner.clone()
    }
    pub(crate) fn definitions(&self) -> HashMap<String, (bool, Arc<dyn Handler>)> {
        self.registry
            .entries
            .iter()
            .map(|(name, r)| (name.clone(), (r.replay_safe, r.handler.clone())))
            .collect()
    }
    pub(crate) fn policies(&self) -> HashMap<String, (bool, u32)> {
        self.registry
            .entries
            .iter()
            .map(|(name, registration)| {
                (
                    name.clone(),
                    (registration.replay_safe, registration.version),
                )
            })
            .collect()
    }
    /// Opens exclusively and reconciles interrupted invocations. Parked tasks are
    /// untouched. Missing definitions remain blocked until registered.
    pub fn open(path: impl AsRef<Path>, registry: Registry) -> Result<Self, Error> {
        Self::attach(Arc::new(OwnedStore::open(path)?), registry)
    }

    /// Open a standalone task scheduler with conversation abort cleanup enabled.
    /// The harness and scheduler share the same exclusive store owner.
    pub fn open_with_conversation_runtime(
        path: impl AsRef<Path>,
        registry: Registry,
        options: crate::runtime::Options,
    ) -> Result<Self, Error> {
        let harness = crate::runtime::Harness::open_paused(path, options)
            .map_err(|e| Error::Handler(format!("conversation runtime: {e}")))?;
        harness
            .attach_tasks(registry)
            .map_err(|e| Error::Handler(format!("conversation runtime: {e}")))
    }

    #[cfg(test)]
    pub(crate) fn set_sealed_test_hooks(
        &mut self,
        kind: &str,
        before: Option<Arc<dyn Fn() + Send + Sync>>,
        after: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> bool {
        let Some(capture) = self
            .registry
            .entries
            .get_mut(kind)
            .and_then(|entry| entry.receipt_capture.as_mut())
        else {
            return false;
        };
        let capture = Arc::make_mut(capture);
        capture.before_intent = before;
        capture.after_intent = after;
        self.publish_wake_definitions().is_ok()
    }

    #[cfg(test)]
    pub(crate) fn set_sealed_busy_test_hook(
        &mut self,
        kind: &str,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) -> bool {
        let Some(capture) = self
            .registry
            .entries
            .get_mut(kind)
            .and_then(|entry| entry.receipt_capture.as_mut())
        else {
            return false;
        };
        Arc::make_mut(capture).busy_hook = Some(hook);
        self.publish_wake_definitions().is_ok()
    }

    /// Install only a sealed Tool executor on a standalone scheduler. No
    /// arbitrary Handler can be supplied on this path. Child admission remains
    /// unavailable until its identity can be staged with the parent transition.
    pub(crate) fn install_sealed_tool(
        &mut self,
        kind: String,
        tool: Arc<dyn pi_rs_core::Tool>,
        implementation: &str,
        version: u32,
    ) -> Result<
        (
            Arc<crate::async_task_tool::SealedExecutor>,
            crate::async_task_tool::ToolIdentity,
        ),
        String,
    > {
        self.install_sealed_tool_with_hook(kind, tool, implementation, version, None, false)
    }

    pub(crate) fn install_sealed_tool_with_hook(
        &mut self,
        kind: String,
        tool: Arc<dyn pi_rs_core::Tool>,
        implementation: &str,
        version: u32,
        hook: Option<(String, Arc<dyn crate::extension::Hook>)>,
        after: bool,
    ) -> Result<
        (
            Arc<crate::async_task_tool::SealedExecutor>,
            crate::async_task_tool::ToolIdentity,
        ),
        String,
    > {
        if self.validator.is_some() || kind.is_empty() {
            return Err("sealed tool requires a standalone scheduler and task kind".into());
        }
        let _identity_lease = crate::nested_identity::checked_lease(&tool, implementation)
            .map_err(|_| "tool identity is missing, busy, or changed")?;
        let mut identity =
            crate::async_task_tool::ToolIdentity::new(tool.as_ref(), implementation, version)?;
        if let Some((id, _)) = &hook {
            identity = identity.with_before_hook(id.clone())?;
        }
        if after {
            if hook.is_none() {
                return Err("after hook requires a hook registration".into());
            }
            identity = identity.with_after_hook();
        }
        if serde_json::to_vec(&identity)
            .map_err(|e| e.to_string())?
            .len()
            > 65_536
        {
            return Err("tool identity exceeds size limit".into());
        }
        let executor = Arc::new(crate::async_task_tool::SealedExecutor::new(
            tool,
            identity.clone(),
            implementation.into(),
            version,
            hook.map(|(_, hook)| hook),
        ));
        let handler: Arc<dyn Handler> = executor.clone();
        let mut registry = self.registry.clone();
        if !registry.register_shared_versioned(kind.clone(), version, false, handler) {
            return Err("task kind is already registered or invalid".into());
        }
        registry
            .entries
            .get_mut(&kind)
            .expect("inserted")
            .receipt_capture = Some(Arc::new(ReceiptCapture {
            identity: identity.clone(),
            executor: executor.clone(),
            implementation: implementation.into(),
            version,
            #[cfg(test)]
            before_intent: None,
            #[cfg(test)]
            busy_hook: None,
            #[cfg(test)]
            after_intent: None,
        }));
        self.replace_registry(registry).map_err(|e| e.to_string())?;
        Ok((executor, identity))
    }

    pub(crate) fn conversation_harness(&self) -> Option<crate::runtime::Harness> {
        self.validator.as_ref().map(|harness| harness.worker())
    }

    /// Attach to an existing owner. Only one task scheduler may be attached to
    /// that owner at a time, including during recovery and handler execution.
    /// The scheduler retains the owner until it is dropped. Callers must fence
    /// all other task executors and direct TaskStore mutations themselves.
    /// Failed attachment or recovery releases the in-process guard.
    pub fn attach(owner: Arc<OwnedStore>, registry: Registry) -> Result<Self, Error> {
        if !crate::owned_subagent::check_inheritance_provenance(owner.store())
            .map_err(task_store::Error::from)?
        {
            return Err(Error::UntrustedToolInheritance);
        }
        let live_invocations = Arc::new(Mutex::new(InvocationState::default()));
        let wake_reader = WakeReader {
            owner: owner.clone(),
            live_invocations: live_invocations.clone(),
            definitions: Arc::new(std::sync::RwLock::new(WakeDefinitions {
                registry: registry.clone(),
                failed_migrations: HashMap::new(),
            })),
        };
        let mut scheduler = Self {
            attachment: Arc::new(Attachment::acquire(owner)?),
            live_invocations,
            failure_fence: Arc::new(Mutex::new(())),
            wake_reader,
            registry,
            validator: None,
            cleanup_jobs: tokio::task::JoinSet::new(),
            cleanup_running: HashSet::new(),
            cleanup_cursor: None,
            cleanup_timeout: std::time::Duration::from_secs(30),
            cleanup_failures: HashMap::new(),
            poisoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reservations_stopped: false,
            reservation_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            failed_migrations: HashMap::new(),
            deferred_recovery: HashSet::new(),
            #[cfg(test)]
            result_commit_hook: None,
            #[cfg(test)]
            after_order_hook: None,
            #[cfg(test)]
            reservation_commit_hook: None,
            #[cfg(test)]
            after_result_load_hook: None,
            #[cfg(test)]
            preparation_conflict_hook: None,
            #[cfg(test)]
            failure_commit_hook: None,
            #[cfg(test)]
            failure_committed_hook: None,
            #[cfg(test)]
            inject_batch_prepare_conflict: false,
            #[cfg(test)]
            inject_post_commit_error: false,
            #[cfg(test)]
            inject_preparation_error: 0,
            #[cfg(test)]
            inject_failure_commit_error: 0,
            #[cfg(test)]
            inject_abort_commit_error: 0,
            #[cfg(test)]
            inject_aborted_yield_error: 0,
            #[cfg(test)]
            yield_before_preparation: false,
            #[cfg(test)]
            yield_after_graph_conflict: false,
            #[cfg(test)]
            yielded_prepared: None,
            activity: Default::default(),
        };
        scheduler.reconcile()?;
        Ok(scheduler)
    }
    fn ts(&self) -> TaskStore<'_> {
        TaskStore::new(self.attachment.owner.store())
    }
    #[cfg(test)]
    pub(crate) fn inject_unknown_post_commit(&mut self) {
        self.inject_post_commit_error = true;
    }
    pub(crate) fn failure_publication(&self) -> FailurePublication {
        FailurePublication {
            fence: self.failure_fence.clone(),
            state: self.live_invocations.clone(),
        }
    }
    pub(crate) fn check(&self) -> Result<(), Error> {
        let state = self.live_invocations.lock().map_err(|_| Error::Poisoned)?;
        if self.poisoned.load(std::sync::atomic::Ordering::Acquire) || state.poisoned {
            Err(Error::Poisoned)
        } else if state.handler_failed {
            Err(Error::Stopped)
        } else {
            Ok(())
        }
    }
    /// The scheduler mutex must remain held from this check through the
    /// quiescent operation, so no new invocation can enter the live ledger.
    pub(crate) fn check_quiescent(&self) -> Result<bool, Error> {
        let state = self.live_invocations.lock().map_err(|_| Error::Poisoned)?;
        if self.poisoned.load(std::sync::atomic::Ordering::Acquire) || state.poisoned {
            Err(Error::Poisoned)
        } else if state.handler_failed {
            Err(Error::Stopped)
        } else {
            Ok(state.attempts.is_empty())
        }
    }

    fn yield_aborted_running(&self, latest: &task_store::StoredTask) -> Result<(), Error> {
        #[cfg(test)]
        if self.inject_aborted_yield_error == 1 {
            return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
        }
        self.ts().update(latest, |task| task.yield_ready())?;
        #[cfg(test)]
        if self.inject_aborted_yield_error == 2 {
            return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
        }
        Ok(())
    }

    fn abort_failed_running(
        &mut self,
        owner: &Arc<OwnedStore>,
        root: Uuid,
        running: Uuid,
        gate: std::sync::MutexGuard<'_, ()>,
    ) -> Result<(), Error> {
        if let Some(harness) = self.validator.as_mut() {
            drop(gate);
            owner
                .conversation_controls()
                .fence(|| harness.abort_owned_graph(None, Some(root)))
                .map_err(|e| Error::Handler(format!("fail-fast abort: {e:?}")))?;
            let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
            let current = self.ts().load(running)?.ok_or(Error::Ownership)?;
            if !current.task.abort_requested() {
                return Err(Error::Ownership);
            }
            self.ts().update(&current, |task| task.yield_ready())?;
        } else {
            request_abort_subtree(&self.ts(), root)?;
            let current = self.ts().load(running)?.ok_or(Error::Ownership)?;
            if !current.task.abort_requested() {
                return Err(Error::Ownership);
            }
            self.ts().update(&current, |task| task.yield_ready())?;
            drop(gate);
        }
        Ok(())
    }

    fn registered(&self, task: &Task) -> Result<&Registration, Error> {
        let entry = self
            .registry
            .entries
            .get(task.kind().ok_or(Error::Registration)?)
            .ok_or(Error::Registration)?;
        Ok(entry)
    }
    /// Replace registrations after a clean scheduling boundary. Failed migrations
    /// are retried only when their task definition changes. Returns `Busy`
    /// while any detached invocation or completed result retains its lease.
    pub fn replace_registry(&mut self, registry: Registry) -> Result<(), Error> {
        if !self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)?
            .attempts
            .is_empty()
        {
            return Err(Error::Busy);
        }
        let owner = self.owner();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        self.replace_registry_locked(registry)
    }
    pub(crate) fn bind_harness(&mut self, harness: crate::runtime::Harness) {
        self.activity = harness.worker_activity();
        self.validator = Some(harness);
    }
    pub(crate) fn replace_registry_locked(&mut self, registry: Registry) -> Result<(), Error> {
        self.check()?;
        if !self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)?
            .attempts
            .is_empty()
        {
            return Err(Error::Busy);
        }
        if let Some(harness) = &self.validator {
            harness
                .validate_task_tool_registry(&registry)
                .map_err(|_| Error::Registration)?;
        }
        self.registry = registry;
        self.publish_wake_definitions()?;
        self.activity.notify();
        self.reconcile()
    }
    /// Inspect compatibility without executing migration code.
    pub fn blocked_reason(&self, task: &Task) -> Option<BlockedReason> {
        if task.is_completing() {
            return None;
        }
        let Ok(reg) = self.registered(task) else {
            return Some(BlockedReason::MissingTask);
        };
        if task.definition_version() > reg.version {
            return Some(BlockedReason::TaskTooOld);
        }
        if task.definition_version() < reg.version {
            if let Some((identity, error)) = self.failed_migrations.get(&task.id())
                && Arc::ptr_eq(identity, &reg.identity)
            {
                return Some(BlockedReason::MigrationFailed(error.clone()));
            }
            if reg.migrate.is_none() {
                return Some(BlockedReason::MigrationFailed("missing migration".into()));
            }
        }
        None
    }
    /// Poll the current blocked state. Terminal tasks are excluded; this does
    /// not execute migrations or record a durable event history.
    pub fn blocked_tasks(&self) -> Result<Vec<BlockedTask>, Error> {
        self.check()?;
        Ok(self
            .ts()
            .list()?
            .into_iter()
            .filter_map(|snapshot| {
                let task = snapshot.task;
                if task.is_terminal() {
                    return None;
                }
                self.blocked_reason(&task).map(|reason| BlockedTask {
                    id: task.id(),
                    kind: task.kind().map(str::to_owned),
                    owner: task.owner().clone(),
                    reason,
                })
            })
            .collect())
    }
    /// Inspect one task-store snapshot without polling waits or invoking user code.
    pub fn wake_snapshot(&self, now_millis: i64) -> Result<WakeSnapshot, Error> {
        self.check()?;
        self.wake_reader.snapshot(now_millis)
    }

    // Compute migration on a detached record; persist it together with reservation.
    fn reserve_resolved(
        &mut self,
        snapshot: &task_store::StoredTask,
        graph_fence: Option<u64>,
    ) -> Result<bool, Error> {
        if self.blocked_reason(&snapshot.task).is_some() {
            return Ok(false);
        }
        let reg = self.registered(&snapshot.task)?;
        let mut next = snapshot.clone();
        if snapshot.task.definition_version() < reg.version {
            let migration = reg.migrate.as_ref().unwrap().clone();
            let version = reg.version;
            let identity = reg.identity.clone();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                migration(
                    snapshot.task.input(),
                    snapshot.task.checkpoint(),
                    snapshot.task.definition_version(),
                )
            }))
            .unwrap_or_else(|_| Err("migration panicked".into()));
            if self
                .reservation_stop
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(Error::Stopped);
            }
            match result {
                Ok((input, checkpoint)) => next.task.migrate(version, input, checkpoint),
                Err(error) => {
                    self.failed_migrations
                        .insert(snapshot.task.id(), (identity, error));
                    self.publish_wake_definitions()?;
                    // A failed migration changes readiness without a store
                    // commit or a handler invocation. Wake observers must
                    // still re-read the newly published definition view.
                    self.activity.notify();
                    return Ok(false);
                }
            }
        }
        let child_checks = self.ts().refresh_child_wake(&mut next.task)?;
        next.task
            .start_attempt(Uuid::new_v4())
            .map_err(Error::Transition)?;
        self.poisoned
            .store(true, std::sync::atomic::Ordering::Release);
        let mut batch = self.ts().prepare_save(&[Save::Update(&next)])?;
        batch.checks.extend(child_checks);
        #[cfg(test)]
        if let Some(hook) = &self.reservation_commit_hook {
            hook(0, self.ts().store());
        }
        // Reservation starts at this fenced stop check. A concurrent stop
        // after it joins this already-started reservation.
        if self
            .reservation_stop
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
            return Err(Error::Stopped);
        }
        let commit = if let Some(version) = graph_fence {
            self.ts().store().commit_with_prefix_put_fence(
                &batch.checks,
                &batch.mutations,
                &[
                    task_store::PREFIX,
                    crate::ownership_edge::PREFIX,
                    b"runtime/conversation/",
                ],
                version,
            )
        } else {
            self.ts().store().commit(&batch.checks, &batch.mutations)
        };
        match commit {
            Ok(()) => (),
            Err(durable_store::Error::Conflict { .. }) => {
                // No reservation or effects committed; rescan on a CAS miss.
                self.poisoned
                    .store(false, std::sync::atomic::Ordering::Release);
                return Ok(false);
            }
            Err(error) => return Err(task_store::Error::from(error).into()),
        }
        Ok(true)
    }
    fn reconcile(&mut self) -> Result<(), Error> {
        let live = self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)?
            .attempts
            .clone();
        'scan: loop {
            let records = self.ts().list()?;
            // The harness is bound only after attach. Defer interrupted work in
            // pending fail-fast ownership graphs, including conversation descendants,
            // until the normal graph-abort path has persisted the complete mark.
            let mut pending_roots = Vec::new();
            for parent in &records {
                if !parent.task.abort_requested()
                    && let TaskState::Waiting(WaitFor::TasksFailFast(ids)) = parent.task.state()
                    && failed_sibling_to_abort(&self.ts(), parent, ids)?.is_some()
                {
                    pending_roots.extend(ids.iter().copied());
                }
            }
            // A settled TaskTool aborts only its nested TaskTool roots, not
            // generic siblings. Defer those roots and their ownership graphs until
            // the normal nested-abort transaction has marked them.
            for parent in &records {
                if parent.task.is_task_tool()
                    && parent.task.is_completing()
                    && has_unmarked_nested(parent.task.id(), &records)
                {
                    pending_roots.extend(records.iter().filter_map(|child| {
                        (child.task.owner() == &TaskOwner::Task(parent.task.id())
                            && child.task.is_task_tool()
                            && !child.task.is_terminal()
                            && !child.task.abort_requested())
                        .then_some(child.task.id())
                    }));
                }
            }
            // A held failure is itself cancellation intent; its descendants must
            // not be recovered before that graph is marked either.
            for record in &records {
                if matches!(record.task.state(), TaskState::CompletingFailed(_))
                    && !record.task.abort_requested()
                    && has_owned_work(self.owner().store(), record.task.id(), &records, true)?
                {
                    pending_roots.push(record.task.id());
                }
            }
            let pending = !pending_roots.is_empty();
            if pending {
                let graph = crate::ownership_edge::CleanupGraph::load(self.owner().store())
                    .map_err(|e| Error::Handler(format!("ownership graph: {e}")))?;
                // Valid edges enumerate known descendants. For damaged graphs,
                // also retain every interrupted task whose ancestry cannot
                // prove it lies outside the pending root.
                let uncertain: Vec<_> = pending_roots
                    .iter()
                    .copied()
                    .filter(|id| graph.uncertain_task(*id))
                    .collect();
                let mut task_ids = HashSet::new();
                let mut conversations = HashSet::new();
                while let Some(id) = pending_roots.pop() {
                    if !task_ids.insert(id) {
                        continue;
                    }
                    for record in &records {
                        if record.task.owner() == &TaskOwner::Task(id) {
                            pending_roots.push(record.task.id());
                        }
                    }
                    for edge in graph.task_edges(id) {
                        if edge.mode == crate::ownership_edge::Mode::Foreground
                            && conversations.insert(edge.child)
                        {
                            for record in &records {
                                if record.task.owner() == &TaskOwner::Conversation(edge.child) {
                                    pending_roots.push(record.task.id());
                                }
                            }
                        }
                    }
                }
                let owner = self.owner();
                self.deferred_recovery
                    .extend(records.iter().filter_map(|item| {
                        (item.task.state() == &TaskState::Running
                            && !live.contains(&item.task.id())
                            && (task_ids.contains(&item.task.id())
                                || uncertain.iter().any(|root| {
                                    !task_ancestors_available(
                                        owner.store(),
                                        &item.task,
                                        &records,
                                        |parent| parent.id() == *root,
                                    )
                                })))
                        .then_some(item.task.id())
                    }));
            }
            let deferred = if pending {
                HashSet::new()
            } else {
                std::mem::take(&mut self.deferred_recovery)
            };
            for listed in records {
                if live.contains(&listed.task.id()) {
                    continue;
                }
                if pending && self.deferred_recovery.contains(&listed.task.id()) {
                    continue;
                }
                if !deferred.is_empty() && !deferred.contains(&listed.task.id()) {
                    continue;
                }
                let Some(item) = self.ts().load(listed.task.id())? else {
                    continue;
                };
                if item.task.state() != &TaskState::Running {
                    continue;
                }
                if item.task.abort_requested() {
                    // Defer even restart-limit failure until owned cleanup settles.
                    self.ts().update(&item, |task| task.recover())?;
                    continue;
                }
                let after_recovery = if !item.task.replay_safe() {
                    match crate::async_task_protocol::read_raw_result(
                        self.ts().store(),
                        item.task.id(),
                    ) {
                        Ok(Some(_)) => Some(
                            crate::async_task_protocol::prepare_after_recovery(
                                self.ts().store(),
                                &item,
                            )
                            .map_err(|e| Error::Handler(format!("after recovery rejected: {e}")))?,
                        ),
                        Ok(None) => None,
                        Err(error) => {
                            return Err(Error::Handler(format!(
                                "raw result recovery failed: {error}"
                            )));
                        }
                    }
                } else {
                    None
                };
                if let Some(proof) = after_recovery {
                    if item.task.restarts().saturating_add(1) >= item.task.restart_limit() {
                        self.finish_failed(&item, "after hook restart limit".into())?;
                        self.deferred_recovery.extend(deferred);
                        continue 'scan;
                    }
                    let mut next = item.clone();
                    next.task.recover().map_err(Error::Transition)?;
                    let mut batch = self.ts().prepare_save(&[task_store::Save::Update(&next)])?;
                    batch.checks.extend(proof.checks);
                    self.ts()
                        .store()
                        .commit(&batch.checks, &batch.mutations)
                        .map_err(task_store::Error::from)?;
                    continue;
                }
                if !item.task.replay_safe() {
                    self.finish_failed(
                        &item,
                        "interrupted: execution outcome requires reconciliation".into(),
                    )?;
                    self.deferred_recovery.extend(deferred);
                    continue 'scan;
                }
                let Ok(reg) = self.registered(&item.task) else {
                    // Leave a task blocked until its definition is restored.
                    continue;
                };
                if item.task.replay_safe() && reg.replay_safe {
                    let exhausted =
                        item.task.restarts().saturating_add(1) >= item.task.restart_limit();
                    if exhausted {
                        self.finish_failed(&item, "restart limit".into())?;
                        self.deferred_recovery.extend(deferred);
                        continue 'scan;
                    } else {
                        self.ts().update(&item, |task| task.recover())?;
                    }
                } else {
                    self.finish_failed(
                        &item,
                        "interrupted: execution outcome requires reconciliation".into(),
                    )?;
                    self.deferred_recovery.extend(deferred);
                    continue 'scan;
                }
            }
            return Ok(());
        }
    }
    /// Create a root task. Child creation is performed atomically by `Step::Wait`.
    pub fn submit(&self, task: &Task) -> Result<(), Error> {
        self.check()?;
        let reg = self.registered(task)?;
        if reg.version != task.definition_version()
            || reg.replay_safe != task.replay_safe()
            || !matches!(task.owner(), TaskOwner::Conversation(_))
            || task.state() != &TaskState::Ready
        {
            return Err(Error::Ownership);
        }
        let owner = self.owner();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        if let TaskOwner::Conversation(id) = task.owner() {
            let key = format!("runtime/conversation/{id}");
            if let Some(entry) = owner
                .store()
                .get(key.as_bytes())
                .map_err(task_store::Error::from)?
            {
                let value: serde_json::Value =
                    serde_json::from_slice(&entry.value).map_err(task_store::Error::from)?;
                if value
                    .get("abort_requested")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    return Err(Error::Ownership);
                }
            }
        }
        self.ts().save(&[Save::Create(task)])?;
        Ok(())
    }
    pub fn signal(&self, signal: &Signal) -> Result<(), Error> {
        self.check()?;
        let owner = self.owner();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        self.ts().send_signal(signal)?;
        Ok(())
    }
    /// Deliver a typed task signal using task/key deduplication.
    pub fn deliver_signal(
        &self,
        task_id: Uuid,
        token: &task_store::SignalToken,
        key: &str,
        value: Value,
    ) -> Result<task_store::SignalResult, Error> {
        self.check()?;
        let owner = self.owner();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        Ok(self.ts().deliver_signal(task_id, token, key, value)?)
    }
    /// Deliver with an explicit clock value, matching `tick`'s time source.
    pub fn deliver_signal_at(
        &self,
        task_id: Uuid,
        token: &task_store::SignalToken,
        key: &str,
        value: Value,
        now_millis: i64,
    ) -> Result<task_store::SignalResult, Error> {
        self.check()?;
        let owner = self.owner();
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        Ok(self
            .ts()
            .deliver_signal_at(task_id, token, key, value, now_millis)?)
    }
    pub fn signal_receipt(&self, task_id: Uuid, signal_id: Uuid) -> Result<Option<Signal>, Error> {
        self.check()?;
        Ok(self.ts().signal(task_id, signal_id)?)
    }
    pub fn get(&self, id: Uuid) -> Result<Option<Task>, Error> {
        self.check()?;
        Ok(self.ts().load(id)?.map(|s| s.task))
    }
    pub fn list(&self) -> Result<Vec<Task>, Error> {
        self.check()?;
        Ok(self.ts().list()?.into_iter().map(|s| s.task).collect())
    }

    /// Reevaluate waits, then run one ready task. Returns whether progress occurred.
    /// Errors after reservation poison this instance, including handler errors.
    /// Checked-document conflicts are recovered without returning an error.
    pub fn tick(&mut self, now_millis: i64) -> Result<bool, Error> {
        // A synchronous tick cannot transfer a blocked outcome to its caller.
        // Do not start one while detached invocations still own their attempts.
        if self.has_live_invocations()? {
            return Err(Error::Busy);
        }
        let reservation = match self.reserve_tick(now_millis) {
            Err(Error::Stopped)
                if self
                    .live_invocations
                    .lock()
                    .map_err(|_| Error::Poisoned)?
                    .handler_failed =>
            {
                return Err(Error::Poisoned);
            }
            other => other?,
        };
        match reservation {
            TickReservation::Progress(progress) => Ok(progress),
            TickReservation::Maintenance(_) => {
                unreachable!("serial reservation does not defer maintenance")
            }
            TickReservation::Invocation(invocation) => self
                .settle_invocation(invocation.execute()?)
                .map_err(|failure| match failure {
                    SettlementError::Rejected { error, .. }
                    | SettlementError::Handler(error)
                    | SettlementError::Indeterminate(error) => error,
                    SettlementError::Pending { .. }
                    | SettlementError::PendingPrepared { .. }
                    | SettlementError::RejectedPrepared { .. }
                    | SettlementError::IndeterminatePrepared { .. }
                    | SettlementError::IndeterminateOwned { .. } => {
                        unreachable!("serial settlement does not return pending")
                    }
                    #[cfg(test)]
                    SettlementError::Yielded { .. } => {
                        unreachable!("test yield is not enabled for serial tick")
                    }
                }),
        }
    }

    /// Failures of individual conversation cleanup callbacks, keyed by conversation.
    /// A durable uncertain attempt remains marked until explicitly reconciled.
    pub fn conversation_cleanup_failures(&self) -> &HashMap<Uuid, String> {
        &self.cleanup_failures
    }

    /// Acknowledge an externally reconciled interrupted cleanup and permit retry.
    pub fn reconcile_owned_cleanup(&mut self, id: Uuid) -> Result<(), Error> {
        self.validator
            .as_mut()
            .ok_or_else(|| Error::Handler("conversation runtime is not attached".into()))?
            .reconcile_owned_cleanup(id)
            .map_err(|error| Error::Handler(error.to_string()))?;
        self.cleanup_failures.remove(&id);
        Ok(())
    }

    /// Drive one task step and schedule bounded, independent conversation cleanup.
    /// Retained queued inputs are never executed by these cleanup workers.
    pub async fn tick_with_conversation_cleanup(
        &mut self,
        now_millis: i64,
        cancel: &pi_rs_core::CancellationToken,
    ) -> Result<bool, Error> {
        const MAX_CLEANUPS: usize = 8;
        if self.validator.is_none() {
            return Err(Error::Handler(
                "conversation runtime is not attached".into(),
            ));
        }
        if cancel.is_cancelled() {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(Error::Handler("conversation cleanup interrupted".into()));
        }
        let mut progress = self.tick(now_millis)?;
        let candidates = self
            .validator
            .as_ref()
            .unwrap()
            .owned_abort_candidates()
            .map_err(|error| Error::Handler(format!("conversation cleanup discovery: {error}")))?;
        let start = self
            .cleanup_cursor
            .and_then(|cursor| candidates.iter().position(|id| *id > cursor))
            .unwrap_or(0);
        for id in candidates
            .iter()
            .cycle()
            .skip(start)
            .take(candidates.len())
            .copied()
        {
            if self.cleanup_running.len() >= MAX_CLEANUPS {
                break;
            }
            if self.cleanup_running.contains(&id) || self.cleanup_failures.contains_key(&id) {
                continue;
            }
            let control = self.owner().conversation_controls().get(id);
            let Ok(lock) = control.worker.try_lock_owned() else {
                continue;
            };
            let worker = self.validator.as_ref().unwrap().worker();
            let timeout = self.cleanup_timeout;
            let store_owner = self.owner();
            // Keep the scheduler attachment until a cancelled worker has actually stopped.
            let attachment = self.attachment.clone();
            let cancel = cancel.clone();
            self.cleanup_running.insert(id);
            self.cleanup_cursor = Some(id);
            self.cleanup_jobs.spawn(async move {
                let _attachment = attachment;
                let result = match tokio::time::timeout(
                    timeout,
                    worker.settle_owned_abort(id, &cancel, lock),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => match store_owner
                        .store()
                        .get(&crate::runtime::owned_cleanup_key(id))
                    {
                        // No marker means the future stopped before it could dispatch.
                        Ok(None) => Ok(false),
                        Ok(Some(_)) => Err(crate::runtime::Error::Invalid(
                            "cleanup timed out; reconcile before retry".into(),
                        )),
                        Err(error) => Err(crate::runtime::Error::Storage(error)),
                    },
                };
                (id, result)
            });
            progress = true;
        }
        if !self.cleanup_running.is_empty() {
            // A dropped tick still fences the scheduler: a cleanup callback may
            // have made an external effect whose result is not yet observed.
            struct CleanupGuard {
                poisoned: Arc<std::sync::atomic::AtomicBool>,
                complete: bool,
            }
            impl Drop for CleanupGuard {
                fn drop(&mut self) {
                    if !self.complete {
                        self.poisoned
                            .store(true, std::sync::atomic::Ordering::Release);
                    }
                }
            }
            let mut guard = CleanupGuard {
                poisoned: self.poisoned.clone(),
                complete: false,
            };
            tokio::task::yield_now().await;
            while let Some(result) = self.cleanup_jobs.try_join_next() {
                match result {
                    Ok((id, Ok(changed))) => {
                        self.cleanup_running.remove(&id);
                        progress |= changed;
                    }
                    Ok((
                        _,
                        Err(
                            error @ (crate::runtime::Error::Storage(_)
                            | crate::runtime::Error::Failed),
                        ),
                    )) => {
                        return Err(Error::Handler(format!("conversation cleanup: {error}")));
                    }
                    Ok((id, Err(error))) => {
                        self.cleanup_running.remove(&id);
                        self.cleanup_failures.insert(id, error.to_string());
                    }
                    Err(error) => {
                        return Err(Error::Handler(format!(
                            "conversation cleanup worker: {error}"
                        )));
                    }
                }
            }
            guard.complete = true;
            progress |= !self.cleanup_running.is_empty();
        }
        Ok(progress)
    }

    /// Reserve at most one invocation; run it outside the scheduler mutex.
    pub fn reserve_tick(&mut self, now_millis: i64) -> Result<TickReservation, Error> {
        self.reserve_tick_inner(now_millis, false, false)
    }

    /// Reserve without waiting for conversation poll fences. A maintenance
    /// value must be retried outside the scheduler mutex when a fence is busy.
    pub fn try_reserve_tick(&mut self, now_millis: i64) -> Result<TickReservation, Error> {
        self.reserve_tick_inner(now_millis, true, false)
    }

    // The driver's overflow worker is reserved for direct generic children of
    // live TaskTool invocations, so ordinary handlers cannot consume it.
    pub(crate) fn try_reserve_task_tool_child(
        &mut self,
        now_millis: i64,
    ) -> Result<TickReservation, Error> {
        self.reserve_tick_inner(now_millis, true, true)
    }

    fn deferred_maintenance(&self, intent: MaintenanceIntent) -> TickReservation {
        TickReservation::Maintenance(PendingMaintenance {
            attachment: self.attachment.clone(),
            fence: self.failure_fence.clone(),
            state: self.live_invocations.clone(),
            scheduler_poison: self.poisoned.clone(),
            intent,
        })
    }

    fn reserve_tick_inner(
        &mut self,
        now_millis: i64,
        nonblocking: bool,
        task_tool_child_only: bool,
    ) -> Result<TickReservation, Error> {
        let fence = self.failure_fence.clone();
        let _failure_guard = if nonblocking {
            fence.try_lock().map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => Error::Busy,
                std::sync::TryLockError::Poisoned(_) => Error::Poisoned,
            })?
        } else {
            fence.lock().map_err(|_| Error::Poisoned)?
        };
        self.check()?;
        if self.reservations_stopped
            || self
                .reservation_stop
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Stopped);
        }
        // Completion may have been published after the coordinator's scan.
        // Keep dependent reservations behind its checked settlement.
        if nonblocking && self.has_published_outcomes()? {
            return Err(Error::Busy);
        }
        let owner = self.owner();
        let gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        let mut progress = false;
        // Finalization uses the same graph fence as admission, including work
        // admitted through an already-owned conversation after the hold.
        let graph_version = owner
            .store()
            .put_version()
            .map_err(task_store::Error::from)?;
        let held_tasks = self.ts().list()?;
        for snapshot in &held_tasks {
            if !snapshot.task.is_completing() {
                continue;
            }
            if matches!(snapshot.task.state(), TaskState::CompletingFailed(_))
                && !snapshot.task.abort_requested()
                && owned_graph_uncertain(owner.store(), snapshot.task.id())?
            {
                // Do not turn an unproven graph into cancellation authority.
                // Keep this owner parked while unrelated work continues.
                continue;
            }
            let live = has_live_owned(owner.store(), snapshot.task.id(), &held_tasks)?;
            let needs_cascade = matches!(snapshot.task.state(), TaskState::CompletingFailed(_))
                && !snapshot.task.abort_requested()
                && (live || has_owned_work(owner.store(), snapshot.task.id(), &held_tasks, true)?);
            if !needs_cascade
                && snapshot.task.is_task_tool()
                && has_unmarked_nested(snapshot.task.id(), &held_tasks)
            {
                if nonblocking && self.validator.is_some() {
                    return Ok(
                        self.deferred_maintenance(MaintenanceIntent::Nested(snapshot.task.id()))
                    );
                }
                if let Some(harness) = self.validator.as_mut() {
                    drop(gate);
                    owner
                        .conversation_controls()
                        .fence(|| harness.abort_nested_task_tools(snapshot.task.id()))
                        .map_err(|e| Error::Handler(format!("nested tool abort: {e:?}")))?;
                } else {
                    request_abort_nested_tools(&self.ts(), snapshot.task.id())?;
                }
                return Ok(TickReservation::Progress(true));
            }
            if needs_cascade {
                // A held failure is cancellation intent. Persist the mark and
                // cascade before letting any descendant execute again.
                if nonblocking && self.validator.is_some() {
                    return Ok(
                        self.deferred_maintenance(MaintenanceIntent::Failed(snapshot.task.id()))
                    );
                }
                if let Some(harness) = self.validator.as_mut() {
                    drop(gate);
                    owner
                        .conversation_controls()
                        .fence(|| harness.abort_owned_graph(None, Some(snapshot.task.id())))
                        .map_err(|e| Error::Handler(format!("failure abort: {e:?}")))?;
                } else {
                    request_abort_subtree(&self.ts(), snapshot.task.id())?;
                }
                return Ok(TickReservation::Progress(true));
            }
            if !live {
                let mut next = snapshot.clone();
                next.task.finalize().map_err(Error::Transition)?;
                let batch = self.ts().prepare_save(&[Save::Update(&next)])?;
                match owner.store().commit_with_prefix_put_fence(
                    &batch.checks,
                    &batch.mutations,
                    &[
                        task_store::PREFIX,
                        crate::ownership_edge::PREFIX,
                        b"runtime/conversation/",
                    ],
                    graph_version,
                ) {
                    Ok(()) | Err(durable_store::Error::Conflict { .. }) => {
                        return Ok(TickReservation::Progress(true));
                    }
                    Err(error) => return Err(task_store::Error::from(error).into()),
                }
            }
        }
        for snapshot in self.ts().list()? {
            if !snapshot.task.abort_requested()
                && let TaskState::Waiting(WaitFor::TasksFailFast(ids)) = snapshot.task.state()
                && let Some(id) = failed_sibling_to_abort(&self.ts(), &snapshot, ids)?
            {
                // Mark one entire ownership graph, then rescan. In particular,
                // a Running handler must return before its record can settle.
                if nonblocking && self.validator.is_some() {
                    return Ok(
                        self.deferred_maintenance(MaintenanceIntent::FailFast(snapshot.task.id()))
                    );
                }
                if let Some(harness) = self.validator.as_mut() {
                    drop(gate);
                    let result = owner
                        .conversation_controls()
                        .fence(|| harness.abort_owned_graph(None, Some(id)));
                    if let Err(error) = result {
                        // The sibling may have settled after dropping the gate.
                        if !self.ts().load(id)?.is_some_and(|s| s.task.is_terminal()) {
                            return Err(Error::Handler(format!("fail-fast abort: {error:?}")));
                        }
                    }
                } else {
                    request_abort_subtree(&self.ts(), id)?;
                }
                return Ok(TickReservation::Progress(true));
            }
        }
        if !self.deferred_recovery.is_empty() {
            let before = self.deferred_recovery.clone();
            self.reconcile()?;
            // A graph that remains uncertain must not starve independent work.
            if self.deferred_recovery != before {
                return Ok(TickReservation::Progress(true));
            }
        }
        for snapshot in self.ts().list()? {
            if !snapshot.task.abort_requested()
                && matches!(snapshot.task.state(), TaskState::Waiting(_))
            {
                match self.ts().poll_wait(&snapshot, now_millis) {
                    Ok(WakeResult::Woken { .. }) => progress = true,
                    Ok(WakeResult::Pending) => (),
                    Err(task_store::Error::Store(durable_store::Error::Conflict { .. })) => {
                        // A concurrent signal or task transition changed the snapshot.
                        // Rescan on the next tick instead of failing the driver.
                        progress = true;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let tasks = self.ts().list()?;
        // Use one snapshot of the active ownership graph. Admission is fenced
        // by the task gate held above.
        let cleanup_graph = if tasks
            .iter()
            .any(|task| task.task.abort_requested() && !task.task.is_terminal())
        {
            Some(
                crate::ownership_edge::CleanupGraph::load(owner.store())
                    .map_err(|e| Error::Handler(format!("ownership graph: {e}")))?,
            )
        } else {
            None
        };
        let live_invocations = self.live_invocations.clone();
        let eligible = |task: &Task| {
            if !task_tool_child_only {
                return true;
            }
            let TaskOwner::Task(parent) = task.owner() else {
                return false;
            };
            !task.is_task_tool()
                && !task.is_background()
                && tasks.iter().any(|entry| {
                    entry.task.id() == *parent
                        && live_task_tool_parent(&entry.task, &live_invocations)
                })
        };
        // Children settle before their owner, including work below terminal task
        // anchors. Ignore only graph edges explicitly identified as cyclic.
        let mut abortable = None;
        for snapshot in &tasks {
            if !eligible(&snapshot.task)
                || !snapshot.task.abort_requested()
                || snapshot.task.state() != &TaskState::Ready
            {
                continue;
            }
            let graph = cleanup_graph.as_ref().unwrap();
            let mut pending = false;
            let mut anchors = vec![snapshot.task.id()];
            let mut seen = HashSet::new();
            while let Some(id) = anchors.pop() {
                if !seen.insert(id) {
                    continue;
                }
                let anchor = tasks
                    .iter()
                    .find(|item| item.task.id() == id)
                    .ok_or(Error::Ownership)?;
                for child in &tasks {
                    if child.task.owner() != &TaskOwner::Task(id)
                        || child.task.is_background()
                        // A terminal anchor can lead back to the live root
                        // even after the graph no longer reports a live cycle.
                        || seen.contains(&child.task.id())
                        || graph.cyclic_task_edge(id, child.task.id())
                    {
                        continue;
                    }
                    if child.task.is_terminal() {
                        anchors.push(child.task.id());
                    } else {
                        pending = true;
                        break;
                    }
                }
                if pending {
                    break;
                }
                if graph.uncertain_task(id) {
                    pending = true;
                    break;
                }
                for edge in graph.task_edges(id) {
                    if edge.mode != crate::ownership_edge::Mode::Foreground {
                        continue;
                    }
                    let key = format!("runtime/conversation/{}", edge.child);
                    let entry = owner
                        .store()
                        .get(key.as_bytes())
                        .map_err(task_store::Error::from)?;
                    let child = entry.and_then(|entry| {
                        serde_json::from_slice::<crate::runtime::Conversation>(&entry.value).ok()
                    });
                    let Some(child) = child else {
                        pending = true;
                        break;
                    };
                    if child.id != edge.child || edge.task_owner != *anchor.task.owner() {
                        pending = true;
                        break;
                    }
                    if child.abort_requested && !graph.cyclic_conversation_edge(id, child.id) {
                        pending = true;
                        break;
                    }
                }
                if pending {
                    break;
                }
            }
            if !pending {
                abortable = Some(snapshot);
                break;
            }
        }
        if let Some(snapshot) = abortable {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            if snapshot.task.restarts() > 0
                && snapshot.task.restarts() >= snapshot.task.restart_limit()
            {
                self.ts()
                    .update(snapshot, |task| task.fail_abort_restart())?;
                self.poisoned
                    .store(false, std::sync::atomic::Ordering::Release);
                return Ok(TickReservation::Progress(true));
            }
            if let Some(reason) = self.blocked_reason(&snapshot.task) {
                let diagnostic = orphan_diagnostic(&reason);
                self.ts().update(snapshot, |t| {
                    t.start()?;
                    t.fail(diagnostic)
                })?;
                self.poisoned
                    .store(false, std::sync::atomic::Ordering::Release);
                return Ok(TickReservation::Progress(true));
            }
            let abort = self
                .registered(&snapshot.task)
                .ok()
                .and_then(|r| r.abort.clone());
            if let Some(abort) = abort {
                if !self.reserve_resolved(snapshot, None)? {
                    let reason = self
                        .blocked_reason(&snapshot.task)
                        .ok_or(Error::Registration)?;
                    let diagnostic = orphan_diagnostic(&reason);
                    self.ts().update(snapshot, |t| {
                        t.start()?;
                        t.fail(diagnostic)
                    })?;
                    self.poisoned
                        .store(false, std::sync::atomic::Ordering::Release);
                    return Ok(TickReservation::Progress(true));
                }
                let running = self
                    .ts()
                    .load(snapshot.task.id())?
                    .ok_or(Error::Ownership)?;
                let invocation = self.make_invocation(running, abort, true)?;
                self.poisoned
                    .store(false, std::sync::atomic::Ordering::Release);
                drop(gate);
                return Ok(TickReservation::Invocation(Box::new(invocation)));
            } else {
                self.ts().update(snapshot, |t| {
                    t.start()?;
                    t.fail("orphaned: missing abort handler or task definition".into())
                })?;
            }
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(TickReservation::Progress(true));
        }
        let Some(snapshot) = tasks
            .iter()
            .find(|s| {
                eligible(&s.task)
                    && s.task.state() == &TaskState::Ready
                    && !s.task.abort_requested()
                    && self.blocked_reason(&s.task).is_none()
                    && self.sealed_identity_not_busy(&s.task)
                    && self.ancestors_available(&s.task, &tasks)
            })
            .cloned()
        else {
            return Ok(TickReservation::Progress(progress));
        };
        let handler = self.registered(&snapshot.task)?.handler.clone();
        if !self.reserve_resolved(&snapshot, None)? {
            return Ok(TickReservation::Progress(true));
        }
        let running = self
            .ts()
            .load(snapshot.task.id())?
            .ok_or(Error::Ownership)?;
        let invocation = self.make_invocation(running, handler, false)?;
        self.poisoned
            .store(false, std::sync::atomic::Ordering::Release);
        drop(gate);
        Ok(TickReservation::Invocation(Box::new(invocation)))
    }

    #[allow(clippy::too_many_arguments)]
    fn settle_handler_result(
        &mut self,
        running: task_store::StoredTask,
        result: Result<Step, String>,
        mut documents: crate::document::DetachedTaskDocuments,
        reported_handler_failure: &mut bool,
        _gate: std::sync::MutexGuard<'_, ()>,
        nonblocking_abort: bool,
        ordering_graph_version: u64,
        deferred: &mut Option<DeferredSettlement>,
    ) -> Result<bool, Error> {
        let owner = self.owner();
        if let Some(error) = &documents.task_input_storage {
            return Err(crate::document::Error::TaskInputStore(error.clone()).into());
        }
        #[cfg(test)]
        if let Some(hook) = &self.after_order_hook {
            hook(0, owner.store());
        }
        // Capture the graph boundary before loading the reservation. A task
        // rewrite after that load must trigger the graph retry, even when the
        // writer does not share this owner's gate.
        let success_graph_version = ordering_graph_version;
        // An abort can commit while the synchronous handler is running. Its
        // output (including proposed children) must never cross that fence.
        let current = self.ts().load(running.task.id())?.ok_or(Error::Ownership)?;
        #[cfg(test)]
        if let Some(hook) = &self.after_result_load_hook {
            hook(0, owner.store());
        }
        if current.version != running.version && current.task.abort_requested() {
            self.ts().update(&current, |task| task.yield_ready())?;
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(true);
        }
        if current.task != running.task {
            return Err(task_store::Error::Store(durable_store::Error::Conflict {
                key: task_store::key(running.task.id()),
            })
            .into());
        }
        // Admission may have rewritten an identical reservation while the
        // handler ran. The gate keeps cooperating admissions out until commit.
        let original_version = running.version;
        let original_running = running.clone();
        let running = current.clone();
        let pending_root = match pending_abort_root_for_running(
            &self.ts(),
            running.task.id(),
            self.validator.is_none(),
        ) {
            Err(Error::Busy) if nonblocking_abort => {
                *deferred = Some(DeferredSettlement::Unprepared(
                    original_running,
                    result,
                    documents,
                ));
                return Err(Error::Busy);
            }
            other => other?,
        };
        if let Some(root) = pending_root {
            if nonblocking_abort && self.validator.is_some() {
                *deferred = Some(DeferredSettlement::Unprepared(
                    original_running,
                    result,
                    documents,
                ));
                return Err(Error::PendingAbort(root));
            }
            self.abort_failed_running(&owner, root, running.task.id(), _gate)?;
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(true);
        }
        if documents.task_input_conflict.is_some() {
            if running.task.replay_safe() && self.registered(&running.task)?.replay_safe {
                self.ts().update(&current, |task| task.yield_ready())?;
            } else {
                return self.finish_running_conflict(
                    &current,
                    "document conflict: execution outcome requires reconciliation".into(),
                    &owner,
                    _gate,
                    nonblocking_abort,
                    deferred,
                );
            }
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
            return Ok(true);
        }
        let step = match result {
            Ok(step) => step,
            Err(reason) if running.task.is_task_tool() => {
                return self.finish_running_failure(
                    &running,
                    reason,
                    &owner,
                    _gate,
                    nonblocking_abort,
                    deferred,
                );
            }
            Err(reason) => {
                *reported_handler_failure = true;
                return Err(Error::Handler(reason));
            }
        };
        if let Step::Fail(reason) = step {
            // Failure discards all staged effects, including child admissions.
            return self.finish_running_failure(
                &running,
                reason,
                &owner,
                _gate,
                nonblocking_abort,
                deferred,
            );
        }
        let mut staged = std::mem::take(&mut documents.keyed_children);
        let parent_key = task_store::key(running.task.id());
        for (_, _, prepared) in &mut staged {
            for check in &mut prepared.checks {
                if check.key == parent_key
                    && check.expected == durable_store::Expected::Version(prepared.parent_version)
                {
                    check.expected = durable_store::Expected::Version(running.version);
                }
            }
        }
        for (key, child, _) in &staged {
            let reg = self.registered(child)?;
            if reg.version != child.definition_version()
                || reg.replay_safe != child.replay_safe()
                || documents
                    .keyed_child_handlers
                    .get(key)
                    .is_some_and(|expected| !Arc::ptr_eq(expected, &reg.handler))
            {
                return Err(Error::Registration);
            }
        }
        let mut next = running.clone();
        let mut children = Vec::new();
        let mut wait_graph_fence = false;
        match step {
            Step::Continue(checkpoint) => {
                next.task.advance(checkpoint).map_err(Error::Transition)?;
                next.task.yield_ready().map_err(Error::Transition)?;
            }
            Step::Wait {
                checkpoint,
                condition,
                children: proposed,
            } => {
                if next.task.checkpoint() != &checkpoint {
                    next.task.advance(checkpoint).map_err(Error::Transition)?;
                }
                let mut ids = std::collections::HashSet::new();
                for child in proposed {
                    if child.owner() != &TaskOwner::Task(next.task.id())
                        || child.state() != &TaskState::Ready
                        || !ids.insert(child.id())
                    {
                        return Err(Error::Ownership);
                    }
                    let reg = self.registered(&child)?;
                    if reg.version != child.definition_version()
                        || reg.replay_safe != child.replay_safe()
                    {
                        return Err(Error::Registration);
                    }
                    children.push(child);
                }
                if let WaitFor::Tasks(ref waiting)
                | WaitFor::TasksAny(ref waiting)
                | WaitFor::TasksFailFast(ref waiting) = condition
                {
                    wait_graph_fence = true;
                    let mut seen = std::collections::HashSet::new();
                    for id in waiting {
                        if *id == next.task.id() || !seen.insert(*id) {
                            return self.reject_invalid_wait(
                                &running,
                                &owner,
                                _gate,
                                nonblocking_abort,
                                deferred,
                            );
                        }
                        if !children.iter().any(|c| c.id() == *id)
                            && !staged.iter().any(|(_, _, p)| p.child == *id)
                        {
                            let Some(child) = self.ts().load(*id)? else {
                                return self.reject_invalid_wait(
                                    &running,
                                    &owner,
                                    _gate,
                                    nonblocking_abort,
                                    deferred,
                                );
                            };
                            if matches!(condition, WaitFor::TasksFailFast(_))
                                && child.task.owner() != &TaskOwner::Task(next.task.id())
                            {
                                return self.reject_invalid_wait(
                                    &running,
                                    &owner,
                                    _gate,
                                    nonblocking_abort,
                                    deferred,
                                );
                            }
                        }
                    }
                    let new_children: HashMap<_, _> = children
                        .iter()
                        .map(|child| (child.id(), child))
                        .chain(
                            staged
                                .iter()
                                .filter(|(_, _, p)| !p.mutations.is_empty())
                                .map(|(_, child, p)| (p.child, child)),
                        )
                        .collect();
                    match validate_wait_graph(owner.store(), next.task.id(), waiting, &new_children)
                    {
                        Ok(()) => (),
                        Err(Error::Ownership) => {
                            return self.reject_invalid_wait(
                                &running,
                                &owner,
                                _gate,
                                nonblocking_abort,
                                deferred,
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                next.task.wait(condition).map_err(Error::Transition)?;
            }
            Step::Succeed(value) => {
                // Include children staged in this transaction, which are not
                // present in the committed graph snapshot yet.
                if staged.iter().any(|(_, _, p)| !p.mutations.is_empty())
                    || has_live_owned(owner.store(), next.task.id(), &self.ts().list()?)?
                {
                    next.task.hold_success(value).map_err(Error::Transition)?;
                } else {
                    next.task.succeed(value).map_err(Error::Transition)?;
                }
            }
            Step::Fail(_) => unreachable!(),
        }
        // A newly admitted child can adopt legacy orphans even if this step
        // does not wait for it. Such adoption must not create a cycle with
        // the live owner or its durable waits.
        let new_children: HashMap<_, _> = children
            .iter()
            .map(|child| (child.id(), child))
            .chain(
                staged
                    .iter()
                    .filter(|(_, _, p)| !p.mutations.is_empty())
                    .map(|(_, child, p)| (p.child, child)),
            )
            .collect();
        if !new_children.is_empty() {
            wait_graph_fence = true;
            let targets: Vec<_> = new_children.keys().copied().collect();
            match validate_wait_graph(owner.store(), next.task.id(), &targets, &new_children) {
                Ok(()) => (),
                Err(Error::Ownership) => {
                    return self.reject_invalid_wait(
                        &running,
                        &owner,
                        _gate,
                        nonblocking_abort,
                        deferred,
                    );
                }
                Err(error) => return Err(error),
            }
        }
        // Retain the proposed graph and wait targets for validation after an
        // unrelated graph-only conflict, without running the handler again.
        let retry_children: HashMap<Uuid, Task> = new_children
            .iter()
            .map(|(id, task)| (*id, (*task).clone()))
            .collect();
        let retry_wait = match next.task.state() {
            TaskState::Waiting(
                WaitFor::Tasks(ids) | WaitFor::TasksAny(ids) | WaitFor::TasksFailFast(ids),
            ) => ids.clone(),
            _ => Vec::new(),
        };
        next.task
            .record_completed_attempt(&running.task)
            .map_err(Error::Transition)?;
        let mut batch = vec![Save::Update(&next)];
        batch.extend(children.iter().map(Save::Create));
        // Validate all newly created children as one ownership graph, including
        // collisions with children returned directly by the handler.
        batch.extend(staged.iter().filter_map(|(_, child, prepared)| {
            (!prepared.mutations.is_empty()).then_some(Save::Create(child))
        }));
        #[cfg(test)]
        if self.inject_preparation_error == 1 {
            return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
        }
        let mut reservation_only = HashMap::<Uuid, Vec<crate::runtime::SubmissionNotify>>::new();
        let mut reservation_destinations = Vec::new();
        let mut reservation_conversations_only = false;
        let document_result = documents.prepare(&running).and_then(|mut prepared| {
            if let Some(intent) = &prepared.task_input {
                reservation_conversations_only = !prepared
                    .document_batch
                    .checks
                    .iter()
                    .chain(prepared.transaction.checks())
                    .chain(intent.ancestry_checks.iter())
                    .any(|check| check.key == b"runtime/conversation/");
                for item in &intent.admissions {
                    if item.notification_binding.is_none() {
                        continue;
                    }
                    let Some(notify) = &item.notify else { continue };
                    reservation_destinations.push(notify.clone());
                    let revision = task_store::signal_revision_key(notify.task_id);
                    if !prepared
                        .document_batch
                        .checks
                        .iter()
                        .chain(prepared.transaction.checks())
                        .any(|check| check.key == revision)
                    {
                        reservation_only
                            .entry(notify.task_id)
                            .or_default()
                            .push(notify.clone());
                    }
                }
                let harness = self.validator.as_ref().ok_or_else(|| {
                    crate::document::Error::InvalidSessionAdmission(
                        "task input requires an attached harness".into(),
                    )
                })?;
                let mut batch = harness.prepare_task_input(intent)?;
                let mut ancestry = crate::task_store::OwnedBatch {
                    checks: intent.ancestry_checks.clone(),
                    mutations: Vec::new(),
                };
                // Any of these independently captured batches may fence the
                // same ancestor. Preserve deadlines and turn disagreement
                // into a commit conflict so the task's replay policy applies.
                crate::document::reconcile_task_checks(&mut prepared.document_batch, &mut ancestry);
                crate::document::reconcile_task_checks(&mut ancestry, &mut batch);
                crate::document::reconcile_task_checks(&mut prepared.document_batch, &mut batch);
                crate::document::reconcile_task_checks(&mut prepared.document_batch, &mut ancestry);
                let mut combined = crate::document::Transaction::new();
                combined.append(prepared.document_batch)?;
                combined.append(ancestry)?;
                combined.append(batch)?;
                prepared.document_batch = combined.into_owned_batch()?;
            }
            Ok(prepared)
        });
        #[cfg(test)]
        if self.inject_preparation_error == 2 {
            return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
        }
        #[cfg(test)]
        let document_result = if self.inject_batch_prepare_conflict {
            Err(crate::document::Error::Store(
                durable_store::Error::Conflict {
                    key: b"test/prepare".to_vec(),
                },
            ))
        } else {
            document_result
        };
        #[cfg(test)]
        if let Some(hook) = &self.preparation_conflict_hook {
            hook(0, owner.store());
        }
        let prepared_documents = match document_result {
            Ok(prepared) => prepared,
            Err(crate::document::Error::Store(durable_store::Error::Conflict { .. })) => {
                if let Some(root) = pending_abort_root_for_running(
                    &self.ts(),
                    running.task.id(),
                    self.validator.is_none(),
                )? {
                    if nonblocking_abort && self.validator.is_some() {
                        *deferred = Some(DeferredSettlement::PreparationConflict(
                            PreparationConflict {
                                attachment: self.attachment.clone(),
                                running: current.clone(),
                            },
                        ));
                        return Err(Error::PendingAbort(root));
                    }
                    self.abort_failed_running(&owner, root, running.task.id(), _gate)?;
                    self.poisoned
                        .store(false, std::sync::atomic::Ordering::Release);
                    return Ok(true);
                }
                // A staged runtime checkpoint observed a newer root while
                // preparing; no effects committed. Follow normal replay policy.
                if running.task.replay_safe() && self.registered(&running.task)?.replay_safe {
                    self.ts().update(&current, |task| task.yield_ready())?;
                } else {
                    return self.finish_running_conflict(
                        &current,
                        "document conflict: execution outcome requires reconciliation".into(),
                        &owner,
                        _gate,
                        nonblocking_abort,
                        deferred,
                    );
                }
                self.poisoned
                    .store(false, std::sync::atomic::Ordering::Release);
                return Ok(true);
            }
            Err(error) => return Err(error.into()),
        };
        let has_task_submission = prepared_documents.task_input.is_some();
        let mut transaction = prepared_documents.transaction;
        let mut document_batch = prepared_documents.document_batch;
        // Checks observed at reservation are equivalent to the reloaded task.
        // Preserve independently observed versions and any deadline.
        for check in &mut document_batch.checks {
            if check.key == parent_key {
                match &mut check.expected {
                    durable_store::Expected::Version(version)
                    | durable_store::Expected::VersionBefore(version, _)
                        if *version == original_version =>
                    {
                        *version = running.version
                    }
                    _ => {}
                }
            }
        }
        let mut document_keys: HashSet<_> = document_batch
            .checks
            .iter()
            .map(|c| c.key.clone())
            .collect();
        // One graph batch: the parent transition replaces admission's parent bump.
        // Both use the captured version, so aborts and competing graph writers
        // fence the whole transaction, without a self-conflict.
        let mut graph = self.ts().prepare_save(&batch)?;
        let mut staged_signal_checks = HashSet::new();
        for (_, _, prepared) in staged {
            for check in prepared.checks {
                if check.key.starts_with(b"durable/signal-revision/") {
                    staged_signal_checks.insert(check.key.clone());
                }
                document_keys.insert(check.key.clone());
                if !graph.checks.contains(&check) {
                    graph.checks.push(check);
                }
            }
            for mutation in prepared.mutations {
                let key = match &mutation {
                    durable_store::Mutation::Put { key, .. }
                    | durable_store::Mutation::Delete { key } => key,
                };
                if *key != parent_key && !key.starts_with(task_store::PREFIX) {
                    graph.mutations.push(mutation);
                }
            }
        }
        // A separately prepared graph participant may also have observed a
        // signal revision. Such an observation remains an original fence.
        let mut reservation_wait = HashMap::new();
        reservation_only.retain(|task, _| {
            let key = task_store::signal_revision_key(*task);
            if !graph.checks.iter().any(|check| check.key == key) {
                return true;
            }
            if *task != next.task.id() || staged_signal_checks.contains(&key) {
                return false;
            }
            let keys = match next.task.state() {
                TaskState::Waiting(WaitFor::TypedSignal(key)) => vec![key.clone()],
                TaskState::Waiting(WaitFor::TypedSleep { signals, .. }) if !signals.is_empty() => {
                    signals.clone()
                }
                _ => return false,
            };
            reservation_wait.insert(*task, keys);
            true
        });
        reservation_conversations_only &= !reservation_destinations.is_empty()
            && !graph
                .checks
                .iter()
                .any(|check| check.key == b"runtime/conversation/");
        crate::document::reconcile_task_checks(&mut document_batch, &mut graph);
        transaction.append(document_batch)?;
        transaction.append(graph)?;
        let mut notifications = None;
        if has_task_submission && let Some(harness) = &self.validator {
            let settlements =
                crate::runtime::submission_settlements(owner.store(), transaction.mutations())
                    .map_err(|error| {
                        crate::document::Error::InvalidSessionAdmission(error.to_string())
                    })?;
            if !settlements.is_empty() || !reservation_destinations.is_empty() {
                let mut assembly = TaskNotificationAssembly {
                    original: transaction.clone(),
                    clocks: vec![None; settlements.len()],
                    settlements,
                    reservation_only,
                    reservation_wait,
                    reservation_destinations,
                    reservation_conversations_only,
                    retries: 0,
                };
                transaction = match assembly.assemble(owner.store(), harness) {
                    Ok(transaction) => transaction,
                    Err(
                        error @ (crate::document::Error::Signal(
                            task_store::Error::SignalKeyMismatch
                            | task_store::Error::SignalMismatch
                            | task_store::Error::SignalKeyConsumed
                            | task_store::Error::InvalidSignalPayload,
                        )
                        | crate::document::Error::InvalidSessionAdmission(_)),
                    ) => {
                        return self.finish_running_failure(
                            &running,
                            error.to_string(),
                            &owner,
                            _gate,
                            nonblocking_abort,
                            deferred,
                        );
                    }
                    Err(error) => return Err(error.into()),
                };
                notifications = Some(assembly);
            }
        }
        let _ = wait_graph_fence;
        let mut prepared = PreparedSettlement {
            attachment: self.attachment.clone(),
            transaction,
            notifications,
            running,
            candidate: next,
            wait_targets: retry_wait,
            staged_children: retry_children,
            conflict_keys: document_keys,
            graph_version: success_graph_version,
            attempts: 0,
            commit_uncertain: false,
        };
        let result = self.commit_prepared_settlement(
            &mut prepared,
            &owner,
            _gate,
            nonblocking_abort,
            deferred,
        );
        if nonblocking_abort && matches!(result, Err(Error::PendingAbort(_) | Error::Busy)) {
            if deferred.is_none() {
                *deferred = Some(DeferredSettlement::Prepared(prepared));
            }
            return result;
        }
        #[cfg(test)]
        if matches!(result, Err(Error::TestYield)) {
            self.yielded_prepared = Some(prepared);
            return result;
        }
        if nonblocking_abort && result.is_err() && deferred.is_none() {
            *deferred = Some(DeferredSettlement::IndeterminatePrepared(prepared));
        }
        result
    }

    fn commit_prepared_settlement(
        &mut self,
        prepared: &mut PreparedSettlement,
        owner: &Arc<OwnedStore>,
        _gate: std::sync::MutexGuard<'_, ()>,
        nonblocking_abort: bool,
        deferred: &mut Option<DeferredSettlement>,
    ) -> Result<bool, Error> {
        const GRAPH_RETRIES: usize = 8;
        let mut notification_conflict_exhausted = false;
        let committed = loop {
            #[cfg(test)]
            if let Some(hook) = &self.result_commit_hook {
                hook(prepared.attempts, owner.store());
            }
            let result = prepared
                .transaction
                .commit_task_graph(owner.store(), prepared.graph_version);
            let Err(crate::document::Error::Store(durable_store::Error::Conflict { ref key })) =
                result
            else {
                prepared.commit_uncertain = result.is_err();
                break result;
            };
            prepared.commit_uncertain = false;
            if key == b"runtime/conversation/"
                && let Some(assembly) = &mut prepared.notifications
                && assembly.reservation_conversations_only
            {
                if !assembly.refresh_reservation_conversations(owner.store())? {
                    break result;
                }
                let harness = self.validator.as_ref().ok_or(Error::Ownership)?;
                prepared.transaction = assembly.assemble(owner.store(), harness)?;
                // The independent graph prefix fence still needs its normal
                // ownership/abort revalidation below.
            }
            if let Some(assembly) = &mut prepared.notifications
                && (assembly.settlements.iter().any(|(notify, _, _)| {
                    task_store::key(notify.task_id) == *key
                        || task_store::signal_revision_key(notify.task_id) == *key
                }) || assembly
                    .reservation_only
                    .keys()
                    .any(|task| task_store::signal_revision_key(*task) == *key))
            {
                let refreshed = assembly.refresh_reservation_revision(owner.store(), key)?;
                let old = assembly
                    .original
                    .checks()
                    .iter()
                    .find(|check| check.key == *key);
                let new = prepared
                    .transaction
                    .checks()
                    .iter()
                    .find(|check| check.key == *key);
                let retry = refreshed
                    || match (old, new) {
                        (None, Some(_)) => true,
                        (Some(old), Some(new))
                            if assembly
                                .settlements
                                .iter()
                                .any(|(notify, _, _)| task_store::key(notify.task_id) == *key) =>
                        {
                            crate::runtime::notification_deadline_only_conflict(
                                owner.store(),
                                key,
                                &old.expected,
                                &new.expected,
                            )
                            .map_err(task_store::Error::from)?
                        }
                        _ => false,
                    };
                if retry {
                    if assembly.retries >= 3 {
                        // The original checked transaction has not committed.
                        // Use the ordinary replay/reconciliation policy after the
                        // bounded notification-only reassembly budget is spent.
                        notification_conflict_exhausted = true;
                        break result;
                    }
                    let harness = self.validator.as_ref().ok_or(Error::Ownership)?;
                    prepared.transaction = assembly.assemble(owner.store(), harness)?;
                    assembly.retries += 1;
                    continue;
                }
            }
            if !(key == task_store::PREFIX
                || key == crate::ownership_edge::PREFIX
                || key == b"runtime/conversation/")
                || prepared.attempts >= GRAPH_RETRIES
            {
                break result;
            }
            #[cfg(test)]
            if self.yield_after_graph_conflict {
                return Err(Error::TestYield);
            }
            match prepared.revalidate(self, owner, &_gate)? {
                PreparedRevalidation::Ready => (),
                PreparedRevalidation::BlockedOrder => return Err(Error::Busy),
                PreparedRevalidation::PendingAbort(root) => {
                    if nonblocking_abort && self.validator.is_some() {
                        return Err(Error::PendingAbort(root));
                    }
                    self.abort_failed_running(owner, root, prepared.running.task.id(), _gate)?;
                    self.poisoned
                        .store(false, std::sync::atomic::Ordering::Release);
                    return Ok(true);
                }
                PreparedRevalidation::Aborted(latest) => {
                    self.yield_aborted_running(&latest)?;
                    self.poisoned
                        .store(false, std::sync::atomic::Ordering::Release);
                    return Ok(true);
                }
                PreparedRevalidation::Stale => break result,
            }
            prepared.attempts += 1;
        };
        #[cfg(test)]
        if committed.is_ok() && self.inject_post_commit_error {
            prepared.commit_uncertain = true;
            return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
        }
        match committed {
            Ok(()) => {}
            Err(crate::document::Error::Store(durable_store::Error::Conflict { key })) => {
                if let Some(root) = pending_abort_root_for_running(
                    &self.ts(),
                    prepared.running.task.id(),
                    self.validator.is_none(),
                )? {
                    if nonblocking_abort && self.validator.is_some() {
                        return Err(Error::PendingAbort(root));
                    }
                    self.abort_failed_running(owner, root, prepared.running.task.id(), _gate)?;
                    self.poisoned
                        .store(false, std::sync::atomic::Ordering::Release);
                    return Ok(true);
                }
                let current = self
                    .ts()
                    .load(prepared.running.task.id())?
                    .ok_or(Error::Ownership)?;
                if current.version != prepared.running.version {
                    if current.task.abort_requested() {
                        self.yield_aborted_running(&current)?;
                    } else {
                        return Err(crate::document::Error::Store(
                            durable_store::Error::Conflict { key },
                        )
                        .into());
                    }
                } else if notification_conflict_exhausted
                    || prepared.conflict_keys.contains(&key)
                    || key == task_store::PREFIX
                    || key == crate::ownership_edge::PREFIX
                    || key == b"runtime/conversation/"
                {
                    // The checked batch definitely did not commit. Only registered
                    // replay-safe invocations may be run again after external effects.
                    if prepared.running.task.replay_safe()
                        && self.registered(&prepared.running.task)?.replay_safe
                    {
                        self.ts().update(&current, |task| task.yield_ready())?;
                    } else {
                        return self.finish_running_conflict(
                            &current,
                            "document conflict: execution outcome requires reconciliation".into(),
                            owner,
                            _gate,
                            nonblocking_abort,
                            deferred,
                        );
                    }
                } else {
                    return Err(
                        crate::document::Error::Store(durable_store::Error::Conflict { key })
                            .into(),
                    );
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.poisoned
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(true)
    }

    /// Revalidate retained state before its first resumed commit. The caller
    /// retains the settlement and its lease when abort intent remains pending.
    #[allow(dead_code)] // The pending-settlement coordinator will call this.
    fn resume_prepared_settlement(
        &mut self,
        prepared: &mut PreparedSettlement,
        owner: &Arc<OwnedStore>,
        gate: std::sync::MutexGuard<'_, ()>,
    ) -> Result<bool, Error> {
        match prepared.revalidate(self, owner, &gate)? {
            PreparedRevalidation::Ready => {
                self.commit_prepared_settlement(prepared, owner, gate, false, &mut None)
            }
            PreparedRevalidation::PendingAbort(root) => Err(Error::PendingAbort(root)),
            PreparedRevalidation::BlockedOrder => Err(Error::Busy),
            PreparedRevalidation::Aborted(latest) => {
                self.yield_aborted_running(&latest)?;
                self.poisoned
                    .store(false, std::sync::atomic::Ordering::Release);
                Ok(true)
            }
            PreparedRevalidation::Stale => {
                Err(task_store::Error::Store(durable_store::Error::Conflict {
                    key: task_store::key(prepared.running.task.id()),
                })
                .into())
            }
        }
    }
    /// A child must not execute while an interrupted or incompatible ancestor is
    /// still awaiting recovery; such an ancestor can later cancel the subtree.
    fn ancestors_available(&self, task: &Task, tasks: &[task_store::StoredTask]) -> bool {
        task_ancestors_available(self.owner().store(), task, tasks, |parent| {
            parent.abort_requested()
                || parent.is_terminal()
                || matches!(parent.state(), TaskState::CompletingFailed(_))
                || (parent.state() == &TaskState::Running
                    && !live_task_tool_parent(parent, &self.live_invocations))
                || self.blocked_reason(parent).is_some()
        })
    }
    /// Persist abort intent for the live subtree. The synchronous scheduler can
    /// accept this only between invocations; subsequent ticks drain children first.
    pub fn request_abort(&mut self, id: Uuid) -> Result<(), Error> {
        self.check()?;
        if self.validator.is_some() {
            let root = self.ts().load(id)?.ok_or(Error::Ownership)?;
            if root.task.is_terminal() {
                return Err(Error::Transition(TransitionError::InvalidState));
            }
        }
        let result = if let Some(harness) = self.validator.as_mut() {
            let owner = self.attachment.owner.clone();
            owner
                .conversation_controls()
                .fence(|| harness.abort_owned_graph(None, Some(id)))
                .map(|_| ())
                .map_err(|e| Error::Handler(format!("foreground abort: {e:?}")))
        } else {
            request_abort_subtree(&self.ts(), id)
        };
        if result.is_err() && (self.validator.is_some() || matches!(result, Err(Error::Store(_)))) {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
        }
        result
    }

    /// Cancel a task and all of its owned descendants in one transaction.
    /// This requires exclusive scheduler ownership and cannot interrupt `tick`.
    /// Returns `Busy` while any detached invocation retains its lease; use
    /// `request_abort` to mark a running attempt for bottom-up cleanup.
    pub fn cancel(&mut self, id: Uuid) -> Result<(), Error> {
        self.check()?;
        if !self
            .live_invocations
            .lock()
            .map_err(|_| Error::Poisoned)?
            .attempts
            .is_empty()
        {
            return Err(Error::Busy);
        }
        let records = self.ts().list()?;
        let root = records
            .iter()
            .find(|s| s.task.id() == id)
            .ok_or(Error::Ownership)?;
        if root.task.is_terminal() {
            return Err(Error::Transition(TransitionError::InvalidState));
        }
        if self.validator.is_some() {
            let mut ids = HashSet::from([id]);
            loop {
                let before = ids.len();
                for record in &records {
                    if let TaskOwner::Task(parent) = record.task.owner()
                        && ids.contains(parent)
                    {
                        ids.insert(record.task.id());
                    }
                }
                if ids.len() == before {
                    break;
                }
            }
            if crate::ownership_edge::reject_foreground_children(
                self.attachment.owner.store(),
                records
                    .iter()
                    .filter(|record| ids.contains(&record.task.id()))
                    .map(|record| record.task.id()),
            )
            .map_err(|e| Error::Handler(format!("ownership edges: {e}")))?
            {
                return self.request_abort(id);
            }
        }
        self.poisoned
            .store(true, std::sync::atomic::Ordering::Release);
        let result = self.cancel_roots(&records, &[id], None);
        if result.is_ok() {
            self.poisoned
                .store(false, std::sync::atomic::Ordering::Release);
        }
        result
    }

    fn cancel_roots(
        &self,
        records: &[task_store::StoredTask],
        roots: &[Uuid],
        extra: Option<task_store::StoredTask>,
    ) -> Result<(), Error> {
        let mut ids: HashSet<Uuid> = roots.iter().copied().collect();
        loop {
            let before = ids.len();
            for record in records {
                if let TaskOwner::Task(parent) = record.task.owner()
                    && ids.contains(parent)
                {
                    ids.insert(record.task.id());
                }
            }
            if ids.len() == before {
                break;
            }
        }
        // A held outcome cannot be overwritten by immediate cancellation.
        // Switch these subtrees to the durable bottom-up abort path.
        if records
            .iter()
            .any(|record| ids.contains(&record.task.id()) && record.task.is_completing())
        {
            for id in roots {
                let root = records
                    .iter()
                    .find(|record| record.task.id() == *id)
                    .ok_or(Error::Ownership)?;
                if !root.task.is_terminal() {
                    request_abort_subtree(&self.ts(), *id)?;
                }
            }
            return Ok(());
        }
        if crate::ownership_edge::reject_foreground_children(
            self.attachment.owner.store(),
            records
                .iter()
                .filter(|record| ids.contains(&record.task.id()))
                .map(|record| record.task.id()),
        )
        .map_err(|e| Error::Handler(format!("ownership edges: {e}")))?
        {
            return Err(Error::ForegroundAbortUnsupported);
        }
        let mut updates = Vec::new();
        for record in records {
            if ids.contains(&record.task.id())
                && !record.task.is_terminal()
                && !record.task.abort_requested()
            {
                let mut next = record.clone();
                next.task.cancel().map_err(Error::Transition)?;
                updates.push(next);
            }
        }
        if let Some(next) = extra {
            updates.push(next);
        }
        let batch: Vec<_> = updates.iter().map(Save::Update).collect();
        self.ts().save(&batch)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_running_failure(
        &mut self,
        parent: &task_store::StoredTask,
        reason: String,
        owner: &Arc<OwnedStore>,
        gate: std::sync::MutexGuard<'_, ()>,
        nonblocking_abort: bool,
        deferred: &mut Option<DeferredSettlement>,
    ) -> Result<bool, Error> {
        self.finish_running_owned_failure(
            parent,
            reason,
            owner,
            gate,
            nonblocking_abort,
            deferred,
            true,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn finish_running_conflict(
        &mut self,
        parent: &task_store::StoredTask,
        reason: String,
        owner: &Arc<OwnedStore>,
        gate: std::sync::MutexGuard<'_, ()>,
        nonblocking_abort: bool,
        deferred: &mut Option<DeferredSettlement>,
    ) -> Result<bool, Error> {
        self.finish_running_owned_failure(
            parent,
            reason,
            owner,
            gate,
            nonblocking_abort,
            deferred,
            false,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn finish_running_owned_failure(
        &mut self,
        parent: &task_store::StoredTask,
        reason: String,
        owner: &Arc<OwnedStore>,
        gate: std::sync::MutexGuard<'_, ()>,
        nonblocking_abort: bool,
        deferred: &mut Option<DeferredSettlement>,
        completed: bool,
    ) -> Result<bool, Error> {
        let mut settlement = FailureSettlement {
            attachment: self.attachment.clone(),
            running: parent.clone(),
            reason,
            completed,
            check_pending: true,
            attempt: 0,
            candidate: None,
        };
        match self.commit_failure_settlement(&mut settlement, || {}) {
            Ok(()) => drop(gate),
            Err(Error::PendingAbort(root)) => {
                if nonblocking_abort && self.validator.is_some() {
                    *deferred = Some(DeferredSettlement::Failure(settlement));
                    return Err(Error::PendingAbort(root));
                }
                self.abort_failed_running(owner, root, parent.task.id(), gate)?;
            }
            Err(Error::Busy) if nonblocking_abort => {
                *deferred = Some(DeferredSettlement::Failure(settlement));
                return Err(Error::Busy);
            }
            Err(error) => {
                if nonblocking_abort {
                    *deferred = Some(DeferredSettlement::IndeterminateFailure(settlement));
                }
                return Err(error);
            }
        }
        self.poisoned
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(true)
    }
    fn reject_invalid_wait(
        &mut self,
        parent: &task_store::StoredTask,
        owner: &Arc<OwnedStore>,
        gate: std::sync::MutexGuard<'_, ()>,
        nonblocking_abort: bool,
        deferred: &mut Option<DeferredSettlement>,
    ) -> Result<bool, Error> {
        self.finish_running_failure(
            parent,
            "invalid task wait or ownership cycle".into(),
            owner,
            gate,
            nonblocking_abort,
            deferred,
        )
    }
    fn finish_failed(&self, parent: &task_store::StoredTask, reason: String) -> Result<(), Error> {
        self.finish_failed_inner(parent, reason, false)
    }
    fn finish_failed_inner(
        &self,
        parent: &task_store::StoredTask,
        reason: String,
        completed: bool,
    ) -> Result<(), Error> {
        self.finish_failed_inner_with(parent, reason, completed, || {})
    }

    fn finish_failed_inner_with(
        &self,
        parent: &task_store::StoredTask,
        reason: String,
        completed: bool,
        before_commit: impl FnMut(),
    ) -> Result<(), Error> {
        self.finish_failed_inner_with_pending(parent, reason, completed, completed, before_commit)
    }

    fn finish_failed_inner_with_pending(
        &self,
        parent: &task_store::StoredTask,
        reason: String,
        completed: bool,
        check_pending: bool,
        before_commit: impl FnMut(),
    ) -> Result<(), Error> {
        let mut settlement = FailureSettlement {
            attachment: self.attachment.clone(),
            running: parent.clone(),
            reason,
            completed,
            check_pending,
            attempt: 0,
            candidate: None,
        };
        self.commit_failure_settlement(&mut settlement, before_commit)
    }

    fn commit_failure_settlement(
        &self,
        settlement: &mut FailureSettlement,
        mut before_commit: impl FnMut(),
    ) -> Result<(), Error> {
        let store = self.ts();
        loop {
            let graph_version = store
                .store()
                .put_version()
                .map_err(task_store::Error::from)?;
            if self.has_prior_outcome(&self.owner(), &settlement.running.task)
                || !self.separate_from_failures(&self.owner(), &settlement.running.task)
            {
                return Err(Error::Busy);
            }
            if settlement.check_pending {
                let current = store
                    .load(settlement.running.task.id())?
                    .ok_or(Error::Ownership)?;
                if current.version != settlement.running.version {
                    if current.task.abort_requested() {
                        return Err(Error::PendingAbort(settlement.running.task.id()));
                    }
                    if current.task != settlement.running.task {
                        return Err(task_store::Error::Store(durable_store::Error::Conflict {
                            key: task_store::key(settlement.running.task.id()),
                        })
                        .into());
                    }
                    settlement.running = current;
                }
            }
            if settlement.check_pending
                && let Some(root) = pending_abort_root_for_running(
                    &store,
                    settlement.running.task.id(),
                    self.validator.is_none(),
                )?
            {
                return Err(Error::PendingAbort(root));
            }
            let records = store.list()?;
            let live = has_owned_work(store.store(), settlement.running.task.id(), &records, true)?;
            let mut next = settlement.running.clone();
            if live {
                next.task
                    .hold_failure(settlement.reason.clone())
                    .map_err(Error::Transition)?;
            } else {
                next.task
                    .fail(settlement.reason.clone())
                    .map_err(Error::Transition)?;
            }
            if settlement.completed {
                next.task
                    .record_completed_attempt(&settlement.running.task)
                    .map_err(Error::Transition)?;
            }
            let batch = store.prepare_save(&[Save::Update(&next)])?;
            settlement.candidate = Some(FailureCandidate {
                task: next,
                batch,
                graph_version,
            });
            let candidate = settlement
                .candidate
                .as_ref()
                .expect("candidate just prepared");
            debug_assert_eq!(candidate.task.task.id(), settlement.running.task.id());
            #[cfg(test)]
            if let Some(hook) = &self.failure_commit_hook {
                hook(settlement.attempt, store.store());
            }
            before_commit();
            #[cfg(test)]
            if self.inject_failure_commit_error == 1 {
                return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
            }
            let commit = store.store().commit_with_prefix_put_fence(
                &candidate.batch.checks,
                &candidate.batch.mutations,
                &[
                    task_store::PREFIX,
                    crate::ownership_edge::PREFIX,
                    b"runtime/conversation/",
                ],
                candidate.graph_version,
            );
            #[cfg(test)]
            if commit.is_ok() && self.inject_failure_commit_error == 2 {
                return Err(task_store::Error::Store(durable_store::Error::LockPoisoned).into());
            }
            match commit {
                Ok(()) => {
                    #[cfg(test)]
                    if let Some(hook) = &self.failure_committed_hook {
                        hook(settlement.attempt, store.store());
                    }
                    return Ok(());
                }
                Err(durable_store::Error::Conflict { ref key })
                    if settlement.attempt < 31
                        && (key == task_store::PREFIX
                            || key == crate::ownership_edge::PREFIX
                            || key == b"runtime/conversation/"
                            || (settlement.check_pending
                                && *key == task_store::key(settlement.running.task.id()))) =>
                {
                    let current = store.load(settlement.running.task.id())?;
                    if current.as_ref().map(|item| item.version) != Some(settlement.running.version)
                    {
                        if let Some(current) = current {
                            if settlement.check_pending && current.task.abort_requested() {
                                return Err(Error::PendingAbort(settlement.running.task.id()));
                            }
                            if settlement.check_pending && current.task == settlement.running.task {
                                settlement.running = current;
                            } else {
                                return Err(task_store::Error::Store(
                                    durable_store::Error::Conflict {
                                        key: task_store::key(settlement.running.task.id()),
                                    },
                                )
                                .into());
                            }
                        } else {
                            return Err(Error::Ownership);
                        }
                    }
                    settlement.attempt += 1;
                    continue;
                }
                Err(error) => return Err(task_store::Error::from(error).into()),
            }
        }
    }
}

pub(crate) fn failed_sibling_to_abort(
    ts: &TaskStore<'_>,
    parent: &task_store::StoredTask,
    ids: &[Uuid],
) -> Result<Option<Uuid>, Error> {
    let records = ts.list()?;
    let mut seen = HashSet::new();
    let mut failed = false;
    for id in ids {
        let child = records
            .iter()
            .find(|s| s.task.id() == *id)
            .ok_or(Error::Ownership)?;
        if !seen.insert(*id) || child.task.owner() != &TaskOwner::Task(parent.task.id()) {
            return Err(Error::Ownership);
        }
        failed |= matches!(
            child.task.state(),
            TaskState::Failed(_) | TaskState::CompletingFailed(_) | TaskState::Cancelled
        );
    }
    Ok(failed
        .then(|| {
            ids.iter().copied().find(|id| {
                records.iter().any(|record| {
                    record.task.id() == *id
                        && !record.task.is_terminal()
                        && !record.task.abort_requested()
                        && !matches!(record.task.state(), TaskState::CompletingFailed(_))
                })
            })
        })
        .flatten())
}

// Locate a pending cancellation root that owns this invocation. The caller holds
// the task gate and fences graph puts through the result commit.
pub(crate) fn pending_abort_root_for_running(
    ts: &TaskStore<'_>,
    running: Uuid,
    include_background: bool,
) -> Result<Option<Uuid>, Error> {
    let records = ts.list()?;
    let mut roots = Vec::new();
    for parent in &records {
        if let TaskState::Waiting(WaitFor::TasksFailFast(ids)) = parent.task.state()
            && !parent.task.abort_requested()
            && failed_sibling_to_abort(ts, parent, ids)?.is_some()
        {
            roots.extend(ids.iter().copied().filter(|id| {
                records.iter().any(|entry| {
                    entry.task.id() == *id
                        && !entry.task.is_terminal()
                        && !entry.task.abort_requested()
                        && !matches!(entry.task.state(), TaskState::CompletingFailed(_))
                })
            }));
        }
        if matches!(parent.task.state(), TaskState::CompletingFailed(_))
            && !parent.task.abort_requested()
        {
            roots.push(parent.task.id());
        }
        if parent.task.is_task_tool() && parent.task.is_completing() {
            roots.extend(records.iter().filter_map(|child| {
                (child.task.owner() == &TaskOwner::Task(parent.task.id())
                    && child.task.is_task_tool()
                    && !child.task.is_terminal()
                    && !child.task.abort_requested())
                .then_some(child.task.id())
            }));
        }
    }
    if roots.is_empty() {
        return Ok(None);
    }
    let graph = crate::ownership_edge::CleanupGraph::load(ts.store())
        .map_err(|error| Error::Handler(format!("ownership graph: {error}")))?;
    for root in roots {
        if graph.uncertain_task(root) {
            if root == running {
                return Ok(Some(root));
            }
            let running_task = records
                .iter()
                .find(|item| item.task.id() == running)
                .ok_or(Error::Ownership)?;
            // A valid foreground owner chain proves this invocation is outside
            // the uncertain root. Otherwise retain the outcome for revalidation.
            if include_background
                || !task_ancestors_available(ts.store(), &running_task.task, &records, |parent| {
                    parent.id() == root
                })
            {
                return Err(Error::Busy);
            }
            continue;
        }
        let mut pending = vec![root];
        let mut seen = HashSet::new();
        while let Some(task_id) = pending.pop() {
            if !seen.insert(task_id) {
                continue;
            }
            if task_id == running {
                return Ok(Some(root));
            }
            // Background task descendants are traversed only in the broader
            // mode and are not represented in the foreground uncertainty walk.
            if graph.uncertain_task(task_id) {
                return Err(Error::Busy);
            }
            let task = records
                .iter()
                .find(|entry| entry.task.id() == task_id)
                .ok_or(Error::Ownership)?;
            for child in &records {
                if child.task.owner() == &TaskOwner::Task(task_id)
                    && (include_background || !child.task.is_background())
                {
                    pending.push(child.task.id());
                }
            }
            for edge in graph.task_edges(task_id) {
                if edge.task_owner != *task.task.owner() {
                    return Err(Error::Ownership);
                }
                if edge.mode == crate::ownership_edge::Mode::Foreground {
                    for child in &records {
                        if child.task.owner() == &TaskOwner::Conversation(edge.child)
                            && (include_background || !child.task.is_background())
                        {
                            pending.push(child.task.id());
                        }
                    }
                }
            }
        }
    }
    Ok(None)
}

// Follow durable waits and live foreground ownership from every target. The
// caller fences graph puts between this scan and the waiting commit.
pub(crate) fn validate_wait_graph(
    store: &durable_store::Store,
    waiter: Uuid,
    targets: &[Uuid],
    new_children: &HashMap<Uuid, &Task>,
) -> Result<(), Error> {
    let tasks = TaskStore::new(store).list()?;
    let by_id: HashMap<_, _> = tasks
        .iter()
        .map(|item| (item.task.id(), &item.task))
        .collect();
    let edges = crate::ownership_edge::EdgeStore::new(store);
    let mut pending: Vec<_> = targets.iter().map(|id| (*id, false)).collect();
    let mut seen = HashSet::new();
    while let Some((id, through_ownership)) = pending.pop() {
        if id == waiter {
            return Err(Error::Ownership);
        }
        // A newly created task can acquire pre-existing orphan descendants;
        // inspect them too instead of assuming a fresh ID has no graph edges.
        let task = by_id
            .get(&id)
            .copied()
            .or_else(|| new_children.get(&id).copied())
            .ok_or(Error::Ownership)?;
        // A terminal wait target is already settled, but a terminal task
        // reached through ownership can lead to an owned conversation with
        // newly admitted live work.
        if task.is_terminal() && !through_ownership {
            continue;
        }
        if !seen.insert(id) {
            continue;
        }
        if !task.is_terminal()
            && !task.abort_requested()
            && let TaskState::Waiting(
                WaitFor::Tasks(on) | WaitFor::TasksAny(on) | WaitFor::TasksFailFast(on),
            ) = task.state()
        {
            pending.extend(on.iter().map(|id| (*id, false)));
        }
        pending.extend(tasks.iter().filter_map(|child| {
            (child.task.owner() == &TaskOwner::Task(id) && !child.task.is_background())
                .then_some((child.task.id(), true))
        }));
        pending.extend(new_children.values().filter_map(|child| {
            (child.owner() == &TaskOwner::Task(id) && !child.is_background())
                .then_some((child.id(), true))
        }));
        for edge in edges
            .children(id)
            .map_err(|e| Error::Handler(format!("ownership edges: {e}")))?
        {
            if edge.edge.task_owner != *task.owner() {
                return Err(Error::Ownership);
            }
            if edge.edge.mode == crate::ownership_edge::Mode::Foreground {
                pending.extend(tasks.iter().filter_map(|child| {
                    (child.task.owner() == &TaskOwner::Conversation(edge.edge.child)
                        && !child.task.is_background())
                    .then_some((child.task.id(), true))
                }));
            }
        }
    }
    Ok(())
}

pub(crate) type MaintenanceRoots = (Option<Uuid>, Option<Uuid>);

pub(crate) fn resolve_maintenance(
    ts: &TaskStore<'_>,
    intent: MaintenanceIntent,
) -> Result<Option<MaintenanceRoots>, Error> {
    let records = ts.list()?;
    let id = match intent {
        MaintenanceIntent::Nested(id)
        | MaintenanceIntent::Failed(id)
        | MaintenanceIntent::FailFast(id) => id,
    };
    let Some(parent) = records.iter().find(|item| item.task.id() == id) else {
        return Ok(None);
    };
    match intent {
        MaintenanceIntent::Nested(_)
            if parent.task.is_task_tool()
                && parent.task.is_completing()
                && has_unmarked_nested(id, &records) =>
        {
            Ok(Some((None, Some(id))))
        }
        MaintenanceIntent::Failed(_)
            if !parent.task.abort_requested()
                && matches!(parent.task.state(), TaskState::CompletingFailed(_))
                && !owned_graph_uncertain(ts.store(), id)?
                && has_owned_work(ts.store(), id, &records, true)? =>
        {
            Ok(Some((Some(id), None)))
        }
        MaintenanceIntent::FailFast(_) if !parent.task.abort_requested() => {
            if let TaskState::Waiting(WaitFor::TasksFailFast(ids)) = parent.task.state() {
                Ok(failed_sibling_to_abort(ts, parent, ids)?.map(|root| (Some(root), None)))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

// A TaskTool may own unrelated generic tasks; only TaskTool children are
// cancelled by normal tool settlement. All roots are marked in one commit.
fn has_unmarked_nested(parent: Uuid, tasks: &[task_store::StoredTask]) -> bool {
    tasks.iter().any(|child| {
        child.task.owner() == &TaskOwner::Task(parent)
            && child.task.is_task_tool()
            && !child.task.is_terminal()
            && !child.task.abort_requested()
    })
}

fn owned_graph_uncertain(store: &durable_store::Store, root: Uuid) -> Result<bool, Error> {
    Ok(crate::ownership_edge::CleanupGraph::load(store)
        .map_err(|error| Error::Handler(format!("ownership graph: {error}")))?
        .uncertain_task(root))
}

// Walk through terminal tasks and owned conversations as well: work may be
// admitted in an owned conversation after its original task has finished.
fn has_live_owned(
    store: &durable_store::Store,
    root: Uuid,
    tasks: &[task_store::StoredTask],
) -> Result<bool, Error> {
    has_owned_work(store, root, tasks, false)
}

// Before a failure is marked, queued abortable inputs require a cascade even
// when their conversation has no runnable work. Kept inputs never hold drain.
fn has_owned_work(
    store: &durable_store::Store,
    root: Uuid,
    tasks: &[task_store::StoredTask],
    include_abortable: bool,
) -> Result<bool, Error> {
    let graph = crate::ownership_edge::CleanupGraph::load(store)
        .map_err(|e| Error::Handler(format!("ownership graph: {e}")))?;
    let failure_cleanup = tasks.iter().any(|record| {
        record.task.id() == root && matches!(record.task.state(), TaskState::CompletingFailed(_))
    });
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        for child in tasks
            .iter()
            .filter(|item| item.task.owner() == &TaskOwner::Task(id) && !item.task.is_background())
        {
            if !child.task.is_terminal() {
                return Ok(true);
            }
            pending.push(child.task.id());
        }
        if graph.uncertain_task(id) {
            return Ok(true);
        }
        for edge in graph.task_edges(id) {
            let parent = tasks
                .iter()
                .find(|item| item.task.id() == id)
                .ok_or(Error::Ownership)?;
            if edge.task_owner != *parent.task.owner() {
                return Err(Error::Ownership);
            }
            if edge.mode != crate::ownership_edge::Mode::Foreground {
                continue;
            }
            // An aborting conversation is still being drained by the harness.
            let key = format!("runtime/conversation/{}", edge.child);
            let record = store.get(key.as_bytes()).map_err(task_store::Error::from)?;
            let Some(conversation) = record.and_then(|entry| {
                serde_json::from_slice::<crate::runtime::Conversation>(&entry.value).ok()
            }) else {
                return Ok(true);
            };
            if conversation.id != edge.child {
                return Ok(true);
            }
            if conversation.abort_requested
                || if failure_cleanup {
                    conversation.is_failure_cleanup_busy()
                } else {
                    conversation.is_idle_wait_busy()
                }
                || (include_abortable && conversation.has_abortable_queued_inputs())
            {
                return Ok(true);
            }
            for child in tasks.iter().filter(|item| {
                item.task.owner() == &TaskOwner::Conversation(edge.child)
                    && !item.task.is_background()
            }) {
                if !child.task.is_terminal() {
                    return Ok(true);
                }
                pending.push(child.task.id());
            }
        }
    }
    Ok(false)
}

/// Mark the entire existing subtree atomically. Checking every live record also
/// fences a concurrent handler trying to publish a child under that record.
pub(crate) fn request_abort_subtree(ts: &TaskStore<'_>, id: Uuid) -> Result<(), Error> {
    request_abort_subtree_with(ts, id, || {})
}

fn request_abort_subtree_with(
    ts: &TaskStore<'_>,
    id: Uuid,
    before_commit: impl FnMut(),
) -> Result<(), Error> {
    request_abort_roots_with(ts, id, false, before_commit)
}

fn request_abort_nested_tools(ts: &TaskStore<'_>, parent: Uuid) -> Result<(), Error> {
    request_abort_roots_with(ts, parent, true, || {})
}

fn request_abort_roots_with(
    ts: &TaskStore<'_>,
    id: Uuid,
    nested: bool,
    mut before_commit: impl FnMut(),
) -> Result<(), Error> {
    loop {
        let graph_version = ts.store().put_version().map_err(task_store::Error::from)?;
        let records = ts.list()?;
        let root = records
            .iter()
            .find(|s| s.task.id() == id)
            .ok_or(Error::Ownership)?;
        if root.task.is_terminal()
            || (nested && (!root.task.is_task_tool() || !root.task.is_completing()))
        {
            return Err(Error::Transition(TransitionError::InvalidState));
        }
        let mut ids = if nested {
            records
                .iter()
                .filter_map(|record| {
                    (record.task.owner() == &TaskOwner::Task(id)
                        && record.task.is_task_tool()
                        && !record.task.is_terminal())
                    .then_some(record.task.id())
                })
                .collect()
        } else {
            HashSet::from([id])
        };
        loop {
            let before = ids.len();
            for record in &records {
                if let TaskOwner::Task(parent) = record.task.owner()
                    && ids.contains(parent)
                {
                    ids.insert(record.task.id());
                }
            }
            if ids.len() == before {
                break;
            }
        }
        if crate::ownership_edge::reject_foreground_children(
            ts.store(),
            records
                .iter()
                .filter(|record| ids.contains(&record.task.id()))
                .map(|record| record.task.id()),
        )
        .map_err(|e| Error::Handler(format!("ownership edges: {e}")))?
        {
            return Err(Error::ForegroundAbortUnsupported);
        }
        let mut updates = Vec::new();
        for record in &records {
            if ids.contains(&record.task.id())
                && !record.task.is_terminal()
                && !record.task.abort_requested()
            {
                let mut next = record.clone();
                next.task.request_abort().map_err(Error::Transition)?;
                updates.push(next);
            }
        }
        let mut batch = ts.prepare_save(&updates.iter().map(Save::Update).collect::<Vec<_>>())?;
        for record in &records {
            if ids.contains(&record.task.id())
                && (record.task.is_terminal() || record.task.abort_requested())
            {
                batch.checks.push(durable_store::Check {
                    key: task_store::key(record.task.id()),
                    expected: durable_store::Expected::Version(record.version),
                });
            }
        }
        for task_id in &ids {
            let (check, mutation) =
                crate::ownership_edge::prepare_abort_fence(ts.store(), &TaskOwner::Task(*task_id))
                    .map_err(task_store::Error::from)?;
            batch.checks.push(check);
            batch.mutations.push(mutation);
        }
        before_commit();
        match ts
            .store()
            .commit_with_prefix_put_fence(
                &batch.checks,
                &batch.mutations,
                &[task_store::PREFIX, crate::ownership_edge::PREFIX],
                graph_version,
            )
            .map_err(task_store::Error::from)
        {
            Ok(()) => return Ok(()),
            Err(task_store::Error::Store(durable_store::Error::Conflict { .. })) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
#[path = "task_document_tests.rs"]
mod task_document_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn cp(phase: &str) -> Checkpoint {
        Checkpoint {
            phase: phase.into(),
            data: json!(null),
        }
    }

    #[test]
    fn handler_admission_rejects_replaced_attempt_or_executor() {
        use std::{sync::mpsc, time::Duration};
        struct Parent {
            child: Arc<crate::task_tool::TaskTool>,
            entered: mpsc::Sender<()>,
            resume: Mutex<mpsc::Receiver<()>>,
            rejected: mpsc::Sender<bool>,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_admission(
                &self,
                _: &Task,
                _: &mut crate::document::TaskDocuments<'_>,
                admission: &AdmissionHandle<'_>,
            ) -> Result<Step, String> {
                self.entered.send(()).unwrap();
                self.resume.lock().unwrap().recv().unwrap();
                self.rejected
                    .send(matches!(
                        admission.admit_task_tool_child(&self.child, "key", json!({})),
                        Err(Error::Ownership)
                    ))
                    .unwrap();
                Ok(Step::Succeed(json!(null)))
            }
        }
        for replace_token in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let child_handler: Arc<dyn Handler> =
                Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
            let child = Arc::new(crate::task_tool::TaskTool::new(
                pi_rs_core::ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                },
                "child",
                true,
                1,
                child_handler.clone(),
            ));
            let (entered_tx, entered_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let (rejected_tx, rejected_rx) = mpsc::channel();
            let parent_handler: Arc<dyn Handler> = Arc::new(Parent {
                child,
                entered: entered_tx,
                resume: Mutex::new(resume_rx),
                rejected: rejected_tx,
            });
            let mut registry = Registry::default();
            registry.register_shared("parent", true, parent_handler);
            registry.register_shared("child", true, child_handler);
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                4,
            )
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let invocation = async_admission_reserve(&mut scheduler);
            let old_attempt = invocation.running.task.attempt_id();
            let worker = std::thread::spawn(move || invocation.execute().unwrap());
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            if replace_token {
                scheduler
                    .live_invocations
                    .lock()
                    .unwrap()
                    .leases
                    .insert(parent.id(), (Uuid::new_v4(), old_attempt));
            } else {
                let current = scheduler.ts().load(parent.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&current, |task| task.recover())
                    .unwrap();
                let ready = scheduler.ts().load(parent.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&ready, |task| task.start_attempt(Uuid::new_v4()))
                    .unwrap();
            }
            resume_tx.send(()).unwrap();
            assert!(rejected_rx.recv_timeout(Duration::from_secs(5)).unwrap());
            assert_eq!(scheduler.ts().list().unwrap().len(), 1);
            drop(worker.join().unwrap());
        }
    }

    fn async_admission_setup(
        path: &std::path::Path,
    ) -> (Scheduler, Arc<crate::task_tool::TaskTool>) {
        let handler: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
        let child = Arc::new(crate::task_tool::TaskTool::new(
            pi_rs_core::ToolSpec {
                name: "child".into(),
                description: "".into(),
                parameters: json!({"type":"object"}),
            },
            "child",
            true,
            1,
            handler.clone(),
        ));
        let mut registry = Registry::default();
        assert!(registry.register_shared("parent", true, handler.clone()));
        assert!(registry.register_shared("child", true, handler));
        (Scheduler::open(path, registry).unwrap(), child)
    }

    fn async_admission_reserve(scheduler: &mut Scheduler) -> Box<DetachedInvocation> {
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("expected invocation")
        };
        invocation
    }

    #[test]
    fn async_admission_rejects_running_generic_parent() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        assert!(matches!(
            invocation.admit_task_tool_child(&child, "key", json!({})),
            Err(Error::Ownership)
        ));
        assert_eq!(scheduler.ts().list().unwrap().len(), 1);
    }

    #[test]
    fn async_admission_child_runs_while_task_tool_parent_is_live() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        let child_id = invocation
            .admit_task_tool_child(&child, "key", json!({}))
            .unwrap();
        let child_invocation = async_admission_reserve(&mut scheduler);
        assert_eq!(child_invocation.task_id(), child_id);
        assert!(
            scheduler
                .settle_invocation(child_invocation.execute().unwrap())
                .unwrap()
        );
        assert_eq!(
            scheduler.get(child_id).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!(null))
        );
        assert_eq!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        drop(invocation);
    }

    #[test]
    fn async_admission_rejects_retired_lease() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let mut invocation = async_admission_reserve(&mut scheduler);
        invocation.lease.retired = true;
        assert!(matches!(
            invocation.admit_task_tool_child(&child, "key", json!({})),
            Err(Error::Ownership)
        ));
        assert_eq!(scheduler.ts().list().unwrap().len(), 1);
    }

    #[test]
    fn ancestor_result_publication_fences_live_descendant_admission() {
        use std::{sync::mpsc, time::Duration};
        for admission_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (returning_tx, returning_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let release_rx = Mutex::new(release_rx);
            let parent_handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                returning_tx.send(()).unwrap();
                Ok(Step::Fail("parent failure".into()))
            });
            let child_handler: Arc<dyn Handler> =
                Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
            let child = crate::task_tool::TaskTool::new(
                pi_rs_core::ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                },
                "child",
                true,
                1,
                child_handler.clone(),
            );
            let mut registry = Registry::default();
            registry.register_shared("parent", true, parent_handler);
            registry.register_shared("child", true, child_handler);
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                4,
            )
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let parent_invocation = async_admission_reserve(&mut scheduler);
            let child_id = parent_invocation
                .admit_task_tool_child(&child, "child", json!({}))
                .unwrap();
            let child_invocation = async_admission_reserve(&mut scheduler);
            assert_eq!(child_invocation.task_id(), child_id);
            let worker = std::thread::spawn(move || parent_invocation.execute().unwrap());
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let owner = scheduler.owner();
            let (completed, admitted) = if admission_first {
                // Hold the publisher at its fence while the real handler returns;
                // admission must commit before publication can acquire the fence.
                let publication_gate = scheduler.failure_fence.lock().unwrap();
                release_tx.send(()).unwrap();
                returning_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let id = child_invocation
                    .admit_task_tool_child(&child, "racing", json!({}))
                    .expect("admission before publication must succeed");
                assert!(scheduler.ts().load(id).unwrap().is_some());
                drop(publication_gate);
                (worker.join().unwrap(), Some(id))
            } else {
                // Hold admission at the task gate until the handler's result has
                // actually been published, while its durable task is still Running.
                let task_gate = owner.task_gate().lock().unwrap();
                std::thread::scope(|scope| {
                    let (started_tx, started_rx) = mpsc::channel();
                    let child_invocation = &child_invocation;
                    let child = &child;
                    let admission = scope.spawn(move || {
                        started_tx.send(()).unwrap();
                        child_invocation.admit_task_tool_child(child, "racing", json!({}))
                    });
                    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    release_tx.send(()).unwrap();
                    returning_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    let completed = worker.join().unwrap();
                    assert_eq!(
                        scheduler
                            .ts()
                            .load(parent.id())
                            .unwrap()
                            .unwrap()
                            .task
                            .state(),
                        &TaskState::Running
                    );
                    drop(task_gate);
                    assert!(
                        matches!(admission.join().unwrap(), Err(Error::Ownership)),
                        "admission after publication must be rejected"
                    );
                    (completed, None)
                })
            };
            assert!(matches!(
                child_invocation.admit_task_tool_child(&child, "after", json!({})),
                Err(Error::Ownership)
            ));
            assert_eq!(
                scheduler.ts().list().unwrap().len(),
                if admitted.is_some() { 3 } else { 2 }
            );
            drop(completed);
        }
    }

    #[test]
    fn conversation_root_abort_races_admission_under_real_graph_fence() {
        use crate::recovery::UncertainPolicy;
        use crate::runtime::{Config, Harness, Options};
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        struct Stub;
        #[async_trait::async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        for order in 0..12 {
            let dir = tempfile::tempdir().unwrap();
            let mut harness = Harness::open_paused(
                dir.path().join("db"),
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: UncertainPolicy::RequireReconciliation,
                },
            )
            .unwrap();
            let conversation = harness
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: "".into(),
                    max_output_tokens: 10,
                })
                .unwrap();
            let handler: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
            let child = crate::task_tool::TaskTool::new(
                pi_rs_core::ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                },
                "child",
                true,
                1,
                handler.clone(),
            );
            let mut registry = Registry::default();
            registry.register_shared("parent", true, handler.clone());
            registry.register_shared("child", true, handler);
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let parent =
                task(1, TaskOwner::Conversation(conversation), "parent", true, 4).into_task_tool();
            scheduler.submit(&parent).unwrap();
            let invocation = async_admission_reserve(&mut scheduler);
            let owner = scheduler.owner();
            let mut abort = || {
                owner
                    .conversation_controls()
                    .fence(|| harness.abort_owned_graph(Some(conversation), None))
                    .unwrap();
            };
            let admitted = match order {
                0 => {
                    let result = invocation.admit_task_tool_child(&child, "race", json!({}));
                    assert!(result.is_ok());
                    abort();
                    result
                }
                1 => {
                    abort();
                    let result = invocation.admit_task_tool_child(&child, "race", json!({}));
                    assert!(result.is_err());
                    result
                }
                _ => {
                    let barrier = std::sync::Barrier::new(2);
                    std::thread::scope(|scope| {
                        let worker = scope.spawn(|| {
                            barrier.wait();
                            invocation.admit_task_tool_child(&child, "race", json!({}))
                        });
                        barrier.wait();
                        abort();
                        worker.join().unwrap()
                    })
                }
            };
            assert!(
                scheduler
                    .ts()
                    .load(parent.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
            );
            if let Ok(id) = admitted {
                assert!(
                    scheduler
                        .ts()
                        .load(id)
                        .unwrap()
                        .unwrap()
                        .task
                        .abort_requested()
                );
            } else {
                assert_eq!(scheduler.ts().list().unwrap().len(), 1);
            }
            assert!(
                invocation
                    .admit_task_tool_child(&child, "after", json!({}))
                    .is_err()
            );
        }
    }

    #[test]
    fn async_admission_reconnect_concurrent_and_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        let ids = std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                invocation
                    .admit_task_tool_child(&child, "key", json!({"x":1}))
                    .unwrap()
            });
            let b = scope.spawn(|| {
                invocation
                    .admit_task_tool_child(&child, "key", json!({"x":1}))
                    .unwrap()
            });
            (a.join().unwrap(), b.join().unwrap())
        });
        assert_eq!(ids.0, ids.1);
        // A graph-only rewrite advances the stored version without replacing
        // the attempt or executor; reconnect and another admission remain valid.
        let snapshot = scheduler.ts().load(parent.id()).unwrap().unwrap();
        let parent_key = task_store::key(parent.id());
        scheduler
            .owner()
            .store()
            .commit(
                &[durable_store::Check {
                    key: parent_key.clone(),
                    expected: durable_store::Expected::Version(snapshot.version),
                }],
                &[durable_store::Mutation::Put {
                    key: parent_key,
                    value: serde_json::to_vec(&snapshot.task).unwrap(),
                }],
            )
            .unwrap();
        assert_eq!(
            invocation
                .admit_task_tool_child(&child, "key", json!({"x":1}))
                .unwrap(),
            ids.0
        );
        assert!(
            invocation
                .admit_task_tool_child(&child, "key", json!({"x":2}))
                .is_err()
        );
        assert_eq!(scheduler.ts().list().unwrap().len(), 2);
        let current = scheduler.ts().load(parent.id()).unwrap().unwrap();
        scheduler
            .ts()
            .update(&current, |t| t.request_abort())
            .unwrap();
        assert!(
            invocation
                .admit_task_tool_child(&child, "another", json!({}))
                .is_err()
        );
        assert_eq!(scheduler.ts().list().unwrap().len(), 2);
    }

    #[test]
    fn async_admission_rejects_old_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        let current = scheduler.ts().load(parent.id()).unwrap().unwrap();
        scheduler.ts().update(&current, |t| t.recover()).unwrap();
        let ready = scheduler.ts().load(parent.id()).unwrap().unwrap();
        scheduler
            .ts()
            .update(&ready, |t| t.start_attempt(Uuid::new_v4()))
            .unwrap();
        assert!(
            invocation
                .admit_task_tool_child(&child, "key", json!({}))
                .is_err()
        );
        assert_eq!(scheduler.ts().list().unwrap().len(), 1);
    }

    #[test]
    fn async_admission_rejects_retired_executor_even_with_same_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        let attempt = invocation.running.task.attempt_id();
        // Model an old worker retaining its snapshot after ownership moved to
        // another executor, including the ABA case where the durable attempt
        // record was not changed. Its lease token must not authorize writes.
        invocation
            .lease
            .state
            .lock()
            .unwrap()
            .leases
            .insert(parent.id(), (Uuid::new_v4(), attempt));
        assert!(
            invocation
                .admit_task_tool_child(&child, "key", json!({}))
                .is_err()
        );
        assert_eq!(scheduler.ts().list().unwrap().len(), 1);
    }

    #[test]
    fn async_admission_races_subtree_abort() {
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                4,
            )
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let invocation = async_admission_reserve(&mut scheduler);
            let owner = scheduler.owner();
            let barrier = std::sync::Barrier::new(2);
            let result = std::thread::scope(|scope| {
                let admission = scope.spawn(|| {
                    barrier.wait();
                    invocation.admit_task_tool_child(&child, "key", json!({}))
                });
                barrier.wait();
                let _gate = owner.task_gate().lock().unwrap();
                request_abort_subtree(&TaskStore::new(owner.store()), parent.id()).unwrap();
                drop(_gate);
                admission.join().unwrap()
            });
            let records = scheduler.ts().list().unwrap();
            assert!(
                records
                    .iter()
                    .find(|r| r.task.id() == parent.id())
                    .unwrap()
                    .task
                    .abort_requested()
            );
            if let Ok(id) = result {
                assert!(
                    records
                        .iter()
                        .find(|r| r.task.id() == id)
                        .unwrap()
                        .task
                        .abort_requested()
                );
            } else {
                assert_eq!(records.len(), 1);
            }
        }
    }

    #[test]
    fn async_admission_parent_completion_cleans_child() {
        let dir = tempfile::tempdir().unwrap();
        let (mut scheduler, child) = async_admission_setup(&dir.path().join("db"));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        let child_id = invocation
            .admit_task_tool_child(&child, "key", json!({}))
            .unwrap();
        assert!(
            scheduler
                .settle_invocation(invocation.execute().unwrap())
                .unwrap()
        );
        assert!(scheduler.tick(0).unwrap());
        assert!(
            scheduler
                .ts()
                .load(child_id)
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn async_admission_crash_worker() {
        let Some(path) = std::env::var_os("PI_ASYNC_ADMISSION_CRASH_DB") else {
            return;
        };
        let (mut scheduler, child) = async_admission_setup(std::path::Path::new(&path));
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let invocation = async_admission_reserve(&mut scheduler);
        if std::env::var_os("PI_ASYNC_ADMISSION_BEFORE").is_some() {
            std::process::exit(78);
        }
        let id = invocation
            .admit_task_tool_child(&child, "key", json!({"x":1}))
            .unwrap();
        std::fs::write(
            std::path::Path::new(&path).with_extension("child"),
            id.to_string(),
        )
        .unwrap();
        std::process::exit(79);
    }

    #[test]
    fn async_admission_reconnects_after_process_crash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_scheduler::tests::async_admission_crash_worker",
            ])
            .env("PI_ASYNC_ADMISSION_CRASH_DB", &path)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(79));
        let original =
            Uuid::parse_str(&std::fs::read_to_string(path.with_extension("child")).unwrap())
                .unwrap();
        let (mut scheduler, child) = async_admission_setup(&path);
        let invocation = async_admission_reserve(&mut scheduler);
        assert_eq!(
            invocation
                .admit_task_tool_child(&child, "key", json!({"x":1}))
                .unwrap(),
            original
        );
        assert_eq!(scheduler.ts().list().unwrap().len(), 2);
    }

    #[test]
    fn async_admission_recovers_crash_before_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_scheduler::tests::async_admission_crash_worker",
            ])
            .env("PI_ASYNC_ADMISSION_CRASH_DB", &path)
            .env("PI_ASYNC_ADMISSION_BEFORE", "1")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(78));
        let (mut scheduler, child) = async_admission_setup(&path);
        assert_eq!(scheduler.ts().list().unwrap().len(), 1);
        let invocation = async_admission_reserve(&mut scheduler);
        invocation
            .admit_task_tool_child(&child, "key", json!({"x":1}))
            .unwrap();
        assert_eq!(scheduler.ts().list().unwrap().len(), 2);
    }

    #[test]
    fn exhausted_notification_revision_conflicts_follow_replay_policy() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{
            Config, Harness, Options, SubmissionNotify, submission_settled_signal,
        };
        use crate::task_driver::Driver;
        use crate::task_store::{Save, SignalResult, SignalToken, TaskStore};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Write {
            target: Uuid,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Write {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.submit_write(
                    "exhausted".into(),
                    "app.note".into(),
                    None,
                    Some(SubmissionNotify {
                        task_id: self.target,
                        key: "settled".into(),
                    }),
                )
                .map_err(|e| e.to_string())?;
                let address = Address {
                    scope: "test".into(),
                    name: "exhaustion".into(),
                    key: "".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(&address, json!({"done":true}).as_object().unwrap().clone())
                    .map_err(|e| e.to_string())?;
                Ok(Step::Succeed(json!(null)))
            }
        }
        for replay_safe in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(
                &path,
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            let conversation = harness
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: String::new(),
                    max_output_tokens: 10,
                })
                .unwrap();
            let target = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("target"),
                3,
            )
            .unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            let tasks = TaskStore::new(&store);
            tasks.save(&[Save::Create(&target)]).unwrap();
            tasks
                .reserve(&tasks.load(target.id()).unwrap().unwrap())
                .unwrap();
            tasks
                .update(&tasks.load(target.id()).unwrap().unwrap(), |task| {
                    task.wait(WaitFor::TypedSleep {
                        until_millis: None,
                        signals: vec![submission_settled_signal().key("settled").unwrap()],
                    })
                })
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "writer",
                replay_safe,
                Write {
                    target: target.id(),
                    calls: calls.clone(),
                },
            );
            registry.register("probe", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let writer = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("writer"),
                3,
            )
            .unwrap()
            .registered("writer".into(), replay_safe);
            let commits = Arc::new(AtomicUsize::new(0));
            let observed = commits.clone();
            let target_id = target.id();
            scheduler.set_result_commit_hook(Arc::new(move |_, store| {
                let attempt = observed.fetch_add(1, Ordering::SeqCst);
                if attempt < 4 {
                    assert_eq!(
                        TaskStore::new(store)
                            .deliver_signal_at(
                                target_id,
                                &SignalToken::new("concurrent", 1).unwrap(),
                                &format!("key-{attempt}"),
                                json!(attempt),
                                0
                            )
                            .unwrap(),
                        SignalResult::Delivered
                    );
                }
            }));
            let driver = Driver::start(scheduler);
            driver.submit(&writer).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let final_task = loop {
                let task = driver
                    .get(writer.id())
                    .expect("driver stopped after notification conflicts")
                    .unwrap();
                if task.is_terminal() {
                    break task;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "writer did not settle"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            let address = Address {
                scope: "test".into(),
                name: "exhaustion".into(),
                key: "".into(),
            };
            let document = Documents::new(&store).get(&address).unwrap();
            let notification = tasks
                .read_typed_signal(target.id(), &submission_settled_signal(), "settled")
                .unwrap();
            if replay_safe {
                assert!(matches!(final_task.state(), TaskState::Succeeded(_)));
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                assert!(document.is_some());
                assert!(notification.is_some());
                assert_eq!(
                    harness
                        .conversation(conversation)
                        .unwrap()
                        .application_writes
                        .len(),
                    1
                );
            } else {
                assert!(matches!(final_task.state(), TaskState::Failed(_)));
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert!(document.is_none());
                assert!(notification.is_none());
                assert!(
                    harness
                        .conversation(conversation)
                        .unwrap()
                        .application_writes
                        .is_empty()
                );
            }
            for index in 0..4 {
                assert_eq!(
                    tasks
                        .read_typed_signal(
                            target.id(),
                            &SignalToken::new("concurrent", 1).unwrap(),
                            format!("key-{index}")
                        )
                        .unwrap(),
                    Some(json!(index))
                );
            }
            let probe = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("probe"),
                3,
            )
            .unwrap()
            .registered("probe".into(), true);
            driver.submit(&probe).expect("driver must remain usable");
            loop {
                let task = driver.get(probe.id()).unwrap().unwrap();
                if task.is_terminal() {
                    assert!(matches!(task.state(), TaskState::Succeeded(_)));
                    break;
                }
                assert!(std::time::Instant::now() < deadline, "probe did not settle");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            driver.shutdown().unwrap();
        }
    }

    #[test]
    fn task_notification_revision_race_preserves_original_signal_fences() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{
            Config, Harness, Options, SubmissionNotify, submission_settled_signal,
        };
        use crate::task_store::{Save, SignalResult, SignalToken, TaskStore};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Write {
            target: Uuid,
            original_signal: bool,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Write {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.submit_write(
                    "revision-write".into(),
                    "app.note".into(),
                    None,
                    Some(SubmissionNotify {
                        task_id: self.target,
                        key: "settled".into(),
                    }),
                )
                .map_err(|e| e.to_string())?;
                if self.original_signal {
                    docs.signal_at(
                        self.target,
                        &SignalToken::new("original", 1).unwrap(),
                        "key",
                        json!(1),
                        0,
                    )
                    .map_err(|e| e.to_string())?;
                }
                let address = Address {
                    scope: "test".into(),
                    name: "revision".into(),
                    key: "".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(&address, json!({"done":true}).as_object().unwrap().clone())
                    .map_err(|e| e.to_string())?;
                Ok(Step::Succeed(json!(null)))
            }
        }
        for original_signal in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(
                &path,
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            let conversation = harness
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: String::new(),
                    max_output_tokens: 10,
                })
                .unwrap();
            let target = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("target"),
                3,
            )
            .unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            let tasks = TaskStore::new(&store);
            tasks.save(&[Save::Create(&target)]).unwrap();
            tasks
                .reserve(&tasks.load(target.id()).unwrap().unwrap())
                .unwrap();
            tasks
                .update(&tasks.load(target.id()).unwrap().unwrap(), |task| {
                    task.wait(WaitFor::TypedSleep {
                        until_millis: None,
                        signals: vec![submission_settled_signal().key("settled").unwrap()],
                    })
                })
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "revision-write",
                false,
                Write {
                    target: target.id(),
                    original_signal,
                    calls: calls.clone(),
                },
            );
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let writer = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("writer"),
                3,
            )
            .unwrap()
            .registered("revision-write".into(), false);
            scheduler.submit(&writer).unwrap();
            let commits = Arc::new(AtomicUsize::new(0));
            let observed = commits.clone();
            let target_id = target.id();
            scheduler.set_result_commit_hook(Arc::new(move |_, store| {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert_eq!(
                        TaskStore::new(store)
                            .deliver_signal_at(
                                target_id,
                                &SignalToken::new("concurrent", 1).unwrap(),
                                "key",
                                json!(7),
                                0
                            )
                            .unwrap(),
                        SignalResult::Delivered
                    );
                }
            }));
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let current = scheduler.get(writer.id()).unwrap().unwrap();
            let address = Address {
                scope: "test".into(),
                name: "revision".into(),
                key: "".into(),
            };
            let document = Documents::new(&store).get(&address).unwrap();
            let notification = tasks
                .read_typed_signal(target.id(), &submission_settled_signal(), "settled")
                .unwrap();
            assert_eq!(
                tasks
                    .read_typed_signal(
                        target.id(),
                        &SignalToken::new("concurrent", 1).unwrap(),
                        "key"
                    )
                    .unwrap(),
                Some(json!(7))
            );
            if original_signal {
                assert!(matches!(current.state(), TaskState::Failed(_)));
                assert!(document.is_none());
                assert!(notification.is_none());
                assert!(
                    tasks
                        .read_typed_signal(
                            target.id(),
                            &SignalToken::new("original", 1).unwrap(),
                            "key"
                        )
                        .unwrap()
                        .is_none()
                );
                assert!(
                    harness
                        .conversation(conversation)
                        .unwrap()
                        .application_writes
                        .is_empty()
                );
                assert_eq!(commits.load(Ordering::SeqCst), 1);
            } else {
                assert!(matches!(current.state(), TaskState::Succeeded(_)));
                assert!(document.is_some());
                assert!(notification.is_some());
                assert_eq!(
                    harness
                        .conversation(conversation)
                        .unwrap()
                        .application_writes
                        .len(),
                    1
                );
                assert_eq!(commits.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[test]
    fn task_write_notification_deadline_reassembles_without_weakening_original_fence() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{
            Config, Harness, Options, SubmissionNotify, submission_settled_signal,
        };
        use crate::task_store::{Save, SignalState, SignalToken, TaskStore};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Write {
            target: Uuid,
            original_deadline: bool,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Write {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.submit_write(
                    "deadline-write".into(),
                    "app.note".into(),
                    None,
                    Some(SubmissionNotify {
                        task_id: self.target,
                        key: "settled".into(),
                    }),
                )
                .map_err(|e| e.to_string())?;
                if self.original_deadline {
                    docs.signal_at(
                        self.target,
                        &SignalToken::new("other", 1).unwrap(),
                        "other",
                        json!(1),
                        0,
                    )
                    .map_err(|e| e.to_string())?;
                }
                let address = Address {
                    scope: "test".into(),
                    name: "deadline".into(),
                    key: "".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(&address, json!({"done":true}).as_object().unwrap().clone())
                    .map_err(|e| e.to_string())?;
                Ok(Step::Succeed(json!(null)))
            }
        }
        for original_deadline in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(
                &path,
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            harness.set_signal_clock(Arc::new(|| 0));
            let conversation = harness
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: String::new(),
                    max_output_tokens: 10,
                })
                .unwrap();
            let target = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("target"),
                3,
            )
            .unwrap();
            let store = durable_store::Store::open(&path).unwrap();
            let tasks = TaskStore::new(&store);
            tasks.save(&[Save::Create(&target)]).unwrap();
            tasks
                .reserve(&tasks.load(target.id()).unwrap().unwrap())
                .unwrap();
            tasks
                .update(&tasks.load(target.id()).unwrap().unwrap(), |task| {
                    task.wait(WaitFor::TypedSleep {
                        until_millis: Some(20),
                        signals: vec![
                            submission_settled_signal().key("settled").unwrap(),
                            SignalToken::new("other", 1).unwrap().key("other").unwrap(),
                        ],
                    })
                })
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "deadline-write",
                false,
                Write {
                    target: target.id(),
                    original_deadline,
                    calls: calls.clone(),
                },
            );
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let writer = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                cp("writer"),
                3,
            )
            .unwrap()
            .registered("deadline-write".into(), false);
            scheduler.submit(&writer).unwrap();
            let commits = Arc::new(AtomicUsize::new(0));
            let observed = commits.clone();
            scheduler.set_result_commit_hook(Arc::new(move |_, _| {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(60));
                }
            }));
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let current = scheduler.get(writer.id()).unwrap().unwrap();
            let address = Address {
                scope: "test".into(),
                name: "deadline".into(),
                key: "".into(),
            };
            let document = Documents::new(&store).get(&address).unwrap();
            let signal = tasks
                .read_typed_signal_state(target.id(), &submission_settled_signal(), "settled")
                .unwrap();
            if original_deadline {
                assert!(matches!(current.state(), TaskState::Failed(_)));
                assert!(document.is_none());
                assert!(signal.is_none());
                assert!(
                    harness
                        .conversation(conversation)
                        .unwrap()
                        .application_writes
                        .is_empty()
                );
                assert_eq!(commits.load(Ordering::SeqCst), 1);
            } else {
                assert!(matches!(current.state(), TaskState::Succeeded(_)));
                assert!(document.is_some());
                assert!(matches!(signal, Some(SignalState::Expired)));
                assert_eq!(
                    harness
                        .conversation(conversation)
                        .unwrap()
                        .application_writes
                        .len(),
                    1
                );
                assert_eq!(commits.load(Ordering::SeqCst), 2);
            }
        }
    }

    #[test]
    fn task_input_snapshot_race_replays_without_stopping_scheduler() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{Config, Harness, Options};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Racing {
            path: std::path::PathBuf,
            conversation: Uuid,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Racing {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
                docs.submit_input_with_snapshot_hook("race".into(), "hello".into(), || {
                    if first {
                        let store = durable_store::Store::open(&self.path).unwrap();
                        let key = Harness::key(self.conversation);
                        let old = store.get(&key).unwrap().unwrap();
                        store
                            .commit(
                                &[durable_store::Check {
                                    key: key.clone(),
                                    expected: durable_store::Expected::Version(old.version),
                                }],
                                &[durable_store::Mutation::Put {
                                    key,
                                    value: old.value,
                                }],
                            )
                            .unwrap();
                    }
                })
                .map_err(|e| e.to_string())?;
                let address = Address {
                    scope: "test".into(),
                    name: "snapshot-race".into(),
                    key: "".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(&address, json!({"done":true}).as_object().unwrap().clone())
                    .map_err(|e| e.to_string())?;
                Ok(Step::Succeed(json!(null)))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(
            &path,
            Options {
                model: Arc::new(Stub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
            },
        )
        .unwrap();
        let conversation = harness
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "race",
            true,
            Racing {
                path: path.clone(),
                conversation,
                calls: calls.clone(),
            },
        );
        let mut scheduler = harness.attach_tasks(registry).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            cp("start"),
            3,
        )
        .unwrap()
        .registered("race".into(), true);
        scheduler.submit(&task).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(
            harness
                .conversation(conversation)
                .unwrap()
                .submissions
                .is_empty()
        );
        let address = Address {
            scope: "test".into(),
            name: "snapshot-race".into(),
            key: "".into(),
        };
        assert!(
            Documents::new(&durable_store::Store::open(&path).unwrap())
                .get(&address)
                .unwrap()
                .is_none()
        );
        assert!(scheduler.tick(0).unwrap());
        assert!(matches!(
            scheduler.get(task.id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            harness
                .conversation(conversation)
                .unwrap()
                .submissions
                .len(),
            1
        );
        assert!(
            Documents::new(&durable_store::Store::open(&path).unwrap())
                .get(&address)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn checked_abort_rechecks_intent_under_task_gate() {
        use crate::runtime::{Config, Harness, Options};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::{sync::mpsc, time::Duration};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        for stale in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut harness = Harness::open_paused(
                dir.path().join("db"),
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            let nested = harness
                .create_conversation(Config {
                    model: "stub".into(),
                    system_prompt: "".into(),
                    max_output_tokens: 20,
                })
                .unwrap();
            let scheduler = harness.attach_tasks(Registry::default()).unwrap();
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            parent.start().unwrap();
            parent
                .wait(WaitFor::TasksFailFast(vec![
                    Uuid::from_u128(2),
                    Uuid::from_u128(3),
                ]))
                .unwrap();
            let mut running = task(2, TaskOwner::Task(parent.id()), "running", false, 3);
            running.start().unwrap();
            let mut failed = task(3, TaskOwner::Task(parent.id()), "failed", false, 3);
            failed.start().unwrap();
            failed.fail("failure".into()).unwrap();
            let child = task(4, TaskOwner::Task(running.id()), "child", false, 3);
            let nested_task = task(5, TaskOwner::Conversation(nested), "nested", false, 3);
            scheduler
                .ts()
                .save(&[
                    Save::Create(&parent),
                    Save::Create(&running),
                    Save::Create(&failed),
                    Save::Create(&child),
                    Save::Create(&nested_task),
                ])
                .unwrap();
            let owner = scheduler.owner();
            let edge = crate::ownership_edge::Edge {
                task: running.id(),
                task_owner: running.owner().clone(),
                child: nested,
                mode: crate::ownership_edge::Mode::Foreground,
            };
            let prepared = crate::ownership_edge::EdgeStore::new(owner.store())
                .prepare_create(&edge)
                .unwrap();
            owner
                .store()
                .commit(&prepared.checks, &prepared.mutations)
                .unwrap();
            let gate = owner.task_gate().lock().unwrap();
            assert_eq!(
                pending_abort_root_for_running(&scheduler.ts(), running.id(), false).unwrap(),
                Some(running.id())
            );
            let (started_tx, started_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let mut worker = harness.worker();
            let id = running.id();
            let thread = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                done_tx.send(worker.abort_pending_task_graph(id)).unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
            if stale {
                let parent = scheduler.ts().load(parent.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&parent, |task| task.request_abort())
                    .unwrap();
            }
            drop(gate);
            let result = done_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            thread.join().unwrap();
            assert_eq!(result.is_some(), !stale);
            assert_eq!(
                scheduler
                    .ts()
                    .load(running.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested(),
                !stale
            );
            assert_eq!(
                scheduler
                    .ts()
                    .load(nested_task.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested(),
                !stale
            );
            assert_eq!(
                harness.conversation(nested).unwrap().abort_requested,
                !stale
            );
            assert_eq!(
                scheduler
                    .ts()
                    .load(child.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested(),
                !stale
            );
        }
    }

    #[test]
    fn deferred_nested_cleanup_marks_two_live_handlers_before_their_results() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{Harness, Options};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::{Condvar, mpsc};
        use std::time::Duration;
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Blocked {
            entered: mpsc::Sender<Uuid>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }
        impl Handler for Blocked {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let address = Address {
                    scope: "maintenance".into(),
                    name: "result".into(),
                    key: task.id().to_string(),
                };
                docs.get(&address).unwrap();
                docs.put(
                    &address,
                    json!({"value": "must discard"})
                        .as_object()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                self.entered.send(task.id()).unwrap();
                let (lock, wake) = &*self.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
                Ok(Step::Succeed(json!("must discard")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let harness = Harness::open_paused(
            dir.path().join("db"),
            Options {
                model: Arc::new(Stub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
            },
        )
        .unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register(
            "nested",
            false,
            Blocked {
                entered: entered_tx,
                release: release.clone(),
            },
        );
        let mut scheduler = harness.attach_tasks(registry).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        )
        .into_task_tool();
        parent.start().unwrap();
        let a = task(2, TaskOwner::Task(parent.id()), "nested", false, 3).into_task_tool();
        let b = task(3, TaskOwner::Task(parent.id()), "nested", false, 3).into_task_tool();
        scheduler
            .ts()
            .save(&[Save::Create(&parent), Save::Create(&a), Save::Create(&b)])
            .unwrap();
        let current = scheduler.ts().load(parent.id()).unwrap().unwrap();
        scheduler
            .ts()
            .update(&current, |task| task.wait(WaitFor::Signal("resume".into())))
            .unwrap();
        let TickReservation::Invocation(a_invocation) = scheduler.try_reserve_tick(0).unwrap()
        else {
            panic!("A")
        };
        let TickReservation::Invocation(b_invocation) = scheduler.try_reserve_tick(0).unwrap()
        else {
            panic!("B")
        };
        let a_thread = std::thread::spawn(move || a_invocation.execute().unwrap());
        let b_thread = std::thread::spawn(move || b_invocation.execute().unwrap());
        let mut entered = [
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        ];
        entered.sort();
        assert_eq!(entered, [a.id(), b.id()]);
        let current = scheduler.ts().load(parent.id()).unwrap().unwrap();
        scheduler
            .ts()
            .update(&current, |task| {
                task.wake()?;
                task.start()?;
                task.hold_success(json!("parent"))
            })
            .unwrap();
        let owner = scheduler.owner();
        let control = owner.conversation_controls().get(Uuid::from_u128(99));
        let poll = control.gate.lock().unwrap();
        let TickReservation::Maintenance(pending) = scheduler.try_reserve_tick(0).unwrap() else {
            panic!("maintenance")
        };
        let mut cleanup = harness.worker();
        assert!(!pending.try_cleanup(&mut cleanup).unwrap());
        drop(poll);
        assert!(pending.try_cleanup(&mut cleanup).unwrap());
        for id in [a.id(), b.id()] {
            assert!(
                scheduler
                    .ts()
                    .load(id)
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
            );
        }
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let a_result = a_thread.join().unwrap();
        let b_result = b_thread.join().unwrap();
        match scheduler.try_settle_invocation(a_result) {
            Ok(_) => {
                scheduler.try_settle_invocation(b_result).unwrap();
            }
            Err(SettlementError::Pending { completed }) => {
                scheduler.try_settle_invocation(b_result).unwrap();
                scheduler
                    .try_resume_pending(PendingSettlement::Unprepared(*completed))
                    .unwrap();
            }
            result => panic!("unexpected settlement: {result:?}"),
        }
        for id in [a.id(), b.id()] {
            let current = scheduler.ts().load(id).unwrap().unwrap().task;
            assert_eq!(current.state(), &TaskState::Ready);
            assert!(current.abort_requested());
            let address = Address {
                scope: "maintenance".into(),
                name: "result".into(),
                key: id.to_string(),
            };
            assert!(
                Documents::new(owner.store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn deferred_maintenance_rechecks_graph_and_allows_independent_progress() {
        use crate::runtime::{Harness, Options};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        for kind in ["nested", "failed", "failfast"] {
            for stale in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let harness = Harness::open_paused(
                    dir.path().join("db"),
                    Options {
                        model: Arc::new(Stub),
                        tools: vec![],
                        restart_limit: 3,
                        max_turns: 3,
                        uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                    },
                )
                .unwrap();
                let mut registry = Registry::default();
                registry.register("independent", false, |_: &Task| {
                    Ok(Step::Succeed(json!("independent")))
                });
                let mut scheduler = harness.attach_tasks(registry).unwrap();
                let mut parent = task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "missing",
                    false,
                    3,
                );
                if kind == "nested" {
                    parent = parent.into_task_tool();
                }
                parent.start().unwrap();
                let mut a = task(2, TaskOwner::Task(parent.id()), "missing", false, 3);
                let mut b = task(3, TaskOwner::Task(parent.id()), "missing", false, 3);
                if kind == "nested" {
                    a = a.into_task_tool();
                    b = b.into_task_tool();
                }
                a.start().unwrap();
                b.start().unwrap();
                let mut failed = task(4, TaskOwner::Task(parent.id()), "missing", false, 3);
                failed.start().unwrap();
                failed.fail("failed".into()).unwrap();
                let independent = task(
                    5,
                    TaskOwner::Conversation(Uuid::from_u128(10)),
                    "independent",
                    false,
                    3,
                );
                let generic = task(7, TaskOwner::Task(parent.id()), "missing", false, 3);
                scheduler
                    .ts()
                    .save(&[
                        Save::Create(&parent),
                        Save::Create(&a),
                        Save::Create(&b),
                        Save::Create(&failed),
                        Save::Create(&independent),
                        Save::Create(&generic),
                    ])
                    .unwrap();
                let parent_record = scheduler.ts().load(parent.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&parent_record, |task| match kind {
                        "nested" => task.hold_success(json!("held")),
                        "failed" => task.hold_failure("held".into()),
                        _ => task.wait(WaitFor::TasksFailFast(vec![
                            Uuid::from_u128(2),
                            Uuid::from_u128(3),
                            Uuid::from_u128(4),
                        ])),
                    })
                    .unwrap();
                let owner = scheduler.owner();
                let control = owner.conversation_controls().get(Uuid::from_u128(99));
                let poll = control.gate.lock().unwrap();
                let TickReservation::Maintenance(pending) = scheduler.try_reserve_tick(0).unwrap()
                else {
                    panic!("maintenance: {kind}")
                };
                let mut cleanup = harness.worker();
                assert!(!pending.try_cleanup(&mut cleanup).unwrap());
                let invocation = scheduler.reserve_independent_root().unwrap().unwrap();
                assert_eq!(invocation.task_id(), independent.id());
                scheduler
                    .settle_invocation(invocation.execute().unwrap())
                    .unwrap();
                assert_eq!(
                    scheduler
                        .ts()
                        .load(independent.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Succeeded(json!("independent"))
                );
                let admitted =
                    task(6, TaskOwner::Task(parent.id()), "missing", false, 3).into_task_tool();
                if kind == "nested" {
                    // Simulate a raw concurrent writer bypassing TaskStore admission;
                    // the graph snapshot must still include the newly visible root.
                    let key = task_store::key(admitted.id());
                    owner
                        .store()
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Missing,
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: serde_json::to_vec(&admitted).unwrap(),
                            }],
                        )
                        .unwrap();
                }
                if stale {
                    let current = scheduler.ts().load(parent.id()).unwrap().unwrap();
                    scheduler
                        .ts()
                        .update(&current, |task| {
                            if kind == "failfast" {
                                task.wake()
                            } else {
                                task.finalize()
                            }
                        })
                        .unwrap();
                }
                drop(poll);
                assert!(pending.try_cleanup(&mut cleanup).unwrap());
                let a_marked = scheduler
                    .ts()
                    .load(a.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested();
                let b_marked = scheduler
                    .ts()
                    .load(b.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested();
                if stale {
                    assert!(!a_marked && !b_marked);
                } else if kind == "failfast" {
                    assert!(a_marked ^ b_marked);
                } else {
                    assert!(a_marked && b_marked);
                }
                if kind == "nested" {
                    assert_eq!(
                        scheduler
                            .ts()
                            .load(admitted.id())
                            .unwrap()
                            .unwrap()
                            .task
                            .abort_requested(),
                        !stale
                    );
                    assert!(
                        !scheduler
                            .ts()
                            .load(generic.id())
                            .unwrap()
                            .unwrap()
                            .task
                            .abort_requested()
                    );
                }
            }
        }
    }

    #[test]
    fn pre_prepare_busy_returns_lease_and_independent_root_can_run() {
        use crate::document::{Address, Documents, TaskDocuments, Transaction};
        use crate::runtime::{Harness, Options};
        use crate::task_store::{SignalResult, SignalToken};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Staging(Address, bool);
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                parent: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                docs.get(&self.0).unwrap();
                docs.put(
                    &self.0,
                    json!({"value": "discard"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(5), &token, "key", json!(7), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                let staged = task(6, TaskOwner::Task(parent.id()), "spawn", false, 3);
                docs.stage_keyed_child("keyed", "tool", staged.input(), staged.clone())
                    .unwrap();
                if self.1 {
                    Ok(Step::Fail("retained failure".into()))
                } else {
                    Ok(Step::Succeed(json!("discard")))
                }
            }
        }
        for timing in [
            "before",
            "prepare_conflict",
            "prepare_conflict_stale",
            "prepare_conflict_stale_rewrite",
            "prepare_conflict_direct_abort",
            "prepare_conflict_ancestor_abort",
            "prepare_conflict_changed",
            "failure",
            "failure_stale",
            "failure_stale_rewrite",
            "failure_direct_abort",
            "failure_ancestor_abort",
            "failure_changed",
            "preparation",
            "commit",
            "stale",
            "stale_doc",
            "stale_signal",
            "poison",
            "poison_prepared",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let harness = Harness::open_paused(
                dir.path().join("db"),
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            let address = Address {
                scope: "pending".into(),
                name: "result".into(),
                key: "".into(),
            };
            let mut registry = Registry::default();
            registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.register_with_abort(
                "child",
                false,
                Staging(address.clone(), timing.starts_with("failure")),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            registry.register("independent", false, |_: &Task| {
                Ok(Step::Succeed(json!("independent")))
            });
            registry.register("spawn", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            parent.start().unwrap();
            parent
                .wait(WaitFor::TasksFailFast(vec![
                    Uuid::from_u128(2),
                    Uuid::from_u128(3),
                ]))
                .unwrap();
            let child = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
            let sibling = task(3, TaskOwner::Task(parent.id()), "missing", false, 3);
            let other = task(
                4,
                TaskOwner::Conversation(Uuid::from_u128(10)),
                "independent",
                false,
                3,
            );
            let receiver = task(
                5,
                TaskOwner::Conversation(Uuid::from_u128(11)),
                "missing",
                false,
                3,
            );
            scheduler
                .ts()
                .save(&[
                    Save::Create(&parent),
                    Save::Create(&child),
                    Save::Create(&sibling),
                    Save::Create(&other),
                    Save::Create(&receiver),
                ])
                .unwrap();
            let TickReservation::Invocation(child_invocation) = scheduler.reserve_tick(0).unwrap()
            else {
                panic!("child")
            };
            assert_eq!(child_invocation.task_id(), child.id());
            let completed = child_invocation.execute().unwrap();
            let owner = scheduler.owner();
            let control = owner.conversation_controls().get(Uuid::from_u128(99));
            let poll = control.gate.lock().unwrap();
            if timing.starts_with("prepare_conflict") || timing.starts_with("failure") {
                let hook: CommitHook = Arc::new(move |attempt, store| {
                    if attempt == 0 {
                        let tasks = TaskStore::new(store);
                        let sibling = tasks.load(Uuid::from_u128(3)).unwrap().unwrap();
                        tasks
                            .update(&sibling, |task| {
                                task.start()?;
                                task.fail("failed".into())
                            })
                            .unwrap();
                    }
                });
                if timing.starts_with("prepare_conflict") {
                    scheduler.inject_batch_prepare_conflict = true;
                    scheduler.preparation_conflict_hook = Some(hook);
                } else {
                    scheduler.failure_commit_hook = Some(hook);
                }
            } else if timing == "preparation" {
                scheduler.after_result_load_hook = Some(Arc::new(move |_, store| {
                    let tasks = TaskStore::new(store);
                    let sibling = tasks.load(Uuid::from_u128(3)).unwrap().unwrap();
                    tasks
                        .update(&sibling, |task| {
                            task.start()?;
                            task.fail("failed".into())
                        })
                        .unwrap();
                }));
            } else if matches!(
                timing,
                "commit" | "stale" | "stale_doc" | "stale_signal" | "poison_prepared"
            ) {
                scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
                    let tasks = TaskStore::new(store);
                    let sibling = tasks.load(Uuid::from_u128(3)).unwrap().unwrap();
                    tasks
                        .update(&sibling, |task| {
                            task.start()?;
                            task.fail("failed".into())
                        })
                        .unwrap();
                }));
            } else {
                let sibling = scheduler.ts().load(sibling.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&sibling, |task| {
                        task.start()?;
                        task.fail("failed".into())
                    })
                    .unwrap();
            }
            let pending = match scheduler.try_settle_invocation(completed) {
                Err(SettlementError::Pending { completed }) => {
                    PendingSettlement::Unprepared(*completed)
                }
                Err(SettlementError::PendingPrepared { pending })
                    if timing.starts_with("prepare_conflict")
                        || timing.starts_with("failure")
                        || matches!(
                            timing,
                            "commit"
                                | "stale"
                                | "stale_doc"
                                | "stale_signal"
                                | "poison_prepared"
                                | "prepare_conflict"
                                | "prepare_conflict_stale"
                                | "failure"
                                | "failure_stale"
                        ) =>
                {
                    *pending
                }
                result => panic!("expected pending: {result:?}"),
            };
            scheduler.after_result_load_hook = None;
            scheduler.result_commit_hook = None;
            scheduler.preparation_conflict_hook = None;
            scheduler.failure_commit_hook = None;
            scheduler.inject_batch_prepare_conflict = false;
            if timing.starts_with("failure") {
                assert!(
                    matches!(&pending, PendingSettlement::Failure { settlement, .. } if settlement.reason == "retained failure" && settlement.candidate.is_some())
                );
            }
            if timing.starts_with("prepare_conflict") {
                assert!(matches!(
                    &pending,
                    PendingSettlement::PreparationConflict { .. }
                ));
            }
            if matches!(
                timing,
                "commit" | "stale" | "stale_doc" | "stale_signal" | "poison_prepared"
            ) {
                match &pending {
                    PendingSettlement::Prepared { settlement, .. } => {
                        assert!(settlement.staged_children.contains_key(&Uuid::from_u128(6)));
                        assert_eq!(
                            settlement.candidate.task.state(),
                            &TaskState::Completing(json!("discard"))
                        );
                    }
                    _ => panic!("expected retained prepared transaction"),
                }
            }
            let mut cleanup_worker = harness.worker();
            assert!(!pending.try_cleanup(&mut cleanup_worker).unwrap());
            assert!(
                scheduler
                    .live_invocations
                    .lock()
                    .unwrap()
                    .attempts
                    .contains(&child.id())
            );
            let independent = scheduler.reserve_independent_root().unwrap().unwrap();
            assert_eq!(independent.task_id(), other.id());
            scheduler
                .settle_invocation(independent.execute().unwrap())
                .unwrap();
            assert_eq!(
                scheduler.get(other.id()).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("independent"))
            );
            assert!(
                !scheduler
                    .ts()
                    .load(child.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
            );
            if timing == "poison" || timing == "poison_prepared" {
                scheduler
                    .poisoned
                    .store(true, std::sync::atomic::Ordering::Release);
                drop(poll);
                assert!(matches!(
                    pending.try_cleanup(&mut cleanup_worker),
                    Err(Error::Poisoned)
                ));
                assert!(
                    !scheduler
                        .ts()
                        .load(child.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .abort_requested()
                );
                drop(pending);
                continue;
            }
            if matches!(timing, "stale" | "stale_doc" | "stale_signal")
                || timing.contains("_stale")
                || timing.ends_with("_changed")
            {
                // Simulate an external graph rewrite after conflict discovery.
                let _gate = owner.task_gate().lock().unwrap();
                let current = scheduler.ts().load(sibling.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&current, |task| {
                        *task = sibling.clone();
                        Ok(())
                    })
                    .unwrap();
            }
            if timing.ends_with("_rewrite") {
                let current = scheduler.ts().load(child.id()).unwrap().unwrap();
                scheduler.ts().update(&current, |_| Ok(())).unwrap();
                let rewritten = scheduler.ts().load(child.id()).unwrap().unwrap();
                assert_ne!(rewritten.version, current.version);
                assert_eq!(rewritten.task, current.task);
            }
            if timing.ends_with("_direct_abort") {
                request_abort_subtree(&scheduler.ts(), child.id()).unwrap();
            }
            if timing.ends_with("_ancestor_abort") {
                request_abort_subtree(&scheduler.ts(), parent.id()).unwrap();
            }
            if timing.ends_with("_changed") {
                let current = scheduler.ts().load(child.id()).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&current, |task| {
                        task.recover()?;
                        task.start_attempt(Uuid::from_u128(888))
                    })
                    .unwrap();
            }
            if timing == "stale_doc" {
                let mut external = Transaction::new();
                external
                    .put(
                        &address,
                        durable_store::Expected::Missing,
                        json!({"value":"external"}).as_object().unwrap(),
                    )
                    .unwrap();
                external.commit(owner.store()).unwrap();
            }
            if timing == "stale_signal" {
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    scheduler
                        .ts()
                        .deliver_signal_at(receiver.id(), &token, "key", json!(8), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
            }
            drop(poll);
            let failure_fence = scheduler.failure_fence.clone();
            let failure_guard = failure_fence.lock().unwrap();
            assert!(!pending.try_cleanup(&mut cleanup_worker).unwrap());
            drop(failure_guard);
            assert!(pending.try_cleanup(&mut cleanup_worker).unwrap());
            if timing.ends_with("_changed") {
                let result = scheduler.try_resume_pending(pending);
                let Err(SettlementError::RejectedPrepared { pending, .. }) = result else {
                    panic!("changed attempt must remain owned and rejected");
                };
                assert!(
                    scheduler
                        .live_invocations
                        .lock()
                        .unwrap()
                        .attempts
                        .contains(&child.id())
                );
                assert_eq!(
                    scheduler
                        .ts()
                        .load(child.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .attempt_id(),
                    Some(Uuid::from_u128(888))
                );
                drop(pending);
                continue;
            }
            scheduler.try_resume_pending(pending).unwrap();
            let child = scheduler.ts().load(child.id()).unwrap().unwrap().task;
            if timing == "stale" {
                assert_eq!(child.state(), &TaskState::Completing(json!("discard")));
                assert!(!child.abort_requested());
            } else if timing.starts_with("failure_stale") {
                assert!(
                    matches!(child.state(), TaskState::Failed(reason) if reason == "retained failure")
                );
            } else if timing == "stale_doc"
                || timing == "stale_signal"
                || timing.starts_with("prepare_conflict_stale")
            {
                assert!(
                    matches!(child.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
                );
            } else {
                assert_eq!(child.state(), &TaskState::Ready);
                assert!(child.abort_requested());
            }
            assert!(
                !scheduler
                    .live_invocations
                    .lock()
                    .unwrap()
                    .attempts
                    .contains(&child.id())
            );
            let document = Documents::new(owner.store()).get(&address).unwrap();
            assert_eq!(
                document.map(|doc| doc.value["value"].clone()),
                if timing == "stale" {
                    Some(json!("discard"))
                } else if timing == "stale_doc" {
                    Some(json!("external"))
                } else {
                    None
                }
            );
            assert_eq!(
                scheduler.get(Uuid::from_u128(6)).unwrap().is_some(),
                timing == "stale"
            );
            let token = SignalToken::new("answer", 1).unwrap();
            assert_eq!(
                scheduler
                    .ts()
                    .read_typed_signal(receiver.id(), &token, "key")
                    .unwrap(),
                if timing == "stale" {
                    Some(json!(7))
                } else if timing == "stale_signal" {
                    Some(json!(8))
                } else {
                    None
                }
            );
        }
    }
    fn task(id: u128, owner: TaskOwner, kind: &str, safe: bool, limit: u32) -> Task {
        Task::new(Uuid::from_u128(id), owner, cp("start"), limit)
            .unwrap()
            .registered(kind.into(), safe)
    }

    #[test]
    fn late_foreground_edge_fences_failure_and_independent_reservation() {
        use crate::runtime::{Config, Harness, Options};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        for mode in ["failure", "reservation"] {
            let dir = tempfile::tempdir().unwrap();
            let mut harness = Harness::open_paused(
                dir.path().join("db"),
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            let conversation = harness
                .create_conversation(Config {
                    model: "stub".into(),
                    system_prompt: "".into(),
                    max_output_tokens: 10,
                })
                .unwrap();
            let mut registry = Registry::default();
            registry.register("a", false, |_: &Task| Ok(Step::Fail("first".into())));
            registry.register("b", false, |_: &Task| Ok(Step::Fail("second".into())));
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let a = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "a",
                false,
                3,
            );
            let b = task(2, TaskOwner::Conversation(conversation), "b", false, 3);
            scheduler
                .ts()
                .save(&[Save::Create(&a), Save::Create(&b)])
                .unwrap();
            let TickReservation::Invocation(a_invocation) = scheduler.reserve_tick(0).unwrap()
            else {
                panic!("A")
            };
            let b_invocation = if mode == "failure" {
                let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap()
                else {
                    panic!("B")
                };
                Some(invocation)
            } else {
                None
            };
            let a_result = a_invocation.execute().unwrap();
            let edge = crate::ownership_edge::Edge {
                task: a.id(),
                task_owner: a.owner().clone(),
                child: conversation,
                mode: crate::ownership_edge::Mode::Foreground,
            };
            let once = Arc::new(AtomicBool::new(false));
            let hook: CommitHook = Arc::new(move |_, store| {
                if !once.swap(true, Ordering::SeqCst) {
                    let prepared = crate::ownership_edge::EdgeStore::new(store)
                        .prepare_create(&edge)
                        .unwrap();
                    store.commit(&prepared.checks, &prepared.mutations).unwrap();
                }
            });
            if let Some(invocation) = b_invocation {
                let result = invocation.execute().unwrap();
                scheduler.failure_commit_hook = Some(hook);
                assert!(matches!(
                    scheduler.try_settle_invocation(result),
                    Err(SettlementError::PendingPrepared { .. })
                ));
                assert_eq!(
                    scheduler.ts().load(b.id()).unwrap().unwrap().task.state(),
                    &TaskState::Running
                );
            } else {
                scheduler.reservation_commit_hook = Some(hook);
                assert!(scheduler.reserve_independent_root().unwrap().is_none());
                assert!(scheduler.reserve_independent_root().unwrap().is_none());
                assert_eq!(
                    scheduler.ts().load(b.id()).unwrap().unwrap().task.state(),
                    &TaskState::Ready
                );
            }
            drop(a_result);
        }
    }

    #[test]
    fn priority_scope_must_be_fenced_against_late_foreground_edge() {
        use crate::runtime::{Config, Harness, Options};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut harness = Harness::open_paused(
            dir.path().join("db"),
            Options {
                model: Arc::new(Stub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
            },
        )
        .unwrap();
        let conversation = harness
            .create_conversation(Config {
                model: "stub".into(),
                system_prompt: "".into(),
                max_output_tokens: 10,
            })
            .unwrap();
        let mut registry = Registry::default();
        registry.register("a", false, |_: &Task| Ok(Step::Fail("failure".into())));
        registry.register("b", false, |_: &Task| Ok(Step::Succeed(json!("must wait"))));
        let mut scheduler = harness.attach_tasks(registry).unwrap();
        let a = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "a",
            false,
            3,
        );
        let b = task(2, TaskOwner::Conversation(conversation), "b", false, 3);
        scheduler
            .ts()
            .save(&[Save::Create(&a), Save::Create(&b)])
            .unwrap();
        let TickReservation::Invocation(a_invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b_invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a_result = a_invocation.execute().unwrap();
        let b_result = b_invocation.execute().unwrap();
        let edge = crate::ownership_edge::Edge {
            task: a.id(),
            task_owner: a.owner().clone(),
            child: conversation,
            mode: crate::ownership_edge::Mode::Foreground,
        };
        scheduler.after_order_hook = Some(Arc::new(move |_, store| {
            let prepared = crate::ownership_edge::EdgeStore::new(store)
                .prepare_create(&edge)
                .unwrap();
            store.commit(&prepared.checks, &prepared.mutations).unwrap();
        }));
        let result = scheduler.try_settle_invocation(b_result);
        assert!(
            matches!(
                result,
                Err(SettlementError::Pending { .. } | SettlementError::PendingPrepared { .. })
            ),
            "B must not publish after the foreground link appears: {result:?}"
        );
        drop(a_result);
    }

    #[test]
    fn published_child_failure_blocks_its_tasktool_root() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| {
            Ok(Step::Succeed(json!("parent")))
        });
        registry.register("child", false, |_: &Task| {
            Ok(Step::Fail("child failed".into()))
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        )
        .into_task_tool();
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksAny(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let child = task(2, TaskOwner::Task(parent.id()), "child", false, 3).into_task_tool();
        let winner = task(3, TaskOwner::Task(parent.id()), "missing", false, 3);
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&winner),
            ])
            .unwrap();
        let TickReservation::Invocation(child_invocation) = scheduler.reserve_tick(0).unwrap()
        else {
            panic!("child")
        };
        let winner = scheduler.ts().load(winner.id()).unwrap().unwrap();
        scheduler
            .ts()
            .update(&winner, |task| {
                task.start()?;
                task.succeed(json!("winner"))
            })
            .unwrap();
        let TickReservation::Invocation(parent_invocation) = scheduler.reserve_tick(0).unwrap()
        else {
            panic!("parent")
        };
        let child_result = child_invocation.execute().unwrap();
        let parent_result = parent_invocation.execute().unwrap();
        let parent_pending = match scheduler.try_settle_invocation(parent_result) {
            Err(SettlementError::Pending { completed }) => {
                PendingSettlement::Unprepared(*completed)
            }
            result => panic!("ancestor must wait for published child failure: {result:?}"),
        };
        scheduler.try_settle_invocation(child_result).unwrap();
        scheduler.try_resume_pending(parent_pending).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .load(child.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Failed("child failed".into())
        );
    }

    #[test]
    fn foreground_scope_is_stable_for_legacy_ownership_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let owner = scheduler.owner();
        let a = task(1, TaskOwner::Task(Uuid::from_u128(2)), "x", false, 3);
        let b = task(2, TaskOwner::Task(Uuid::from_u128(1)), "x", false, 3);
        let child = task(3, TaskOwner::Task(a.id()), "x", false, 3);
        for item in [&a, &b, &child] {
            let key = task_store::key(item.id());
            owner
                .store()
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(item).unwrap(),
                    }],
                )
                .unwrap();
        }
        assert_eq!(Scheduler::foreground_root(&owner, &a), Some(a.id()));
        assert_eq!(Scheduler::foreground_root(&owner, &b), Some(a.id()));
        assert_eq!(Scheduler::foreground_root(&owner, &child), Some(a.id()));
    }

    #[test]
    fn unrelated_task_owned_child_progresses_while_failure_is_published() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register("fail", false, |_: &Task| Ok(Step::Fail("failed".into())));
        registry.register("other", false, |_: &Task| Ok(Step::Succeed(json!("other"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let mut first = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        first.start().unwrap();
        first.wait(WaitFor::Signal("first".into())).unwrap();
        let failed = task(2, TaskOwner::Task(first.id()), "fail", false, 3);
        let mut second = task(
            3,
            TaskOwner::Conversation(Uuid::from_u128(10)),
            "parent",
            false,
            3,
        );
        second.start().unwrap();
        second.wait(WaitFor::Signal("second".into())).unwrap();
        let other = task(4, TaskOwner::Task(second.id()), "other", false, 3);
        scheduler
            .ts()
            .save(&[
                Save::Create(&first),
                Save::Create(&failed),
                Save::Create(&second),
                Save::Create(&other),
            ])
            .unwrap();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("failure")
        };
        let failure = invocation.execute().unwrap();
        let other_invocation = scheduler
            .reserve_independent_root()
            .unwrap()
            .expect("independent generic child");
        assert_eq!(other_invocation.task_id(), other.id());
        scheduler
            .try_settle_invocation(other_invocation.execute().unwrap())
            .unwrap();
        assert_eq!(
            scheduler
                .ts()
                .load(other.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("other"))
        );
        scheduler.try_settle_invocation(failure).unwrap();
    }

    #[test]
    fn earlier_published_failure_fences_later_sibling_result() {
        for failure_first in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.register("a", false, |_: &Task| Ok(Step::Fail("failed".into())));
            registry.register("b", false, |_: &Task| {
                Ok(Step::Succeed(json!("must abort")))
            });
            registry.register("other", false, |_: &Task| {
                Ok(Step::Succeed(json!("independent")))
            });
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            parent.start().unwrap();
            parent
                .wait(WaitFor::TasksFailFast(vec![
                    Uuid::from_u128(2),
                    Uuid::from_u128(3),
                ]))
                .unwrap();
            let a = task(2, TaskOwner::Task(parent.id()), "a", false, 3);
            let b = task(3, TaskOwner::Task(parent.id()), "b", false, 3);
            let other = task(
                4,
                TaskOwner::Conversation(Uuid::from_u128(10)),
                "other",
                false,
                3,
            );
            scheduler
                .ts()
                .save(&[
                    Save::Create(&parent),
                    Save::Create(&a),
                    Save::Create(&b),
                    Save::Create(&other),
                ])
                .unwrap();
            let TickReservation::Invocation(a_invocation) = scheduler.reserve_tick(0).unwrap()
            else {
                panic!("A")
            };
            let TickReservation::Invocation(b_invocation) = scheduler.reserve_tick(0).unwrap()
            else {
                panic!("B")
            };
            let (a_result, b_result) = if failure_first {
                (
                    a_invocation.execute().unwrap(),
                    b_invocation.execute().unwrap(),
                )
            } else {
                let b = b_invocation.execute().unwrap();
                (a_invocation.execute().unwrap(), b)
            };
            let b_pending = match scheduler.try_settle_invocation(b_result) {
                Err(SettlementError::Pending { completed }) => {
                    PendingSettlement::Unprepared(*completed)
                }
                result => panic!("later sibling must wait: {result:?}"),
            };
            let other = scheduler.reserve_independent_root().unwrap().unwrap();
            scheduler
                .try_settle_invocation(other.execute().unwrap())
                .unwrap();
            scheduler.try_settle_invocation(a_result).unwrap();
            scheduler.try_resume_pending(b_pending).unwrap();
            let b = scheduler.ts().load(b.id()).unwrap().unwrap().task;
            assert_eq!(b.state(), &TaskState::Ready);
            assert!(b.abort_requested());
        }
    }

    #[test]
    fn failed_attempt_allows_other_completed_invocation_to_drain() {
        let dir = tempfile::tempdir().unwrap();
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let effects_b = effects.clone();
        let mut registry = Registry::default();
        registry.register("fails", false, |_: &Task| Err("handler failed".into()));
        registry.register("succeeds", false, move |_: &Task| {
            effects_b.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Step::Succeed(json!("done")))
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "fails", false, 3))
            .unwrap();
        scheduler
            .submit(&task(2, owner, "succeeds", false, 3))
            .unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a = a.execute().unwrap();
        let b = b.execute().unwrap();
        assert_eq!(effects.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        assert!(
            matches!(scheduler.settle_invocation(a), Err(SettlementError::Handler(Error::Handler(reason))) if reason == "handler failed")
        );
        scheduler.try_settle_invocation(b).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn abort_supersedes_reported_handler_failure_and_resumes_cleanup() {
        for ancestor in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            registry.register("fails", false, |_: &Task| Err("failed".into()));
            registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.set_abort("fails", |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.set_abort("parent", |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let conversation = TaskOwner::Conversation(Uuid::from_u128(9));
            if ancestor {
                scheduler
                    .submit(&task(3, conversation.clone(), "parent", false, 3))
                    .unwrap();
            }
            let owner = if ancestor {
                TaskOwner::Task(Uuid::from_u128(3))
            } else {
                conversation
            };
            scheduler
                .ts()
                .save(&[Save::Create(&task(1, owner, "fails", false, 3))])
                .unwrap();
            let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
                panic!("invocation")
            };
            let completed = invocation.execute().unwrap();
            assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
            request_abort_subtree(
                &scheduler.ts(),
                Uuid::from_u128(if ancestor { 3 } else { 1 }),
            )
            .unwrap();
            scheduler.settle_invocation(completed).unwrap();
            assert!(scheduler.check().is_ok());
            for _ in 0..4 {
                scheduler.tick(0).unwrap();
                if scheduler
                    .get(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .is_terminal()
                {
                    break;
                }
            }
            assert_eq!(
                scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
        }
    }

    #[test]
    fn abort_clears_only_its_own_failed_handler() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("fails", false, |_: &Task| Err("failed".into()));
        registry.set_abort("fails", |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "fails", false, 3))
            .unwrap();
        scheduler
            .submit(&task(2, owner, "fails", false, 3))
            .unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a = a.execute().unwrap();
        let b = b.execute().unwrap();
        request_abort_subtree(&scheduler.ts(), Uuid::from_u128(1)).unwrap();
        scheduler.settle_invocation(a).unwrap();
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        assert_eq!(
            scheduler.live_invocations.lock().unwrap().failed_handlers,
            HashSet::from([Uuid::from_u128(2)])
        );
        request_abort_subtree(&scheduler.ts(), Uuid::from_u128(2)).unwrap();
        scheduler.settle_invocation(b).unwrap();
        assert!(scheduler.check().is_ok());
        scheduler.tick(0).unwrap();
        scheduler.tick(0).unwrap();
        for id in [1, 2] {
            assert_eq!(
                scheduler.get(Uuid::from_u128(id)).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
        }
    }

    #[test]
    fn handler_failure_retains_task_owned_outcome_for_graph_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("fails", false, |_: &Task| Err("failed".into()));
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(3, owner.clone(), "work", false, 3))
            .unwrap();
        scheduler
            .submit(&task(1, owner, "fails", false, 3))
            .unwrap();
        scheduler
            .ts()
            .save(&[Save::Create(&task(
                2,
                TaskOwner::Task(Uuid::from_u128(3)),
                "work",
                false,
                3,
            ))])
            .unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a = a.execute().unwrap();
        let b = b.execute().unwrap();
        assert!(matches!(
            scheduler.settle_invocation(a),
            Err(SettlementError::Handler(_))
        ));
        // A separate foreground task tree may safely drain even when it is
        // itself task-owned; the failed handler belongs to a different root.
        scheduler.settle_invocation(b).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn stopped_reservations_allow_existing_invocation_to_settle() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "work", false, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "work", false, 3)).unwrap();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("reservation")
        };
        scheduler.stop_reservations();
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        scheduler
            .settle_invocation(invocation.execute().unwrap())
            .unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn wrong_scheduler_returns_result_for_original_scheduler() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut original = Scheduler::open(dir.path().join("first"), registry).unwrap();
        let mut other = Scheduler::open(dir.path().join("second"), Registry::default()).unwrap();
        original
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "work",
                false,
                3,
            ))
            .unwrap();
        let TickReservation::Invocation(invocation) = original.reserve_tick(0).unwrap() else {
            panic!("reservation")
        };
        let completed = invocation.execute().unwrap();
        let completed = match other.settle_invocation(completed) {
            Err(SettlementError::Rejected {
                error: Error::Ownership,
                completed,
            }) => completed,
            result => panic!("expected ownership rejection: {result:?}"),
        };
        original.settle_invocation(*completed).unwrap();
        assert_eq!(
            original.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn prepared_resume_rejects_a_lease_from_another_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut first = Scheduler::open(dir.path().join("first"), registry.clone()).unwrap();
        let mut second = Scheduler::open(dir.path().join("second"), registry).unwrap();
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "work",
            false,
            3,
        );
        first.submit(&root).unwrap();
        second.submit(&root).unwrap();
        let TickReservation::Invocation(a) = first.reserve_tick(0).unwrap() else {
            panic!("first")
        };
        let TickReservation::Invocation(b) = second.reserve_tick(0).unwrap() else {
            panic!("second")
        };
        let CompletedInvocation { lease, .. } = a.execute().unwrap();
        let CompletedInvocation {
            running,
            lease: other_lease,
            ..
        } = b.execute().unwrap();
        let settlement = PreparedSettlement {
            attachment: second.attachment.clone(),
            transaction: crate::document::Transaction::new(),
            notifications: None,
            candidate: running.clone(),
            running,
            wait_targets: vec![],
            staged_children: HashMap::new(),
            conflict_keys: HashSet::new(),
            graph_version: 0,
            attempts: 0,
            commit_uncertain: false,
        };
        let pending = PendingSettlement::Prepared { lease, settlement };
        assert!(matches!(
            first.try_resume_pending(pending),
            Err(SettlementError::RejectedPrepared {
                error: Error::Ownership,
                ..
            })
        ));
        drop(other_lease);
    }

    #[test]
    fn settlement_conflict_is_not_classified_as_handler_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "work",
                false,
                3,
            ))
            .unwrap();
        scheduler
            .submit(&task(
                2,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "work",
                false,
                3,
            ))
            .unwrap();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("reservation")
        };
        let TickReservation::Invocation(other) = scheduler.reserve_tick(0).unwrap() else {
            panic!("other reservation")
        };
        let completed = invocation.execute().unwrap();
        let other = other.execute().unwrap();
        let running = scheduler.ts().load(Uuid::from_u128(1)).unwrap().unwrap();
        scheduler
            .ts()
            .update(&running, |task| task.yield_ready())
            .unwrap();
        assert!(matches!(
            scheduler.settle_invocation(completed),
            Err(SettlementError::IndeterminateOwned {
                error: Error::Store(task_store::Error::Store(
                    durable_store::Error::Conflict { .. }
                )),
                ..
            })
        ));
        assert!(matches!(
            scheduler.settle_invocation(other),
            Err(SettlementError::Rejected {
                error: Error::Poisoned,
                ..
            })
        ));
    }

    #[test]
    fn uncertain_aborted_yield_retains_prepared_transaction_and_lease() {
        use crate::document::{Address, Documents, TaskDocuments};
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Staged(Address);
        impl Handler for Staged {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                docs.get(&self.0).unwrap();
                docs.put(
                    &self.0,
                    json!({"value": "held"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("held")))
            }
        }
        for resumed in [false, true] {
            for injection in [1, 2] {
                let dir = tempfile::tempdir().unwrap();
                let address = Address {
                    scope: "abort-yield".into(),
                    name: "doc".into(),
                    key: "".into(),
                };
                let mut registry = Registry::default();
                registry.register("work", false, Staged(address.clone()));
                let mut scheduler =
                    Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
                let owner = scheduler.owner();
                scheduler
                    .ts()
                    .save(&[
                        Save::Create(&task(
                            1,
                            TaskOwner::Conversation(Uuid::from_u128(9)),
                            "work",
                            false,
                            3,
                        )),
                        Save::Create(&task(
                            2,
                            TaskOwner::Conversation(Uuid::from_u128(10)),
                            "missing",
                            false,
                            3,
                        )),
                    ])
                    .unwrap();
                let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap()
                else {
                    panic!("reservation")
                };
                let completed = invocation.execute().unwrap();
                let attempt = completed.running.task.attempt_id();
                let once = Arc::new(AtomicBool::new(false));
                scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
                    if !once.swap(true, Ordering::SeqCst) {
                        let tasks = TaskStore::new(store);
                        let id = Uuid::from_u128(if resumed { 2 } else { 1 });
                        let current = tasks.load(id).unwrap().unwrap();
                        tasks
                            .update(&current, |task| {
                                if resumed {
                                    Ok(())
                                } else {
                                    task.request_abort()
                                }
                            })
                            .unwrap();
                    }
                }));
                scheduler.yield_after_graph_conflict = resumed;
                scheduler.inject_aborted_yield_error = injection;
                let result = if resumed {
                    let pending = match scheduler.try_settle_invocation(completed) {
                        Err(SettlementError::Yielded { pending }) => pending,
                        result => panic!("expected prepared yield: {result:?}"),
                    };
                    scheduler.result_commit_hook = None;
                    scheduler.yield_after_graph_conflict = false;
                    let current = scheduler.ts().load(Uuid::from_u128(1)).unwrap().unwrap();
                    scheduler
                        .ts()
                        .update(&current, |task| task.request_abort())
                        .unwrap();
                    scheduler.try_resume_pending(*pending)
                } else {
                    scheduler.try_settle_invocation(completed)
                };
                let pending = match result {
                    Err(
                        SettlementError::IndeterminatePrepared { pending, .. }
                        | SettlementError::IndeterminateOwned { pending, .. },
                    ) => pending,
                    result => panic!("prepared candidate must be retained: {result:?}"),
                };
                assert!(matches!(&*pending, PendingSettlement::Prepared { .. }));
                assert_eq!(pending.attempt_id(), attempt);
                assert_eq!(
                    pending.candidate_task().unwrap().state(),
                    &TaskState::Succeeded(json!("held"))
                );
                assert!(
                    Documents::new(owner.store())
                        .get(&address)
                        .unwrap()
                        .is_none()
                );
                let stored = scheduler
                    .ts()
                    .load(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .task;
                assert!(stored.abort_requested());
                assert_eq!(
                    stored.state(),
                    if injection == 1 {
                        &TaskState::Running
                    } else {
                        &TaskState::Ready
                    }
                );
                let retained = match scheduler.try_resume_pending(*pending) {
                    Err(SettlementError::RejectedPrepared {
                        error: Error::Poisoned,
                        pending,
                    }) => pending,
                    result => panic!("blind retry must be fenced: {result:?}"),
                };
                drop(scheduler);
                assert!(matches!(
                    Scheduler::attach(owner.clone(), registry.clone()),
                    Err(Error::AlreadyAttached)
                ));
                drop(retained);
                assert!(Scheduler::attach(owner.clone(), registry).is_ok());
            }
        }
    }

    #[test]
    fn uncertain_document_preparation_retains_original_staged_input() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::task_store::{SignalResult, SignalToken};
        struct Staged(Address);
        impl Handler for Staged {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                parent: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                docs.get(&self.0).unwrap();
                docs.put(
                    &self.0,
                    json!({"value": "retained"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(2), &token, "key", json!(7), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                let child = task(3, TaskOwner::Task(parent.id()), "child", false, 3);
                docs.stage_keyed_child("child", "tool", child.input(), child.clone())
                    .unwrap();
                Ok(Step::Succeed(json!("result")))
            }
        }
        for injection in [1, 2, 3] {
            let dir = tempfile::tempdir().unwrap();
            let address = Address {
                scope: "uncertain".into(),
                name: "doc".into(),
                key: "".into(),
            };
            let mut registry = Registry::default();
            registry.register("parent", false, Staged(address.clone()));
            registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
            let owner = scheduler.owner();
            scheduler
                .ts()
                .save(&[
                    Save::Create(&task(
                        1,
                        TaskOwner::Conversation(Uuid::from_u128(9)),
                        "parent",
                        false,
                        3,
                    )),
                    Save::Create(&task(
                        2,
                        TaskOwner::Conversation(Uuid::from_u128(10)),
                        "missing",
                        false,
                        3,
                    )),
                ])
                .unwrap();
            let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
                panic!("reservation")
            };
            let completed = invocation.execute().unwrap();
            let attempt = completed.running.task.attempt_id();
            scheduler.inject_preparation_error = if injection == 3 { 0 } else { injection };
            scheduler.inject_post_commit_error = injection == 3;
            let pending = match scheduler.try_settle_invocation(completed) {
                Err(
                    SettlementError::IndeterminateOwned { pending, .. }
                    | SettlementError::IndeterminatePrepared { pending, .. },
                ) => pending,
                result => panic!("expected retained input: {result:?}"),
            };
            assert_eq!(pending.task_id(), Uuid::from_u128(1));
            if injection != 3 {
                let completed = pending.completed().expect("original invocation");
                assert!(
                    matches!(completed.handler_result(), Some(Ok(Step::Succeed(value))) if *value == json!("result"))
                );
                let docs = completed.staged_documents().unwrap();
                assert_eq!(
                    docs.writes().get(&address).unwrap().as_ref().unwrap()["value"],
                    json!("retained")
                );
                assert!(docs.signal_batch().mutations.iter().any(|mutation| matches!(mutation, durable_store::Mutation::Put { key, .. } if key.starts_with(b"durable/signal/"))));
                let child = docs.keyed_children().next().unwrap();
                assert_eq!(child.key, "child");
                assert_eq!(child.tool, "tool");
                assert_eq!(child.task.id(), Uuid::from_u128(3));
            }
            match &*pending {
                PendingSettlement::Unprepared(completed) => {
                    assert_eq!(completed.running.task.attempt_id(), attempt);
                    let InvocationResult::Handler(Ok(Step::Succeed(value)), documents) =
                        &completed.result
                    else {
                        panic!("original result")
                    };
                    assert_eq!(*value, json!("result"));
                    assert_eq!(documents.keyed_children.len(), 1);
                    let prepared = (**documents).clone().prepare(&completed.running).unwrap();
                    assert!(prepared.document_batch.mutations.iter().any(|mutation| matches!(mutation, durable_store::Mutation::Put { key, .. } if key.starts_with(b"durable/signal/"))));
                    assert!(prepared.document_batch.mutations.len() >= 2);
                    assert_eq!(prepared.keyed_children.len(), 1);
                }
                PendingSettlement::Prepared { settlement, .. } if injection == 3 => {
                    assert_eq!(settlement.running.task.attempt_id(), attempt);
                    assert!(settlement.staged_children.contains_key(&Uuid::from_u128(3)));
                    assert_eq!(
                        Documents::new(owner.store())
                            .get(&address)
                            .unwrap()
                            .unwrap()
                            .value["value"],
                        json!("retained")
                    );
                    let token = SignalToken::new("answer", 1).unwrap();
                    assert_eq!(
                        scheduler
                            .ts()
                            .read_typed_signal(Uuid::from_u128(2), &token, "key")
                            .unwrap(),
                        Some(json!(7))
                    );
                }
                _ => panic!("retained input or transaction required"),
            }
            let stored = scheduler
                .ts()
                .load(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .task;
            let expected = if injection == 3 {
                TaskState::Completing(json!("result"))
            } else {
                TaskState::Running
            };
            assert_eq!(stored.state(), &expected);
            let retained = match scheduler.try_resume_pending(*pending) {
                Err(SettlementError::Rejected {
                    error: Error::Poisoned,
                    completed,
                }) => Box::new(PendingSettlement::Unprepared(*completed)),
                Err(SettlementError::RejectedPrepared {
                    error: Error::Poisoned,
                    pending,
                }) => pending,
                result => panic!("blind retry must be rejected: {result:?}"),
            };
            drop(scheduler);
            assert!(matches!(
                Scheduler::attach(owner.clone(), registry.clone()),
                Err(Error::AlreadyAttached)
            ));
            drop(retained);
            assert!(Scheduler::attach(owner.clone(), registry).is_ok());
        }
    }

    #[test]
    fn uncertain_abort_commit_retains_original_outcome_and_lease() {
        for injection in [1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            registry.register_with_abort(
                "work",
                false,
                |_: &Task| Ok(Step::Succeed(json!(null))),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
            let owner = scheduler.owner();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "work",
                    false,
                    3,
                ))
                .unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
            let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
                panic!("abort reservation")
            };
            let completed = invocation.execute().unwrap();
            assert!(matches!(completed.result, InvocationResult::Abort(None)));
            scheduler.inject_abort_commit_error = injection;
            let pending = match scheduler.try_settle_invocation(completed) {
                Err(SettlementError::IndeterminateOwned { pending, .. }) => pending,
                result => panic!("expected retained abort: {result:?}"),
            };
            assert!(
                matches!(&*pending, PendingSettlement::Unprepared(completed) if matches!(completed.result, InvocationResult::Abort(None)))
            );
            let expected = if injection == 1 {
                TaskState::Running
            } else {
                TaskState::Cancelled
            };
            assert_eq!(
                scheduler
                    .ts()
                    .load(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .task
                    .state(),
                &expected
            );
            let retained = match scheduler.try_resume_pending(*pending) {
                Err(SettlementError::Rejected {
                    error: Error::Poisoned,
                    completed,
                }) => completed,
                result => panic!("blind retry must be rejected: {result:?}"),
            };
            drop(scheduler);
            assert!(matches!(
                Scheduler::attach(owner.clone(), registry.clone()),
                Err(Error::AlreadyAttached)
            ));
            drop(retained);
            assert!(Scheduler::attach(owner.clone(), registry).is_ok());
        }
    }

    #[test]
    fn uncertain_failure_commit_retains_reason_candidate_and_lease() {
        for resumed in [false, true] {
            for injection in [1, 2] {
                let dir = tempfile::tempdir().unwrap();
                let mut registry = Registry::default();
                registry.register("work", false, |_: &Task| {
                    Ok(Step::Fail("exact failure".into()))
                });
                registry.register("independent", false, |_: &Task| {
                    Ok(Step::Succeed(json!("other")))
                });
                let mut scheduler =
                    Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
                let owner = scheduler.owner();
                scheduler
                    .submit(&task(
                        1,
                        TaskOwner::Conversation(Uuid::from_u128(9)),
                        "work",
                        false,
                        3,
                    ))
                    .unwrap();
                scheduler
                    .submit(&task(
                        2,
                        TaskOwner::Conversation(Uuid::from_u128(10)),
                        "independent",
                        false,
                        3,
                    ))
                    .unwrap();
                let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap()
                else {
                    panic!("reservation")
                };
                let TickReservation::Invocation(other) = scheduler.reserve_tick(0).unwrap() else {
                    panic!("other reservation")
                };
                let other = other.execute().unwrap();
                let completed = invocation.execute().unwrap();
                let attempt = completed.running.task.attempt_id();
                scheduler.inject_failure_commit_error = injection;
                let result = if resumed {
                    let CompletedInvocation { running, lease, .. } = completed;
                    let settlement = FailureSettlement {
                        attachment: scheduler.attachment.clone(),
                        running,
                        reason: "exact failure".into(),
                        completed: true,
                        check_pending: true,
                        attempt: 0,
                        candidate: None,
                    };
                    scheduler.try_resume_pending(PendingSettlement::Failure { lease, settlement })
                } else {
                    scheduler.try_settle_invocation(completed)
                };
                let pending = match result {
                    Err(SettlementError::IndeterminateOwned { pending, .. }) => pending,
                    result => panic!("expected retained failure: {result:?}"),
                };
                assert_eq!(pending.attempt_id(), attempt);
                assert_eq!(pending.failure_reason(), Some("exact failure"));
                assert_eq!(
                    pending.candidate_task().unwrap().state(),
                    &TaskState::Failed("exact failure".into())
                );
                match &*pending {
                    PendingSettlement::Failure { settlement, .. } => {
                        assert_eq!(settlement.reason, "exact failure");
                        assert_eq!(settlement.running.task.attempt_id(), attempt);
                        let candidate = settlement.candidate.as_ref().unwrap();
                        assert_eq!(
                            candidate.task.task.state(),
                            &TaskState::Failed("exact failure".into())
                        );
                        assert_eq!(candidate.task.task.retired_attempt_id(), attempt);
                    }
                    _ => panic!("owned failure required"),
                }
                let stored = scheduler
                    .ts()
                    .load(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .task;
                let expected = if injection == 1 {
                    TaskState::Running
                } else {
                    TaskState::Failed("exact failure".into())
                };
                assert_eq!(stored.state(), &expected);
                let pending = match scheduler.try_resume_pending(*pending) {
                    Err(SettlementError::RejectedPrepared {
                        error: Error::Poisoned,
                        pending,
                    }) => pending,
                    result => panic!("blind retry must be rejected: {result:?}"),
                };
                let other = match scheduler.try_settle_invocation(other) {
                    Err(SettlementError::Rejected {
                        error: Error::Poisoned,
                        completed,
                    }) => completed,
                    result => panic!("independent outcome must remain owned: {result:?}"),
                };
                assert_eq!(other.task_id(), Uuid::from_u128(2));
                drop(scheduler);
                assert!(matches!(
                    Scheduler::attach(owner.clone(), registry.clone()),
                    Err(Error::AlreadyAttached)
                ));
                drop(pending);
                assert!(matches!(
                    Scheduler::attach(owner.clone(), registry.clone()),
                    Err(Error::AlreadyAttached)
                ));
                drop(other);
                assert!(Scheduler::attach(owner.clone(), registry).is_ok());
            }
        }
    }

    #[test]
    fn uncertain_prepared_commit_returns_owned_candidate_and_keeps_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
        let owner = scheduler.owner();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "work",
                false,
                3,
            ))
            .unwrap();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("reservation")
        };
        let completed = invocation.execute().unwrap();
        let attempt = completed.running.task.attempt_id();
        scheduler.inject_post_commit_error = true;
        let pending = match scheduler.try_settle_invocation(completed) {
            Err(SettlementError::IndeterminatePrepared {
                error: Error::Store(_),
                pending,
            }) => pending,
            result => panic!("expected owned indeterminate candidate: {result:?}"),
        };
        assert_eq!(pending.task_id(), Uuid::from_u128(1));
        match &*pending {
            PendingSettlement::Prepared { settlement, .. } => {
                assert!(settlement.commit_uncertain);
                assert_eq!(settlement.running.task.attempt_id(), attempt);
                assert_eq!(
                    settlement.candidate.task.state(),
                    &TaskState::Succeeded(json!("done"))
                );
            }
            _ => panic!("prepared result required"),
        }
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
        assert!(
            matches!(scheduler.try_resume_pending(*pending), Err(SettlementError::RejectedPrepared { error: Error::Poisoned, pending }) if {
                assert_eq!(pending.task_id(), Uuid::from_u128(1));
                drop(scheduler);
                assert!(matches!(Scheduler::attach(owner.clone(), registry), Err(Error::AlreadyAttached)));
                true
            })
        );
    }

    #[test]
    fn uncertain_post_commit_error_blocks_other_result_without_dropping_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "work", false, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "work", false, 3)).unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a = a.execute().unwrap();
        let b = b.execute().unwrap();
        scheduler.inject_post_commit_error = true;
        assert!(matches!(
            scheduler.settle_invocation(a),
            Err(SettlementError::IndeterminatePrepared {
                error: Error::Store(_),
                ..
            })
        ));
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
        let b = match scheduler.settle_invocation(b) {
            Err(SettlementError::Rejected {
                error: Error::Poisoned,
                completed,
            }) => completed,
            result => panic!("expected retained B: {result:?}"),
        };
        assert_eq!(b.task_id(), Uuid::from_u128(2));
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Poisoned)));
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
    }

    #[test]
    fn concurrent_handler_failure_drains_other_staged_documents() {
        use crate::document::{Address, Documents, TaskDocuments};
        use std::{sync::mpsc, time::Duration};
        struct Staging {
            started: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let address = Address {
                    scope: "shared".into(),
                    name: "result".into(),
                    key: "b".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(&address, json!({"value": 42}).as_object().unwrap().clone())
                    .map_err(|e| e.to_string())?;
                self.started.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
                Ok(Step::Succeed(json!("b")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let (a_started_tx, a_started_rx) = mpsc::channel();
        let (a_release_tx, a_release_rx) = mpsc::channel();
        let a_release_rx = Mutex::new(a_release_rx);
        let (b_started_tx, b_started_rx) = mpsc::channel();
        let (b_release_tx, b_release_rx) = mpsc::channel();
        let mut registry = Registry::default();
        registry.register("a", false, move |_: &Task| {
            a_started_tx.send(()).unwrap();
            a_release_rx.lock().unwrap().recv().unwrap();
            Err("failure".into())
        });
        registry.register(
            "b",
            false,
            Staging {
                started: b_started_tx,
                release: Mutex::new(b_release_rx),
            },
        );
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "a", false, 3))
            .unwrap();
        scheduler
            .submit(&task(2, owner.clone(), "b", false, 3))
            .unwrap();
        scheduler.submit(&task(3, owner, "b", false, 3)).unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a = std::thread::spawn(move || a.execute().unwrap());
        let b = std::thread::spawn(move || b.execute().unwrap());
        a_started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        b_started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        a_release_tx.send(()).unwrap();
        let a = a.join().unwrap();
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        b_release_tx.send(()).unwrap();
        let b = b.join().unwrap();
        assert!(matches!(
            scheduler.settle_invocation(a),
            Err(SettlementError::Handler(_))
        ));
        scheduler.settle_invocation(b).unwrap();
        let address = Address {
            scope: "shared".into(),
            name: "result".into(),
            key: "b".into(),
        };
        let doc = Documents::new(scheduler.owner().store())
            .get(&address)
            .unwrap()
            .unwrap();
        assert_eq!(doc.value["value"], 42);
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("b"))
        );
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(3))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn handler_panic_publication_is_ordered_with_other_commit() {
        use std::{sync::mpsc, time::Duration};
        struct NotifyOnUnwind(mpsc::Sender<()>);
        impl Drop for NotifyOnUnwind {
            fn drop(&mut self) {
                self.0.send(()).unwrap();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let (go_tx, go_rx) = mpsc::channel();
        let go_rx = Mutex::new(go_rx);
        let (panicking_tx, panicking_rx) = mpsc::channel();
        let panicking_rx = Mutex::new(panicking_rx);
        let mut registry = Registry::default();
        registry.register("panic", false, move |_: &Task| -> Result<Step, String> {
            go_rx.lock().unwrap().recv().unwrap();
            let _notify = NotifyOnUnwind(panicking_tx.clone());
            panic!("handler panic");
        });
        registry.register("work", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "panic", false, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "work", false, 3)).unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let a = std::thread::spawn(move || a.execute());
        let b = b.execute().unwrap();
        let state = scheduler.live_invocations.clone();
        scheduler.result_commit_hook = Some(Arc::new(move |attempt, _| {
            if attempt == 0 {
                go_tx.send(()).unwrap();
                panicking_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                assert!(!state.lock().unwrap().poisoned);
            }
        }));
        scheduler.settle_invocation(b).unwrap();
        assert!(a.join().is_err());
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn published_handler_panic_retains_other_result() {
        use crate::document::{Address, Documents, TaskDocuments};
        struct Staged(Address);
        impl Handler for Staged {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                docs.get(&self.0).unwrap();
                docs.put(
                    &self.0,
                    json!({"value": "retained"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("done")))
            }
        }
        let address = Address {
            scope: "panic".into(),
            name: "drain".into(),
            key: "".into(),
        };
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("panic", false, |_: &Task| -> Result<Step, String> {
            panic!("handler panic")
        });
        registry.register("work", false, Staged(address.clone()));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "panic", false, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "work", false, 3)).unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        let b = b.execute().unwrap();
        assert!(std::thread::spawn(move || a.execute()).join().is_err());
        scheduler.settle_invocation(b).unwrap();
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        assert_eq!(
            scheduler
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
        assert_eq!(
            Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"],
            json!("retained")
        );
    }

    #[test]
    fn prepared_independent_result_drains_after_handler_panic() {
        use crate::document::{Address, Documents, TaskDocuments};
        struct Staged(Address);
        impl Handler for Staged {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                docs.get(&self.0).unwrap();
                docs.put(
                    &self.0,
                    json!({"value": "prepared"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("done")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let address = Address {
            scope: "panic".into(),
            name: "prepared".into(),
            key: "".into(),
        };
        let mut registry = Registry::default();
        registry.register("panic", false, |_: &Task| -> Result<Step, String> {
            panic!("handler panic")
        });
        registry.register("work", false, Staged(address.clone()));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .ts()
            .save(&[
                Save::Create(&task(1, owner.clone(), "panic", false, 3)),
                Save::Create(&task(2, owner.clone(), "work", false, 3)),
                Save::Create(&task(3, owner, "missing", false, 3)),
            ])
            .unwrap();
        let TickReservation::Invocation(a) = scheduler.reserve_tick(0).unwrap() else {
            panic!("A")
        };
        let TickReservation::Invocation(b) = scheduler.reserve_tick(0).unwrap() else {
            panic!("B")
        };
        scheduler.yield_after_graph_conflict = true;
        scheduler.result_commit_hook = Some(Arc::new(|_, store| {
            let tasks = TaskStore::new(store);
            let other = tasks.load(Uuid::from_u128(3)).unwrap().unwrap();
            tasks.update(&other, |_| Ok(())).unwrap();
        }));
        let pending = match scheduler.settle_invocation(b.execute().unwrap()) {
            Err(SettlementError::Yielded { pending }) => *pending,
            result => panic!("expected prepared yield: {result:?}"),
        };
        assert!(matches!(pending, PendingSettlement::Prepared { .. }));
        scheduler.yield_after_graph_conflict = false;
        scheduler.result_commit_hook = None;
        assert!(std::thread::spawn(move || a.execute()).join().is_err());
        assert!(matches!(scheduler.reserve_tick(0), Err(Error::Stopped)));
        scheduler.try_resume_pending(pending).unwrap();
        assert_eq!(
            Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"],
            json!("prepared")
        );
    }

    #[test]
    fn detached_handler_allows_another_reservation_without_recovery() {
        use std::{sync::mpsc, time::Duration};
        let dir = tempfile::tempdir().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = Registry::default();
        registry.register("slow", true, move |_: &Task| {
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(json!("slow")))
        });
        registry.register("quick", true, |_: &Task| Ok(Step::Succeed(json!("quick"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "slow", true, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "quick", true, 3)).unwrap();
        let TickReservation::Invocation(slow) = scheduler.reserve_tick(0).unwrap() else {
            panic!("slow reservation")
        };
        assert_eq!(slow.task_id(), Uuid::from_u128(1));
        let thread = std::thread::spawn(move || slow.execute().unwrap());
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            scheduler.replace_registry(registry),
            Err(Error::Busy)
        ));
        scheduler.reconcile().unwrap();
        let slow_record = scheduler.get(Uuid::from_u128(1)).unwrap().unwrap();
        assert_eq!(slow_record.state(), &TaskState::Running);
        assert_eq!(slow_record.restarts(), 0);
        let TickReservation::Invocation(quick) = scheduler.reserve_tick(0).unwrap() else {
            panic!("quick reservation")
        };
        assert_eq!(quick.task_id(), Uuid::from_u128(2));
        scheduler
            .settle_invocation(quick.execute().unwrap())
            .unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("quick"))
        );
        assert!(matches!(
            scheduler.reserve_tick(0).unwrap(),
            TickReservation::Progress(false)
        ));
        release_tx.send(()).unwrap();
        scheduler.settle_invocation(thread.join().unwrap()).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("slow"))
        );
    }

    #[test]
    fn detached_abort_handler_preserves_catch_and_drains_after_concurrent_work() {
        use std::{sync::mpsc, time::Duration};
        let dir = tempfile::tempdir().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = Registry::default();
        registry.register_with_abort(
            "aborted",
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            move |_: &Task| {
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                panic!("abort panic")
            },
        );
        registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!("other"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "aborted", true, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "other", true, 3)).unwrap();
        scheduler.request_abort(Uuid::from_u128(1)).unwrap();
        let TickReservation::Invocation(abort) = scheduler.reserve_tick(0).unwrap() else {
            panic!("abort reservation")
        };
        let thread = std::thread::spawn(move || abort.execute().unwrap());
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        scheduler.reconcile().unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Running
        );
        let TickReservation::Invocation(other) = scheduler.reserve_tick(0).unwrap() else {
            panic!("other reservation")
        };
        scheduler
            .settle_invocation(other.execute().unwrap())
            .unwrap();
        release_tx.send(()).unwrap();
        scheduler.settle_invocation(thread.join().unwrap()).unwrap();
        assert!(
            matches!(scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(), TaskState::Failed(reason) if reason == "abort handler panicked")
        );
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("other"))
        );
    }

    #[test]
    fn detached_lease_fences_reattachment_and_abandonment() {
        use std::{sync::mpsc, time::Duration};
        let dir = tempfile::tempdir().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = Registry::default();
        registry.register("job", true, move |_: &Task| {
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(json!(null)))
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "job",
                true,
                3,
            ))
            .unwrap();
        let owner = scheduler.owner();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("reservation")
        };
        let worker = std::thread::spawn(move || invocation.execute().unwrap());
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(scheduler);
        assert!(matches!(
            Scheduler::attach(owner.clone(), registry.clone()),
            Err(Error::AlreadyAttached)
        ));
        release_tx.send(()).unwrap();
        let completed = worker.join().unwrap();
        assert!(matches!(
            Scheduler::attach(owner.clone(), registry.clone()),
            Err(Error::AlreadyAttached)
        ));
        drop(completed);
        let recovered = Scheduler::attach(owner, registry).unwrap();
        let task = recovered.get(Uuid::from_u128(1)).unwrap().unwrap();
        assert_eq!(task.state(), &TaskState::Ready);
        assert_eq!(task.restarts(), 1);
    }

    #[test]
    fn abort_of_detached_running_attempt_discards_result_before_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register_with_abort(
            "job",
            true,
            |_: &Task| Ok(Step::Succeed(json!("discarded"))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "job",
                true,
                3,
            ))
            .unwrap();
        let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
            panic!("reservation")
        };
        scheduler.request_abort(Uuid::from_u128(1)).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Running
        );
        scheduler
            .settle_invocation(invocation.execute().unwrap())
            .unwrap();
        let pending = scheduler.get(Uuid::from_u128(1)).unwrap().unwrap();
        assert_eq!(pending.state(), &TaskState::Ready);
        assert!(pending.abort_requested());
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
    }

    #[test]
    fn task_tool_cleanup_faults_are_terminal_and_other_children_finish_after_reopen() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for parent_fails in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let counts: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
            let make_registry = || {
                let mut registry = Registry::default();
                registry.register("parent", true, move |_: &Task| {
                    Ok(if parent_fails {
                        Step::Fail("parent failure".into())
                    } else {
                        Step::Succeed(json!("parent success"))
                    })
                });
                for (index, kind) in ["err", "panic", "invalid", "ok"].into_iter().enumerate() {
                    let count = counts[index].clone();
                    registry.register_with_abort(
                        kind,
                        true,
                        |_: &Task| -> Result<Step, String> { panic!("child must not run") },
                        move |_: &Task| {
                            count.fetch_add(1, Ordering::SeqCst);
                            match index {
                                0 => Err("cleanup error".into()),
                                1 => panic!("cleanup panic"),
                                2 => Ok(Step::Continue(cp("invalid"))),
                                _ => Ok(Step::Succeed(json!(null))),
                            }
                        },
                    );
                }
                registry
            };
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                4,
            )
            .into_task_tool();
            let children: Vec<_> = ["err", "panic", "invalid", "ok"]
                .into_iter()
                .enumerate()
                .map(|(i, kind)| {
                    let mut child =
                        task(i as u128 + 2, TaskOwner::Task(parent.id()), kind, true, 4)
                            .into_task_tool();
                    child.start().unwrap();
                    child.wait(WaitFor::Signal("hold".into())).unwrap();
                    child
                })
                .collect();
            let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
            let mut writes = vec![Save::Create(&parent)];
            writes.extend(children.iter().map(Save::Create));
            scheduler.ts().save(&writes).unwrap();
            assert!(scheduler.tick(0).unwrap()); // parent outcome held
            assert!(scheduler.tick(0).unwrap()); // all children marked
            assert!(scheduler.tick(0).unwrap()); // first cleanup fault commits
            assert!(
                children.iter().any(|child| scheduler
                    .get(child.id())
                    .unwrap()
                    .unwrap()
                    .is_terminal())
            );
            assert!(scheduler.get(parent.id()).unwrap().unwrap().is_completing());
            drop(scheduler);
            let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
            for _ in 0..12 {
                if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
                assert!(scheduler.tick(0).unwrap());
            }
            let parent_state = scheduler.get(parent.id()).unwrap().unwrap();
            if parent_fails {
                assert_eq!(
                    parent_state.state(),
                    &TaskState::Failed("parent failure".into())
                );
            } else {
                assert_eq!(
                    parent_state.state(),
                    &TaskState::Succeeded(json!("parent success"))
                );
            }
            let states: Vec<_> = children
                .iter()
                .map(|child| scheduler.get(child.id()).unwrap().unwrap().state().clone())
                .collect();
            assert_eq!(
                states[0],
                TaskState::Failed("abort handler failed: cleanup error".into())
            );
            assert_eq!(
                states[1],
                TaskState::Failed("abort handler panicked".into())
            );
            assert_eq!(
                states[2],
                TaskState::Failed("abort handler must return Succeed or Fail".into())
            );
            assert_eq!(states[3], TaskState::Cancelled);
            assert!(counts.iter().all(|count| count.load(Ordering::SeqCst) == 1));
            drop(scheduler);
            let scheduler = Scheduler::open(&path, make_registry()).unwrap();
            assert_eq!(
                scheduler.get(parent.id()).unwrap().unwrap().state(),
                parent_state.state()
            );
            assert!(counts.iter().all(|count| count.load(Ordering::SeqCst) == 1));
        }
    }

    #[cfg(unix)]
    #[test]
    fn task_tool_crash_fixture() {
        use std::io::Write;
        let Ok(path) = std::env::var("PI_TASK_TOOL_CRASH_DB") else {
            return;
        };
        let fail = std::env::var_os("PI_TASK_TOOL_CRASH_FAIL").is_some();
        let owner = Arc::new(OwnedStore::open(path).unwrap());
        let store_owner = owner.clone();
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register("nested", false, move |_: &Task| {
            let store = TaskStore::new(store_owner.store());
            let parent = store.load(Uuid::from_u128(9)).unwrap().unwrap();
            store
                .update(&parent, |task| {
                    task.start()?;
                    if fail {
                        task.hold_failure("failed".into())
                    } else {
                        task.hold_success(json!("done"))
                    }
                })
                .unwrap();
            println!("TASK_TOOL_NESTED_RUNNING");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        });
        registry.register("generic", true, |_: &Task| {
            Ok(Step::Succeed(json!("generic")))
        });
        let mut scheduler = Scheduler::attach(owner, registry).unwrap();
        let parent = task(
            9,
            TaskOwner::Conversation(Uuid::from_u128(99)),
            "parent",
            false,
            4,
        )
        .into_task_tool();
        let nested = task(1, TaskOwner::Task(parent.id()), "nested", false, 4).into_task_tool();
        let mut generic = task(2, TaskOwner::Task(parent.id()), "generic", true, 4);
        generic.start().unwrap();
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&nested),
                Save::Create(&generic),
            ])
            .unwrap();
        scheduler.tick(0).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn task_tool_crash_recovery_fences_failed_owned_graph() {
        use std::{
            io::{BufRead, BufReader},
            process::{Child, Command, Stdio},
            sync::mpsc,
            time::Duration,
        };
        struct KillOnDrop(Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for fail in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "task_scheduler::tests::task_tool_crash_fixture",
                    "--nocapture",
                ])
                .env("PI_TASK_TOOL_CRASH_DB", &path)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            if fail {
                command.env("PI_TASK_TOOL_CRASH_FAIL", "1");
            }
            let mut child = KillOnDrop(command.spawn().unwrap());
            let stdout = child.0.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    if line.unwrap().contains("TASK_TOOL_NESTED_RUNNING") {
                        let _ = tx.send(());
                        break;
                    }
                }
            });
            rx.recv_timeout(Duration::from_secs(20))
                .expect("nested handler did not start");
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let mut registry = Registry::default();
            registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.register_with_abort(
                "nested",
                false,
                |_: &Task| panic!("must not replay"),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            registry.register_with_abort(
                "generic",
                true,
                |_: &Task| Ok(Step::Succeed(json!("generic"))),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            let nested = Uuid::from_u128(1);
            let generic = Uuid::from_u128(2);
            let parent = Uuid::from_u128(9);
            assert_eq!(
                scheduler.get(nested).unwrap().unwrap().state(),
                &TaskState::Running
            );
            assert_eq!(
                scheduler.get(generic).unwrap().unwrap().state(),
                if fail {
                    &TaskState::Running
                } else {
                    &TaskState::Ready
                }
            );
            assert!(!scheduler.get(generic).unwrap().unwrap().abort_requested());
            scheduler.tick(0).unwrap();
            assert!(scheduler.get(nested).unwrap().unwrap().abort_requested());
            assert_eq!(
                scheduler.get(nested).unwrap().unwrap().state(),
                &TaskState::Running
            );
            assert_eq!(
                scheduler.get(generic).unwrap().unwrap().abort_requested(),
                fail
            );
            for _ in 0..12 {
                scheduler.tick(0).unwrap();
                if scheduler.get(parent).unwrap().unwrap().is_terminal() {
                    break;
                }
            }
            assert_eq!(
                scheduler.get(nested).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            let expected_generic = if fail {
                TaskState::Cancelled
            } else {
                TaskState::Succeeded(json!("generic"))
            };
            assert_eq!(
                scheduler.get(generic).unwrap().unwrap().state(),
                &expected_generic
            );
            let result = scheduler.get(parent).unwrap().unwrap();
            if fail {
                assert_eq!(result.state(), &TaskState::Failed("failed".into()));
            } else {
                assert_eq!(result.state(), &TaskState::Succeeded(json!("done")));
            }
        }
    }

    #[test]
    fn task_tool_child_staged_with_return_is_admitted_then_aborted() {
        use crate::{document::TaskDocuments, task_tool::TaskTool};
        use pi_rs_core::ToolSpec;
        struct Parent {
            child: Arc<TaskTool>,
            id: Arc<Mutex<Option<Uuid>>>,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                parent: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                *self.id.lock().unwrap() = Some(
                    self.child
                        .start_child(parent, docs, "nested", json!({}))?
                        .task_id(),
                );
                Ok(Step::Succeed(json!("result")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let child_handler: Arc<dyn Handler> = Arc::new(|_: &Task| -> Result<Step, String> {
            panic!("nested handler ran during settlement")
        });
        let child = Arc::new(TaskTool::new(
            ToolSpec {
                name: "child".into(),
                description: "".into(),
                parameters: json!({"type":"object"}),
            },
            "child",
            true,
            1,
            child_handler.clone(),
        ));
        let id = Arc::new(Mutex::new(None));
        let parent_handler: Arc<dyn Handler> = Arc::new(Parent {
            child,
            id: id.clone(),
        });
        let mut registry = Registry::default();
        registry.register_shared("parent", true, parent_handler);
        registry.register_shared("child", true, child_handler);
        registry.set_abort("child", |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        )
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let child = id.lock().unwrap().unwrap();
        assert!(scheduler.get(parent.id()).unwrap().unwrap().is_completing());
        assert!(scheduler.get(child).unwrap().unwrap().is_task_tool());
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.get(child).unwrap().unwrap().abort_requested());
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(child).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("result"))
        );
    }

    #[test]
    fn task_tool_settlement_preserves_ownership_across_restart() {
        for scenario in ["success", "failure", "error", "interrupted"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tasks.db");
            let mut registry = Registry::default();
            let success = scenario == "success";
            let error = scenario == "error";
            registry.register("parent", false, move |_: &Task| {
                if error {
                    return Err("handler error".into());
                }
                Ok(if success {
                    Step::Succeed(json!("ok"))
                } else {
                    Step::Fail("failed".into())
                })
            });
            registry.register_with_abort(
                "child",
                true,
                |_: &Task| Ok(Step::Succeed(json!("ordinary"))),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            registry.register_with_abort(
                "child-failed-cleanup",
                true,
                |_: &Task| Ok(Step::Succeed(json!("ordinary"))),
                |_: &Task| Ok(Step::Fail("cleanup failed".into())),
            );
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                4,
            )
            .into_task_tool();
            if scenario == "interrupted" {
                parent.start().unwrap();
            }
            let mut a = task(2, TaskOwner::Task(parent.id()), "child", true, 4).into_task_tool();
            let mut b = task(
                3,
                TaskOwner::Task(parent.id()),
                "child-failed-cleanup",
                true,
                4,
            )
            .into_task_tool();
            // Same handler kind as nested children, but generic: never identify by kind.
            let mut generic = task(4, TaskOwner::Task(parent.id()), "child", true, 4);
            let mut external = task(
                5,
                TaskOwner::Conversation(Uuid::from_u128(10)),
                "child",
                true,
                4,
            );
            for t in [&mut a, &mut b, &mut generic, &mut external] {
                t.start().unwrap();
                t.wait(WaitFor::Signal("release".into())).unwrap();
            }
            {
                let scheduler = Scheduler::open(&path, registry.clone()).unwrap();
                scheduler
                    .ts()
                    .save(&[
                        Save::Create(&parent),
                        Save::Create(&a),
                        Save::Create(&b),
                        Save::Create(&generic),
                        Save::Create(&external),
                    ])
                    .unwrap();
            }
            let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
            if scenario != "interrupted" {
                assert!(scheduler.tick(0).unwrap());
            }
            assert!(scheduler.get(parent.id()).unwrap().unwrap().is_completing());
            drop(scheduler); // restart before the cleanup marks are committed
            let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
            assert!(scheduler.wake_snapshot(0).unwrap().active);
            assert!(scheduler.tick(0).unwrap());
            for child in [&a, &b] {
                assert!(
                    scheduler
                        .get(child.id())
                        .unwrap()
                        .unwrap()
                        .abort_requested()
                );
            }
            assert_eq!(
                scheduler
                    .get(generic.id())
                    .unwrap()
                    .unwrap()
                    .abort_requested(),
                !success
            );
            assert!(
                !scheduler
                    .get(external.id())
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            assert!(scheduler.get(parent.id()).unwrap().unwrap().is_completing());
            drop(scheduler); // marks and cleanup must survive another restart
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            for _ in 0..8 {
                if scheduler.get(a.id()).unwrap().unwrap().is_terminal()
                    && scheduler.get(b.id()).unwrap().unwrap().is_terminal()
                {
                    break;
                }
                assert!(scheduler.tick(0).unwrap());
            }
            assert_eq!(
                scheduler.get(a.id()).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            assert!(
                matches!(scheduler.get(b.id()).unwrap().unwrap().state(), TaskState::Failed(reason) if reason == "cleanup failed")
            );
            assert!(scheduler.get(parent.id()).unwrap().unwrap().is_completing());
            if success {
                scheduler
                    .signal(&Signal::new(
                        Uuid::new_v4(),
                        generic.id(),
                        "release".into(),
                        json!(null),
                    ))
                    .unwrap();
            }
            for _ in 0..8 {
                if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
                assert!(scheduler.tick(0).unwrap());
            }
            let result = scheduler.get(parent.id()).unwrap().unwrap();
            if success {
                assert_eq!(result.state(), &TaskState::Succeeded(json!("ok")));
            } else {
                assert!(matches!(result.state(), TaskState::Failed(_)));
            }
            assert!(
                !scheduler
                    .get(external.id())
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            assert!(matches!(
                scheduler.get(external.id()).unwrap().unwrap().state(),
                TaskState::Waiting(_)
            ));
        }
    }

    #[test]
    fn any_wait_on_unowned_task_survives_reopen_and_reports_ended() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut registry = Registry::default();
        registry.register("waiter", true, |task: &Task| {
            if task.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: cp("woken"),
                    condition: WaitFor::TasksAny(vec![Uuid::from_u128(2)]),
                    children: vec![],
                })
            } else {
                assert_eq!(task.ended_children(), Some(&[Uuid::from_u128(2)][..]));
                Ok(Step::Succeed(json!("done")))
            }
        });
        registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!("other"))));
        let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "waiter", true, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "other", true, 3)).unwrap();
        scheduler.tick(0).unwrap();
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        scheduler.tick(0).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn empty_any_wait_resumes_on_next_tick() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("waiter", true, |task: &Task| {
            if task.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: cp("woken"),
                    condition: WaitFor::TasksAny(vec![]),
                    children: vec![],
                })
            } else {
                assert_eq!(task.ended_children(), Some(&[][..]));
                Ok(Step::Succeed(json!(null)))
            }
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "waiter",
                true,
                3,
            ))
            .unwrap();
        scheduler.tick(0).unwrap();
        assert!(scheduler.wake_snapshot(0).unwrap().active);
        scheduler.tick(0).unwrap();
        assert!(
            scheduler
                .get(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .is_terminal()
        );
    }

    #[test]
    fn all_settled_wait_accepts_unowned_success_and_failure() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("waiter", true, |task: &Task| {
            if task.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: cp("woken"),
                    condition: WaitFor::Tasks(vec![Uuid::from_u128(2), Uuid::from_u128(3)]),
                    children: vec![],
                })
            } else {
                assert_eq!(
                    task.ended_children(),
                    Some(&[Uuid::from_u128(2), Uuid::from_u128(3)][..])
                );
                Ok(Step::Succeed(json!("done")))
            }
        });
        registry.register("other", true, |task: &Task| {
            if task.id() == Uuid::from_u128(2) {
                Ok(Step::Fail("failed".into()))
            } else {
                Ok(Step::Succeed(json!("ok")))
            }
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "waiter", true, 3))
            .unwrap();
        scheduler
            .submit(&task(2, owner.clone(), "other", true, 3))
            .unwrap();
        scheduler.submit(&task(3, owner, "other", true, 3)).unwrap();
        for _ in 0..4 {
            assert!(scheduler.tick(0).unwrap());
        }
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Failed("failed".into())
        );
        assert_eq!(
            scheduler.get(Uuid::from_u128(3)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("ok"))
        );
    }

    #[test]
    fn fail_fast_rejects_unowned_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("waiter", true, |_: &Task| {
            Ok(Step::Wait {
                checkpoint: cp("wait"),
                condition: WaitFor::TasksFailFast(vec![Uuid::from_u128(2)]),
                children: vec![],
            })
        });
        registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "waiter", true, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "other", true, 3)).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(
            matches!(scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("invalid task wait"))
        );
        assert!(scheduler.tick(0).unwrap());
        assert!(
            scheduler
                .get(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .is_terminal()
        );
    }

    #[test]
    fn aborting_external_waiter_does_not_abort_its_target() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register_with_abort(
            "waiter",
            true,
            |_: &Task| {
                Ok(Step::Wait {
                    checkpoint: cp("wait"),
                    condition: WaitFor::TasksAny(vec![Uuid::from_u128(2)]),
                    children: vec![],
                })
            },
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!("other"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .submit(&task(1, owner.clone(), "waiter", true, 3))
            .unwrap();
        scheduler.submit(&task(2, owner, "other", true, 3)).unwrap();
        scheduler.tick(0).unwrap();
        scheduler.request_abort(Uuid::from_u128(1)).unwrap();
        assert!(
            !scheduler
                .get(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("other"))
        );
    }

    #[test]
    fn external_wait_rejects_wait_and_ownership_cycles() {
        for owner_cycle in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            registry.register("waiter", true, |_: &Task| {
                Ok(Step::Wait {
                    checkpoint: cp("wait"),
                    condition: WaitFor::TasksAny(vec![Uuid::from_u128(2)]),
                    children: vec![],
                })
            });
            registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let root_owner = TaskOwner::Conversation(Uuid::from_u128(9));
            let waiter_owner = if owner_cycle {
                TaskOwner::Task(Uuid::from_u128(2))
            } else {
                root_owner.clone()
            };
            let waiter = task(1, waiter_owner, "waiter", true, 3);
            let mut other = task(2, root_owner, "other", true, 3);
            if !owner_cycle {
                other.start().unwrap();
                other.wait(WaitFor::TasksAny(vec![waiter.id()])).unwrap();
            }
            scheduler
                .ts()
                .save(&[Save::Create(&waiter), Save::Create(&other)])
                .unwrap();
            assert!(scheduler.tick(0).unwrap());
            assert!(
                matches!(scheduler.get(waiter.id()).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("invalid task wait"))
            );
            assert!(scheduler.tick(0).unwrap());
            assert!(scheduler.get(other.id()).unwrap().unwrap().is_terminal());
        }
    }

    #[test]
    fn new_waited_child_cannot_adopt_an_orphan_cycle() {
        for wait_for_child in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            registry.register("waiter", true, move |parent: &Task| {
                let child = task(2, TaskOwner::Task(parent.id()), "child", true, 3);
                let on = if wait_for_child {
                    vec![child.id()]
                } else {
                    vec![]
                };
                Ok(Step::Wait {
                    checkpoint: cp("wait"),
                    condition: WaitFor::TasksAny(on),
                    children: vec![child],
                })
            });
            registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.register("orphan", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.register("independent", true, |_: &Task| {
                Ok(Step::Succeed(json!("ok")))
            });
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let owner = TaskOwner::Conversation(Uuid::from_u128(9));
            let waiter = task(1, owner.clone(), "waiter", true, 3);
            let mut orphan = task(3, TaskOwner::Task(Uuid::from_u128(2)), "orphan", true, 3);
            let independent = task(4, owner, "independent", true, 3);
            orphan.start().unwrap();
            orphan.wait(WaitFor::TasksAny(vec![waiter.id()])).unwrap();
            scheduler
                .ts()
                .save(&[
                    Save::Create(&waiter),
                    Save::Create(&orphan),
                    Save::Create(&independent),
                ])
                .unwrap();
            assert!(scheduler.tick(0).unwrap());
            assert!(
                matches!(scheduler.get(waiter.id()).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("invalid task wait"))
            );
            assert!(scheduler.get(Uuid::from_u128(2)).unwrap().is_none());
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(
                scheduler.get(independent.id()).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("ok"))
            );
        }
    }

    #[test]
    fn staged_child_cannot_adopt_an_orphan_cycle_on_success() {
        struct Parent(Arc<dyn Handler>);
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                parent: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(parent.id()), "child", true, 3);
                docs.stage_task_tool_child("key", "tool", &json!(null), child, self.0.clone())?;
                Ok(Step::Succeed(json!("not committed")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let child_handler: Arc<dyn Handler> = Arc::new(|_: &Task| Ok(Step::Succeed(json!(null))));
        let mut registry = Registry::default();
        registry.register("waiter", true, Parent(child_handler.clone()));
        registry.register_shared("child", true, child_handler);
        registry.register("orphan", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let waiter = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "waiter",
            true,
            3,
        );
        let mut orphan = task(3, TaskOwner::Task(Uuid::from_u128(2)), "orphan", true, 3);
        orphan.start().unwrap();
        orphan.wait(WaitFor::TasksAny(vec![waiter.id()])).unwrap();
        scheduler
            .ts()
            .save(&[Save::Create(&waiter), Save::Create(&orphan)])
            .unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(
            matches!(scheduler.get(waiter.id()).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("invalid task wait"))
        );
        assert!(scheduler.get(Uuid::from_u128(2)).unwrap().is_none());
    }
    #[test]
    fn completing_survives_reopen_without_parent_definition_and_wakes_child() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        let child = task(2, TaskOwner::Task(root.id()), "child", true, 4);
        let mut registry = Registry::default();
        registry.register("parent", true, |_: &Task| {
            Ok(Step::Succeed(json!({"result": 42})))
        });
        registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!("child"))));
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        scheduler
            .ts()
            .save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(json!({"result": 42}))
        );
        assert!(!scheduler.get(root.id()).unwrap().unwrap().is_terminal());
        drop(scheduler);
        let mut registry = Registry::default();
        registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!("child"))));
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert!(scheduler.wake_snapshot(0).unwrap().active);
        assert!(scheduler.blocked_tasks().unwrap().is_empty());
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("child"))
        );
        assert!(scheduler.wake_snapshot(0).unwrap().active);
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!({"result": 42}))
        );
    }

    #[test]
    fn abort_of_completing_parent_preserves_outcome_and_drains_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("parent", true, |_: &Task| Ok(Step::Succeed(json!(7))));
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| Ok(Step::Succeed(json!(0))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(dir.path().join("tasks.db"), registry).unwrap();
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        let child = task(2, TaskOwner::Task(root.id()), "child", true, 4);
        scheduler
            .ts()
            .save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        scheduler.tick(0).unwrap();
        scheduler.request_abort(root.id()).unwrap();
        let held = scheduler.get(root.id()).unwrap().unwrap();
        assert_eq!(held.state(), &TaskState::Completing(json!(7)));
        assert!(held.abort_requested());
        assert!(
            scheduler
                .get(child.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        scheduler.tick(0).unwrap();
        let finished = scheduler.get(root.id()).unwrap().unwrap();
        assert_eq!(finished.state(), &TaskState::Succeeded(json!(7)));
        assert!(finished.abort_requested());
    }

    #[test]
    fn failed_parent_holds_outcome_and_drains_child_after_reopen() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let aborts = Arc::new(AtomicUsize::new(0));
        let make_registry = || {
            let mut registry = Registry::default();
            registry.register("parent", true, |_: &Task| Ok(Step::Fail("failure".into())));
            let aborts = aborts.clone();
            registry.register_with_abort(
                "child",
                true,
                |_: &Task| panic!("child must be aborted before invocation"),
                move |_: &Task| {
                    aborts.fetch_add(1, Ordering::SeqCst);
                    Ok(Step::Succeed(json!(null)))
                },
            );
            registry
        };
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        let child = task(2, TaskOwner::Task(root.id()), "child", true, 4);
        let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
        scheduler
            .ts()
            .save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::CompletingFailed("failure".into())
        );
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
        assert!(scheduler.wake_snapshot(0).unwrap().active);
        scheduler.tick(0).unwrap();
        assert!(
            scheduler
                .get(child.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Failed("failure".into())
        );
    }

    #[test]
    fn failed_outcome_retries_unrelated_graph_commit() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        let mut parent = task(1, owner.clone(), "parent", true, 3);
        parent.start_attempt(Uuid::new_v4()).unwrap();
        let child = task(2, TaskOwner::Task(parent.id()), "child", true, 3);
        scheduler
            .ts()
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        let running = scheduler.ts().load(parent.id()).unwrap().unwrap();
        let other = task(3, owner, "other", true, 3);
        let mut injected = false;
        scheduler
            .finish_failed_inner_with(&running, "failed".into(), true, || {
                if !injected {
                    injected = true;
                    scheduler.ts().save(&[Save::Create(&other)]).unwrap();
                }
            })
            .unwrap();
        assert!(injected);
        assert_eq!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            &TaskState::CompletingFailed("failed".into())
        );
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Ready
        );
        assert_eq!(
            scheduler.get(other.id()).unwrap().unwrap().state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn owned_failure_settlement_retries_same_result_after_graph_conflict() {
        use crate::document::{Address, Documents, TaskDocuments};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Failure {
            calls: Arc<AtomicUsize>,
            attempt: Arc<Mutex<Option<Uuid>>>,
            address: Address,
        }
        impl Handler for Failure {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                *self.attempt.lock().unwrap() = task.attempt_id();
                docs.get(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value": "discard"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Fail("precise failure".into()))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let address = Address {
            scope: "failure".into(),
            name: "document".into(),
            key: "".into(),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let attempt = Arc::new(Mutex::new(None));
        let retries = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "parent",
            false,
            Failure {
                calls: calls.clone(),
                attempt: attempt.clone(),
                address: address.clone(),
            },
        );
        registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        scheduler.submit(&root).unwrap();
        let observed = retries.clone();
        scheduler.failure_commit_hook = Some(Arc::new(move |iteration, store| {
            observed.fetch_add(1, Ordering::SeqCst);
            if iteration == 0 {
                let child = task(2, TaskOwner::Task(Uuid::from_u128(1)), "child", false, 3);
                let tasks = TaskStore::new(store);
                tasks.save(&[Save::Create(&child)]).unwrap();
                let current = tasks.load(Uuid::from_u128(1)).unwrap().unwrap();
                tasks.update(&current, |_| Ok(())).unwrap();
            }
        }));
        scheduler.tick(0).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(retries.load(Ordering::SeqCst), 2);
        let stored = scheduler.get(root.id()).unwrap().unwrap();
        assert_eq!(
            stored.state(),
            &TaskState::CompletingFailed("precise failure".into())
        );
        assert_eq!(stored.retired_attempt_id(), *attempt.lock().unwrap());
        assert!(stored.retired_attempt_id().is_some());
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Ready
        );
        assert!(
            Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn failure_settlement_remains_owned_when_abort_supersedes_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start_attempt(Uuid::new_v4()).unwrap();
        scheduler.ts().save(&[Save::Create(&parent)]).unwrap();
        let running = scheduler.ts().load(parent.id()).unwrap().unwrap();
        let mut settlement = FailureSettlement {
            attachment: scheduler.attachment.clone(),
            running: running.clone(),
            reason: "retained failure".into(),
            completed: true,
            check_pending: true,
            attempt: 0,
            candidate: None,
        };
        let result = scheduler.commit_failure_settlement(&mut settlement, || {
            scheduler
                .ts()
                .update(&running, |task| task.request_abort())
                .unwrap();
        });
        assert!(matches!(result, Err(Error::PendingAbort(id)) if id == parent.id()));
        assert_eq!(settlement.running, running);
        assert_eq!(settlement.reason, "retained failure");
        let candidate = settlement.candidate.as_ref().unwrap();
        assert_eq!(
            candidate.task.task.state(),
            &TaskState::Failed("retained failure".into())
        );
        assert_eq!(
            candidate.task.task.retired_attempt_id(),
            running.task.attempt_id()
        );
        assert!(!candidate.batch.checks.is_empty());
        assert!(!candidate.batch.mutations.is_empty());
    }

    #[test]
    fn failed_outcome_does_not_retry_changed_parent_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            3,
        );
        parent.start_attempt(Uuid::new_v4()).unwrap();
        scheduler.ts().save(&[Save::Create(&parent)]).unwrap();
        let running = scheduler.ts().load(parent.id()).unwrap().unwrap();
        let mut calls = 0;
        let result = scheduler.finish_failed_inner_with(&running, "failed".into(), true, || {
            calls += 1;
            if calls == 1 {
                scheduler
                    .ts()
                    .update(&running, |task| task.request_abort())
                    .unwrap();
            }
        });
        assert!(matches!(result, Err(Error::PendingAbort(id)) if id == parent.id()));
        assert_eq!(calls, 1);
        let current = scheduler.get(parent.id()).unwrap().unwrap();
        assert!(current.abort_requested());
        assert_eq!(current.state(), &TaskState::Running);
    }

    #[test]
    fn completing_includes_child_created_with_success() {
        struct Parent;
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                parent: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(parent.id()), "child", true, 4);
                docs.stage_keyed_child("child", "tool", child.input(), child.clone())?;
                Ok(Step::Succeed(json!("done")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("parent", true, Parent);
        registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!("child"))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        scheduler.submit(&root).unwrap();
        let forbidden = task(3, TaskOwner::Task(root.id()), "child", true, 4);
        let stale = scheduler
            .ts()
            .prepare_save(&[Save::Create(&forbidden)])
            .unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Completing(json!("done"))
        );
        assert_eq!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            &TaskState::Ready
        );
        assert!(matches!(
            scheduler
                .ts()
                .store()
                .commit(&stale.checks, &stale.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(matches!(
            scheduler.ts().save(&[Save::Create(&forbidden)]),
            Err(task_store::Error::InvalidOwnership)
        ));
        scheduler.tick(0).unwrap();
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn child_commit_after_scan_forces_standalone_abort_retry() {
        use std::sync::mpsc;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        let ts = scheduler.ts();
        let mut root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(100)),
            "job",
            true,
            4,
        );
        root.request_abort().unwrap();
        ts.save(&[Save::Create(&root)]).unwrap();
        let version = ts.load(root.id()).unwrap().unwrap().version;
        let child = task(2, TaskOwner::Task(root.id()), "job", true, 4);
        let owner = scheduler.owner();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let store = owner.store();
            let batch = TaskStore::new(store)
                .prepare_save(&[Save::Create(&child)])
                .unwrap();
            store.commit(&batch.checks, &batch.mutations).unwrap();
            done_tx.send(()).unwrap();
        });
        let mut attempts = 0;
        request_abort_subtree_with(&ts, root.id(), || {
            attempts += 1;
            if attempts == 1 {
                ready_tx.send(()).unwrap();
                done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        })
        .unwrap();
        writer.join().unwrap();
        assert_eq!(attempts, 2);
        assert_eq!(ts.load(root.id()).unwrap().unwrap().version, version);
        assert!(
            ts.load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        drop(scheduler);
        let reopened = Scheduler::open(&path, Registry::default()).unwrap();
        assert!(
            reopened
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn repeated_abort_fences_prepared_child_without_rewriting_reservation() {
        use std::sync::mpsc;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        let mut root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(100)),
            "job",
            true,
            4,
        );
        root.request_abort().unwrap();
        scheduler.ts().save(&[Save::Create(&root)]).unwrap();
        let version = scheduler.ts().load(root.id()).unwrap().unwrap().version;
        let child = task(2, TaskOwner::Task(root.id()), "job", true, 4);
        let owner = scheduler.owner();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let store = owner.store();
            let batch = TaskStore::new(store)
                .prepare_save(&[Save::Create(&child)])
                .unwrap();
            ready_tx.send(()).unwrap();
            release_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            store.commit(&batch.checks, &batch.mutations)
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        scheduler.request_abort(root.id()).unwrap();
        assert_eq!(
            scheduler.ts().load(root.id()).unwrap().unwrap().version,
            version
        );
        release_tx.send(()).unwrap();
        assert!(matches!(
            thread.join().unwrap(),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(scheduler.ts().load(Uuid::from_u128(2)).unwrap().is_none());
        drop(scheduler);
        let reopened = Scheduler::open(&path, Registry::default()).unwrap();
        assert!(reopened.ts().load(Uuid::from_u128(2)).unwrap().is_none());
    }

    #[test]
    fn repeated_abort_retries_when_marked_task_changes() {
        use std::sync::mpsc;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        let ts = scheduler.ts();
        let mut root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(100)),
            "job",
            true,
            4,
        );
        root.request_abort().unwrap();
        ts.save(&[Save::Create(&root)]).unwrap();
        let parent = ts.load(root.id()).unwrap().unwrap();
        let child = task(2, TaskOwner::Task(root.id()), "job", true, 4);
        let owner = scheduler.owner();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            owner
                .store()
                .commit(
                    &[durable_store::Check {
                        key: task_store::key(parent.task.id()),
                        expected: durable_store::Expected::Version(parent.version),
                    }],
                    &[
                        durable_store::Mutation::Put {
                            key: task_store::key(parent.task.id()),
                            value: serde_json::to_vec(&parent.task).unwrap(),
                        },
                        durable_store::Mutation::Put {
                            key: task_store::key(child.id()),
                            value: serde_json::to_vec(&child).unwrap(),
                        },
                    ],
                )
                .unwrap();
            done_tx.send(()).unwrap();
        });
        let mut attempts = 0;
        request_abort_subtree_with(&ts, root.id(), || {
            attempts += 1;
            if attempts == 1 {
                ready_tx.send(()).unwrap();
                done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        })
        .unwrap();
        writer.join().unwrap();
        assert_eq!(attempts, 2);
        assert!(
            ts.load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        drop(scheduler);
        let reopened = Scheduler::open(&path, Registry::default()).unwrap();
        assert!(
            reopened
                .ts()
                .load(Uuid::from_u128(2))
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }

    #[test]
    fn retired_attempt_survives_reopen_and_is_fenced_by_replay_and_abort() {
        use std::sync::Mutex;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let make_registry = || {
            let mut registry = Registry::default();
            let seen = seen.clone();
            registry.register("job", true, move |t: &Task| {
                assert_eq!(t.retired_attempt_id(), None);
                seen.lock().unwrap().push(t.attempt_id().unwrap());
                if t.checkpoint().phase == "start" {
                    Ok(Step::Continue(cp("again")))
                } else {
                    Ok(Step::Wait {
                        checkpoint: cp("waiting"),
                        condition: WaitFor::DeadlineMillis(100),
                        children: vec![],
                    })
                }
            });
            registry
        };
        let id = Uuid::from_u128(1);
        let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(2)),
                "job",
                true,
                5,
            ))
            .unwrap();
        assert!(scheduler.tick(0).unwrap());
        let first = scheduler.ts().load(id).unwrap().unwrap();
        let first_attempt = seen.lock().unwrap()[0];
        assert_eq!(first.task.retired_attempt_id(), Some(first_attempt));
        assert_eq!(first.task.attempt_id(), None);
        let stale_check = task_store::retired_attempt_check(&first, first_attempt).unwrap();
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .load(id)
                .unwrap()
                .unwrap()
                .task
                .retired_attempt_id(),
            Some(first_attempt)
        );
        assert!(scheduler.tick(0).unwrap());
        let second = scheduler.ts().load(id).unwrap().unwrap();
        let second_attempt = seen.lock().unwrap()[1];
        assert_ne!(first_attempt, second_attempt);
        assert_eq!(second.task.retired_attempt_id(), Some(second_attempt));
        assert!(scheduler.ts().store().commit(&[stale_check], &[]).is_err());
        let before_abort = task_store::retired_attempt_check(&second, second_attempt).unwrap();
        scheduler.request_abort(id).unwrap();
        assert!(scheduler.ts().store().commit(&[before_abort], &[]).is_err());
        let aborted = scheduler.ts().load(id).unwrap().unwrap();
        assert!(aborted.task.abort_requested());
        assert_eq!(aborted.task.retired_attempt_id(), None);
        assert!(matches!(
            task_store::retired_attempt_check(&aborted, second_attempt),
            Err(task_store::Error::StaleRetiredAttempt)
        ));
    }

    #[test]
    fn retired_identity_is_recorded_for_committed_success_and_failure() {
        use std::sync::Mutex;
        for fail in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let seen = Arc::new(Mutex::new(None));
            let observed = seen.clone();
            let mut registry = Registry::default();
            registry.register("job", true, move |t: &Task| {
                *observed.lock().unwrap() = t.attempt_id();
                if fail {
                    Ok(Step::Fail("failed".into()))
                } else {
                    Ok(Step::Succeed(json!("done")))
                }
            });
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let t = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(2)),
                "job",
                true,
                5,
            );
            scheduler.submit(&t).unwrap();
            scheduler.tick(0).unwrap();
            let result = scheduler.ts().load(t.id()).unwrap().unwrap();
            let attempt = seen.lock().unwrap().unwrap();
            assert_eq!(result.task.retired_attempt_id(), Some(attempt));
            assert!(task_store::retired_attempt_check(&result, attempt).is_ok());
        }
    }

    #[test]
    fn legacy_record_and_migration_do_not_invent_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let original = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        let mut legacy = serde_json::to_value(&original).unwrap();
        legacy.as_object_mut().unwrap().remove("retired_attempt_id");
        let legacy: Task = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.retired_attempt_id(), None);
        {
            let db = durable_store::Store::open(&path).unwrap();
            TaskStore::new(&db).save(&[Save::Create(&legacy)]).unwrap();
        }
        let mut registry = Registry::default();
        registry.register_versioned(
            "job",
            2,
            true,
            |t: &Task| {
                assert_eq!(t.definition_version(), 2);
                assert!(t.attempt_id().is_some());
                assert_eq!(t.retired_attempt_id(), None);
                Ok(Step::Succeed(json!(null)))
            },
            Some(Arc::new(|input, checkpoint, _| {
                Ok((input.clone(), checkpoint.clone()))
            })),
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        scheduler.tick(0).unwrap();
        assert!(
            scheduler
                .get(original.id())
                .unwrap()
                .unwrap()
                .retired_attempt_id()
                .is_some()
        );
    }

    #[test]
    fn versions_block_and_retry_on_registry_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut r = Registry::default();
        r.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut s = Scheduler::open(&path, r).unwrap();
        let t = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        s.submit(&t).unwrap();
        s.replace_registry(Registry::default()).unwrap();
        assert_eq!(s.blocked_reason(&t), Some(BlockedReason::MissingTask));
        assert_eq!(
            s.blocked_tasks().unwrap(),
            vec![BlockedTask {
                id: t.id(),
                kind: Some("job".into()),
                owner: t.owner().clone(),
                reason: BlockedReason::MissingTask,
            }]
        );
        assert!(!s.tick(0).unwrap());
        let mut r = Registry::default();
        r.register_versioned(
            "job",
            2,
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            None,
        );
        s.replace_registry(r).unwrap();
        assert!(matches!(
            s.blocked_reason(&t),
            Some(BlockedReason::MigrationFailed(_))
        ));
        assert!(!s.tick(0).unwrap());
        let mut r = Registry::default();
        r.register_versioned(
            "job",
            2,
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            Some(Arc::new(|_, _, _| Err("bad data".into()))),
        );
        s.replace_registry(r).unwrap();
        assert!(s.tick(0).unwrap());
        assert_eq!(
            s.blocked_reason(&t),
            Some(BlockedReason::MigrationFailed("bad data".into()))
        );
        assert!(!s.tick(0).unwrap());
        assert_eq!(s.get(t.id()).unwrap().unwrap(), t);
        let mut r = Registry::default();
        r.register_versioned(
            "job",
            2,
            true,
            |t: &Task| {
                assert_eq!(t.input(), &json!({"migrated": true}));
                assert_eq!(t.checkpoint(), &cp("new"));
                Ok(Step::Succeed(json!(1)))
            },
            Some(Arc::new(|_, _, from| {
                assert_eq!(from, 1);
                Ok((json!({"migrated":true}), cp("new")))
            })),
        );
        s.replace_registry(r).unwrap();
        assert!(s.tick(0).unwrap());
        let finished = s.get(t.id()).unwrap().unwrap();
        assert_eq!(finished.definition_version(), 2);
        drop(s);
        let mut r = Registry::default();
        r.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let s = Scheduler::open(&path, r).unwrap();
        assert_eq!(s.blocked_reason(&finished), Some(BlockedReason::TaskTooOld));
        assert!(s.blocked_tasks().unwrap().is_empty());
    }

    #[test]
    fn panicking_migration_blocks_without_mutation_and_retries_on_registry_change() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let calls = Arc::new(AtomicUsize::new(0));
        let migration_calls = calls.clone();
        let mut initial = Registry::default();
        initial.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(&path, initial).unwrap();
        let task = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        scheduler.submit(&task).unwrap();
        let mut registry = Registry::default();
        registry.register_versioned(
            "job",
            2,
            true,
            |_: &Task| panic!("blocked task must not execute"),
            Some(Arc::new(move |_, _, _| {
                migration_calls.fetch_add(1, Ordering::SeqCst);
                panic!("bad migration")
            })),
        );
        scheduler.replace_registry(registry).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(scheduler.get(task.id()).unwrap().unwrap(), task);
        assert_eq!(
            scheduler.blocked_tasks().unwrap()[0].reason,
            BlockedReason::MigrationFailed("migration panicked".into())
        );
        assert!(!scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut replacement = Registry::default();
        replacement.register_versioned(
            "job",
            2,
            true,
            |_: &Task| Ok(Step::Succeed(json!("done"))),
            Some(Arc::new(|_, _, _| Ok((json!(42), cp("new"))))),
        );
        scheduler.replace_registry(replacement).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let finished = scheduler.get(task.id()).unwrap().unwrap();
        assert_eq!(finished.definition_version(), 2);
        assert!(matches!(finished.state(), TaskState::Succeeded(_)));
    }

    #[test]
    fn migration_failure_survives_unrelated_registry_changes_and_retries_after_reopen() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut initial = Registry::default();
        initial.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(&path, initial).unwrap();
        let task = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        scheduler.submit(&task).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut registry = Registry::default();
        registry.register_versioned(
            "job",
            2,
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            Some(Arc::new(move |_, _, _| {
                count.fetch_add(1, Ordering::SeqCst);
                panic!("migration");
            })),
        );
        scheduler.replace_registry(registry.clone()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        scheduler.replace_registry(registry.clone()).unwrap();
        assert!(!scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(scheduler.get(task.id()).unwrap(), Some(task.clone()));
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(scheduler.get(task.id()).unwrap(), Some(task));
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn abort_with_panicking_migration_persists_orphan_without_running_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut initial = Registry::default();
        initial.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(&path, initial).unwrap();
        let task = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        scheduler.submit(&task).unwrap();
        let mut registry = Registry::default();
        registry.register_versioned(
            "job",
            2,
            true,
            |_: &Task| panic!("run"),
            Some(Arc::new(|_, _, _| panic!("migration"))),
        );
        registry.set_abort("job", |_: &Task| panic!("cleanup"));
        scheduler.replace_registry(registry.clone()).unwrap();
        scheduler.request_abort(task.id()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let result = scheduler.get(task.id()).unwrap().unwrap();
        assert_eq!(result.definition_version(), 1);
        assert_eq!(result.input(), task.input());
        assert_eq!(result.checkpoint(), task.checkpoint());
        assert!(matches!(
            result.state(),
            TaskState::Failed(reason) if reason == "orphaned: migration failed: migration panicked"
        ));
        drop(scheduler);
        let scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(scheduler.get(task.id()).unwrap(), Some(result));
    }

    #[test]
    fn migrated_reservation_survives_crash_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut r = Registry::default();
        r.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let s = Scheduler::open(&path, r).unwrap();
        let t = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        s.submit(&t).unwrap();
        drop(s);
        let mut r = Registry::default();
        r.register_versioned(
            "job",
            2,
            true,
            |_: &Task| -> Result<Step, String> { panic!("simulated crash") },
            Some(Arc::new(|_, _, _| Ok((json!(42), cp("migrated"))))),
        );
        let mut s = Scheduler::open(&path, r).unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.tick(0))).is_err());
        drop(s);
        let mut r = Registry::default();
        r.register_versioned(
            "job",
            2,
            true,
            |t: &Task| {
                assert_eq!(t.input(), &json!(42));
                assert_eq!(t.checkpoint(), &cp("migrated"));
                assert_eq!(t.restarts(), 1);
                Ok(Step::Succeed(json!(null)))
            },
            None,
        );
        let mut s = Scheduler::open(&path, r).unwrap();
        assert!(s.tick(0).unwrap());
        assert!(matches!(
            s.get(t.id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
    }

    #[test]
    fn abort_during_document_handler_discards_conflicting_output() {
        use crate::document::{Address, TaskDocuments, Transaction};
        use durable_store::{Expected, Store};
        struct AbortDuring {
            path: std::path::PathBuf,
        }
        impl Handler for AbortDuring {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let addr = Address {
                    scope: "race".into(),
                    name: "doc".into(),
                    key: "".into(),
                };
                docs.get(&addr).unwrap();
                docs.put(&addr, json!({"attempt": true}).as_object().unwrap().clone())
                    .unwrap();
                let db = Store::open(&self.path).unwrap();
                let mut tx = Transaction::new();
                tx.put(
                    &addr,
                    Expected::Missing,
                    json!({"external": true}).as_object().unwrap(),
                )
                .unwrap();
                tx.commit(&db).unwrap();
                request_abort_subtree(&TaskStore::new(&db), task.id()).unwrap();
                Ok(Step::Continue(cp("must-not-commit")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut registry = Registry::default();
        registry.register("job", true, AbortDuring { path: path.clone() });
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        let t = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            "job",
            true,
            5,
        );
        scheduler.submit(&t).unwrap();
        scheduler.tick(0).unwrap();
        let after = scheduler.get(t.id()).unwrap().unwrap();
        assert!(after.abort_requested());
        assert_eq!(after.retired_attempt_id(), None);
        assert_eq!(after.state(), &TaskState::Ready);
        assert_eq!(after.checkpoint(), t.checkpoint());
        let db = Store::open(&path).unwrap();
        let addr = Address {
            scope: "race".into(),
            name: "doc".into(),
            key: "".into(),
        };
        assert_eq!(
            crate::document::Documents::new(&db)
                .get(&addr)
                .unwrap()
                .unwrap()
                .value,
            *json!({"external": true}).as_object().unwrap()
        );
    }

    #[test]
    fn abort_mark_survives_reopen_and_drains_children_once() {
        use std::sync::Mutex;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let make_registry = || {
            let mut registry = Registry::default();
            let calls = calls.clone();
            registry.register_with_abort(
                "tree",
                true,
                |t: &Task| {
                    if t.id() == Uuid::from_u128(1) {
                        Ok(Step::Wait {
                            checkpoint: cp("waiting"),
                            condition: WaitFor::Tasks(vec![Uuid::from_u128(2)]),
                            children: vec![task(2, TaskOwner::Task(t.id()), "tree", true, 5)],
                        })
                    } else {
                        Ok(Step::Succeed(json!(null)))
                    }
                },
                move |t: &Task| {
                    calls.lock().unwrap().push(t.id());
                    Ok(Step::Succeed(json!(null)))
                },
            );
            registry
        };
        {
            let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "tree",
                    true,
                    5,
                ))
                .unwrap();
            scheduler.tick(0).unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
        }
        {
            let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
            assert!(
                scheduler
                    .get(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            assert!(scheduler.tick(0).unwrap());
            assert!(scheduler.tick(0).unwrap());
            assert!(!scheduler.tick(0).unwrap());
        }
        let mut scheduler = Scheduler::open(&path, make_registry()).unwrap();
        assert!(!scheduler.tick(0).unwrap());
        assert_eq!(
            *calls.lock().unwrap(),
            vec![Uuid::from_u128(2), Uuid::from_u128(1)]
        );
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
    }

    #[test]
    fn interrupted_abort_reservation_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut registry = Registry::default();
        registry.register_with_abort(
            "tree",
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        {
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "tree",
                    true,
                    5,
                ))
                .unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
            let snapshot = scheduler.ts().load(Uuid::from_u128(1)).unwrap().unwrap();
            scheduler.ts().reserve(&snapshot).unwrap(); // crash after reservation
        }
        let mut registry = Registry::default();
        registry.register_with_abort(
            "tree",
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(
            scheduler
                .get(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .restarts(),
            1
        );
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
    }

    #[test]
    fn missing_abort_definition_settles_with_diagnostic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut registry = Registry::default();
        registry.register("tree", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        {
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "tree",
                    true,
                    5,
                ))
                .unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
        }
        let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(
            matches!(scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("orphaned"))
        );
    }

    #[test]
    fn blocked_abort_drains_owned_child_and_persists_reason_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut registry = Registry::default();
        registry.register("parent", true, |t: &Task| {
            Ok(Step::Wait {
                checkpoint: cp("waiting"),
                condition: WaitFor::Tasks(vec![Uuid::from_u128(2)]),
                children: vec![task(2, TaskOwner::Task(t.id()), "child", true, 5)],
            })
        });
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        {
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "parent",
                    true,
                    5,
                ))
                .unwrap();
            assert!(scheduler.tick(0).unwrap());
            let mut registry = Registry::default();
            registry.register_with_abort(
                "child",
                true,
                |_: &Task| Ok(Step::Succeed(json!(null))),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            scheduler.replace_registry(registry).unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(
                scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            assert_eq!(
                scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                &TaskState::Ready
            );
        }
        {
            let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(
                scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                &TaskState::Failed("orphaned: missing task definition".into())
            );
        }
        let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Failed("orphaned: missing task definition".into())
        );
        assert!(scheduler.blocked_tasks().unwrap().is_empty());
        assert!(!scheduler.tick(0).unwrap());
    }

    #[test]
    fn blocked_abort_persists_version_and_migration_diagnostics() {
        for scenario in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tasks.db");
            let mut registry = Registry::default();
            registry.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "job",
                    true,
                    5,
                ))
                .unwrap();
            let mut registry = Registry::default();
            if scenario == 0 {
                // The stored version is newer than the available definition.
                let snapshot = scheduler.ts().load(Uuid::from_u128(1)).unwrap().unwrap();
                scheduler
                    .ts()
                    .update(&snapshot, |t| {
                        t.migrate(2, json!(null), cp("start"));
                        Ok(())
                    })
                    .unwrap();
                registry.register("job", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            } else {
                registry.register_versioned(
                    "job",
                    2,
                    true,
                    |_: &Task| Ok(Step::Succeed(json!(null))),
                    if scenario == 1 {
                        None
                    } else {
                        Some(Arc::new(|_, _, _| Err("bad checkpoint".into())))
                    },
                );
                registry.set_abort("job", |_: &Task| Ok(Step::Succeed(json!(null))));
            }
            scheduler.replace_registry(registry).unwrap();
            scheduler.request_abort(Uuid::from_u128(1)).unwrap();
            assert!(scheduler.tick(0).unwrap());
            let expected = match scenario {
                0 => "orphaned: task definition is older than stored version",
                1 => "orphaned: migration failed: missing migration",
                _ => "orphaned: migration failed: bad checkpoint",
            };
            drop(scheduler);
            let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
            assert_eq!(
                scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                &TaskState::Failed(expected.into())
            );
        }
    }

    #[test]
    fn missing_task_definition_does_not_block_other_ready_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut original = Registry::default();
        original.register("missing", true, |_: &Task| {
            Ok(Step::Succeed(json!("unused")))
        });
        original.register("available", true, |_: &Task| {
            Ok(Step::Succeed(json!("done")))
        });
        {
            let scheduler = Scheduler::open(&path, original).unwrap();
            scheduler
                .submit(&task(
                    1,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "missing",
                    true,
                    5,
                ))
                .unwrap();
            scheduler
                .submit(&task(
                    2,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    "available",
                    true,
                    5,
                ))
                .unwrap();
        }
        let mut partial = Registry::default();
        partial.register("available", true, |_: &Task| {
            Ok(Step::Succeed(json!("done")))
        });
        let mut scheduler = Scheduler::open(&path, partial).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Ready
        );
        assert!(matches!(
            scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
    }
    #[test]
    fn missing_interrupted_parent_cannot_dispatch_its_children() {
        for safe in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tasks.db");
            let owner = Arc::new(OwnedStore::open(&path).unwrap());
            let store = TaskStore::new(owner.store());
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                safe,
                5,
            );
            parent.start().unwrap();
            let child = task(2, TaskOwner::Task(parent.id()), "child", true, 5);
            store
                .save(&[Save::Create(&parent), Save::Create(&child)])
                .unwrap();
            drop(owner);
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let captured = calls.clone();
            let mut registry = Registry::default();
            registry.register("child", true, move |_: &Task| {
                captured.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Step::Succeed(json!("done")))
            });
            if !safe {
                let mut scheduler = Scheduler::open(&path, registry).unwrap();
                assert!(matches!(
                    scheduler.get(parent.id()).unwrap().unwrap().state(),
                    TaskState::CompletingFailed(_)
                ));
                assert!(scheduler.wake_snapshot(0).unwrap().active);
                scheduler.tick(0).unwrap();
                assert!(
                    scheduler
                        .get(child.id())
                        .unwrap()
                        .unwrap()
                        .abort_requested()
                );
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
                continue;
            }
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            assert!(!scheduler.tick(0).unwrap());
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(
                scheduler.get(child.id()).unwrap().unwrap().state(),
                &TaskState::Ready
            );
        }
    }

    #[test]
    fn wake_snapshot_respects_blocked_ancestors_and_pollable_waits() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = Registry::default();
        registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "missing",
            true,
            3,
        );
        parent.start().unwrap();
        parent.wait(WaitFor::DeadlineMillis(100)).unwrap();
        let child = task(2, TaskOwner::Task(parent.id()), "child", true, 3);
        scheduler
            .ts()
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        let before = scheduler.ts().list().unwrap();
        let early = scheduler.wake_snapshot(99).unwrap();
        assert!(!early.active);
        assert_eq!(early.at, Some(100));
        assert_eq!(early.blocked.len(), 1);
        let due = scheduler.wake_snapshot(100).unwrap();
        assert!(!due.active);
        assert_eq!(due.at, None);
        assert_eq!(due.blocked.len(), 1);
        let after = scheduler.ts().list().unwrap();
        assert_eq!(
            serde_json::to_value(
                before
                    .iter()
                    .map(|s| (&s.task, s.version))
                    .collect::<Vec<_>>()
            )
            .unwrap(),
            serde_json::to_value(
                after
                    .iter()
                    .map(|s| (&s.task, s.version))
                    .collect::<Vec<_>>()
            )
            .unwrap()
        );
    }

    #[test]
    fn incompatible_parked_parent_blocks_child_across_reopen_until_definition_restored() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        for stored_version in [1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let calls = Arc::new(AtomicUsize::new(0));
            let make_registry = |parent_version| {
                let mut registry = Registry::default();
                registry.register_versioned(
                    "parent",
                    parent_version,
                    false,
                    |_: &Task| Ok(Step::Succeed(json!("parent"))),
                    None,
                );
                let calls = calls.clone();
                registry.register("child", true, move |_: &Task| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Step::Succeed(json!("child")))
                });
                registry
            };
            let parent_id = Uuid::from_u128(1);
            let child_id = Uuid::from_u128(2);
            {
                let owner = Arc::new(OwnedStore::open(&path).unwrap());
                let store = TaskStore::new(owner.store());
                let mut parent = Task::new(
                    parent_id,
                    TaskOwner::Conversation(Uuid::from_u128(9)),
                    cp("start"),
                    5,
                )
                .unwrap()
                .registered_versioned("parent".into(), false, stored_version, json!(null))
                .unwrap();
                parent.start().unwrap();
                parent.wait(WaitFor::TasksAny(vec![child_id])).unwrap();
                let child = task(2, TaskOwner::Task(parent_id), "child", true, 5);
                store
                    .save(&[Save::Create(&parent), Save::Create(&child)])
                    .unwrap();
            }
            // Cover both an older definition and a newer one without migration.
            let incompatible = if stored_version == 1 { 2 } else { 1 };
            {
                let mut scheduler = Scheduler::open(&path, make_registry(incompatible)).unwrap();
                assert!(!scheduler.wake_snapshot(0).unwrap().active);
                assert!(!scheduler.tick(0).unwrap());
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert_eq!(
                    scheduler.get(child_id).unwrap().unwrap().state(),
                    &TaskState::Ready
                );
            }
            let mut scheduler = Scheduler::open(&path, make_registry(incompatible)).unwrap();
            assert!(!scheduler.wake_snapshot(0).unwrap().active);
            assert!(!scheduler.tick(0).unwrap());
            scheduler
                .replace_registry(make_registry(stored_version))
                .unwrap();
            assert!(scheduler.wake_snapshot(0).unwrap().active);
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(scheduler.tick(0).unwrap());
            assert_eq!(
                scheduler.get(parent_id).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("parent"))
            );
        }
    }

    #[test]
    fn migration_preserves_child_wake_before_reservation_and_after_recovery() {
        for recovered in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = Registry::default();
            registry.register_versioned(
                "parent",
                2,
                true,
                |_: &Task| Ok(Step::Succeed(json!(null))),
                Some(Arc::new(|input, checkpoint, _| {
                    Ok((input.clone(), checkpoint.clone()))
                })),
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let parent_id = Uuid::from_u128(1);
            let a = Uuid::from_u128(2);
            let b = Uuid::from_u128(3);
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                3,
            );
            let mut first = task(2, TaskOwner::Task(parent_id), "child", true, 3);
            let second = task(3, TaskOwner::Task(parent_id), "child", true, 3);
            first.start().unwrap();
            first.succeed(json!(null)).unwrap();
            parent.start().unwrap();
            parent.wait(WaitFor::TasksAny(vec![b, a])).unwrap();
            scheduler
                .ts()
                .save(&[
                    Save::Create(&parent),
                    Save::Create(&first),
                    Save::Create(&second),
                ])
                .unwrap();
            let waiting = scheduler.ts().load(parent_id).unwrap().unwrap();
            scheduler.ts().poll_wait(&waiting, 0).unwrap();
            let ready = scheduler.ts().load(parent_id).unwrap().unwrap();
            if recovered {
                scheduler.ts().reserve(&ready).unwrap();
                let running = scheduler.ts().load(parent_id).unwrap().unwrap();
                scheduler.ts().recover(&running).unwrap();
            }
            let child = scheduler.ts().load(b).unwrap().unwrap();
            scheduler.ts().update(&child, |t| t.cancel()).unwrap();
            let ready = scheduler.ts().load(parent_id).unwrap().unwrap();
            assert!(scheduler.reserve_resolved(&ready, None).unwrap());
            let running = scheduler.ts().load(parent_id).unwrap().unwrap();
            let expected = if recovered { vec![a] } else { vec![b, a] };
            assert_eq!(running.task.ended_children(), Some(expected.as_slice()));
            assert_eq!(running.task.definition_version(), 2);
        }
    }

    #[cfg(unix)]
    #[test]
    fn child_wake_process_death_worker() {
        use std::io::Write;
        let Ok(path) = std::env::var("PI_CHILD_WAKE_TEST_DB") else {
            return;
        };
        let safe = std::env::var("PI_CHILD_WAKE_TEST_SAFE").unwrap() == "yes";
        let parent = Uuid::from_u128(1);
        let child = Uuid::from_u128(2);
        let mut registry = Registry::default();
        registry.register("parent", safe, move |t: &Task| {
            if t.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![child]),
                    children: vec![task(2, TaskOwner::Task(parent), "child", true, 3)],
                })
            } else {
                assert_eq!(t.ended_children(), Some(&[child][..]));
                println!("CHILD_WAKE_RESERVED");
                std::io::stdout().flush().unwrap();
                loop {
                    std::thread::park();
                }
            }
        });
        registry.register("child", true, |_: &Task| {
            Ok(Step::Succeed(serde_json::json!("done")))
        });
        let mut scheduler = Scheduler::open(path, registry).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                safe,
                3,
            ))
            .unwrap();
        scheduler.tick(0).unwrap();
        scheduler.tick(0).unwrap();
        scheduler.tick(0).unwrap();
        panic!("handler returned after reserved wake");
    }

    #[cfg(unix)]
    #[test]
    fn child_wake_process_death_replays_only_safe_handler() {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        };
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for safe in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "task_scheduler::tests::child_wake_process_death_worker",
                    "--nocapture",
                ])
                .env("PI_CHILD_WAKE_TEST_DB", &path)
                .env("PI_CHILD_WAKE_TEST_SAFE", if safe { "yes" } else { "no" })
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let mut child = KillOnDrop(child);
            let stdout = child.0.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if line.contains("CHILD_WAKE_RESERVED") {
                        let _ = tx.send(());
                        break;
                    }
                }
            });
            rx.recv_timeout(std::time::Duration::from_secs(20))
                .expect("handler did not reach reserved child wake");
            child.0.kill().unwrap();
            assert!(!child.0.wait().unwrap().success());
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = calls.clone();
            let mut registry = Registry::default();
            registry.register("parent", safe, move |t: &Task| {
                seen.fetch_add(1, Ordering::SeqCst);
                assert_eq!(t.ended_children(), Some(&[Uuid::from_u128(2)][..]));
                Ok(Step::Succeed(serde_json::json!("resumed")))
            });
            registry.register("child", true, |_: &Task| {
                Ok(Step::Succeed(serde_json::json!("done")))
            });
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            assert_eq!(
                scheduler
                    .get(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .retired_attempt_id(),
                None
            );
            if safe {
                assert_eq!(
                    scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                    &TaskState::Ready
                );
                scheduler.tick(0).unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                assert_eq!(
                    scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                    &TaskState::Succeeded(serde_json::json!("resumed"))
                );
            } else {
                assert!(matches!(
                    scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                    TaskState::Failed(_)
                ));
                assert_eq!(calls.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[test]
    fn child_wake_payload_survives_reservation_recovery_and_reaches_handler() {
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let parent = Uuid::from_u128(1);
        let a = Uuid::from_u128(2);
        let b = Uuid::from_u128(3);
        let c = Uuid::from_u128(4);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let mut registry = Registry::default();
        registry.register("parent", true, move |parent_task: &Task| {
            if parent_task.checkpoint().phase == "start" {
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![a, b, c]),
                    children: vec![
                        task(2, TaskOwner::Task(parent), "child", true, 3),
                        task(3, TaskOwner::Task(parent), "child", true, 3),
                        task(4, TaskOwner::Task(parent), "child", true, 3),
                    ],
                })
            } else {
                captured
                    .lock()
                    .unwrap()
                    .push(parent_task.ended_children().map(<[Uuid]>::to_vec));
                Ok(Step::Wait {
                    checkpoint: cp("after"),
                    condition: WaitFor::Signal("hold".into()),
                    children: vec![],
                })
            }
        });
        registry.register("child", true, |task: &Task| {
            if task.id() == Uuid::from_u128(2) {
                Ok(Step::Succeed(serde_json::json!("done")))
            } else {
                Ok(Step::Wait {
                    checkpoint: cp("hold"),
                    condition: WaitFor::Signal("hold".into()),
                    children: vec![],
                })
            }
        });
        let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
        scheduler
            .submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                3,
            ))
            .unwrap();
        scheduler.tick(0).unwrap(); // parent creates and waits for its children
        scheduler.tick(0).unwrap(); // first child finishes
        let waiting = scheduler.ts().load(parent).unwrap().unwrap();
        assert!(matches!(
            scheduler.ts().poll_wait(&waiting, 0).unwrap(),
            WakeResult::Woken { .. }
        ));
        let ready = scheduler.ts().load(parent).unwrap().unwrap();
        assert_eq!(ready.task.ended_children(), Some(&[a][..]));
        // A child ending after wake but before reservation must be included.
        let second = scheduler.ts().load(b).unwrap().unwrap();
        scheduler.ts().update(&second, |t| t.cancel()).unwrap();
        scheduler.ts().reserve(&ready).unwrap(); // process dies after reservation
        // The first reservation freezes the wake; later child completion must
        // not change what a replayed invocation sees.
        let third = scheduler.ts().load(c).unwrap().unwrap();
        scheduler.ts().update(&third, |t| t.cancel()).unwrap();
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(
            scheduler.get(parent).unwrap().unwrap().ended_children(),
            Some(&[a, b][..])
        );
        scheduler.tick(0).unwrap();
        assert_eq!(*observed.lock().unwrap(), vec![Some(vec![a, b])]);
        assert_eq!(
            scheduler.get(parent).unwrap().unwrap().ended_children(),
            None
        );
    }

    #[test]
    fn fail_fast_conflict_fallback_rescans_before_retirement() {
        use crate::document::{Address, Documents, TaskDocuments, Transaction};
        use durable_store::Expected;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Staging {
            owned: Address,
            conflict: Address,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.acquire_owned(&self.owned).unwrap();
                docs.put(
                    &self.owned,
                    json!({"value":"staged"}).as_object().unwrap().clone(),
                )
                .unwrap();
                docs.get(&self.conflict).unwrap();
                docs.put(
                    &self.conflict,
                    json!({"value":"staged"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("must discard")))
            }
        }
        for mode in ["prepare", "commit", "direct"] {
            let dir = tempfile::tempdir().unwrap();
            let owned = Address {
                scope: "fallback".into(),
                name: "owned".into(),
                key: mode.into(),
            };
            let conflict = Address {
                scope: "fallback".into(),
                name: "conflict".into(),
                key: mode.into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let cleanups = Arc::new(AtomicUsize::new(0));
            let observed = cleanups.clone();
            let mut registry = Registry::default();
            registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            registry.register_with_abort(
                "child",
                false,
                Staging {
                    owned: owned.clone(),
                    conflict: conflict.clone(),
                    calls: calls.clone(),
                },
                move |_: &Task| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(Step::Succeed(json!(null)))
                },
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let mut parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            parent.start().unwrap();
            parent
                .wait(WaitFor::TasksFailFast(vec![
                    Uuid::from_u128(2),
                    Uuid::from_u128(3),
                ]))
                .unwrap();
            let child = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
            let sibling = task(3, TaskOwner::Task(parent.id()), "other", false, 3);
            scheduler
                .ts()
                .save(&[
                    Save::Create(&parent),
                    Save::Create(&child),
                    Save::Create(&sibling),
                ])
                .unwrap();
            let mut tx = Transaction::new();
            tx.acquire_task_document(&owned, child.id()).unwrap();
            tx.put(
                &owned,
                Expected::Missing,
                json!({"value":"original"}).as_object().unwrap(),
            )
            .unwrap();
            tx.put(
                &conflict,
                Expected::Missing,
                json!({"value":"initial"}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(scheduler.owner().store()).unwrap();
            scheduler.inject_batch_prepare_conflict = mode == "prepare";
            let conflict_address = conflict.clone();
            scheduler.result_commit_hook = Some(Arc::new(move |attempt, store| {
                if mode != "prepare" && attempt == 0 {
                    let version = Documents::new(store)
                        .get(&conflict_address)
                        .unwrap()
                        .unwrap()
                        .version;
                    let mut tx = Transaction::new();
                    tx.put(
                        &conflict_address,
                        Expected::Version(version),
                        json!({"value":"external"}).as_object().unwrap(),
                    )
                    .unwrap();
                    tx.commit(store).unwrap();
                }
            }));
            let injected = Arc::new(AtomicBool::new(false));
            let once = injected.clone();
            scheduler.failure_commit_hook = Some(Arc::new(move |_, store| {
                if !once.swap(true, Ordering::SeqCst) {
                    let ts = TaskStore::new(store);
                    if mode == "direct" {
                        request_abort_subtree(&ts, Uuid::from_u128(2)).unwrap();
                    } else {
                        let sibling = ts.load(Uuid::from_u128(3)).unwrap().unwrap();
                        ts.update(&sibling, |task| {
                            task.start()?;
                            task.fail("failure".into())
                        })
                        .unwrap();
                    }
                }
            }));
            scheduler.tick(0).unwrap();
            let after = scheduler.get(child.id()).unwrap().unwrap();
            assert_eq!(after.state(), &TaskState::Ready, "{mode}");
            assert!(after.abort_requested(), "{mode}");
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(cleanups.load(Ordering::SeqCst), 0);
            assert_eq!(
                Documents::new(scheduler.owner().store())
                    .get(&owned)
                    .unwrap()
                    .unwrap()
                    .value["value"],
                "original"
            );
            assert!(matches!(
                scheduler.get(parent.id()).unwrap().unwrap().state(),
                TaskState::Waiting(_)
            ));
            scheduler.tick(0).unwrap();
            assert_eq!(cleanups.load(Ordering::SeqCst), 1);
            assert_eq!(
                scheduler.get(child.id()).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            assert!(
                Documents::new(scheduler.owner().store())
                    .get(&owned)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn fail_fast_failure_commit_rescans_after_graph_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut running = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
        running.start_attempt(Uuid::new_v4()).unwrap();
        let failed = task(3, TaskOwner::Task(parent.id()), "child", false, 3);
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&running),
                Save::Create(&failed),
            ])
            .unwrap();
        let snapshot = scheduler.ts().load(running.id()).unwrap().unwrap();
        let mut injected = false;
        let result =
            scheduler.finish_failed_inner_with(&snapshot, "own failure".into(), true, || {
                if !injected {
                    injected = true;
                    let sibling = scheduler.ts().load(failed.id()).unwrap().unwrap();
                    scheduler
                        .ts()
                        .update(&sibling, |task| {
                            task.start()?;
                            task.fail("failure".into())
                        })
                        .unwrap();
                }
            });
        assert!(matches!(result, Err(Error::PendingAbort(id)) if id == running.id()));
        assert_eq!(
            scheduler.get(running.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
    }

    #[test]
    fn fail_fast_graph_conflict_marks_running_before_result_commit() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register_with_abort(
            "child",
            false,
            move |_: &Task| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(Step::Succeed(json!("must discard")))
            },
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let child = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
        let failed = task(3, TaskOwner::Task(parent.id()), "failed", false, 3);
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&failed),
            ])
            .unwrap();
        let injected = Arc::new(AtomicBool::new(false));
        let once = injected.clone();
        scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
            if !once.swap(true, Ordering::SeqCst) {
                let ts = TaskStore::new(store);
                let sibling = ts.load(Uuid::from_u128(3)).unwrap().unwrap();
                ts.update(&sibling, |task| {
                    task.start()?;
                    task.fail("failure".into())
                })
                .unwrap();
            }
        }));
        scheduler.tick(0).unwrap();
        let held = scheduler.get(child.id()).unwrap().unwrap();
        assert_eq!(held.state(), &TaskState::Ready);
        assert!(held.abort_requested());
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.get(parent.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn graph_retry_preserves_staged_signal_checks() {
        use crate::document::TaskDocuments;
        use crate::task_store::{SignalResult, SignalToken};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Sender(Arc<AtomicUsize>);
        impl Handler for Sender {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(3), &token, "key", json!(1), 99)
                        .unwrap(),
                    SignalResult::Delivered
                );
                Ok(Step::Succeed(json!("must not commit")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register("sender", false, Sender(calls.clone()));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let sender = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "sender",
            false,
            3,
        );
        let other = task(
            2,
            TaskOwner::Conversation(Uuid::from_u128(10)),
            "other",
            false,
            3,
        );
        let receiver = task(
            3,
            TaskOwner::Conversation(Uuid::from_u128(11)),
            "receiver",
            false,
            3,
        );
        scheduler
            .ts()
            .save(&[
                Save::Create(&sender),
                Save::Create(&other),
                Save::Create(&receiver),
            ])
            .unwrap();
        scheduler.result_commit_hook = Some(Arc::new(move |attempt, store| {
            let ts = TaskStore::new(store);
            if attempt == 0 {
                let other = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
                ts.update(&other, |task| {
                    task.start()?;
                    task.yield_ready()
                })
                .unwrap();
            } else if attempt == 1 {
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    ts.deliver_signal_at(Uuid::from_u128(3), &token, "key", json!(2), 99)
                        .unwrap(),
                    SignalResult::Delivered
                );
            }
        }));
        scheduler.tick(0).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            scheduler.get(sender.id()).unwrap().unwrap().state(),
            TaskState::Failed(_)
        ));
        let token = SignalToken::new("answer", 1).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .deliver_signal_at(receiver.id(), &token, "key", json!(2), 99)
                .unwrap(),
            SignalResult::Duplicate
        );
        assert_eq!(
            scheduler
                .ts()
                .read_typed_signal(receiver.id(), &token, "key")
                .unwrap(),
            Some(json!(2))
        );
    }

    #[test]
    fn prepared_settlement_retains_effects_across_graph_conflict() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::task_store::{SignalResult, SignalToken};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Staging {
            calls: Arc<AtomicUsize>,
            address: Address,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                parent: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.acquire_owned(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value": "retained"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(3), &token, "key", json!(7), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                let keyed = task(4, TaskOwner::Task(parent.id()), "child", false, 3);
                docs.stage_keyed_child("keyed", "tool", keyed.input(), keyed.clone())
                    .unwrap();
                let direct = task(5, TaskOwner::Task(parent.id()), "child", false, 3);
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![keyed.id(), direct.id()]),
                    children: vec![direct],
                })
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let address = Address {
            scope: "prepared".into(),
            name: "document".into(),
            key: "".into(),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "parent",
            false,
            Staging {
                calls: calls.clone(),
                address: address.clone(),
            },
        );
        registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = TaskOwner::Conversation(Uuid::from_u128(9));
        scheduler
            .ts()
            .save(&[
                Save::Create(&task(1, owner.clone(), "parent", false, 3)),
                Save::Create(&task(2, owner.clone(), "other", false, 3)),
                Save::Create(&task(3, owner, "receiver", false, 3)),
            ])
            .unwrap();
        let observed = attempts.clone();
        scheduler.result_commit_hook = Some(Arc::new(move |attempt, store| {
            observed.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                let ts = TaskStore::new(store);
                let other = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
                ts.update(&other, |task| {
                    task.start()?;
                    task.yield_ready()
                })
                .unwrap();
            }
        }));
        scheduler.tick(0).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Waiting(WaitFor::TasksAny(vec![
                Uuid::from_u128(4),
                Uuid::from_u128(5)
            ]))
        );
        assert_eq!(
            Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"],
            "retained"
        );
        assert_eq!(
            scheduler
                .owner()
                .store()
                .task_document_owner(&address.storage_key().unwrap())
                .unwrap(),
            Some(Uuid::from_u128(1))
        );
        let token = SignalToken::new("answer", 1).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .read_typed_signal(Uuid::from_u128(3), &token, "key")
                .unwrap(),
            Some(json!(7))
        );
        for id in [4, 5] {
            assert_eq!(
                scheduler.get(Uuid::from_u128(id)).unwrap().unwrap().owner(),
                &TaskOwner::Task(Uuid::from_u128(1))
            );
        }
    }

    #[test]
    fn yielded_settlement_boundaries_retain_lease_attempt_and_effects() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::task_store::{SignalResult, SignalToken};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct Staging {
            calls: Arc<AtomicUsize>,
            attempt: Arc<Mutex<Option<Uuid>>>,
            address: Address,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                *self.attempt.lock().unwrap() = task.attempt_id();
                docs.get(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value":"staged"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(3), &token, "key", json!(7), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                let keyed = super::tests::task(4, TaskOwner::Task(task.id()), "child", false, 3);
                docs.stage_keyed_child("keyed", "tool", keyed.input(), keyed.clone())
                    .unwrap();
                let direct = super::tests::task(5, TaskOwner::Task(task.id()), "child", false, 3);
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![keyed.id(), direct.id()]),
                    children: vec![direct],
                })
            }
        }

        for after_conflict in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let address = Address {
                scope: "yield".into(),
                name: "doc".into(),
                key: "".into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let attempt = Arc::new(Mutex::new(None));
            let mut registry = Registry::default();
            registry.register(
                "parent",
                false,
                Staging {
                    calls: calls.clone(),
                    attempt: attempt.clone(),
                    address: address.clone(),
                },
            );
            registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let owner = TaskOwner::Conversation(Uuid::from_u128(9));
            scheduler
                .ts()
                .save(&[
                    Save::Create(&task(1, owner.clone(), "parent", false, 3)),
                    Save::Create(&task(2, owner.clone(), "other", false, 3)),
                    Save::Create(&task(3, owner, "receiver", false, 3)),
                ])
                .unwrap();
            let TickReservation::Invocation(invocation) = scheduler.reserve_tick(0).unwrap() else {
                panic!("reservation")
            };
            let completed = invocation.execute().unwrap();
            let attempt_id = attempt.lock().unwrap().expect("attempt recorded");
            scheduler.yield_before_preparation = !after_conflict;
            scheduler.yield_after_graph_conflict = after_conflict;
            if after_conflict {
                let once = Arc::new(AtomicBool::new(false));
                scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
                    if !once.swap(true, Ordering::SeqCst) {
                        let tasks = TaskStore::new(store);
                        let other = tasks.load(Uuid::from_u128(2)).unwrap().unwrap();
                        tasks
                            .update(&other, |task| {
                                task.start()?;
                                task.yield_ready()
                            })
                            .unwrap();
                    }
                }));
            }
            let pending = match scheduler.settle_invocation(completed) {
                Err(SettlementError::Yielded { pending }) => *pending,
                result => panic!("expected yield: {result:?}"),
            };
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(
                scheduler
                    .live_invocations
                    .lock()
                    .unwrap()
                    .attempts
                    .contains(&Uuid::from_u128(1))
            );
            assert_eq!(
                scheduler
                    .ts()
                    .load(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .task
                    .attempt_id(),
                Some(attempt_id)
            );
            assert!(
                Documents::new(scheduler.owner().store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
            assert!(scheduler.get(Uuid::from_u128(4)).unwrap().is_none());
            let token = SignalToken::new("answer", 1).unwrap();
            assert_eq!(
                scheduler
                    .ts()
                    .read_typed_signal(Uuid::from_u128(3), &token, "key")
                    .unwrap(),
                None
            );
            scheduler.yield_before_preparation = false;
            scheduler.yield_after_graph_conflict = false;
            match pending {
                PendingSettlement::Unprepared(completed) if !after_conflict => {
                    scheduler.settle_invocation(completed).unwrap();
                }
                PendingSettlement::Prepared {
                    mut lease,
                    mut settlement,
                } if after_conflict => {
                    let owner = scheduler.owner();
                    let fence = scheduler.failure_fence.clone();
                    let failure_guard = fence.lock().unwrap();
                    let task_gate = owner.task_gate().lock().unwrap();
                    let result =
                        scheduler.resume_prepared_settlement(&mut settlement, &owner, task_gate);
                    if result.is_ok() {
                        lease.retired = true;
                    }
                    drop(failure_guard);
                    drop(lease);
                    result.unwrap();
                }
                other => panic!("unexpected yield: {other:?}"),
            }
            let root = scheduler.get(Uuid::from_u128(1)).unwrap().unwrap();
            assert_eq!(
                root.state(),
                &TaskState::Waiting(WaitFor::TasksAny(vec![
                    Uuid::from_u128(4),
                    Uuid::from_u128(5)
                ]))
            );
            assert_eq!(root.retired_attempt_id(), Some(attempt_id));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(
                !scheduler
                    .live_invocations
                    .lock()
                    .unwrap()
                    .attempts
                    .contains(&root.id())
            );
            assert_eq!(
                Documents::new(scheduler.owner().store())
                    .get(&address)
                    .unwrap()
                    .unwrap()
                    .value["value"],
                "staged"
            );
            assert_eq!(
                scheduler
                    .ts()
                    .read_typed_signal(Uuid::from_u128(3), &token, "key")
                    .unwrap(),
                Some(json!(7))
            );
            for id in [4, 5] {
                assert_eq!(
                    scheduler.get(Uuid::from_u128(id)).unwrap().unwrap().owner(),
                    &TaskOwner::Task(root.id())
                );
            }
        }
    }

    #[test]
    fn prepared_revalidation_refreshes_stale_graph_and_keeps_document_checks() {
        use crate::document::{Address, Documents, Transaction};
        use durable_store::Expected;
        for document_changed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut scheduler =
                Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
            let owner = scheduler.owner();
            let gate = owner.task_gate().lock().unwrap();
            let mut root = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            root.start_attempt(Uuid::new_v4()).unwrap();
            scheduler.ts().save(&[Save::Create(&root)]).unwrap();
            let running = scheduler.ts().load(root.id()).unwrap().unwrap();
            let boundary = owner.store().put_version().unwrap();
            let mut candidate = running.clone();
            candidate.task.succeed(json!("result")).unwrap();
            candidate
                .task
                .record_completed_attempt(&running.task)
                .unwrap();
            let address = Address {
                scope: "revalidate".into(),
                name: "doc".into(),
                key: "".into(),
            };
            let mut transaction = Transaction::new();
            transaction
                .put(
                    &address,
                    Expected::Missing,
                    json!({"value":"staged"}).as_object().unwrap(),
                )
                .unwrap();
            transaction
                .append(
                    scheduler
                        .ts()
                        .prepare_save(&[Save::Update(&candidate)])
                        .unwrap(),
                )
                .unwrap();
            let mut prepared = PreparedSettlement {
                attachment: scheduler.attachment.clone(),
                transaction,
                notifications: None,
                running: running.clone(),
                candidate,
                wait_targets: vec![],
                staged_children: HashMap::new(),
                conflict_keys: HashSet::from([address.storage_key().unwrap()]),
                graph_version: boundary,
                attempts: 0,
                commit_uncertain: false,
            };
            let child = task(2, TaskOwner::Task(root.id()), "child", false, 3);
            scheduler.ts().save(&[Save::Create(&child)]).unwrap();
            scheduler.ts().update(&running, |_| Ok(())).unwrap();
            assert!(matches!(
                prepared.revalidate(&scheduler, &owner, &gate).unwrap(),
                PreparedRevalidation::Ready
            ));
            assert!(prepared.running.version > running.version);
            assert_eq!(
                prepared.candidate.task.state(),
                &TaskState::Completing(json!("result"))
            );
            if document_changed {
                let mut external = Transaction::new();
                external
                    .put(
                        &address,
                        Expected::Missing,
                        json!({"value":"external"}).as_object().unwrap(),
                    )
                    .unwrap();
                external.commit(owner.store()).unwrap();
                assert!(matches!(
                    prepared
                        .transaction
                        .commit_task_graph(owner.store(), prepared.graph_version),
                    Err(crate::document::Error::Store(
                        durable_store::Error::Conflict { .. }
                    ))
                ));
                assert_eq!(
                    scheduler
                        .ts()
                        .load(root.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Running
                );
                assert_eq!(
                    Documents::new(owner.store())
                        .get(&address)
                        .unwrap()
                        .unwrap()
                        .value["value"],
                    "external"
                );
            } else {
                scheduler
                    .resume_prepared_settlement(&mut prepared, &owner, gate)
                    .unwrap();
                assert_eq!(
                    scheduler
                        .ts()
                        .load(root.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Completing(json!("result"))
                );
                assert_eq!(
                    Documents::new(owner.store())
                        .get(&address)
                        .unwrap()
                        .unwrap()
                        .value["value"],
                    "staged"
                );
            }
        }
    }

    #[test]
    fn prepared_revalidation_reports_pending_abort_before_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let owner = scheduler.owner();
        let gate = owner.task_gate().lock().unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut child = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
        child.start_attempt(Uuid::new_v4()).unwrap();
        let mut sibling = task(3, TaskOwner::Task(parent.id()), "sibling", false, 3);
        sibling.start().unwrap();
        sibling.fail("failure".into()).unwrap();
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&sibling),
            ])
            .unwrap();
        let running = scheduler.ts().load(child.id()).unwrap().unwrap();
        let mut candidate = running.clone();
        candidate.task.succeed(json!("must wait")).unwrap();
        candidate
            .task
            .record_completed_attempt(&running.task)
            .unwrap();
        let mut transaction = crate::document::Transaction::new();
        transaction
            .append(
                scheduler
                    .ts()
                    .prepare_save(&[Save::Update(&candidate)])
                    .unwrap(),
            )
            .unwrap();
        let mut prepared = PreparedSettlement {
            attachment: scheduler.attachment.clone(),
            transaction,
            notifications: None,
            running,
            candidate,
            wait_targets: vec![],
            staged_children: HashMap::new(),
            conflict_keys: HashSet::new(),
            graph_version: owner.store().put_version().unwrap(),
            attempts: 0,
            commit_uncertain: false,
        };
        assert!(
            matches!(prepared.revalidate(&scheduler, &owner, &gate).unwrap(), PreparedRevalidation::PendingAbort(id) if id == child.id())
        );
        assert_eq!(
            prepared.candidate.task.state(),
            &TaskState::Succeeded(json!("must wait"))
        );
        assert_eq!(
            scheduler
                .ts()
                .load(child.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
        assert!(
            matches!(scheduler.resume_prepared_settlement(&mut prepared, &owner, gate), Err(Error::PendingAbort(id)) if id == child.id())
        );
        assert_eq!(
            prepared.candidate.task.state(),
            &TaskState::Succeeded(json!("must wait"))
        );
        assert_eq!(
            scheduler
                .ts()
                .load(child.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
    }

    #[test]
    fn result_survives_same_reservation_rewrite_after_load_and_respects_abort() {
        use crate::document::{Address, Documents, TaskDocuments};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct HandlerWithDocument(Arc<AtomicUsize>, Address);
        impl Handler for HandlerWithDocument {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                docs.get(&self.1).unwrap();
                docs.put(
                    &self.1,
                    json!({"result": "retained"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("retained")))
            }
        }

        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let address = Address {
                scope: "test".into(),
                name: "result".into(),
                key: "".into(),
            };
            let mut registry = Registry::default();
            registry.register(
                "handler",
                false,
                HandlerWithDocument(calls.clone(), address.clone()),
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let root = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "handler",
                false,
                3,
            );
            scheduler.ts().save(&[Save::Create(&root)]).unwrap();
            scheduler.after_result_load_hook = Some(Arc::new(move |_, store| {
                let tasks = TaskStore::new(store);
                let running = tasks.load(Uuid::from_u128(1)).unwrap().unwrap();
                tasks
                    .update(&running, |task| {
                        if abort {
                            task.request_abort()?;
                        }
                        Ok(())
                    })
                    .unwrap();
            }));
            scheduler.tick(0).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let result = scheduler.get(root.id()).unwrap().unwrap();
            let document = Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap();
            if abort {
                assert_eq!(result.state(), &TaskState::Ready);
                assert!(result.abort_requested());
                assert!(document.is_none());
            } else {
                assert_eq!(result.state(), &TaskState::Succeeded(json!("retained")));
                assert_eq!(document.unwrap().value["result"], "retained");
            }
        }
    }

    #[test]
    fn graph_retry_preserves_original_running_version_check() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = Registry::default();
        registry.register("handler", false, move |_: &Task| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(Step::Succeed(json!("must not commit")))
        });
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "handler",
            false,
            3,
        );
        let other = task(
            2,
            TaskOwner::Conversation(Uuid::from_u128(10)),
            "other",
            false,
            3,
        );
        scheduler
            .ts()
            .save(&[Save::Create(&root), Save::Create(&other)])
            .unwrap();
        scheduler.result_commit_hook = Some(Arc::new(move |attempt, store| {
            let ts = TaskStore::new(store);
            if attempt == 0 {
                let other = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
                ts.update(&other, |task| {
                    task.start()?;
                    task.yield_ready()
                })
                .unwrap();
            } else if attempt == 1 {
                let running = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
                ts.update(&running, |task| task.advance(cp("external")))
                    .unwrap();
            }
        }));
        assert!(scheduler.tick(0).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let current = scheduler.ts().load(root.id()).unwrap().unwrap().task;
        assert_eq!(current.state(), &TaskState::Running);
        assert_eq!(current.checkpoint().phase, "external");
    }

    #[cfg(unix)]
    #[test]
    fn late_child_process_fixture() {
        use crate::document::{Address, TaskDocuments};
        use crate::task_store::SignalToken;
        use std::io::Write;
        use std::sync::atomic::{AtomicBool, Ordering};
        let Ok(path) = std::env::var("PI_LATE_CHILD_CRASH_DB") else {
            return;
        };
        struct Writer;
        impl Handler for Writer {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let address = Address {
                    scope: "crash".into(),
                    name: "result".into(),
                    key: "".into(),
                };
                docs.get(&address).unwrap();
                docs.put(
                    &address,
                    json!({"value":"committed"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                docs.signal_at(Uuid::from_u128(3), &token, "key", json!(7), 0)
                    .unwrap();
                Ok(Step::Succeed(json!("success")))
            }
        }
        let mut registry = Registry::default();
        registry.register("parent", false, Writer);
        registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(path, registry).unwrap();
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        let child = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
        let receiver = task(
            3,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "receiver",
            false,
            3,
        );
        scheduler
            .ts()
            .save(&[Save::Create(&parent), Save::Create(&receiver)])
            .unwrap();
        let once = Arc::new(AtomicBool::new(false));
        scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
            if !once.swap(true, Ordering::SeqCst) {
                let ts = TaskStore::new(store);
                ts.save(&[Save::Create(&child)]).unwrap();
                let parent = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
                ts.update(&parent, |_| Ok(())).unwrap();
            }
        }));
        scheduler.tick(0).unwrap();
        assert_eq!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            &TaskState::Completing(json!("success"))
        );
        println!("LATE_CHILD_COMMITTED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }

    #[cfg(unix)]
    #[test]
    fn late_child_result_survives_process_kill_without_replay() {
        use crate::document::{Address, Documents};
        use crate::task_store::SignalToken;
        use std::{
            io::{BufRead, BufReader},
            process::{Child, Command, Stdio},
            sync::mpsc,
            time::Duration,
        };
        struct KillOnDrop(Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "task_scheduler::tests::late_child_process_fixture",
                    "--nocapture",
                ])
                .env("PI_LATE_CHILD_CRASH_DB", &path)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("LATE_CHILD_COMMITTED") {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        rx.recv_timeout(Duration::from_secs(30))
            .expect("held outcome did not commit");
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| panic!("parent must not replay"));
        registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Completing(json!("success"))
        );
        let address = Address {
            scope: "crash".into(),
            name: "result".into(),
            key: "".into(),
        };
        assert_eq!(
            Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"],
            "committed"
        );
        let token = SignalToken::new("answer", 1).unwrap();
        assert_eq!(
            scheduler
                .ts()
                .read_typed_signal(Uuid::from_u128(3), &token, "key")
                .unwrap(),
            Some(json!(7))
        );
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler
                .get(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .is_terminal()
            {
                break;
            }
        }
        assert_eq!(
            scheduler.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("success"))
        );
    }

    #[test]
    fn late_child_rebase_keeps_signals_keyed_children_and_runtime_update() {
        use crate::document::{Address, TaskDocuments};
        use crate::runtime::{Config, Harness, Options};
        use crate::task_store::{SignalResult, SignalToken};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        fn options() -> Options {
            Options {
                model: Arc::new(Stub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
            }
        }
        fn config(model: &str) -> Config {
            Config {
                model: model.into(),
                system_prompt: "prompt".into(),
                max_output_tokens: 20,
            }
        }
        struct Staging {
            address: Address,
            keyed: bool,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.get(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value":"staged"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(task.id(), &token, "self", json!(7), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(3), &token, "other", json!(8), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                if self.keyed {
                    let child = Task::new(
                        Uuid::from_u128(4),
                        TaskOwner::Task(task.id()),
                        Checkpoint {
                            phase: "start".into(),
                            data: json!(null),
                        },
                        3,
                    )
                    .unwrap()
                    .registered("child".into(), false);
                    docs.stage_keyed_child("nested", "tool", child.input(), child.clone())
                        .unwrap();
                }
                docs.append_entry("app.before", json!(1)).unwrap();
                docs.configure_runtime_at_checkpoint(config("changed"))
                    .unwrap();
                docs.append_entry("app.after", json!(2)).unwrap();
                Ok(Step::Succeed(json!("success")))
            }
        }
        for keyed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let conversation = harness.create_conversation(config("original")).unwrap();
            let address = Address {
                scope: conversation.to_string(),
                name: "late".into(),
                key: "".into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "parent",
                false,
                Staging {
                    address: address.clone(),
                    keyed,
                    calls: calls.clone(),
                },
            );
            registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!("child"))));
            let mut scheduler = harness.attach_tasks(registry.clone()).unwrap();
            let root = task(1, TaskOwner::Conversation(conversation), "parent", false, 3);
            let late = task(2, TaskOwner::Task(root.id()), "child", false, 3);
            let receiver = task(
                3,
                TaskOwner::Conversation(conversation),
                "receiver",
                false,
                3,
            );
            scheduler
                .ts()
                .save(&[Save::Create(&root), Save::Create(&receiver)])
                .unwrap();
            let once = Arc::new(AtomicBool::new(false));
            scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
                if !once.swap(true, Ordering::SeqCst) {
                    let ts = TaskStore::new(store);
                    ts.save(&[Save::Create(&late)]).unwrap();
                    let parent = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
                    ts.update(&parent, |_| Ok(())).unwrap();
                }
            }));
            scheduler.tick(0).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                &TaskState::Completing(json!("success"))
            );
            assert_eq!(
                harness.conversation(conversation).unwrap().config.model,
                "changed"
            );
            assert_eq!(
                harness.document(&address).unwrap().unwrap().value["value"],
                "staged"
            );
            let entries =
                crate::application_entry::list(scheduler.owner().store(), conversation).unwrap();
            assert_eq!(entries.len(), 2);
            let token = SignalToken::new("answer", 1).unwrap();
            assert_eq!(
                scheduler
                    .ts()
                    .read_typed_signal(root.id(), &token, "self")
                    .unwrap(),
                Some(json!(7))
            );
            assert_eq!(
                scheduler
                    .ts()
                    .read_typed_signal(receiver.id(), &token, "other")
                    .unwrap(),
                Some(json!(8))
            );
            assert_eq!(scheduler.get(Uuid::from_u128(4)).unwrap().is_some(), keyed);
            drop(scheduler);
            drop(harness);
            let harness = Harness::open_paused(&path, options()).unwrap();
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            for _ in 0..12 {
                scheduler.tick(0).unwrap();
                if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("success"))
            );
            assert_eq!(
                harness.conversation(conversation).unwrap().config.model,
                "changed"
            );
            assert_eq!(
                crate::application_entry::list(scheduler.owner().store(), conversation)
                    .unwrap()
                    .len(),
                2
            );
        }
    }

    #[test]
    fn changed_owned_child_read_fails_unsafe_parent_before_cleanup() {
        use crate::document::TaskDocuments;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        struct Parent(Arc<AtomicUsize>);
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                let child = docs.get_owned_child(Uuid::from_u128(2))?;
                assert_eq!(child.state(), &TaskState::Ready);
                Ok(Step::Succeed(json!("early result")))
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register("parent", false, Parent(calls.clone()));
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| panic!("child must not execute"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        let child = task(2, TaskOwner::Task(parent.id()), "child", true, 3);
        scheduler
            .ts()
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        let once = Arc::new(AtomicBool::new(false));
        scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
            if !once.swap(true, Ordering::SeqCst) {
                let ts = TaskStore::new(store);
                let child = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
                ts.update(&child, |task| task.start()).unwrap();
            }
        }));
        scheduler.tick(0).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            TaskState::CompletingFailed(reason)
                if reason == "document conflict: execution outcome requires reconciliation"
        ));
    }

    #[test]
    fn late_child_rebase_preserves_real_document_and_signal_conflicts() {
        use crate::document::{Address, Documents, TaskDocuments, Transaction};
        use crate::task_store::{SignalResult, SignalToken};
        use durable_store::Expected;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Writer {
            address: Address,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Writer {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.get(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value":"staged"}).as_object().unwrap().clone(),
                )
                .unwrap();
                let token = SignalToken::new("answer", 1).unwrap();
                assert_eq!(
                    docs.signal_at(Uuid::from_u128(3), &token, "key", json!(7), 0)
                        .unwrap(),
                    SignalResult::Delivered
                );
                Ok(Step::Succeed(json!("must not commit")))
            }
        }
        for signal_conflict in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let address = Address {
                scope: "late-conflict".into(),
                name: "doc".into(),
                key: "".into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "parent",
                false,
                Writer {
                    address: address.clone(),
                    calls: calls.clone(),
                },
            );
            registry.register_with_abort(
                "child",
                false,
                |_: &Task| panic!("child must not run"),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let root = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            let late = task(2, TaskOwner::Task(root.id()), "child", false, 3);
            let receiver = task(
                3,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "receiver",
                false,
                3,
            );
            scheduler
                .ts()
                .save(&[Save::Create(&root), Save::Create(&receiver)])
                .unwrap();
            let mut tx = Transaction::new();
            tx.put(
                &address,
                Expected::Missing,
                json!({"value":"initial"}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(scheduler.owner().store()).unwrap();
            let changed = address.clone();
            scheduler.result_commit_hook = Some(Arc::new(move |attempt, store| {
                let ts = TaskStore::new(store);
                if attempt == 0 {
                    ts.save(&[Save::Create(&late)]).unwrap();
                    let parent = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
                    ts.update(&parent, |_| Ok(())).unwrap();
                } else if attempt == 1 && signal_conflict {
                    let token = SignalToken::new("answer", 1).unwrap();
                    assert_eq!(
                        ts.deliver_signal_at(Uuid::from_u128(3), &token, "key", json!(8), 0)
                            .unwrap(),
                        SignalResult::Delivered
                    );
                } else if attempt == 1 {
                    let version = Documents::new(store)
                        .get(&changed)
                        .unwrap()
                        .unwrap()
                        .version;
                    let mut tx = Transaction::new();
                    tx.put(
                        &changed,
                        Expected::Version(version),
                        json!({"value":"external"}).as_object().unwrap(),
                    )
                    .unwrap();
                    tx.commit(store).unwrap();
                }
            }));
            scheduler.tick(0).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(matches!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                TaskState::CompletingFailed(_)
            ));
            let value = Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"]
                .clone();
            assert_eq!(
                value,
                if signal_conflict {
                    json!("initial")
                } else {
                    json!("external")
                }
            );
            let token = SignalToken::new("answer", 1).unwrap();
            assert_eq!(
                scheduler
                    .ts()
                    .read_typed_signal(receiver.id(), &token, "key")
                    .unwrap(),
                if signal_conflict {
                    Some(json!(8))
                } else {
                    None
                }
            );
        }
    }

    #[test]
    fn late_child_rebase_rejects_changed_attempt_and_respects_abort() {
        use crate::document::{Address, Documents, TaskDocuments};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Writer {
            address: Address,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Writer {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.get(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value":"must not commit"})
                        .as_object()
                        .unwrap()
                        .clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("must not commit")))
            }
        }
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let address = Address {
                scope: "fence".into(),
                name: "late".into(),
                key: "".into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register_with_abort(
                "parent",
                false,
                Writer {
                    address: address.clone(),
                    calls: calls.clone(),
                },
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            registry.register_with_abort(
                "child",
                false,
                |_: &Task| panic!("child must not run"),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let root = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            let child = task(2, TaskOwner::Task(root.id()), "child", false, 3);
            scheduler.ts().save(&[Save::Create(&root)]).unwrap();
            let once = Arc::new(AtomicBool::new(false));
            scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
                if !once.swap(true, Ordering::SeqCst) {
                    let ts = TaskStore::new(store);
                    ts.save(&[Save::Create(&child)]).unwrap();
                    if abort {
                        request_abort_subtree(&ts, Uuid::from_u128(1)).unwrap();
                    } else {
                        let current = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
                        ts.update(&current, |task| task.advance(cp("changed")))
                            .unwrap();
                    }
                }
            }));
            let result = scheduler.tick(0);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(
                Documents::new(scheduler.owner().store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
            let current = scheduler.ts().load(root.id()).unwrap().unwrap().task;
            if abort {
                assert!(result.unwrap());
                assert_eq!(current.state(), &TaskState::Ready);
                assert!(current.abort_requested());
                for _ in 0..8 {
                    scheduler.tick(0).unwrap();
                    if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                        break;
                    }
                }
                assert_eq!(
                    scheduler.get(root.id()).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
            } else {
                assert!(result.is_err());
                assert_eq!(current.state(), &TaskState::Running);
                assert_eq!(current.checkpoint().phase, "changed");
            }
        }
    }

    #[test]
    fn successful_non_replay_safe_result_with_late_child_survives_reopen() {
        use crate::document::{Address, Documents, TaskDocuments, Transaction};
        use durable_store::Expected;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Staging {
            owned: Address,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.acquire_owned(&self.owned).unwrap();
                docs.put(
                    &self.owned,
                    json!({"value":"staged"}).as_object().unwrap().clone(),
                )
                .unwrap();
                Ok(Step::Succeed(json!("success")))
            }
        }
        for identical_rewrite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let owned = Address {
                scope: "late-child".into(),
                name: "owned".into(),
                key: "".into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "parent",
                false,
                Staging {
                    owned: owned.clone(),
                    calls: calls.clone(),
                },
            );
            registry.register("child", false, |_: &Task| Ok(Step::Succeed(json!("child"))));
            let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
            let root = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                false,
                3,
            );
            let child = task(2, TaskOwner::Task(root.id()), "child", false, 3);
            scheduler.ts().save(&[Save::Create(&root)]).unwrap();
            let mut tx = Transaction::new();
            tx.acquire_task_document(&owned, root.id()).unwrap();
            tx.put(
                &owned,
                Expected::Missing,
                json!({"value":"original"}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(scheduler.owner().store()).unwrap();
            let once = Arc::new(AtomicBool::new(false));
            scheduler.result_commit_hook = Some(Arc::new(move |_, store| {
                if !once.swap(true, Ordering::SeqCst) {
                    let ts = TaskStore::new(store);
                    ts.save(&[Save::Create(&child)]).unwrap();
                    if identical_rewrite {
                        let current = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
                        ts.update(&current, |_| Ok(())).unwrap();
                    }
                }
            }));
            scheduler.tick(0).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                &TaskState::Completing(json!("success"))
            );
            assert_eq!(
                Documents::new(scheduler.owner().store())
                    .get(&owned)
                    .unwrap()
                    .unwrap()
                    .value["value"],
                "staged"
            );
            drop(scheduler);
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            for _ in 0..8 {
                scheduler.tick(0).unwrap();
                if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("success"))
            );
            assert!(
                Documents::new(scheduler.owner().store())
                    .get(&owned)
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn result_graph_conflicts_retry_without_replaying_handler() {
        use crate::document::{Address, Documents, TaskDocuments, Transaction};
        use durable_store::Expected;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Staging {
            calls: Arc<AtomicUsize>,
            address: Address,
        }
        impl Handler for Staging {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.get(&self.address).map_err(|e| e.to_string())?;
                docs.put(
                    &self.address,
                    json!({"value":"handler"}).as_object().unwrap().clone(),
                )
                .map_err(|e| e.to_string())?;
                Ok(Step::Succeed(json!("done")))
            }
        }
        for mode in ["once", "persistent", "document", "graph_then_document"] {
            let dir = tempfile::tempdir().unwrap();
            let address = Address {
                scope: "retry".into(),
                name: mode.into(),
                key: "".into(),
            };
            let calls = Arc::new(AtomicUsize::new(0));
            let writes = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "handler",
                false,
                Staging {
                    calls: calls.clone(),
                    address: address.clone(),
                },
            );
            let mut scheduler = Scheduler::open(dir.path().join("db"), registry).unwrap();
            let root = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "handler",
                false,
                3,
            );
            let other = task(
                2,
                TaskOwner::Conversation(Uuid::from_u128(10)),
                "other",
                false,
                3,
            );
            scheduler
                .ts()
                .save(&[Save::Create(&root), Save::Create(&other)])
                .unwrap();
            let mut tx = Transaction::new();
            tx.put(
                &address,
                Expected::Missing,
                json!({"value":"initial"}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(scheduler.owner().store()).unwrap();
            let hook_address = address.clone();
            let observed = writes.clone();
            scheduler.result_commit_hook = Some(Arc::new(move |attempt, store| {
                if (mode == "document" && attempt == 0)
                    || (mode == "graph_then_document" && attempt == 1)
                {
                    let version = Documents::new(store)
                        .get(&hook_address)
                        .unwrap()
                        .unwrap()
                        .version;
                    let mut tx = Transaction::new();
                    tx.put(
                        &hook_address,
                        Expected::Version(version),
                        json!({"value":"external"}).as_object().unwrap(),
                    )
                    .unwrap();
                    tx.commit(store).unwrap();
                    observed.fetch_add(1, Ordering::SeqCst);
                } else if mode == "persistent"
                    || ((mode == "once" || mode == "graph_then_document") && attempt == 0)
                {
                    let ts = TaskStore::new(store);
                    let snapshot = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
                    ts.update(&snapshot, |task| {
                        task.start()?;
                        task.yield_ready()
                    })
                    .unwrap();
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            }));
            scheduler.tick(0).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1, "{mode}");
            let state = scheduler.get(root.id()).unwrap().unwrap();
            let document = Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .unwrap();
            if mode == "once" {
                assert_eq!(state.state(), &TaskState::Succeeded(json!("done")));
                assert_eq!(document.value["value"], "handler");
                assert_eq!(writes.load(Ordering::SeqCst), 1);
            } else {
                assert!(
                    matches!(state.state(), TaskState::Failed(reason) if reason.contains("document conflict")),
                    "{mode}: {:?}",
                    state.state()
                );
                assert_eq!(
                    document.value["value"],
                    if mode == "document" || mode == "graph_then_document" {
                        "external"
                    } else {
                        "initial"
                    }
                );
                if mode == "persistent" {
                    assert_eq!(writes.load(Ordering::SeqCst), 9);
                }
            }
        }
    }

    #[test]
    fn fail_fast_result_scan_finds_running_descendants() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::open(dir.path().join("db"), Registry::default()).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let root = task(2, TaskOwner::Task(parent.id()), "root", false, 3);
        let mut failed = task(3, TaskOwner::Task(parent.id()), "failed", false, 3);
        failed.start().unwrap();
        failed.fail("failure".into()).unwrap();
        let mut nested = task(4, TaskOwner::Task(root.id()), "nested", false, 3);
        nested.start().unwrap();
        let unrelated = task(
            5,
            TaskOwner::Conversation(Uuid::from_u128(10)),
            "other",
            false,
            3,
        );
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&root),
                Save::Create(&failed),
                Save::Create(&nested),
                Save::Create(&unrelated),
            ])
            .unwrap();
        assert_eq!(
            pending_abort_root_for_running(&scheduler.ts(), nested.id(), false).unwrap(),
            Some(root.id())
        );
        assert_eq!(
            pending_abort_root_for_running(&scheduler.ts(), unrelated.id(), false).unwrap(),
            None
        );
    }

    #[test]
    fn fail_fast_recovery_failure_marks_remaining_running_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut first = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
        first.start().unwrap();
        let mut second = task(3, TaskOwner::Task(parent.id()), "child", false, 3);
        second.start().unwrap();
        let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&first),
                Save::Create(&second),
            ])
            .unwrap();
        drop(scheduler);
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register_with_abort(
            "child",
            false,
            |_: &Task| panic!("must not replay"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert!(matches!(
            scheduler.get(first.id()).unwrap().unwrap().state(),
            TaskState::Failed(_)
        ));
        assert_eq!(
            scheduler.get(second.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        scheduler.tick(0).unwrap();
        assert!(
            scheduler
                .get(second.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(second.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.get(parent.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn fail_fast_cancelled_child_marks_and_wakes_before_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut cancelled = task(2, TaskOwner::Task(parent.id()), "child", false, 3);
        cancelled.cancel().unwrap();
        let mut running = task(3, TaskOwner::Task(parent.id()), "child", false, 3);
        running.start().unwrap();
        let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&cancelled),
                Save::Create(&running),
            ])
            .unwrap();
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        assert_eq!(
            scheduler.get(running.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        assert!(scheduler.wake_snapshot(0).unwrap().active);
        scheduler.tick(0).unwrap();
        let marked = scheduler.get(running.id()).unwrap().unwrap();
        assert_eq!(marked.state(), &TaskState::Running);
        assert!(marked.abort_requested());
        assert!(scheduler.wake_snapshot(0).unwrap().active);
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register_with_abort(
            "child",
            false,
            |_: &Task| panic!("must not replay"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        scheduler.replace_registry(registry).unwrap();
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(running.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.get(parent.id()).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn fail_fast_reopen_marks_before_recovering_non_replay_safe_without_definition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let root = Uuid::from_u128(1);
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            false,
            3,
        );
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let mut failed = task(2, TaskOwner::Task(root), "child", false, 3);
        failed.start().unwrap();
        failed.fail("failure".into()).unwrap();
        let mut interrupted = task(3, TaskOwner::Task(root), "child", false, 3);
        interrupted.start().unwrap();
        let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        scheduler
            .ts()
            .save(&[
                Save::Create(&parent),
                Save::Create(&failed),
                Save::Create(&interrupted),
            ])
            .unwrap();
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        assert_eq!(
            scheduler.get(interrupted.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        assert!(
            !scheduler
                .get(interrupted.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        scheduler.tick(0).unwrap();
        let marked = scheduler.get(interrupted.id()).unwrap().unwrap();
        assert_eq!(marked.state(), &TaskState::Running);
        assert!(marked.abort_requested());
        drop(scheduler); // another crash immediately after the durable mark
        let mut registry = Registry::default();
        registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register_with_abort(
            "child",
            false,
            |_: &Task| panic!("must not replay"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        for _ in 0..8 {
            scheduler.tick(0).unwrap();
            if scheduler.get(root).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(interrupted.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(scheduler.get(root).unwrap().unwrap().is_terminal());
    }

    #[test]
    fn fail_fast_marks_running_non_replay_safe_and_recovers_multiple_siblings() {
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let parent = Uuid::from_u128(1);
        let running = Uuid::from_u128(3);
        let other = Uuid::from_u128(5);
        let mut p = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "p",
            false,
            3,
        );
        p.start().unwrap();
        p.wait(WaitFor::TasksFailFast(vec![
            Uuid::from_u128(2),
            running,
            other,
        ]))
        .unwrap();
        let mut failed = task(2, TaskOwner::Task(parent), "c", false, 3);
        failed.start().unwrap();
        failed.fail("failed".into()).unwrap();
        let mut live = task(3, TaskOwner::Task(parent), "c", false, 3);
        live.start().unwrap();
        let leaf = task(4, TaskOwner::Task(running), "c", false, 3);
        let sibling = task(5, TaskOwner::Task(parent), "c", false, 3);
        let scheduler = Scheduler::open(&path, Registry::default()).unwrap();
        scheduler
            .ts()
            .save(&[
                Save::Create(&p),
                Save::Create(&failed),
                Save::Create(&live),
                Save::Create(&leaf),
                Save::Create(&sibling),
            ])
            .unwrap();
        let mut scheduler = scheduler;
        scheduler.tick(0).unwrap();
        let marked = scheduler.get(running).unwrap().unwrap();
        assert_eq!(marked.state(), &TaskState::Running);
        assert!(marked.abort_requested());
        assert!(!scheduler.get(other).unwrap().unwrap().abort_requested());
        assert!(matches!(
            scheduler.get(parent).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        drop(scheduler);
        let cleaned = Arc::new(Mutex::new(Vec::new()));
        let observed = cleaned.clone();
        let mut registry = Registry::default();
        registry.register("p", false, |_t: &Task| Ok(Step::Succeed(json!(null))));
        registry.register_with_abort(
            "c",
            false,
            |_t: &Task| Ok(Step::Succeed(json!(null))),
            move |t: &Task| {
                observed.lock().unwrap().push(t.id());
                Ok(Step::Succeed(json!(null)))
            },
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(
            scheduler.get(running).unwrap().unwrap().state(),
            &TaskState::Running
        );
        for _ in 0..16 {
            scheduler.tick(0).unwrap();
            if scheduler.get(parent).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert!(scheduler.get(parent).unwrap().unwrap().is_terminal());
        for id in [running, Uuid::from_u128(4), other] {
            assert_eq!(
                scheduler.get(id).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
        }
        let cleaned = cleaned.lock().unwrap();
        let leaf_index = cleaned
            .iter()
            .position(|id| *id == Uuid::from_u128(4))
            .unwrap();
        let parent_index = cleaned.iter().position(|id| *id == running).unwrap();
        assert!(leaf_index < parent_index);
    }

    #[test]
    fn any_and_fail_fast_survive_reopen_and_cancel_descendants() {
        for fail_fast in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tasks.db");
            let parent = Uuid::from_u128(1);
            let child1 = Uuid::from_u128(2);
            let child2 = Uuid::from_u128(3);
            let grandchild = Uuid::from_u128(4);
            let s = Scheduler::open(&path, Registry::default()).unwrap();
            let mut p = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "p",
                false,
                2,
            );
            p.start().unwrap();
            p.wait(if fail_fast {
                WaitFor::TasksFailFast(vec![child1, child2])
            } else {
                WaitFor::TasksAny(vec![child1, child2])
            })
            .unwrap();
            let mut c1 = task(2, TaskOwner::Task(parent), "c", false, 2);
            c1.start().unwrap();
            c1.fail("failure".into()).unwrap();
            let c2 = task(3, TaskOwner::Task(parent), "c", false, 2);
            let gc = task(4, TaskOwner::Task(child2), "c", false, 2);
            s.ts()
                .save(&[
                    Save::Create(&p),
                    Save::Create(&c1),
                    Save::Create(&c2),
                    Save::Create(&gc),
                ])
                .unwrap();
            drop(s);
            let mut r = Registry::default();
            r.register("p", false, |_t: &Task| {
                Ok(Step::Wait {
                    checkpoint: cp("after"),
                    condition: WaitFor::Signal("hold".into()),
                    children: vec![],
                })
            });
            r.register_with_abort(
                "c",
                false,
                |_t: &Task| {
                    Ok(Step::Wait {
                        checkpoint: cp("hold"),
                        condition: WaitFor::Signal("hold".into()),
                        children: vec![],
                    })
                },
                |_t: &Task| Ok(Step::Succeed(json!(null))),
            );
            let mut s = Scheduler::open(&path, r).unwrap();
            for _ in 0..12 {
                s.tick(0).unwrap();
                if matches!(
                    s.get(parent).unwrap().unwrap().state(),
                    TaskState::Waiting(WaitFor::Signal(_))
                ) {
                    break;
                }
            }
            assert!(matches!(
                s.get(parent).unwrap().unwrap().state(),
                TaskState::Waiting(WaitFor::Signal(_))
            ));
            if fail_fast {
                assert_eq!(
                    s.get(child2).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
                assert_eq!(
                    s.get(grandchild).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
            } else {
                assert_eq!(s.get(child2).unwrap().unwrap().state(), &TaskState::Ready);
                s.cancel(parent).unwrap();
                assert_eq!(
                    s.get(child2).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
                assert_eq!(
                    s.get(grandchild).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
            }
            drop(s);
            let s = Scheduler::open(&path, Registry::default()).unwrap();
            assert_eq!(
                s.get(child2).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
        }
    }

    #[test]
    fn interrupted_parent_holds_failure_and_aborts_child_without_replay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let s = Scheduler::open(&path, Registry::default()).unwrap();
        let mut parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "p",
            false,
            2,
        );
        let mut child = task(2, TaskOwner::Task(parent.id()), "c", true, 2);
        parent.start().unwrap();
        child.start().unwrap();
        s.ts()
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        drop(s);
        let mut r = Registry::default();
        r.register("p", false, |_t: &Task| panic!("unsafe parent replayed"));
        r.register("c", true, |_t: &Task| panic!("cancelled child replayed"));
        let mut s = Scheduler::open(&path, r).unwrap();
        assert!(matches!(
            s.get(parent.id()).unwrap().unwrap().state(),
            TaskState::CompletingFailed(_)
        ));
        // Recovery stays deferred until the parent's failure cascade is marked.
        assert_eq!(
            s.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        s.tick(0).unwrap();
        assert!(s.get(child.id()).unwrap().unwrap().abort_requested());
        for _ in 0..4 {
            if s.get(child.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            s.tick(0).unwrap();
        }
        assert!(s.get(child.id()).unwrap().unwrap().is_terminal());
        for _ in 0..4 {
            if s.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            s.tick(0).unwrap();
        }
        assert!(matches!(
            s.get(parent.id()).unwrap().unwrap().state(),
            TaskState::Failed(_)
        ));
    }

    #[test]
    fn signal_timer_children_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let parent_id = Uuid::from_u128(1);
        let child_id = Uuid::from_u128(2);
        fn registry() -> Registry {
            let mut r = Registry::default();
            r.register("parent", true, |t: &Task| {
                Ok(match t.checkpoint().phase.as_str() {
                    "start" => Step::Wait {
                        checkpoint: cp("child"),
                        condition: WaitFor::Tasks(vec![Uuid::from_u128(2)]),
                        children: vec![task(2, TaskOwner::Task(t.id()), "child", true, 2)],
                    },
                    "child" => Step::Wait {
                        checkpoint: cp("signal"),
                        condition: WaitFor::Signal("ok".into()),
                        children: vec![],
                    },
                    "signal" => {
                        assert!(t.delivered_signal().is_some());
                        Step::Wait {
                            checkpoint: cp("timer"),
                            condition: WaitFor::DeadlineMillis(100),
                            children: vec![],
                        }
                    }
                    _ => Step::Succeed(json!(42)),
                })
            });
            r.register("child", true, |_t: &Task| Ok(Step::Succeed(json!("done"))));
            r
        }
        let mut s = Scheduler::open(&path, registry()).unwrap();
        s.submit(&task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            2,
        ))
        .unwrap();
        assert!(s.tick(0).unwrap());
        assert_eq!(
            s.get(child_id).unwrap().unwrap().owner(),
            &TaskOwner::Task(parent_id)
        );
        drop(s);
        let mut s = Scheduler::open(&path, registry()).unwrap();
        assert!(s.tick(0).unwrap()); // child
        assert!(s.tick(0).unwrap()); // wake parent and wait on signal
        assert!(!s.tick(0).unwrap());
        s.signal(&Signal::new(
            Uuid::from_u128(7),
            parent_id,
            "ok".into(),
            json!(1),
        ))
        .unwrap();
        assert!(s.tick(0).unwrap());
        drop(s);
        let mut s = Scheduler::open(&path, registry()).unwrap();
        assert!(!s.tick(99).unwrap());
        assert!(s.tick(100).unwrap());
        assert_eq!(
            s.get(parent_id).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!(42))
        );
    }
    #[test]
    fn interrupted_invocations_replay_only_when_marked_safe() {
        let dir = tempfile::tempdir().unwrap();
        for safe in [false, true] {
            let path = dir.path().join(format!("{safe}.db"));
            let mut registry = Registry::default();
            registry.register("x", safe, |_t: &Task| Err("uncertain".into()));
            let mut s = Scheduler::open(&path, registry).unwrap();
            s.submit(&task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "x",
                safe,
                3,
            ))
            .unwrap();
            assert!(matches!(s.tick(0), Err(Error::Handler(_))));
            assert!(matches!(s.tick(0), Err(Error::Poisoned)));
            drop(s);
            let mut registry = Registry::default();
            registry.register("x", safe, |_t: &Task| {
                Ok(Step::Succeed(json!("should not execute")))
            });
            let mut s = Scheduler::open(&path, registry).unwrap();
            assert_eq!(
                s.get(Uuid::from_u128(1))
                    .unwrap()
                    .unwrap()
                    .retired_attempt_id(),
                None
            );
            if safe {
                assert_eq!(
                    s.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                    &TaskState::Ready
                );
                s.tick(0).unwrap();
                assert!(matches!(
                    s.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                    TaskState::Succeeded(_)
                ));
            } else {
                assert!(matches!(
                    s.get(Uuid::from_u128(1)).unwrap().unwrap().state(),
                    TaskState::Failed(_)
                ));
            }
        }
    }
    #[test]
    fn owner_lock_and_atomic_child_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut r = Registry::default();
        r.register("x", true, |t: &Task| {
            let child = task(2, TaskOwner::Task(t.id()), "x", true, 2);
            Ok(Step::Wait {
                checkpoint: cp("next"),
                condition: WaitFor::Tasks(vec![child.id()]),
                children: vec![child.clone(), child],
            })
        });
        let mut s = Scheduler::open(&path, r).unwrap();
        assert!(matches!(
            Scheduler::open(&path, Registry::default()),
            Err(Error::Owner(_))
        ));
        s.submit(&task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "x",
            true,
            2,
        ))
        .unwrap();
        assert!(matches!(s.tick(0), Err(Error::Ownership)));
        assert!(s.ts().load(Uuid::from_u128(2)).unwrap().is_none());
        assert_eq!(
            s.ts()
                .load(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .task
                .checkpoint(),
            &cp("start")
        );
    }
    #[test]
    fn stale_child_read_with_keyed_admission_obeys_replay_policy() {
        struct Parent(std::path::PathBuf);
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(t.id()), "child", true, 5);
                if t.checkpoint().phase == "start" {
                    docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                    return Ok(Step::Continue(cp("read")));
                }
                docs.get_owned_child(child.id())?;
                let db = durable_store::Store::open(&self.0).map_err(|e| e.to_string())?;
                let tasks = task_store::TaskStore::new(&db);
                let old = tasks.load(child.id()).map_err(|e| e.to_string())?.unwrap();
                tasks
                    .update(&old, |child| child.start())
                    .map_err(|e| e.to_string())?;
                let id = docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![id]),
                    children: vec![],
                })
            }
        }
        for replay_safe in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tasks.db");
            let mut registry = Registry::default();
            registry.register("parent", replay_safe, Parent(path.clone()));
            registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                replay_safe,
                5,
            );
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            scheduler.submit(&parent).unwrap();
            scheduler.tick(0).unwrap();
            let second = scheduler.tick(0);
            if replay_safe {
                second.unwrap();
                let state = scheduler.get(parent.id()).unwrap().unwrap();
                assert_eq!(state.state(), &TaskState::Ready);
                assert_eq!(state.checkpoint(), &cp("read"));
            } else {
                second.unwrap();
                assert!(
                    matches!(scheduler.get(parent.id()).unwrap().unwrap().state(), TaskState::CompletingFailed(reason) if reason.contains("reconciliation"))
                );
                assert_eq!(
                    scheduler.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
                    &TaskState::Running
                );
            }
        }
    }

    #[test]
    fn staged_keyed_child_live_handler_reopens_and_replays_identity() {
        struct Parent;
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let candidate = if t.checkpoint().phase == "start" {
                    2
                } else {
                    3
                };
                let child = task(candidate, TaskOwner::Task(t.id()), "child", true, 5);
                let id = docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                if t.checkpoint().phase == "start" {
                    Ok(Step::Wait {
                        checkpoint: cp("waiting"),
                        condition: WaitFor::TasksAny(vec![id]),
                        children: vec![],
                    })
                } else {
                    Ok(Step::Succeed(json!(id.to_string())))
                }
            }
        }
        fn registry() -> Registry {
            let mut r = Registry::default();
            r.register("parent", true, Parent);
            r.register("child", true, |_: &Task| Ok(Step::Succeed(json!("done"))));
            r
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            5,
        );
        {
            let mut s = Scheduler::open(&path, registry()).unwrap();
            s.submit(&parent).unwrap();
            s.tick(0).unwrap();
            assert_eq!(
                s.get(parent.id()).unwrap().unwrap().state(),
                &TaskState::Waiting(WaitFor::TasksAny(vec![Uuid::from_u128(2)]))
            );
            assert!(s.get(Uuid::from_u128(2)).unwrap().is_some());
        }
        let mut s = Scheduler::open(&path, registry()).unwrap();
        for _ in 0..4 {
            s.tick(0).unwrap();
        }
        assert!(matches!(
            s.get(parent.id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
        assert!(s.get(Uuid::from_u128(3)).unwrap().is_none());
    }

    #[test]
    fn staged_existing_child_composes_with_signal_deadline() {
        use crate::task_store::{SignalResult, SignalToken};
        struct Parent(bool);
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(t.id()), "child", true, 5);
                let id = docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                if t.checkpoint().phase == "start" {
                    Ok(Step::Continue(cp("signal")))
                } else {
                    let token = SignalToken::new("answer", 1).unwrap();
                    assert!(matches!(
                        docs.signal_at(id, &token, "a", json!(1), 0).unwrap(),
                        SignalResult::Delivered
                    ));
                    if self.0 {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    Ok(Step::Wait {
                        checkpoint: cp("waiting"),
                        condition: WaitFor::TasksAny(vec![id]),
                        children: vec![],
                    })
                }
            }
        }
        for expired in [false, true] {
            let mut registry = Registry::default();
            registry.register("parent", true, Parent(expired));
            registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            let dir = tempfile::tempdir().unwrap();
            let mut s = Scheduler::open(dir.path().join("tasks.db"), registry).unwrap();
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                5,
            );
            s.submit(&parent).unwrap();
            s.tick(0).unwrap();
            let child = s.ts().load(Uuid::from_u128(2)).unwrap().unwrap();
            let token = SignalToken::new("answer", 1).unwrap();
            s.ts()
                .update(&child, |t| {
                    t.start()?;
                    t.wait(WaitFor::TypedSleep {
                        until_millis: Some(if expired { 10 } else { 60_000 }),
                        signals: vec![token.key("a").unwrap()],
                    })
                })
                .unwrap();
            s.tick(0).unwrap();
            let current = s.get(parent.id()).unwrap().unwrap();
            if expired {
                assert_eq!(current.state(), &TaskState::Ready);
                assert_eq!(current.checkpoint(), &cp("signal"));
            } else {
                assert_eq!(
                    current.state(),
                    &TaskState::Waiting(WaitFor::TasksAny(vec![child.task.id()]))
                );
            }
            assert!(matches!(
                (
                    expired,
                    s.ts()
                        .deliver_signal_at(child.task.id(), &token, "a", json!(1), 0)
                        .unwrap()
                ),
                (true, SignalResult::Delivered) | (false, SignalResult::Duplicate)
            ));
        }
    }

    #[test]
    fn staged_unregistered_child_is_discarded_on_failure() {
        struct Parent;
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(t.id()), "unregistered", true, 5);
                docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                Ok(Step::Fail("failed deliberately".into()))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut registry = Registry::default();
        registry.register("parent", true, Parent);
        let mut s = Scheduler::open(&path, registry).unwrap();
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            5,
        );
        s.submit(&parent).unwrap();
        s.tick(0).unwrap();
        assert!(matches!(
            s.get(parent.id()).unwrap().unwrap().state(),
            TaskState::Failed(_)
        ));
        assert!(s.get(Uuid::from_u128(2)).unwrap().is_none());
        drop(s);
        let s = Scheduler::open(&path, Registry::default()).unwrap();
        assert!(matches!(
            s.get(parent.id()).unwrap().unwrap().state(),
            TaskState::Failed(_)
        ));
        assert!(s.get(Uuid::from_u128(2)).unwrap().is_none());
    }

    #[test]
    fn staged_children_cannot_reuse_a_candidate_id() {
        struct Parent(bool);
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(t.id()), "child", true, 5);
                let id = docs.stage_keyed_child("first", "tool", child.input(), child.clone())?;
                let children = if self.0 {
                    vec![child.clone()]
                } else {
                    docs.stage_keyed_child("second", "tool", child.input(), child.clone())?;
                    vec![]
                };
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![id]),
                    children,
                })
            }
        }
        for direct in [false, true] {
            let mut registry = Registry::default();
            registry.register("parent", true, Parent(direct));
            registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            let dir = tempfile::tempdir().unwrap();
            let mut s = Scheduler::open(dir.path().join("tasks.db"), registry).unwrap();
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                5,
            );
            s.submit(&parent).unwrap();
            assert!(s.tick(0).is_err());
            assert!(s.ts().load(Uuid::from_u128(2)).unwrap().is_none());
            assert_eq!(
                s.ts().load(parent.id()).unwrap().unwrap().task.state(),
                &TaskState::Running
            );
        }
    }

    #[test]
    fn staged_keyed_child_conflict_and_abort_discard_before_replay() {
        use crate::document::{Address, TaskDocuments, Transaction};
        use durable_store::{Expected, Store};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Parent {
            path: std::path::PathBuf,
            calls: Arc<AtomicUsize>,
            abort: bool,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let addr = Address {
                    scope: "stage".into(),
                    name: "conflict".into(),
                    key: "".into(),
                };
                docs.get(&addr).unwrap();
                let child = task(2, TaskOwner::Task(t.id()), "child", true, 5);
                let id = docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let db = Store::open(&self.path).unwrap();
                    let mut tx = Transaction::new();
                    tx.put(
                        &addr,
                        Expected::Missing,
                        json!({"external": true}).as_object().unwrap(),
                    )
                    .unwrap();
                    tx.commit(&db).unwrap();
                    if self.abort {
                        request_abort_subtree(&TaskStore::new(&db), t.id()).unwrap();
                    }
                }
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![id]),
                    children: vec![],
                })
            }
        }
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("tasks.db");
            let calls = Arc::new(AtomicUsize::new(0));
            let registry = || {
                let mut r = Registry::default();
                r.register(
                    "parent",
                    true,
                    Parent {
                        path: path.clone(),
                        calls: calls.clone(),
                        abort,
                    },
                );
                r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
                r
            };
            let parent = task(
                1,
                TaskOwner::Conversation(Uuid::from_u128(9)),
                "parent",
                true,
                5,
            );
            {
                let mut s = Scheduler::open(&path, registry()).unwrap();
                s.submit(&parent).unwrap();
                s.tick(0).unwrap();
                assert!(s.get(Uuid::from_u128(2)).unwrap().is_none());
                let current = s.get(parent.id()).unwrap().unwrap();
                assert_eq!(current.state(), &TaskState::Ready);
                assert_eq!(current.abort_requested(), abort);
            }
            let mut s = Scheduler::open(&path, registry()).unwrap();
            if !abort {
                s.tick(0).unwrap();
                assert!(s.get(Uuid::from_u128(2)).unwrap().is_some());
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            } else {
                s.tick(0).unwrap();
                assert!(s.get(Uuid::from_u128(2)).unwrap().is_none());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }

    #[test]
    fn staged_keyed_child_parent_version_conflict_reopens_and_replays() {
        use durable_store::Store;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Parent {
            path: std::path::PathBuf,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                t: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let child = task(2, TaskOwner::Task(t.id()), "child", true, 5);
                let id = docs.stage_keyed_child("tool-a", "tool", child.input(), child.clone())?;
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let db = Store::open(&self.path).unwrap();
                    let snapshot = TaskStore::new(&db).load(t.id()).unwrap().unwrap();
                    crate::keyed_child::admit(
                        &db,
                        &snapshot,
                        "tool-a",
                        "tool",
                        child.input(),
                        &child,
                    )
                    .unwrap();
                }
                Ok(Step::Wait {
                    checkpoint: cp("waiting"),
                    condition: WaitFor::TasksAny(vec![id]),
                    children: vec![],
                })
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = || {
            let mut r = Registry::default();
            r.register(
                "parent",
                true,
                Parent {
                    path: path.clone(),
                    calls: calls.clone(),
                },
            );
            r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
            r
        };
        let parent = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            5,
        );
        {
            let mut s = Scheduler::open(&path, registry()).unwrap();
            s.submit(&parent).unwrap();
            assert!(s.tick(0).unwrap());
            assert_eq!(
                s.get(parent.id()).unwrap().unwrap().state(),
                &TaskState::Ready
            );
        }
        let mut s = Scheduler::open(&path, registry()).unwrap();
        s.tick(0).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            s.get(parent.id()).unwrap().unwrap().state(),
            &TaskState::Waiting(WaitFor::TasksAny(vec![Uuid::from_u128(2)]))
        );
        assert!(s.get(Uuid::from_u128(2)).unwrap().is_some());
    }
}

#[cfg(test)]
mod foreground_ancestor_regressions {
    use super::*;
    use crate::ownership_edge::{Edge, EdgeStore, Mode};
    use crate::runtime::{Config, Harness, Options};
    use async_trait::async_trait;
    use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Stub;
    #[async_trait]
    impl Model for Stub {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
            unreachable!()
        }
    }
    fn options() -> Options {
        Options {
            model: Arc::new(Stub),
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
        }
    }
    fn config() -> Config {
        Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        }
    }
    fn registered(id: u128, owner: TaskOwner, kind: &str) -> Task {
        Task::new(
            Uuid::from_u128(id),
            owner,
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            5,
        )
        .unwrap()
        .registered(kind.into(), true)
    }
    fn setup(path: &std::path::Path, mode: Mode, disposition: &str) -> (Uuid, Uuid) {
        let mut harness = Harness::open_paused(path, options()).unwrap();
        let root = harness.create_conversation(config()).unwrap();
        let conversation = harness.create_conversation(config()).unwrap();
        let parent = registered(1, TaskOwner::Conversation(root), "parent").into_task_tool();
        let child = registered(2, TaskOwner::Conversation(conversation), "child");
        let timer = registered(3, TaskOwner::Task(child.id()), "timer");
        let db = durable_store::Store::open(path).unwrap();
        let store = TaskStore::new(&db);
        store.save(&[Save::Create(&parent)]).unwrap();
        let edge = Edge {
            task: parent.id(),
            task_owner: parent.owner().clone(),
            child: conversation,
            mode,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let snapshot = store.load(parent.id()).unwrap().unwrap();
        store
            .update(&snapshot, |task| {
                task.start()?;
                match disposition {
                    "failure" => task.hold_failure("failed".into())?,
                    "terminal_success" => task.succeed(json!("done"))?,
                    "terminal_failure" => task.fail("failed".into())?,
                    "terminal_cancel" => task.cancel()?,
                    _ => (),
                }
                Ok(())
            })
            .unwrap();
        // Create the conversation's work after the anchor transition, including
        // after terminal success; this is supported by the ownership model.
        store
            .save(&[Save::Create(&child), Save::Create(&timer)])
            .unwrap();
        let snapshot = store.load(timer.id()).unwrap().unwrap();
        store
            .update(&snapshot, |task| {
                task.start()?;
                task.wait(WaitFor::DeadlineMillis(10))
            })
            .unwrap();
        if disposition == "abort" {
            harness.abort_owned_graph(None, Some(parent.id())).unwrap();
        }
        (parent.id(), child.id())
    }
    fn registry(calls: Arc<AtomicUsize>, include_parent: bool) -> Registry {
        let mut registry = Registry::default();
        if include_parent {
            registry.register("parent", true, |_: &Task| {
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "wait".into(),
                        data: json!(null),
                    },
                    condition: WaitFor::DeadlineMillis(1000),
                    children: vec![],
                })
            });
        }
        registry.register_with_abort(
            "child",
            true,
            move |_: &Task| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Step::Succeed(json!(null)))
            },
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry.register_with_abort(
            "timer",
            true,
            |_: &Task| Ok(Step::Succeed(json!(null))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry
    }

    #[test]
    fn reopened_missing_task_tool_fences_foreground_reservation_and_wake() {
        for mode in [Mode::Foreground, Mode::Background] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (parent, child) = setup(&path, mode, "running");
            let calls = Arc::new(AtomicUsize::new(0));
            let mut scheduler = Scheduler::open(&path, registry(calls.clone(), false)).unwrap();
            assert_eq!(
                scheduler.get(parent).unwrap().unwrap().state(),
                &TaskState::Running
            );
            let wake = scheduler.wake_snapshot(10).unwrap();
            assert_eq!(wake.active, mode == Mode::Background);
            assert!(scheduler.tick(10).unwrap()); // poll due timer, and possibly reserve child
            if mode == Mode::Foreground {
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                assert_eq!(
                    scheduler.get(child).unwrap().unwrap().state(),
                    &TaskState::Ready
                );
                assert!(!scheduler.wake_snapshot(10).unwrap().active);
                assert!(!scheduler.tick(10).unwrap());
                scheduler
                    .replace_registry(registry(calls.clone(), true))
                    .unwrap();
                assert!(scheduler.wake_snapshot(10).unwrap().active);
                for _ in 0..4 {
                    if calls.load(Ordering::SeqCst) > 0 {
                        break;
                    }
                    scheduler.tick(10).unwrap();
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn terminal_conversation_anchors_distinguish_success_from_failure() {
        for (disposition, allowed) in [
            ("terminal_success", true),
            ("terminal_failure", false),
            ("terminal_cancel", false),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (_, child) = setup(&path, Mode::Foreground, disposition);
            let calls = Arc::new(AtomicUsize::new(0));
            let mut scheduler = Scheduler::open(&path, registry(calls.clone(), false)).unwrap();
            assert_eq!(
                scheduler.wake_snapshot(0).unwrap().active,
                allowed,
                "{disposition}"
            );
            let independent = scheduler.reserve_independent_root().unwrap();
            assert_eq!(independent.is_some(), allowed, "{disposition}");
            if let Some(invocation) = independent {
                scheduler
                    .settle_invocation(invocation.execute().unwrap())
                    .unwrap();
            } else {
                let _ = scheduler.tick(0).unwrap();
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                usize::from(allowed),
                "{disposition}"
            );
            if !allowed {
                assert_eq!(
                    scheduler.get(child).unwrap().unwrap().state(),
                    &TaskState::Ready
                );
            }
        }
    }

    #[test]
    fn conversation_work_traverses_successful_task_anchor_chain() {
        for (ancestor_outcome, allowed) in
            [("success", true), ("failure", false), ("cancel", false)]
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let root = harness.create_conversation(config()).unwrap();
            let conversation = harness.create_conversation(config()).unwrap();
            let db = durable_store::Store::open(&path).unwrap();
            let tasks = TaskStore::new(&db);
            let ancestor = registered(9, TaskOwner::Conversation(root), "missing_ancestor");
            let anchor = registered(1, TaskOwner::Task(ancestor.id()), "missing_anchor");
            let child = registered(2, TaskOwner::Conversation(conversation), "child");
            let second_child = registered(3, TaskOwner::Conversation(conversation), "child");
            tasks
                .save(&[Save::Create(&ancestor), Save::Create(&anchor)])
                .unwrap();
            let edge = Edge {
                task: anchor.id(),
                task_owner: anchor.owner().clone(),
                child: conversation,
                mode: Mode::Foreground,
            };
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let snapshot = tasks.load(anchor.id()).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    task.start()?;
                    task.succeed(json!("done"))
                })
                .unwrap();
            let snapshot = tasks.load(ancestor.id()).unwrap().unwrap();
            tasks
                .update(&snapshot, |task| {
                    task.start()?;
                    match ancestor_outcome {
                        "success" => task.succeed(json!("done")),
                        "failure" => task.fail("failed".into()),
                        _ => task.cancel(),
                    }
                })
                .unwrap();
            tasks
                .save(&[Save::Create(&child), Save::Create(&second_child)])
                .unwrap();
            drop(harness);
            let calls = Arc::new(AtomicUsize::new(0));
            let mut scheduler = Scheduler::open(&path, registry(calls.clone(), false)).unwrap();
            assert_eq!(
                scheduler.wake_snapshot(0).unwrap().active,
                allowed,
                "{ancestor_outcome}"
            );
            let invocation = scheduler.reserve_independent_root().unwrap();
            assert_eq!(invocation.is_some(), allowed, "{ancestor_outcome}");
            if let Some(invocation) = invocation {
                scheduler
                    .settle_invocation(invocation.execute().unwrap())
                    .unwrap();
                assert!(scheduler.tick(0).unwrap());
            } else {
                assert!(!scheduler.tick(0).unwrap());
            }
            assert_eq!(calls.load(Ordering::SeqCst), if allowed { 2 } else { 0 });
        }
    }

    #[test]
    fn foreground_child_does_not_execute_during_parent_abort_or_failure_recovery() {
        for disposition in ["abort", "failure"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (_, child) = setup(&path, Mode::Foreground, disposition);
            let calls = Arc::new(AtomicUsize::new(0));
            let harness = Harness::open_paused(&path, options()).unwrap();
            let mut scheduler = harness
                .attach_tasks(registry(calls.clone(), false))
                .unwrap();
            for _ in 0..16 {
                if scheduler.get(child).unwrap().unwrap().is_terminal() {
                    break;
                }
                scheduler.tick(10).unwrap();
            }
            assert!(
                scheduler.get(child).unwrap().unwrap().is_terminal(),
                "{disposition}"
            );
            assert_ne!(
                scheduler.get(child).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!(null))
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{disposition}");
        }
    }
    #[test]
    fn background_tasks_and_descendants_ignore_ordinary_conversation_abort_after_reopen() {
        for with_descendants in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let conversation = harness.create_conversation(config()).unwrap();
            let db = durable_store::Store::open(&path).unwrap();
            let tasks = TaskStore::new(&db);
            let kind = if with_descendants { "parent" } else { "child" };
            let mut background = registered(11, TaskOwner::Conversation(conversation), kind)
                .background()
                .unwrap();
            if with_descendants {
                background.start().unwrap();
                background.wait(WaitFor::Signal("hold".into())).unwrap();
            }
            let first = registered(12, TaskOwner::Task(background.id()), "child");
            let second = registered(13, TaskOwner::Task(background.id()), "child");
            tasks.save(&[Save::Create(&background)]).unwrap();
            if with_descendants {
                tasks
                    .save(&[Save::Create(&first), Save::Create(&second)])
                    .unwrap();
            }
            harness.request_abort(conversation).unwrap();
            assert!(harness.conversation(conversation).unwrap().abort_requested);
            assert!(
                !tasks
                    .load(background.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested()
            );
            drop(harness);
            let calls = Arc::new(AtomicUsize::new(0));
            let mut scheduler = Scheduler::open(&path, registry(calls.clone(), true)).unwrap();
            assert!(scheduler.wake_snapshot(0).unwrap().active);
            let invocation = scheduler
                .reserve_independent_root()
                .unwrap()
                .expect("background work is runnable");
            scheduler
                .settle_invocation(invocation.execute().unwrap())
                .unwrap();
            if with_descendants {
                assert!(scheduler.tick(0).unwrap());
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if with_descendants { 2 } else { 1 }
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn held_tool_cascade_process_fixture() {
        use std::io::Write;
        let Ok(path) = std::env::var("PI_HELD_TOOL_CASCADE_DB") else {
            return;
        };
        let marked = std::env::var_os("PI_HELD_TOOL_CASCADE_MARKED").is_some();
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let root = harness.create_conversation(config()).unwrap();
        let foreground = harness.create_conversation(config()).unwrap();
        let background = harness.create_conversation(config()).unwrap();
        let db = durable_store::Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        let parent = registered(1, TaskOwner::Conversation(root), "parent").into_task_tool();
        let child = registered(2, TaskOwner::Conversation(foreground), "child");
        let mut nested = registered(3, TaskOwner::Task(parent.id()), "nested").into_task_tool();
        let mut independent = registered(4, TaskOwner::Conversation(background), "background");
        nested.start().unwrap();
        nested.wait(WaitFor::Signal("hold".into())).unwrap();
        independent.start().unwrap();
        independent.wait(WaitFor::Signal("hold".into())).unwrap();
        tasks
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&nested),
                Save::Create(&independent),
            ])
            .unwrap();
        for (conversation, mode) in [
            (foreground, Mode::Foreground),
            (background, Mode::Background),
        ] {
            let edge = Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child: conversation,
                mode,
            };
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
        }
        let mut registry = Registry::default();
        registry.register("parent", true, |_: &Task| {
            Ok(Step::Fail("held tool failure".into()))
        });
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| panic!("child ran"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry.register_with_abort(
            "nested",
            true,
            |_: &Task| panic!("nested ran"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry.register("background", true, |_: &Task| panic!("background ran"));
        let mut scheduler = harness.attach_tasks(registry).unwrap();
        let child_snapshot = tasks.load(child.id()).unwrap().unwrap();
        tasks.update(&child_snapshot, |task| task.start()).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(matches!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            TaskState::CompletingFailed(_)
        ));
        if marked {
            assert!(scheduler.tick(0).unwrap());
            assert!(
                scheduler
                    .get(child.id())
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            assert!(
                scheduler
                    .get(nested.id())
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            assert!(harness.conversation(foreground).unwrap().abort_requested);
        }
        println!("HELD_TOOL_CASCADE_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn held_task_tool_failure_cascades_across_reopen_before_and_after_marks() {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        use std::sync::mpsc;
        use std::time::Duration;
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for marked in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut command = Command::new(std::env::current_exe().unwrap());
            command.args(["--exact", "task_scheduler::foreground_ancestor_regressions::held_tool_cascade_process_fixture", "--nocapture"])
                .env("PI_HELD_TOOL_CASCADE_DB", &path)
                .stdout(Stdio::piped()).stderr(Stdio::inherit());
            if marked {
                command.env("PI_HELD_TOOL_CASCADE_MARKED", "1");
            }
            let mut process = KillOnDrop(command.spawn().unwrap());
            let stdout = process.0.stdout.take().unwrap();
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    if line.unwrap().contains("HELD_TOOL_CASCADE_READY") {
                        let _ = tx.send(());
                        break;
                    }
                }
            });
            rx.recv_timeout(Duration::from_secs(20))
                .expect("fixture did not persist the held outcome");
            process.0.kill().unwrap();
            process.0.wait().unwrap();
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let db = durable_store::Store::open(&path).unwrap();
            let parent = Uuid::from_u128(1);
            let child = Uuid::from_u128(2);
            let nested = Uuid::from_u128(3);
            let independent = Uuid::from_u128(4);
            let edges = EdgeStore::new(&db).children(parent).unwrap();
            let foreground = edges
                .iter()
                .find(|e| e.edge.mode == Mode::Foreground)
                .unwrap()
                .edge
                .child;
            let background = edges
                .iter()
                .find(|e| e.edge.mode == Mode::Background)
                .unwrap()
                .edge
                .child;
            let mut registry = Registry::default();
            registry.register_with_abort(
                "child",
                true,
                |_: &Task| panic!("interrupted child replayed"),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            registry.register_with_abort(
                "nested",
                true,
                |_: &Task| panic!("nested replayed"),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            registry.register("background", true, |_: &Task| panic!("background ran"));
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            if !marked {
                assert_eq!(
                    scheduler.get(child).unwrap().unwrap().state(),
                    &TaskState::Running
                );
                assert!(scheduler.tick(0).unwrap());
            }
            assert!(scheduler.get(parent).unwrap().unwrap().abort_requested());
            assert!(scheduler.get(child).unwrap().unwrap().abort_requested());
            assert!(scheduler.get(nested).unwrap().unwrap().abort_requested());
            assert!(harness.conversation(foreground).unwrap().abort_requested);
            assert!(!harness.conversation(background).unwrap().abort_requested);
            assert!(
                !scheduler
                    .get(independent)
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            for _ in 0..16 {
                if scheduler.get(child).unwrap().unwrap().is_terminal()
                    && scheduler.get(nested).unwrap().unwrap().is_terminal()
                {
                    break;
                }
                scheduler.tick(0).unwrap();
            }
            assert_eq!(
                scheduler.get(child).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            assert_eq!(
                scheduler.get(nested).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            harness
                .drive_owned_cleanup(foreground, &pi_rs_core::CancellationToken::new())
                .await
                .unwrap();
            for _ in 0..8 {
                if scheduler.get(parent).unwrap().unwrap().is_terminal() {
                    break;
                }
                scheduler.tick(0).unwrap();
            }
            assert_eq!(
                scheduler.get(parent).unwrap().unwrap().state(),
                &TaskState::Failed("held tool failure".into())
            );
            assert!(matches!(
                scheduler.get(independent).unwrap().unwrap().state(),
                TaskState::Waiting(_)
            ));
        }
    }
}

#[cfg(test)]
mod standalone_foreground_cleanup_tests {
    use super::*;
    use crate::document::{Address, Documents, Transaction};
    use crate::ownership_edge::{Edge, EdgeStore, Mode};
    use crate::runtime::{AdmissionOptions, Config, Harness, OnAbort, Options, Status};
    use async_trait::async_trait;
    use durable_store::{Expected, Store};
    use pi_rs_core::{CancellationToken, Error as ModelError, Model, ModelRequest, ModelStream};
    use serde_json::json;

    struct Stub;
    #[async_trait]
    impl Model for Stub {
        async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
            panic!("cleanup must not run a model")
        }
    }
    fn options() -> Options {
        Options {
            model: Arc::new(Stub),
            tools: vec![],
            restart_limit: 3,
            max_turns: 3,
            uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
        }
    }
    fn registry() -> Registry {
        let mut registry = Registry::default();
        registry.register_with_abort(
            "parent",
            true,
            |_: &Task| Ok(Step::Fail("failed".into())),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry.register_with_abort(
            "child",
            true,
            |_: &Task| panic!("child ran"),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        registry
    }
    fn task(id: u128, owner: TaskOwner, kind: &str) -> Task {
        Task::new(
            Uuid::from_u128(id),
            owner,
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered(kind.into(), true)
    }
    #[tokio::test]
    async fn standalone_abort_and_failure_drain_conversation_after_restart() {
        for mode in ["abort", "cancel", "failure"] {
            let failure = mode == "failure";
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let cfg = Config {
                model: "stub".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            };
            let root_conversation = harness.create_conversation(cfg.clone()).unwrap();
            let child_conversation = harness.create_conversation(cfg).unwrap();
            let _active = harness
                .submit(child_conversation, None, "active".into())
                .unwrap();
            let kept = harness
                .submit_with_options(
                    child_conversation,
                    None,
                    "keep".into(),
                    AdmissionOptions {
                        on_abort: OnAbort::Keep,
                        ..Default::default()
                    },
                )
                .unwrap();
            let withdrawn = harness
                .submit(child_conversation, None, "withdraw".into())
                .unwrap();
            let reset = harness
                .reset(child_conversation, Some("queued reset".into()))
                .unwrap();
            let root = task(1, TaskOwner::Conversation(root_conversation), "parent");
            let child = task(2, TaskOwner::Conversation(child_conversation), "child");
            let db = Store::open(&path).unwrap();
            TaskStore::new(&db)
                .save(&[Save::Create(&root), Save::Create(&child)])
                .unwrap();
            let edge = Edge {
                task: root.id(),
                task_owner: root.owner().clone(),
                child: child_conversation,
                mode: Mode::Foreground,
            };
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let address = Address {
                scope: "scope".into(),
                name: "owned".into(),
                key: String::new(),
            };
            let child_address = Address {
                scope: "scope".into(),
                name: "child".into(),
                key: String::new(),
            };
            let mut tx = Transaction::new();
            tx.acquire_task_document(&address, root.id()).unwrap();
            tx.acquire_task_document(&child_address, child.id())
                .unwrap();
            tx.put(
                &address,
                Expected::Missing,
                json!({"v":1}).as_object().unwrap(),
            )
            .unwrap();
            tx.put(
                &child_address,
                Expected::Missing,
                json!({"v":2}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(&db).unwrap();
            drop(harness);
            let mut scheduler =
                Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
            if failure {
                assert!(scheduler.tick(0).unwrap());
                assert!(scheduler.tick(0).unwrap());
            } else if mode == "cancel" {
                scheduler.cancel(root.id()).unwrap();
            } else {
                scheduler.request_abort(root.id()).unwrap();
            }
            assert!(
                scheduler
                    .get(child.id())
                    .unwrap()
                    .unwrap()
                    .abort_requested()
            );
            assert!(Documents::new(&db).get(&address).unwrap().is_some());
            assert!(Documents::new(&db).get(&child_address).unwrap().is_some());
            drop(scheduler);
            let mut scheduler =
                Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
            let cancel = CancellationToken::new();
            for _ in 0..20 {
                if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
                scheduler
                    .tick_with_conversation_cleanup(0, &cancel)
                    .await
                    .unwrap();
            }
            assert_eq!(
                scheduler.get(child.id()).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            let expected = if failure {
                TaskState::Failed("failed".into())
            } else {
                TaskState::Cancelled
            };
            assert_eq!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                &expected
            );
            assert!(Documents::new(&db).get(&address).unwrap().is_none());
            assert!(Documents::new(&db).get(&child_address).unwrap().is_none());
            let state: crate::runtime::Conversation = serde_json::from_slice(
                &db.get(format!("runtime/conversation/{child_conversation}").as_bytes())
                    .unwrap()
                    .unwrap()
                    .value,
            )
            .unwrap();
            assert!(!state.abort_requested);
            let persisted = serde_json::to_value(&state).unwrap();
            assert_eq!(persisted["pending_resets"][0]["id"], reset.to_string());
            assert_eq!(
                state
                    .submissions
                    .iter()
                    .find(|s| s.id == kept)
                    .unwrap()
                    .status,
                Status::Queued
            );
            assert_eq!(
                state
                    .submissions
                    .iter()
                    .find(|s| s.id == withdrawn)
                    .unwrap()
                    .status,
                Status::Unanswered {
                    reason: "aborted".into()
                }
            );
        }
    }
    #[tokio::test]
    async fn standalone_driver_finishes_foreground_abort_and_failure() {
        for failure in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let cfg = Config {
                model: "stub".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            };
            let root_conversation = harness.create_conversation(cfg.clone()).unwrap();
            let child_conversation = harness.create_conversation(cfg).unwrap();
            harness
                .submit(child_conversation, None, "active".into())
                .unwrap();
            let kept = harness
                .submit_with_options(
                    child_conversation,
                    None,
                    "keep".into(),
                    AdmissionOptions {
                        on_abort: OnAbort::Keep,
                        ..Default::default()
                    },
                )
                .unwrap();
            let root = task(1, TaskOwner::Conversation(root_conversation), "parent");
            let child = task(2, TaskOwner::Conversation(child_conversation), "child");
            let db = Store::open(&path).unwrap();
            TaskStore::new(&db)
                .save(&[Save::Create(&root), Save::Create(&child)])
                .unwrap();
            let edge = Edge {
                task: root.id(),
                task_owner: root.owner().clone(),
                child: child_conversation,
                mode: Mode::Foreground,
            };
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            drop(harness);
            let mut definitions = Registry::default();
            definitions.register_with_abort(
                "parent",
                true,
                move |task: &Task| {
                    if failure {
                        Ok(Step::Fail("failed".into()))
                    } else {
                        Ok(Step::Wait {
                            checkpoint: task.checkpoint().clone(),
                            condition: WaitFor::Signal("hold".into()),
                            children: vec![],
                        })
                    }
                },
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            definitions.register_with_abort(
                "child",
                true,
                |task: &Task| {
                    Ok(Step::Wait {
                        checkpoint: task.checkpoint().clone(),
                        condition: WaitFor::Signal("hold".into()),
                        children: vec![],
                    })
                },
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            let scheduler =
                Scheduler::open_with_conversation_runtime(&path, definitions, options()).unwrap();
            assert!(matches!(
                Scheduler::open(&path, registry()),
                Err(Error::Owner(_))
            ));
            let driver = crate::task_driver::Driver::start(scheduler);
            if !failure {
                driver.request_abort(root.id()).unwrap();
            }
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                driver.wait_for_task(root.id(), &CancellationToken::new()),
            )
            .await
            .unwrap()
            .unwrap();
            let expected = if failure {
                TaskState::Failed("failed".into())
            } else {
                TaskState::Cancelled
            };
            assert_eq!(result.state(), &expected);
            assert_eq!(
                driver.get(child.id()).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            let state: crate::runtime::Conversation = serde_json::from_slice(
                &db.get(format!("runtime/conversation/{child_conversation}").as_bytes())
                    .unwrap()
                    .unwrap()
                    .value,
            )
            .unwrap();
            assert_eq!(
                state
                    .submissions
                    .iter()
                    .find(|s| s.id == kept)
                    .unwrap()
                    .status,
                Status::Queued
            );
            driver.shutdown().unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_follows_foreground_edge_below_terminal_task_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root_conversation = harness.create_conversation(cfg.clone()).unwrap();
        let child_conversation = harness.create_conversation(cfg).unwrap();
        let root = task(1, TaskOwner::Conversation(root_conversation), "parent");
        let anchor = task(2, TaskOwner::Task(root.id()), "parent");
        let child = task(3, TaskOwner::Conversation(child_conversation), "child");
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks
            .save(&[
                Save::Create(&root),
                Save::Create(&anchor),
                Save::Create(&child),
            ])
            .unwrap();
        let edge = Edge {
            task: anchor.id(),
            task_owner: anchor.owner().clone(),
            child: child_conversation,
            mode: Mode::Foreground,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let snapshot = tasks.load(anchor.id()).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| {
                task.start()?;
                task.succeed(json!("done"))
            })
            .unwrap();
        drop(harness);
        let mut plain = Scheduler::open(&path, registry()).unwrap();
        assert!(matches!(
            plain.request_abort(root.id()),
            Err(Error::ForegroundAbortUnsupported)
        ));
        assert!(!plain.get(child.id()).unwrap().unwrap().abort_requested());
        drop(plain);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        scheduler.cancel(root.id()).unwrap();
        assert!(
            scheduler
                .get(child.id())
                .unwrap()
                .unwrap()
                .abort_requested()
        );
        for _ in 0..20 {
            if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
        }
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(
            scheduler.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(
            scheduler.get(anchor.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[tokio::test]
    async fn manual_cleanup_does_not_wait_for_unrelated_conversation_worker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let conversation = harness
            .create_conversation(Config {
                model: "stub".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        drop(harness);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        assert!(scheduler.request_abort(Uuid::nil()).is_err());
        let control = scheduler.owner().conversation_controls().get(conversation);
        let _worker = control.worker.lock().await;
        let progress = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            scheduler.tick_with_conversation_cleanup(0, &CancellationToken::new()),
        )
        .await
        .expect("unrelated worker blocked cleanup")
        .unwrap();
        assert!(!progress);
    }

    #[tokio::test]
    async fn interrupted_manual_cleanup_requires_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = harness.create_conversation(cfg.clone()).unwrap();
        let conversation = harness.create_conversation(cfg).unwrap();
        let root = task(1, TaskOwner::Conversation(owner), "parent");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&root)]).unwrap();
        let edge = Edge {
            task: root.id(),
            task_owner: root.owner().clone(),
            child: conversation,
            mode: Mode::Foreground,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        drop(harness);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        scheduler.request_abort(root.id()).unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            scheduler
                .tick_with_conversation_cleanup(0, &cancel)
                .await
                .is_err()
        );
        assert!(matches!(scheduler.tick(0), Err(Error::Poisoned)));
        drop(scheduler);
        let cancel = CancellationToken::new();
        let mut reopened =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        assert!(
            reopened
                .tick_with_conversation_cleanup(0, &cancel)
                .await
                .unwrap()
        );
    }
    #[tokio::test]
    async fn marked_unrelated_busy_worker_does_not_block_owned_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let a = harness.create_conversation(cfg.clone()).unwrap();
        let b = harness.create_conversation(cfg.clone()).unwrap();
        let (unrelated, owned) = if a.to_string() < b.to_string() {
            (a, b)
        } else {
            (b, a)
        };
        let root_conversation = harness.create_conversation(cfg).unwrap();
        harness.request_abort(unrelated).unwrap();
        let root = task(1, TaskOwner::Conversation(root_conversation), "parent");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&root)]).unwrap();
        let edge = Edge {
            task: root.id(),
            task_owner: root.owner().clone(),
            child: owned,
            mode: Mode::Foreground,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        drop(harness);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        scheduler.request_abort(root.id()).unwrap();
        let control = scheduler.owner().conversation_controls().get(unrelated);
        let _busy = control.worker.lock().await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            for _ in 0..10 {
                if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                    return;
                }
                scheduler
                    .tick_with_conversation_cleanup(0, &CancellationToken::new())
                    .await
                    .unwrap();
            }
            panic!("owned cleanup did not finish");
        })
        .await
        .expect("unrelated marked worker blocked cleanup");
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(
            scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(unrelated)
                .unwrap()
                .abort_requested
        );
        assert!(
            !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(owned)
                .unwrap()
                .abort_requested
        );
        assert!(!scheduler.tick(0).unwrap());
    }
    #[tokio::test]
    async fn unrelated_marked_cleanup_error_is_not_executed() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct FailingUsage(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for FailingUsage {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                panic!("model must not run")
            }
            async fn record_usage(
                &self,
                _: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(ModelError::Model("unrelated usage failed".into()))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let a = harness.create_conversation(cfg.clone()).unwrap();
        let b = harness.create_conversation(cfg.clone()).unwrap();
        let (unrelated, owned) = if a.to_string() < b.to_string() {
            (a, b)
        } else {
            (b, a)
        };
        let owner = harness.create_conversation(cfg).unwrap();
        harness.request_abort(unrelated).unwrap();
        let root = task(1, TaskOwner::Conversation(owner), "parent");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&root)]).unwrap();
        let edge = Edge {
            task: root.id(),
            task_owner: root.owner().clone(),
            child: owned,
            mode: Mode::Foreground,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        drop(harness);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut runtime_options = options();
        runtime_options.model = Arc::new(FailingUsage(calls.clone()));
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), runtime_options).unwrap();
        scheduler.request_abort(root.id()).unwrap();
        let key = format!("runtime/conversation/{unrelated}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        let mut state: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        state["phase"] =
            json!({"ModelRunning": {"turns":0, "request_id":Uuid::new_v4(), "restarts":0}});
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&state).unwrap(),
            }],
        )
        .unwrap();
        for _ in 0..10 {
            if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
        }
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(unrelated)
                .unwrap()
                .abort_requested
        );
        assert!(
            !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(owned)
                .unwrap()
                .abort_requested
        );
        assert!(!scheduler.tick(0).unwrap());
        // Verify the unrelated fixture fails if a worker tries to settle it.
        assert!(
            scheduler
                .validator
                .as_mut()
                .unwrap()
                .resume_conversation(unrelated, &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn independent_aborted_child_of_successful_completing_owner_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = harness.create_conversation(cfg.clone()).unwrap();
        let child = harness.create_conversation(cfg).unwrap();
        harness.submit(child, None, "active".into()).unwrap();
        let root = task(1, TaskOwner::Conversation(owner), "success");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&root)]).unwrap();
        let edge = Edge {
            task: root.id(),
            task_owner: root.owner().clone(),
            child,
            mode: Mode::Foreground,
        };
        let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let mut definitions = Registry::default();
        definitions.register("success", true, |_: &Task| Ok(Step::Succeed(json!("done"))));
        drop(harness);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, definitions.clone(), options())
                .unwrap();
        for _ in 0..4 {
            scheduler.tick(0).unwrap();
            if matches!(
                scheduler.get(root.id()).unwrap().unwrap().state(),
                TaskState::Completing(_)
            ) {
                break;
            }
        }
        assert!(matches!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            TaskState::Completing(_)
        ));
        scheduler
            .validator
            .as_mut()
            .unwrap()
            .request_abort(child)
            .unwrap();
        drop(scheduler);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, definitions, options()).unwrap();
        for _ in 0..50 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(root.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
        assert!(
            !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(child)
                .unwrap()
                .abort_requested
        );
    }

    #[tokio::test]
    async fn independent_owned_graph_callback_error_or_delay_does_not_block_other_graph() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct ControlledUsage {
            request: Uuid,
            fail: bool,
            entered: Arc<AtomicUsize>,
            release: Arc<tokio::sync::Notify>,
        }
        #[async_trait]
        impl Model for ControlledUsage {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                panic!("model must not run")
            }
            async fn record_usage(
                &self,
                request: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                assert_eq!(request, self.request);
                self.entered.fetch_add(1, Ordering::SeqCst);
                if self.fail {
                    return Err(ModelError::Model("usage failed".into()));
                }
                self.release.notified().await;
                Ok(())
            }
        }
        for fail in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(&path, options()).unwrap();
            let cfg = Config {
                model: "stub".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            };
            let owner = harness.create_conversation(cfg.clone()).unwrap();
            let a = harness.create_conversation(cfg.clone()).unwrap();
            let b = harness.create_conversation(cfg).unwrap();
            let (slow, fast) = if a < b { (a, b) } else { (b, a) };
            let first = task(1, TaskOwner::Conversation(owner), "parent");
            let second = task(2, TaskOwner::Conversation(owner), "parent");
            let db = Store::open(&path).unwrap();
            TaskStore::new(&db)
                .save(&[Save::Create(&first), Save::Create(&second)])
                .unwrap();
            for (task, child) in [(&first, slow), (&second, fast)] {
                let edge = Edge {
                    task: task.id(),
                    task_owner: task.owner().clone(),
                    child,
                    mode: Mode::Foreground,
                };
                let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
                db.commit(&batch.checks, &batch.mutations).unwrap();
            }
            drop(harness);
            let request = Uuid::new_v4();
            let entered = Arc::new(AtomicUsize::new(0));
            let release = Arc::new(tokio::sync::Notify::new());
            let mut opts = options();
            opts.model = Arc::new(ControlledUsage {
                request,
                fail,
                entered: entered.clone(),
                release: release.clone(),
            });
            let mut scheduler =
                Scheduler::open_with_conversation_runtime(&path, registry(), opts).unwrap();
            scheduler.request_abort(first.id()).unwrap();
            scheduler.request_abort(second.id()).unwrap();
            let key = format!("runtime/conversation/{slow}").into_bytes();
            let entry = db.get(&key).unwrap().unwrap();
            let mut state: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            state["phase"] =
                json!({"ModelRunning": {"turns":0, "request_id":request, "restarts":0}});
            db.commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&state).unwrap(),
                }],
            )
            .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                for _ in 0..100 {
                    scheduler
                        .tick_with_conversation_cleanup(0, &CancellationToken::new())
                        .await
                        .unwrap();
                    if entered.load(Ordering::SeqCst) > 0
                        && scheduler.get(second.id()).unwrap().unwrap().is_terminal()
                    {
                        return;
                    }
                }
                panic!("independent graph did not complete");
            })
            .await
            .expect("other graph stalled");
            assert_eq!(
                scheduler.get(second.id()).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
            assert!(!scheduler.get(first.id()).unwrap().unwrap().is_terminal());
            if fail {
                assert!(
                    scheduler
                        .conversation_cleanup_failures()
                        .contains_key(&slow)
                );
            } else {
                release.notify_one();
                for _ in 0..50 {
                    scheduler
                        .tick_with_conversation_cleanup(0, &CancellationToken::new())
                        .await
                        .unwrap();
                    if scheduler.get(first.id()).unwrap().unwrap().is_terminal() {
                        break;
                    }
                }
                assert_eq!(
                    scheduler.get(first.id()).unwrap().unwrap().state(),
                    &TaskState::Cancelled
                );
            }
        }
    }
    #[tokio::test]
    async fn busy_owned_locks_and_unmarked_invalid_transcript_do_not_starve_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = harness.create_conversation(cfg.clone()).unwrap();
        let mut children: Vec<_> = (0..10)
            .map(|_| harness.create_conversation(cfg.clone()).unwrap())
            .collect();
        children.sort();
        let db = Store::open(&path).unwrap();
        for (i, child) in children.iter().copied().enumerate() {
            let parent = task((i + 1) as u128, TaskOwner::Conversation(owner), "parent");
            TaskStore::new(&db).save(&[Save::Create(&parent)]).unwrap();
            let edge = Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            };
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let tasks = TaskStore::new(&db);
            let record = tasks.load(parent.id()).unwrap().unwrap();
            tasks
                .update(&record, |task| {
                    task.start()?;
                    task.succeed(json!(null))
                })
                .unwrap();
            if i < 9 {
                harness.request_abort(child).unwrap();
            }
        }
        drop(harness);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        let bad = children[9];
        let key = format!("runtime/conversation/{bad}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        value["transcript"] = json!("invalid transcript");
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
        assert!(
            scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(bad)
                .is_err()
        );
        let mut locks = Vec::new();
        for child in &children[..8] {
            locks.push(
                scheduler
                    .owner()
                    .conversation_controls()
                    .get(*child)
                    .worker
                    .lock_owned()
                    .await,
            );
        }
        for _ in 0..30 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(children[8])
                .unwrap()
                .abort_requested
            {
                break;
            }
        }
        assert!(
            !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(children[8])
                .unwrap()
                .abort_requested
        );
        for child in &children[..8] {
            assert!(
                scheduler
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(*child)
                    .unwrap()
                    .abort_requested
            );
        }
        drop(locks);
        // Repair and mark the last conversation. The first eight now return
        // immediately without progress; rotating candidates must still reach it.
        let current = db
            .get(format!("runtime/conversation/{bad}").as_bytes())
            .unwrap()
            .unwrap();
        db.commit(
            &[durable_store::Check {
                key: format!("runtime/conversation/{bad}").into_bytes(),
                expected: Expected::Version(current.version),
            }],
            &[durable_store::Mutation::Put {
                key: format!("runtime/conversation/{bad}").into_bytes(),
                value: entry.value,
            }],
        )
        .unwrap();
        scheduler
            .validator
            .as_mut()
            .unwrap()
            .request_abort(bad)
            .unwrap();
        for child in &children[..8] {
            let key = format!("runtime/conversation/{child}").into_bytes();
            let record = db.get(&key).unwrap().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
            value["phase"] = json!({"Blocked": {"turns":0, "calls":[], "index":0, "operation_id":Uuid::new_v4(), "reason":"uncertain"}});
            db.commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: Expected::Version(record.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        }
        for _ in 0..40 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(bad)
                .unwrap()
                .abort_requested
            {
                break;
            }
        }
        assert!(
            !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(bad)
                .unwrap()
                .abort_requested
        );
    }

    #[tokio::test]
    async fn cleanup_crash_worker() {
        let Ok(path) = std::env::var("PI_CLEANUP_CRASH_DB") else {
            return;
        };
        struct CrashModel(std::path::PathBuf);
        #[async_trait]
        impl Model for CrashModel {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
            async fn record_usage(
                &self,
                _: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                std::fs::write(&self.0, b"effect started").unwrap();
                std::future::pending().await
            }
        }
        let mut opts = options();
        opts.model = Arc::new(CrashModel(
            std::env::var("PI_CLEANUP_CRASH_READY").unwrap().into(),
        ));
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(path, registry(), opts).unwrap();
        if let Ok(mode) = std::env::var("PI_CLEANUP_CRASH_DRIVER") {
            if mode == "competing" {
                let mut direct = scheduler.validator.as_ref().unwrap().worker();
                let conversation =
                    Uuid::parse_str(&std::env::var("PI_CLEANUP_CRASH_CHILD").unwrap()).unwrap();
                tokio::spawn(async move {
                    let _ = direct
                        .resume_conversation(conversation, &CancellationToken::new())
                        .await;
                });
                let ready =
                    std::path::PathBuf::from(std::env::var("PI_CLEANUP_CRASH_READY").unwrap());
                while !ready.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                let entered = Arc::new(tokio::sync::Semaphore::new(0));
                crate::runtime::driver_cleanup_hooks()
                    .lock()
                    .unwrap()
                    .insert(conversation, entered.clone());
                let _driver = crate::task_driver::Driver::start(scheduler);
                entered.acquire().await.unwrap().forget();
                std::fs::write(ready.with_extension("driver"), b"started").unwrap();
                std::future::pending::<()>().await;
                return;
            }
            let _driver = crate::task_driver::Driver::start(scheduler);
            std::future::pending::<()>().await;
            return;
        }
        loop {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn killed_cleanup_requires_reconciliation_before_reopen_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CountModel(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for CountModel {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
            async fn record_usage(
                &self,
                _: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let ready = dir.path().join("effect-started");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let db = Store::open(&path).unwrap();
        let parent = task(1, TaskOwner::Conversation(owner), "parent");
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let batch = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            })
            .unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.succeed(json!(null))
            })
            .unwrap();
        h.request_abort(child).unwrap();
        let key = format!("runtime/conversation/{child}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        value["phase"] =
            json!({"ModelRunning": {"turns":0, "request_id":Uuid::new_v4(), "restarts":0}});
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
        drop(h);
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_scheduler::standalone_foreground_cleanup_tests::cleanup_crash_worker",
                "--nocapture",
            ])
            .env("PI_CLEANUP_CRASH_DB", &path)
            .env("PI_CLEANUP_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !ready.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        process.kill().unwrap();
        process.wait().unwrap();
        observed.expect("child did not enter external usage settlement");
        let calls = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(CountModel(calls.clone()));
        let mut direct = Harness::open_paused(&path, opts.clone()).unwrap();
        assert!(
            direct
                .resume_conversation(child, &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(direct);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), opts).unwrap();
        for _ in 0..5 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(child)
                .unwrap()
                .abort_requested
        );
        scheduler.reconcile_owned_cleanup(child).unwrap();
        for _ in 0..20 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(child)
                .unwrap()
                .abort_requested
            {
                break;
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(child)
                .unwrap()
                .abort_requested
        );
    }

    #[tokio::test]
    async fn killed_driver_cleanup_requires_reconciliation_before_reopen_retry() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CountModel(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for CountModel {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
            async fn record_usage(
                &self,
                _: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let ready = dir.path().join("effect-started");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let db = Store::open(&path).unwrap();
        let parent = task(1, TaskOwner::Conversation(owner), "parent");
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let batch = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            })
            .unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.succeed(json!(null))
            })
            .unwrap();
        h.request_abort(child).unwrap();
        let key = format!("runtime/conversation/{child}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        value["phase"] =
            json!({"ModelRunning": {"turns":0, "request_id":Uuid::new_v4(), "restarts":0}});
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
        drop(h);
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_scheduler::standalone_foreground_cleanup_tests::cleanup_crash_worker",
                "--nocapture",
            ])
            .env("PI_CLEANUP_CRASH_DB", &path)
            .env("PI_CLEANUP_CRASH_READY", &ready)
            .env("PI_CLEANUP_CRASH_DRIVER", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !ready.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        process.kill().unwrap();
        process.wait().unwrap();
        observed.expect("child did not enter external usage settlement");
        let calls = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(CountModel(calls.clone()));
        let scheduler = Scheduler::open_with_conversation_runtime(&path, registry(), opts).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if driver
                    .conversation_cleanup_failures()
                    .unwrap()
                    .contains_key(&child)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("driver did not report uncertain cleanup");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        driver.reconcile_owned_cleanup(child).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let entry = db
                    .get(format!("runtime/conversation/{child}").as_bytes())
                    .unwrap()
                    .unwrap();
                let state: crate::runtime::Conversation =
                    serde_json::from_slice(&entry.value).unwrap();
                if !state.abort_requested {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("driver did not retry after reconciliation");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
    }

    #[tokio::test]
    async fn competing_direct_and_driver_cleanup_crash_requires_reconciliation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct CountModel(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for CountModel {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
            async fn record_usage(
                &self,
                _: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let ready = dir.path().join("effect-started");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let db = Store::open(&path).unwrap();
        let parent = task(1, TaskOwner::Conversation(owner), "parent");
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let batch = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            })
            .unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.succeed(json!(null))
            })
            .unwrap();
        h.request_abort(child).unwrap();
        let key = format!("runtime/conversation/{child}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        value["phase"] =
            json!({"ModelRunning": {"turns":0, "request_id":Uuid::new_v4(), "restarts":0}});
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
        drop(h);
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "task_scheduler::standalone_foreground_cleanup_tests::cleanup_crash_worker",
                "--nocapture",
            ])
            .env("PI_CLEANUP_CRASH_DB", &path)
            .env("PI_CLEANUP_CRASH_READY", &ready)
            .env("PI_CLEANUP_CRASH_DRIVER", "competing")
            .env("PI_CLEANUP_CRASH_CHILD", child.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !ready.with_extension("driver").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        process.kill().unwrap();
        process.wait().unwrap();
        observed.expect("child did not enter external usage settlement");
        let calls = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(CountModel(calls.clone()));
        let scheduler = Scheduler::open_with_conversation_runtime(&path, registry(), opts).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if driver
                    .conversation_cleanup_failures()
                    .unwrap()
                    .contains_key(&child)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("driver did not report uncertain cleanup");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        driver.reconcile_owned_cleanup(child).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let entry = db
                    .get(format!("runtime/conversation/{child}").as_bytes())
                    .unwrap()
                    .unwrap();
                let state: crate::runtime::Conversation =
                    serde_json::from_slice(&entry.value).unwrap();
                if !state.abort_requested {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("driver did not retry after reconciliation");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
    }

    #[tokio::test]
    async fn dangling_and_invalid_unrelated_candidates_do_not_block_valid_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = h.create_conversation(cfg.clone()).unwrap();
        let children: Vec<_> = (0..4)
            .map(|_| h.create_conversation(cfg.clone()).unwrap())
            .collect();
        let db = Store::open(&path).unwrap();
        for (i, child) in children.iter().enumerate() {
            let parent = task((i + 1) as u128, TaskOwner::Conversation(owner), "parent");
            let tasks = TaskStore::new(&db);
            tasks.save(&[Save::Create(&parent)]).unwrap();
            let batch = EdgeStore::new(&db)
                .prepare_create(&Edge {
                    task: parent.id(),
                    task_owner: parent.owner().clone(),
                    child: *child,
                    mode: Mode::Foreground,
                })
                .unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            tasks
                .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                    task.start()?;
                    task.succeed(json!(null))
                })
                .unwrap();
        }
        let aborted = task(99, TaskOwner::Conversation(owner), "parent");
        TaskStore::new(&db).save(&[Save::Create(&aborted)]).unwrap();
        h.request_abort_task(aborted.id()).unwrap();
        h.request_abort(children[2]).unwrap();
        drop(h);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        let missing = format!("runtime/conversation/{}", children[0]).into_bytes();
        let entry = db.get(&missing).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: missing.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Delete { key: missing }],
        )
        .unwrap();
        let invalid = format!("runtime/conversation/{}", children[1]).into_bytes();
        let entry = db.get(&invalid).unwrap().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        value["abort_requested"] = json!("invalid");
        db.commit(
            &[durable_store::Check {
                key: invalid.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key: invalid,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
        let malformed = crate::ownership_edge::edge_key(children[3]);
        let entry = db.get(&malformed).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: malformed.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key: malformed,
                value: b"not-json".to_vec(),
            }],
        )
        .unwrap();
        for _ in 0..30 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if !scheduler
                .validator
                .as_ref()
                .unwrap()
                .conversation(children[2])
                .unwrap()
                .abort_requested
            {
                // The opaque edge could belong to any task; no abort handler
                // may run until the host repairs it.
                assert!(
                    scheduler
                        .get(aborted.id())
                        .unwrap()
                        .unwrap()
                        .abort_requested()
                );
                return;
            }
        }
        panic!("unrelated invalid candidates blocked cleanup");
    }

    #[tokio::test]
    async fn standalone_driver_isolates_malformed_records_and_holds_broken_owner() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let broken_child = h.create_conversation(cfg.clone()).unwrap();
        let good_child = h.create_conversation(cfg.clone()).unwrap();
        let malformed = h.create_conversation(cfg).unwrap();
        let broken = task(1, TaskOwner::Conversation(root), "parent");
        let anchor = task(2, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks
            .save(&[Save::Create(&broken), Save::Create(&anchor)])
            .unwrap();
        for (parent, child) in [(&broken, broken_child), (&anchor, good_child)] {
            let edge = EdgeStore::new(&db)
                .prepare_create(&Edge {
                    task: parent.id(),
                    task_owner: parent.owner().clone(),
                    child,
                    mode: Mode::Foreground,
                })
                .unwrap();
            db.commit(&edge.checks, &edge.mutations).unwrap();
        }
        tasks
            .update(&tasks.load(anchor.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.succeed(json!(null))
            })
            .unwrap();
        h.request_abort_task(broken.id()).unwrap();
        h.request_abort(good_child).unwrap();
        drop(h);
        let scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        let key = format!("runtime/conversation/{malformed}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: b"broken-json".to_vec(),
            }],
        )
        .unwrap();
        let key = crate::ownership_edge::edge_key(broken_child);
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: b"broken-edge".to_vec(),
            }],
        )
        .unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let entry = db
                    .get(format!("runtime/conversation/{good_child}").as_bytes())
                    .unwrap()
                    .unwrap();
                let state: crate::runtime::Conversation =
                    serde_json::from_slice(&entry.value).unwrap();
                if !state.abort_requested {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("malformed unrelated records blocked valid driver cleanup");
        let task = driver.get(broken.id()).unwrap().unwrap();
        assert!(task.abort_requested());
        assert!(!task.is_terminal());
        driver.shutdown().unwrap();
    }

    #[test]
    fn ownership_edge_cannot_race_an_ownerless_cleanup_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let parent = task(1, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&parent)]).unwrap();
        let batch = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            })
            .unwrap();
        let key = crate::runtime::owned_cleanup_key(child);
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&Uuid::new_v4()).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            db.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(EdgeStore::new(&db).get(child).unwrap().is_none());
    }

    #[test]
    fn uncertain_failed_owner_is_parked_while_unrelated_task_runs() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let broken = task(1, TaskOwner::Conversation(root), "parent");
        let good = task(2, TaskOwner::Conversation(root), "work");
        let db = Store::open(dir.path().join("db")).unwrap();
        let tasks = TaskStore::new(&db);
        tasks
            .save(&[Save::Create(&broken), Save::Create(&good)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                broken.id(),
                broken.owner().clone(),
                "child",
                cfg,
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let descendant = task(3, TaskOwner::Conversation(child), "work");
        tasks.save(&[Save::Create(&descendant)]).unwrap();
        tasks
            .update(&tasks.load(broken.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.hold_failure("failed".into())
            })
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let mut registry = Registry::default();
        registry.register("work", true, move |_: &Task| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(Step::Succeed(json!(null)))
        });
        let scheduler = h.attach_tasks(registry).unwrap();
        tasks
            .reserve(&tasks.load(descendant.id()).unwrap().unwrap())
            .unwrap();
        let edge_key = crate::ownership_edge::edge_key(child);
        let entry = db.get(&edge_key).unwrap().unwrap();
        let mut edge: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        edge["mode"] = json!("corrupt");
        db.commit(
            &[durable_store::Check {
                key: edge_key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key: edge_key,
                value: serde_json::to_vec(&edge).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            pending_abort_root_for_running(&tasks, descendant.id(), false),
            Err(Error::Busy)
        ));
        drop(scheduler);
        drop(h);
        let h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let seen = calls.clone();
        let mut registry = Registry::default();
        registry.register("work", true, move |_: &Task| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(Step::Succeed(json!(null)))
        });
        let mut scheduler = h
            .attach_tasks(registry)
            .expect("unrelated damaged edge must not block attachment");
        assert_eq!(
            scheduler.get(descendant.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        // A deferred failure selected before corruption must revalidate too.
        assert!(
            resolve_maintenance(&tasks, MaintenanceIntent::Failed(broken.id()))
                .unwrap()
                .is_none()
        );
        for _ in 0..10 {
            scheduler.tick(0).unwrap();
            if calls.load(Ordering::SeqCst) > 0 {
                break;
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let state = scheduler.get(broken.id()).unwrap().unwrap();
        assert!(matches!(state.state(), TaskState::CompletingFailed(_)));
        assert!(!state.abort_requested());
        assert!(scheduler.get(good.id()).unwrap().unwrap().is_terminal());
    }

    async fn exercise_in_flight_uncertain_graph(shutdown_before_repair: bool) {
        use std::{
            sync::{
                atomic::{AtomicUsize, Ordering},
                mpsc,
            },
            time::Duration,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let parent = task(1, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.wait(WaitFor::Signal("hold".into()))
            })
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                parent.id(),
                parent.owner().clone(),
                "child",
                cfg.clone(),
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let damaged = h
            .ensure_task_owned_subagent(
                parent.id(),
                parent.owner().clone(),
                "damaged",
                cfg.clone(),
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let independent_root = h.create_conversation(cfg).unwrap();
        let descendant = task(2, TaskOwner::Conversation(child), "blocked");
        tasks.save(&[Save::Create(&descendant)]).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut registry = Registry::default();
        registry.register("parent", true, |_: &Task| panic!("waiting parent ran"));
        registry.register("blocked", true, move |_: &Task| {
            observed.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(json!("must be discarded")))
        });
        registry.register("work", true, |_: &Task| {
            Ok(Step::Succeed(json!("independent")))
        });
        let scheduler = h.attach_tasks(registry).unwrap();
        let owner = scheduler.owner();
        let driver = crate::task_driver::Driver::start(scheduler);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let gate = owner.task_gate().lock().unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.wake()?;
                task.start()?;
                task.hold_failure("failed".into())
            })
            .unwrap();
        let key = crate::ownership_edge::edge_key(damaged);
        let entry = db.get(&key).unwrap().unwrap();
        let original = entry.value.clone();
        let mut edge: serde_json::Value = serde_json::from_slice(&original).unwrap();
        edge["mode"] = json!("corrupt");
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&edge).unwrap(),
            }],
        )
        .unwrap();
        drop(gate);
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !driver.has_pending_task(descendant.id()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("completed invocation was not retained");
        let good = task(3, TaskOwner::Conversation(independent_root), "work");
        driver.submit(&good).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if driver.get(good.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("unrelated work did not complete");
        assert_eq!(
            driver.get(descendant.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        if shutdown_before_repair {
            let attempt = tasks
                .load(descendant.id())
                .unwrap()
                .unwrap()
                .task
                .attempt_id();
            let (shutdown_tx, shutdown_rx) = mpsc::channel();
            let shutdown = std::thread::spawn(move || {
                shutdown_tx.send(driver.shutdown_with_pending()).unwrap();
            });
            let report = shutdown_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("shutdown must return a pending uncertain settlement")
                .unwrap();
            shutdown.join().unwrap();
            assert!(report.failure().is_none());
            assert_eq!(report.pending().len(), 1);
            let pending = &report.pending()[0];
            assert_eq!(pending.task_id(), descendant.id());
            assert_eq!(pending.attempt_id(), attempt);
            assert!(pending.completed().is_some());
            assert_eq!(
                pending.observed_task().unwrap().unwrap().state(),
                &TaskState::Running
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            drop(report);
            return;
        }
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: original,
            }],
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while driver.has_pending_task(descendant.id()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("repaired graph did not settle pending outcome");
        assert_ne!(
            driver.get(descendant.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("must be discarded"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
    }

    #[tokio::test]
    async fn in_flight_result_waits_for_uncertain_failed_graph_without_stopping_driver() {
        exercise_in_flight_uncertain_graph(false).await;
    }

    #[tokio::test]
    async fn descendant_submission_waits_for_uncertain_graph_and_is_discarded_on_abort() {
        use std::{
            sync::{
                Mutex,
                atomic::{AtomicUsize, Ordering},
                mpsc,
            },
            time::Duration,
        };

        struct Staged {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
            calls: Arc<AtomicUsize>,
            address: Address,
        }
        impl Handler for Staged {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut crate::document::TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                docs.submit_write(
                    "uncertain-descendant".into(),
                    "app.note".into(),
                    Some(json!({"value":1})),
                    None,
                )
                .map_err(|error| error.to_string())?;
                docs.get(&self.address).map_err(|error| error.to_string())?;
                docs.put(
                    &self.address,
                    json!({"committed":true}).as_object().unwrap().clone(),
                )
                .map_err(|error| error.to_string())?;
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| error.to_string())?;
                Ok(Step::Succeed(json!("should not commit")))
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let independent_root = h.create_conversation(cfg.clone()).unwrap();
        let parent = task(1, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.wait(WaitFor::Signal("hold".into()))
            })
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                parent.id(),
                parent.owner().clone(),
                "child",
                cfg.clone(),
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let damaged = h
            .ensure_task_owned_subagent(
                parent.id(),
                parent.owner().clone(),
                "damaged",
                cfg,
                Mode::Foreground,
            )
            .unwrap()
            .child;
        let intermediate = task(2, TaskOwner::Conversation(child), "parent");
        let descendant = task(3, TaskOwner::Task(intermediate.id()), "submit");
        tasks
            .save(&[Save::Create(&intermediate), Save::Create(&descendant)])
            .unwrap();
        tasks
            .update(&tasks.load(intermediate.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.wait(WaitFor::Signal("hold".into()))
            })
            .unwrap();
        let address = Address {
            scope: "test".into(),
            name: "uncertain-descendant".into(),
            key: "".into(),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut registry = Registry::default();
        registry.register("parent", true, |_: &Task| panic!("waiting parent ran"));
        registry.register(
            "submit",
            true,
            Staged {
                entered: entered_tx,
                release: Mutex::new(release_rx),
                calls: calls.clone(),
                address: address.clone(),
            },
        );
        registry.set_abort("parent", |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.set_abort("submit", |_: &Task| Ok(Step::Succeed(json!(null))));
        registry.register("work", true, |_: &Task| {
            Ok(Step::Succeed(json!("independent")))
        });
        let scheduler = h.attach_tasks(registry).unwrap();
        let owner = scheduler.owner();
        let driver = crate::task_driver::Driver::start(scheduler);
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("descendant did not stage submission");
        let gate = owner.task_gate().lock().unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.wake()?;
                task.start()?;
                task.hold_failure("failed".into())
            })
            .unwrap();
        let key = crate::ownership_edge::edge_key(damaged);
        let entry = db.get(&key).unwrap().unwrap();
        let original = entry.value.clone();
        let mut edge: serde_json::Value = serde_json::from_slice(&original).unwrap();
        edge["mode"] = json!("corrupt");
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&edge).unwrap(),
            }],
        )
        .unwrap();
        drop(gate);
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !driver.has_pending_task(descendant.id()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("descendant outcome was not retained");
        assert_eq!(
            driver.get(descendant.id()).unwrap().unwrap().state(),
            &TaskState::Running
        );
        assert!(h.conversation(child).unwrap().application_writes.is_empty());
        assert!(Documents::new(&db).get(&address).unwrap().is_none());
        let independent = task(4, TaskOwner::Conversation(independent_root), "work");
        driver.submit(&independent).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !driver.get(independent.id()).unwrap().unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("independent task was blocked");
        assert!(driver.has_pending_task(descendant.id()));
        assert!(h.conversation(child).unwrap().application_writes.is_empty());
        assert!(Documents::new(&db).get(&address).unwrap().is_none());
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: original,
            }],
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while driver.has_pending_task(descendant.id()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("repaired graph did not release the outcome");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !driver.get(descendant.id()).unwrap().unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("failure cascade did not cancel descendant");
        assert_eq!(
            driver.get(descendant.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
        drop(owner);
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.conversation(child).unwrap().application_writes.is_empty());
        assert!(Documents::new(&db).get(&address).unwrap().is_none());
    }

    #[tokio::test]
    async fn shutdown_returns_in_flight_result_before_uncertain_graph_is_repaired() {
        exercise_in_flight_uncertain_graph(true).await;
    }

    #[test]
    fn anchoring_existing_subagent_and_cleanup_marker_fence_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let child = h.ensure_subagent(root, "race", cfg.clone()).unwrap().child;
        let db = Store::open(&path).unwrap();
        let marker_key = crate::runtime::owned_cleanup_key(child);
        let root_key = format!("runtime/conversation/{child}").into_bytes();
        let root_version = db.get(&root_key).unwrap().unwrap().version;
        let edge_key = crate::ownership_edge::edge_key(child);
        let injected = Store::open(&path).unwrap();
        let m = marker_key.clone();
        let e = edge_key.clone();
        crate::subagent::set_before_anchor_commit(Box::new(move || {
            injected
                .commit(
                    &[
                        durable_store::Check {
                            key: m.clone(),
                            expected: Expected::Missing,
                        },
                        durable_store::Check {
                            key: e,
                            expected: Expected::Missing,
                        },
                        durable_store::Check {
                            key: root_key,
                            expected: Expected::Version(root_version),
                        },
                    ],
                    &[durable_store::Mutation::Put {
                        key: m,
                        value: serde_json::to_vec(&Uuid::new_v4()).unwrap(),
                    }],
                )
                .unwrap();
        }));
        assert!(h.anchor_subagent(root, "race").is_err());
        assert!(EdgeStore::new(&db).get(child).unwrap().is_none());
        let marker = db.get(&marker_key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: marker_key.clone(),
                expected: Expected::Version(marker.version),
            }],
            &[durable_store::Mutation::Delete { key: marker_key }],
        )
        .unwrap();
        h.anchor_subagent(root, "race").unwrap();
        // The reverse order is fenced by the cleanup transaction's edge check.
        let second = h.ensure_subagent(root, "reverse", cfg).unwrap().child;
        let edge_key = crate::ownership_edge::edge_key(second);
        let marker_key = crate::runtime::owned_cleanup_key(second);
        let root_key = format!("runtime/conversation/{second}").into_bytes();
        let root_version = db.get(&root_key).unwrap().unwrap().version;
        h.anchor_subagent(root, "reverse").unwrap();
        assert!(matches!(
            db.commit(
                &[
                    durable_store::Check {
                        key: marker_key.clone(),
                        expected: Expected::Missing
                    },
                    durable_store::Check {
                        key: edge_key,
                        expected: Expected::Missing
                    },
                    durable_store::Check {
                        key: root_key,
                        expected: Expected::Version(root_version)
                    },
                ],
                &[durable_store::Mutation::Put {
                    key: marker_key,
                    value: serde_json::to_vec(&Uuid::new_v4()).unwrap()
                }]
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
    }

    #[test]
    fn completing_task_isolates_corrupt_reserved_edge_and_fences_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let broken = task(1, TaskOwner::Conversation(root), "parent");
        let good = task(2, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks
            .save(&[Save::Create(&broken), Save::Create(&good)])
            .unwrap();
        let child = h
            .ensure_task_owned_subagent(
                broken.id(),
                broken.owner().clone(),
                "child",
                cfg,
                Mode::Foreground,
            )
            .unwrap()
            .child;
        for task in [&broken, &good] {
            tasks
                .update(&tasks.load(task.id()).unwrap().unwrap(), |task| {
                    task.start()?;
                    task.hold_success(json!("done"))
                })
                .unwrap();
        }
        let mut scheduler = h.attach_tasks(registry()).unwrap();
        let key = crate::ownership_edge::edge_key(child);
        let entry = db.get(&key).unwrap().unwrap();
        let mut edge: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        edge["mode"] = json!("corrupt");
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&edge).unwrap(),
            }],
        )
        .unwrap();
        for _ in 0..5 {
            scheduler.tick(0).unwrap();
            if scheduler.get(good.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert!(matches!(
            scheduler.get(good.id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ));
        assert!(matches!(
            scheduler.get(broken.id()).unwrap().unwrap().state(),
            TaskState::Completing(_)
        ));
    }

    #[tokio::test]
    async fn missing_owned_child_holds_its_task_but_not_an_unrelated_abort() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let broken = task(1, TaskOwner::Conversation(root), "parent");
        let independent = task(2, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db)
            .save(&[Save::Create(&broken), Save::Create(&independent)])
            .unwrap();
        let edge = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: broken.id(),
                task_owner: broken.owner().clone(),
                child,
                mode: Mode::Foreground,
            })
            .unwrap();
        db.commit(&edge.checks, &edge.mutations).unwrap();
        h.request_abort_task(broken.id()).unwrap();
        h.request_abort_task(independent.id()).unwrap();
        drop(h);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        let key = format!("runtime/conversation/{child}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Delete { key }],
        )
        .unwrap();
        for _ in 0..20 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if scheduler
                .get(independent.id())
                .unwrap()
                .unwrap()
                .is_terminal()
            {
                break;
            }
        }
        assert!(
            scheduler
                .get(independent.id())
                .unwrap()
                .unwrap()
                .is_terminal()
        );
        assert!(!scheduler.get(broken.id()).unwrap().unwrap().is_terminal());
    }

    #[tokio::test]
    async fn pre_marker_timeout_retries_after_summary_fence_is_released() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let parent = task(1, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let edge = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            })
            .unwrap();
        db.commit(&edge.checks, &edge.mutations).unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.start()?;
                task.succeed(json!(null))
            })
            .unwrap();
        h.request_abort(child).unwrap();
        drop(h);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        scheduler.cleanup_timeout = Duration::from_millis(20);
        let control = scheduler.owner().conversation_controls().get(child);
        let guard = control.summary_execution.read().await;
        scheduler
            .tick_with_conversation_cleanup(0, &CancellationToken::new())
            .await
            .unwrap();
        assert!(scheduler.cleanup_running.contains(&child));
        tokio::time::sleep(Duration::from_millis(60)).await;
        scheduler
            .tick_with_conversation_cleanup(0, &CancellationToken::new())
            .await
            .unwrap();
        assert!(!scheduler.cleanup_running.contains(&child));
        assert!(
            !scheduler
                .conversation_cleanup_failures()
                .contains_key(&child)
        );
        assert!(
            db.get(&crate::runtime::owned_cleanup_key(child))
                .unwrap()
                .is_none()
        );
        drop(guard);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                scheduler
                    .tick_with_conversation_cleanup(0, &CancellationToken::new())
                    .await
                    .unwrap();
                if !scheduler
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(child)
                    .unwrap()
                    .abort_requested
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup did not retry after the summary fence was released");
    }

    #[tokio::test]
    async fn missing_background_child_does_not_hold_owner_abort() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let root = h.create_conversation(cfg.clone()).unwrap();
        let child = h.create_conversation(cfg).unwrap();
        let parent = task(1, TaskOwner::Conversation(root), "parent");
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db).save(&[Save::Create(&parent)]).unwrap();
        let edge = EdgeStore::new(&db)
            .prepare_create(&Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Background,
            })
            .unwrap();
        db.commit(&edge.checks, &edge.mutations).unwrap();
        h.request_abort_task(parent.id()).unwrap();
        drop(h);
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        let key = format!("runtime/conversation/{child}").into_bytes();
        let entry = db.get(&key).unwrap().unwrap();
        db.commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Delete { key }],
        )
        .unwrap();
        for _ in 0..20 {
            scheduler
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if scheduler.get(parent.id()).unwrap().unwrap().is_terminal() {
                break;
            }
        }
        assert_eq!(
            scheduler.get(parent.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
    }

    #[tokio::test]
    async fn timed_out_cleanup_slots_release_for_an_unrelated_graph() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Stalled(Arc<AtomicUsize>);
        #[async_trait]
        impl Model for Stalled {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                panic!("model must not run")
            }
            async fn record_usage(
                &self,
                _: Uuid,
                _: &pi_rs_core::UsageReport,
            ) -> Result<(), ModelError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                std::future::pending().await
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        let cfg = Config {
            model: "stub".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let owner = harness.create_conversation(cfg.clone()).unwrap();
        let mut children: Vec<_> = (0..9)
            .map(|_| harness.create_conversation(cfg.clone()).unwrap())
            .collect();
        children.sort();
        let db = Store::open(&path).unwrap();
        for (i, child) in children.iter().copied().enumerate() {
            let parent = task((i + 1) as u128, TaskOwner::Conversation(owner), "parent");
            TaskStore::new(&db).save(&[Save::Create(&parent)]).unwrap();
            let edge = Edge {
                task: parent.id(),
                task_owner: parent.owner().clone(),
                child,
                mode: Mode::Foreground,
            };
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
            let tasks = TaskStore::new(&db);
            let record = tasks.load(parent.id()).unwrap().unwrap();
            tasks
                .update(&record, |task| {
                    task.start()?;
                    task.succeed(json!(null))
                })
                .unwrap();
            harness.request_abort(child).unwrap();
        }
        drop(harness);
        let entered = Arc::new(AtomicUsize::new(0));
        let mut opts = options();
        opts.model = Arc::new(Stalled(entered.clone()));
        let mut scheduler =
            Scheduler::open_with_conversation_runtime(&path, registry(), opts).unwrap();
        scheduler.cleanup_timeout = std::time::Duration::from_millis(250);
        for child in &children[..8] {
            let key = format!("runtime/conversation/{child}").into_bytes();
            let entry = db.get(&key).unwrap().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            value["phase"] =
                json!({"ModelRunning": {"turns":0, "request_id":Uuid::new_v4(), "restarts":0}});
            db.commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        }
        scheduler
            .tick_with_conversation_cleanup(0, &CancellationToken::new())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                scheduler
                    .tick_with_conversation_cleanup(0, &CancellationToken::new())
                    .await
                    .unwrap();
                if !scheduler
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(children[8])
                    .unwrap()
                    .abort_requested
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("saturated cleanups starved the ninth graph");
        assert_eq!(entered.load(Ordering::SeqCst), 8);
        for child in &children[..8] {
            assert!(
                scheduler
                    .conversation_cleanup_failures()
                    .contains_key(child)
            );
            assert!(
                scheduler
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(*child)
                    .unwrap()
                    .abort_requested
            );
        }
        drop(scheduler);
        let mut reopened =
            Scheduler::open_with_conversation_runtime(&path, registry(), options()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while reopened.conversation_cleanup_failures().len() < 8 {
                reopened
                    .tick_with_conversation_cleanup(0, &CancellationToken::new())
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reopened attempts did not remain blocked");
        assert_eq!(entered.load(Ordering::SeqCst), 8);
        for id in &children[..8] {
            assert!(
                reopened
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(*id)
                    .unwrap()
                    .abort_requested
            );
            reopened.reconcile_owned_cleanup(*id).unwrap();
        }
        for _ in 0..50 {
            reopened
                .tick_with_conversation_cleanup(0, &CancellationToken::new())
                .await
                .unwrap();
            if children[..8].iter().all(|id| {
                !reopened
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(*id)
                    .unwrap()
                    .abort_requested
            }) {
                break;
            }
        }
        for id in &children[..8] {
            assert!(
                !reopened
                    .validator
                    .as_ref()
                    .unwrap()
                    .conversation(*id)
                    .unwrap()
                    .abort_requested
            );
        }
    }

    #[test]
    fn task_notification_reservation_fences_signal_at_commit() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{
            Config, Harness, Options, SubmissionNotify, submission_settled_signal,
        };
        use crate::task_store::{Signal, TaskStore};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Writer {
            target: Uuid,
            local: bool,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Writer {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
                let notify = Some(SubmissionNotify {
                    task_id: task.id(),
                    key: "collision".into(),
                });
                let result = if self.local {
                    docs.submit_write_without_request_id("app.reservation".into(), None, notify)
                } else {
                    docs.submit_write_to_session_conversation_without_request_id(
                        self.target,
                        "app.reservation".into(),
                        None,
                        notify,
                    )
                };
                if attempt > 0 {
                    assert!(result.is_err());
                    return Ok(Step::Succeed(json!(null)));
                }
                result.map_err(|e| e.to_string())?;
                let address = Address {
                    scope: "test".into(),
                    name: "reservation-fence".into(),
                    key: "".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(
                    &address,
                    json!({"effect":true}).as_object().unwrap().clone(),
                )
                .map_err(|e| e.to_string())?;
                Ok(Step::Continue(Checkpoint {
                    phase: "next".into(),
                    data: json!(null),
                }))
            }
        }
        for local in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut harness = Harness::open_paused(
                &path,
                Options {
                    model: Arc::new(Stub),
                    tools: vec![],
                    restart_limit: 3,
                    max_turns: 3,
                    uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                },
            )
            .unwrap();
            let config = || Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            };
            let owner = harness.create_conversation(config()).unwrap();
            let target = if local {
                owner
            } else {
                harness.create_conversation(config()).unwrap()
            };
            let store = durable_store::Store::open(&path).unwrap();
            let sessions = crate::session_membership::SessionMembership::new(&store);
            let session = sessions.create().unwrap();
            sessions.enroll(session, owner).unwrap();
            if !local {
                sessions.enroll(session, target).unwrap();
            }
            let calls = Arc::new(AtomicUsize::new(0));
            let mut registry = Registry::default();
            registry.register(
                "reservation-fence",
                true,
                Writer {
                    target,
                    local,
                    calls: calls.clone(),
                },
            );
            let mut scheduler = harness.attach_tasks(registry).unwrap();
            let writer = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(owner),
                Checkpoint {
                    phase: "start".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("reservation-fence".into(), true);
            scheduler.submit(&writer).unwrap();
            let once = Arc::new(AtomicBool::new(false));
            let writer_id = writer.id();
            scheduler.set_result_commit_hook(Arc::new(move |_, store| {
            if !once.swap(true, Ordering::SeqCst) {
                let id = Uuid::new_v4();
                let value = json!({"id":id,"conversationId":owner,"type":"write","status":"done","entry":id});
                let signal = Signal::new_typed(Uuid::new_v4(), writer_id, &submission_settled_signal(), "collision", value).unwrap();
                TaskStore::new(store).send_signal(&signal).unwrap();
            }
        }));
            for _ in 0..8 {
                scheduler.tick(0).unwrap();
                if scheduler.get(writer.id()).unwrap().unwrap().is_terminal() {
                    break;
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert!(matches!(
                scheduler.get(writer.id()).unwrap().unwrap().state(),
                TaskState::Succeeded(_)
            ));
            assert!(
                harness
                    .conversation(target)
                    .unwrap()
                    .application_writes
                    .is_empty()
            );
            let address = Address {
                scope: "test".into(),
                name: "reservation-fence".into(),
                key: "".into(),
            };
            assert!(Documents::new(&store).get(&address).unwrap().is_none());
            let binding = format!(
                "durable/owned-subagent-notify/{}/636f6c6c6973696f6e",
                writer.id()
            );
            assert!(store.get(binding.as_bytes()).unwrap().is_none());
            assert!(
                TaskStore::new(&store)
                    .read_typed_signal(writer.id(), &submission_settled_signal(), "collision")
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn queued_reservation_rebuilds_unrelated_signal_and_conversation_conflicts() {
        use crate::document::{Address, Documents, TaskDocuments};
        use crate::runtime::{
            AdmissionOptions, Config, Harness, Options, Status, SubmissionNotify,
            submission_settled_signal,
        };
        use crate::task_store::{Signal, SignalResult, SignalToken, TaskStore};
        use async_trait::async_trait;
        use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        struct Stub;
        #[async_trait]
        impl Model for Stub {
            async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
                unreachable!()
            }
        }
        struct Queued {
            target: Uuid,
            local: bool,
            observe: bool,
            calls: Arc<AtomicUsize>,
        }
        impl Handler for Queued {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<Step, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let notify = Some(SubmissionNotify {
                    task_id: task.id(),
                    key: "queued".into(),
                });
                if self.local {
                    docs.submit_input_without_request_id(
                        "queued".into(),
                        AdmissionOptions::default(),
                        notify,
                    )
                } else {
                    docs.submit_input_to_session_conversation_without_request_id(
                        self.target,
                        "queued".into(),
                        AdmissionOptions::default(),
                        notify,
                    )
                }
                .map_err(|e| e.to_string())?;
                let address = Address {
                    scope: "test".into(),
                    name: "queued-proof".into(),
                    key: "".into(),
                };
                docs.get(&address).map_err(|e| e.to_string())?;
                docs.put(
                    &address,
                    json!({"committed":true}).as_object().unwrap().clone(),
                )
                .map_err(|e| e.to_string())?;
                if self.observe {
                    let token = SignalToken::new("handler-observation", 1).unwrap();
                    docs.signal_at(task.id(), &token, "handler", json!(3), 0)
                        .map_err(|e| e.to_string())?;
                }
                Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: json!(null),
                    },
                    condition: WaitFor::TypedSleep {
                        until_millis: None,
                        signals: vec![
                            submission_settled_signal().key("queued").unwrap(),
                            SignalToken::new("unrelated", 1)
                                .unwrap()
                                .key("consumed")
                                .unwrap(),
                        ],
                    },
                    children: vec![],
                })
            }
        }
        for local in [false, true] {
            for safe in [false, true] {
                for mode in 0..5 {
                    if safe && mode >= 2 {
                        continue;
                    }
                    let signal_race = mode == 1 || mode == 2;
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("db");
                    let mut harness = Harness::open_paused(
                        &path,
                        Options {
                            model: Arc::new(Stub),
                            tools: vec![],
                            restart_limit: 3,
                            max_turns: 3,
                            uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
                        },
                    )
                    .unwrap();
                    let config = || Config {
                        model: "test".into(),
                        system_prompt: String::new(),
                        max_output_tokens: 10,
                    };
                    let owner = harness.create_conversation(config()).unwrap();
                    let target = if local {
                        owner
                    } else {
                        harness.create_conversation(config()).unwrap()
                    };
                    let unrelated = harness.create_conversation(config()).unwrap();
                    harness.submit(target, None, "busy".into()).unwrap();
                    let store = durable_store::Store::open(&path).unwrap();
                    if !local {
                        let sessions = crate::session_membership::SessionMembership::new(&store);
                        let session = sessions.create().unwrap();
                        sessions.enroll(session, owner).unwrap();
                        sessions.enroll(session, target).unwrap();
                    }
                    let calls = Arc::new(AtomicUsize::new(0));
                    let mut registry = Registry::default();
                    registry.register(
                        "queued-proof",
                        safe,
                        Queued {
                            target,
                            local,
                            observe: mode == 2,
                            calls: calls.clone(),
                        },
                    );
                    let mut scheduler = harness.attach_tasks(registry).unwrap();
                    let task = Task::new(
                        Uuid::new_v4(),
                        TaskOwner::Conversation(owner),
                        Checkpoint {
                            phase: "start".into(),
                            data: json!(null),
                        },
                        3,
                    )
                    .unwrap()
                    .registered("queued-proof".into(), safe);
                    scheduler.submit(&task).unwrap();
                    let once = Arc::new(AtomicBool::new(false));
                    let task_id = task.id();
                    scheduler.set_result_commit_hook(Arc::new(move |_, store| {
                        if once.swap(true, Ordering::SeqCst) {
                            return;
                        }
                        if mode == 3 {
                            let id = Uuid::new_v4();
                            let token = SignalToken::new("unrelated", 1).unwrap();
                            let signal =
                                Signal::new_typed(id, task_id, &token, "consumed", json!(7))
                                    .unwrap();
                            let mut value = serde_json::to_value(signal).unwrap();
                            value["consumed"] = json!(true);
                            let mut signal_key = b"durable/signal/".to_vec();
                            signal_key.extend_from_slice(task_id.as_bytes());
                            signal_key.extend_from_slice(id.as_bytes());
                            store
                                .commit(
                                    &[],
                                    &[
                                        durable_store::Mutation::Put {
                                            key: signal_key,
                                            value: serde_json::to_vec(&value).unwrap(),
                                        },
                                        durable_store::Mutation::Put {
                                            key: task_store::signal_revision_key(task_id),
                                            value: vec![],
                                        },
                                    ],
                                )
                                .unwrap();
                        } else if signal_race {
                            let token = SignalToken::new("unrelated", 1).unwrap();
                            assert_eq!(
                                TaskStore::new(store)
                                    .deliver_signal_at(task_id, &token, "other", json!(7), 0)
                                    .unwrap(),
                                SignalResult::Delivered
                            );
                        } else {
                            let key = format!("runtime/conversation/{unrelated}").into_bytes();
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
                            if mode == 4 {
                                let address = Address {
                                    scope: "test".into(),
                                    name: "queued-proof".into(),
                                    key: "".into(),
                                };
                                let mut tx = crate::document::Transaction::new();
                                tx.put(
                                    &address,
                                    durable_store::Expected::Missing,
                                    json!({"external":true}).as_object().unwrap(),
                                )
                                .unwrap();
                                tx.commit(store).unwrap();
                            }
                        }
                    }));
                    assert!(scheduler.tick(0).unwrap());
                    let current = scheduler.get(task.id()).unwrap().unwrap();
                    if mode >= 2 {
                        assert!(
                            matches!(current.state(), TaskState::Failed(_)),
                            "original signal, wait or document proof weakened: {:?}",
                            current.state()
                        );
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        assert_eq!(harness.conversation(target).unwrap().submissions.len(), 1);
                        let binding =
                            format!("durable/owned-subagent-notify/{}/717565756564", task.id());
                        assert!(store.get(binding.as_bytes()).unwrap().is_none());
                        continue;
                    }
                    assert!(
                        matches!(current.state(), TaskState::Waiting(_)),
                        "local={local} safe={safe} signal={signal_race}: {:?}",
                        current.state()
                    );
                    assert_eq!(calls.load(Ordering::SeqCst), 1);
                    let state = harness.conversation(target).unwrap();
                    assert_eq!(state.submissions.len(), 2);
                    assert_eq!(state.submissions[1].status, Status::Queued);
                    let binding =
                        format!("durable/owned-subagent-notify/{}/717565756564", task.id());
                    let bound: (Uuid, Uuid) = serde_json::from_slice(
                        &store.get(binding.as_bytes()).unwrap().unwrap().value,
                    )
                    .unwrap();
                    assert_eq!(bound, (target, state.submissions[1].id));
                    let address = Address {
                        scope: "test".into(),
                        name: "queued-proof".into(),
                        key: "".into(),
                    };
                    assert!(Documents::new(&store).get(&address).unwrap().is_some());
                }
            }
        }
    }
}
