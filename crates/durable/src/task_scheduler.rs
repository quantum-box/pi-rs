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
}
impl<F> Handler for F
where
    F: Fn(&Task) -> Result<Step, String> + Send + Sync,
{
    fn run(&self, task: &Task) -> Result<Step, String> {
        self(task)
    }
}

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
struct Registration {
    identity: Arc<()>,
    version: u32,
    migrate: Option<Arc<Migration>>,
    replay_safe: bool,
    handler: Arc<dyn Handler>,
    abort: Option<Arc<dyn Handler>>,
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
    #[error("invalid ownership or child creation")]
    Ownership,
    #[error("foreground owned conversation abort is not supported")]
    ForegroundAbortUnsupported,
    #[error("task has live owned children")]
    LiveChildren,
    #[error("handler failed with an uncertain outcome: {0}")]
    Handler(String),
    #[error("a task scheduler is already attached to this owner")]
    AlreadyAttached,
    #[error("scheduler must be reopened after an invocation or commit failure")]
    Poisoned,
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
                TaskState::Completing(_)
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
                    if reason.is_none() && definitions.ancestors_available(task, &tasks) =>
                {
                    result.active = true
                }
                TaskState::Running if reason.is_none() => result.active = true,
                TaskState::Waiting(wait) => {
                    let deadline = match wait {
                        WaitFor::DeadlineMillis(at) => Some(*at),
                        WaitFor::Sleep { until_millis, .. }
                        | WaitFor::TypedSleep { until_millis, .. } => *until_millis,
                        _ => None,
                    };
                    let ready = ts.wait_ready(item, now_millis)?;
                    if ready
                        && ((reason.is_none() && definitions.ancestors_available(task, &tasks))
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
impl WakeDefinitions {
    fn blocked_reason(&self, task: &Task) -> Option<BlockedReason> {
        if matches!(task.state(), TaskState::Completing(_)) {
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
    fn ancestors_available(&self, task: &Task, tasks: &[task_store::StoredTask]) -> bool {
        let mut owner = task.owner();
        let mut visited = HashSet::new();
        while let TaskOwner::Task(id) = owner {
            if !visited.insert(*id) {
                return false;
            }
            let Some(parent) = tasks.iter().find(|entry| entry.task.id() == *id) else {
                return false;
            };
            if parent.task.abort_requested()
                || parent.task.is_terminal()
                || parent.task.state() == &TaskState::Running
                || self.blocked_reason(&parent.task).is_some()
            {
                return false;
            }
            owner = parent.task.owner();
        }
        true
    }
}

pub struct Scheduler {
    attachment: Attachment,
    wake_reader: WakeReader,
    registry: Registry,
    validator: Option<crate::runtime::Harness>,
    poisoned: bool,
    failed_migrations: HashMap<Uuid, (Arc<()>, String)>,
    activity: crate::worker_activity::WorkerActivity,
}
impl Scheduler {
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

    /// Attach to an existing owner. Only one task scheduler may be attached to
    /// that owner at a time, including during recovery and handler execution.
    /// The scheduler retains the owner until it is dropped. Callers must fence
    /// all other task executors and direct TaskStore mutations themselves.
    /// Failed attachment or recovery releases the in-process guard.
    pub fn attach(owner: Arc<OwnedStore>, registry: Registry) -> Result<Self, Error> {
        let wake_reader = WakeReader {
            owner: owner.clone(),
            definitions: Arc::new(std::sync::RwLock::new(WakeDefinitions {
                registry: registry.clone(),
                failed_migrations: HashMap::new(),
            })),
        };
        let mut scheduler = Self {
            attachment: Attachment::acquire(owner)?,
            wake_reader,
            registry,
            validator: None,
            poisoned: false,
            failed_migrations: HashMap::new(),
            activity: Default::default(),
        };
        scheduler.reconcile()?;
        Ok(scheduler)
    }
    fn ts(&self) -> TaskStore<'_> {
        TaskStore::new(self.attachment.owner.store())
    }
    pub(crate) fn check(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
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
    /// are retried only when their task definition changes.
    pub fn replace_registry(&mut self, registry: Registry) -> Result<(), Error> {
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
        if matches!(task.state(), TaskState::Completing(_)) {
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
    fn reserve_resolved(&mut self, snapshot: &task_store::StoredTask) -> Result<bool, Error> {
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
        self.poisoned = true;
        let mut batch = self.ts().prepare_save(&[Save::Update(&next)])?;
        batch.checks.extend(child_checks);
        match self.ts().store().commit(&batch.checks, &batch.mutations) {
            Ok(()) => (),
            Err(durable_store::Error::Conflict { .. }) => {
                // No reservation or effects committed; rescan on a CAS miss.
                self.poisoned = false;
                return Ok(false);
            }
            Err(error) => return Err(task_store::Error::from(error).into()),
        }
        Ok(true)
    }
    fn reconcile(&mut self) -> Result<(), Error> {
        for listed in self.ts().list()? {
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
            if !item.task.replay_safe() {
                self.finish_failed(
                    &item,
                    "interrupted: execution outcome requires reconciliation".into(),
                )?;
                continue;
            }
            let Ok(reg) = self.registered(&item.task) else {
                // Leave a task blocked until its definition is restored.
                continue;
            };
            if item.task.replay_safe() && reg.replay_safe {
                let exhausted = item.task.restarts().saturating_add(1) >= item.task.restart_limit();
                if exhausted {
                    self.finish_failed(&item, "restart limit".into())?;
                } else {
                    self.ts().update(&item, |task| task.recover())?;
                }
            } else {
                self.finish_failed(
                    &item,
                    "interrupted: execution outcome requires reconciliation".into(),
                )?;
            }
        }
        Ok(())
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
        self.check()?;
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
            if matches!(snapshot.task.state(), TaskState::Completing(_))
                && !has_live_owned(owner.store(), snapshot.task.id(), &held_tasks)?
            {
                let mut next = snapshot.clone();
                next.task.finalize_success().map_err(Error::Transition)?;
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
                    Ok(()) | Err(durable_store::Error::Conflict { .. }) => return Ok(true),
                    Err(error) => return Err(task_store::Error::from(error).into()),
                }
            }
        }
        for snapshot in self.ts().list()? {
            if !snapshot.task.abort_requested()
                && let TaskState::Waiting(WaitFor::TasksFailFast(ids)) = snapshot.task.state()
            {
                self.poisoned = true;
                self.cancel_failed_siblings(&snapshot, ids)?;
                self.poisoned = false;
            }
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
        // Children settle before their owner, including when definitions are missing.
        let mut abortable = None;
        for snapshot in &tasks {
            if !snapshot.task.abort_requested()
                || snapshot.task.state() != &TaskState::Ready
                || tasks.iter().any(|child| {
                    child.task.owner() == &TaskOwner::Task(snapshot.task.id())
                        && !child.task.is_terminal()
                        && !child.task.is_background()
                        && !cleanup_graph
                            .as_ref()
                            .unwrap()
                            .cyclic_task_edge(snapshot.task.id(), child.task.id())
                })
            {
                continue;
            }
            // The persisted abort mark is cleared only after conversation cleanup
            // and its foreground task descendants have settled. Read under the
            // task gate so no new owned work can race this decision.
            let mut pending = false;
            for edge in crate::ownership_edge::EdgeStore::new(owner.store())
                .children(snapshot.task.id())
                .map_err(|e| Error::Handler(format!("ownership edges: {e}")))?
            {
                if edge.edge.mode != crate::ownership_edge::Mode::Foreground {
                    continue;
                }
                let key = format!("runtime/conversation/{}", edge.edge.child);
                let entry = owner
                    .store()
                    .get(key.as_bytes())
                    .map_err(task_store::Error::from)?
                    .ok_or(Error::Ownership)?;
                let child: crate::runtime::Conversation =
                    serde_json::from_slice(&entry.value).map_err(task_store::Error::from)?;
                if child.id != edge.edge.child || edge.edge.task_owner != *snapshot.task.owner() {
                    return Err(Error::Ownership);
                }
                if child.abort_requested
                    && !cleanup_graph
                        .as_ref()
                        .unwrap()
                        .cyclic_conversation_edge(snapshot.task.id(), child.id)
                {
                    pending = true;
                    break;
                }
            }
            if !pending {
                abortable = Some(snapshot);
                break;
            }
        }
        if let Some(snapshot) = abortable {
            self.poisoned = true;
            if snapshot.task.restarts() > 0
                && snapshot.task.restarts() >= snapshot.task.restart_limit()
            {
                self.ts()
                    .update(snapshot, |task| task.fail_abort_restart())?;
                self.poisoned = false;
                return Ok(true);
            }
            if let Some(reason) = self.blocked_reason(&snapshot.task) {
                let diagnostic = orphan_diagnostic(&reason);
                self.ts().update(snapshot, |t| {
                    t.start()?;
                    t.fail(diagnostic)
                })?;
                self.poisoned = false;
                return Ok(true);
            }
            let abort = self
                .registered(&snapshot.task)
                .ok()
                .and_then(|r| r.abort.clone());
            if let Some(abort) = abort {
                if !self.reserve_resolved(snapshot)? {
                    let reason = self
                        .blocked_reason(&snapshot.task)
                        .ok_or(Error::Registration)?;
                    let diagnostic = orphan_diagnostic(&reason);
                    self.ts().update(snapshot, |t| {
                        t.start()?;
                        t.fail(diagnostic)
                    })?;
                    self.poisoned = false;
                    return Ok(true);
                }
                let _activity = self
                    .activity
                    .enter(crate::worker_activity::WorkerKind::Task, snapshot.task.id());
                let running = self
                    .ts()
                    .load(snapshot.task.id())?
                    .ok_or(Error::Ownership)?;
                drop(gate);
                let result = abort.run(&running.task);
                let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
                match result.map_err(Error::Handler)? {
                    Step::Succeed(_) => self.ts().update(&running, |t| t.cancel())?,
                    Step::Fail(reason) => self.ts().update(&running, |t| t.fail(reason))?,
                    _ => {
                        return Err(Error::Handler(
                            "abort handler must return Succeed or Fail".into(),
                        ));
                    }
                }
            } else {
                self.ts().update(snapshot, |t| {
                    t.start()?;
                    t.fail("orphaned: missing abort handler or task definition".into())
                })?;
            }
            self.poisoned = false;
            return Ok(true);
        }
        let Some(snapshot) = tasks
            .iter()
            .find(|s| {
                s.task.state() == &TaskState::Ready
                    && !s.task.abort_requested()
                    && self.blocked_reason(&s.task).is_none()
                    && self.ancestors_available(&s.task, &tasks)
            })
            .cloned()
        else {
            return Ok(progress);
        };
        let handler = self.registered(&snapshot.task)?.handler.clone();
        if !self.reserve_resolved(&snapshot)? {
            return Ok(true);
        }
        let _activity = self
            .activity
            .enter(crate::worker_activity::WorkerKind::Task, snapshot.task.id());
        let running = self
            .ts()
            .load(snapshot.task.id())?
            .ok_or(Error::Ownership)?;
        drop(gate);
        let mut documents = crate::document::TaskDocuments::new(owner.store(), &running.task)?;
        let result = handler.run_with_documents(&running.task, &mut documents);
        let _gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        // An abort can commit while the synchronous handler is running. Its
        // output (including proposed children) must never cross that fence.
        let current = self.ts().load(running.task.id())?.ok_or(Error::Ownership)?;
        if current.version != running.version && current.task.abort_requested() {
            self.ts().update(&current, |task| task.yield_ready())?;
            self.poisoned = false;
            return Ok(true);
        }
        let step = result.map_err(Error::Handler)?;
        if let Step::Fail(reason) = step {
            // Failure discards all staged effects, including child admissions.
            self.finish_failed_completed(&running, reason)?;
            self.poisoned = false;
            return Ok(true);
        }
        let staged = std::mem::take(&mut documents.keyed_children);
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
        let success_graph_version = owner
            .store()
            .put_version()
            .map_err(task_store::Error::from)?;
        let mut next = running.clone();
        let mut children = Vec::new();
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
                    let mut seen = std::collections::HashSet::new();
                    for id in waiting {
                        if !seen.insert(*id) {
                            return Err(Error::Ownership);
                        }
                        if !children.iter().any(|c| c.id() == *id)
                            && !staged.iter().any(|(_, _, p)| p.child == *id)
                        {
                            let child = self.ts().load(*id)?.ok_or(Error::Ownership)?;
                            if child.task.owner() != &TaskOwner::Task(next.task.id()) {
                                return Err(Error::Ownership);
                            }
                        }
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
        let mut transaction = crate::document::Transaction::new();
        let mut document_batch = match documents.into_batch() {
            Ok(batch) => batch,
            Err(crate::document::Error::Store(durable_store::Error::Conflict { .. })) => {
                // A staged runtime checkpoint observed a newer root while
                // preparing; no effects committed. Follow normal replay policy.
                if running.task.replay_safe() && self.registered(&running.task)?.replay_safe {
                    self.ts().update(&current, |task| task.yield_ready())?;
                } else {
                    self.finish_failed(
                        &current,
                        "document conflict: execution outcome requires reconciliation".into(),
                    )?;
                }
                self.poisoned = false;
                return Ok(true);
            }
            Err(error) => return Err(error.into()),
        };
        let mut document_keys: HashSet<_> = document_batch
            .checks
            .iter()
            .map(|c| c.key.clone())
            .collect();
        // One graph batch: the parent transition replaces admission's parent bump.
        // Both use the captured version, so aborts and competing graph writers
        // fence the whole transaction, without a self-conflict.
        let mut graph = self.ts().prepare_save(&batch)?;
        let parent_key = task_store::key(running.task.id());
        for (_, _, prepared) in staged {
            for check in prepared.checks {
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
        crate::document::reconcile_task_checks(&mut document_batch, &mut graph);
        transaction.append(document_batch)?;
        transaction.append(graph)?;
        let success = matches!(
            next.task.state(),
            TaskState::Succeeded(_) | TaskState::Completing(_)
        );
        let committed = if success {
            transaction.commit_task_graph(owner.store(), success_graph_version)
        } else {
            transaction.commit(owner.store())
        };
        match committed {
            Ok(()) => {}
            Err(crate::document::Error::Store(durable_store::Error::Conflict { key })) => {
                let current = self.ts().load(running.task.id())?.ok_or(Error::Ownership)?;
                if current.version != running.version {
                    if current.task.abort_requested() {
                        self.ts().update(&current, |task| task.yield_ready())?;
                    } else {
                        return Err(crate::document::Error::Store(
                            durable_store::Error::Conflict { key },
                        )
                        .into());
                    }
                } else if document_keys.contains(&key)
                    || key == task_store::PREFIX
                    || key == crate::ownership_edge::PREFIX
                    || key == b"runtime/conversation/"
                {
                    // The checked batch definitely did not commit. Only registered
                    // replay-safe invocations may be run again after external effects.
                    if running.task.replay_safe() && self.registered(&running.task)?.replay_safe {
                        self.ts().update(&current, |task| task.yield_ready())?;
                    } else {
                        self.finish_failed(
                            &current,
                            "document conflict: execution outcome requires reconciliation".into(),
                        )?;
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
        self.poisoned = false;
        Ok(true)
    }
    /// A child must not execute while an interrupted or incompatible ancestor is
    /// still awaiting recovery; such an ancestor can later cancel the subtree.
    fn ancestors_available(&self, task: &Task, tasks: &[task_store::StoredTask]) -> bool {
        let mut owner = task.owner();
        let mut visited = HashSet::new();
        while let TaskOwner::Task(id) = owner {
            if !visited.insert(*id) {
                return false;
            }
            let Some(parent) = tasks.iter().find(|entry| entry.task.id() == *id) else {
                return false;
            };
            if parent.task.abort_requested()
                || parent.task.is_terminal()
                || parent.task.state() == &TaskState::Running
                || self.blocked_reason(&parent.task).is_some()
            {
                return false;
            }
            owner = parent.task.owner();
        }
        true
    }
    /// Persist abort intent for the live subtree. The synchronous scheduler can
    /// accept this only between invocations; subsequent ticks drain children first.
    pub fn request_abort(&mut self, id: Uuid) -> Result<(), Error> {
        self.check()?;
        let result = request_abort_subtree(&self.ts(), id);
        if matches!(result, Err(Error::Store(_))) {
            self.poisoned = true;
        }
        result
    }

    /// Cancel a task and all of its owned descendants in one transaction.
    /// This requires exclusive scheduler ownership and cannot interrupt `tick`.
    pub fn cancel(&mut self, id: Uuid) -> Result<(), Error> {
        self.check()?;
        let records = self.ts().list()?;
        let root = records
            .iter()
            .find(|s| s.task.id() == id)
            .ok_or(Error::Ownership)?;
        if root.task.is_terminal() {
            return Err(Error::Transition(TransitionError::InvalidState));
        }
        self.poisoned = true;
        let result = self.cancel_roots(&records, &[id], None);
        if result.is_ok() {
            self.poisoned = false;
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
        // The immediate-cancel API cannot safely settle a held outcome or
        // its owner. Use request_abort to drain this subtree instead.
        if records.iter().any(|record| {
            ids.contains(&record.task.id())
                && matches!(record.task.state(), TaskState::Completing(_))
        }) {
            return Err(Error::LiveChildren);
        }
        if crate::ownership_edge::reject_foreground_children(
            self.attachment.owner.store(),
            records
                .iter()
                .filter(|record| ids.contains(&record.task.id()) && !record.task.is_terminal())
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

    fn finish_failed_completed(
        &self,
        parent: &task_store::StoredTask,
        reason: String,
    ) -> Result<(), Error> {
        self.finish_failed_inner(parent, reason, true)
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
        let store = self.ts();
        let graph_version = store
            .store()
            .put_version()
            .map_err(task_store::Error::from)?;
        let records = store.list()?;
        // Failure holding and its cancellation cascade are not implemented yet.
        // Leave the running record intact instead of publishing a premature end.
        if has_live_owned(store.store(), parent.task.id(), &records)? {
            return Err(Error::LiveChildren);
        }
        let mut next = parent.clone();
        next.task.fail(reason).map_err(Error::Transition)?;
        if completed {
            next.task
                .record_completed_attempt(&parent.task)
                .map_err(Error::Transition)?;
        }
        let batch = store.prepare_save(&[Save::Update(&next)])?;
        store
            .store()
            .commit_with_prefix_put_fence(
                &batch.checks,
                &batch.mutations,
                &[
                    task_store::PREFIX,
                    crate::ownership_edge::PREFIX,
                    b"runtime/conversation/",
                ],
                graph_version,
            )
            .map_err(task_store::Error::from)?;
        Ok(())
    }

    fn cancel_failed_siblings(
        &self,
        parent: &task_store::StoredTask,
        ids: &[Uuid],
    ) -> Result<(), Error> {
        let records = self.ts().list()?;
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
            failed |= matches!(child.task.state(), TaskState::Failed(_));
        }
        if failed {
            self.cancel_roots(&records, ids, None)?;
        }
        Ok(())
    }
}

// Walk through terminal tasks and owned conversations as well: work may be
// admitted in an owned conversation after its original task has finished.
fn has_live_owned(
    store: &durable_store::Store,
    root: Uuid,
    tasks: &[task_store::StoredTask],
) -> Result<bool, Error> {
    let edges = crate::ownership_edge::EdgeStore::new(store);
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
        for edge in edges
            .children(id)
            .map_err(|e| Error::Handler(format!("ownership edges: {e}")))?
        {
            let parent = tasks
                .iter()
                .find(|item| item.task.id() == id)
                .ok_or(Error::Ownership)?;
            if edge.edge.task_owner != *parent.task.owner() {
                return Err(Error::Ownership);
            }
            if edge.edge.mode != crate::ownership_edge::Mode::Foreground {
                continue;
            }
            // An aborting conversation is still being drained by the harness.
            let key = format!("runtime/conversation/{}", edge.edge.child);
            let record = store
                .get(key.as_bytes())
                .map_err(task_store::Error::from)?
                .ok_or(Error::Ownership)?;
            let conversation: crate::runtime::Conversation =
                serde_json::from_slice(&record.value).map_err(task_store::Error::from)?;
            if conversation.id != edge.edge.child {
                return Err(Error::Ownership);
            }
            if conversation.abort_requested {
                return Ok(true);
            }
            for child in tasks.iter().filter(|item| {
                item.task.owner() == &TaskOwner::Conversation(edge.edge.child)
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
    mut before_commit: impl FnMut(),
) -> Result<(), Error> {
    loop {
        let graph_version = ts.store().put_version().map_err(task_store::Error::from)?;
        let records = ts.list()?;
        let root = records
            .iter()
            .find(|s| s.task.id() == id)
            .ok_or(Error::Ownership)?;
        if root.task.is_terminal() {
            return Err(Error::Transition(TransitionError::InvalidState));
        }
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
            ts.store(),
            records
                .iter()
                .filter(|record| ids.contains(&record.task.id()) && !record.task.is_terminal())
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
mod tests {
    use super::*;
    use serde_json::json;
    fn cp(phase: &str) -> Checkpoint {
        Checkpoint {
            phase: phase.into(),
            data: json!(null),
        }
    }
    fn task(id: u128, owner: TaskOwner, kind: &str, safe: bool, limit: u32) -> Task {
        Task::new(Uuid::from_u128(id), owner, cp("start"), limit)
            .unwrap()
            .registered(kind.into(), safe)
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
                assert!(matches!(
                    Scheduler::open(&path, registry),
                    Err(Error::LiveChildren)
                ));
                let db = durable_store::Store::open(&path).unwrap();
                let tasks = TaskStore::new(&db);
                assert_eq!(
                    tasks.load(parent.id()).unwrap().unwrap().task.state(),
                    &TaskState::Running
                );
                assert_eq!(
                    tasks.load(child.id()).unwrap().unwrap().task.state(),
                    &TaskState::Ready
                );
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
            assert!(scheduler.reserve_resolved(&ready).unwrap());
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
            r.register("c", false, |_t: &Task| {
                Ok(Step::Wait {
                    checkpoint: cp("hold"),
                    condition: WaitFor::Signal("hold".into()),
                    children: vec![],
                })
            });
            let mut s = Scheduler::open(&path, r).unwrap();
            s.tick(0).unwrap();
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
    fn interrupted_parent_with_live_child_fails_closed_without_replay() {
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
        assert!(matches!(
            Scheduler::open(&path, r),
            Err(Error::LiveChildren)
        ));
        let db = durable_store::Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        assert_eq!(
            tasks.load(parent.id()).unwrap().unwrap().task.state(),
            &TaskState::Running
        );
        assert_eq!(
            tasks.load(child.id()).unwrap().unwrap().task.state(),
            &TaskState::Running
        );
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
                assert!(matches!(second, Err(Error::LiveChildren)));
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
            assert!(s.tick(0).is_err());
            assert!(matches!(s.tick(0), Err(Error::Poisoned)));
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
