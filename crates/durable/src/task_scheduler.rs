//! Explicitly driven scheduler for generic tasks, separate from the conversation runtime.
//!
//! The scheduler holds the store's exclusive cooperative owner lock. `tick` runs
//! at most one invocation synchronously; callers periodically call it again,
//! including after signals and deadlines. Dropping between ticks is a clean stop.
//! Interrupted invocations replay only when both recorded and current policies
//! allow it. External effects cannot be made atomic with this
//! store: handlers must be pure or use destination-side idempotency/reconciliation.
//! A storage error or panic during an invocation requires dropping this scheduler
//! and reopening it; never assume an unsuccessful commit did not happen.
//!
//! This scheduler supports direct-child all, any and fail-fast waits. It has no
//! background tasks, combined waits, or conversation integration.
//! Cancellation changes durable state only and cannot undo external effects or
//! interrupt a synchronous handler. Direct TaskStore writers must be fenced.
//! Terminal success still requires all owned work to be finished.

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

struct Registration {
    replay_safe: bool,
    handler: Arc<dyn Handler>,
    abort: Option<Arc<dyn Handler>>,
}
#[derive(Default)]
pub struct Registry {
    entries: HashMap<String, Registration>,
}
impl Registry {
    pub fn register_shared(
        &mut self,
        kind: impl Into<String>,
        replay_safe: bool,
        handler: Arc<dyn Handler>,
    ) -> bool {
        use std::collections::hash_map::Entry;
        match self.entries.entry(kind.into()) {
            Entry::Vacant(v) => {
                v.insert(Registration {
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
                    replay_safe,
                    handler: Arc::new(handler),
                    abort: None,
                });
                true
            }
            Entry::Occupied(_) => false,
        }
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

pub struct Scheduler {
    attachment: Attachment,
    registry: Registry,
    poisoned: bool,
}
impl Scheduler {
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
    pub(crate) fn policies(&self) -> HashMap<String, bool> {
        self.registry
            .entries
            .iter()
            .map(|(name, registration)| (name.clone(), registration.replay_safe))
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
        let mut scheduler = Self {
            attachment: Attachment::acquire(owner)?,
            registry,
            poisoned: false,
        };
        scheduler.reconcile()?;
        Ok(scheduler)
    }
    fn ts(&self) -> TaskStore<'_> {
        TaskStore::new(self.attachment.owner.store())
    }
    fn check(&self) -> Result<(), Error> {
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
    fn reconcile(&mut self) -> Result<(), Error> {
        for listed in self.ts().list()? {
            let Some(item) = self.ts().load(listed.task.id())? else {
                continue;
            };
            if item.task.state() != &TaskState::Running {
                continue;
            }
            if item.task.abort_requested() {
                if item.task.restarts().saturating_add(1) >= item.task.restart_limit() {
                    self.ts()
                        .update(&item, |task| task.fail("abort restart limit".into()))?;
                } else {
                    self.ts().update(&item, |task| task.recover())?;
                }
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
        if reg.replay_safe != task.replay_safe()
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
        self.ts().send_signal(signal)?;
        Ok(())
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
    pub fn tick(&mut self, now_millis: i64) -> Result<bool, Error> {
        self.check()?;
        let owner = self.owner();
        let gate = owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        let mut progress = false;
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
        // Children settle before their owner, including when definitions are missing.
        if let Some(snapshot) = tasks.iter().find(|s| {
            s.task.abort_requested()
                && s.task.state() == &TaskState::Ready
                && !tasks.iter().any(|child| {
                    child.task.owner() == &TaskOwner::Task(s.task.id()) && !child.task.is_terminal()
                })
        }) {
            self.poisoned = true;
            let abort = self
                .registered(&snapshot.task)
                .ok()
                .and_then(|r| r.abort.clone());
            if let Some(abort) = abort {
                self.ts().reserve(snapshot)?;
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
                    && self.registered(&s.task).is_ok()
                    && self.ancestors_available(&s.task, &tasks)
            })
            .cloned()
        else {
            return Ok(progress);
        };
        let handler = self.registered(&snapshot.task)?.handler.clone();
        self.poisoned = true;
        self.ts().reserve(&snapshot)?;
        let running = self
            .ts()
            .load(snapshot.task.id())?
            .ok_or(Error::Ownership)?;
        drop(gate);
        let result = handler.run(&running.task);
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
                    if reg.replay_safe != child.replay_safe() {
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
                        if !children.iter().any(|c| c.id() == *id) {
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
                self.ensure_no_live_children(next.task.id())?;
                next.task.succeed(value).map_err(Error::Transition)?;
            }
            Step::Fail(reason) => {
                self.finish_failed(&running, reason)?;
                self.poisoned = false;
                return Ok(true);
            }
        }
        let mut batch = vec![Save::Update(&next)];
        batch.extend(children.iter().map(Save::Create));
        match self.ts().save(&batch) {
            Ok(()) => {}
            Err(task_store::Error::Store(durable_store::Error::Conflict { .. })) => {
                let current = self.ts().load(running.task.id())?.ok_or(Error::Ownership)?;
                if !current.task.abort_requested() {
                    return Err(Error::Ownership);
                }
                self.ts().update(&current, |task| task.yield_ready())?;
            }
            Err(error) => return Err(error.into()),
        }
        self.poisoned = false;
        Ok(true)
    }
    /// A child must not execute while an interrupted or missing ancestor is
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
                || self.registered(&parent.task).is_err()
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

    fn finish_failed(&self, parent: &task_store::StoredTask, reason: String) -> Result<(), Error> {
        let records = self.ts().list()?;
        let roots: Vec<_> = records
            .iter()
            .filter(|s| s.task.owner() == &TaskOwner::Task(parent.task.id()))
            .map(|s| s.task.id())
            .collect();
        let mut next = parent.clone();
        next.task.fail(reason).map_err(Error::Transition)?;
        self.cancel_roots(&records, &roots, Some(next))
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

    fn ensure_no_live_children(&self, id: Uuid) -> Result<(), Error> {
        if self
            .ts()
            .list()?
            .iter()
            .any(|s| s.task.owner() == &TaskOwner::Task(id) && !s.task.is_terminal())
        {
            Err(Error::LiveChildren)
        } else {
            Ok(())
        }
    }
}

/// Mark the entire existing subtree atomically. Checking every live record also
/// fences a concurrent handler trying to publish a child under that record.
pub(crate) fn request_abort_subtree(ts: &TaskStore<'_>, id: Uuid) -> Result<(), Error> {
    loop {
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
        match ts.save(&updates.iter().map(Save::Update).collect::<Vec<_>>()) {
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
            let mut scheduler = Scheduler::open(&path, registry).unwrap();
            assert!(!scheduler.tick(0).unwrap());
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
            let child_state = scheduler.get(child.id()).unwrap().unwrap();
            assert_eq!(
                child_state.state(),
                if safe {
                    &TaskState::Ready
                } else {
                    &TaskState::Cancelled
                }
            );
        }
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
    fn interrupted_parent_cancels_running_child_without_replay() {
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
        let s = Scheduler::open(&path, r).unwrap();
        assert!(matches!(
            s.get(parent.id()).unwrap().unwrap().state(),
            TaskState::Failed(_)
        ));
        assert_eq!(
            s.get(child.id()).unwrap().unwrap().state(),
            &TaskState::Cancelled
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
}
