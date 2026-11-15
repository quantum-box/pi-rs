//! Background driver for the generic task scheduler sharing a Harness owner.
//! The host registers handlers before starting the driver. A handler must be
//! bounded or cooperative: shutdown waits for an in-progress synchronous step.
use crate::{
    task::{Task, TaskOwner, TaskState},
    task_scheduler,
    task_store::{self, Save, Signal, TaskStore},
};
use durable_store::owner::OwnedStore;
use std::{
    collections::HashMap,
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Task(#[from] task_scheduler::Error),
    #[error("task driver lock poisoned")]
    Poisoned,
    #[error("task driver stopped: {0}")]
    Stopped(String),
    #[error("task {0} does not exist")]
    UnknownTask(Uuid),
    #[error("task wait cancelled")]
    WaitCancelled,
    #[error("conversation runtime: {0}")]
    Runtime(#[from] crate::runtime::Error),
}
pub(crate) struct Shared {
    scheduler: Mutex<task_scheduler::Scheduler>,
    activity: crate::worker_activity::WorkerActivity,
    owner: Arc<OwnedStore>,
    state: Mutex<State>,
    wake: Condvar,
    harness: Option<Mutex<crate::runtime::Harness>>,
    cleanup_stop: pi_rs_core::CancellationToken,
    notifications: Mutex<Option<SyncSender<()>>>,
    subscription: Mutex<Option<Receiver<()>>>,
}

#[allow(dead_code)] // Private handoff foundation; no tool dispatch is connected.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuiescentError {
    #[error("scheduler is busy")]
    Busy,
    #[error("task driver has stopped")]
    Stopped,
    #[error("scheduler or driver lock poisoned")]
    Poisoned,
    #[error(transparent)]
    Scheduler(#[from] task_scheduler::Error),
    #[error(transparent)]
    Lease(#[from] crate::executor_handoff::Error),
}
#[derive(Default)]
struct State {
    stop: bool,
    failure: Option<String>,
    policies: HashMap<String, (bool, u32)>,
    definitions: HashMap<String, (bool, Arc<dyn task_scheduler::Handler>)>,
}
/// Drop asks the thread to stop; `shutdown` additionally waits for it. The
/// thread retains storage ownership until its current handler returns.
pub struct Driver {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
    cleanup: Option<JoinHandle<()>>,
}
impl Driver {
    pub fn start(scheduler: task_scheduler::Scheduler) -> Self {
        Self::start_inner(scheduler, None)
    }
    pub(crate) fn start_with_harness(
        scheduler: task_scheduler::Scheduler,
        harness: crate::runtime::Harness,
    ) -> Self {
        Self::start_inner(scheduler, Some(harness))
    }
    fn start_inner(
        scheduler: task_scheduler::Scheduler,
        harness: Option<crate::runtime::Harness>,
    ) -> Self {
        let owner = scheduler.owner();
        let activity = scheduler.worker_activity();
        let policies = scheduler.policies();
        let definitions = scheduler.definitions();
        let (notify_tx, notify_rx) = mpsc::sync_channel(1);
        // Seed the subscription so recovery before subscription cannot be missed.
        let _ = notify_tx.try_send(());
        let shared = Arc::new(Shared {
            scheduler: Mutex::new(scheduler),
            activity,
            owner,
            state: Mutex::new(State {
                policies,
                definitions,
                ..State::default()
            }),
            wake: Condvar::new(),
            harness: harness.map(Mutex::new),
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            notifications: Mutex::new(Some(notify_tx)),
            subscription: Mutex::new(Some(notify_rx)),
        });
        let task_shared = shared.clone();
        let worker = thread::spawn(move || {
            // Close the host channel even if a handler panics.
            struct CloseOnExit(Arc<Shared>);
            impl Drop for CloseOnExit {
                fn drop(&mut self) {
                    self.0.close_notifications();
                }
            }
            let _close = CloseOnExit(task_shared.clone());
            loop {
                if task_shared.state.lock().map_or(true, |s| s.stop) {
                    break;
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64;
                let result = task_shared.tick_and_record_failure(now);
                match result {
                    Ok(true) => {
                        task_shared.notify_host();
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        if let Ok(mut state) = task_shared.state.lock() {
                            state.failure = Some(error);
                            state.stop = true;
                        }
                        task_shared.close_notifications();
                        break;
                    }
                }
                let Ok(state) = task_shared.state.lock() else {
                    break;
                };
                if state.stop {
                    break;
                }
                let _ = task_shared
                    .wake
                    .wait_timeout(state, Duration::from_millis(100));
            }
        });
        let cleanup = shared.harness.as_ref().map(|_| {
            let shared = shared.clone();
            thread::spawn(move || cleanup_loop(shared))
        });
        Self {
            shared,
            worker: Some(worker),
            cleanup,
        }
    }
    /// Take the driver's single, capacity-one work-change subscription. The first
    /// hint is immediate, including when work was recovered before subscription.
    /// Hints coalesce and may be spurious; inspect durable state after receiving.
    /// The channel closes on shutdown or driver failure. Taking it is one-shot.
    pub fn subscribe_wake(&self) -> Result<Option<Receiver<()>>, Error> {
        let _state = self.check()?;
        Ok(self
            .shared
            .subscription
            .lock()
            .map_err(|_| Error::Poisoned)?
            .take())
    }
    /// Observe live task and, when attached, conversation executors.
    pub fn worker_activity(&self) -> crate::worker_activity::WorkerActivity {
        self.shared.activity.clone()
    }
    pub(crate) fn weak(&self) -> std::sync::Weak<Shared> {
        Arc::downgrade(&self.shared)
    }
    fn check(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        let state = self.shared.state.lock().map_err(|_| Error::Poisoned)?;
        if state.stop {
            return Err(Error::Stopped(
                state.failure.clone().unwrap_or_else(|| "shutdown".into()),
            ));
        }
        Ok(state)
    }
    pub fn submit(&self, task: &Task) -> Result<(), Error> {
        let state = self.check()?;
        let registered = task
            .kind()
            .and_then(|kind| state.policies.get(kind))
            .copied();
        if registered != Some((task.replay_safe(), task.definition_version()))
            || !matches!(task.owner(), TaskOwner::Conversation(_))
            || task.state() != &TaskState::Ready
        {
            return Err(task_scheduler::Error::Ownership.into());
        }
        let _gate = self
            .shared
            .owner
            .task_gate()
            .lock()
            .map_err(|_| Error::Poisoned)?;
        if let TaskOwner::Conversation(id) = task.owner() {
            let key = format!("runtime/conversation/{id}");
            let entry = self
                .shared
                .owner
                .store()
                .get(key.as_bytes())
                .map_err(|e| task_scheduler::Error::Store(e.into()))?
                .ok_or(task_scheduler::Error::Ownership)?;
            let value: serde_json::Value = serde_json::from_slice(&entry.value)
                .map_err(|e| task_scheduler::Error::Store(e.into()))?;
            if value
                .get("abort_requested")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                return Err(task_scheduler::Error::Ownership.into());
            }
        }
        TaskStore::new(self.shared.owner.store())
            .save(&[Save::Create(task)])
            .map_err(task_scheduler::Error::from)?;
        self.shared.notify_work();
        Ok(())
    }
    /// Replace the complete registry at a scheduling boundary. This waits for
    /// an in-flight handler and wakes the worker to reconsider blocked tasks.
    pub fn replace_registry(&self, registry: task_scheduler::Registry) -> Result<(), Error> {
        let validator = self
            .shared
            .harness
            .as_ref()
            .map(|h| h.lock().map(|h| h.worker()).map_err(|_| Error::Poisoned))
            .transpose()?;
        let mut scheduler = self.shared.scheduler.lock().map_err(|_| Error::Poisoned)?;
        let mut state = self.check()?;
        let _gate = self
            .shared
            .owner
            .task_gate()
            .lock()
            .map_err(|_| Error::Poisoned)?;
        if let Some(harness) = validator {
            harness.validate_task_tool_registry(&registry)?;
        }
        let result = scheduler.replace_registry_locked(registry);
        state.policies = scheduler.policies();
        state.definitions = scheduler.definitions();
        if let Err(error) = result {
            state.failure = Some(error.to_string());
            state.stop = true;
            self.shared.wake.notify_all();
            self.shared.close_notifications();
            return Err(error.into());
        }
        self.shared.notify_work();
        Ok(())
    }

    /// Inspect generic task wake state at a scheduling boundary using the supplied
    /// Unix-millisecond clock. This may wait for an in-flight handler to finish.
    pub fn wake_snapshot(&self, now_millis: i64) -> Result<task_scheduler::WakeSnapshot, Error> {
        let scheduler = self.shared.scheduler.lock().map_err(|_| Error::Poisoned)?;
        let _state = self.check()?;
        Ok(scheduler.wake_snapshot(now_millis)?)
    }

    /// Snapshot nonterminal tasks currently blocked by their definitions.
    pub fn blocked_tasks(&self) -> Result<Vec<task_scheduler::BlockedTask>, Error> {
        let scheduler = self.shared.scheduler.lock().map_err(|_| Error::Poisoned)?;
        let _state = self.check()?;
        Ok(scheduler.blocked_tasks()?)
    }

    /// Persist the abort without waiting for a running handler. The handler's
    /// output is discarded when it returns; cleanup then runs on the worker.
    pub fn request_abort(&self, id: Uuid) -> Result<(), Error> {
        self.request_abort_after_check(id, || {})
    }

    fn request_abort_after_check(&self, id: Uuid, after_check: impl FnOnce()) -> Result<(), Error> {
        // A conversation worker can consult driver state while holding its
        // execution gate. Do not hold driver state while fencing those gates.
        drop(self.check()?);
        after_check();
        if let Some(harness) = &self.shared.harness {
            let mut harness = harness.lock().map_err(|_| Error::Poisoned)?;
            self.shared
                .owner
                .conversation_controls()
                .fence(|| harness.abort_owned_graph(None, Some(id)))
                .map_err(|e| match e {
                    durable_store::controls::FenceError::Poisoned => Error::Poisoned,
                    durable_store::controls::FenceError::Commit(e) => Error::Runtime(e),
                })?;
        } else {
            let _gate = self
                .shared
                .owner
                .task_gate()
                .lock()
                .map_err(|_| Error::Poisoned)?;
            task_scheduler::request_abort_subtree(&TaskStore::new(self.shared.owner.store()), id)?;
        }
        self.shared.notify_work();
        Ok(())
    }
    pub fn signal(&self, signal: &Signal) -> Result<(), Error> {
        let _state = self.check()?;
        let _gate = self
            .shared
            .owner
            .task_gate()
            .lock()
            .map_err(|_| Error::Poisoned)?;
        TaskStore::new(self.shared.owner.store())
            .send_signal(signal)
            .map_err(task_scheduler::Error::from)?;
        self.shared.notify_work();
        Ok(())
    }
    /// Deliver a typed signal and wake the background scheduler when accepted.
    pub fn deliver_signal(
        &self,
        task_id: Uuid,
        token: &task_store::SignalToken,
        key: &str,
        value: serde_json::Value,
    ) -> Result<task_store::SignalResult, Error> {
        let _state = self.check()?;
        let _gate = self
            .shared
            .owner
            .task_gate()
            .lock()
            .map_err(|_| Error::Poisoned)?;
        let result = TaskStore::new(self.shared.owner.store())
            .deliver_signal(task_id, token, key, value)
            .map_err(task_scheduler::Error::from)?;
        if result == task_store::SignalResult::Delivered {
            self.shared.notify_work();
        }
        Ok(result)
    }
    pub fn get(&self, id: Uuid) -> Result<Option<Task>, Error> {
        let _state = self.check()?;
        Ok(TaskStore::new(self.shared.owner.store())
            .load(id)
            .map_err(task_scheduler::Error::from)?
            .map(|snapshot| snapshot.task))
    }
    /// Wait for a persisted terminal task. Cancelling or dropping this future
    /// stops only the wait, not the task. The driver must remain running.
    /// Live records are polled at most every 100 ms, including after reopen.
    pub async fn wait_for_task(
        &self,
        id: Uuid,
        cancel: &pi_rs_core::CancellationToken,
    ) -> Result<Task, Error> {
        loop {
            if cancel.is_cancelled() {
                return Err(Error::WaitCancelled);
            }
            let task = self.get(id)?.ok_or(Error::UnknownTask(id))?;
            if task.is_terminal() {
                return Ok(task);
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::WaitCancelled),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.stop_worker();
        let worker_panicked = self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err());
        self.shared.cleanup_stop.cancel();
        let cleanup_panicked = self
            .cleanup
            .take()
            .is_some_and(|worker| worker.join().is_err());
        if worker_panicked || cleanup_panicked {
            return Err(Error::Stopped(format!(
                "worker panicked: {worker_panicked}; cleanup worker panicked: {cleanup_panicked}"
            )));
        }
        let state = self.shared.state.lock().map_err(|_| Error::Poisoned)?;
        if let Some(error) = &state.failure {
            return Err(Error::Stopped(error.clone()));
        }
        Ok(())
    }
    fn stop(&self) {
        self.shared.cleanup_stop.cancel();
        self.stop_worker();
    }
    fn stop_worker(&self) {
        self.shared.close_notifications();
        if let Ok(mut state) = self.shared.state.lock() {
            state.stop = true;
            self.shared.wake.notify_all();
        }
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        self.stop();
    }
}

// Discover persisted marks on every pass, so shutdown/reopen needs no volatile queue.
fn cleanup_loop(shared: Arc<Shared>) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            fail_cleanup(&shared, error.to_string());
            return;
        }
    };
    runtime.block_on(async {
        let mut active: tokio::task::JoinSet<(Uuid, Result<(), crate::runtime::Error>)> =
            tokio::task::JoinSet::new();
        let mut ids = std::collections::HashSet::new();
        loop {
            if shared.cleanup_stop.is_cancelled() {
                break;
            }
            while let Some(result) = active.try_join_next() {
                match result {
                    Ok((id, Ok(()))) => {
                        ids.remove(&id);
                    }
                    Ok((_, Err(error))) => {
                        fail_cleanup(&shared, error.to_string());
                        return;
                    }
                    Err(error) => {
                        fail_cleanup(&shared, error.to_string());
                        return;
                    }
                }
            }
            let discovered = (|| -> Result<Vec<Uuid>, String> {
                let store = shared.owner.store();
                // Snapshot all three namespaces together. Inspect an edge only
                // for a marked child, so unrelated corrupt legacy edges cannot
                // stop recovery of an otherwise independent conversation.
                let entries: HashMap<_, _> = store
                    .list_prefixes(&[
                        crate::task_store::PREFIX,
                        crate::ownership_edge::PREFIX,
                        b"runtime/conversation/",
                    ])
                    .map_err(|e| e.to_string())?
                    .into_iter()
                    .collect();
                let mut found = Vec::new();
                for (key, entry) in &entries {
                    if !key.starts_with(b"runtime/conversation/") {
                        continue;
                    }
                    let child: crate::runtime::Conversation =
                        serde_json::from_slice(&entry.value).map_err(|e| e.to_string())?;
                    if !child.abort_requested {
                        continue;
                    }
                    if *key != format!("runtime/conversation/{}", child.id).into_bytes() {
                        return Err("conversation key mismatch".into());
                    }
                    let edge_key = crate::ownership_edge::edge_key(child.id);
                    let Some(edge_entry) = entries.get(&edge_key) else {
                        continue;
                    };
                    let edge = crate::ownership_edge::decode(&edge_key, edge_entry.clone())
                        .map_err(|e| e.to_string())?;
                    let task_key = crate::task_store::key(edge.edge.task);
                    let task_entry = entries.get(&task_key).ok_or("ownership task missing")?;
                    let task = crate::task_store::decode(&task_key, task_entry.clone())
                        .map_err(|e| e.to_string())?;
                    if edge.edge.task_owner != *task.task.owner() {
                        return Err("ownership edge owner mismatch".into());
                    }
                    // A child can be aborted independently of its live owner,
                    // and explicit abort can cross background boundaries.
                    found.push(child.id);
                }
                Ok(found)
            })();
            match discovered {
                Ok(found) => {
                    for id in found {
                        if ids.insert(id) {
                            let mut worker = shared
                                .harness
                                .as_ref()
                                .unwrap()
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .worker();
                            let stop = shared.cleanup_stop.clone();
                            active.spawn(async move {
                                (id, worker.drive_owned_cleanup(id, &stop).await)
                            });
                        }
                    }
                }
                Err(error) => {
                    fail_cleanup(&shared, error);
                    return;
                }
            }
            tokio::select! {
                _ = shared.cleanup_stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(100)) => (),
            }
        }
        while active.join_next().await.is_some() {}
    });
}
fn fail_cleanup(shared: &Shared, error: String) {
    if let Ok(mut state) = shared.state.lock() {
        state.failure = Some(error);
        state.stop = true;
        shared.wake.notify_all();
    }
    shared.cleanup_stop.cancel();
    shared.close_notifications();
}

impl Shared {
    // Publish a failed tick before releasing the scheduler mutex. Otherwise a
    // quiescent commit can slip between an unpoisoned tick error and stop.
    fn tick_and_record_failure(&self, now: i64) -> Result<bool, String> {
        let mut scheduler = self
            .scheduler
            .lock()
            .map_err(|_| "task scheduler lock poisoned".to_string())?;
        let result = scheduler.tick(now).map_err(|e| e.to_string());
        if let Err(error) = &result
            && let Ok(mut state) = self.state.lock()
        {
            state.failure = Some(error.clone());
            state.stop = true;
        }
        result
    }

    /// Commit only while the driver owns no executing scheduler handler. The
    /// scheduler mutex is held through the conversation lease's checked commit.
    /// The caller must hold no conversation worker, poll, registry, or task gate;
    /// try-lock only avoids waiting for the scheduler, not the other fences. The
    /// stopped flag is checked at entry; shutdown may race an already-started
    /// commit. This cannot join detached work or authorize external effects.
    #[allow(dead_code)]
    pub(crate) fn try_quiescent_commit(
        &self,
        binding: crate::executor_handoff::Binding,
        batch: task_store::OwnedBatch,
    ) -> Result<(), QuiescentError> {
        let scheduler = self.scheduler.try_lock().map_err(|e| match e {
            std::sync::TryLockError::WouldBlock => QuiescentError::Busy,
            std::sync::TryLockError::Poisoned(_) => QuiescentError::Poisoned,
        })?;
        scheduler.check()?;
        {
            let state = self.state.lock().map_err(|_| QuiescentError::Poisoned)?;
            if state.stop {
                return Err(QuiescentError::Stopped);
            }
        }
        let lease =
            crate::executor_handoff::ConversationLease::acquire(self.owner.clone(), binding)?;
        lease.commit(batch)?;
        drop(lease);
        drop(scheduler);
        Ok(())
    }

    fn notify_host(&self) {
        if let Ok(mut sender) = self.notifications.lock()
            && let Some(tx) = sender.as_ref()
            && matches!(tx.try_send(()), Err(TrySendError::Disconnected(_)))
        {
            *sender = None;
        }
    }
    fn notify_work(&self) {
        self.wake.notify_one();
        self.notify_host();
    }
    fn close_notifications(&self) {
        if let Ok(mut sender) = self.notifications.lock() {
            sender.take();
        }
    }
    pub(crate) fn with_admission<T>(
        &self,
        tasks: &[Task],
        conversation: Uuid,
        f: impl FnOnce() -> Result<T, crate::runtime::Error>,
    ) -> Result<T, crate::runtime::Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        if state.stop
            || tasks.iter().any(|task| {
                task.kind()
                    .and_then(|kind| state.policies.get(kind))
                    .copied()
                    != Some((task.replay_safe(), task.definition_version()))
                    || task.owner() != &TaskOwner::Conversation(conversation)
                    || task.state() != &TaskState::Ready
                    || task.abort_requested()
            })
        {
            return Err(crate::runtime::Error::Invalid(
                "invalid or unregistered admission task".into(),
            ));
        }
        let _gate = self
            .owner
            .task_gate()
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        let store = TaskStore::new(self.owner.store());
        for task in tasks {
            if store
                .load(task.id())
                .map_err(|e| crate::runtime::Error::Invalid(format!("task lookup: {e}")))?
                .is_some()
            {
                return Err(crate::runtime::Error::Invalid(
                    "admission task ID already exists".into(),
                ));
            }
        }
        let result = f();
        if result.is_ok() {
            self.notify_work();
        }
        result
    }
    // Called while the conversation execution gate may be held. Match the
    // admission lock order: state before task gate, never scheduler or a
    // conversation gate while holding state. Replacement holds state while
    // publishing its registry, so validation and commit form one boundary.
    pub(crate) fn check_running_and_wake(&self) -> Result<(), crate::runtime::Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        if state.stop {
            return Err(crate::runtime::Error::Invalid(format!(
                "task driver stopped: {}",
                state.failure.as_deref().unwrap_or("shutdown")
            )));
        }
        self.notify_work();
        Ok(())
    }

    pub(crate) fn with_hook_children<T>(
        &self,
        tasks: &[Task],
        handlers: &[Arc<dyn task_scheduler::Handler>],
        conversation: Uuid,
        f: impl FnOnce() -> Result<T, crate::runtime::Error>,
    ) -> Result<T, crate::runtime::Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        if state.stop
            || tasks.len() != handlers.len()
            || tasks.iter().zip(handlers).any(|(task, handler)| {
                let Some(kind) = task.kind() else { return true };
                let Some((replay, registered)) = state.definitions.get(kind) else {
                    return true;
                };
                !Arc::ptr_eq(handler, registered)
                    || state.policies.get(kind).copied()
                        != Some((task.replay_safe(), task.definition_version()))
                    || *replay != task.replay_safe()
                    || task.owner() != &TaskOwner::Conversation(conversation)
                    || task.state() != &TaskState::Ready
                    || task.abort_requested()
            })
        {
            return Err(crate::runtime::Error::Invalid(
                "task driver registration mismatch".into(),
            ));
        }
        let _gate = self
            .owner
            .task_gate()
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        let result = f();
        if result.is_ok() {
            self.notify_work();
        }
        result
    }
    pub(crate) fn policy_for(
        &self,
        name: &str,
        handler: &Arc<dyn task_scheduler::Handler>,
    ) -> Option<bool> {
        let state = self.state.lock().ok()?;
        if state.stop {
            return None;
        }
        let (policy, registered) = state.definitions.get(name)?;
        if state.policies.get(name)?.1 != 1 {
            return None;
        }
        Arc::ptr_eq(handler, registered).then_some(*policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, WaitFor};
    use serde_json::json;
    use std::sync::mpsc;

    #[test]
    fn quiescent_commit_waits_for_handler_retirement_and_fences_abort_reopen() {
        use crate::executor_handoff::{self, Binding};
        use crate::runtime::{Harness, ToolAttempt};
        use durable_store::{Check, Expected, Mutation};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register("block", true, move |_: &Task| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("done")))
        });
        registry.register("parent", true, |_: &Task| {
            Ok(task_scheduler::Step::Succeed(json!(null)))
        });
        let scheduler = task_scheduler::Scheduler::open(&path, registry.clone()).unwrap();
        let owner = scheduler.owner();
        let b = Binding {
            conversation: Uuid::from_u128(1),
            tool: ToolAttempt {
                operation_id: Uuid::from_u128(2),
                attempt_id: Uuid::from_u128(3),
            },
            task: Uuid::from_u128(4),
            task_attempt: Uuid::from_u128(5),
        };
        let conversation = json!({"id":b.conversation,"config":{"model":"test","system_prompt":"","max_output_tokens":1},"messages":[],"submissions":[],"active":null,"tool_attempt":b.tool,"abort_requested":false,"phase":{"ToolRunning":{"turns":0,"calls":[],"index":0,"operation_id":b.tool.operation_id,"replay":"Safe","arguments":{},"restarts":0,"replay_ready":false}}});
        let conv_key = Harness::key(b.conversation);
        owner
            .store()
            .commit(
                &[Check {
                    key: conv_key.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: conv_key,
                    value: serde_json::to_vec(&conversation).unwrap(),
                }],
            )
            .unwrap();
        let mut parent = Task::new(
            b.task,
            TaskOwner::Conversation(b.conversation),
            Checkpoint {
                phase: "parent".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), true);
        parent.start_attempt(b.task_attempt).unwrap();
        TaskStore::new(owner.store())
            .save(&[Save::Create(&parent)])
            .unwrap();
        let bind = executor_handoff::prepare_binding(owner.store(), &b).unwrap();
        owner.store().commit(&bind.checks, &bind.mutations).unwrap();
        let worker = Task::new(
            Uuid::from_u128(20),
            TaskOwner::Conversation(Uuid::from_u128(30)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("block".into(), true);
        scheduler.submit(&worker).unwrap();
        let shared = Arc::new(Shared {
            owner: owner.clone(),
            state: Mutex::new(State::default()),
            activity: scheduler.worker_activity(),
            scheduler: Mutex::new(scheduler),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        });
        let ticking = shared.clone();
        let thread = thread::spawn(move || ticking.scheduler.lock().unwrap().tick(0).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let effect = || task_store::OwnedBatch {
            checks: vec![Check {
                key: b"test/quiet".to_vec(),
                expected: Expected::Missing,
            }],
            mutations: vec![Mutation::Put {
                key: b"test/quiet".to_vec(),
                value: vec![1],
            }],
        };
        assert!(matches!(
            shared.try_quiescent_commit(b.clone(), effect()),
            Err(QuiescentError::Busy)
        ));
        assert!(owner.store().get(b"test/quiet").unwrap().is_none());
        release_tx.send(()).unwrap();
        assert!(thread.join().unwrap());
        // Hold the task gate so the commit pauses after acquiring the scheduler
        // mutex; another tick must not enter until the checked commit finishes.
        let task_gate = owner.task_gate().lock().unwrap();
        let committing = shared.clone();
        let binding = b.clone();
        let commit_thread = thread::spawn(move || {
            loop {
                let batch = task_store::OwnedBatch {
                    checks: vec![Check {
                        key: b"test/quiet".to_vec(),
                        expected: Expected::Missing,
                    }],
                    mutations: vec![Mutation::Put {
                        key: b"test/quiet".to_vec(),
                        value: vec![1],
                    }],
                };
                match committing.try_quiescent_commit(binding.clone(), batch) {
                    Err(QuiescentError::Busy) => thread::yield_now(),
                    result => break result,
                }
            }
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while shared.scheduler.try_lock().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "commit did not acquire scheduler"
            );
            thread::yield_now();
        }
        assert!(owner.store().get(b"test/quiet").unwrap().is_none());
        drop(task_gate);
        commit_thread.join().unwrap().unwrap();
        assert_eq!(
            owner.store().get(b"test/quiet").unwrap().unwrap().value,
            vec![1]
        );
        let tasks = TaskStore::new(owner.store());
        let running = tasks.load(b.task).unwrap().unwrap();
        tasks.update(&running, |task| task.recover()).unwrap();
        let ready = tasks.load(b.task).unwrap().unwrap();
        let mut next = b.clone();
        next.task_attempt = Uuid::from_u128(6);
        tasks
            .update(&ready, |task| task.start_attempt(next.task_attempt))
            .unwrap();
        assert!(matches!(
            shared.try_quiescent_commit(
                b.clone(),
                task_store::OwnedBatch {
                    checks: vec![],
                    mutations: vec![]
                }
            ),
            Err(QuiescentError::Lease(executor_handoff::Error::Stale))
        ));
        let bind = executor_handoff::prepare_binding(owner.store(), &next).unwrap();
        owner.store().commit(&bind.checks, &bind.mutations).unwrap();
        shared
            .try_quiescent_commit(
                next.clone(),
                task_store::OwnedBatch {
                    checks: vec![],
                    mutations: vec![],
                },
            )
            .unwrap();
        let running = tasks.load(b.task).unwrap().unwrap();
        tasks.update(&running, |task| task.request_abort()).unwrap();
        assert!(matches!(
            shared.try_quiescent_commit(
                next.clone(),
                task_store::OwnedBatch {
                    checks: vec![],
                    mutations: vec![]
                }
            ),
            Err(QuiescentError::Lease(executor_handoff::Error::Stale))
        ));
        shared.state.lock().unwrap().stop = true;
        assert!(matches!(
            shared.try_quiescent_commit(
                next.clone(),
                task_store::OwnedBatch {
                    checks: vec![],
                    mutations: vec![]
                }
            ),
            Err(QuiescentError::Stopped)
        ));
        drop(shared);
        drop(owner);
        let scheduler = task_scheduler::Scheduler::open(&path, registry).unwrap();
        assert!(scheduler.get(b.task).unwrap().unwrap().abort_requested());
        assert!(matches!(
            executor_handoff::checks(scheduler.owner().store(), &next),
            Err(executor_handoff::Error::Stale)
        ));
    }

    #[test]
    fn tick_failure_stops_quiescent_commits_even_before_reservation() {
        use crate::executor_handoff::Binding;
        use crate::runtime::ToolAttempt;
        use durable_store::{Check, Expected, Mutation};

        let dir = tempfile::tempdir().unwrap();
        let scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), Default::default()).unwrap();
        let owner = scheduler.owner();
        // Malformed task data makes tick fail during its initial scan, before
        // any reservation sets the scheduler's own poisoned flag.
        owner
            .store()
            .commit(
                &[Check {
                    key: task_store::key(Uuid::from_u128(10)),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: task_store::key(Uuid::from_u128(10)),
                    value: b"invalid".to_vec(),
                }],
            )
            .unwrap();
        let shared = Shared {
            owner,
            activity: scheduler.worker_activity(),
            scheduler: Mutex::new(scheduler),
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        };
        assert!(shared.tick_and_record_failure(0).is_err());
        assert!(shared.scheduler.lock().unwrap().check().is_ok());
        assert!(shared.state.lock().unwrap().failure.is_some());
        let binding = Binding {
            conversation: Uuid::from_u128(1),
            tool: ToolAttempt {
                operation_id: Uuid::from_u128(2),
                attempt_id: Uuid::from_u128(3),
            },
            task: Uuid::from_u128(4),
            task_attempt: Uuid::from_u128(5),
        };
        assert!(matches!(
            shared.try_quiescent_commit(
                binding,
                task_store::OwnedBatch {
                    checks: vec![],
                    mutations: vec![],
                }
            ),
            Err(QuiescentError::Stopped)
        ));
    }

    #[test]
    fn wake_hints_are_bounded_and_shutdown_races_are_safe() {
        use std::sync::mpsc::TryRecvError;
        let dir = tempfile::tempdir().unwrap();
        let scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), Default::default()).unwrap();
        let driver = Driver::start(scheduler);
        let rx = driver.subscribe_wake().unwrap().unwrap();
        rx.try_recv().unwrap();
        for _ in 0..1000 {
            driver.shared.notify_host();
        }
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let shared = driver.shared.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let other_barrier = barrier.clone();
        let notifying = thread::spawn(move || {
            other_barrier.wait();
            for _ in 0..1000 {
                shared.notify_host();
            }
        });
        barrier.wait();
        driver.shutdown().unwrap();
        notifying.join().unwrap();
        while rx.try_recv().is_ok() {}
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn signal_delivery_waits_for_task_snapshot_gate() {
        let dir = tempfile::tempdir().unwrap();
        let mut registry = task_scheduler::Registry::default();
        registry.register("wait", true, |_: &Task| {
            Ok(task_scheduler::Step::Wait {
                checkpoint: Checkpoint {
                    phase: "wait".into(),
                    data: json!(null),
                },
                condition: WaitFor::Signal("go".into()),
                children: vec![],
            })
        });
        let mut scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("wait".into(), true);
        scheduler.submit(&task).unwrap();
        scheduler.tick(0).unwrap();
        let shared = Arc::new(Shared {
            owner: scheduler.owner(),
            state: Mutex::new(State::default()),
            activity: scheduler.worker_activity(),
            scheduler: Mutex::new(scheduler),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        });
        let driver = Driver {
            shared,
            worker: None,
            cleanup: None,
        };
        let shared = driver.shared.clone();
        let gate = shared.owner.task_gate().lock().unwrap();
        let signal = Signal::new(Uuid::new_v4(), task.id(), "go".into(), json!(null));
        let signal_id = signal.id;
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let sender = thread::spawn(move || {
            started_tx.send(()).unwrap();
            driver.signal(&signal).unwrap();
            done_tx.send(()).unwrap();
            driver
        });
        started_rx.recv().unwrap();
        // The signal holds state while it waits for the gate. Observing this
        // lock proves the delivery attempt has reached the critical section.
        let limit = std::time::Instant::now() + Duration::from_secs(5);
        while shared.state.try_lock().is_ok() {
            assert!(
                std::time::Instant::now() < limit,
                "signal bypassed the task gate"
            );
            thread::yield_now();
        }
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert!(
            TaskStore::new(shared.owner.store())
                .signal(task.id(), signal_id)
                .unwrap()
                .is_none()
        );
        drop(gate);
        let driver = sender.join().unwrap();
        done_rx.recv().unwrap();
        assert!(
            TaskStore::new(shared.owner.store())
                .signal(task.id(), signal_id)
                .unwrap()
                .is_some()
        );
        driver.shutdown().unwrap();
    }

    #[test]
    fn hook_commit_and_registry_replacement_are_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let handler: Arc<dyn task_scheduler::Handler> =
            Arc::new(|_: &Task| Ok(task_scheduler::Step::Succeed(json!(null))));
        let mut registry = task_scheduler::Registry::default();
        registry.register_shared("hook", true, handler.clone());
        let mut scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), Default::default()).unwrap();
        scheduler.replace_registry(registry).unwrap();
        let shared = Arc::new(Shared {
            owner: scheduler.owner(),
            state: Mutex::new(State {
                policies: scheduler.policies(),
                definitions: scheduler.definitions(),
                ..State::default()
            }),
            activity: scheduler.worker_activity(),
            scheduler: Mutex::new(scheduler),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        });
        let conversation = Uuid::new_v4();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            1,
        )
        .unwrap()
        .registered("hook".into(), true);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let commit_shared = shared.clone();
        let commit_child = child.clone();
        let commit_handler = handler.clone();
        let commit = thread::spawn(move || {
            commit_shared.with_hook_children(
                std::slice::from_ref(&commit_child),
                &[commit_handler],
                conversation,
                || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    TaskStore::new(commit_shared.owner.store())
                        .save(&[Save::Create(&commit_child)])
                        .unwrap();
                    Ok(())
                },
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // The save holds both locks; replacement cannot publish a new definition.
        assert!(shared.state.try_lock().is_err());
        assert!(shared.owner.task_gate().try_lock().is_err());
        let driver = Driver {
            shared: shared.clone(),
            worker: None,
            cleanup: None,
        };
        let (started_tx, started_rx) = mpsc::channel();
        let replacement = thread::spawn(move || {
            let mut registry = task_scheduler::Registry::default();
            registry.register("hook", true, |_: &Task| {
                Ok(task_scheduler::Step::Succeed(json!(null)))
            });
            started_tx.send(()).unwrap();
            driver.replace_registry(registry).unwrap();
            driver
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        release_tx.send(()).unwrap();
        commit.join().unwrap().unwrap();
        let driver = replacement.join().unwrap();
        assert!(
            TaskStore::new(shared.owner.store())
                .load(child.id())
                .unwrap()
                .is_some()
        );
        // Same policy/version with a different handler must fail before saving.
        let mut called = false;
        assert!(
            shared
                .with_hook_children(
                    std::slice::from_ref(&child),
                    std::slice::from_ref(&handler),
                    conversation,
                    || {
                        called = true;
                        Ok(())
                    }
                )
                .is_err()
        );
        assert!(!called);
        // Restore the selected handler, then independently reject a task with
        // a different persisted version.
        let mut registry = task_scheduler::Registry::default();
        registry.register_shared("hook", true, handler.clone());
        driver.replace_registry(registry).unwrap();
        let versioned = child
            .clone()
            .registered_versioned("hook".into(), true, 2, json!(null))
            .unwrap();
        assert!(
            shared
                .with_hook_children(
                    &[versioned],
                    std::slice::from_ref(&handler),
                    conversation,
                    || Ok(())
                )
                .is_err()
        );
        // The same selected handler is still rejected if replay changed.
        let mut registry = task_scheduler::Registry::default();
        registry.register_shared("hook", false, handler.clone());
        driver.replace_registry(registry).unwrap();
        assert!(
            shared
                .with_hook_children(
                    std::slice::from_ref(&child),
                    std::slice::from_ref(&handler),
                    conversation,
                    || Ok(())
                )
                .is_err()
        );
        let mut registry = task_scheduler::Registry::default();
        registry.register_versioned(
            "hook",
            2,
            true,
            |_: &Task| Ok(task_scheduler::Step::Succeed(json!(null))),
            None,
        );
        driver.replace_registry(registry).unwrap();
        assert!(
            shared
                .with_hook_children(&[child], &[handler], conversation, || Ok(()))
                .is_err()
        );
        driver.shutdown().unwrap();
    }

    #[test]
    fn replacement_exposes_failures_and_restarts_blocked_work() {
        let dir = tempfile::tempdir().unwrap();
        let scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), Default::default()).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "old".into(),
                data: json!(null),
            },
            5,
        )
        .unwrap()
        .registered("job".into(), true);
        TaskStore::new(scheduler.owner().store())
            .save(&[Save::Create(&task)])
            .unwrap();
        let driver = Driver::start(scheduler);
        assert_eq!(
            driver.blocked_tasks().unwrap()[0].reason,
            task_scheduler::BlockedReason::MissingTask
        );
        let mut bad = task_scheduler::Registry::default();
        bad.register_versioned(
            "job",
            2,
            true,
            |_: &Task| Ok(task_scheduler::Step::Succeed(json!(null))),
            Some(Arc::new(|_, _, _| Err("bad checkpoint".into()))),
        );
        driver.replace_registry(bad).unwrap();
        assert_eq!(
            driver.shared.state.lock().unwrap().policies.get("job"),
            Some(&(true, 2))
        );
        assert!(driver.submit(&task).is_err());
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let blocked = driver.blocked_tasks().unwrap();
            if blocked.iter().any(|item| matches!(&item.reason, task_scheduler::BlockedReason::MigrationFailed(e) if e == "bad checkpoint"))
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        assert_eq!(driver.get(task.id()).unwrap().unwrap(), task);
        let mut good = task_scheduler::Registry::default();
        good.register_versioned(
            "job",
            2,
            true,
            |t: &Task| {
                assert_eq!(t.checkpoint().phase, "new");
                Ok(task_scheduler::Step::Succeed(json!(true)))
            },
            Some(Arc::new(|_, _, _| {
                Ok((
                    json!(null),
                    Checkpoint {
                        phase: "new".into(),
                        data: json!(null),
                    },
                ))
            })),
        );
        driver.replace_registry(good).unwrap();
        loop {
            if driver.get(task.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(driver.blocked_tasks().unwrap().is_empty());
        assert_eq!(
            driver.get(task.id()).unwrap().unwrap().definition_version(),
            2
        );
        driver.shutdown().unwrap();
    }

    #[test]
    fn abort_fence_does_not_hold_state_against_conversation_policy_lookup() {
        use crate::{
            recovery::UncertainPolicy,
            runtime::{Config, Harness, Options},
        };
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
                restart_limit: 1,
                max_turns: 1,
                uncertain_policy: UncertainPolicy::InterruptedResult,
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
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            1,
        )
        .unwrap();
        TaskStore::new(harness.subagent_store().unwrap())
            .save(&[Save::Create(&task)])
            .unwrap();
        let handler: Arc<dyn task_scheduler::Handler> =
            Arc::new(|_: &Task| Ok(task_scheduler::Step::Succeed(json!(null))));
        let mut registry = task_scheduler::Registry::default();
        registry.register_shared("test", true, handler.clone());
        let scheduler = harness.attach_tasks(registry).unwrap();
        // Construct without worker threads so they cannot consume the test task.
        let shared = Arc::new(Shared {
            owner: scheduler.owner(),
            state: Mutex::new(State {
                policies: scheduler.policies(),
                definitions: scheduler.definitions(),
                ..State::default()
            }),
            activity: scheduler.worker_activity(),
            scheduler: Mutex::new(scheduler),
            wake: Condvar::new(),
            harness: Some(Mutex::new(harness.worker())),
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        });
        let gate = shared.owner.conversation_controls().get(conversation).gate;
        let driver = Driver {
            shared: shared.clone(),
            worker: None,
            cleanup: None,
        };
        let (gate_held_tx, gate_held_rx) = mpsc::channel();
        let (lookup_tx, lookup_rx) = mpsc::channel();
        let (policy_done_tx, policy_done_rx) = mpsc::channel();
        let lookup = thread::spawn(move || {
            let _gate = gate.lock().unwrap();
            gate_held_tx.send(()).unwrap();
            lookup_rx.recv().unwrap();
            let policy = shared.policy_for("test", &handler);
            let child = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(conversation),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                1,
            )
            .unwrap()
            .registered("test".into(), true);
            shared
                .with_hook_children(&[child], &[handler], conversation, || Ok(()))
                .unwrap();
            policy_done_tx.send(policy).unwrap();
        });
        gate_held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (checked_tx, checked_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let (abort_done_tx, abort_done_rx) = mpsc::channel();
        let task_id = task.id();
        let abort = thread::spawn(move || {
            let result = driver.request_abort_after_check(task_id, || {
                checked_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
            });
            abort_done_tx.send(result).unwrap();
            driver
        });
        checked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // The lookup occurs while the abort call is paused immediately after
        // checking state, and while the conversation poll gate is held.
        lookup_tx.send(()).unwrap();
        continue_tx.send(()).unwrap();
        assert_eq!(
            policy_done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("policy lookup deadlocked with abort"),
            Some(true)
        );
        abort_done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("abort fence deadlocked")
            .unwrap();
        lookup.join().unwrap();
        let driver = abort.join().unwrap();
        assert!(driver.get(task_id).unwrap().unwrap().abort_requested());
        driver.shutdown().unwrap();
    }

    #[test]
    fn abort_during_handler_fences_children_and_drains() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: json!(null),
        };
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register_with_abort(
            "test",
            true,
            move |_: &Task| {
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                Ok(task_scheduler::Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "wait".into(),
                        data: json!(null),
                    },
                    condition: WaitFor::Tasks(vec![child]),
                    children: vec![
                        Task::new(
                            child,
                            TaskOwner::Task(parent),
                            Checkpoint {
                                phase: "start".into(),
                                data: json!(null),
                            },
                            3,
                        )
                        .unwrap()
                        .registered("test".into(), true),
                    ],
                })
            },
            |_: &Task| Ok(task_scheduler::Step::Succeed(json!(null))),
        );
        let scheduler = task_scheduler::Scheduler::open(&path, registry).unwrap();
        scheduler
            .submit(
                &Task::new(
                    parent,
                    TaskOwner::Conversation(Uuid::new_v4()),
                    checkpoint,
                    3,
                )
                .unwrap()
                .registered("test".into(), true),
            )
            .unwrap();
        let driver = Driver::start(scheduler);
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        driver.request_abort(parent).unwrap();
        assert!(driver.get(parent).unwrap().unwrap().abort_requested());
        release_tx.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if driver.get(parent).unwrap().unwrap().is_terminal() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "cleanup did not finish"
            );
            std::thread::yield_now();
        }
        assert!(driver.get(child).unwrap().is_none());
        driver.shutdown().unwrap();
        let reopened =
            task_scheduler::Scheduler::open(&path, task_scheduler::Registry::default()).unwrap();
        assert!(reopened.get(parent).unwrap().unwrap().abort_requested());
        assert!(reopened.get(child).unwrap().is_none());
    }
}
