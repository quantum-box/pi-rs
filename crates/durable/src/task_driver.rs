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
    time::Duration,
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
    #[error("task driver stopped with unresolved owned outcomes")]
    Unresolved(Box<ShutdownReport>),
    #[error("task {0} does not exist")]
    UnknownTask(Uuid),
    #[error("task wait cancelled")]
    WaitCancelled,
    #[error("conversation runtime: {0}")]
    Runtime(#[from] crate::runtime::Error),
}
pub(crate) struct Shared {
    scheduler: Mutex<task_scheduler::Scheduler>,
    failure_publication: task_scheduler::FailurePublication,
    reservation_stop: Arc<std::sync::atomic::AtomicBool>,
    registry_gate: Mutex<()>,
    registry_replacing: std::sync::atomic::AtomicBool,
    activity: crate::worker_activity::WorkerActivity,
    wake_reader: task_scheduler::WakeReader,
    owner: Arc<OwnedStore>,
    state: Mutex<State>,
    pending: Mutex<Vec<task_scheduler::PendingSettlement>>,
    wake: Condvar,
    harness: Option<Mutex<crate::runtime::Harness>>,
    cleanup_stop: pi_rs_core::CancellationToken,
    unhooked_cancellations: Mutex<HashMap<(Uuid, Uuid), pi_rs_core::CancellationToken>>,
    notifications: Mutex<Option<SyncSender<()>>>,
    subscription: Mutex<Option<Receiver<()>>>,
}

// A dropped Driver has no return channel. Keep its unresolved leases available
// for explicit recovery rather than discarding replay-unsafe outcomes.
static ABANDONED_PENDING: std::sync::OnceLock<Mutex<Vec<task_scheduler::PendingSettlement>>> =
    std::sync::OnceLock::new();
impl Drop for Shared {
    fn drop(&mut self) {
        let pending = self.pending.get_mut().unwrap_or_else(|e| e.into_inner());
        if !pending.is_empty() {
            ABANDONED_PENDING
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .append(pending);
        }
    }
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
/// Shutdown status and outcomes that could not safely be settled.
/// Keeping this value keeps the original store attachment and attempts alive.
#[must_use]
#[derive(Debug)]
pub struct ShutdownReport {
    failure: Option<String>,
    pending: Vec<task_scheduler::PendingSettlement>,
}
impl ShutdownReport {
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }
    pub fn pending(&self) -> &[task_scheduler::PendingSettlement] {
        &self.pending
    }
    pub fn into_pending(self) -> Vec<task_scheduler::PendingSettlement> {
        self.pending
    }
}

/// Drop asks the thread to stop; `shutdown` additionally waits for it. The
/// thread retains storage ownership until its current handler returns.
pub struct Driver {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
    cleanup: Option<JoinHandle<()>>,
}
impl Driver {
    #[cfg(test)]
    pub(crate) fn settle_for_test(
        &self,
        completed: task_scheduler::CompletedInvocation,
    ) -> Result<bool, task_scheduler::SettlementError> {
        self.shared
            .scheduler
            .lock()
            .unwrap()
            .settle_invocation(completed)
    }
    #[cfg(test)]
    pub(crate) fn has_pending_task(&self, id: Uuid) -> bool {
        self.shared
            .pending
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.task_id() == id)
    }
    /// Take outcomes retained when a driver was dropped without shutdown.
    /// The corresponding store attachments remain held until these values drop.
    pub fn take_abandoned_pending() -> Vec<task_scheduler::PendingSettlement> {
        std::mem::take(
            &mut *ABANDONED_PENDING
                .get_or_init(|| Mutex::new(Vec::new()))
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }
    pub fn start(scheduler: task_scheduler::Scheduler) -> Self {
        let harness = scheduler.conversation_harness();
        Self::start_inner(scheduler, harness)
    }
    pub(crate) fn start_with_harness(
        scheduler: task_scheduler::Scheduler,
        harness: crate::runtime::Harness,
    ) -> Self {
        Self::start_inner(scheduler, Some(harness))
    }
    fn start_inner(
        mut scheduler: task_scheduler::Scheduler,
        harness: Option<crate::runtime::Harness>,
    ) -> Self {
        let (serial_pending, serial_pending_failed) = scheduler.take_serial_pending_for_driver();
        let owner = scheduler.owner();
        let activity = scheduler.worker_activity();
        let wake_reader = scheduler.wake_reader();
        let policies = scheduler.policies();
        let definitions = scheduler.definitions();
        let (notify_tx, notify_rx) = mpsc::sync_channel(1);
        // Seed the subscription so recovery before subscription cannot be missed.
        let _ = notify_tx.try_send(());
        let shared = Arc::new(Shared {
            registry_gate: Mutex::new(()),
            registry_replacing: std::sync::atomic::AtomicBool::new(false),
            reservation_stop: scheduler.reservation_stop_token(),
            failure_publication: scheduler.failure_publication(),
            scheduler: Mutex::new(scheduler),
            activity,
            wake_reader,
            owner,
            state: Mutex::new(State {
                policies,
                definitions,
                ..State::default()
            }),
            pending: Mutex::new(serial_pending.into_iter().collect()),
            wake: Condvar::new(),
            harness: harness.map(Mutex::new),
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            unhooked_cancellations: Mutex::new(HashMap::new()),
            notifications: Mutex::new(Some(notify_tx)),
            subscription: Mutex::new(Some(notify_rx)),
        });
        if serial_pending_failed {
            shared
                .stop_for_failure("indeterminate serial settlement requires reconciliation".into());
        }
        if let Some(harness) = &shared.harness {
            let bound = harness.lock().map_err(|_| ()).and_then(|harness| {
                harness
                    .bind_task_driver(Arc::downgrade(&shared))
                    .map_err(|_| ())
            });
            if bound.is_err() {
                shared.stop_for_failure("conversation driver binding failed".into());
            }
        }
        let task_shared = shared.clone();
        let worker = thread::spawn(move || {
            // Close the host channel even if a handler panics.
            struct CloseOnExit(Arc<Shared>);
            impl Drop for CloseOnExit {
                fn drop(&mut self) {
                    if std::thread::panicking() {
                        self.0.failure_publication.indeterminate(|| {
                            self.0
                                .reservation_stop
                                .store(true, std::sync::atomic::Ordering::Release);
                            if let Ok(mut state) = self.0.state.lock() {
                                state.stop = true;
                                state.failure = Some("task worker panicked".into());
                            }
                        });
                    }
                    if std::thread::panicking() {
                        self.0.cancel_unhooked();
                    }
                    self.0.activity.notify();
                    self.0.close_notifications();
                }
            }
            let _close = CloseOnExit(task_shared.clone());
            task_shared.run_bounded_workers();
        });
        let cleanup = shared.harness.as_ref().map(|_| {
            let shared = shared.clone();
            thread::spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    cleanup_loop(shared.clone())
                }));
                if let Err(payload) = result {
                    fail_cleanup(&shared, "cleanup worker panicked".into());
                    std::panic::resume_unwind(payload);
                }
            })
        });
        Self {
            shared,
            worker: Some(worker),
            cleanup,
        }
    }
    /// Failures of individual conversation cleanup workers, keyed by conversation.
    pub fn conversation_cleanup_failures(&self) -> Result<HashMap<Uuid, String>, Error> {
        Ok(self
            .shared
            .owner
            .conversation_controls()
            .worker_errors()
            .lock()
            .map_err(|_| Error::Poisoned)?
            .clone())
    }

    /// Acknowledge externally reconciled cleanup effects and permit a retry.
    pub fn reconcile_owned_cleanup(&self, id: Uuid) -> Result<(), Error> {
        let harness = self
            .shared
            .harness
            .as_ref()
            .ok_or_else(|| Error::Stopped("conversation runtime is not attached".into()))?;
        harness
            .lock()
            .map_err(|_| Error::Poisoned)?
            .reconcile_owned_cleanup(id)?;
        self.shared
            .owner
            .conversation_controls()
            .worker_errors()
            .lock()
            .map_err(|_| Error::Poisoned)?
            .remove(&id);
        self.shared.wake.notify_all();
        Ok(())
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
    #[cfg(test)]
    pub(crate) fn test_failure_publication(&self) -> task_scheduler::FailurePublication {
        self.shared.failure_publication.clone()
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
        // Serialize publication state with the commit without taking the
        // settlement fence, which can be waiting for a conversation gate.
        let _publication = self
            .shared
            .failure_publication
            .admission()
            .map_err(|_| task_scheduler::Error::Poisoned)?;
        if let TaskOwner::Conversation(id) = task.owner() {
            if !_publication.permits_admission(&self.shared.owner, *id, None) {
                return Err(task_scheduler::Error::Busy.into());
            }
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
        let _serial = self
            .shared
            .registry_gate
            .lock()
            .map_err(|_| Error::Poisoned)?;
        self.shared
            .registry_replacing
            .store(true, std::sync::atomic::Ordering::Release);
        struct Replacing<'a>(&'a Shared);
        impl Drop for Replacing<'_> {
            fn drop(&mut self) {
                self.0
                    .registry_replacing
                    .store(false, std::sync::atomic::Ordering::Release);
                self.0.wake.notify_all();
            }
        }
        let _replacing = Replacing(&self.shared);
        loop {
            let mut scheduler = self.shared.scheduler.lock().map_err(|_| Error::Poisoned)?;
            let failure = match self.shared.failure_publication.try_lock() {
                Ok(guard) => guard,
                Err(std::sync::TryLockError::WouldBlock) => {
                    drop(scheduler);
                    self.wait_for_registry_boundary()?;
                    continue;
                }
                Err(std::sync::TryLockError::Poisoned(_)) => return Err(Error::Poisoned),
            };
            if scheduler.has_live_invocations()? {
                drop(failure);
                drop(scheduler);
                self.wait_for_registry_boundary()?;
                continue;
            }
            let mut state = self.check()?;
            let gate = self
                .shared
                .owner
                .task_gate()
                .lock()
                .map_err(|_| Error::Poisoned)?;
            if let Some(harness) = &validator {
                harness.validate_task_tool_registry(&registry)?;
            }
            let result = scheduler.replace_registry_locked(registry.clone());
            if matches!(result, Err(task_scheduler::Error::Busy)) {
                drop(gate);
                drop(state);
                drop(failure);
                drop(scheduler);
                self.wait_for_registry_boundary()?;
                continue;
            }
            state.policies = scheduler.policies();
            state.definitions = scheduler.definitions();
            if let Err(error) = result {
                state.failure = Some(error.to_string());
                state.stop = true;
                self.shared
                    .reservation_stop
                    .store(true, std::sync::atomic::Ordering::Release);
                self.shared.wake.notify_all();
                self.shared.activity.notify();
                self.shared.close_notifications();
                return Err(error.into());
            }
            self.shared.notify_work();
            return Ok(());
        }
    }

    fn wait_for_registry_boundary(&self) -> Result<(), Error> {
        let state = self.check()?;
        let (state, _) = self
            .shared
            .wake
            .wait_timeout(state, Duration::from_millis(100))
            .map_err(|_| Error::Poisoned)?;
        if state.stop {
            return Err(Error::Stopped(
                state.failure.clone().unwrap_or_else(|| "shutdown".into()),
            ));
        }
        Ok(())
    }

    /// Inspect generic task wake state at a scheduling boundary using the supplied
    /// Unix-millisecond clock. Does not acquire the handler's scheduler mutex.
    pub fn wake_snapshot(&self, now_millis: i64) -> Result<task_scheduler::WakeSnapshot, Error> {
        self.shared.wake_snapshot(now_millis)
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
            #[cfg(test)]
            let _gate = {
                let hook = abort_gate_hooks().lock().unwrap().remove(&id);
                if let Some(hook) = hook {
                    match self.shared.owner.task_gate().try_lock() {
                        Err(std::sync::TryLockError::WouldBlock) => {
                            hook();
                            self.shared
                                .owner
                                .task_gate()
                                .lock()
                                .map_err(|_| Error::Poisoned)?
                        }
                        _ => panic!("abort gate was not contended"),
                    }
                } else {
                    self.shared
                        .owner
                        .task_gate()
                        .lock()
                        .map_err(|_| Error::Poisoned)?
                }
            };
            #[cfg(not(test))]
            let _gate = self
                .shared
                .owner
                .task_gate()
                .lock()
                .map_err(|_| Error::Poisoned)?;
            task_scheduler::request_abort_subtree(&TaskStore::new(self.shared.owner.store()), id)?;
            let active = match self.shared.unhooked_cancellations.lock() {
                Ok(active) => active,
                Err(poisoned) => {
                    let active = poisoned.into_inner();
                    for token in active.values() {
                        token.cancel();
                    }
                    drop(active);
                    self.shared.notify_work();
                    return Err(Error::Poisoned);
                }
            };
            let mut load_error = None;
            for ((task_id, _), token) in active.iter() {
                match TaskStore::new(self.shared.owner.store()).load(*task_id) {
                    Ok(task) if task.as_ref().is_none_or(|task| task.task.abort_requested()) => {
                        token.cancel()
                    }
                    Ok(_) => (),
                    Err(error) => {
                        token.cancel();
                        load_error.get_or_insert(error);
                    }
                }
            }
            drop(active);
            if let Some(error) = load_error {
                self.shared.notify_work();
                return Err(task_scheduler::Error::from(error).into());
            }
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
    /// Stop and join the driver, returning any outcomes that require external
    /// reconciliation. Dropping a returned outcome explicitly abandons its lease.
    pub fn shutdown_with_pending(mut self) -> Result<ShutdownReport, Error> {
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
        let state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        let failure = if worker_panicked || cleanup_panicked {
            Some(format!(
                "worker panicked: {worker_panicked}; cleanup worker panicked: {cleanup_panicked}"
            ))
        } else {
            state.failure.clone()
        };
        drop(state);
        let pending = std::mem::take(
            &mut *self
                .shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        Ok(ShutdownReport { failure, pending })
    }
    /// Preserve the existing successful shutdown result. Unresolved outcomes
    /// travel in the error rather than being silently discarded.
    pub fn shutdown(self) -> Result<(), Error> {
        let report = self.shutdown_with_pending()?;
        if !report.pending.is_empty() {
            return Err(Error::Unresolved(Box::new(report)));
        }
        if let Some(error) = report.failure {
            return Err(Error::Stopped(error));
        }
        Ok(())
    }
    fn stop(&self) {
        self.shared.cleanup_stop.cancel();
        self.stop_worker();
    }
    #[cfg(test)]
    pub(crate) fn active_unhooked_count_for_test(&self) -> usize {
        self.shared.unhooked_cancellations.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn stop_worker_for_test(&self) {
        self.stop_worker();
    }

    fn stop_worker(&self) {
        self.shared
            .reservation_stop
            .store(true, std::sync::atomic::Ordering::Release);
        self.shared.cancel_unhooked();
        self.shared.close_notifications();
        if let Ok(mut state) = self.shared.state.lock() {
            state.stop = true;
            self.shared.wake.notify_all();
        }
        self.shared.activity.notify();
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
        let mut blocked = std::collections::HashSet::new();
        loop {
            if shared.cleanup_stop.is_cancelled() {
                break;
            }
            while let Some(result) = active.try_join_next() {
                match result {
                    Ok((id, Ok(()))) => {
                        ids.remove(&id);
                        shared
                            .owner
                            .conversation_controls()
                            .worker_errors()
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&id);
                    }
                    Ok((
                        id,
                        Err(
                            error @ (crate::runtime::Error::Storage(_)
                            | crate::runtime::Error::Failed),
                        ),
                    )) => {
                        ids.remove(&id);
                        fail_cleanup(&shared, error.to_string());
                        return;
                    }
                    Ok((id, Err(error))) => {
                        ids.remove(&id);
                        blocked.insert(id);
                        shared
                            .owner
                            .conversation_controls()
                            .worker_errors()
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(id, error.to_string());
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
                    let Ok(child) =
                        serde_json::from_slice::<crate::runtime::Conversation>(&entry.value)
                    else {
                        continue;
                    };
                    if !child.abort_requested {
                        continue;
                    }
                    if *key != format!("runtime/conversation/{}", child.id).into_bytes() {
                        continue;
                    }
                    let edge_key = crate::ownership_edge::edge_key(child.id);
                    let Some(edge_entry) = entries.get(&edge_key) else {
                        continue;
                    };
                    let report = |reason: String| {
                        shared
                            .owner
                            .conversation_controls()
                            .worker_errors()
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .insert(child.id, reason);
                    };
                    let edge = match crate::ownership_edge::decode(&edge_key, edge_entry.clone()) {
                        Ok(edge) => edge,
                        Err(error) => {
                            report(format!("invalid ownership edge: {error}"));
                            continue;
                        }
                    };
                    let task_key = crate::task_store::key(edge.edge.task);
                    let Some(task_entry) = entries.get(&task_key) else {
                        report("ownership task missing".into());
                        continue;
                    };
                    let task = match crate::task_store::decode(&task_key, task_entry.clone()) {
                        Ok(task) => task,
                        Err(error) => {
                            report(format!("invalid ownership task: {error}"));
                            continue;
                        }
                    };
                    if edge.edge.task_owner != *task.task.owner() {
                        report("ownership edge owner mismatch".into());
                        continue;
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
                        if blocked.contains(&id) {
                            match shared
                                .owner
                                .store()
                                .get(&crate::runtime::owned_cleanup_key(id))
                            {
                                Ok(Some(_)) => continue,
                                Ok(None) => {
                                    blocked.remove(&id);
                                }
                                Err(error) => {
                                    fail_cleanup(&shared, error.to_string());
                                    return;
                                }
                            }
                        }
                        if ids.insert(id) {
                            let worker = shared
                                .harness
                                .as_ref()
                                .unwrap()
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .worker();
                            let stop = shared.cleanup_stop.clone();
                            active.spawn(async move {
                                (id, worker.drive_owned_cleanup_fenced(id, &stop).await)
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
    shared.failure_publication.indeterminate(|| {
        shared
            .reservation_stop
            .store(true, std::sync::atomic::Ordering::Release);
        if let Ok(mut state) = shared.state.lock() {
            state.failure = Some(error);
            state.stop = true;
            shared.wake.notify_all();
        }
    });
    shared.cancel_unhooked();
    shared.activity.notify();
    shared.cleanup_stop.cancel();
    shared.close_notifications();
}

enum WorkerEvent {
    Released,
    Completed(Box<task_scheduler::CompletedInvocation>),
    Failed(String),
}

#[cfg(test)]
type BeforeExecuteHook = Box<dyn FnOnce(&task_scheduler::DetachedInvocation) + Send>;
#[cfg(test)]
fn before_execute_hooks() -> &'static Mutex<std::collections::HashMap<Uuid, BeforeExecuteHook>> {
    static HOOKS: std::sync::OnceLock<Mutex<std::collections::HashMap<Uuid, BeforeExecuteHook>>> =
        std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}

#[cfg(test)]
pub(crate) fn before_execute_for_test(
    id: Uuid,
    hook: impl FnOnce(&task_scheduler::DetachedInvocation) + Send + 'static,
) {
    before_execute_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}

#[cfg(test)]
type BoundaryHooks = Mutex<std::collections::HashMap<Uuid, Box<dyn FnOnce() + Send>>>;
#[cfg(test)]
fn after_execute_hooks() -> &'static BoundaryHooks {
    static HOOKS: std::sync::OnceLock<BoundaryHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(crate) fn after_execute_for_test(id: Uuid, hook: impl FnOnce() + Send + 'static) {
    after_execute_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}

#[cfg(test)]
fn abort_gate_hooks() -> &'static BoundaryHooks {
    static HOOKS: std::sync::OnceLock<BoundaryHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(crate) fn abort_gate_contention_for_test(id: Uuid, hook: impl FnOnce() + Send + 'static) {
    abort_gate_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}

#[cfg(test)]
fn pending_take_hooks() -> &'static BoundaryHooks {
    static HOOKS: std::sync::OnceLock<BoundaryHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(crate) fn after_pending_take_for_test(id: Uuid, hook: impl FnOnce() + Send + 'static) {
    pending_take_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}

#[cfg(test)]
fn pending_enqueue_hooks() -> &'static BoundaryHooks {
    static HOOKS: std::sync::OnceLock<BoundaryHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(crate) fn after_pending_enqueue_for_test(id: Uuid, hook: impl FnOnce() + Send + 'static) {
    pending_enqueue_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}

struct WorkerPool {
    jobs: Option<mpsc::Sender<Box<task_scheduler::DetachedInvocation>>>,
    done: mpsc::Receiver<WorkerEvent>,
    inbox: std::collections::VecDeque<WorkerEvent>,
    workers: Vec<JoinHandle<()>>,
    shared: Arc<Shared>,
}
impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.jobs.take();
        let mut failed = false;
        for worker in self.workers.drain(..) {
            failed |= worker.join().is_err();
        }
        while let Ok(event) = self.done.try_recv() {
            self.inbox.push_back(event);
        }
        while let Some(event) = self.inbox.pop_front() {
            match event {
                WorkerEvent::Completed(completed) => self
                    .shared
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(task_scheduler::PendingSettlement::Unprepared(*completed)),
                WorkerEvent::Released => (),
                WorkerEvent::Failed(_) => failed = true,
            }
        }
        if failed {
            fail_cleanup(
                &self.shared,
                "task worker pool failed while draining".into(),
            );
        }
    }
}

impl Shared {
    pub(crate) fn notify_extension_publication(&self) {
        self.notify_work();
    }
    fn cancel_unhooked(&self) {
        let tokens: Vec<_> = self
            .unhooked_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect();
        for token in tokens {
            token.cancel();
        }
    }

    // Abort marks can also be committed by scheduler cleanup and fail-fast,
    // without passing through Driver::request_abort.
    fn cancel_marked_unhooked(&self) -> Result<(), String> {
        let _gate = self.owner.task_gate().lock().map_err(|e| e.to_string())?;
        let active = self
            .unhooked_cancellations
            .lock()
            .map_err(|e| e.to_string())?;
        let tasks = TaskStore::new(self.owner.store());
        for ((id, _), token) in active.iter() {
            match tasks.load(*id) {
                Ok(Some(task)) if !task.task.abort_requested() => {
                    match task_scheduler::pending_abort_root_excluding_self(
                        &tasks,
                        *id,
                        true,
                        task.task.holds_interrupted_cleanup(),
                    ) {
                        Ok(Some(_)) => token.cancel(),
                        Ok(None) => (),
                        Err(task_scheduler::Error::Busy)
                            if task.task.holds_interrupted_cleanup() =>
                        {
                            // Stop the hook until ownership can be proven, but keep its
                            // completed invocation available for checked settlement.
                            token.cancel();
                        }
                        Err(error) => {
                            token.cancel();
                            return Err(error.to_string());
                        }
                    }
                }
                Ok(_) => token.cancel(),
                Err(error) => {
                    token.cancel();
                    return Err(error.to_string());
                }
            }
        }
        Ok(())
    }

    fn stop_for_failure(&self, error: String) {
        self.reservation_stop
            .store(true, std::sync::atomic::Ordering::Release);
        if let Ok(mut state) = self.state.lock() {
            state.failure.get_or_insert(error);
            state.stop = true;
            self.wake.notify_all();
        }
        self.cancel_unhooked();
        self.activity.notify();
        self.close_notifications();
    }

    fn run_bounded_workers(self: &Arc<Self>) {
        const WORKERS: usize = 4;
        const TOTAL_WORKERS: usize = WORKERS + 1;
        let (jobs_tx, jobs_rx) = mpsc::channel::<Box<task_scheduler::DetachedInvocation>>();
        let jobs_rx = Arc::new(Mutex::new(jobs_rx));
        let (done_tx, done_rx) = mpsc::channel::<WorkerEvent>();
        let mut workers = Vec::with_capacity(TOTAL_WORKERS);
        for _ in 0..TOTAL_WORKERS {
            let jobs = jobs_rx.clone();
            let done = done_tx.clone();
            let shared = self.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let job = jobs.lock().unwrap_or_else(|e| e.into_inner()).recv();
                    let Ok(job) = job else {
                        break;
                    };
                    let task_id = job.task_id();
                    let registration_key = job.cancellation_registration_key();
                    if let Some(token) = job.unhooked_cancellation() {
                        let registration = (|| -> Result<(), String> {
                            let _gate =
                                shared.owner.task_gate().lock().map_err(|e| e.to_string())?;
                            let task = TaskStore::new(shared.owner.store())
                                .load(task_id)
                                .map_err(|e| e.to_string())?
                                .ok_or("reserved task missing")?;
                            let mut active = shared
                                .unhooked_cancellations
                                .lock()
                                .map_err(|e| e.to_string())?;
                            if task.task.abort_requested()
                                || shared
                                    .reservation_stop
                                    .load(std::sync::atomic::Ordering::Acquire)
                            {
                                token.cancel();
                            }
                            if active.contains_key(&registration_key) {
                                return Err("duplicate unhooked executor".into());
                            }
                            active.insert(registration_key, token);
                            Ok(())
                        })();
                        if let Err(error) = registration {
                            let _ = done.send(WorkerEvent::Failed(error));
                            break;
                        }
                    }
                    let event = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        #[cfg(test)]
                        {
                            let hook = before_execute_hooks()
                                .lock()
                                .unwrap()
                                .remove(&job.task_id());
                            if let Some(hook) = hook {
                                hook(&job);
                            }
                        }
                        job.execute()
                    })) {
                        Ok(Ok(mut completed)) => match completed.capture_marked_receipt() {
                            Ok(()) => WorkerEvent::Completed(Box::new(completed)),
                            Err(error) if error.pre_transaction_busy() => {
                                // The outcome and lease remain owned until receipt capture succeeds.
                                shared
                                    .pending
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .push(task_scheduler::PendingSettlement::Unprepared(completed));
                                WorkerEvent::Released
                            }
                            Err(error) => {
                                let message = format!("uncertain receipt capture: {error}");
                                // Publish the failure before exposing the unsaved result;
                                // the coordinator must never settle it ahead of the receipt.
                                shared.stop_for_failure(message.clone());
                                shared
                                    .pending
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .push(task_scheduler::PendingSettlement::Unprepared(completed));
                                WorkerEvent::Failed(message)
                            }
                        },
                        Ok(Err(task_scheduler::Error::Stopped)) => WorkerEvent::Released,
                        Ok(Err(error)) => WorkerEvent::Failed(error.to_string()),
                        Err(payload) => {
                            crate::extension::discard_panic_payload(payload);
                            WorkerEvent::Failed("task handler panicked".into())
                        }
                    };
                    #[cfg(test)]
                    if matches!(event, WorkerEvent::Released) {
                        let hook = after_execute_hooks().lock().unwrap().remove(&task_id);
                        if let Some(hook) = hook {
                            hook();
                        }
                    }
                    if let Ok(mut active) = shared.unhooked_cancellations.lock() {
                        active.remove(&registration_key);
                    }
                    if let Err(mpsc::SendError(event)) = done.send(event) {
                        if let WorkerEvent::Completed(completed) = event {
                            shared
                                .pending
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push(task_scheduler::PendingSettlement::Unprepared(*completed));
                        }
                        break;
                    }
                }
            }));
        }
        drop(done_tx);
        let mut pool = WorkerPool {
            jobs: Some(jobs_tx),
            done: done_rx,
            inbox: std::collections::VecDeque::new(),
            workers,
            shared: self.clone(),
        };
        let mut active = 0usize;
        let mut maintenance: Option<task_scheduler::PendingMaintenance> = None;
        loop {
            if let Err(error) = self.cancel_marked_unhooked() {
                self.stop_for_failure(format!("abort cancellation scan failed: {error}"));
            }
            while let Ok(event) = pool.done.try_recv() {
                pool.inbox.push_back(event);
            }
            let mut progressed = false;
            while let Some(event) = pool.inbox.pop_front() {
                active = active.saturating_sub(1);
                match event {
                    WorkerEvent::Completed(completed) => {
                        let result = match self.scheduler.lock() {
                            Ok(mut scheduler) => scheduler.try_settle_invocation(*completed),
                            Err(_) => {
                                self.pending.lock().unwrap_or_else(|e| e.into_inner()).push(
                                    task_scheduler::PendingSettlement::Unprepared(*completed),
                                );
                                self.stop_for_failure("task scheduler lock poisoned".into());
                                continue;
                            }
                        };
                        if let Err(error) = self.record_settlement(result) {
                            self.stop_for_failure(error);
                        }
                    }
                    WorkerEvent::Released => (),
                    WorkerEvent::Failed(error) => self.stop_for_failure(error),
                }
                self.notify_work();
                progressed = true;
            }
            let (stopped, failed) = match self.state.lock() {
                Ok(state) => (state.stop, state.failure.is_some()),
                Err(_) => {
                    self.stop_for_failure("task driver lock poisoned".into());
                    (true, true)
                }
            };
            let has_pending = !self
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty();
            // Once a worker reports an indeterminate failure, retain every
            // pending lease for shutdown/reconciliation instead of settling it.
            if has_pending && !failed {
                let before = self.pending.lock().unwrap_or_else(|e| e.into_inner()).len();
                match self.retry_pending() {
                    Ok(true) => {
                        if self.pending.lock().unwrap_or_else(|e| e.into_inner()).len() < before {
                            progressed = true;
                        }
                        self.notify_work();
                    }
                    Ok(false) => (),
                    Err(error) => {
                        self.stop_for_failure(error);
                    }
                }
            }
            if let Some(intent) = &maintenance {
                if let Some(harness) = &self.harness {
                    match harness.try_lock() {
                        Ok(harness) => {
                            let mut worker = harness.worker();
                            drop(harness);
                            match intent.try_cleanup(&mut worker) {
                                Ok(true) => {
                                    maintenance = None;
                                    progressed = true;
                                    self.notify_work();
                                }
                                Ok(false) => (),
                                Err(error) => {
                                    fail_cleanup(self, error.to_string());
                                    maintenance = None;
                                }
                            }
                        }
                        Err(std::sync::TryLockError::WouldBlock) => (),
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            fail_cleanup(self, "harness lock poisoned".into());
                            maintenance = None;
                        }
                    }
                } else {
                    fail_cleanup(self, "missing maintenance harness".into());
                    maintenance = None;
                }
            }
            let stopped_now = stopped
                || self
                    .reservation_stop
                    .load(std::sync::atomic::Ordering::Acquire);
            let graph_pending = maintenance.is_some()
                || !self
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty();
            if !stopped_now
                && (!self
                    .registry_replacing
                    .load(std::sync::atomic::Ordering::Acquire)
                    || graph_pending)
                && active < TOTAL_WORKERS
            {
                let pending = !self
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty();
                let reservation = match self.scheduler.lock() {
                    Ok(mut scheduler) => {
                        if self
                            .registry_replacing
                            .load(std::sync::atomic::Ordering::Acquire)
                            && maintenance.is_none()
                            && !pending
                        {
                            Err(task_scheduler::Error::Busy)
                        } else if maintenance.is_some()
                            || pending
                            || scheduler.has_published_outcomes().unwrap_or(true)
                        {
                            if active >= WORKERS {
                                Ok(task_scheduler::TickReservation::Progress(false))
                            } else {
                                scheduler.reserve_independent_root().map(|value| {
                                    value
                                        .map(task_scheduler::TickReservation::Invocation)
                                        .unwrap_or(task_scheduler::TickReservation::Progress(false))
                                })
                            }
                        } else if active >= WORKERS {
                            scheduler.try_reserve_tick_live(true)
                        } else {
                            scheduler.try_reserve_tick_live(false)
                        }
                    }
                    Err(_) => Err(task_scheduler::Error::Poisoned),
                };
                match reservation {
                    Ok(task_scheduler::TickReservation::Invocation(job)) => {
                        active += 1;
                        if let Err(mpsc::SendError(job)) =
                            pool.jobs.as_ref().expect("pool open").send(job)
                        {
                            active -= 1;
                            self.stop_for_failure("task worker pool disconnected".into());
                            if job.unhooked_cancellation().is_some() {
                                drop(job);
                                continue;
                            }
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                job.execute()
                            })) {
                                Ok(Ok(mut completed)) => {
                                    if let Err(error) = completed.capture_marked_receipt() {
                                        self.stop_for_failure(format!(
                                            "uncertain receipt capture: {error}"
                                        ));
                                    }
                                    self.pending.lock().unwrap_or_else(|e| e.into_inner()).push(
                                        task_scheduler::PendingSettlement::Unprepared(completed),
                                    )
                                }
                                Ok(Err(_)) => (),
                                Err(payload) => crate::extension::discard_panic_payload(payload),
                            }
                        }
                        progressed = true;
                    }
                    Ok(task_scheduler::TickReservation::Maintenance(intent)) => {
                        maintenance = Some(intent);
                        progressed = true;
                    }
                    Ok(task_scheduler::TickReservation::Progress(value)) => progressed |= value,
                    Err(task_scheduler::Error::Busy) => (),
                    Err(task_scheduler::Error::Stopped)
                        if active > 0
                            || self
                                .reservation_stop
                                .load(std::sync::atomic::Ordering::Acquire) => {}
                    Err(error) => fail_cleanup(self, error.to_string()),
                }
            }
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let stopping = state.stop;
            let failed_now = state.failure.is_some() || failed;
            drop(state);
            let remaining = !self
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty();
            if stopping && active == 0 && remaining && !self.cleanup_stop.is_cancelled() {
                let uncertain = self
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .iter()
                    .any(|pending| pending.waits_for_uncertain_graph(self.harness.is_none()));
                if uncertain {
                    // The caller must repair this graph; retrying cannot drain
                    // the lease. Stop cleanup before joining the coordinator.
                    self.cleanup_stop.cancel();
                }
            }
            if stopping
                && active == 0
                && (failed_now
                    || (!remaining && maintenance.is_none())
                    || self.cleanup_stop.is_cancelled())
            {
                break;
            }
            if !progressed {
                match pool.done.recv_timeout(Duration::from_millis(100)) {
                    Ok(event) => pool.inbox.push_back(event),
                    Err(mpsc::RecvTimeoutError::Timeout) => (),
                    Err(mpsc::RecvTimeoutError::Disconnected) if active != 0 => {
                        self.stop_for_failure("task worker pool disconnected".into());
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => (),
                }
            }
        }
        // Drop joins every worker and transfers any unconsumed completed
        // outcomes even if the coordinator is unwinding.
        drop(pool);
    }

    pub(crate) fn wake_snapshot(
        &self,
        now_millis: i64,
    ) -> Result<task_scheduler::WakeSnapshot, Error> {
        let check = || {
            let state = self.state.lock().map_err(|_| Error::Poisoned)?;
            if state.stop {
                return Err(Error::Stopped(
                    state.failure.clone().unwrap_or_else(|| "shutdown".into()),
                ));
            }
            Ok(())
        };
        check()?;
        let snapshot = self.wake_reader.snapshot(now_millis)?;
        check()?;
        Ok(snapshot)
    }

    // The serial driver owns every completed invocation until settlement or
    // transfer to the pending queue. Scheduler operations remain serialized.
    #[cfg(test)]
    fn tick_and_record_failure(&self, now: i64) -> Result<bool, String> {
        let mut scheduler = self
            .scheduler
            .lock()
            .map_err(|_| "task scheduler lock poisoned".to_string())?;
        if self.state.lock().map_or(true, |state| state.stop) {
            return Ok(false);
        }
        let result = (|| -> Result<bool, String> {
            let reservation = match scheduler.reserve_tick(now) {
                Err(task_scheduler::Error::Stopped)
                    if self
                        .reservation_stop
                        .load(std::sync::atomic::Ordering::Acquire) =>
                {
                    return Ok(false);
                }
                other => other.map_err(|e| e.to_string())?,
            };
            match reservation {
                task_scheduler::TickReservation::Progress(progress) => Ok(progress),
                task_scheduler::TickReservation::Maintenance(_) => {
                    unreachable!("serial reservation")
                }
                task_scheduler::TickReservation::Invocation(invocation) => {
                    let completed = invocation.execute().map_err(|e| e.to_string())?;
                    self.record_settlement(scheduler.try_settle_invocation(completed))
                }
            }
        })();
        if let Err(error) = &result {
            self.reservation_stop
                .store(true, std::sync::atomic::Ordering::Release);
            if let Ok(mut state) = self.state.lock() {
                state.failure.get_or_insert_with(|| error.clone());
                state.stop = true;
            }
            self.activity.notify();
        }
        result
    }

    fn record_settlement(
        &self,
        result: Result<bool, task_scheduler::SettlementError>,
    ) -> Result<bool, String> {
        use task_scheduler::{PendingSettlement, SettlementError};
        let (pending, failure) = match result {
            Ok(progress) => return Ok(progress),
            Err(SettlementError::Pending { completed }) => {
                (PendingSettlement::Unprepared(*completed), None)
            }
            Err(SettlementError::PendingPrepared { pending }) => (*pending, None),
            Err(SettlementError::Rejected { error, completed }) => (
                PendingSettlement::Unprepared(*completed),
                Some(error.to_string()),
            ),
            Err(SettlementError::RejectedPrepared { error, pending })
            | Err(SettlementError::IndeterminatePrepared { error, pending })
            | Err(SettlementError::IndeterminateOwned { error, pending }) => {
                (*pending, Some(error.to_string()))
            }
            Err(SettlementError::Handler(error) | SettlementError::Indeterminate(error)) => {
                return Err(error.to_string());
            }
            #[cfg(test)]
            Err(SettlementError::Yielded { pending }) => (*pending, None),
        };
        #[cfg(test)]
        let pending_task = pending.task_id();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(pending);
        #[cfg(test)]
        {
            let hook = pending_enqueue_hooks()
                .lock()
                .unwrap()
                .remove(&pending_task);
            if let Some(hook) = hook {
                hook();
            }
        }
        if let Some(error) = failure {
            Err(error)
        } else {
            Ok(false)
        }
    }

    fn retry_pending(&self) -> Result<bool, String> {
        let pending = {
            // The worker publishes receipt failure under state before adding
            // its result. Hold state through removal to close that race.
            let state = self
                .state
                .lock()
                .map_err(|_| "task driver state poisoned".to_string())?;
            if state.failure.is_some() {
                return Ok(false);
            }
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            if pending.is_empty() {
                return Ok(false);
            }
            pending.remove(0)
        };
        #[cfg(test)]
        {
            let hook = pending_take_hooks()
                .lock()
                .unwrap()
                .remove(&pending.task_id());
            if let Some(hook) = hook {
                hook();
            }
        }
        let result = match self.scheduler.lock() {
            Ok(mut scheduler) => scheduler.try_resume_pending(pending),
            Err(_) => {
                self.pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(pending);
                return Err("task scheduler lock poisoned".into());
            }
        };
        if self.record_settlement(result)? {
            return Ok(true);
        }
        let Some(harness) = &self.harness else {
            return Ok(false);
        };
        let mut worker = match harness.try_lock() {
            Ok(harness) => harness.worker(),
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => return Err("harness lock poisoned".into()),
        };
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        match pending
            .last()
            .expect("pending retained")
            .try_cleanup(&mut worker)
        {
            Ok(progress) => Ok(progress),
            Err(error) => Err(error.to_string()),
        }
    }

    /// Commit only while the driver owns no executing scheduler handler. The
    /// scheduler mutex is held through the conversation lease's checked commit.
    /// The caller must hold no conversation worker, poll, registry, or task gate;
    /// try-lock only avoids waiting for the scheduler, not the other fences. The
    /// stopped flag is checked at entry; shutdown may race an already-started
    /// commit. Live detached invocations, including completed but unsettled
    /// outcomes, reject the commit. This cannot authorize external effects.
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
        let _failure = self
            .failure_publication
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => QuiescentError::Busy,
                std::sync::TryLockError::Poisoned(_) => QuiescentError::Poisoned,
            })?;
        if !scheduler.check_quiescent()? {
            return Err(QuiescentError::Busy);
        }
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
        let _publication = self
            .failure_publication
            .admission()
            .map_err(|_| crate::runtime::Error::Failed)?;
        if !_publication.permits_admission(&self.owner, conversation, None) {
            return Err(crate::runtime::Error::TaskPublicationBusy);
        }
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
    // Capture every ownership link, including links beyond a background boundary,
    // while applying cancellation only to the foreground part of the chain.
    // The caller holds both the task gate and the failure publication ledger.
    fn session_owner_checks(
        store: &durable_store::Store,
        conversation: Uuid,
        boundary: u64,
    ) -> Result<Vec<durable_store::Check>, crate::runtime::Error> {
        use crate::ownership_edge::{self, EdgeStore, Mode};
        use durable_store::{Check, Expected};
        let invalid = || {
            crate::runtime::Error::Invalid(
                "invalid or aborting conversation ownership chain".into(),
            )
        };
        let tasks = TaskStore::new(store);
        let edges = EdgeStore::new(store);
        let mut next = TaskOwner::Conversation(conversation);
        let mut seen = std::collections::HashSet::new();
        let mut checks = Vec::new();
        let mut foreground = true;
        loop {
            let identity = match &next {
                TaskOwner::Task(id) => (true, *id),
                TaskOwner::Conversation(id) => (false, *id),
            };
            if !seen.insert(identity) {
                return Err(invalid());
            }
            if foreground {
                let mut check = ownership_edge::abort_fence_check(store, &next)?;
                if check.expected == Expected::Missing {
                    check.expected = Expected::MissingUnchangedSince(boundary);
                }
                checks.push(check);
            }
            match next {
                TaskOwner::Conversation(id) => {
                    let key = format!("runtime/conversation/{id}").into_bytes();
                    let entry = store.get(&key)?.ok_or_else(invalid)?;
                    let state: crate::runtime::Conversation =
                        serde_json::from_slice(&entry.value).map_err(|_| invalid())?;
                    if state.id != id || (foreground && state.abort_requested) {
                        return Err(invalid());
                    }
                    // The transaction already checks its own conversation version.
                    if id != conversation {
                        checks.push(Check {
                            key,
                            expected: Expected::Version(entry.version),
                        });
                    }
                    let key = ownership_edge::edge_key(id);
                    let Some(edge) = edges.get(id).map_err(|_| invalid())? else {
                        checks.push(Check {
                            key,
                            expected: Expected::MissingUnchangedSince(boundary),
                        });
                        break;
                    };
                    checks.push(Check {
                        key,
                        expected: Expected::Version(edge.version),
                    });
                    let owner = tasks
                        .load(edge.edge.task)
                        .map_err(|_| invalid())?
                        .ok_or_else(invalid)?;
                    if owner.task.owner() != &edge.edge.task_owner {
                        return Err(invalid());
                    }
                    if edge.edge.mode == Mode::Background {
                        foreground = false;
                    }
                    next = TaskOwner::Task(edge.edge.task);
                }
                TaskOwner::Task(id) => {
                    let stored = tasks.load(id).map_err(|_| invalid())?.ok_or_else(invalid)?;
                    if foreground {
                        let successful_anchor =
                            matches!(stored.task.state(), TaskState::Succeeded(_));
                        if stored.task.abort_requested()
                            || matches!(stored.task.state(), TaskState::CompletingFailed(_))
                            || (stored.task.is_terminal() && !successful_anchor)
                            || task_scheduler::pending_abort_root_for_running(&tasks, id, false)
                                .map_err(|_| invalid())?
                                .is_some()
                        {
                            return Err(invalid());
                        }
                    }
                    checks.push(Check {
                        key: task_store::key(id),
                        expected: Expected::Version(stored.version),
                    });
                    if stored.task.is_background() {
                        foreground = false;
                    }
                    next = stored.task.owner().clone();
                }
            }
        }
        Ok(checks)
    }
    /// Validate a live parent attempt, registry and ownership graph while holding
    /// the same admission gate used by executor finalization.
    pub(crate) fn with_session_child<T>(
        &self,
        intents: &[crate::session_transaction::SessionChild],
        roots: &[Task],
        conversation: Uuid,
        f: impl FnOnce(u64, Vec<durable_store::Check>) -> Result<T, crate::runtime::Error>,
    ) -> Result<T, crate::runtime::Error> {
        let state = self
            .state
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        if state.stop
            || roots.iter().any(|task| {
                task.kind()
                    .and_then(|kind| state.policies.get(kind))
                    .copied()
                    != Some((task.replay_safe(), task.definition_version()))
                    || task.owner() != &TaskOwner::Conversation(conversation)
                    || !task.is_fresh_admission()
            })
            || intents.iter().any(|intent| {
                let child = &intent.child;
                child
                    .kind()
                    .and_then(|kind| state.policies.get(kind))
                    .copied()
                    != Some((child.replay_safe(), child.definition_version()))
                    || child.owner() != &TaskOwner::Task(intent.parent.task.id())
                    || !child.is_fresh_admission()
                    || child.id().is_nil()
                    || child.is_background()
                    || child.is_task_tool()
                    || (!intent.staged_parent
                        && (intent
                            .parent
                            .task
                            .kind()
                            .and_then(|kind| state.policies.get(kind))
                            .copied()
                            != Some((
                                intent.parent.task.replay_safe(),
                                intent.parent.task.definition_version(),
                            ))
                            || intent.parent.task.state() != &TaskState::Running
                            || intent.parent.task.abort_requested()
                            || intent.parent.task.attempt_id().is_none()))
            })
        {
            return Err(crate::runtime::Error::Invalid(
                "invalid owned task admission".into(),
            ));
        }
        let _gate = self
            .owner
            .task_gate()
            .lock()
            .map_err(|_| crate::runtime::Error::Failed)?;
        let _publication = self
            .failure_publication
            .admission()
            .map_err(|_| crate::runtime::Error::Failed)?;
        let store = self.owner.store();
        let tasks = TaskStore::new(store);
        if !_publication.permits_admission(&self.owner, conversation, None)
            || intents
                .iter()
                .filter(|intent| !intent.staged_parent)
                .any(|intent| {
                    !_publication.permits_admission(
                        &self.owner,
                        conversation,
                        Some(&intent.parent.task),
                    )
                })
        {
            return Err(crate::runtime::Error::TaskPublicationBusy);
        }
        let boundary = store.put_version()?;
        let owner_checks = if crate::ownership_edge::EdgeStore::new(store)
            .get(conversation)
            .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?
            .is_some()
        {
            Self::session_owner_checks(store, conversation, boundary)?
        } else {
            Vec::new()
        };
        let mut candidates = std::collections::HashMap::new();
        for task in roots.iter().chain(
            intents
                .iter()
                .filter(|intent| intent.creating)
                .map(|intent| &intent.child),
        ) {
            if tasks
                .load(task.id())
                .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?
                .is_some()
                || candidates.insert(task.id(), task).is_some()
            {
                return Err(crate::runtime::Error::Invalid(
                    "task ID already exists".into(),
                ));
            }
        }
        for intent in intents {
            let parent = &intent.parent;
            if intent.staged_parent {
                if candidates.get(&parent.task.id()).copied() != Some(&parent.task) {
                    return Err(crate::runtime::Error::Invalid(
                        "staged parent changed".into(),
                    ));
                }
            } else {
                let mut owner = parent.task.owner().clone();
                let mut seen = std::collections::HashSet::from([parent.task.id()]);
                loop {
                    match owner {
                        TaskOwner::Conversation(id) if id == conversation => break,
                        TaskOwner::Conversation(_) => {
                            return Err(crate::runtime::Error::Invalid(
                                "parent belongs to another conversation".into(),
                            ));
                        }
                        TaskOwner::Task(id) => {
                            if !seen.insert(id) {
                                return Err(crate::runtime::Error::Invalid(
                                    "cyclic task ownership".into(),
                                ));
                            }
                            let ancestor = tasks
                                .load(id)
                                .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?
                                .ok_or_else(|| {
                                    crate::runtime::Error::Invalid("missing parent ancestor".into())
                                })?;
                            if ancestor.task.abort_requested()
                                || ancestor.task.is_terminal()
                                || matches!(ancestor.task.state(), TaskState::CompletingFailed(_))
                            {
                                return Err(crate::runtime::Error::Invalid(
                                    "parent ancestor is closed or aborting".into(),
                                ));
                            }
                            owner = ancestor.task.owner().clone();
                        }
                    }
                }
                if tasks
                    .load(parent.task.id())
                    .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?
                    .as_ref()
                    != Some(parent)
                    || crate::task_scheduler::pending_abort_root_for_running(
                        &tasks,
                        parent.task.id(),
                        false,
                    )
                    .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?
                    .is_some()
                {
                    return Err(crate::runtime::Error::Invalid(
                        "parent attempt changed or is aborting".into(),
                    ));
                }
            }
            if intent.creating {
                crate::task_scheduler::validate_wait_graph(
                    store,
                    parent.task.id(),
                    &[intent.child.id()],
                    &candidates,
                )
                .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?;
            }
        }
        // A fresh ID can already have orphaned descendants in the store. Check
        // every new owner, including roots, against those descendants too.
        let stored = tasks
            .list()
            .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?;
        let edges = crate::ownership_edge::EdgeStore::new(store);
        for owner in candidates.values() {
            let mut targets: Vec<Uuid> = stored
                .iter()
                .map(|item| &item.task)
                .chain(candidates.values().copied())
                .filter(|child| {
                    child.owner() == &TaskOwner::Task(owner.id()) && !child.is_background()
                })
                .map(|child| child.id())
                .collect();
            for edge in edges
                .children(owner.id())
                .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?
            {
                if edge.edge.task_owner != *owner.owner() {
                    return Err(crate::runtime::Error::Invalid(
                        "invalid owned conversation edge".into(),
                    ));
                }
                if edge.edge.mode == crate::ownership_edge::Mode::Foreground {
                    targets.extend(
                        stored
                            .iter()
                            .map(|item| &item.task)
                            .chain(candidates.values().copied())
                            .filter(|child| {
                                child.owner() == &TaskOwner::Conversation(edge.edge.child)
                                    && !child.is_background()
                            })
                            .map(|child| child.id()),
                    );
                }
            }
            if !targets.is_empty() {
                crate::task_scheduler::validate_wait_graph(
                    store,
                    owner.id(),
                    &targets,
                    &candidates,
                )
                .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))?;
            }
        }
        let result = f(boundary, owner_checks);
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
        let _publication = self
            .failure_publication
            .admission()
            .map_err(|_| crate::runtime::Error::Failed)?;
        if !_publication.permits_admission(&self.owner, conversation, None) {
            return Err(crate::runtime::Error::TaskPublicationBusy);
        }
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
    fn panic_cleanup_commit_error_retains_parent_and_child_outcomes() {
        use crate::task_scheduler::{AdmissionHandle, Handler, Step};
        use std::{
            sync::atomic::{AtomicUsize, Ordering},
            time::Duration,
        };
        struct Parent {
            child: Arc<crate::task_tool::TaskTool>,
            admitted: mpsc::Sender<Uuid>,
            proceed: Mutex<mpsc::Receiver<()>>,
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
                let id = admission
                    .admit_task_tool_child(&self.child, "child", json!({}))
                    .unwrap();
                self.admitted.send(id).unwrap();
                self.proceed.lock().unwrap().recv().unwrap();
                panic!("failure before cleanup commit")
            }
        }
        for boundary in [1, 2] {
            let dir = tempfile::tempdir().unwrap();
            let effects = Arc::new(AtomicUsize::new(0));
            let effect_counter = effects.clone();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let release_rx = Mutex::new(release_rx);
            let child_handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
                effect_counter.fetch_add(1, Ordering::SeqCst);
                entered_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
                Ok(Step::Succeed(json!("child")))
            });
            let child = Arc::new(crate::task_tool::TaskTool::new(
                pi_rs_core::ToolSpec {
                    name: "child".into(),
                    description: "".into(),
                    parameters: json!({"type":"object"}),
                },
                "child",
                false,
                1,
                child_handler.clone(),
            ));
            let (admitted_tx, admitted_rx) = mpsc::channel();
            let (proceed_tx, proceed_rx) = mpsc::channel();
            let parent_handler: Arc<dyn Handler> = Arc::new(Parent {
                child,
                admitted: admitted_tx,
                proceed: Mutex::new(proceed_rx),
            });
            let mut registry = task_scheduler::Registry::default();
            registry.register_shared("parent", false, parent_handler);
            registry.register_shared("child", false, child_handler);
            let mut scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
            let parent_id = Uuid::new_v4();
            let parent = Task::new(
                parent_id,
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "start".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("parent".into(), false)
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            scheduler.inject_failure_commit_error_for_test(boundary);
            let driver = Driver::start(scheduler);
            let child_id = admitted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            proceed_tx.send(()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !driver.shared.state.lock().unwrap().stop {
                assert!(
                    std::time::Instant::now() < deadline,
                    "injected failure did not stop driver"
                );
                thread::yield_now();
            }
            release_tx.send(()).unwrap();
            let report = driver.shutdown_with_pending().unwrap();
            assert!(report.failure().is_some());
            assert!(
                report
                    .pending()
                    .iter()
                    .any(|p| p.task_id() == parent_id && p.failure_reason().is_some())
            );
            assert!(
                report
                    .pending()
                    .iter()
                    .any(|p| p.task_id() == child_id && p.completed().is_some())
            );
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn panic_after_admission_aborts_and_joins_running_child() {
        use crate::document::{Address, Documents};
        use crate::task_scheduler::{AdmissionHandle, Handler, Step};
        use std::{
            sync::atomic::{AtomicUsize, Ordering},
            time::Duration,
        };
        struct Parent {
            child: Arc<crate::task_tool::TaskTool>,
            admitted: mpsc::Sender<Uuid>,
            panic_now: Mutex<mpsc::Receiver<()>>,
            address: Address,
        }
        impl Handler for Parent {
            fn run(&self, _: &Task) -> Result<Step, String> {
                unreachable!()
            }
            fn run_with_admission(
                &self,
                _: &Task,
                documents: &mut crate::document::TaskDocuments<'_>,
                admission: &AdmissionHandle<'_>,
            ) -> Result<Step, String> {
                documents.get(&self.address).unwrap();
                documents
                    .put(
                        &self.address,
                        json!({"staged": true}).as_object().unwrap().clone(),
                    )
                    .unwrap();
                // Panic must take priority over a replayable input conflict.
                documents.task_input_conflict = Some(b"test/conflict".to_vec());
                let id = admission
                    .admit_task_tool_child(&self.child, "child", json!({}))
                    .unwrap();
                self.admitted.send(id).unwrap();
                self.panic_now.lock().unwrap().recv().unwrap();
                panic!("parent failed after admission")
            }
        }
        struct Stub;
        #[async_trait::async_trait]
        impl pi_rs_core::Model for Stub {
            async fn stream(
                &self,
                _: pi_rs_core::ModelRequest,
            ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
                unreachable!()
            }
        }
        for attached in [false, true] {
            for parent_replay_safe in [false, true] {
                for shutdown_early in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("db");
                    let effects = Arc::new(AtomicUsize::new(0));
                    let (child_entered_tx, child_entered_rx) = mpsc::channel();
                    let (child_release_tx, child_release_rx) = mpsc::channel();
                    let child_release_rx = Mutex::new(child_release_rx);
                    let recorded = effects.clone();
                    let child_handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
                        recorded.fetch_add(1, Ordering::SeqCst);
                        child_entered_tx.send(()).unwrap();
                        child_release_rx.lock().unwrap().recv().unwrap();
                        Ok(Step::Succeed(json!("external work returned")))
                    });
                    let child = Arc::new(crate::task_tool::TaskTool::new(
                        pi_rs_core::ToolSpec {
                            name: "child".into(),
                            description: "".into(),
                            parameters: json!({"type":"object"}),
                        },
                        "child",
                        false,
                        1,
                        child_handler.clone(),
                    ));
                    let (admitted_tx, admitted_rx) = mpsc::channel();
                    let (panic_tx, panic_rx) = mpsc::channel();
                    let address = Address {
                        scope: "test".into(),
                        name: "panic-staged".into(),
                        key: "".into(),
                    };
                    let parent_handler: Arc<dyn Handler> = Arc::new(Parent {
                        child,
                        admitted: admitted_tx,
                        panic_now: Mutex::new(panic_rx),
                        address: address.clone(),
                    });
                    let mut registry = task_scheduler::Registry::default();
                    registry.register_shared("parent", parent_replay_safe, parent_handler);
                    registry.register_shared("child", false, child_handler);
                    let aborts = Arc::new(AtomicUsize::new(0));
                    let abort_count = aborts.clone();
                    assert!(registry.set_abort("child", move |_: &Task| {
                        abort_count.fetch_add(1, Ordering::SeqCst);
                        Ok(Step::Succeed(json!(null)))
                    }));
                    let (scheduler, conversation, harness) = if attached {
                        let mut harness = crate::runtime::Harness::open_paused(
                            &path,
                            crate::runtime::Options {
                                model: Arc::new(Stub),
                                tools: vec![],
                                restart_limit: 3,
                                max_turns: 3,
                                uncertain_policy:
                                    crate::recovery::UncertainPolicy::RequireReconciliation,
                            },
                        )
                        .unwrap();
                        let conversation = harness
                            .create_conversation(crate::runtime::Config {
                                model: "test".into(),
                                system_prompt: "".into(),
                                max_output_tokens: 10,
                            })
                            .unwrap();
                        let scheduler = harness.attach_tasks(registry).unwrap();
                        (scheduler, conversation, Some(harness))
                    } else {
                        (
                            task_scheduler::Scheduler::open(&path, registry).unwrap(),
                            Uuid::new_v4(),
                            None,
                        )
                    };
                    // Recovery scans task keys in order. Keep the unsafe child before its
                    // parent to exercise interruption before the parent's cleanup cascade.
                    let parent_id = Uuid::from_u128(u128::MAX);
                    let parent = Task::new(
                        parent_id,
                        TaskOwner::Conversation(conversation),
                        Checkpoint {
                            phase: "start".into(),
                            data: json!(null),
                        },
                        3,
                    )
                    .unwrap()
                    .registered("parent".into(), parent_replay_safe)
                    .into_task_tool();
                    scheduler.submit(&parent).unwrap();
                    let owner = scheduler.owner();
                    let driver = Driver::start(scheduler);
                    let child_id = admitted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    child_entered_rx
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                    assert_eq!(effects.load(Ordering::SeqCst), 1);
                    panic_tx.send(()).unwrap();
                    let deadline = std::time::Instant::now() + Duration::from_secs(5);
                    let tasks = TaskStore::new(owner.store());
                    while !tasks
                        .load(child_id)
                        .unwrap()
                        .unwrap()
                        .task
                        .abort_requested()
                    {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "panic did not mark the running child for abort"
                        );
                        thread::yield_now();
                    }
                    let parent = tasks.load(parent_id).unwrap().unwrap().task;
                    let child = tasks.load(child_id).unwrap().unwrap().task;
                    assert!(matches!(
                        parent.state(),
                        crate::task::TaskState::CompletingFailed(_)
                    ));
                    assert_eq!(child.state(), &crate::task::TaskState::Running);
                    assert!(child.abort_requested());
                    assert_eq!(aborts.load(Ordering::SeqCst), 0);
                    assert_eq!(effects.load(Ordering::SeqCst), 1);
                    assert!(
                        Documents::new(owner.store())
                            .get(&address)
                            .unwrap()
                            .is_none()
                    );
                    let report = if shutdown_early {
                        let (started_tx, started_rx) = mpsc::channel();
                        let (report_tx, report_rx) = mpsc::channel();
                        let stop = thread::spawn(move || {
                            started_tx.send(()).unwrap();
                            report_tx.send(driver.shutdown_with_pending()).unwrap();
                        });
                        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                        assert!(
                            matches!(
                                report_rx.recv_timeout(Duration::from_millis(100)),
                                Err(mpsc::RecvTimeoutError::Timeout)
                            ),
                            "shutdown returned while child executor was running"
                        );
                        child_release_tx.send(()).unwrap();
                        let report = report_rx
                            .recv_timeout(Duration::from_secs(5))
                            .unwrap()
                            .unwrap();
                        stop.join().unwrap();
                        report
                    } else {
                        child_release_tx.send(()).unwrap();
                        while !tasks.load(parent_id).unwrap().unwrap().task.is_terminal() {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "cleanup did not settle"
                            );
                            thread::yield_now();
                        }
                        let parent = tasks.load(parent_id).unwrap().unwrap().task;
                        let child = tasks.load(child_id).unwrap().unwrap().task;
                        assert!(
                            matches!(parent.state(), crate::task::TaskState::Failed(reason)
                        if reason.contains("panicked") && !reason.contains("parent failed after admission"))
                        );
                        assert_eq!(child.state(), &crate::task::TaskState::Cancelled);
                        assert_eq!(aborts.load(Ordering::SeqCst), 1);
                        driver.shutdown_with_pending().unwrap()
                    };
                    assert!(report.failure().is_none());
                    assert!(report.pending().is_empty());
                    assert_eq!(effects.load(Ordering::SeqCst), 1);
                    drop(report);
                    drop(owner);
                    drop(harness);
                    let child_replays = effects.clone();
                    let mut registry = task_scheduler::Registry::default();
                    registry.register(
                        "parent",
                        parent_replay_safe,
                        |_: &Task| -> Result<Step, String> { panic!("completed parent replayed") },
                    );
                    registry.register("child", false, move |_: &Task| {
                        child_replays.fetch_add(1, Ordering::SeqCst);
                        Ok(Step::Succeed(json!("unexpected replay")))
                    });
                    let abort_count = aborts.clone();
                    registry.set_abort("child", move |_: &Task| {
                        abort_count.fetch_add(1, Ordering::SeqCst);
                        Ok(Step::Succeed(json!(null)))
                    });
                    let mut recovered = task_scheduler::Scheduler::open(&path, registry).unwrap();
                    for _ in 0..20 {
                        if recovered.get(parent_id).unwrap().unwrap().is_terminal() {
                            break;
                        }
                        recovered.tick(0).unwrap();
                    }
                    assert!(
                        matches!(recovered.get(parent_id).unwrap().unwrap().state(), crate::task::TaskState::Failed(reason) if reason.contains("panicked"))
                    );
                    assert_eq!(
                        recovered.get(child_id).unwrap().unwrap().state(),
                        &crate::task::TaskState::Cancelled
                    );
                    assert_eq!(aborts.load(Ordering::SeqCst), 1);
                    assert_eq!(effects.load(Ordering::SeqCst), 1);
                }
            }
        }
    }

    #[test]
    fn driver_passes_live_admission_to_handler_and_fences_abort() {
        use crate::task_scheduler::{AdmissionHandle, Handler, Step};
        use std::time::Duration;
        struct Parent {
            child: Arc<crate::task_tool::TaskTool>,
            admitted: mpsc::Sender<Uuid>,
            proceed: Mutex<mpsc::Receiver<()>>,
            after: mpsc::Sender<bool>,
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
                let ids = std::thread::scope(|scope| {
                    let a = scope.spawn(|| {
                        admission
                            .admit_task_tool_child(&self.child, "child", json!({}))
                            .unwrap()
                    });
                    let b = scope.spawn(|| {
                        admission
                            .admit_task_tool_child(&self.child, "child", json!({}))
                            .unwrap()
                    });
                    (a.join().unwrap(), b.join().unwrap())
                });
                assert_eq!(ids.0, ids.1);
                self.admitted.send(ids.0).unwrap();
                self.proceed.lock().unwrap().recv().unwrap();
                let rejected = admission
                    .admit_task_tool_child(&self.child, "child", json!({}))
                    .is_err();
                self.after.send(rejected).unwrap();
                Ok(Step::Succeed(json!("parent")))
            }
        }
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (child_started_tx, child_started_rx) = mpsc::channel();
            let (child_release_tx, child_release_rx) = mpsc::channel();
            let child_release_rx = Mutex::new(child_release_rx);
            let child_handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
                child_started_tx.send(()).unwrap();
                child_release_rx.lock().unwrap().recv().unwrap();
                Ok(Step::Succeed(json!("child")))
            });
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
            let (admitted_tx, admitted_rx) = mpsc::channel();
            let (proceed_tx, proceed_rx) = mpsc::channel();
            let (after_tx, after_rx) = mpsc::channel();
            let parent_handler: Arc<dyn Handler> = Arc::new(Parent {
                child,
                admitted: admitted_tx,
                proceed: Mutex::new(proceed_rx),
                after: after_tx,
            });
            let mut registry = task_scheduler::Registry::default();
            registry.register_shared("parent", true, parent_handler);
            registry.register_shared("child", true, child_handler);
            let scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
            let id = Uuid::new_v4();
            let parent = Task::new(
                id,
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "start".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("parent".into(), true)
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
            let driver = Driver::start(scheduler);
            let child_id = admitted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            child_started_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            assert_eq!(
                driver.get(child_id).unwrap().unwrap().owner(),
                &TaskOwner::Task(id)
            );
            if abort {
                driver.request_abort(id).unwrap();
            }
            proceed_tx.send(()).unwrap();
            assert_eq!(
                after_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
                abort
            );
            child_release_tx.send(()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let parent = driver.get(id).unwrap().unwrap();
                if parent.is_terminal() {
                    assert!(driver.get(child_id).unwrap().unwrap().is_terminal());
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "parent did not settle"
                );
                std::thread::yield_now();
            }
            driver.shutdown().unwrap();
        }
    }

    #[test]
    fn driver_runs_admitted_task_tool_child_while_parent_handler_is_live() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let (parent_entered_tx, parent_entered_rx) = mpsc::channel();
        let (parent_release_tx, parent_release_rx) = mpsc::channel();
        let parent_release_rx = Mutex::new(parent_release_rx);
        let parent_handler: Arc<dyn task_scheduler::Handler> = Arc::new(move |_: &Task| {
            parent_entered_tx.send(()).unwrap();
            parent_release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("parent")))
        });
        let (child_entered_tx, child_entered_rx) = mpsc::channel();
        let (child_release_tx, child_release_rx) = mpsc::channel();
        let child_release_rx = Mutex::new(child_release_rx);
        let child_handler: Arc<dyn task_scheduler::Handler> = Arc::new(move |_: &Task| {
            child_entered_tx.send(()).unwrap();
            child_release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("child")))
        });
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
        let mut registry = task_scheduler::Registry::default();
        assert!(registry.register_shared("parent", true, parent_handler));
        assert!(registry.register_shared("child", true, child_handler));
        let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        let parent_id = Uuid::new_v4();
        let parent = Task::new(
            parent_id,
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), true)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let (admitted_tx, admitted_rx) = mpsc::channel();
        before_execute_hooks().lock().unwrap().insert(
            parent_id,
            Box::new(move |invocation| {
                admitted_tx
                    .send(invocation.admit_task_tool_child(&child, "nested", json!({})))
                    .unwrap();
            }),
        );
        let driver = Driver::start(scheduler);
        let child_id = admitted_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        parent_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        child_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let tasks = TaskStore::new(driver.shared.owner.store());
        assert_eq!(
            tasks.load(parent_id).unwrap().unwrap().task.state(),
            &crate::task::TaskState::Running
        );
        assert_eq!(
            tasks.load(child_id).unwrap().unwrap().task.state(),
            &crate::task::TaskState::Running
        );
        child_release_tx.send(()).unwrap();
        parent_release_tx.send(()).unwrap();
        driver.shutdown().unwrap();
    }

    #[test]
    fn task_tool_child_waits_for_ordinary_worker_capacity() {
        use std::{collections::HashMap, time::Duration};
        let dir = tempfile::tempdir().unwrap();
        let parents: Vec<_> = (0..4)
            .map(|i| Uuid::from_u128(0x7f_0000_1000 + i))
            .collect();
        let (entered_tx, entered_rx) = mpsc::channel();
        let mut releases = Vec::new();
        let mut receivers = HashMap::new();
        for id in &parents {
            let (tx, rx) = mpsc::channel();
            releases.push(tx);
            receivers.insert(*id, Mutex::new(rx));
        }
        let parent_handler: Arc<dyn task_scheduler::Handler> = Arc::new(move |task: &Task| {
            entered_tx.send(task.id()).unwrap();
            receivers[&task.id()].lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!(null)))
        });
        let (child_tx, child_rx) = mpsc::channel();
        let child_handler: Arc<dyn task_scheduler::Handler> = Arc::new(move |_: &Task| {
            child_tx.send(()).unwrap();
            Ok(task_scheduler::Step::Succeed(json!(null)))
        });
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
        let mut registry = task_scheduler::Registry::default();
        assert!(registry.register_shared("parent", true, parent_handler));
        assert!(registry.register_shared("child", true, child_handler));
        let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        for id in &parents {
            let parent = Task::new(
                *id,
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "start".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("parent".into(), true)
            .into_task_tool();
            scheduler.submit(&parent).unwrap();
        }
        let (admitted_tx, admitted_rx) = mpsc::channel();
        before_execute_hooks().lock().unwrap().insert(
            parents[0],
            Box::new(move |invocation| {
                admitted_tx
                    .send(invocation.admit_task_tool_child(&child, "nested", json!({})))
                    .unwrap();
            }),
        );
        let driver = Driver::start(scheduler);
        let child_id = admitted_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        let mut running = std::collections::HashSet::new();
        for _ in 0..4 {
            running.insert(entered_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        assert_eq!(running.len(), 4);
        let tasks = TaskStore::new(driver.shared.owner.store());
        assert_eq!(
            tasks.load(child_id).unwrap().unwrap().task.state(),
            &crate::task::TaskState::Ready
        );
        // The overflow reservation explicitly excludes TaskTool children.
        assert!(matches!(
            driver
                .shared
                .scheduler
                .lock()
                .unwrap()
                .try_reserve_task_tool_child(0)
                .unwrap(),
            task_scheduler::TickReservation::Progress(false)
        ));
        assert!(matches!(
            child_rx.recv_timeout(Duration::from_millis(150)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        releases[1].send(()).unwrap();
        child_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        for index in [0, 2, 3] {
            releases[index].send(()).unwrap();
        }
        driver.shutdown().unwrap();
    }

    #[test]
    fn owned_session_chain_checks_fence_abort_and_graph_aba() {
        use crate::ownership_edge::{self, Edge, Mode};
        use durable_store::{Check, Expected, Mutation};
        let dir = tempfile::tempdir().unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let root = Uuid::new_v4();
        let child = Uuid::new_v4();
        let ancestor = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(root),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("work".into(), true);
        let owner = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(ancestor.id()),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("work".into(), true);
        TaskStore::new(&store)
            .save(&[Save::Create(&ancestor), Save::Create(&owner)])
            .unwrap();
        let config = crate::runtime::Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let edge = Edge {
            task: owner.id(),
            task_owner: owner.owner().clone(),
            child,
            mode: Mode::Foreground,
        };
        let edge_key = ownership_edge::edge_key(child);
        let root_key = format!("runtime/conversation/{root}").into_bytes();
        let child_key = format!("runtime/conversation/{child}").into_bytes();
        store
            .commit(
                &[],
                &[
                    Mutation::Put {
                        key: root_key,
                        value: serde_json::to_vec(&crate::runtime::Harness::empty_conversation(
                            root,
                            config.clone(),
                        ))
                        .unwrap(),
                    },
                    Mutation::Put {
                        key: child_key,
                        value: serde_json::to_vec(&crate::runtime::Harness::empty_conversation(
                            child, config,
                        ))
                        .unwrap(),
                    },
                    Mutation::Put {
                        key: edge_key.clone(),
                        value: serde_json::to_vec(&edge).unwrap(),
                    },
                ],
            )
            .unwrap();
        let boundary = store.put_version().unwrap();
        let checks = Shared::session_owner_checks(&store, child, boundary).unwrap();
        let (abort_check, abort_write) =
            ownership_edge::prepare_abort_fence(&store, &TaskOwner::Task(ancestor.id())).unwrap();
        store.commit(&[abort_check], &[abort_write]).unwrap();
        assert!(
            store
                .commit_with_prefix_put_fence(
                    &checks,
                    &[],
                    &[task_store::PREFIX, ownership_edge::PREFIX],
                    boundary
                )
                .is_err()
        );
        let boundary = store.put_version().unwrap();
        let checks = Shared::session_owner_checks(&store, child, boundary).unwrap();
        let old = store.get(&edge_key).unwrap().unwrap();
        store
            .commit(
                &[Check {
                    key: edge_key.clone(),
                    expected: Expected::Version(old.version),
                }],
                &[
                    Mutation::Delete {
                        key: edge_key.clone(),
                    },
                    Mutation::Put {
                        key: edge_key,
                        value: old.value,
                    },
                ],
            )
            .unwrap();
        assert!(
            store
                .commit_with_prefix_put_fence(
                    &checks,
                    &[],
                    &[task_store::PREFIX, ownership_edge::PREFIX],
                    boundary
                )
                .is_err()
        );
    }

    #[test]
    fn published_handler_failure_fences_admissions_before_running_record_settles() {
        for outcome in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = task_scheduler::Registry::default();
            registry.register("work", false, move |_: &Task| {
                if outcome == 1 {
                    panic!("test handler panic");
                }
                if outcome == 2 {
                    return Ok(task_scheduler::Step::Fail("test failure".into()));
                }
                Err("test handler error".into())
            });
            let mut scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
            let conversation = Uuid::from_u128(9);
            let owned_conversation = Uuid::from_u128(10);
            let unrelated = Uuid::from_u128(11);
            let make_task = |id, owner| {
                Task::new(
                    Uuid::from_u128(id),
                    owner,
                    Checkpoint {
                        phase: "start".into(),
                        data: json!({}),
                    },
                    3,
                )
                .unwrap()
                .registered("work".into(), false)
            };
            let parent = make_task(1, TaskOwner::Conversation(conversation));
            scheduler.submit(&parent).unwrap();
            let task_scheduler::TickReservation::Invocation(invocation) =
                scheduler.reserve_tick(0).unwrap()
            else {
                panic!("expected invocation")
            };
            let owner = scheduler.owner();
            let snapshot = TaskStore::new(owner.store())
                .load(parent.id())
                .unwrap()
                .unwrap();
            let edge = crate::ownership_edge::Edge {
                task: parent.id(),
                task_owner: TaskOwner::Conversation(conversation),
                child: owned_conversation,
                mode: crate::ownership_edge::Mode::Foreground,
            };
            let mutations = vec![
                durable_store::Mutation::Put {
                    key: crate::ownership_edge::edge_key(owned_conversation),
                    value: serde_json::to_vec(&edge).unwrap(),
                },
                durable_store::Mutation::Put {
                    key: format!("runtime/conversation/{owned_conversation}").into_bytes(),
                    value: serde_json::to_vec(&json!({"abort_requested": false})).unwrap(),
                },
                durable_store::Mutation::Put {
                    key: format!("runtime/conversation/{unrelated}").into_bytes(),
                    value: serde_json::to_vec(&json!({"abort_requested": false})).unwrap(),
                },
            ];
            owner.store().commit(&[], &mutations).unwrap();
            let policies = scheduler.policies();
            let shared = Arc::new(Shared {
                owner,
                state: Mutex::new(State {
                    policies,
                    ..State::default()
                }),
                wake_reader: scheduler.wake_reader(),
                activity: scheduler.worker_activity(),
                registry_gate: Mutex::new(()),
                registry_replacing: std::sync::atomic::AtomicBool::new(false),
                reservation_stop: scheduler.reservation_stop_token(),
                failure_publication: scheduler.failure_publication(),
                scheduler: Mutex::new(scheduler),
                pending: Mutex::new(Vec::new()),
                wake: Condvar::new(),
                harness: None,
                cleanup_stop: pi_rs_core::CancellationToken::new(),
                unhooked_cancellations: Mutex::new(HashMap::new()),
                notifications: Mutex::new(None),
                subscription: Mutex::new(None),
            });
            let driver = Driver {
                shared: shared.clone(),
                worker: None,
                cleanup: None,
            };
            let direct = make_task(2, TaskOwner::Task(parent.id()));
            let before = crate::session_transaction::SessionChild::test_intent(
                snapshot.clone(),
                direct.clone(),
            );
            assert!(
                shared
                    .with_session_child(&[before], &[], conversation, |_, _| Ok(()))
                    .is_ok()
            );
            assert!(
                shared
                    .with_admission(&[], owned_conversation, || Ok(()))
                    .is_ok()
            );
            let publication = shared.failure_publication.try_lock().unwrap();
            let contender = shared.clone();
            let unrelated_task = make_task(5, TaskOwner::Conversation(unrelated));
            let (sent, received) = mpsc::channel();
            let thread = thread::spawn(move || {
                let result = contender.with_admission(
                    std::slice::from_ref(&unrelated_task),
                    unrelated,
                    || {
                        TaskStore::new(contender.owner.store())
                            .save(&[Save::Create(&unrelated_task)])
                            .map_err(|e| crate::runtime::Error::Invalid(e.to_string()))
                    },
                );
                sent.send(result.is_ok()).unwrap();
            });
            assert!(received.recv_timeout(Duration::from_secs(5)).unwrap());
            drop(publication);
            thread.join().unwrap();
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| invocation.execute()));
            assert_eq!(result.is_err(), outcome == 1);
            assert_eq!(
                TaskStore::new(shared.owner.store())
                    .load(parent.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .state(),
                &TaskState::Running
            );
            let intent = crate::session_transaction::SessionChild::test_intent(snapshot, direct);
            assert!(
                shared
                    .with_session_child(&[intent], &[], conversation, |_, _| Ok(()))
                    .is_err()
            );
            let child = make_task(3, TaskOwner::Conversation(owned_conversation));
            assert!(driver.submit(&child).is_err());
            assert!(
                shared
                    .with_session_child(
                        &[],
                        std::slice::from_ref(&child),
                        owned_conversation,
                        |_, _| Ok(())
                    )
                    .is_err()
            );
            assert!(
                shared
                    .with_admission(&[child], owned_conversation, || Ok(()))
                    .is_err()
            );
            assert!(
                shared
                    .with_hook_children(&[], &[], owned_conversation, || Ok(()))
                    .is_err()
            );
            let independent = make_task(4, TaskOwner::Conversation(unrelated));
            driver.submit(&independent).unwrap();
            assert!(shared.with_admission(&[], unrelated, || Ok(())).is_ok());
            let first = shared.clone();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let one = thread::spawn(move || {
                first.with_admission(&[], unrelated, || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(())
                })
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let second = shared.clone();
            let (attempt_tx, attempt_rx) = mpsc::channel();
            let two = thread::spawn(move || {
                attempt_tx.send(()).unwrap();
                second.with_admission(&[], unrelated, || Ok(()))
            });
            attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            release_tx.send(()).unwrap();
            assert!(one.join().unwrap().is_ok());
            assert!(two.join().unwrap().is_ok());
        }
    }

    #[test]
    fn replacement_drains_pool_before_starting_more_handlers() {
        let dir = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (fifth_tx, fifth_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let gate = release.clone();
        let old_fifth = fifth_tx.clone();
        let mut registry = task_scheduler::Registry::default();
        registry.register("work", false, move |task: &Task| {
            if task.id() == Uuid::from_u128(5) {
                old_fifth.send("old").unwrap();
                return Ok(task_scheduler::Step::Succeed(json!(null)));
            }
            entered_tx.send(task.id()).unwrap();
            let (lock, wake) = &*gate;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = wake.wait(ready).unwrap();
            }
            Ok(task_scheduler::Step::Succeed(json!(null)))
        });
        let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        for id in 1..=5 {
            scheduler
                .submit(
                    &Task::new(
                        Uuid::from_u128(id),
                        TaskOwner::Conversation(Uuid::from_u128(id + 10)),
                        Checkpoint {
                            phase: "start".into(),
                            data: json!({}),
                        },
                        3,
                    )
                    .unwrap()
                    .registered("work".into(), false),
                )
                .unwrap();
        }
        let driver = Arc::new(Driver::start(scheduler));
        for _ in 0..4 {
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        let replacing = driver.clone();
        let replacement = thread::spawn(move || {
            let mut registry = task_scheduler::Registry::default();
            registry.register("work", false, move |_: &Task| {
                fifth_tx.send("new").unwrap();
                Ok(task_scheduler::Step::Succeed(json!(null)))
            });
            replacing.replace_registry(registry)
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !driver
            .shared
            .registry_replacing
            .load(std::sync::atomic::Ordering::Acquire)
        {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        replacement.join().unwrap().unwrap();
        assert_eq!(
            fifth_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            "new"
        );
        Arc::try_unwrap(driver).ok().unwrap().shutdown().unwrap();
    }

    #[test]
    fn bounded_workers_overlap_and_shutdown_joins_both() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let gate = release.clone();
        let mut registry = task_scheduler::Registry::default();
        registry.register("work", false, move |task: &Task| {
            count.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(task.id()).unwrap();
            let (lock, wake) = &*gate;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = wake.wait(ready).unwrap();
            }
            Ok(task_scheduler::Step::Succeed(json!("done")))
        });
        let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = scheduler.owner();
        for id in [1, 2] {
            let task = Task::new(
                Uuid::from_u128(id),
                TaskOwner::Conversation(Uuid::from_u128(10 + id)),
                Checkpoint {
                    phase: "start".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap()
            .registered("work".into(), false);
            scheduler.submit(&task).unwrap();
        }
        let driver = Driver::start(scheduler);
        let mut entered = [
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        ];
        entered.sort();
        assert_eq!(entered, [Uuid::from_u128(1), Uuid::from_u128(2)]);
        let (done_tx, done_rx) = mpsc::channel();
        let shutdown = thread::spawn(move || done_tx.send(driver.shutdown_with_pending()).unwrap());
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(150)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let report = done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        shutdown.join().unwrap();
        assert!(report.failure().is_none());
        assert!(report.pending().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        for id in [1, 2] {
            assert_eq!(
                TaskStore::new(owner.store())
                    .load(Uuid::from_u128(id))
                    .unwrap()
                    .unwrap()
                    .task
                    .state(),
                &TaskState::Succeeded(json!("done"))
            );
        }
    }

    #[test]
    fn bounded_workers_progress_independent_root_during_busy_graph_cleanup() {
        use crate::document::{Address, Documents, TaskDocuments};
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
        struct Blocked {
            entered: mpsc::Sender<Uuid>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }
        impl task_scheduler::Handler for Blocked {
            fn run(&self, _: &Task) -> Result<task_scheduler::Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                task: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<task_scheduler::Step, String> {
                let address = Address {
                    scope: "pool-busy".into(),
                    name: "doc".into(),
                    key: task.id().to_string(),
                };
                docs.get(&address).unwrap();
                docs.put(
                    &address,
                    json!({"value":"discard"}).as_object().unwrap().clone(),
                )
                .unwrap();
                self.entered.send(task.id()).unwrap();
                let (lock, wake) = &*self.release;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    ready = wake.wait(ready).unwrap();
                }
                Ok(task_scheduler::Step::Succeed(json!("discard")))
            }
        }
        for mode in ["failfast", "tasktool"] {
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
            let (other_tx, other_rx) = mpsc::channel();
            let release = Arc::new((Mutex::new(false), Condvar::new()));
            let mut registry = task_scheduler::Registry::default();
            registry.register("parent", false, |_: &Task| {
                Ok(task_scheduler::Step::Succeed(json!(null)))
            });
            registry.register_with_abort(
                "nested",
                false,
                Blocked {
                    entered: entered_tx,
                    release: release.clone(),
                },
                |_: &Task| Ok(task_scheduler::Step::Succeed(json!(null))),
            );
            registry.register("other", false, move |_: &Task| {
                other_tx.send(()).unwrap();
                Ok(task_scheduler::Step::Succeed(json!("other")))
            });
            let replacement_registry = registry.clone();
            let scheduler = harness.attach_tasks(registry).unwrap();
            let owner = scheduler.owner();
            let cp = Checkpoint {
                phase: "start".into(),
                data: json!({}),
            };
            let mut parent = Task::new(
                Uuid::from_u128(1),
                TaskOwner::Conversation(Uuid::from_u128(9)),
                cp.clone(),
                3,
            )
            .unwrap()
            .registered("parent".into(), false);
            if mode == "tasktool" {
                parent = parent.into_task_tool();
            }
            parent.start().unwrap();
            if mode == "failfast" {
                parent
                    .wait(WaitFor::TasksFailFast(vec![
                        Uuid::from_u128(2),
                        Uuid::from_u128(3),
                    ]))
                    .unwrap();
            } else {
                parent.wait(WaitFor::Signal("hold".into())).unwrap();
            }
            let mut a = Task::new(
                Uuid::from_u128(2),
                TaskOwner::Task(parent.id()),
                cp.clone(),
                3,
            )
            .unwrap()
            .registered("nested".into(), false);
            let mut b = Task::new(
                Uuid::from_u128(3),
                TaskOwner::Task(parent.id()),
                cp.clone(),
                3,
            )
            .unwrap()
            .registered(
                if mode == "tasktool" {
                    "nested"
                } else {
                    "missing"
                }
                .into(),
                false,
            );
            if mode == "tasktool" {
                a = a.into_task_tool();
                b = b.into_task_tool();
            }
            let tasks = TaskStore::new(owner.store());
            tasks
                .save(&[Save::Create(&parent), Save::Create(&a), Save::Create(&b)])
                .unwrap();
            let driver = Arc::new(Driver::start_with_harness(scheduler, harness.worker()));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            if mode == "tasktool" {
                entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            let control = owner.conversation_controls().get(Uuid::from_u128(99));
            let poll = control.gate.lock().unwrap();
            if mode == "failfast" {
                let current = tasks.load(b.id()).unwrap().unwrap();
                tasks
                    .update(&current, |task| {
                        task.start()?;
                        task.fail("failed".into())
                    })
                    .unwrap();
            } else {
                let current = tasks.load(parent.id()).unwrap().unwrap();
                tasks
                    .update(&current, |task| {
                        task.wake()?;
                        task.start()?;
                        task.hold_success(json!("parent"))
                    })
                    .unwrap();
            }
            {
                let (lock, wake) = &*release;
                *lock.lock().unwrap() = true;
                wake.notify_all();
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while driver.shared.pending.lock().unwrap().is_empty() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "no pending outcome: {mode}"
                );
                thread::yield_now();
            }
            let replacing = driver.clone();
            let (replaced_tx, replaced_rx) = mpsc::channel();
            let replacement = thread::spawn(move || {
                replaced_tx
                    .send(replacing.replace_registry(replacement_registry))
                    .unwrap()
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !driver
                .shared
                .registry_replacing
                .load(std::sync::atomic::Ordering::Acquire)
            {
                assert!(std::time::Instant::now() < deadline);
                thread::yield_now();
            }
            let other = Task::new(
                Uuid::from_u128(4),
                TaskOwner::Conversation(Uuid::from_u128(10)),
                cp,
                3,
            )
            .unwrap()
            .registered("other".into(), false);
            {
                let _gate = owner.task_gate().lock().unwrap();
                tasks.save(&[Save::Create(&other)]).unwrap();
            }
            driver.shared.notify_work();
            other_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("independent root did not execute");
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if matches!(
                    tasks.load(other.id()).unwrap().unwrap().task.state(),
                    TaskState::Succeeded(_)
                ) {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "independent result stalled: {mode}"
                );
                thread::yield_now();
            }
            let address = Address {
                scope: "pool-busy".into(),
                name: "doc".into(),
                key: a.id().to_string(),
            };
            assert!(
                Documents::new(owner.store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
            assert!(matches!(
                replaced_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            drop(poll);
            replaced_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            replacement.join().unwrap();
            let report = Arc::try_unwrap(driver)
                .ok()
                .unwrap()
                .shutdown_with_pending()
                .unwrap();
            assert!(
                report.failure().is_none(),
                "{}",
                report.failure().unwrap_or("")
            );
            assert!(report.pending().is_empty());
            assert!(
                Documents::new(owner.store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
            assert!(!matches!(
                tasks.load(a.id()).unwrap().unwrap().task.state(),
                TaskState::Succeeded(_)
            ));
        }
    }

    #[test]
    fn bounded_workers_retain_results_after_settlement_panic() {
        let dir = tempfile::tempdir().unwrap();
        let (a_enter, a_seen) = mpsc::channel();
        let (a_go, a_wait) = mpsc::channel();
        let (b_enter, b_seen) = mpsc::channel();
        let (b_go, b_wait) = mpsc::channel();
        let a_wait = Mutex::new(a_wait);
        let b_wait = Mutex::new(b_wait);
        let mut registry = task_scheduler::Registry::default();
        registry.register("a", false, move |_: &Task| {
            a_enter.send(()).unwrap();
            a_wait.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("a")))
        });
        registry.register("b", false, move |_: &Task| {
            b_enter.send(()).unwrap();
            b_wait.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("b")))
        });
        let mut scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        for (id, kind) in [(1, "a"), (2, "b")] {
            scheduler
                .submit(
                    &Task::new(
                        Uuid::from_u128(id),
                        TaskOwner::Conversation(Uuid::from_u128(id + 10)),
                        Checkpoint {
                            phase: "start".into(),
                            data: json!({}),
                        },
                        3,
                    )
                    .unwrap()
                    .registered(kind.into(), false),
                )
                .unwrap();
        }
        scheduler.set_result_commit_hook(Arc::new(|_, _| panic!("settlement panic")));
        let driver = Driver::start(scheduler);
        a_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        b_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        a_go.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !driver.shared.state.lock().unwrap().stop {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        b_go.send(()).unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_some());
        assert_eq!(report.pending().len(), 2);
        for id in [1, 2] {
            assert!(report.pending().iter().any(|item| item.task_id() == Uuid::from_u128(id) && item.completed().is_some()));
        }
    }

    #[test]
    fn bounded_workers_drain_independent_result_after_handler_panic() {
        use crate::document::{Address, TaskDocuments};
        struct Staged {
            address: Address,
            entered: mpsc::Sender<()>,
            go: Mutex<mpsc::Receiver<()>>,
        }
        impl task_scheduler::Handler for Staged {
            fn run(&self, _: &Task) -> Result<task_scheduler::Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<task_scheduler::Step, String> {
                docs.get(&self.address).unwrap();
                docs.put(
                    &self.address,
                    json!({"value": "kept"}).as_object().unwrap().clone(),
                )
                .unwrap();
                self.entered.send(()).unwrap();
                self.go.lock().unwrap().recv().unwrap();
                Ok(task_scheduler::Step::Succeed(json!("kept")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let (a_enter, a_seen) = mpsc::channel();
        let (a_go, a_wait) = mpsc::channel();
        let (b_enter, b_seen) = mpsc::channel();
        let (b_go, b_wait) = mpsc::channel();
        let a_wait = Mutex::new(a_wait);
        let address = Address {
            scope: "pool".into(),
            name: "panic".into(),
            key: "".into(),
        };
        let mut registry = task_scheduler::Registry::default();
        registry.register(
            "a",
            false,
            move |_: &Task| -> Result<task_scheduler::Step, String> {
                a_enter.send(()).unwrap();
                a_wait.lock().unwrap().recv().unwrap();
                panic!("handler panic")
            },
        );
        registry.register(
            "b",
            false,
            Staged {
                address: address.clone(),
                entered: b_enter,
                go: Mutex::new(b_wait),
            },
        );
        let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = scheduler.owner();
        for (id, kind) in [(1, "a"), (2, "b")] {
            scheduler
                .submit(
                    &Task::new(
                        Uuid::from_u128(id),
                        TaskOwner::Conversation(Uuid::from_u128(id + 10)),
                        Checkpoint {
                            phase: "start".into(),
                            data: json!({}),
                        },
                        3,
                    )
                    .unwrap()
                    .registered(kind.into(), false),
                )
                .unwrap();
        }
        let driver = Driver::start(scheduler);
        a_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        b_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        a_go.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !driver.shared.state.lock().unwrap().stop {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        b_go.send(()).unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_some());
        assert!(report.pending().is_empty());
        assert_eq!(
            crate::document::Documents::new(owner.store())
                .get(&address)
                .unwrap()
                .unwrap()
                .value["value"],
            json!("kept")
        );
    }

    #[test]
    fn bounded_workers_drain_known_failure_and_retain_unknown_results() {
        for unknown in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (a_enter_tx, a_enter_rx) = mpsc::channel();
            let (a_go_tx, a_go_rx) = mpsc::channel();
            let (b_enter_tx, b_enter_rx) = mpsc::channel();
            let (b_go_tx, b_go_rx) = mpsc::channel();
            let a_go_rx = Mutex::new(a_go_rx);
            let b_go_rx = Mutex::new(b_go_rx);
            let mut registry = task_scheduler::Registry::default();
            registry.register("a", false, move |_: &Task| {
                a_enter_tx.send(()).unwrap();
                a_go_rx.lock().unwrap().recv().unwrap();
                if unknown {
                    Ok(task_scheduler::Step::Succeed(json!("a")))
                } else {
                    Err("handler failed".into())
                }
            });
            registry.register("b", false, move |_: &Task| {
                b_enter_tx.send(()).unwrap();
                b_go_rx.lock().unwrap().recv().unwrap();
                Ok(task_scheduler::Step::Succeed(json!("b")))
            });
            let mut scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
            let owner = scheduler.owner();
            for (id, kind) in [(1, "a"), (2, "b")] {
                scheduler
                    .submit(
                        &Task::new(
                            Uuid::from_u128(id),
                            TaskOwner::Conversation(Uuid::from_u128(id + 10)),
                            Checkpoint {
                                phase: "start".into(),
                                data: json!({}),
                            },
                            3,
                        )
                        .unwrap()
                        .registered(kind.into(), false),
                    )
                    .unwrap();
            }
            if unknown {
                scheduler.inject_unknown_post_commit();
            }
            let driver = Driver::start(scheduler);
            a_enter_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            b_enter_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            a_go_tx.send(()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !driver.shared.state.lock().unwrap().stop {
                assert!(std::time::Instant::now() < deadline);
                thread::yield_now();
            }
            b_go_tx.send(()).unwrap();
            let report = driver.shutdown_with_pending().unwrap();
            assert!(report.failure().is_some());
            if unknown {
                assert_eq!(report.pending().len(), 2);
                assert!(
                    report
                        .pending()
                        .iter()
                        .any(|item| item.task_id() == Uuid::from_u128(1))
                );
                assert!(
                    report
                        .pending()
                        .iter()
                        .any(|item| item.task_id() == Uuid::from_u128(2)
                            && item.completed().is_some())
                );
                assert_eq!(
                    TaskStore::new(owner.store())
                        .load(Uuid::from_u128(2))
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Running
                );
            } else {
                assert!(report.pending().is_empty());
                assert_eq!(
                    TaskStore::new(owner.store())
                        .load(Uuid::from_u128(2))
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Succeeded(json!("b"))
                );
            }
        }
    }

    #[test]
    fn shutdown_fences_migration_and_last_reservation_check() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for boundary in ["migration", "commit"] {
            let dir = tempfile::tempdir().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let release = Arc::new(Mutex::new(release_rx));
            let calls = Arc::new(AtomicUsize::new(0));
            let handler_calls = calls.clone();
            let mut registry = task_scheduler::Registry::default();
            if boundary == "migration" {
                let release = release.clone();
                let migration_entered = entered_tx.clone();
                registry.register_versioned(
                    "work",
                    2,
                    false,
                    move |_: &Task| {
                        handler_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(task_scheduler::Step::Succeed(json!(null)))
                    },
                    Some(Arc::new(move |input, checkpoint, _| {
                        migration_entered.send(()).unwrap();
                        release.lock().unwrap().recv().unwrap();
                        Ok((input.clone(), checkpoint.clone()))
                    })),
                );
            } else {
                registry.register("work", false, move |_: &Task| {
                    handler_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(task_scheduler::Step::Succeed(json!(null)))
                });
            }
            let mut scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
            let owner = scheduler.owner();
            let task = Task::new(
                Uuid::from_u128(1),
                TaskOwner::Conversation(Uuid::from_u128(9)),
                Checkpoint {
                    phase: "start".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap()
            .registered("work".into(), false);
            TaskStore::new(owner.store())
                .save(&[Save::Create(&task)])
                .unwrap();
            if boundary == "commit" {
                let release = release.clone();
                scheduler.set_reservation_commit_hook(Arc::new(move |_, _| {
                    entered_tx.send(()).unwrap();
                    release.lock().unwrap().recv().unwrap();
                }));
            }
            let driver = Driver::start(scheduler);
            let shared = driver.shared.clone();
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let (done_tx, done_rx) = mpsc::channel();
            let shutdown =
                thread::spawn(move || done_tx.send(driver.shutdown_with_pending()).unwrap());
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !shared.reservation_stop.load(Ordering::Acquire) {
                assert!(std::time::Instant::now() < deadline);
                thread::yield_now();
            }
            assert!(matches!(
                done_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            release_tx.send(()).unwrap();
            let report = done_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            shutdown.join().unwrap();
            assert!(report.failure().is_none());
            assert!(report.pending().is_empty());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let stored = TaskStore::new(owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task;
            assert_eq!(stored.state(), &TaskState::Ready);
            assert!(stored.attempt_id().is_none());
            assert_eq!(stored.definition_version(), 1);
        }
    }

    #[test]
    fn registry_replacement_waits_for_detached_live_invocation() {
        let dir = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register("work", false, move |_: &Task| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("old")))
        });
        let mut scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        let task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("work".into(), false);
        scheduler.submit(&task).unwrap();
        let task_scheduler::TickReservation::Invocation(invocation) =
            scheduler.reserve_tick(0).unwrap()
        else {
            panic!("reservation")
        };
        let execution = thread::spawn(move || invocation.execute().unwrap());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let driver = Arc::new(Driver::start(scheduler));
        let replacing = driver.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let replacement = thread::spawn(move || {
            let mut registry = task_scheduler::Registry::default();
            registry.register("work", false, |_: &Task| {
                Ok(task_scheduler::Step::Succeed(json!("new")))
            });
            started_tx.send(()).unwrap();
            done_tx.send(replacing.replace_registry(registry)).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(150)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(!driver.shared.state.lock().unwrap().stop);
        release_tx.send(()).unwrap();
        let completed = execution.join().unwrap();
        driver
            .shared
            .scheduler
            .lock()
            .unwrap()
            .settle_invocation(completed)
            .unwrap();
        driver.shared.wake.notify_all();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        replacement.join().unwrap();
        assert!(!driver.shared.state.lock().unwrap().stop);
        assert_eq!(
            driver.get(task.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("old"))
        );
        Arc::try_unwrap(driver).ok().unwrap().shutdown().unwrap();
    }

    #[test]
    fn shutdown_transfers_uncertain_outcome_and_keeps_attachment() {
        for legacy_shutdown in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let count = calls.clone();
            let mut registry = task_scheduler::Registry::default();
            registry.register("work", false, move |_: &Task| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(task_scheduler::Step::Succeed(json!("result")))
            });
            let mut scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
            let owner = scheduler.owner();
            let task = Task::new(
                Uuid::from_u128(1),
                TaskOwner::Conversation(Uuid::from_u128(9)),
                Checkpoint {
                    phase: "start".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap()
            .registered("work".into(), false);
            scheduler.submit(&task).unwrap();
            scheduler.inject_unknown_post_commit();
            let driver = Driver::start(scheduler);
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !driver.shared.state.lock().unwrap().stop {
                assert!(std::time::Instant::now() < deadline, "worker did not stop");
                thread::yield_now();
            }
            let report = if legacy_shutdown {
                match driver.shutdown() {
                    Err(Error::Unresolved(report)) => *report,
                    other => panic!("expected owned shutdown error: {other:?}"),
                }
            } else {
                driver.shutdown_with_pending().unwrap()
            };
            assert!(report.failure().is_some());
            assert_eq!(report.pending().len(), 1);
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            let pending = &report.pending()[0];
            assert_eq!(pending.task_id(), task.id());
            assert!(pending.attempt_id().is_some());
            assert_eq!(
                pending.observed_task().unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("result"))
            );
            assert_eq!(
                pending.candidate_task().unwrap().state(),
                &TaskState::Succeeded(json!("result"))
            );
            assert_eq!(
                TaskStore::new(owner.store())
                    .load(task.id())
                    .unwrap()
                    .unwrap()
                    .task
                    .state(),
                &TaskState::Succeeded(json!("result"))
            );
            assert!(matches!(
                task_scheduler::Scheduler::attach(owner.clone(), registry.clone()),
                Err(task_scheduler::Error::AlreadyAttached)
            ));
            let retained = report.into_pending();
            assert!(matches!(
                task_scheduler::Scheduler::attach(owner.clone(), registry.clone()),
                Err(task_scheduler::Error::AlreadyAttached)
            ));
            drop(retained);
            assert!(task_scheduler::Scheduler::attach(owner.clone(), registry).is_ok());
        }
    }

    #[test]
    fn cleanup_failure_during_handler_returns_unsettled_result() {
        let dir = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register("work", false, move |_: &Task| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("retained")))
        });
        let scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
        let owner = scheduler.owner();
        let task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("work".into(), false);
        scheduler.submit(&task).unwrap();
        let driver = Driver::start(scheduler);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let attempt = TaskStore::new(owner.store())
            .load(task.id())
            .unwrap()
            .unwrap()
            .task
            .attempt_id();
        fail_cleanup(&driver.shared, "cleanup commit uncertain".into());
        release_tx.send(()).unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert_eq!(report.pending().len(), 1);
        assert_eq!(report.pending()[0].task_id(), task.id());
        assert_eq!(report.pending()[0].attempt_id(), attempt);
        assert!(matches!(
            &report.pending()[0],
            task_scheduler::PendingSettlement::Unprepared(_)
        ));
        let completed = report.pending()[0].completed().unwrap();
        assert!(
            matches!(completed.handler_result(), Some(Ok(task_scheduler::Step::Succeed(value))) if *value == json!("retained"))
        );
        assert!(completed.staged_documents().is_some());
        assert_eq!(
            TaskStore::new(owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
        assert!(matches!(
            task_scheduler::Scheduler::attach(owner.clone(), registry.clone()),
            Err(task_scheduler::Error::AlreadyAttached)
        ));
        drop(report);
        assert!(task_scheduler::Scheduler::attach(owner.clone(), registry).is_ok());
    }

    #[test]
    fn shutdown_waits_for_known_pending_abort_cleanup() {
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
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register("parent", false, |_: &Task| {
            Ok(task_scheduler::Step::Succeed(json!(null)))
        });
        registry.register("child", false, move |_: &Task| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("discard")))
        });
        let scheduler = harness.attach_tasks(registry).unwrap();
        let owner = scheduler.owner();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: json!({}),
        };
        let mut parent = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![
                Uuid::from_u128(2),
                Uuid::from_u128(3),
            ]))
            .unwrap();
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(parent.id()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("child".into(), false);
        let sibling = Task::new(
            Uuid::from_u128(3),
            TaskOwner::Task(parent.id()),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("missing".into(), false);
        TaskStore::new(owner.store())
            .save(&[
                Save::Create(&parent),
                Save::Create(&child),
                Save::Create(&sibling),
            ])
            .unwrap();
        let control = owner.conversation_controls().get(Uuid::from_u128(99));
        let poll = control.gate.lock().unwrap();
        let driver = Driver::start_with_harness(scheduler, harness.worker());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let tasks = TaskStore::new(owner.store());
        let sibling = tasks.load(sibling.id()).unwrap().unwrap();
        tasks
            .update(&sibling, |task| {
                task.start()?;
                task.fail("failed".into())
            })
            .unwrap();
        release_tx.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while driver.shared.pending.lock().unwrap().is_empty() {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        let (done_tx, done_rx) = mpsc::channel();
        let shutdown = thread::spawn(move || done_tx.send(driver.shutdown_with_pending()).unwrap());
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(150)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(poll);
        let report = done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        shutdown.join().unwrap();
        assert!(report.failure().is_none());
        assert!(report.pending().is_empty());
        let child = tasks.load(child.id()).unwrap().unwrap().task;
        assert!(child.abort_requested());
        assert_eq!(child.state(), &TaskState::Ready);
    }

    #[test]
    fn dropped_driver_keeps_unresolved_outcomes_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register("work", false, move |_: &Task| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("held")))
        });
        let scheduler =
            task_scheduler::Scheduler::open(dir.path().join("db"), registry.clone()).unwrap();
        let owner = scheduler.owner();
        let task = Task::new(
            Uuid::from_u128(1234567),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("work".into(), false);
        scheduler.submit(&task).unwrap();
        let driver = Driver::start(scheduler);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        fail_cleanup(&driver.shared, "uncertain cleanup".into());
        let weak = Arc::downgrade(&driver.shared);
        drop(driver);
        release_tx.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        // A failed Weak::upgrade only means the strong count reached zero;
        // Shared::drop can still be transferring its pending outcomes.
        let mut outcomes = Vec::new();
        let index = loop {
            outcomes.extend(Driver::take_abandoned_pending());
            if let Some(index) = outcomes
                .iter()
                .position(|pending| pending.task_id() == task.id())
            {
                break index;
            }
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        };
        assert!(weak.upgrade().is_none());
        let pending = outcomes.swap_remove(index);
        assert!(matches!(
            task_scheduler::Scheduler::attach(owner.clone(), registry.clone()),
            Err(task_scheduler::Error::AlreadyAttached)
        ));
        drop(pending);
        assert!(task_scheduler::Scheduler::attach(owner.clone(), registry).is_ok());
    }

    #[test]
    fn shutdown_joins_reserved_handler_and_settles_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let mut registry = task_scheduler::Registry::default();
        registry.register("work", false, move |_: &Task| {
            count.fetch_add(1, Ordering::SeqCst);
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(task_scheduler::Step::Succeed(json!("done")))
        });
        let scheduler = task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        let owner = scheduler.owner();
        let task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(9)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("work".into(), false);
        scheduler.submit(&task).unwrap();
        let driver = Driver::start(scheduler);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let running = TaskStore::new(owner.store())
            .load(task.id())
            .unwrap()
            .unwrap()
            .task;
        assert_eq!(running.state(), &TaskState::Running);
        let attempt = running.attempt_id();
        let shared = driver.shared.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let shutdown = thread::spawn(move || done_tx.send(driver.shutdown_with_pending()).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !shared.state.lock().unwrap().stop {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(
            TaskStore::new(owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task
                .attempt_id(),
            attempt
        );
        release_tx.send(()).unwrap();
        let report = done_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        shutdown.join().unwrap();
        assert!(report.failure().is_none());
        assert!(report.pending().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            TaskStore::new(owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Succeeded(json!("done"))
        );
    }

    #[test]
    fn cleanup_failure_and_task_commit_are_serialized_in_both_orders() {
        for failure_first in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let mut registry = task_scheduler::Registry::default();
            registry.register("work", false, |_: &Task| {
                Ok(task_scheduler::Step::Succeed(json!("done")))
            });
            let mut scheduler =
                task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
            let owner = scheduler.owner();
            let task = Task::new(
                Uuid::from_u128(1),
                TaskOwner::Conversation(Uuid::from_u128(2)),
                Checkpoint {
                    phase: "start".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap()
            .registered("work".into(), false);
            scheduler.submit(&task).unwrap();
            let task_scheduler::TickReservation::Invocation(invocation) =
                scheduler.reserve_tick(0).unwrap()
            else {
                panic!("invocation")
            };
            let completed = invocation.execute().unwrap();
            let shared = Arc::new(Shared {
                owner: owner.clone(),
                state: Mutex::new(State::default()),
                wake_reader: scheduler.wake_reader(),
                activity: scheduler.worker_activity(),
                registry_gate: Mutex::new(()),
                registry_replacing: std::sync::atomic::AtomicBool::new(false),
                reservation_stop: scheduler.reservation_stop_token(),
                failure_publication: scheduler.failure_publication(),
                scheduler: Mutex::new(scheduler),
                pending: Mutex::new(Vec::new()),
                wake: Condvar::new(),
                harness: None,
                cleanup_stop: pi_rs_core::CancellationToken::new(),
                unhooked_cancellations: Mutex::new(HashMap::new()),
                notifications: Mutex::new(None),
                subscription: Mutex::new(None),
            });
            if failure_first {
                fail_cleanup(&shared, "unknown cleanup outcome".into());
                let rejected = shared
                    .scheduler
                    .lock()
                    .unwrap()
                    .settle_invocation(completed);
                assert!(matches!(
                    rejected,
                    Err(task_scheduler::SettlementError::Rejected {
                        error: task_scheduler::Error::Poisoned,
                        ..
                    })
                ));
                assert_eq!(
                    TaskStore::new(owner.store())
                        .load(task.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Running
                );
                drop(rejected);
            } else {
                let task_gate = owner.task_gate().lock().unwrap();
                let committing = shared.clone();
                let commit = thread::spawn(move || {
                    committing
                        .scheduler
                        .lock()
                        .unwrap()
                        .settle_invocation(completed)
                });
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while shared.failure_publication.try_lock().is_ok() {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "settlement did not acquire failure fence"
                    );
                    thread::yield_now();
                }
                let failing = shared.clone();
                let (started_tx, started_rx) = mpsc::channel();
                let (done_tx, done_rx) = mpsc::channel();
                let failure = thread::spawn(move || {
                    started_tx.send(()).unwrap();
                    fail_cleanup(&failing, "unknown cleanup outcome".into());
                    done_tx.send(()).unwrap();
                });
                started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                assert!(matches!(
                    done_rx.recv_timeout(Duration::from_millis(100)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ));
                drop(task_gate);
                assert!(commit.join().unwrap().unwrap());
                done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                failure.join().unwrap();
                assert_eq!(
                    TaskStore::new(owner.store())
                        .load(task.id())
                        .unwrap()
                        .unwrap()
                        .task
                        .state(),
                    &TaskState::Succeeded(json!("done"))
                );
            }
            assert!(shared.state.lock().unwrap().stop);
            assert!(matches!(
                shared.scheduler.lock().unwrap().reserve_tick(0),
                Err(task_scheduler::Error::Poisoned)
            ));
        }
    }

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
            wake_reader: scheduler.wake_reader(),
            activity: scheduler.worker_activity(),
            registry_gate: Mutex::new(()),
            registry_replacing: std::sync::atomic::AtomicBool::new(false),
            reservation_stop: scheduler.reservation_stop_token(),
            failure_publication: scheduler.failure_publication(),
            scheduler: Mutex::new(scheduler),
            pending: Mutex::new(Vec::new()),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            unhooked_cancellations: Mutex::new(HashMap::new()),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        });
        let task_scheduler::TickReservation::Invocation(invocation) =
            shared.scheduler.lock().unwrap().reserve_tick(0).unwrap()
        else {
            panic!("expected detached reservation");
        };
        let thread = thread::spawn(move || invocation.execute().unwrap());
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
        let completed = thread.join().unwrap();
        // A finished handler is still live until its result has settled.
        assert!(matches!(
            shared.try_quiescent_commit(b.clone(), effect()),
            Err(QuiescentError::Busy)
        ));
        assert!(
            shared
                .scheduler
                .lock()
                .unwrap()
                .settle_invocation(completed)
                .unwrap()
        );
        let competing = Task::new(
            Uuid::from_u128(21),
            TaskOwner::Conversation(Uuid::from_u128(31)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), true);
        shared.scheduler.lock().unwrap().submit(&competing).unwrap();
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
        let reserving = shared.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let reservation_thread = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let mut scheduler = reserving.scheduler.lock().unwrap();
            let task_scheduler::TickReservation::Invocation(invocation) =
                scheduler.reserve_tick(0).unwrap()
            else {
                panic!("expected competing reservation")
            };
            reserved_tx.send(invocation.task_id()).unwrap();
            drop(scheduler);
            let completed = invocation.execute().unwrap();
            reserving
                .scheduler
                .lock()
                .unwrap()
                .settle_invocation(completed)
                .unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(matches!(
            reserved_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(task_gate);
        commit_thread.join().unwrap().unwrap();
        assert_eq!(
            reserved_rx.recv_timeout(Duration::from_secs(3)).unwrap(),
            competing.id()
        );
        reservation_thread.join().unwrap();
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
            wake_reader: scheduler.wake_reader(),
            activity: scheduler.worker_activity(),
            registry_gate: Mutex::new(()),
            registry_replacing: std::sync::atomic::AtomicBool::new(false),
            reservation_stop: scheduler.reservation_stop_token(),
            failure_publication: scheduler.failure_publication(),
            scheduler: Mutex::new(scheduler),
            state: Mutex::new(State::default()),
            pending: Mutex::new(Vec::new()),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            unhooked_cancellations: Mutex::new(HashMap::new()),
            notifications: Mutex::new(None),
            subscription: Mutex::new(None),
        };
        let revision = shared.activity.snapshot_with_revision().0;
        assert!(shared.tick_and_record_failure(0).is_err());
        assert!(shared.activity.snapshot_with_revision().0 > revision);
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
            wake_reader: scheduler.wake_reader(),
            activity: scheduler.worker_activity(),
            registry_gate: Mutex::new(()),
            registry_replacing: std::sync::atomic::AtomicBool::new(false),
            reservation_stop: scheduler.reservation_stop_token(),
            failure_publication: scheduler.failure_publication(),
            scheduler: Mutex::new(scheduler),
            pending: Mutex::new(Vec::new()),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            unhooked_cancellations: Mutex::new(HashMap::new()),
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
            wake_reader: scheduler.wake_reader(),
            activity: scheduler.worker_activity(),
            registry_gate: Mutex::new(()),
            registry_replacing: std::sync::atomic::AtomicBool::new(false),
            reservation_stop: scheduler.reservation_stop_token(),
            failure_publication: scheduler.failure_publication(),
            scheduler: Mutex::new(scheduler),
            pending: Mutex::new(Vec::new()),
            wake: Condvar::new(),
            harness: None,
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            unhooked_cancellations: Mutex::new(HashMap::new()),
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
            wake_reader: scheduler.wake_reader(),
            activity: scheduler.worker_activity(),
            registry_gate: Mutex::new(()),
            registry_replacing: std::sync::atomic::AtomicBool::new(false),
            reservation_stop: scheduler.reservation_stop_token(),
            failure_publication: scheduler.failure_publication(),
            scheduler: Mutex::new(scheduler),
            pending: Mutex::new(Vec::new()),
            wake: Condvar::new(),
            harness: Some(Mutex::new(harness.worker())),
            cleanup_stop: pi_rs_core::CancellationToken::new(),
            unhooked_cancellations: Mutex::new(HashMap::new()),
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
    fn fail_fast_concurrent_handler_discards_documents_before_retirement() {
        use crate::document::{Address, Documents, TaskDocuments, Transaction};
        use durable_store::Expected;
        struct BlockingHandler {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
            owned: Address,
            staged: Address,
        }
        impl task_scheduler::Handler for BlockingHandler {
            fn run(&self, _: &Task) -> Result<task_scheduler::Step, String> {
                unreachable!()
            }
            fn run_with_documents(
                &self,
                _: &Task,
                docs: &mut TaskDocuments<'_>,
            ) -> Result<task_scheduler::Step, String> {
                docs.acquire_owned(&self.owned).map_err(|e| e.to_string())?;
                docs.put(
                    &self.owned,
                    json!({"value":"uncommitted"}).as_object().unwrap().clone(),
                )
                .map_err(|e| e.to_string())?;
                docs.get(&self.staged).map_err(|e| e.to_string())?;
                docs.put(
                    &self.staged,
                    json!({"value":"uncommitted"}).as_object().unwrap().clone(),
                )
                .map_err(|e| e.to_string())?;
                self.entered.send(()).map_err(|e| e.to_string())?;
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(30))
                    .map_err(|e| e.to_string())?;
                Ok(task_scheduler::Step::Succeed(json!("normal result")))
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let parent_id = Uuid::from_u128(1);
        let running_id = Uuid::from_u128(2);
        let failed_id = Uuid::from_u128(3);
        let owned = Address {
            scope: "race".into(),
            name: "owned".into(),
            key: "".into(),
        };
        let staged = Address {
            scope: "race".into(),
            name: "staged".into(),
            key: "".into(),
        };
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (cleanup_tx, cleanup_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let finish_rx = Mutex::new(finish_rx);
        let (parent_tx, parent_rx) = mpsc::channel();
        let mut registry = task_scheduler::Registry::default();
        registry.register("parent", false, move |_: &Task| {
            parent_tx.send(()).unwrap();
            Ok(task_scheduler::Step::Succeed(json!("parent")))
        });
        registry.register_with_abort(
            "running",
            false,
            BlockingHandler {
                entered: entered_tx,
                release: Mutex::new(release_rx),
                owned: owned.clone(),
                staged: staged.clone(),
            },
            move |_: &Task| {
                cleanup_tx.send(()).unwrap();
                finish_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(30))
                    .unwrap();
                Ok(task_scheduler::Step::Succeed(json!(null)))
            },
        );
        let scheduler = task_scheduler::Scheduler::open(&path, registry).unwrap();
        let owner = scheduler.owner();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: json!(null),
        };
        let mut parent = Task::new(
            parent_id,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        parent.start().unwrap();
        parent
            .wait(WaitFor::TasksFailFast(vec![running_id, failed_id]))
            .unwrap();
        let running = Task::new(
            running_id,
            TaskOwner::Task(parent_id),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("running".into(), false);
        let failed = Task::new(failed_id, TaskOwner::Task(parent_id), checkpoint, 3)
            .unwrap()
            .registered("failed".into(), false);
        let tasks = TaskStore::new(owner.store());
        tasks
            .save(&[
                Save::Create(&parent),
                Save::Create(&running),
                Save::Create(&failed),
            ])
            .unwrap();
        let mut tx = Transaction::new();
        tx.acquire_task_document(&owned, running_id).unwrap();
        tx.put(
            &owned,
            Expected::Missing,
            json!({"value":"original"}).as_object().unwrap(),
        )
        .unwrap();
        tx.commit(owner.store()).unwrap();
        let docs = Documents::new(owner.store());
        let (_, mut watch) = docs.watch_task_document(&owned).unwrap();
        let driver = Driver::start(scheduler);
        entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        // Commit the failure through the task gate, while the driver remains
        // inside the handler. Do not invoke the abort helper from the test.
        let marking_owner = owner.clone();
        thread::spawn(move || {
            let _gate = marking_owner.task_gate().lock().unwrap();
            let tasks = TaskStore::new(marking_owner.store());
            let failed = tasks.load(failed_id).unwrap().unwrap();
            tasks
                .update(&failed, |task| {
                    task.start()?;
                    task.fail("failure".into())
                })
                .unwrap();
        })
        .join()
        .unwrap();
        let running = driver.get(running_id).unwrap().unwrap();
        assert_eq!(running.state(), &TaskState::Running);
        assert!(!running.abort_requested());
        assert!(matches!(
            driver.get(parent_id).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        assert_eq!(
            docs.get(&owned).unwrap().unwrap().value["value"],
            "original"
        );
        assert!(docs.get(&staged).unwrap().is_none());
        assert!(watch.poll(10).unwrap().is_empty());
        assert!(parent_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        cleanup_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let cleanup = driver.get(running_id).unwrap().unwrap();
        assert_eq!(cleanup.state(), &TaskState::Running);
        assert!(cleanup.abort_requested());
        assert!(matches!(
            driver.get(parent_id).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ));
        assert_eq!(
            docs.get(&owned).unwrap().unwrap().value["value"],
            "original"
        );
        assert!(docs.get(&staged).unwrap().is_none());
        assert!(watch.poll(10).unwrap().is_empty());
        assert!(parent_rx.try_recv().is_err());
        finish_tx.send(()).unwrap();
        parent_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !driver.get(parent_id).unwrap().unwrap().is_terminal() {
            assert!(std::time::Instant::now() < deadline);
            thread::yield_now();
        }
        assert_eq!(
            driver.get(running_id).unwrap().unwrap().state(),
            &TaskState::Cancelled
        );
        assert!(docs.get(&owned).unwrap().is_none());
        assert!(docs.get(&staged).unwrap().is_none());
        let frames = watch.poll(10).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(frames[0].retired);
        assert_eq!(
            frames[0].document.as_ref().unwrap().value["value"],
            "original"
        );
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
