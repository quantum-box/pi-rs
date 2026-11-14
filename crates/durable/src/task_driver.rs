//! Background driver for the generic task scheduler sharing a Harness owner.
//! The host registers handlers before starting the driver. A handler must be
//! bounded or cooperative: shutdown waits for an in-progress synchronous step.
use crate::{
    task::{Task, TaskOwner, TaskState},
    task_scheduler,
    task_store::{Save, Signal, TaskStore},
};
use durable_store::owner::OwnedStore;
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
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
    #[error("conversation runtime: {0}")]
    Runtime(#[from] crate::runtime::Error),
}
pub(crate) struct Shared {
    scheduler: Mutex<task_scheduler::Scheduler>,
    owner: Arc<OwnedStore>,
    policies: HashMap<String, (bool, u32)>,
    definitions: HashMap<String, (bool, Arc<dyn task_scheduler::Handler>)>,
    state: Mutex<State>,
    wake: Condvar,
    harness: Option<Mutex<crate::runtime::Harness>>,
    cleanup_stop: pi_rs_core::CancellationToken,
}
#[derive(Default)]
struct State {
    stop: bool,
    failure: Option<String>,
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
        let policies = scheduler.policies();
        let definitions = scheduler.definitions();
        let shared = Arc::new(Shared {
            scheduler: Mutex::new(scheduler),
            owner,
            policies,
            definitions,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            harness: harness.map(Mutex::new),
            cleanup_stop: pi_rs_core::CancellationToken::new(),
        });
        let task_shared = shared.clone();
        let worker = thread::spawn(move || {
            loop {
                if task_shared.state.lock().map_or(true, |s| s.stop) {
                    break;
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(i64::MAX as u128) as i64;
                let result = task_shared
                    .scheduler
                    .lock()
                    .map_err(|_| "task scheduler lock poisoned".to_string())
                    .and_then(|mut scheduler| scheduler.tick(now).map_err(|e| e.to_string()));
                match result {
                    Ok(true) => continue,
                    Ok(false) => {}
                    Err(error) => {
                        if let Ok(mut state) = task_shared.state.lock() {
                            state.failure = Some(error);
                            state.stop = true;
                        }
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
        let _state = self.check()?;
        let registered = task
            .kind()
            .and_then(|kind| self.shared.policies.get(kind))
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
        self.shared.wake.notify_one();
        Ok(())
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
        self.shared.wake.notify_one();
        Ok(())
    }
    pub fn signal(&self, signal: &Signal) -> Result<(), Error> {
        let _state = self.check()?;
        TaskStore::new(self.shared.owner.store())
            .send_signal(signal)
            .map_err(task_scheduler::Error::from)?;
        self.shared.wake.notify_one();
        Ok(())
    }
    pub fn get(&self, id: Uuid) -> Result<Option<Task>, Error> {
        let _state = self.check()?;
        Ok(TaskStore::new(self.shared.owner.store())
            .load(id)
            .map_err(task_scheduler::Error::from)?
            .map(|snapshot| snapshot.task))
    }
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.stop();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| Error::Stopped("worker panicked".into()))?;
        }
        if let Some(cleanup) = self.cleanup.take() {
            cleanup
                .join()
                .map_err(|_| Error::Stopped("cleanup worker panicked".into()))?;
        }
        let state = self.shared.state.lock().map_err(|_| Error::Poisoned)?;
        if let Some(error) = &state.failure {
            return Err(Error::Stopped(error.clone()));
        }
        Ok(())
    }
    fn stop(&self) {
        self.shared.cleanup_stop.cancel();
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
                let tasks = TaskStore::new(store).list().map_err(|e| e.to_string())?;
                let edges = crate::ownership_edge::EdgeStore::new(store);
                let mut found = Vec::new();
                for task in tasks
                    .iter()
                    .filter(|t| t.task.abort_requested() || t.task.is_terminal())
                {
                    for edge in edges.children(task.task.id()).map_err(|e| e.to_string())? {
                        if edge.edge.task_owner != *task.task.owner() {
                            return Err("ownership edge owner mismatch".into());
                        }
                        if edge.edge.mode != crate::ownership_edge::Mode::Foreground {
                            continue;
                        }
                        let h = shared
                            .harness
                            .as_ref()
                            .unwrap()
                            .lock()
                            .map_err(|e| e.to_string())?;
                        if h.conversation(edge.edge.child)
                            .map_err(|e| e.to_string())?
                            .abort_requested
                        {
                            found.push(edge.edge.child);
                        }
                    }
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
}

impl Shared {
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
                    .and_then(|kind| self.policies.get(kind))
                    .copied()
                    != Some(task.replay_safe())
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
            self.wake.notify_one();
        }
        result
    }
    pub(crate) fn policy_for(
        &self,
        name: &str,
        handler: &Arc<dyn task_scheduler::Handler>,
    ) -> Option<bool> {
        if self.state.lock().ok()?.stop {
            return None;
        }
        let (policy, registered) = self.definitions.get(name)?;
        if self.policies.get(name)?.1 != 1 {
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
            policies: scheduler.policies(),
            definitions: scheduler.definitions(),
            scheduler: Mutex::new(scheduler),
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
            harness: Some(Mutex::new(harness.worker())),
            cleanup_stop: pi_rs_core::CancellationToken::new(),
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
