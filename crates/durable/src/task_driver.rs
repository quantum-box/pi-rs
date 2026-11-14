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
}
struct Shared {
    scheduler: Mutex<task_scheduler::Scheduler>,
    owner: Arc<OwnedStore>,
    policies: HashMap<String, bool>,
    state: Mutex<State>,
    wake: Condvar,
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
}
impl Driver {
    pub fn start(scheduler: task_scheduler::Scheduler) -> Self {
        let owner = scheduler.owner();
        let policies = scheduler.policies();
        let shared = Arc::new(Shared {
            scheduler: Mutex::new(scheduler),
            owner,
            policies,
            state: Mutex::new(State::default()),
            wake: Condvar::new(),
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
        Self {
            shared,
            worker: Some(worker),
        }
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
        if registered != Some(task.replay_safe())
            || !matches!(task.owner(), TaskOwner::Conversation(_))
            || task.state() != &TaskState::Ready
        {
            return Err(task_scheduler::Error::Ownership.into());
        }
        if let TaskOwner::Conversation(id) = task.owner() {
            let key = format!("runtime/conversation/{id}");
            if self
                .shared
                .owner
                .store()
                .get(key.as_bytes())
                .map_err(|e| task_scheduler::Error::Store(e.into()))?
                .is_none()
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
        let state = self.shared.state.lock().map_err(|_| Error::Poisoned)?;
        if let Some(error) = &state.failure {
            return Err(Error::Stopped(error.clone()));
        }
        Ok(())
    }
    fn stop(&self) {
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
