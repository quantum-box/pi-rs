//! In-process executor lifecycle observations. These do not represent persisted work.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

/// Kind of executor being observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerKind {
    Task,
    Conversation,
    /// An independent compaction worker, identified by its operation ID.
    Summary,
}
/// Whether an executor is queued, driving work or parked between polls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerPhase {
    Queued,
    Running,
    Waiting,
}
/// A live executor. Multiple conversation workers may wait for the same ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Worker {
    pub kind: WorkerKind,
    pub id: Uuid,
    pub phase: WorkerPhase,
}

/// A cloneable, nonblocking-on-handlers view of currently live executors.
/// Empty means no tracked executor remains; persisted runnable or sleeping work
/// may still exist. This is not an onWake value or a durable task listing.
struct State {
    workers: HashMap<Uuid, Worker>,
    updates: tokio::sync::watch::Sender<Vec<Worker>>,
}

impl State {
    fn snapshot(&self) -> Vec<Worker> {
        let mut workers: Vec<_> = self.workers.values().copied().collect();
        workers.sort_unstable_by_key(|w| (w.id, w.kind as u8, w.phase as u8));
        workers
    }

    fn publish(&self) {
        if self.updates.receiver_count() != 0 {
            self.updates.send_replace(self.snapshot());
        }
    }
}

#[derive(Clone)]
pub struct WorkerActivity(Arc<Mutex<State>>);

impl Default for WorkerActivity {
    fn default() -> Self {
        let (updates, _) = tokio::sync::watch::channel(Vec::new());
        Self(Arc::new(Mutex::new(State {
            workers: HashMap::new(),
            updates,
        })))
    }
}
impl WorkerActivity {
    pub fn snapshot(&self) -> Vec<Worker> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).snapshot()
    }
    /// Subscribe to coalesced process-local lifecycle snapshots. The initial
    /// value is current at attachment; slow subscribers may skip transitions.
    /// This does not include persisted work and is not an upstream `onWake`.
    pub fn subscribe(&self) -> WorkerUpdates {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.updates.receiver_count() == 0 {
            state.updates.send_replace(state.snapshot());
        }
        WorkerUpdates {
            receiver: state.updates.subscribe(),
        }
    }
    pub(crate) fn enter(&self, kind: WorkerKind, id: Uuid) -> Guard {
        self.enter_at(kind, id, WorkerPhase::Running)
    }
    pub(crate) fn enter_at(&self, kind: WorkerKind, id: Uuid, phase: WorkerPhase) -> Guard {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let token = loop {
            let token = Uuid::new_v4();
            if let std::collections::hash_map::Entry::Vacant(slot) = state.workers.entry(token) {
                slot.insert(Worker { kind, id, phase });
                break token;
            }
        };
        state.publish();
        drop(state);
        Guard {
            activity: self.clone(),
            token,
        }
    }
}

/// A coalesced subscription which never exposes the internal watch lock.
pub struct WorkerUpdates {
    receiver: tokio::sync::watch::Receiver<Vec<Worker>>,
}
impl WorkerUpdates {
    /// Return the latest snapshot and mark it observed.
    pub fn current(&mut self) -> Vec<Worker> {
        self.receiver.borrow_and_update().clone()
    }

    /// Wait for a later lifecycle update. Intermediate states may coalesce.
    pub async fn changed(&mut self) -> Result<Vec<Worker>, tokio::sync::watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.current())
    }
}
pub(crate) struct Guard {
    activity: WorkerActivity,
    token: Uuid,
}
impl Guard {
    pub(crate) fn set(&self, phase: WorkerPhase) {
        let mut state = self.activity.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(worker) = state.workers.get_mut(&self.token)
            && worker.phase != phase
        {
            worker.phase = phase;
            state.publish();
        }
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self.activity.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.workers.remove(&self.token).is_some() {
            state.publish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn subscribers_coalesce_and_close_after_last_owner() {
        let activity = WorkerActivity::default();
        let mut first = activity.subscribe();
        let mut second = activity.subscribe();
        let id = Uuid::new_v4();
        let guard = activity.enter(WorkerKind::Task, id);
        guard.set(WorkerPhase::Waiting);
        let seen = first.changed().await.unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].phase, WorkerPhase::Waiting);
        assert_eq!(second.changed().await.unwrap(), seen);
        guard.set(WorkerPhase::Waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), first.changed())
                .await
                .is_err()
        );
        drop(guard);
        assert!(first.changed().await.unwrap().is_empty());
        assert!(second.changed().await.unwrap().is_empty());
        drop(activity);
        assert!(first.changed().await.is_err());
        assert!(second.changed().await.is_err());
    }

    #[test]
    fn first_subscriber_receives_current_state_without_prior_listeners() {
        let activity = WorkerActivity::default();
        let guard = activity.enter(WorkerKind::Summary, Uuid::new_v4());
        let mut updates = activity.subscribe();
        assert_eq!(updates.current().len(), 1);
        drop(updates);
        drop(guard);
        let mut later = activity.subscribe();
        assert!(later.current().is_empty());
    }
}
