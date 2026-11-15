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
    revision: u128,
    updates: tokio::sync::watch::Sender<Vec<Worker>>,
}

impl State {
    fn snapshot(&self) -> Vec<Worker> {
        let mut workers: Vec<_> = self.workers.values().copied().collect();
        workers.sort_unstable_by_key(|w| (w.id, w.kind as u8, w.phase as u8));
        workers
    }

    fn publish(&mut self) {
        self.revision = self.revision.wrapping_add(1);
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
            revision: 0,
            updates,
        })))
    }
}
impl WorkerActivity {
    pub fn snapshot(&self) -> Vec<Worker> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).snapshot()
    }
    /// Read the local change revision and worker snapshot under the same lock.
    /// The revision also advances for definition changes that leave the worker
    /// list unchanged. It is process-local and wraps after `u128::MAX` changes.
    pub fn snapshot_with_revision(&self) -> (u128, Vec<Worker>) {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        (state.revision, state.snapshot())
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
    /// Wake observers when scheduling definitions change without a store write.
    pub(crate) fn notify(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).publish();
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

    #[test]
    fn revision_tracks_unobserved_changes_and_definition_notifications() {
        let activity = WorkerActivity::default();
        let (initial, workers) = activity.snapshot_with_revision();
        assert!(workers.is_empty());
        let guard = activity.enter(WorkerKind::Task, Uuid::new_v4());
        let (running, workers) = activity.snapshot_with_revision();
        assert_ne!(initial, running);
        assert_eq!(workers.len(), 1);
        guard.set(WorkerPhase::Running);
        assert_eq!(activity.snapshot_with_revision().0, running);
        guard.set(WorkerPhase::Waiting);
        let (waiting, workers) = activity.snapshot_with_revision();
        assert_ne!(running, waiting);
        assert_eq!(workers[0].phase, WorkerPhase::Waiting);
        activity.notify();
        let (definition, unchanged) = activity.snapshot_with_revision();
        assert_ne!(waiting, definition);
        assert_eq!(unchanged, workers);
        drop(guard);
        let (done, workers) = activity.snapshot_with_revision();
        assert_ne!(definition, done);
        assert!(workers.is_empty());
    }
}
