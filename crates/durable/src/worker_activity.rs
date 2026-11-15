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
#[derive(Clone, Default)]
pub struct WorkerActivity(Arc<Mutex<HashMap<Uuid, Worker>>>);
impl WorkerActivity {
    pub fn snapshot(&self) -> Vec<Worker> {
        let mut workers: Vec<_> = self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .copied()
            .collect();
        workers.sort_unstable_by_key(|w| (w.id, w.kind as u8, w.phase as u8));
        workers
    }
    pub(crate) fn enter(&self, kind: WorkerKind, id: Uuid) -> Guard {
        self.enter_at(kind, id, WorkerPhase::Running)
    }
    pub(crate) fn enter_at(&self, kind: WorkerKind, id: Uuid, phase: WorkerPhase) -> Guard {
        let mut workers = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let token = loop {
            let token = Uuid::new_v4();
            if let std::collections::hash_map::Entry::Vacant(slot) = workers.entry(token) {
                slot.insert(Worker { kind, id, phase });
                break token;
            }
        };
        drop(workers);
        Guard {
            activity: self.clone(),
            token,
        }
    }
}
pub(crate) struct Guard {
    activity: WorkerActivity,
    token: Uuid,
}
impl Guard {
    pub(crate) fn set(&self, phase: WorkerPhase) {
        if let Some(worker) = self
            .activity
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&self.token)
        {
            worker.phase = phase;
        }
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.activity
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.token);
    }
}
