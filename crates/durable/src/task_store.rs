//! Atomic persistence and optimistic concurrency for standalone tasks.
//!
//! Recovery is explicit: the host must first establish that the previous
//! executor has stopped. A successful reservation is durable before execution;
//! a commit error can have an unknown outcome and requires inspection on reopen.

use crate::task::{Task, TransitionError};
use durable_store::{Check, Entry, Expected, Mutation, Store};
use uuid::Uuid;

const PREFIX: &[u8] = b"durable/task/";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid task transition: {0:?}")]
    Transition(TransitionError),
    #[error("task record key does not match its ID")]
    InvalidKey,
    #[error("duplicate task in batch")]
    DuplicateTask,
}

/// A task and the storage version that must be supplied for its next update.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredTask {
    pub task: Task,
    pub version: u64,
}

/// One member of an atomic batch. Updates must use a previously read version.
pub enum Save<'a> {
    Create(&'a Task),
    Update(&'a StoredTask),
}

pub struct TaskStore<'a> {
    store: &'a Store,
}

impl<'a> TaskStore<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub fn load(&self, id: Uuid) -> Result<Option<StoredTask>, Error> {
        let key = key(id);
        self.store
            .get(&key)?
            .map(|entry| decode(&key, entry))
            .transpose()
    }

    /// Returns a snapshot; its versions can become stale immediately.
    pub fn list(&self) -> Result<Vec<StoredTask>, Error> {
        self.store
            .list(PREFIX)?
            .into_iter()
            .map(|(key, entry)| decode(&key, entry))
            .collect()
    }

    /// Atomically create and/or update tasks. A conflict leaves the whole batch
    /// untouched. Versions are not returned: a subsequent read may observe a
    /// later writer, so it must not be used as proof of an exclusive reservation.
    pub fn save(&self, tasks: &[Save<'_>]) -> Result<(), Error> {
        let mut checks = Vec::with_capacity(tasks.len());
        let mut mutations = Vec::with_capacity(tasks.len());
        let mut seen = std::collections::HashSet::new();
        for save in tasks {
            let (task, expected) = match save {
                Save::Create(task) => (*task, Expected::Missing),
                Save::Update(stored) => (&stored.task, Expected::Version(stored.version)),
            };
            let key = key(task.id());
            if !seen.insert(key.clone()) {
                return Err(Error::DuplicateTask);
            }
            checks.push(Check {
                key: key.clone(),
                expected,
            });
            mutations.push(Mutation::Put {
                key,
                value: serde_json::to_vec(task)?,
            });
        }
        self.store.commit(&checks, &mutations)?;
        Ok(())
    }

    /// Commit Ready -> Running using the snapshot version. Only the winner of
    /// this CAS may execute. Reload before any subsequent update.
    pub fn reserve(&self, snapshot: &StoredTask) -> Result<(), Error> {
        self.transition(snapshot, Task::start)
    }

    /// Commit Running -> Ready after the host has fenced the former executor.
    /// Conflicts are surfaced, never retried against a potentially new execution.
    pub fn recover(&self, snapshot: &StoredTask) -> Result<(), Error> {
        self.transition(snapshot, Task::recover)
    }

    fn transition(
        &self,
        snapshot: &StoredTask,
        f: fn(&mut Task) -> Result<(), TransitionError>,
    ) -> Result<(), Error> {
        let mut next = snapshot.clone();
        f(&mut next.task).map_err(Error::Transition)?;
        self.save(&[Save::Update(&next)])
    }
}

fn key(id: Uuid) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

fn decode(key_bytes: &[u8], entry: Entry) -> Result<StoredTask, Error> {
    let task: Task = serde_json::from_slice(&entry.value)?;
    if key(task.id()) != key_bytes {
        return Err(Error::InvalidKey);
    }
    Ok(StoredTask {
        task,
        version: entry.version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, TaskOwner, TaskState, WaitFor};
    use serde_json::json;
    use std::sync::{Arc, Barrier};

    fn task(id: u128, limit: u32) -> Task {
        Task::new(
            Uuid::from_u128(id),
            TaskOwner::Conversation(Uuid::from_u128(100)),
            Checkpoint {
                phase: "first".into(),
                data: json!({"n": 1}),
            },
            limit,
        )
        .unwrap()
    }

    #[test]
    fn reopen_recover_and_enforce_restart_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let original = task(1, 1);
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            ts.save(&[Save::Create(&original)]).unwrap();
            ts.reserve(&ts.load(original.id()).unwrap().unwrap())
                .unwrap();
        }
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            let running = ts.load(original.id()).unwrap().unwrap();
            assert_eq!(running.task.state(), &TaskState::Running);
            ts.recover(&running).unwrap();
            assert!(matches!(
                ts.recover(&running),
                Err(Error::Store(durable_store::Error::Conflict { .. }))
            ));
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let recovered = ts.load(original.id()).unwrap().unwrap();
        assert_eq!(recovered.task.state(), &TaskState::Ready);
        assert_eq!(recovered.task.restarts(), 1);
        assert_eq!(recovered.task.checkpoint(), original.checkpoint());
        assert_eq!(recovered.task.owner(), original.owner());
        assert!(matches!(
            ts.reserve(&recovered),
            Err(Error::Transition(TransitionError::RestartLimit))
        ));
        assert_eq!(ts.load(original.id()).unwrap().unwrap(), recovered);
    }

    #[test]
    fn batch_is_atomic_and_rejects_duplicates() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let a = task(1, 3);
        let b = task(2, 3);
        ts.save(&[Save::Create(&a)]).unwrap();
        assert!(matches!(
            ts.save(&[Save::Create(&b), Save::Create(&a)]),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(ts.load(b.id()).unwrap().is_none());
        assert!(matches!(
            ts.save(&[Save::Create(&b), Save::Create(&b)]),
            Err(Error::DuplicateTask)
        ));
        assert_eq!(ts.list().unwrap().len(), 1);
    }

    #[test]
    fn concurrent_reservations_have_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let a = Store::open(&path).unwrap();
        let b = Store::open(&path).unwrap();
        let t = task(1, 3);
        TaskStore::new(&a).save(&[Save::Create(&t)]).unwrap();
        let snapshot = TaskStore::new(&a).load(t.id()).unwrap().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = [a, b]
            .into_iter()
            .map(|db| {
                let barrier = barrier.clone();
                let snapshot = snapshot.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    TaskStore::new(&db).reserve(&snapshot)
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, Err(Error::Store(durable_store::Error::Conflict { .. }))))
                .count(),
            1
        );
        let db = Store::open(&path).unwrap();
        assert_eq!(
            TaskStore::new(&db)
                .load(t.id())
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
    }

    #[test]
    fn updates_and_invalid_transitions_preserve_state() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        let before = ts.load(t.id()).unwrap().unwrap();
        assert!(matches!(
            ts.recover(&before),
            Err(Error::Transition(TransitionError::InvalidState))
        ));
        ts.reserve(&before).unwrap();
        let mut current = ts.load(t.id()).unwrap().unwrap();
        current.task.wait(WaitFor::Signal("go".into())).unwrap();
        ts.save(&[Save::Update(&current)]).unwrap();
        assert!(matches!(
            ts.save(&[Save::Update(&before)]),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        let waiting = ts.load(t.id()).unwrap().unwrap();
        assert!(matches!(
            ts.reserve(&waiting),
            Err(Error::Transition(TransitionError::InvalidState))
        ));
        assert_eq!(ts.load(t.id()).unwrap().unwrap(), waiting);
    }
}
