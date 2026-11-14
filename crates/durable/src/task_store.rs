//! Atomic persistence and optimistic concurrency for standalone tasks.
//!
//! Recovery is explicit: the host must first establish that the previous
//! executor has stopped. A successful reservation is durable before execution;
//! a commit error can have an unknown outcome and requires inspection on reopen.

use crate::task::{Task, TaskOwner, TaskState, TransitionError, WaitFor};
use durable_store::{Check, Entry, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

const PREFIX: &[u8] = b"durable/task/";
const SIGNAL_PREFIX: &[u8] = b"durable/signal/";

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
    #[error("signal ID was already used with different content")]
    SignalMismatch,
    #[error("wait contains a missing, duplicate, or non-child task")]
    InvalidChild,
    #[error("task is not waiting")]
    NotWaiting,
    #[error("signal target was not found")]
    SignalTargetNotFound,
    #[error("signal target is closed")]
    SignalTargetClosed,
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

/// A durable, uniquely identified signal. IDs are scoped to a target task.
/// Reusing an ID with identical content is idempotent, even after consumption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub id: Uuid,
    pub task_id: Uuid,
    pub name: String,
    pub payload: Value,
    consumed: bool,
}

impl Signal {
    pub fn new(id: Uuid, task_id: Uuid, name: String, payload: Value) -> Self {
        Self {
            id,
            task_id,
            name,
            payload,
            consumed: false,
        }
    }
    pub fn is_consumed(&self) -> bool {
        self.consumed
    }
}

/// Result of reevaluating a waiting task. Signal payloads remain readable after
/// delivery via `signal`, including after a crash following the commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeResult {
    Pending,
    Woken { signal: Option<Signal> },
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

    /// Atomically commit any deterministic task transition against its snapshot.
    /// The closure may change checkpoint and state together; on error nothing is saved.
    /// Reload after success. The caller must fence any previous executor.
    pub fn update<F>(&self, snapshot: &StoredTask, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Task) -> Result<(), TransitionError>,
    {
        let mut next = snapshot.clone();
        f(&mut next.task).map_err(Error::Transition)?;
        self.save(&[Save::Update(&next)])
    }

    /// Durably enqueue a signal. The same task/ID/content can be retried safely.
    /// A conflicting ID returns SignalMismatch. Signals are retained as receipts.
    pub fn send_signal(&self, signal: &Signal) -> Result<(), Error> {
        let k = signal_key(signal.task_id, signal.id);
        let mut fresh = signal.clone();
        fresh.consumed = false;
        loop {
            if let Some(entry) = self.store.get(&k)? {
                let old: Signal = serde_json::from_slice(&entry.value)?;
                if old.id == fresh.id
                    && old.task_id == fresh.task_id
                    && old.name == fresh.name
                    && old.payload == fresh.payload
                {
                    return Ok(());
                }
                return Err(Error::SignalMismatch);
            }
            let target = self
                .load(signal.task_id)?
                .ok_or(Error::SignalTargetNotFound)?;
            if target.task.is_terminal() {
                return Err(Error::SignalTargetClosed);
            }
            match self.store.commit(
                &[
                    Check {
                        key: k.clone(),
                        expected: Expected::Missing,
                    },
                    Check {
                        key: key(signal.task_id),
                        expected: Expected::Version(target.version),
                    },
                ],
                &[Mutation::Put {
                    key: k.clone(),
                    value: serde_json::to_vec(&fresh)?,
                }],
            ) {
                Ok(()) => return Ok(()),
                Err(durable_store::Error::Conflict { .. }) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Read a signal receipt, including whether it has been consumed.
    pub fn signal(&self, task_id: Uuid, id: Uuid) -> Result<Option<Signal>, Error> {
        self.store
            .get(&signal_key(task_id, id))?
            .map(|entry| serde_json::from_slice(&entry.value).map_err(Error::from))
            .transpose()
    }

    /// Reevaluate a persisted wait using the supplied wall-clock Unix milliseconds.
    /// Call after startup and whenever a deadline, signal, or child may have changed.
    /// Waking and consuming a signal are one CAS transaction. Child versions are
    /// checked in that same transaction. Conflicts require a fresh snapshot.
    /// Pending is only a snapshot: the scheduler must arrange another reevaluation
    /// without losing concurrent notifications (for example periodic rescanning).
    pub fn poll_wait(&self, snapshot: &StoredTask, now_millis: i64) -> Result<WakeResult, Error> {
        let condition = match snapshot.task.state() {
            TaskState::Waiting(condition) => condition,
            _ => return Err(Error::NotWaiting),
        };
        let mut checks = vec![Check {
            key: key(snapshot.task.id()),
            expected: Expected::Version(snapshot.version),
        }];
        let mut mutations = Vec::new();
        let mut delivered = None;
        match condition {
            WaitFor::DeadlineMillis(deadline) => {
                if now_millis < *deadline {
                    return Ok(WakeResult::Pending);
                }
            }
            WaitFor::Signal(name) => {
                let prefix = signal_task_prefix(snapshot.task.id());
                let mut found = None;
                for (k, entry) in self.store.list(&prefix)? {
                    let signal: Signal = serde_json::from_slice(&entry.value)?;
                    if !signal.consumed && signal.name == *name {
                        found = Some((k, entry.version, signal));
                        break;
                    }
                }
                let Some((k, version, mut signal)) = found else {
                    return Ok(WakeResult::Pending);
                };
                signal.consumed = true;
                checks.push(Check {
                    key: k.clone(),
                    expected: Expected::Version(version),
                });
                mutations.push(Mutation::Put {
                    key: k,
                    value: serde_json::to_vec(&signal)?,
                });
                delivered = Some(signal);
            }
            WaitFor::Tasks(ids) | WaitFor::TasksAny(ids) | WaitFor::TasksFailFast(ids) => {
                let mut seen = std::collections::HashSet::new();
                let mut pending = false;
                let mut ended = false;
                for id in ids {
                    if !seen.insert(*id) {
                        return Err(Error::InvalidChild);
                    }
                    let child = self.load(*id)?.ok_or(Error::InvalidChild)?;
                    if child.task.owner() != &TaskOwner::Task(snapshot.task.id()) {
                        return Err(Error::InvalidChild);
                    }
                    if !child.task.is_terminal() {
                        pending = true;
                    } else {
                        ended = true;
                    }
                    checks.push(Check {
                        key: key(*id),
                        expected: Expected::Version(child.version),
                    });
                }
                if if matches!(condition, WaitFor::TasksAny(_)) {
                    !ended
                } else {
                    pending
                } {
                    return Ok(WakeResult::Pending);
                }
            }
        }
        let mut next = snapshot.task.clone();
        match &delivered {
            Some(signal) => next.wake_with_signal(signal.id),
            None => next.wake(),
        }
        .map_err(Error::Transition)?;
        mutations.push(Mutation::Put {
            key: key(next.id()),
            value: serde_json::to_vec(&next)?,
        });
        self.store.commit(&checks, &mutations)?;
        Ok(WakeResult::Woken { signal: delivered })
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

fn signal_task_prefix(task_id: Uuid) -> Vec<u8> {
    let mut key = SIGNAL_PREFIX.to_vec();
    key.extend_from_slice(task_id.as_bytes());
    key
}

fn signal_key(task_id: Uuid, id: Uuid) -> Vec<u8> {
    let mut key = signal_task_prefix(task_id);
    key.extend_from_slice(id.as_bytes());
    key
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
    #[test]
    fn deadline_survives_reopen_and_stale_wake_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("timer.db");
        let t = task(1, 3);
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            ts.save(&[Save::Create(&t)]).unwrap();
            ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
            let running = ts.load(t.id()).unwrap().unwrap();
            ts.update(&running, |t| t.wait(WaitFor::DeadlineMillis(100)))
                .unwrap();
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let waiting = ts.load(t.id()).unwrap().unwrap();
        assert_eq!(ts.poll_wait(&waiting, 99).unwrap(), WakeResult::Pending);
        assert_eq!(
            ts.poll_wait(&waiting, 100).unwrap(),
            WakeResult::Woken { signal: None }
        );
        assert!(matches!(
            ts.poll_wait(&waiting, 101),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(
            ts.load(t.id()).unwrap().unwrap().task.state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn signal_is_idempotent_and_delivery_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("signals.db");
        let t = task(1, 3);
        let sig = Signal::new(
            Uuid::from_u128(42),
            t.id(),
            "go".into(),
            json!({"value": 3}),
        );
        {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            ts.save(&[Save::Create(&t)]).unwrap();
            ts.send_signal(&sig).unwrap();
            ts.send_signal(&sig).unwrap();
            let mut changed = sig.clone();
            changed.payload = json!(4);
            assert!(matches!(
                ts.send_signal(&changed),
                Err(Error::SignalMismatch)
            ));
            ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
            let running = ts.load(t.id()).unwrap().unwrap();
            ts.update(&running, |t| t.wait(WaitFor::Signal("go".into())))
                .unwrap();
            let waiting = ts.load(t.id()).unwrap().unwrap();
            assert!(matches!(
                ts.poll_wait(&waiting, 0),
                Ok(WakeResult::Woken { signal: Some(_) })
            ));
            assert!(matches!(
                ts.poll_wait(&waiting, 0),
                Ok(WakeResult::Pending) | Err(Error::Store(durable_store::Error::Conflict { .. }))
            ));
        }
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let restored = ts.load(t.id()).unwrap().unwrap();
        let id = restored.task.delivered_signal().unwrap();
        let receipt = ts.signal(t.id(), id).unwrap().unwrap();
        assert!(receipt.is_consumed());
        assert_eq!(receipt.payload, sig.payload);
        ts.send_signal(&sig).unwrap();
        assert!(ts.signal(t.id(), id).unwrap().unwrap().is_consumed());
    }
    #[test]
    fn signal_rejects_missing_and_terminal_targets() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let mut t = task(1, 3);
        let signal = Signal::new(Uuid::new_v4(), t.id(), "go".into(), json!(null));
        assert!(matches!(
            ts.send_signal(&signal),
            Err(Error::SignalTargetNotFound)
        ));
        t.start().unwrap();
        t.succeed(json!("done")).unwrap();
        ts.save(&[Save::Create(&t)]).unwrap();
        assert!(matches!(
            ts.send_signal(&signal),
            Err(Error::SignalTargetClosed)
        ));
    }

    #[test]
    fn children_must_be_owned_terminal_and_unique() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let parent = task(1, 3);
        let child = Task::new(
            Uuid::from_u128(2),
            TaskOwner::Task(parent.id()),
            parent.checkpoint().clone(),
            3,
        )
        .unwrap();
        ts.save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        ts.reserve(&ts.load(parent.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(parent.id()).unwrap().unwrap(), |t| {
            t.wait(WaitFor::Tasks(vec![child.id()]))
        })
        .unwrap();
        let waiting = ts.load(parent.id()).unwrap().unwrap();
        assert_eq!(ts.poll_wait(&waiting, 0).unwrap(), WakeResult::Pending);
        let child_snapshot = ts.load(child.id()).unwrap().unwrap();
        ts.update(&child_snapshot, |t| t.cancel()).unwrap();
        assert_eq!(
            ts.poll_wait(&waiting, 0).unwrap(),
            WakeResult::Woken { signal: None }
        );
        ts.reserve(&ts.load(parent.id()).unwrap().unwrap()).unwrap();
        ts.update(&ts.load(parent.id()).unwrap().unwrap(), |t| {
            t.wait(WaitFor::Tasks(vec![child.id(), child.id()]))
        })
        .unwrap();
        assert!(matches!(
            ts.poll_wait(&ts.load(parent.id()).unwrap().unwrap(), 0),
            Err(Error::InvalidChild)
        ));
    }

    #[test]
    fn failed_atomic_update_does_not_commit_checkpoint() {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let t = task(1, 3);
        ts.save(&[Save::Create(&t)]).unwrap();
        ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
        let running = ts.load(t.id()).unwrap().unwrap();
        let result = ts.update(&running, |t| {
            t.advance(Checkpoint {
                phase: "next".into(),
                data: json!(2),
            })?;
            Err(TransitionError::InvalidState)
        });
        assert!(matches!(
            result,
            Err(Error::Transition(TransitionError::InvalidState))
        ));
        assert_eq!(ts.load(t.id()).unwrap().unwrap(), running);
    }
}
