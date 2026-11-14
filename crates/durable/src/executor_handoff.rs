//! Private checked identity binding. This neither transfers execution nor
//! acquires a conversation worker or scheduler attachment. A future caller must
//! hold the conversation worker/poll fence before the task gate, fence old
//! executors, and include `checks` in every dependent transaction.

use crate::{
    runtime::{Conversation, Harness, ToolAttempt},
    task::{TaskOwner, TaskState},
    task_store::TaskStore,
};
use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Task(#[from] crate::task_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Document(#[from] crate::document::Error),
    #[error("conversation or task attempt is stale, closed or unowned")]
    Stale,
    #[error("task attempt is already bound to a different conversation attempt")]
    Bound,
    #[error("conversation executor is already held")]
    Busy,
    #[error("conversation execution epoch is cancelled")]
    Cancelled,
    #[error("execution fence poisoned")]
    Poisoned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub conversation: Uuid,
    pub tool: ToolAttempt,
    pub task: Uuid,
    pub task_attempt: Uuid,
}

/// Exclusive among conversation workers that honor the shared control. The
/// scheduler task executor is a separate lease and must be retired by the host
/// before a future dispatch may use this capability. Never hold the task gate
/// while acquiring this lease or await while holding either synchronous gate.
pub(crate) struct ConversationLease {
    owner: Arc<durable_store::owner::OwnedStore>,
    control: durable_store::controls::Control,
    binding: Binding,
    _worker: tokio::sync::OwnedMutexGuard<()>,
    failed: AtomicBool,
    commit_lock: Mutex<()>,
}

impl ConversationLease {
    pub(crate) fn acquire(
        owner: Arc<durable_store::owner::OwnedStore>,
        binding: Binding,
    ) -> Result<Self, Error> {
        let control = owner.conversation_controls().get(binding.conversation);
        let worker = control
            .worker
            .clone()
            .try_lock_owned()
            .map_err(|_| Error::Busy)?;
        let lease = Self {
            owner,
            control,
            binding,
            _worker: worker,
            failed: AtomicBool::new(false),
            commit_lock: Mutex::new(()),
        };
        lease.commit(crate::task_store::OwnedBatch {
            checks: vec![],
            mutations: vec![],
        })?;
        Ok(lease)
    }

    /// Commits a fully checked batch under both gates. The caller must stop on
    /// any error and inspect after uncertain storage outcomes; no external
    /// effects or asynchronous work may run inside these short-lived fences.
    pub(crate) fn commit(&self, batch: crate::task_store::OwnedBatch) -> Result<(), Error> {
        let _serial = self.commit_lock.lock().map_err(|_| Error::Poisoned)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::Poisoned);
        }
        let result = self.commit_inner(batch);
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }

    fn commit_inner(&self, batch: crate::task_store::OwnedBatch) -> Result<(), Error> {
        let _poll = self.control.gate.lock().map_err(|_| Error::Poisoned)?;
        if self.control.cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let _task = self.owner.task_gate().lock().map_err(|_| Error::Poisoned)?;
        if self.control.cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mut fences = checks(self.owner.store(), &self.binding)?;
        // Preserve a timed signal's stronger deadline check when it overlaps a
        // lease check, as in keyed child park/document composition.
        for fence in &mut fences {
            if let Expected::Version(version) = fence.expected
                && let Some(stronger) = batch.checks.iter().find(|c| {
                    c.key == fence.key
                        && matches!(c.expected, Expected::VersionBefore(v, _) if v == version)
                })
            {
                fence.expected = stronger.expected.clone();
            }
        }
        let mut tx = crate::document::Transaction::new();
        tx.append(crate::task_store::OwnedBatch {
            checks: fences,
            mutations: vec![],
        })?;
        tx.append(batch)?;
        tx.commit(self.owner.store())?;
        Ok(())
    }
}

fn key(task: Uuid) -> Vec<u8> {
    format!("durable/executor-handoff/{task}").into_bytes()
}

/// Prepare a checked binding. Replacing an old binding requires a *new* task
/// attempt; a caller must also establish that its predecessor has stopped.
/// No execution or handoff becomes authorized by this record alone.
pub(crate) fn prepare_binding(
    store: &Store,
    binding: &Binding,
) -> Result<crate::task_store::OwnedBatch, Error> {
    let mut checks = active_checks(store, binding)?;
    let key = key(binding.task);
    let entry = store.get(&key)?;
    let mut mutations = Vec::new();
    if let Some(existing) = &entry {
        let existing: Binding = serde_json::from_slice(&existing.value)?;
        if existing.task != binding.task || existing.conversation != binding.conversation {
            return Err(Error::Bound);
        }
        if existing != *binding {
            if existing.task_attempt == binding.task_attempt {
                return Err(Error::Bound);
            }
            mutations.push(Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(binding)?,
            });
        }
    } else {
        mutations.push(Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(binding)?,
        });
    }
    checks.push(Check {
        key,
        expected: entry.map_or(Expected::Missing, |e| Expected::Version(e.version)),
    });
    Ok(crate::task_store::OwnedBatch { checks, mutations })
}

/// Checks for composition with a dependent write. The checks are provisional
/// until the same transaction commits; checking and writing separately races.
pub(crate) fn checks(store: &Store, binding: &Binding) -> Result<Vec<Check>, Error> {
    let mut checks = active_checks(store, binding)?;
    let key = key(binding.task);
    let entry = store.get(&key)?.ok_or(Error::Stale)?;
    let stored: Binding = serde_json::from_slice(&entry.value)?;
    if stored != *binding {
        return Err(Error::Stale);
    }
    checks.push(Check {
        key,
        expected: Expected::Version(entry.version),
    });
    Ok(checks)
}

fn active_checks(store: &Store, binding: &Binding) -> Result<Vec<Check>, Error> {
    let conversation_key = Harness::key(binding.conversation);
    let entry = store.get(&conversation_key)?.ok_or(Error::Stale)?;
    let conversation: Conversation = serde_json::from_slice(&entry.value)?;
    if conversation.id != binding.conversation
        || conversation.active_tool_attempt() != Some(binding.tool)
    {
        return Err(Error::Stale);
    }
    let task = TaskStore::new(store)
        .load(binding.task)?
        .ok_or(Error::Stale)?;
    if task.task.owner() != &TaskOwner::Conversation(binding.conversation)
        || task.task.state() != &TaskState::Running
        || task.task.abort_requested()
        || task.task.attempt_id() != Some(binding.task_attempt)
    {
        return Err(Error::Stale);
    }
    Ok(vec![
        Check {
            key: conversation_key,
            expected: Expected::Version(entry.version),
        },
        Check {
            key: crate::task_store::key(binding.task),
            expected: Expected::Version(task.version),
        },
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, Task};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;

    fn binding() -> Binding {
        Binding {
            conversation: Uuid::from_u128(1),
            tool: ToolAttempt {
                operation_id: Uuid::from_u128(2),
                attempt_id: Uuid::from_u128(3),
            },
            task: Uuid::from_u128(4),
            task_attempt: Uuid::from_u128(5),
        }
    }
    fn conversation(b: &Binding) -> serde_json::Value {
        json!({"id":b.conversation,"config":{"model":"test","system_prompt":"","max_output_tokens":1},
            "messages":[],"submissions":[],"active":null,"tool_attempt":b.tool,"abort_requested":false,
            "phase":{"ToolRunning":{"turns":0,"calls":[],"index":0,"operation_id":b.tool.operation_id,
                "replay":"Safe","arguments":{},"restarts":0,"replay_ready":false}}})
    }
    fn put_conversation(db: &Store, value: &serde_json::Value, expected: Expected) {
        let key = Harness::key(binding().conversation);
        db.commit(
            &[Check {
                key: key.clone(),
                expected,
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(value).unwrap(),
            }],
        )
        .unwrap();
    }
    fn setup(db: &Store) -> Binding {
        let b = binding();
        put_conversation(db, &conversation(&b), Expected::Missing);
        let mut task = Task::new(
            b.task,
            TaskOwner::Conversation(b.conversation),
            Checkpoint {
                phase: "one".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), true);
        task.start_attempt(b.task_attempt).unwrap();
        TaskStore::new(db).save(&[Save::Create(&task)]).unwrap();
        b
    }
    fn commit_binding(db: &Store, b: &Binding) {
        let batch = prepare_binding(db, b).unwrap();
        db.commit(&batch.checks, &batch.mutations).unwrap();
    }

    fn effect_batch() -> crate::task_store::OwnedBatch {
        let key = b"test/lease-effect".to_vec();
        crate::task_store::OwnedBatch {
            checks: vec![Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            mutations: vec![Mutation::Put {
                key,
                value: vec![1],
            }],
        }
    }

    #[test]
    fn lease_excludes_other_conversation_workers_and_rechecks_abort() {
        use durable_store::owner::OwnedStore;
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
        let b = setup(owner.store());
        commit_binding(owner.store(), &b);
        let lease = ConversationLease::acquire(owner.clone(), b.clone()).unwrap();
        assert!(matches!(
            ConversationLease::acquire(owner.clone(), b.clone()),
            Err(Error::Busy)
        ));
        assert!(
            owner
                .conversation_controls()
                .get(b.conversation)
                .worker
                .try_lock_owned()
                .is_err()
        );
        let tasks = TaskStore::new(owner.store());
        let task = tasks.load(b.task).unwrap().unwrap();
        tasks.update(&task, |t| t.request_abort()).unwrap();
        assert!(matches!(lease.commit(effect_batch()), Err(Error::Stale)));
        assert!(owner.store().get(b"test/lease-effect").unwrap().is_none());
        drop(lease);
        assert!(matches!(
            ConversationLease::acquire(owner, b),
            Err(Error::Stale)
        ));
    }

    #[test]
    fn lease_checks_epoch_and_replayed_task_before_committing() {
        use durable_store::owner::OwnedStore;
        for cancel_epoch in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
            let b = setup(owner.store());
            commit_binding(owner.store(), &b);
            let lease = ConversationLease::acquire(owner.clone(), b.clone()).unwrap();
            if cancel_epoch {
                owner
                    .conversation_controls()
                    .fence(|| Ok::<_, ()>(vec![b.conversation]))
                    .unwrap();
                assert!(matches!(
                    lease.commit(effect_batch()),
                    Err(Error::Cancelled)
                ));
            } else {
                let tasks = TaskStore::new(owner.store());
                let running = tasks.load(b.task).unwrap().unwrap();
                tasks.update(&running, |t| t.recover()).unwrap();
                let ready = tasks.load(b.task).unwrap().unwrap();
                tasks
                    .update(&ready, |t| t.start_attempt(Uuid::from_u128(88)))
                    .unwrap();
                assert!(matches!(lease.commit(effect_batch()), Err(Error::Stale)));
            }
            assert!(owner.store().get(b"test/lease-effect").unwrap().is_none());
        }
    }

    #[test]
    fn lease_commit_is_atomic_with_binding_fences() {
        use durable_store::owner::OwnedStore;
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
        let b = setup(owner.store());
        commit_binding(owner.store(), &b);
        let lease = ConversationLease::acquire(owner.clone(), b).unwrap();
        lease.commit(effect_batch()).unwrap();
        assert_eq!(
            owner
                .store()
                .get(b"test/lease-effect")
                .unwrap()
                .unwrap()
                .value,
            vec![1]
        );
        assert!(matches!(
            lease.commit(effect_batch()),
            Err(Error::Document(crate::document::Error::Store(
                durable_store::Error::Conflict { .. }
            )))
        ));
        assert!(matches!(
            lease.commit(crate::task_store::OwnedBatch {
                checks: vec![],
                mutations: vec![]
            }),
            Err(Error::Poisoned)
        ));
    }

    #[test]
    fn lease_preserves_stronger_overlapping_deadline_check() {
        use durable_store::owner::OwnedStore;
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
        let b = setup(owner.store());
        commit_binding(owner.store(), &b);
        let lease = ConversationLease::acquire(owner.clone(), b.clone()).unwrap();
        let task = TaskStore::new(owner.store()).load(b.task).unwrap().unwrap();
        let mut batch = effect_batch();
        batch.checks.push(Check {
            key: crate::task_store::key(b.task),
            expected: Expected::VersionBefore(task.version, std::time::Instant::now()),
        });
        assert!(matches!(
            lease.commit(batch),
            Err(Error::Document(crate::document::Error::Store(
                durable_store::Error::Conflict { .. }
            )))
        ));
        assert!(owner.store().get(b"test/lease-effect").unwrap().is_none());
    }

    #[test]
    fn lease_crash_child() {
        use durable_store::owner::OwnedStore;
        let Ok(path) = std::env::var("PI_RS_LEASE_CRASH_DB") else {
            return;
        };
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        let b = setup(owner.store());
        commit_binding(owner.store(), &b);
        let _lease = ConversationLease::acquire(owner, b).unwrap();
        std::fs::write(format!("{path}.ready"), b"held").unwrap();
        std::process::abort();
    }

    #[test]
    fn lease_owner_is_released_after_process_death() {
        use durable_store::owner::OwnedStore;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "executor_handoff::tests::lease_crash_child",
                "--nocapture",
            ])
            .env("PI_RS_LEASE_CRASH_DB", &path)
            .status()
            .unwrap();
        assert!(!status.success());
        assert_eq!(
            std::fs::read(format!("{}.ready", path.display())).unwrap(),
            b"held"
        );
        let owner = Arc::new(OwnedStore::open(&path).unwrap());
        // Recovery is explicit: process death releases locks but does not by
        // itself revoke the old durable attempts.
        let old = binding();
        let tasks = TaskStore::new(owner.store());
        let running = tasks.load(old.task).unwrap().unwrap();
        tasks.update(&running, |t| t.recover()).unwrap();
        let ready = tasks.load(old.task).unwrap().unwrap();
        let mut next = old.clone();
        next.task_attempt = Uuid::from_u128(101);
        next.tool.attempt_id = Uuid::from_u128(102);
        tasks
            .update(&ready, |t| t.start_attempt(next.task_attempt))
            .unwrap();
        let conv = owner
            .store()
            .get(&Harness::key(old.conversation))
            .unwrap()
            .unwrap();
        put_conversation(
            owner.store(),
            &conversation(&next),
            Expected::Version(conv.version),
        );
        assert!(matches!(
            ConversationLease::acquire(owner.clone(), old),
            Err(Error::Stale)
        ));
        commit_binding(owner.store(), &next);
        let lease = ConversationLease::acquire(owner.clone(), next).unwrap();
        lease.commit(effect_batch()).unwrap();
    }

    #[test]
    fn binding_reopen_replay_and_stale_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let old = setup(&db);
        commit_binding(&db, &old);
        let stale_checks = checks(&db, &old).unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        commit_binding(&db, &old); // same reservation is idempotent
        let tasks = TaskStore::new(&db);
        let snapshot = tasks.load(old.task).unwrap().unwrap();
        tasks.update(&snapshot, |task| task.recover()).unwrap();
        let snapshot = tasks.load(old.task).unwrap().unwrap();
        let next_attempt = Uuid::from_u128(6);
        tasks
            .update(&snapshot, |task| task.start_attempt(next_attempt))
            .unwrap();
        let mut next = old.clone();
        next.task_attempt = next_attempt;
        next.tool.attempt_id = Uuid::from_u128(7);
        let conv_key = Harness::key(old.conversation);
        let previous = db.get(&conv_key).unwrap().unwrap();
        put_conversation(
            &db,
            &conversation(&next),
            Expected::Version(previous.version),
        );
        assert!(matches!(
            db.commit(
                &stale_checks,
                &[Mutation::Put {
                    key: b"effect".to_vec(),
                    value: vec![1]
                }]
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(db.get(b"effect").unwrap().is_none());
        assert!(matches!(checks(&db, &old), Err(Error::Stale)));
        commit_binding(&db, &next);
        assert!(checks(&db, &next).is_ok());
        assert!(matches!(checks(&db, &old), Err(Error::Stale)));
    }

    #[test]
    fn aborts_and_concurrent_change_fence_bindings() {
        for abort_task in [false, true] {
            let db = Store::memory().unwrap();
            let b = setup(&db);
            let prepared = prepare_binding(&db, &b).unwrap();
            if abort_task {
                let tasks = TaskStore::new(&db);
                let snapshot = tasks.load(b.task).unwrap().unwrap();
                tasks
                    .update(&snapshot, |task| task.request_abort())
                    .unwrap();
            } else {
                let key = Harness::key(b.conversation);
                let entry = db.get(&key).unwrap().unwrap();
                let mut value = conversation(&b);
                value["abort_requested"] = json!(true);
                put_conversation(&db, &value, Expected::Version(entry.version));
            }
            assert!(matches!(
                db.commit(&prepared.checks, &prepared.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(matches!(prepare_binding(&db, &b), Err(Error::Stale)));
            assert!(db.get(&key(b.task)).unwrap().is_none());
        }
    }

    #[test]
    fn same_task_attempt_cannot_bind_another_conversation_attempt() {
        let db = Store::memory().unwrap();
        let b = setup(&db);
        commit_binding(&db, &b);
        let mut changed = b.clone();
        changed.tool.attempt_id = Uuid::from_u128(99);
        let entry = db.get(&Harness::key(b.conversation)).unwrap().unwrap();
        put_conversation(
            &db,
            &conversation(&changed),
            Expected::Version(entry.version),
        );
        assert!(matches!(prepare_binding(&db, &changed), Err(Error::Bound)));
    }

    #[test]
    fn legacy_running_task_cannot_bind_without_scheduler_attempt() {
        let db = Store::memory().unwrap();
        let b = setup(&db);
        let tasks = TaskStore::new(&db);
        let snapshot = tasks.load(b.task).unwrap().unwrap();
        tasks.update(&snapshot, |task| task.recover()).unwrap();
        let snapshot = tasks.load(b.task).unwrap().unwrap();
        tasks.update(&snapshot, |task| task.start()).unwrap();
        assert!(matches!(prepare_binding(&db, &b), Err(Error::Stale)));
    }

    #[test]
    fn deserialized_legacy_running_task_fails_closed() {
        let db = Store::memory().unwrap();
        let b = setup(&db);
        let task_key = crate::task_store::key(b.task);
        let entry = db.get(&task_key).unwrap().unwrap();
        let mut legacy: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
        legacy.as_object_mut().unwrap().remove("attempt_id");
        db.commit(
            &[Check {
                key: task_key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key: task_key,
                value: serde_json::to_vec(&legacy).unwrap(),
            }],
        )
        .unwrap();
        let task = TaskStore::new(&db).load(b.task).unwrap().unwrap().task;
        assert_eq!(task.state(), &TaskState::Running);
        assert_eq!(task.attempt_id(), None);
        assert!(matches!(prepare_binding(&db, &b), Err(Error::Stale)));
    }

    #[test]
    fn replay_ready_fences_prepared_and_committed_bindings() {
        let db = Store::memory().unwrap();
        let b = setup(&db);
        commit_binding(&db, &b);
        let stale = checks(&db, &b).unwrap();
        let key = Harness::key(b.conversation);
        let entry = db.get(&key).unwrap().unwrap();
        let mut value = conversation(&b);
        value["phase"]["ToolRunning"]["replay_ready"] = json!(true);
        put_conversation(&db, &value, Expected::Version(entry.version));
        assert!(matches!(checks(&db, &b), Err(Error::Stale)));
        assert!(matches!(prepare_binding(&db, &b), Err(Error::Stale)));
        assert!(matches!(
            db.commit(
                &stale,
                &[Mutation::Put {
                    key: b"replayed-effect".to_vec(),
                    value: vec![1],
                }]
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(db.get(b"replayed-effect").unwrap().is_none());
    }

    #[test]
    fn scheduler_recovery_reserves_a_fresh_persisted_attempt() {
        use crate::task_scheduler::{Registry, Scheduler, Step};
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let old = setup(&db);
        commit_binding(&db, &old);
        drop(db);
        let seen = Arc::new(Mutex::new(None));
        let observed = seen.clone();
        let reader_path = path.clone();
        let mut registry = Registry::default();
        registry.register("parent", true, move |task: &Task| {
            let reader = Store::open(&reader_path).unwrap();
            let stored = TaskStore::new(&reader).load(task.id()).unwrap().unwrap();
            assert_eq!(stored.task.attempt_id(), task.attempt_id());
            assert!(task.attempt_id().is_some());
            *observed.lock().unwrap() = task.attempt_id();
            Ok(Step::Succeed(json!("done")))
        });
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(
            scheduler.get(old.task).unwrap().unwrap().state(),
            &TaskState::Ready
        );
        assert!(scheduler.tick(0).unwrap());
        let new_attempt = seen.lock().unwrap().unwrap();
        assert_ne!(new_attempt, old.task_attempt);
        assert_eq!(scheduler.get(old.task).unwrap().unwrap().attempt_id(), None);
        drop(scheduler);
        let db = Store::open(&path).unwrap();
        assert!(matches!(checks(&db, &old), Err(Error::Stale)));
    }

    #[test]
    fn migration_reservation_fences_the_previous_attempt() {
        use crate::task_scheduler::{Registry, Scheduler, Step};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let old = setup(&db);
        commit_binding(&db, &old);
        let stale = checks(&db, &old).unwrap();
        drop(db);
        let reader_path = path.clone();
        let mut registry = Registry::default();
        registry.register_versioned(
            "parent",
            2,
            true,
            move |task: &Task| {
                assert_eq!(task.definition_version(), 2);
                assert_ne!(task.attempt_id(), Some(old.task_attempt));
                assert!(task.attempt_id().is_some());
                let db = Store::open(&reader_path).unwrap();
                assert!(matches!(
                    db.commit(
                        &stale,
                        &[Mutation::Put {
                            key: b"old-effect".to_vec(),
                            value: vec![1],
                        }]
                    ),
                    Err(durable_store::Error::Conflict { .. })
                ));
                assert!(db.get(b"old-effect").unwrap().is_none());
                Ok(Step::Succeed(json!(null)))
            },
            Some(Arc::new(|_, checkpoint, _| {
                Ok((json!({"migrated": true}), checkpoint.clone()))
            })),
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert!(scheduler.tick(0).unwrap());
    }
}
