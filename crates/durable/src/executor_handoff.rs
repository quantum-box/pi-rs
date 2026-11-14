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
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Task(#[from] crate::task_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("conversation or task attempt is stale, closed or unowned")]
    Stale,
    #[error("task attempt is already bound to a different conversation attempt")]
    Bound,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub conversation: Uuid,
    pub tool: ToolAttempt,
    pub task: Uuid,
    pub task_attempt: Uuid,
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
}
