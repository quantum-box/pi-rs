//! Internal storage foundation for keyed, task-owned children. This is not a
//! scheduler lease or a tool park: callers must fence the parent executor and
//! validate the child's registration before admission. Conflicts and uncertain
//! commits require a fresh parent snapshot and inspection before retrying.

use crate::task::{Task, TaskOwner, TaskState};
use crate::task_store::{Error as TaskError, Save, StoredTask, TaskStore};
use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error(transparent)]
    Task(#[from] TaskError),
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("invalid key, child, or tool identity")]
    Invalid,
    #[error("parent is stale, missing, or closed")]
    ParentClosed,
    #[error("key already belongs to a different child identity")]
    IdentityMismatch,
    #[error("stored child is missing or has a different owner")]
    InvalidStoredChild,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    tool: String,
    arguments: Value,
    replay_safe: bool,
    task_kind: String,
    task_version: u32,
    initial_checkpoint: crate::task::Checkpoint,
    restart_limit: u32,
}

#[derive(Serialize, Deserialize)]
struct Record {
    child: Uuid,
    identity: Identity,
}

struct Prepared {
    child: Uuid,
    checks: Vec<Check>,
    mutations: Vec<Mutation>,
}

fn record_key(parent: Uuid, key: &str) -> Vec<u8> {
    let hex: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    format!("durable/keyed-child/{parent}/{hex}").into_bytes()
}

/// Child IDs supplied on duplicate requests are candidates, not identity. The
/// original ID wins. A successful return is linearized against parent closure.
pub(crate) fn admit(
    store: &Store,
    parent: &StoredTask,
    key: &str,
    tool: &str,
    arguments: &Value,
    child: &Task,
) -> Result<Uuid, Error> {
    let prepared = prepare(store, parent, key, tool, arguments, child)?;
    store.commit(&prepared.checks, &prepared.mutations)?;
    Ok(prepared.child)
}

fn prepare(
    store: &Store,
    parent: &StoredTask,
    key: &str,
    tool: &str,
    arguments: &Value,
    child: &Task,
) -> Result<Prepared, Error> {
    let positive_integer = !key.starts_with('0') && key.bytes().all(|b| b.is_ascii_digit());
    let kind = child
        .kind()
        .filter(|s| !s.is_empty())
        .ok_or(Error::Invalid)?;
    if key.is_empty()
        || key.contains('/')
        || key == "__proto__"
        || positive_integer
        || tool.is_empty()
        || child.owner() != &TaskOwner::Task(parent.task.id())
        || child.id() == parent.task.id()
        || child.state() != &TaskState::Ready
        || child.abort_requested()
        || child.input() != arguments
    {
        return Err(Error::Invalid);
    }
    let tasks = TaskStore::new(store);
    let current = tasks.load(parent.task.id())?.ok_or(Error::ParentClosed)?;
    if current != *parent
        || current.task.state() != &TaskState::Running
        || current.task.abort_requested()
    {
        return Err(Error::ParentClosed);
    }
    let identity = Identity {
        tool: tool.into(),
        arguments: arguments.clone(),
        replay_safe: child.replay_safe(),
        task_kind: kind.into(),
        task_version: child.definition_version(),
        initial_checkpoint: child.checkpoint().clone(),
        restart_limit: child.restart_limit(),
    };
    let key = record_key(parent.task.id(), key);
    let parent_key = crate::task_store::key(parent.task.id());
    let parent_check = Check {
        key: parent_key,
        expected: Expected::Version(parent.version),
    };
    if let Some(entry) = store.get(&key)? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if record.identity != identity {
            return Err(Error::IdentityMismatch);
        }
        let existing = tasks.load(record.child)?.ok_or(Error::InvalidStoredChild)?;
        if existing.task.owner() != &TaskOwner::Task(parent.task.id()) {
            return Err(Error::InvalidStoredChild);
        }
        return Ok(Prepared {
            child: record.child,
            checks: vec![
                parent_check,
                Check {
                    key,
                    expected: Expected::Version(entry.version),
                },
                Check {
                    key: crate::task_store::key(record.child),
                    expected: Expected::Version(existing.version),
                },
            ],
            mutations: vec![],
        });
    }
    let mut batch = tasks.prepare_save(&[Save::Create(child)])?;
    batch.checks.push(parent_check);
    // Abort and settlement discover children before committing parent updates.
    // Advance the parent version so a traversal that missed this child retries.
    batch.mutations.push(Mutation::Put {
        key: crate::task_store::key(parent.task.id()),
        value: serde_json::to_vec(&parent.task)?,
    });
    batch.checks.push(Check {
        key: key.clone(),
        expected: Expected::Missing,
    });
    batch.mutations.push(Mutation::Put {
        key,
        value: serde_json::to_vec(&Record {
            child: child.id(),
            identity,
        })?,
    });
    Ok(Prepared {
        child: child.id(),
        checks: batch.checks,
        mutations: batch.mutations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Checkpoint;
    use serde_json::json;

    fn parent(store: &Store) -> StoredTask {
        let mut task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(99)),
            Checkpoint {
                phase: "parent".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap();
        task.start().unwrap();
        let tasks = TaskStore::new(store);
        tasks.save(&[Save::Create(&task)]).unwrap();
        tasks.load(task.id()).unwrap().unwrap()
    }

    fn child(id: u128, owner: Uuid, safe: bool, input: Value) -> Task {
        Task::new(
            Uuid::from_u128(id),
            TaskOwner::Task(owner),
            Checkpoint {
                phase: "tool".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("tool-task".into(), safe, 1, input)
        .unwrap()
    }

    #[test]
    fn duplicate_identity_and_independent_keys_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let db = Store::open(&path).unwrap();
        let p = parent(&db);
        let a = child(2, p.task.id(), true, json!({"x": 1}));
        let b = child(3, p.task.id(), true, json!({"x": 1}));
        assert_eq!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &a).unwrap(),
            a.id()
        );
        let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
        assert_eq!(
            admit(&db, &p, "b", "read", &json!({"x":1}), &b).unwrap(),
            b.id()
        );
        drop(db);
        let db = Store::open(&path).unwrap();
        let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
        let candidate = child(4, p.task.id(), true, json!({"x":1}));
        assert_eq!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &candidate).unwrap(),
            a.id()
        );
        assert!(TaskStore::new(&db).load(candidate.id()).unwrap().is_none());
        assert!(matches!(
            admit(&db, &p, "a", "write", &json!({"x":1}), &candidate),
            Err(Error::IdentityMismatch)
        ));
        let changed = child(4, p.task.id(), true, json!({"x":2}));
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"x":2}), &changed),
            Err(Error::IdentityMismatch)
        ));
        let unsafe_child = child(4, p.task.id(), false, json!({"x":1}));
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &unsafe_child),
            Err(Error::IdentityMismatch)
        ));
        let other_kind = child(4, p.task.id(), true, json!({"x":1}))
            .registered_versioned("other-task".into(), true, 1, json!({"x":1}))
            .unwrap();
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &other_kind),
            Err(Error::IdentityMismatch)
        ));
        let other_version = child(4, p.task.id(), true, json!({"x":1}))
            .registered_versioned("tool-task".into(), true, 2, json!({"x":1}))
            .unwrap();
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &other_version),
            Err(Error::IdentityMismatch)
        ));
        let changed_checkpoint = Task::new(
            Uuid::from_u128(4),
            TaskOwner::Task(p.task.id()),
            Checkpoint {
                phase: "other".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("tool-task".into(), true, 1, json!({"x":1}))
        .unwrap();
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &changed_checkpoint),
            Err(Error::IdentityMismatch)
        ));
        let changed_limit = Task::new(
            Uuid::from_u128(4),
            TaskOwner::Task(p.task.id()),
            Checkpoint {
                phase: "tool".into(),
                data: json!({}),
            },
            4,
        )
        .unwrap()
        .registered_versioned("tool-task".into(), true, 1, json!({"x":1}))
        .unwrap();
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"x":1}), &changed_limit),
            Err(Error::IdentityMismatch)
        ));
        assert_eq!(TaskStore::new(&db).list().unwrap().len(), 3);
    }

    #[test]
    fn prepared_duplicate_race_commits_only_one_child() {
        let db = Store::memory().unwrap();
        let p = parent(&db);
        let a = child(2, p.task.id(), true, json!({}));
        let b = child(3, p.task.id(), true, json!({}));
        let first = prepare(&db, &p, "same", "read", &json!({}), &a).unwrap();
        let second = prepare(&db, &p, "same", "read", &json!({}), &b).unwrap();
        db.commit(&first.checks, &first.mutations).unwrap();
        assert!(matches!(
            db.commit(&second.checks, &second.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
        assert_eq!(
            admit(&db, &p, "same", "read", &json!({}), &b).unwrap(),
            a.id()
        );
        assert!(TaskStore::new(&db).load(b.id()).unwrap().is_none());
    }

    #[test]
    fn independent_prepared_keys_retry_after_parent_version_changes() {
        let db = Store::memory().unwrap();
        let p = parent(&db);
        let a = child(2, p.task.id(), true, json!({}));
        let b = child(3, p.task.id(), true, json!({}));
        let first = prepare(&db, &p, "a", "read", &json!({}), &a).unwrap();
        let second = prepare(&db, &p, "b", "read", &json!({}), &b).unwrap();
        db.commit(&first.checks, &first.mutations).unwrap();
        assert!(matches!(
            db.commit(&second.checks, &second.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
        assert_eq!(admit(&db, &p, "b", "read", &json!({}), &b).unwrap(), b.id());
        let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
        assert_eq!(admit(&db, &p, "a", "read", &json!({}), &b).unwrap(), a.id());
        assert_eq!(admit(&db, &p, "b", "read", &json!({}), &a).unwrap(), b.id());
        assert_eq!(TaskStore::new(&db).list().unwrap().len(), 3);
    }

    #[test]
    fn parent_abort_and_terminal_races_fence_both_create_and_lookup() {
        for terminal in [false, true] {
            for existing in [false, true] {
                let db = Store::memory().unwrap();
                let p = parent(&db);
                let c = child(2, p.task.id(), true, json!({}));
                if existing {
                    admit(&db, &p, "a", "read", &json!({}), &c).unwrap();
                }
                let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
                let pending = prepare(&db, &p, "a", "read", &json!({}), &c).unwrap();
                let tasks = TaskStore::new(&db);
                tasks
                    .update(&p, |t| {
                        if terminal {
                            t.succeed(json!(null))
                        } else {
                            t.request_abort()
                        }
                    })
                    .unwrap();
                assert!(matches!(
                    db.commit(&pending.checks, &pending.mutations),
                    Err(durable_store::Error::Conflict { .. })
                ));
                assert!(matches!(
                    admit(&db, &p, "a", "read", &json!({}), &c),
                    Err(Error::ParentClosed)
                ));
                let closed = tasks.load(p.task.id()).unwrap().unwrap();
                assert!(matches!(
                    admit(&db, &closed, "a", "read", &json!({}), &c),
                    Err(Error::ParentClosed)
                ));
                assert_eq!(tasks.load(c.id()).unwrap().is_some(), existing);
            }
        }
    }

    #[test]
    fn admission_fences_abort_snapshot_that_missed_child() {
        let db = Store::memory().unwrap();
        let p = parent(&db);
        let c = child(2, p.task.id(), true, json!({}));
        let mut abort = p.clone();
        abort.task.request_abort().unwrap();
        let tasks = TaskStore::new(&db);
        let old_traversal = tasks.prepare_save(&[Save::Update(&abort)]).unwrap();
        admit(&db, &p, "a", "read", &json!({}), &c).unwrap();
        assert!(matches!(
            db.commit(&old_traversal.checks, &old_traversal.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        crate::task_scheduler::request_abort_subtree(&tasks, p.task.id()).unwrap();
        assert!(tasks.load(c.id()).unwrap().unwrap().task.abort_requested());
    }

    #[test]
    fn admission_rejects_cycle_through_missing_ancestor() {
        let db = Store::memory().unwrap();
        let mut p = child(1, Uuid::from_u128(2), true, json!({}));
        p.start().unwrap();
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&p)]).unwrap();
        let p = tasks.load(p.id()).unwrap().unwrap();
        let c = child(2, p.task.id(), true, json!({}));
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({}), &c),
            Err(Error::Task(TaskError::InvalidOwnership))
        ));
        assert!(db.get(&record_key(p.task.id(), "a")).unwrap().is_none());
        assert!(tasks.load(c.id()).unwrap().is_none());
    }

    #[test]
    fn rejects_foreign_or_invalid_children_and_inconsistent_records() {
        let db = Store::memory().unwrap();
        let p = parent(&db);
        let wrong_owner = child(2, Uuid::from_u128(77), true, json!({}));
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({}), &wrong_owner),
            Err(Error::Invalid)
        ));
        let c = child(2, p.task.id(), true, json!({}));
        let mut forged = p.clone();
        forged.task.cancel().unwrap();
        assert!(matches!(
            admit(&db, &forged, "a", "read", &json!({}), &c),
            Err(Error::ParentClosed)
        ));
        for key in ["", "1", "__proto__", "a/b"] {
            assert!(matches!(
                admit(&db, &p, key, "read", &json!({}), &c),
                Err(Error::Invalid)
            ));
        }
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({"wrong":true}), &c),
            Err(Error::Invalid)
        ));
        admit(&db, &p, "a", "read", &json!({}), &c).unwrap();
        let p = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
        let child_key = crate::task_store::key(c.id());
        db.commit(&[], &[Mutation::Delete { key: child_key }])
            .unwrap();
        assert!(matches!(
            admit(&db, &p, "a", "read", &json!({}), &c),
            Err(Error::InvalidStoredChild)
        ));
    }

    #[test]
    fn uncommitted_preparation_leaves_no_artifacts_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let db = Store::open(&path).unwrap();
        let p = parent(&db);
        let c = child(2, p.task.id(), true, json!({}));
        let _uncommitted = prepare(&db, &p, "a", "read", &json!({}), &c).unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        assert!(db.get(&record_key(p.task.id(), "a")).unwrap().is_none());
        assert!(TaskStore::new(&db).load(c.id()).unwrap().is_none());
        assert_eq!(admit(&db, &p, "a", "read", &json!({}), &c).unwrap(), c.id());
    }
}
