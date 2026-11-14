//! Internal storage foundation for keyed, task-owned children. This is not a
//! scheduler lease or a tool park: callers must fence the parent executor and
//! validate the child's registration before admission. Conflicts and uncertain
//! commits require a fresh parent snapshot and inspection before retrying.

use crate::task::{Checkpoint, Task, TaskOwner, TaskState, WaitFor};
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
    #[error(transparent)]
    Document(#[from] crate::document::Error),
    #[error("invalid key, child, or tool identity")]
    Invalid,
    #[error("parent is stale, missing, or closed")]
    ParentClosed,
    #[error("key already belongs to a different child identity")]
    IdentityMismatch,
    #[error("stored child is missing or has a different owner")]
    InvalidStoredChild,
    #[error("wait must contain distinct existing keyed children")]
    InvalidWait,
    #[error("document context belongs to a different store or task invocation")]
    DocumentContext,
    #[error("task gate poisoned")]
    GatePoisoned,
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

/// Commit staged task documents and a keyed park together. The caller must
/// retain and fence the parent executor through this call; the task gate only
/// serializes graph decisions and is not an executor lease. Do not call while
/// holding the task gate. A conflict or uncertain commit requires inspection.
pub(crate) fn park_with_documents(
    owner: &durable_store::owner::OwnedStore,
    parent: &StoredTask,
    checkpoint: Checkpoint,
    keys: &[&str],
    documents: crate::document::TaskDocuments<'_>,
) -> Result<(), Error> {
    let _gate = owner.task_gate().lock().map_err(|_| Error::GatePoisoned)?;
    let transaction =
        prepare_park_with_documents(owner.store(), parent, checkpoint, keys, documents)?;
    transaction.commit(owner.store())?;
    Ok(())
}

fn prepare_park_with_documents(
    store: &Store,
    parent: &StoredTask,
    checkpoint: Checkpoint,
    keys: &[&str],
    documents: crate::document::TaskDocuments<'_>,
) -> Result<crate::document::Transaction, Error> {
    if !documents.belongs_to(store, &parent.task) {
        return Err(Error::DocumentContext);
    }
    let mut transaction = crate::document::Transaction::new();
    transaction.append(documents.into_batch()?)?;
    transaction.append(prepare_park_any(store, parent, checkpoint, keys)?)?;
    Ok(transaction)
}

/// Persist a checkpoint and an any-child wait in one checked transaction. The
/// caller must fence the parent executor, hold the scheduler's task gate, and
/// reload after admission or a conflict. This does not start or stop workers.
pub(crate) fn park_any(
    store: &Store,
    parent: &StoredTask,
    checkpoint: Checkpoint,
    keys: &[&str],
) -> Result<(), Error> {
    let batch = prepare_park_any(store, parent, checkpoint, keys)?;
    store.commit(&batch.checks, &batch.mutations)?;
    Ok(())
}

fn prepare_park_any(
    store: &Store,
    parent: &StoredTask,
    checkpoint: Checkpoint,
    keys: &[&str],
) -> Result<crate::task_store::OwnedBatch, Error> {
    let tasks = TaskStore::new(store);
    let current = tasks.load(parent.task.id())?.ok_or(Error::ParentClosed)?;
    if current != *parent
        || current.task.state() != &TaskState::Running
        || current.task.abort_requested()
    {
        return Err(Error::ParentClosed);
    }
    if keys.is_empty() {
        return Err(Error::InvalidWait);
    }
    let mut seen_keys = std::collections::HashSet::new();
    let mut seen_children = std::collections::HashSet::new();
    let mut ids = Vec::with_capacity(keys.len());
    let mut checks = Vec::with_capacity(keys.len() * 2);
    for key in keys {
        if !seen_keys.insert(*key) {
            return Err(Error::InvalidWait);
        }
        let key = record_key(parent.task.id(), key);
        let entry = store.get(&key)?.ok_or(Error::InvalidWait)?;
        let record: Record = serde_json::from_slice(&entry.value)?;
        if !seen_children.insert(record.child) {
            return Err(Error::InvalidWait);
        }
        let child = tasks.load(record.child)?.ok_or(Error::InvalidStoredChild)?;
        if child.task.owner() != &TaskOwner::Task(parent.task.id()) {
            return Err(Error::InvalidStoredChild);
        }
        ids.push(record.child);
        checks.push(Check {
            key,
            expected: Expected::Version(entry.version),
        });
        checks.push(Check {
            key: crate::task_store::key(record.child),
            expected: Expected::Version(child.version),
        });
    }
    let mut next = parent.clone();
    if next.task.checkpoint() != &checkpoint {
        next.task
            .advance(checkpoint)
            .map_err(TaskError::Transition)?;
    }
    next.task
        .wait(WaitFor::TasksAny(ids))
        .map_err(TaskError::Transition)?;
    let mut batch = tasks.prepare_save(&[Save::Update(&next)])?;
    batch.checks.extend(checks);
    Ok(batch)
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

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

    fn registered_parent(store: &Store, safe: bool) -> StoredTask {
        let mut task = Task::new(
            Uuid::from_u128(1),
            TaskOwner::Conversation(Uuid::from_u128(99)),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("parent-task".into(), safe);
        task.start().unwrap();
        let tasks = TaskStore::new(store);
        tasks.save(&[Save::Create(&task)]).unwrap();
        tasks.load(task.id()).unwrap().unwrap()
    }

    fn cp(phase: &str) -> Checkpoint {
        Checkpoint {
            phase: phase.into(),
            data: json!({"saved":true}),
        }
    }

    fn address() -> crate::document::Address {
        crate::document::Address {
            scope: "test".into(),
            name: "park".into(),
            key: "".into(),
        }
    }

    fn staged<'a>(store: &'a Store, parent: &StoredTask) -> crate::document::TaskDocuments<'a> {
        let mut docs = crate::document::TaskDocuments::new(store, &parent.task).unwrap();
        docs.get(&address()).unwrap();
        docs.put(
            &address(),
            json!({"saved":true}).as_object().unwrap().clone(),
        )
        .unwrap();
        docs
    }

    #[test]
    fn documents_and_keyed_park_commit_together_and_scheduler_wakes_after_reopen() {
        use crate::task_scheduler::{Registry, Scheduler, Step};
        use durable_store::owner::OwnedStore;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let owner = OwnedStore::open(&path).unwrap();
        let p = registered_parent(owner.store(), false);
        let (p, a, b) = admit_pair(owner.store(), &p);
        let docs = staged(owner.store(), &p);
        park_with_documents(&owner, &p, cp("parked"), &["a", "b"], docs).unwrap();
        drop(owner);
        let parent_runs = Arc::new(AtomicUsize::new(0));
        let observed = parent_runs.clone();
        let mut registry = Registry::default();
        registry.register("parent-task", false, move |task: &Task| {
            observed.fetch_add(1, Ordering::SeqCst);
            assert_eq!(task.checkpoint(), &cp("parked"));
            Ok(Step::Wait {
                checkpoint: cp("finish"),
                condition: WaitFor::Tasks(vec![Uuid::from_u128(3)]),
                children: vec![],
            })
        });
        registry.register("tool-task", true, |_: &Task| {
            Ok(Step::Succeed(json!("child")))
        });
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(parent_runs.load(Ordering::SeqCst), 0);
        let stored = crate::document::Documents::new(scheduler.owner().store())
            .get(&address())
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.value,
            json!({"saved":true}).as_object().unwrap().clone()
        );
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(parent_runs.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler.get(a.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("child"))
        );
        assert_eq!(
            scheduler.get(b.id()).unwrap().unwrap().state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn stale_document_or_abort_rolls_back_park_and_staged_document() {
        for abort in [false, true] {
            let db = Store::memory().unwrap();
            let p = registered_parent(&db, true);
            let (p, _, _) = admit_pair(&db, &p);
            let docs = staged(&db, &p);
            let tx = prepare_park_with_documents(&db, &p, cp("parked"), &["a", "b"], docs).unwrap();
            if abort {
                TaskStore::new(&db)
                    .update(&p, |t| t.request_abort())
                    .unwrap();
            } else {
                let mut concurrent = crate::document::Transaction::new();
                concurrent
                    .put(
                        &address(),
                        Expected::Missing,
                        json!({"other":true}).as_object().unwrap(),
                    )
                    .unwrap();
                concurrent.commit(&db).unwrap();
            }
            assert!(matches!(
                tx.commit(&db),
                Err(crate::document::Error::Store(
                    durable_store::Error::Conflict { .. }
                ))
            ));
            let after = TaskStore::new(&db).load(p.task.id()).unwrap().unwrap();
            assert_eq!(after.task.state(), &TaskState::Running);
            assert_eq!(after.task.checkpoint(), p.task.checkpoint());
            let doc = crate::document::Documents::new(&db)
                .get(&address())
                .unwrap();
            if abort {
                assert!(doc.is_none());
            } else {
                assert_eq!(
                    doc.unwrap().value,
                    json!({"other":true}).as_object().unwrap().clone()
                );
            }
        }
    }

    #[test]
    fn mismatched_document_store_or_invocation_fails_closed() {
        let db = Store::memory().unwrap();
        let other = Store::memory().unwrap();
        let p = registered_parent(&db, true);
        let (p, _, _) = admit_pair(&db, &p);
        let wrong_store = staged(&other, &p);
        assert!(matches!(
            prepare_park_with_documents(&db, &p, cp("parked"), &["a"], wrong_store),
            Err(Error::DocumentContext)
        ));
        let mut stale = p.clone();
        stale.task.advance(cp("other")).unwrap();
        let wrong_task = staged(&db, &stale);
        assert!(matches!(
            prepare_park_with_documents(&db, &p, cp("parked"), &["a"], wrong_task),
            Err(Error::DocumentContext)
        ));
        assert_eq!(TaskStore::new(&db).load(p.task.id()).unwrap().unwrap(), p);
        assert!(
            crate::document::Documents::new(&db)
                .get(&address())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn poisoned_task_gate_leaves_park_and_documents_unchanged() {
        use durable_store::owner::OwnedStore;
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(OwnedStore::open(dir.path().join("db")).unwrap());
        let p = registered_parent(owner.store(), true);
        let (p, _, _) = admit_pair(owner.store(), &p);
        let cloned = owner.clone();
        let _ = std::thread::spawn(move || {
            let _guard = cloned.task_gate().lock().unwrap();
            panic!("poison gate");
        })
        .join();
        let docs = staged(owner.store(), &p);
        assert!(matches!(
            park_with_documents(&owner, &p, cp("parked"), &["a"], docs),
            Err(Error::GatePoisoned)
        ));
        assert_eq!(
            TaskStore::new(owner.store())
                .load(p.task.id())
                .unwrap()
                .unwrap(),
            p
        );
        assert!(
            crate::document::Documents::new(owner.store())
                .get(&address())
                .unwrap()
                .is_none()
        );
    }

    fn admit_pair(store: &Store, p: &StoredTask) -> (StoredTask, Task, Task) {
        let a = child(2, p.task.id(), true, json!({"n":1}));
        let b = child(3, p.task.id(), true, json!({"n":2}));
        admit(store, p, "a", "tool", &json!({"n":1}), &a).unwrap();
        let p = TaskStore::new(store).load(p.task.id()).unwrap().unwrap();
        admit(store, &p, "b", "tool", &json!({"n":2}), &b).unwrap();
        let p = TaskStore::new(store).load(p.task.id()).unwrap().unwrap();
        (p, a, b)
    }

    #[test]
    fn scheduler_runs_children_and_resumes_unsafe_parked_parent_across_reopens() {
        use crate::task_scheduler::{Registry, Scheduler, Step};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let db = Store::open(&path).unwrap();
        let p = registered_parent(&db, false);
        let (p, a, b) = admit_pair(&db, &p);
        park_any(&db, &p, cp("first"), &["a", "b"]).unwrap();
        drop(db);
        let parents = Arc::new(AtomicUsize::new(0));
        let children = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        let parent_runs = parents.clone();
        assert!(registry.register("parent-task", false, move |task: &Task| {
            parent_runs.fetch_add(1, Ordering::SeqCst);
            Ok(if task.checkpoint().phase == "first" {
                Step::Wait {
                    checkpoint: cp("second"),
                    condition: WaitFor::TasksAny(vec![Uuid::from_u128(3)]),
                    children: vec![],
                }
            } else {
                Step::Succeed(json!("done"))
            })
        }));
        let child_runs = children.clone();
        assert!(registry.register("tool-task", true, move |task: &Task| {
            child_runs.fetch_add(1, Ordering::SeqCst);
            Ok(Step::Succeed(task.input().clone()))
        }));
        let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
        assert_eq!(
            scheduler.get(p.task.id()).unwrap().unwrap().state(),
            &TaskState::Waiting(WaitFor::TasksAny(vec![a.id(), b.id()]))
        );
        assert_eq!(parents.load(Ordering::SeqCst), 0);
        assert!(scheduler.tick(0).unwrap()); // first child
        assert!(scheduler.tick(0).unwrap()); // wake and repark parent
        assert_eq!(parents.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler.get(p.task.id()).unwrap().unwrap().checkpoint(),
            &cp("second")
        );
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        assert_eq!(parents.load(Ordering::SeqCst), 1);
        assert!(scheduler.tick(0).unwrap()); // second child
        assert!(scheduler.tick(0).unwrap()); // wake and finish parent
        assert_eq!(
            scheduler.get(p.task.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("done"))
        );
        assert_eq!(parents.load(Ordering::SeqCst), 2);
        assert_eq!(children.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn interrupted_child_follows_its_replay_policy_while_parent_remains_parked() {
        use crate::task_scheduler::{Registry, Scheduler, Step};
        for safe in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.db");
            let db = Store::open(&path).unwrap();
            let p = registered_parent(&db, false);
            let c = child(2, p.task.id(), safe, json!({}));
            admit(&db, &p, "a", "tool", &json!({}), &c).unwrap();
            let tasks = TaskStore::new(&db);
            let p = tasks.load(p.task.id()).unwrap().unwrap();
            park_any(&db, &p, cp("parked"), &["a"]).unwrap();
            tasks
                .reserve(&tasks.load(c.id()).unwrap().unwrap())
                .unwrap();
            drop(db);
            let mut registry = Registry::default();
            registry.register("parent-task", false, |_: &Task| {
                Ok(Step::Succeed(json!(null)))
            });
            registry.register("tool-task", safe, |_: &Task| {
                Ok(Step::Succeed(json!("replayed")))
            });
            let scheduler = Scheduler::open(&path, registry).unwrap();
            assert!(matches!(
                scheduler.get(p.task.id()).unwrap().unwrap().state(),
                TaskState::Waiting(_)
            ));
            let state = scheduler.get(c.id()).unwrap().unwrap();
            if safe {
                assert_eq!(state.state(), &TaskState::Ready);
            } else {
                assert!(matches!(state.state(), TaskState::Failed(_)));
            }
        }
    }

    #[test]
    fn parked_parent_abort_cascades_to_admitted_children_after_reopen() {
        use crate::task_scheduler::{Registry, Scheduler, Step};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let db = Store::open(&path).unwrap();
        let p = registered_parent(&db, true);
        let (p, a, b) = admit_pair(&db, &p);
        park_any(&db, &p, cp("parked"), &["a", "b"]).unwrap();
        drop(db);
        let mut registry = Registry::default();
        registry.register("parent-task", true, |_: &Task| {
            Ok(Step::Succeed(json!(null)))
        });
        registry.register("tool-task", true, |_: &Task| Ok(Step::Succeed(json!(null))));
        let mut scheduler = Scheduler::open(&path, registry.clone()).unwrap();
        scheduler.request_abort(p.task.id()).unwrap();
        drop(scheduler);
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        for _ in 0..8 {
            if !scheduler.tick(0).unwrap() {
                break;
            }
        }
        for id in [p.task.id(), a.id(), b.id()] {
            assert!(scheduler.get(id).unwrap().unwrap().is_terminal());
            assert!(scheduler.get(id).unwrap().unwrap().abort_requested());
        }
    }

    #[test]
    fn park_fences_admission_abort_and_child_changes_without_partial_checkpoint() {
        for race in 0..4 {
            let db = Store::memory().unwrap();
            let p = registered_parent(&db, true);
            let (p, a, _) = admit_pair(&db, &p);
            let pending = prepare_park_any(&db, &p, cp("parked"), &["a"]).unwrap();
            let tasks = TaskStore::new(&db);
            match race {
                0 => {
                    let c = child(4, p.task.id(), true, json!({}));
                    admit(&db, &p, "c", "tool", &json!({}), &c).unwrap();
                }
                1 => {
                    tasks.update(&p, |t| t.request_abort()).unwrap();
                }
                2 => {
                    tasks.update(&p, |t| t.succeed(json!("settled"))).unwrap();
                }
                _ => {
                    let c = tasks.load(a.id()).unwrap().unwrap();
                    tasks.update(&c, |t| t.cancel()).unwrap();
                }
            }
            assert!(matches!(
                db.commit(&pending.checks, &pending.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            let after = tasks.load(p.task.id()).unwrap().unwrap();
            assert_eq!(after.task.checkpoint(), p.task.checkpoint());
            if race == 2 {
                assert_eq!(after.task.state(), &TaskState::Succeeded(json!("settled")));
            } else {
                assert_eq!(after.task.state(), &TaskState::Running);
            }
        }
    }

    #[test]
    fn settled_child_before_park_wakes_any_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let db = Store::open(&path).unwrap();
        let p = registered_parent(&db, true);
        let (p, a, b) = admit_pair(&db, &p);
        let tasks = TaskStore::new(&db);
        tasks
            .update(&tasks.load(a.id()).unwrap().unwrap(), |t| t.cancel())
            .unwrap();
        park_any(&db, &p, cp("parked"), &["a", "b"]).unwrap();
        drop(db);

        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        let waiting = tasks.load(p.task.id()).unwrap().unwrap();
        assert_eq!(waiting.task.checkpoint(), &cp("parked"));
        assert!(matches!(
            tasks.poll_wait(&waiting, 0).unwrap(),
            crate::task_store::WakeResult::Woken { .. }
        ));
        assert_eq!(
            tasks.load(p.task.id()).unwrap().unwrap().task.state(),
            &TaskState::Ready
        );
        assert_eq!(
            tasks.load(b.id()).unwrap().unwrap().task.state(),
            &TaskState::Ready
        );
    }

    #[test]
    fn retry_park_after_child_settles_preserves_checkpoint_and_wakes() {
        let db = Store::memory().unwrap();
        let p = registered_parent(&db, true);
        let (p, a, _) = admit_pair(&db, &p);
        let pending = prepare_park_any(&db, &p, cp("parked"), &["a"]).unwrap();
        let tasks = TaskStore::new(&db);
        tasks
            .update(&tasks.load(a.id()).unwrap().unwrap(), |t| t.cancel())
            .unwrap();
        assert!(matches!(
            db.commit(&pending.checks, &pending.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert_eq!(tasks.load(p.task.id()).unwrap().unwrap(), p);
        park_any(&db, &p, cp("parked"), &["a"]).unwrap();
        let waiting = tasks.load(p.task.id()).unwrap().unwrap();
        assert_eq!(waiting.task.checkpoint(), &cp("parked"));
        assert!(matches!(
            tasks.poll_wait(&waiting, 0).unwrap(),
            crate::task_store::WakeResult::Woken { .. }
        ));
        assert!(matches!(
            park_any(&db, &p, cp("stale"), &["a"]),
            Err(Error::ParentClosed)
        ));
        assert_eq!(
            tasks.load(p.task.id()).unwrap().unwrap().task.checkpoint(),
            &cp("parked")
        );
    }

    #[test]
    fn park_rejects_missing_duplicate_and_foreign_keys() {
        let db = Store::memory().unwrap();
        let p = registered_parent(&db, true);
        let (p, a, _) = admit_pair(&db, &p);
        for keys in [vec![], vec!["missing"], vec!["a", "a"]] {
            assert!(matches!(
                park_any(&db, &p, cp("no"), &keys),
                Err(Error::InvalidWait)
            ));
        }
        let task_key = crate::task_store::key(a.id());
        db.commit(&[], &[Mutation::Delete { key: task_key }])
            .unwrap();
        assert!(matches!(
            park_any(&db, &p, cp("no"), &["a"]),
            Err(Error::InvalidStoredChild)
        ));
        assert_eq!(TaskStore::new(&db).load(p.task.id()).unwrap().unwrap(), p);
    }

    #[test]
    fn wake_reattaches_original_key_after_reopen_without_recreating_child() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let db = Store::open(&path).unwrap();
        let p = registered_parent(&db, true);
        let (p, a, _) = admit_pair(&db, &p);
        park_any(&db, &p, cp("parked"), &["a", "b"]).unwrap();
        let tasks = TaskStore::new(&db);
        let c = tasks.load(a.id()).unwrap().unwrap();
        tasks.update(&c, |t| t.cancel()).unwrap();
        drop(db);
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        let waiting = tasks.load(p.task.id()).unwrap().unwrap();
        assert!(matches!(
            tasks.poll_wait(&waiting, 0).unwrap(),
            crate::task_store::WakeResult::Woken { .. }
        ));
        tasks
            .reserve(&tasks.load(p.task.id()).unwrap().unwrap())
            .unwrap();
        let running = tasks.load(p.task.id()).unwrap().unwrap();
        let candidate = child(4, p.task.id(), true, json!({"n":1}));
        assert_eq!(
            admit(&db, &running, "a", "tool", &json!({"n":1}), &candidate).unwrap(),
            a.id()
        );
        assert_eq!(running.task.checkpoint(), &cp("parked"));
        assert!(tasks.load(candidate.id()).unwrap().is_none());
    }

    #[test]
    fn park_rejects_key_record_pointing_to_foreign_child() {
        let db = Store::memory().unwrap();
        let p = registered_parent(&db, true);
        let (p, a, _) = admit_pair(&db, &p);
        let foreign = child(9, Uuid::from_u128(77), true, json!({}));
        TaskStore::new(&db).save(&[Save::Create(&foreign)]).unwrap();
        let key = record_key(p.task.id(), "a");
        let entry = db.get(&key).unwrap().unwrap();
        let mut record: Record = serde_json::from_slice(&entry.value).unwrap();
        assert_eq!(record.child, a.id());
        record.child = foreign.id();
        db.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&record).unwrap(),
            }],
        )
        .unwrap();
        assert!(matches!(
            park_any(&db, &p, cp("no"), &["a"]),
            Err(Error::InvalidStoredChild)
        ));
        assert_eq!(TaskStore::new(&db).load(p.task.id()).unwrap().unwrap(), p);
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
