//! Storage-only foundation for task-backed tool dispatch. These records do not
//! grant an executor lease and are not wired to tool execution.
use crate::async_task_tool::ToolIdentity;
use crate::task::{Task, TaskOwner, TaskState};
use crate::task_store::{OwnedBatch, StoredTask, TaskStore};
use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_core::Outcome;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("store: {0}")]
    Store(#[from] durable_store::Error),
    #[error("task: {0}")]
    Task(#[from] crate::task_store::Error),
    #[error("child: {0}")]
    Child(#[from] crate::keyed_child::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("stale or invalid task, identity, intent, or receipt")]
    Invalid,
}

fn identity_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/identity/{task}").into_bytes()
}
fn intent_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/intent/{task}").into_bytes()
}
fn receipt_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/receipt/{task}").into_bytes()
}

/// Compose the child, keyed admission, and definition identity in one batch.
/// This neither validates the current tool registration nor transfers a lease.
pub(crate) fn prepare_child(
    store: &Store,
    parent: &StoredTask,
    key: &str,
    child: &Task,
    identity: &ToolIdentity,
) -> Result<(Uuid, OwnedBatch), Error> {
    if !identity.is_valid() {
        return Err(Error::Invalid);
    }
    let encoded = serde_json::to_string(identity)?;
    if encoded.len() > 65_536 {
        return Err(Error::Invalid);
    }
    let prepared = crate::keyed_child::prepare(store, parent, key, &encoded, child.input(), child)?;
    let id = prepared.child;
    let mut batch = OwnedBatch {
        checks: prepared.checks,
        mutations: prepared.mutations,
    };
    let key = identity_key(id);
    let existing = store.get(&key)?;
    if let Some(ref entry) = existing {
        if serde_json::from_slice::<ToolIdentity>(&entry.value)? != *identity {
            return Err(Error::Invalid);
        }
    } else {
        batch.mutations.push(Mutation::Put {
            key: key.clone(),
            value: encoded.into_bytes(),
        });
    }
    batch.checks.push(Check {
        key,
        expected: existing.map_or(Expected::Missing, |e| Expected::Version(e.version)),
    });
    Ok((id, batch))
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Intent {
    task: Uuid,
    attempt: Uuid,
    identity: ToolIdentity,
}

/// Stage an attempt-specific durable intent. It is deliberately NOT a lease:
/// another abort can commit immediately after it. No external effect may use
/// this as dispatch authority. Restricted to unlinked standalone ownership.
pub(crate) fn prepare_intent(
    store: &Store,
    running: &StoredTask,
    identity: &ToolIdentity,
) -> Result<(Intent, OwnedBatch), Error> {
    if !identity.is_valid() {
        return Err(Error::Invalid);
    }
    let tasks = TaskStore::new(store);
    if running.task.state() != &TaskState::Running || running.task.abort_requested() {
        return Err(Error::Invalid);
    }
    let attempt = running.task.attempt_id().ok_or(Error::Invalid)?;
    let mut checks = Vec::new();
    let mut current = running.clone();
    let mut seen = std::collections::HashSet::new();
    loop {
        if !seen.insert(current.task.id())
            || current.task.abort_requested()
            || current.task.is_terminal()
            || current.task.is_completing()
            || current.task.is_background()
        {
            return Err(Error::Invalid);
        }
        checks.push(Check {
            key: crate::task_store::key(current.task.id()),
            expected: Expected::Version(current.version),
        });
        checks.push(crate::ownership_edge::abort_fence_check(
            store,
            current.task.owner(),
        )?);
        match current.task.owner() {
            TaskOwner::Task(parent) => current = tasks.load(*parent)?.ok_or(Error::Invalid)?,
            TaskOwner::Conversation(id) => {
                let conversation = format!("runtime/conversation/{id}").into_bytes();
                let edge = crate::ownership_edge::edge_key(*id);
                if store.get(&conversation)?.is_some() || store.get(&edge)?.is_some() {
                    return Err(Error::Invalid);
                }
                checks.push(Check {
                    key: conversation,
                    expected: Expected::Missing,
                });
                checks.push(Check {
                    key: edge,
                    expected: Expected::Missing,
                });
                break;
            }
        }
    }
    let identity_key = identity_key(running.task.id());
    let stored_identity = store.get(&identity_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&stored_identity.value)? != *identity {
        return Err(Error::Invalid);
    }
    checks.push(Check {
        key: identity_key,
        expected: Expected::Version(stored_identity.version),
    });
    let key = intent_key(running.task.id());
    if store.get(&key)?.is_some() {
        return Err(Error::Invalid);
    }
    checks.push(Check {
        key: key.clone(),
        expected: Expected::Missing,
    });
    let intent = Intent {
        task: running.task.id(),
        attempt,
        identity: identity.clone(),
    };
    let mutations = vec![Mutation::Put {
        key,
        value: serde_json::to_vec(&intent)?,
    }];
    Ok((intent, OwnedBatch { checks, mutations }))
}

/// Store an uncertain outcome independently of task settlement and abort.
/// The caller must join and authenticate the executor; this checks persisted
/// identity only and is not a lease. A lost commit requires readback.
pub(crate) fn prepare_receipt(
    store: &Store,
    intent: &Intent,
    outcome: &Outcome,
) -> Result<OwnedBatch, Error> {
    if !matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
        return Err(Error::Invalid);
    }
    let intent_key = intent_key(intent.task);
    let stored = store.get(&intent_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<Intent>(&stored.value)? != *intent {
        return Err(Error::Invalid);
    }
    let definition_key = identity_key(intent.task);
    let definition = store.get(&definition_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != intent.identity {
        return Err(Error::Invalid);
    }
    let value = serde_json::to_vec(&(intent, outcome))?;
    if value.len() > 65_536 {
        return Err(Error::Invalid);
    }
    let key = receipt_key(intent.task);
    let existing = store.get(&key)?;
    if existing.as_ref().is_some_and(|entry| entry.value != value) {
        return Err(Error::Invalid);
    }
    let checks = vec![
        Check {
            key: definition_key,
            expected: Expected::Version(definition.version),
        },
        Check {
            key: intent_key,
            expected: Expected::Version(stored.version),
        },
        Check {
            key: key.clone(),
            expected: existing
                .as_ref()
                .map_or(Expected::Missing, |e| Expected::Version(e.version)),
        },
    ];
    let mutations = if existing.is_none() {
        vec![Mutation::Put { key, value }]
    } else {
        vec![]
    };
    Ok(OwnedBatch { checks, mutations })
}

pub(crate) fn read_receipt(store: &Store, intent: &Intent) -> Result<Option<Outcome>, Error> {
    let stored = store.get(&intent_key(intent.task))?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<Intent>(&stored.value)? != *intent {
        return Err(Error::Invalid);
    }
    let definition = store
        .get(&identity_key(intent.task))?
        .ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != intent.identity {
        return Err(Error::Invalid);
    }
    let Some(entry) = store.get(&receipt_key(intent.task))? else {
        return Ok(None);
    };
    let (stored, outcome): (Intent, Outcome) = serde_json::from_slice(&entry.value)?;
    if &stored != intent || !matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
        return Err(Error::Invalid);
    }
    Ok(Some(outcome))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Checkpoint;
    use crate::task_store::Save;
    use pi_rs_core::{Tool, ToolContext, ToolSpec};
    use serde_json::json;

    struct Stub;
    #[async_trait::async_trait]
    impl Tool for Stub {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    fn fixture(store: &Store) -> (Uuid, Uuid, ToolIdentity) {
        let tasks = TaskStore::new(store);
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "parent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.start_attempt(Uuid::new_v4()))
            .unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("leaf".into(), false, 1, json!({}))
        .unwrap()
        .into_task_tool();
        let identity = ToolIdentity::new(&Stub, "implementation-tenant-v1", 1).unwrap();
        let (id, batch) = prepare_child(store, &snapshot, "leaf", &child, &identity).unwrap();
        assert_eq!(id, child.id());
        store.commit(&batch.checks, &batch.mutations).unwrap();
        let snapshot = tasks.load(id).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.start_attempt(Uuid::new_v4()))
            .unwrap();
        (parent.id(), id, identity)
    }
    fn commit(store: &Store, batch: OwnedBatch) {
        store.commit(&batch.checks, &batch.mutations).unwrap();
    }

    #[test]
    fn parent_abort_between_prepare_and_intent_commit_refuses_the_intent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (parent, child, identity) = fixture(&store);
        let tasks = TaskStore::new(&store);
        let snapshot = tasks.load(child).unwrap().unwrap();
        let other = ToolIdentity::new(&Stub, "different-backend", 1).unwrap();
        assert!(prepare_intent(&store, &snapshot, &other).is_err());
        let (_, pending) = prepare_intent(&store, &snapshot, &identity).unwrap();
        let parent_snapshot = tasks.load(parent).unwrap().unwrap();
        tasks.update(&parent_snapshot, Task::request_abort).unwrap();
        assert!(matches!(
            store.commit(&pending.checks, &pending.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(store.get(&intent_key(child)).unwrap().is_none());
        assert!(prepare_intent(&store, &snapshot, &identity).is_err());
    }

    #[test]
    fn receipt_survives_abort_and_reopen_and_rejects_conflicting_rewrites() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let (parent, child, identity) = fixture(&store);
        let tasks = TaskStore::new(&store);
        let (intent, batch) =
            prepare_intent(&store, &tasks.load(child).unwrap().unwrap(), &identity).unwrap();
        commit(&store, batch);
        let snapshot = tasks.load(parent).unwrap().unwrap();
        tasks.update(&snapshot, Task::request_abort).unwrap();
        let child_snapshot = tasks.load(child).unwrap().unwrap();
        tasks.update(&child_snapshot, Task::request_abort).unwrap();
        let pending = Outcome::Pending {
            reference: "remote-42".into(),
        };
        commit(&store, prepare_receipt(&store, &intent, &pending).unwrap());
        assert!(prepare_receipt(&store, &intent, &Outcome::Unknown("other".into())).is_err());
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(
            read_receipt(&store, &intent).unwrap(),
            Some(pending.clone())
        );
        commit(&store, prepare_receipt(&store, &intent, &pending).unwrap());
        assert!(
            prepare_intent(
                &store,
                &TaskStore::new(&store).load(child).unwrap().unwrap(),
                &identity
            )
            .is_err()
        );
    }

    #[test]
    fn child_and_identity_are_one_checked_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let tasks = TaskStore::new(&store);
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "parent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.start_attempt(Uuid::new_v4()))
            .unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("leaf".into(), false, 1, json!({}))
        .unwrap();
        let identity = ToolIdentity::new(&Stub, "backend", 1).unwrap();
        let (_, batch) = prepare_child(&store, &snapshot, "leaf", &child, &identity).unwrap();
        // A competing writer occupying the identity key must prevent child creation.
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: identity_key(child.id()),
                    value: b"occupied".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            store.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(tasks.load(child.id()).unwrap().is_none());
    }

    #[test]
    fn killed_after_receipt_commit_retains_uncertain_reference() {
        const ENV: &str = "PI_RS_ASYNC_RECEIPT_KILL_TEST";
        if let Ok(directory) = std::env::var(ENV) {
            let dir = std::path::Path::new(&directory);
            let store = Store::open(dir.join("db")).unwrap();
            let (_, child, identity) = fixture(&store);
            let (intent, batch) = prepare_intent(
                &store,
                &TaskStore::new(&store).load(child).unwrap().unwrap(),
                &identity,
            )
            .unwrap();
            commit(&store, batch);
            if std::env::var_os("PI_RS_ASYNC_KILL_BEFORE_RECEIPT").is_none() {
                commit(
                    &store,
                    prepare_receipt(
                        &store,
                        &intent,
                        &Outcome::Pending {
                            reference: "remote-kill".into(),
                        },
                    )
                    .unwrap(),
                );
            }
            std::fs::write(dir.join("ready"), child.to_string()).unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_protocol::tests::killed_after_receipt_commit_retains_uncertain_reference")
            .env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("receipt worker did not become ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let intent: Intent =
            serde_json::from_slice(&store.get(&intent_key(id)).unwrap().unwrap().value).unwrap();
        assert_eq!(
            read_receipt(&store, &intent).unwrap(),
            Some(Outcome::Pending {
                reference: "remote-kill".into()
            })
        );
        assert_eq!(
            TaskStore::new(&store)
                .load(id)
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
    }
    #[test]
    fn competing_receipt_commits_never_overwrite_a_reference() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (_, child, identity) = fixture(&store);
        let (intent, batch) = prepare_intent(
            &store,
            &TaskStore::new(&store).load(child).unwrap().unwrap(),
            &identity,
        )
        .unwrap();
        commit(&store, batch);
        let first = Outcome::Pending {
            reference: "first".into(),
        };
        let second = Outcome::Pending {
            reference: "second".into(),
        };
        let one = prepare_receipt(&store, &intent, &first).unwrap();
        let two = prepare_receipt(&store, &intent, &second).unwrap();
        commit(&store, one);
        assert!(matches!(
            store.commit(&two.checks, &two.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert_eq!(read_receipt(&store, &intent).unwrap(), Some(first));
    }
    #[test]
    fn killed_after_intent_before_receipt_leaves_unknown_without_retry() {
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_protocol::tests::killed_after_receipt_commit_retains_uncertain_reference")
            .env("PI_RS_ASYNC_RECEIPT_KILL_TEST", dir.path())
            .env("PI_RS_ASYNC_KILL_BEFORE_RECEIPT", "1").spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("intent worker did not become ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let intent: Intent =
            serde_json::from_slice(&store.get(&intent_key(id)).unwrap().unwrap().value).unwrap();
        assert_eq!(read_receipt(&store, &intent).unwrap(), None);
        let task = TaskStore::new(&store).load(id).unwrap().unwrap();
        assert!(prepare_intent(&store, &task, &intent.identity).is_err());
    }
}
