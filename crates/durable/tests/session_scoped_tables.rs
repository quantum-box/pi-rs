use async_trait::async_trait;
use durable_store::{Mutation, Store};
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    document::{Address, Error as DocumentError},
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options, SubmissionNotify, submission_settled_signal},
    task::{Checkpoint, Task, TaskOwner},
    task_store::{SignalResult, SignalToken, TaskStore},
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

struct Unused;
#[async_trait]
impl Model for Unused {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        panic!("unexpected model")
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Unused),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    }
}
fn task(id: Uuid, owner: Uuid) -> Task {
    Task::new(
        id,
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
}
fn address() -> Address {
    Address {
        scope: "test".into(),
        name: "document".into(),
        key: "one".into(),
    }
}

#[test]
fn scoped_reads_admission_task_creation_and_notification_commit_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let mut registry = pi_rs_durable::task_scheduler::Registry::default();
    registry.register("worker", true, |_: &Task| {
        Ok(pi_rs_durable::task_scheduler::Step::Wait {
            checkpoint: Checkpoint {
                phase: "wait".into(),
                data: json!(null),
            },
            condition: pi_rs_durable::task::WaitFor::Signal("never".into()),
            children: vec![],
        })
    });
    let driver = h.start_tasks(registry).unwrap();
    let task_id = Uuid::new_v4();
    let write_id = h
        .session_transaction(id, |tx| {
            assert_eq!(tx.conversation()?.id, id);
            assert_eq!(
                tx.signal(
                    task_id,
                    &SignalToken::new("wake", 1).unwrap(),
                    "missing",
                    json!(1)
                )?,
                SignalResult::NotFound
            );
            assert!(tx.entry(Uuid::new_v4())?.is_none());
            assert!(tx.submission(Uuid::new_v4())?.is_none());
            assert!(tx.submission_by_request("absent")?.is_none());
            let write = tx.submit_write(
                None,
                "note".into(),
                Some(json!(42)),
                Some(SubmissionNotify {
                    task_id,
                    key: "settled".into(),
                }),
            )?;
            tx.create_task(task(task_id, id).registered("worker".into(), true))?;
            let preview: Vec<_> = tx.entries();
            assert_eq!(
                preview.iter().map(|entry| entry.id).collect::<Vec<_>>(),
                vec![write]
            );
            assert!(matches!(
                tx.conversation(),
                Err(DocumentError::ReadAfterWrite)
            ));
            assert!(matches!(
                tx.checked_entries(),
                Err(DocumentError::ReadAfterWrite)
            ));
            assert!(matches!(
                tx.submission_by_request("absent"),
                Err(DocumentError::ReadAfterWrite)
            ));
            assert!(matches!(
                tx.task(task_id),
                Err(DocumentError::ReadAfterWrite)
            ));
            assert!(matches!(
                tx.entry(write),
                Err(DocumentError::ReadAfterWrite)
            ));
            tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
            Ok(write)
        })
        .unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.session_transaction(id, |tx| {
        assert_eq!(tx.entry(write_id)?.unwrap().data, Some(json!(42)));
        assert_eq!(tx.task(task_id)?.unwrap().task.id(), task_id);
        assert_eq!(tx.document(&address())?.unwrap()["x"], 1);
        Ok(())
    })
    .unwrap();
    let store = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(task_id, &submission_settled_signal(), "settled")
            .unwrap()
            .unwrap()["id"],
        json!(write_id)
    );
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(task_id, &SignalToken::new("wake", 1).unwrap(), "missing")
            .unwrap()
            .is_none()
    );
}

#[test]
fn missing_signal_then_create_rejects_intervening_create_delete() {
    use pi_rs_durable::runtime::Error;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let mut registry = pi_rs_durable::task_scheduler::Registry::default();
    registry.register("worker", true, |_: &Task| {
        Ok(pi_rs_durable::task_scheduler::Step::Wait {
            checkpoint: Checkpoint {
                phase: "wait".into(),
                data: json!(null),
            },
            condition: pi_rs_durable::task::WaitFor::Signal("never".into()),
            children: vec![],
        })
    });
    let driver = h.start_tasks(registry).unwrap();
    let store = Store::open(&path).unwrap();
    let target = Uuid::new_v4();
    let result = h.session_transaction(id, |tx| {
        assert_eq!(
            tx.signal(
                target,
                &SignalToken::new("wake", 1).unwrap(),
                "missing",
                json!(1)
            )?,
            SignalResult::NotFound
        );
        let mut key = b"durable/task/".to_vec();
        key.extend_from_slice(target.as_bytes());
        store.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&task(target, id))?,
            }],
        )?;
        store.commit(&[], &[Mutation::Delete { key }])?;
        tx.create_task(task(target, id).registered("worker".into(), true))?;
        tx.append_entry("note", None)?;
        tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(TaskStore::new(&store).load(target).unwrap().is_none());
    assert!(
        pi_rs_durable::document::Documents::new(&store)
            .get(&address())
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}

#[test]
fn task_read_conflicts_on_update_and_missing_create_delete() {
    use pi_rs_durable::{runtime::Error, task_store::Save};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let existing = Uuid::new_v4();
    TaskStore::new(&store)
        .save(&[Save::Create(&task(existing, id))])
        .unwrap();
    for missing in [false, true] {
        let target = if missing { Uuid::new_v4() } else { existing };
        let result = h.session_transaction(id, |tx| {
            assert_eq!(tx.task(target)?.is_some(), !missing);
            tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
            tx.append_entry("note", None)?;
            let mut key = b"durable/task/".to_vec();
            key.extend_from_slice(target.as_bytes());
            let altered = task(target, id).registered("other".into(), false);
            store.commit(
                &[],
                &[Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&altered)?,
                }],
            )?;
            if missing {
                store.commit(&[], &[Mutation::Delete { key }])?;
            }
            Ok(())
        });
        assert!(matches!(
            result,
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert!(h.conversation(id).unwrap().application_writes.is_empty());
        assert!(
            pi_rs_durable::document::Documents::new(&store)
                .get(&address())
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn task_scope_and_callback_error_never_write() {
    use pi_rs_durable::task_store::Save;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let other = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let existing = Uuid::new_v4();
    TaskStore::new(&store)
        .save(&[Save::Create(&task(existing, other))])
        .unwrap();
    let created = Uuid::new_v4();
    let result: Result<(), _> = h.session_transaction(id, |tx| {
        assert!(tx.task(existing)?.is_none());
        assert!(tx.create_task(task(Uuid::new_v4(), other)).is_err());
        let mut forged = serde_json::to_value(task(Uuid::new_v4(), id))?;
        forged["attempt_id"] = json!(Uuid::new_v4());
        let forged: Task = serde_json::from_value(forged)?;
        assert!(tx.create_task(forged).is_err());
        assert_eq!(tx.conversation()?.id, id);
        tx.create_task(task(created, id))?;
        tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
        Err(DocumentError::InvalidApplicationEntry)
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(created).unwrap().is_none());
    assert!(
        pi_rs_durable::document::Documents::new(&store)
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[test]
fn scoped_creation_requires_driver_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let task_id = Uuid::new_v4();
    let result = h.session_transaction(id, |tx| {
        tx.append_entry("note", None)?;
        tx.create_task(task(task_id, id).registered("missing".into(), true))?;
        tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
        Ok(())
    });
    assert!(result.is_err());
    let store = Store::open(&path).unwrap();
    assert!(TaskStore::new(&store).load(task_id).unwrap().is_none());
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        pi_rs_durable::document::Documents::new(&store)
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[test]
fn scoped_entry_and_task_reads_fence_concurrent_abort() {
    use pi_rs_durable::{runtime::Error, task_store::Save};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let existing = Uuid::new_v4();
    TaskStore::new(&store)
        .save(&[Save::Create(&task(existing, id))])
        .unwrap();
    let result = h.session_transaction(id, |tx| {
        assert!(tx.task(existing)?.is_some());
        tx.append_entry("note", None)?;
        tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
        let key = format!("runtime/conversation/{id}").into_bytes();
        let entry = store.get(&key)?.unwrap();
        let mut state: serde_json::Value = serde_json::from_slice(&entry.value)?;
        state["abort_requested"] = json!(true);
        store.commit(
            &[],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&state)?,
            }],
        )?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(
        pi_rs_durable::document::Documents::new(&store)
            .get(&address())
            .unwrap()
            .is_none()
    );
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
}

#[tokio::test]
async fn async_scoped_reads_share_table_write_rule_and_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let entry = h
        .session_transaction_async(id, |tx| {
            Box::pin(async move {
                assert_eq!(tx.conversation()?.id, id);
                assert!(tx.submission(Uuid::new_v4())?.is_none());
                let id = tx.append_entry("note", Some(json!("async")))?;
                assert!(matches!(tx.entry(id), Err(DocumentError::ReadAfterWrite)));
                tx.put_document(&address(), json!({"x":2}).as_object().unwrap().clone())?;
                Ok(id)
            })
        })
        .await
        .unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.session_transaction(id, |tx| {
        assert_eq!(tx.entry(entry)?.unwrap().data, Some(json!("async")));
        assert_eq!(tx.document(&address())?.unwrap()["x"], 2);
        Ok(())
    })
    .unwrap();
}

#[test]
fn read_only_missing_task_rejects_raw_create_delete_aba() {
    use pi_rs_durable::runtime::Error;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let target = Uuid::new_v4();
    let before = h.history_head(id).unwrap();
    let result = h.session_transaction(id, |tx| {
        assert!(tx.task(target)?.is_none());
        let mut key = b"durable/task/".to_vec();
        key.extend_from_slice(target.as_bytes());
        store.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&task(target, id))?,
            }],
        )?;
        store.commit(&[], &[Mutation::Delete { key }])?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert_eq!(h.history_head(id).unwrap(), before);
}

#[test]
fn task_read_then_signal_and_entry_share_the_same_fence() {
    use pi_rs_durable::task_store::{Save, SignalResult, SignalToken};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let target = Uuid::new_v4();
    TaskStore::new(&store)
        .save(&[Save::Create(&task(target, id))])
        .unwrap();
    let token = SignalToken::new("wake", 1).unwrap();
    h.session_transaction(id, |tx| {
        assert_eq!(tx.task(target)?.unwrap().task.id(), target);
        assert_eq!(
            tx.signal(target, &token, "key", json!({"ok":true}))?,
            SignalResult::Delivered
        );
        assert!(matches!(
            tx.task(target),
            Err(DocumentError::ReadAfterWrite)
        ));
        tx.append_entry("note", None)?;
        tx.put_document(&address(), json!({"x":3}).as_object().unwrap().clone())?;
        Ok(())
    })
    .unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().application_writes.len(), 1);
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(target, &token, "key")
            .unwrap(),
        Some(json!({"ok":true}))
    );
}

#[test]
fn running_driver_rejects_unregistered_task_without_partial_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let driver = h
        .start_tasks(pi_rs_durable::task_scheduler::Registry::default())
        .unwrap();
    let task_id = Uuid::new_v4();
    let result = h.session_transaction(id, |tx| {
        tx.append_entry("note", None)?;
        tx.create_task(task(task_id, id).registered("unregistered".into(), true))?;
        tx.put_document(&address(), json!({"x":1}).as_object().unwrap().clone())?;
        Ok(())
    });
    assert!(result.is_err());
    let store = Store::open(&path).unwrap();
    assert!(TaskStore::new(&store).load(task_id).unwrap().is_none());
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        pi_rs_durable::document::Documents::new(&store)
            .get(&address())
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}

#[test]
fn submission_reads_are_scoped_and_close_after_entry_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let submission = h
        .submit(id, Some("request".into()), "hello".into())
        .unwrap();
    h.session_transaction(id, |tx| {
        assert_eq!(tx.submission(submission)?.unwrap().id, submission);
        assert_eq!(tx.submission_by_request("request")?.unwrap().id, submission);
        assert!(tx.checked_entries()?.is_empty());
        tx.append_entry("note", None)?;
        assert!(matches!(
            tx.submission(submission),
            Err(DocumentError::ReadAfterWrite)
        ));
        assert!(matches!(
            tx.submission_by_request("request"),
            Err(DocumentError::ReadAfterWrite)
        ));
        Ok(())
    })
    .unwrap();
}
