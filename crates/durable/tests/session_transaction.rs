use async_trait::async_trait;
use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    document::{Address, Documents, Error as DocumentError, Transaction},
    recovery::UncertainPolicy,
    runtime::{Config, Error, Harness, Options},
};
use serde_json::{Map, Value, json};
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
fn address() -> Address {
    Address {
        scope: "session".into(),
        name: "state".into(),
        key: "".into(),
    }
}
fn object(n: i64) -> Map<String, Value> {
    json!({"n":n}).as_object().unwrap().clone()
}

#[test]
fn entries_and_documents_read_your_writes_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let id = h
        .session_transaction(conversation, |tx| {
            assert!(tx.document(&address())?.is_none());
            tx.put_document(&address(), object(1))?;
            let first = tx.append_entry("note", Some(json!({"body":"first"})))?;
            assert_eq!(tx.document(&address())?, Some(object(1)));
            tx.delete_document(&address())?;
            assert!(tx.document(&address())?.is_none());
            tx.put_document(&address(), object(2))?;
            let second = tx.append_entry("note", Some(json!({"body":"second"})))?;
            assert_eq!(
                tx.entries()
                    .iter()
                    .map(|entry| entry.id)
                    .collect::<Vec<_>>(),
                vec![first, second]
            );
            assert_eq!(tx.document(&address())?, Some(object(2)));
            Ok(second)
        })
        .unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let c = h.conversation(conversation).unwrap();
    assert_eq!(c.application_writes.len(), 2);
    assert_eq!(c.application_writes[1].id, id);
    assert!(
        c.application_writes
            .iter()
            .all(|entry| entry.placed && !entry.withdrawn)
    );
    let history = h
        .history(
            conversation,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(32).unwrap(),
        )
        .unwrap();
    assert_eq!(
        history.records.last().unwrap().value["application_writes"][1]["id"],
        json!(id)
    );
    let store = Store::open(&path).unwrap();
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(2)
    );
}

#[test]
fn callback_error_discards_entries_and_documents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let result: Result<(), Error> = h.session_transaction(conversation, |tx| {
        tx.put_document(&address(), object(1))?;
        tx.append_entry("note", Some(json!(1)))?;
        Err(DocumentError::InvalidApplicationEntry)
    });
    assert!(matches!(
        result,
        Err(Error::Document(DocumentError::InvalidApplicationEntry))
    ));
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[test]
fn read_only_session_does_not_publish_a_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let head = h.history_head(conversation).unwrap();
    h.session_transaction(conversation, |tx| {
        assert!(tx.document(&address())?.is_none());
        assert!(tx.entries().is_empty());
        Ok(())
    })
    .unwrap();
    assert_eq!(h.history_head(conversation).unwrap(), head);
}

#[test]
fn document_conflict_rolls_back_entry_without_replaying_callback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let other = Store::open(&path).unwrap();
    let mut calls = 0;
    let result = h.session_transaction(conversation, |tx| {
        calls += 1;
        assert!(tx.document(&address())?.is_none());
        tx.append_entry("note", Some(json!(1)))?;
        tx.put_document(&address(), object(1))?;
        let mut writer = Transaction::new();
        writer.put(&address(), Expected::Missing, &object(9))?;
        writer.commit(&other)?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert_eq!(calls, 1);
    drop(other);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(9)
    );
}

#[test]
fn conversation_abort_conflict_rolls_back_documents_and_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let other = Store::open(&path).unwrap();
    let result = h.session_transaction(conversation, |tx| {
        tx.put_document(&address(), object(1))?;
        tx.append_entry("note", None)?;
        let key = format!("runtime/conversation/{conversation}").into_bytes();
        let entry = other.get(&key)?.unwrap();
        let mut state: Value = serde_json::from_slice(&entry.value)?;
        state["abort_requested"] = json!(true);
        other.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
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
    drop(other);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[test]
fn read_only_missing_document_detects_create_delete_aba() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let other = Store::open(&path).unwrap();
    let result = h.session_transaction(conversation, |tx| {
        assert!(tx.document(&address())?.is_none());
        let mut writer = Transaction::new();
        writer.put(&address(), Expected::Missing, &object(1))?;
        writer.commit(&other)?;
        let version = Documents::new(&other).get(&address())?.unwrap().version;
        let mut writer = Transaction::new();
        writer.delete(&address(), version)?;
        writer.commit(&other)?;
        tx.append_entry("note", None)?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
}

#[test]
fn queued_reset_rejects_entry_without_losing_prior_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    h.submit(conversation, None, "active".into()).unwrap();
    h.reset(conversation, None).unwrap();
    let result = h.session_transaction(conversation, |tx| {
        tx.put_document(&address(), object(1))?;
        tx.append_entry("note", None)?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Document(DocumentError::SessionEntryBlocked))
    ));
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
    h.session_transaction(conversation, |tx| tx.put_document(&address(), object(2)))
        .unwrap();
    assert_eq!(
        Documents::new(&Store::open(dir.path().join("db")).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(2)
    );
}

#[test]
fn reserved_entry_kinds_fail_without_writing_documents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    for kind in ["", "pi.internal"] {
        let result = h.session_transaction(conversation, |tx| {
            tx.put_document(&address(), object(1))?;
            tx.append_entry(kind, None)?;
            Ok(())
        });
        assert!(matches!(
            result,
            Err(Error::Document(DocumentError::InvalidSessionEntry))
        ));
    }
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn scheduler_commits_the_same_session_transaction() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    let entry = scheduler
        .session_transaction(id, |tx| {
            tx.put_document(&address(), object(4))?;
            tx.append_entry("note", Some(json!("scheduler")))
        })
        .unwrap();
    assert_eq!(
        scheduler.conversation(id).unwrap().application_writes[0].id,
        entry
    );
    scheduler.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().application_writes[0].id, entry);
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(4)
    );
}

#[cfg(unix)]
#[test]
fn session_transaction_kill_fixture() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_SESSION_TRANSACTION_KILL_DB") else {
        return;
    };
    let mut h = Harness::open_paused(path, options()).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.session_transaction(id, |tx| {
        tx.put_document(&address(), object(7))?;
        tx.append_entry("note", Some(json!("killed")))?;
        Ok(())
    })
    .unwrap();
    println!("SESSION_COMMITTED {id}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn committed_session_transaction_survives_process_kill() {
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
        sync::mpsc,
        time::Duration,
    };
    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "session_transaction_kill_fixture", "--nocapture"])
            .env("PI_SESSION_TRANSACTION_KILL_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let output = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            let line = line.unwrap();
            if let Some((_, id)) = line.split_once("SESSION_COMMITTED ") {
                let _ = tx.send(id.trim().to_owned());
                break;
            }
        }
    });
    let id = Uuid::parse_str(&rx.recv_timeout(Duration::from_secs(30)).unwrap()).unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(id).unwrap().application_writes[0].data,
        Some(json!("killed"))
    );
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(7)
    );
}

#[test]
fn existing_submission_notification_survives_session_commit() {
    use pi_rs_durable::{
        runtime::{AdmissionOptions, SubmissionNotify, WithdrawResult, submission_settled_signal},
        task::{Checkpoint, Task, TaskOwner},
        task_store::{Save, TaskStore},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    let store = Store::open(&path).unwrap();
    TaskStore::new(&store).save(&[Save::Create(&task)]).unwrap();
    h.submit(conversation, None, "active".into()).unwrap();
    let id = h
        .submit_with_notification(
            conversation,
            None,
            "queued".into(),
            AdmissionOptions::default(),
            SubmissionNotify {
                task_id: task.id(),
                key: "done".into(),
            },
        )
        .unwrap();
    h.session_transaction(conversation, |tx| {
        tx.append_entry("note", None)?;
        tx.put_document(&address(), object(5))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        h.withdraw_submission(conversation, id).unwrap(),
        WithdrawResult::Aborted
    );
    drop(h);
    drop(store);
    let store = Store::open(&path).unwrap();
    let value = TaskStore::new(&store)
        .read_typed_signal(task.id(), &submission_settled_signal(), "done")
        .unwrap()
        .unwrap();
    assert_eq!(value["id"], json!(id));
    assert_eq!(value["reason"], "aborted");
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(5)
    );
}

#[test]
fn unrelated_document_write_does_not_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let other = Store::open(&path).unwrap();
    let unrelated = Address {
        scope: "session".into(),
        name: "other".into(),
        key: "".into(),
    };
    h.session_transaction(conversation, |tx| {
        assert!(tx.document(&address())?.is_none());
        let mut writer = Transaction::new();
        writer.put(&unrelated, Expected::Missing, &object(9))?;
        writer.commit(&other)?;
        tx.put_document(&address(), object(1))?;
        tx.append_entry("note", None)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .len(),
        1
    );
    assert_eq!(
        Documents::new(&other)
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(1)
    );
    assert_eq!(
        Documents::new(&other)
            .get(&unrelated)
            .unwrap()
            .unwrap()
            .value,
        object(9)
    );
}
