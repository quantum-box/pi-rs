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
            tx.append_entry("note", Some(json!({"body":"first"})))?;
            assert_eq!(tx.document(&address())?, Some(object(1)));
            tx.delete_document(&address())?;
            assert!(tx.document(&address())?.is_none());
            tx.put_document(&address(), object(2))?;
            let second = tx.append_entry("note", Some(json!({"body":"second"})))?;
            assert!(matches!(tx.entries(), Err(DocumentError::ReadAfterWrite)));
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
        assert!(tx.entries()?.is_empty());
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

fn notification_target(path: &std::path::Path, conversation: Uuid) -> Uuid {
    use pi_rs_durable::{
        task::{Checkpoint, Task, TaskOwner},
        task_store::{Save, TaskStore},
    };
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
    TaskStore::new(&Store::open(path).unwrap())
        .save(&[Save::Create(&task)])
        .unwrap();
    task.id()
}
fn notification(task_id: Uuid) -> pi_rs_durable::runtime::SubmissionNotify {
    pi_rs_durable::runtime::SubmissionNotify {
        task_id,
        key: "done".into(),
    }
}
fn receipt(path: &std::path::Path, task: Uuid) -> Option<Value> {
    use pi_rs_durable::{runtime::submission_settled_signal, task_store::TaskStore};
    TaskStore::new(&Store::open(path).unwrap())
        .read_typed_signal(task, &submission_settled_signal(), "done")
        .unwrap()
}

#[test]
fn admission_write_atomically_commits_notification_document_and_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    let id = h
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(42))?;
            let id = tx.submit_write(
                Some("request".into()),
                "note".into(),
                Some(json!("submitted")),
                Some(notification(task)),
            )?;
            tx.append_entry("note", Some(json!("staged")))?;
            assert!(matches!(tx.entries(), Err(DocumentError::ReadAfterWrite)));
            assert_eq!(tx.document(&address())?, Some(object(42)));
            Ok(id)
        })
        .unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 2);
    assert_eq!(receipt(&path, task).unwrap()["id"], json!(id));
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(42)
    );
}

#[test]
fn admissions_deduplicate_before_validation_and_obey_table_write_boundary() {
    use pi_rs_durable::runtime::AdmissionOptions;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let original = h.submit(c, Some("same".into()), "hello".into()).unwrap();
    h.session_transaction(c, |tx| {
        tx.put_document(&address(), object(1))?;
        assert_eq!(
            tx.submit_input(
                Some("same".into()),
                "".into(),
                AdmissionOptions::default(),
                Some(notification(Uuid::new_v4()))
            )?,
            original
        );
        assert!(matches!(
            tx.submit_continue(Some("same".into()), None),
            Err(DocumentError::InvalidSessionAdmission(_))
        ));
        tx.append_entry("note", None)?;
        assert!(matches!(
            tx.submit_input(
                Some("same".into()),
                "hello".into(),
                AdmissionOptions::default(),
                None
            ),
            Err(DocumentError::ReadAfterWrite)
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(h.conversation(c).unwrap().submissions.len(), 1);
    let d = h.create_conversation(config()).unwrap();
    h.session_transaction(d, |tx| {
        tx.submit_continue(None, None)?;
        assert!(matches!(
            tx.submit_write(None, "note".into(), None, None),
            Err(DocumentError::ReadAfterWrite)
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(h.conversation(d).unwrap().submissions.len(), 1);
}

#[test]
fn admission_callback_and_document_conflicts_roll_back_signal_and_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    let failed: Result<(), Error> = h.session_transaction(c, |tx| {
        tx.put_document(&address(), object(1))?;
        tx.submit_write(None, "note".into(), None, Some(notification(task)))?;
        tx.append_entry("note", None)?;
        Err(DocumentError::InvalidApplicationEntry)
    });
    assert!(failed.is_err());
    let other = Store::open(&path).unwrap();
    let mut calls = 0;
    let failed = h.session_transaction(c, |tx| {
        calls += 1;
        tx.put_document(&address(), object(1))?;
        tx.submit_write(None, "note".into(), None, Some(notification(task)))?;
        tx.append_entry("note", None)?;
        let mut writer = Transaction::new();
        writer.put(&address(), Expected::Missing, &object(9))?;
        writer.commit(&other)?;
        Ok(())
    });
    assert!(matches!(
        failed,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert_eq!(calls, 1);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(c).unwrap().application_writes.is_empty());
    assert!(receipt(&path, task).is_none());
    assert_eq!(
        Documents::new(&other)
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(9)
    );
}

#[test]
fn abort_race_rolls_back_admission_document_and_notification() {
    use pi_rs_durable::runtime::AdmissionOptions;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    let other = Store::open(&path).unwrap();
    let result = h.session_transaction(c, |tx| {
        tx.put_document(&address(), object(1))?;
        tx.submit_input(
            Some("race".into()),
            "hello".into(),
            AdmissionOptions::default(),
            Some(notification(task)),
        )?;
        tx.append_entry("note", None)?;
        let key = format!("runtime/conversation/{c}").into_bytes();
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
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(c).unwrap();
    assert!(state.submissions.is_empty() && state.application_writes.is_empty());
    assert!(receipt(&path, task).is_none());
    assert!(Documents::new(&other).get(&address()).unwrap().is_none());
}

#[test]
fn queued_input_notification_survives_reopen_and_withdrawal() {
    use pi_rs_durable::runtime::{AdmissionOptions, WithdrawResult};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    h.submit(c, None, "active".into()).unwrap();
    let id = h
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(2))?;
            let id = tx.submit_input(
                Some("queued".into()),
                "queued".into(),
                AdmissionOptions::default(),
                Some(notification(task)),
            )?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    assert!(receipt(&path, task).is_none());
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let other_task = notification_target(&path, c);
    h.session_transaction(c, |tx| {
        assert_eq!(
            tx.submit_input(
                Some("queued".into()),
                "".into(),
                AdmissionOptions::default(),
                Some(notification(other_task))
            )?,
            id
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(
        h.withdraw_submission(c, id).unwrap(),
        WithdrawResult::Aborted
    );
    assert_eq!(receipt(&path, task).unwrap()["reason"], "aborted");
    assert!(receipt(&path, other_task).is_none());
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(2)
    );
}

#[tokio::test]
async fn continue_admission_settles_notification_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    let id = h
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(3))?;
            let id = tx.submit_continue(Some("continue".into()), Some(notification(task)))?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    assert!(receipt(&path, task).is_none());
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.resume_conversation(c, &pi_rs_core::CancellationToken::new())
        .await
        .unwrap();
    let receipt = receipt(&path, task).unwrap();
    assert_eq!(receipt["id"], json!(id));
    assert_eq!(receipt["reason"], "nothing_to_answer");
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 1);
}

#[cfg(unix)]
#[test]
fn session_admission_kill_fixture() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_SESSION_ADMISSION_KILL_DB") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let mut h = Harness::open_paused(path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(path, c);
    let id = h
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(7))?;
            let id = tx.submit_write(
                Some("kill".into()),
                "note".into(),
                None,
                Some(notification(task)),
            )?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    println!("ADMISSION_COMMITTED {c} {task} {id}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn session_admission_survives_process_kill() {
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
            .args(["--exact", "session_admission_kill_fixture", "--nocapture"])
            .env("PI_SESSION_ADMISSION_KILL_DB", &path)
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
            if let Some((_, ids)) = line.split_once("ADMISSION_COMMITTED ") {
                let _ = tx.send(ids.to_owned());
                break;
            }
        }
    });
    let ids = rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let ids = ids
        .split_whitespace()
        .map(|s| Uuid::parse_str(s).unwrap())
        .collect::<Vec<_>>();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(ids[0]).unwrap().application_writes.len(), 2);
    assert_eq!(receipt(&path, ids[1]).unwrap()["id"], json!(ids[2]));
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(7)
    );
}

struct Answer;
#[async_trait]
impl Model for Answer {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        use futures_util::{StreamExt, stream};
        use pi_rs_core::ModelEvent;
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}

#[tokio::test]
async fn input_admission_completes_and_notifies_after_reopen() {
    use pi_rs_durable::runtime::AdmissionOptions;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut opts = options();
    opts.model = Arc::new(Answer);
    let mut h = Harness::open_paused(&path, opts).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    let id = h
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(8))?;
            let id = tx.submit_input(
                Some("input".into()),
                "hello".into(),
                AdmissionOptions::default(),
                Some(notification(task)),
            )?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    drop(h);
    let mut opts = options();
    opts.model = Arc::new(Answer);
    let mut h = Harness::open_paused(&path, opts).unwrap();
    h.resume_conversation(c, &pi_rs_core::CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(receipt(&path, task).unwrap()["id"], json!(id));
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 1);
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(8)
    );
}

#[test]
fn transactional_queued_write_is_settled_on_withdrawal() {
    use pi_rs_durable::runtime::WithdrawResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    h.submit(c, None, "active".into()).unwrap();
    h.reset(c, None).unwrap();
    let id = h
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(9))?;
            let id = tx.submit_write(
                Some("queued-write".into()),
                "note".into(),
                None,
                Some(notification(task)),
            )?;
            assert!(matches!(
                tx.append_entry("note", None),
                Err(DocumentError::SessionEntryBlocked)
            ));
            Ok(id)
        })
        .unwrap();
    assert!(receipt(&path, task).is_none());
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.withdraw_submission(c, id).unwrap(),
        WithdrawResult::Aborted
    );
    assert_eq!(receipt(&path, task).unwrap()["reason"], "aborted");
}

#[test]
fn concurrent_duplicate_request_conflicts_without_replaying_callback() {
    use pi_rs_durable::runtime::AdmissionOptions;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    // Capture a real admission as the independently committed competing state.
    let template = h.create_conversation(config()).unwrap();
    let winner = h
        .submit(template, Some("shared".into()), "winner".into())
        .unwrap();
    let template_state = serde_json::to_value(h.conversation(template).unwrap()).unwrap();
    let mut winning_state = serde_json::to_value(h.conversation(c).unwrap()).unwrap();
    winning_state["submissions"] = template_state["submissions"].clone();
    let other = Store::open(&path).unwrap();
    let mut calls = 0;
    let result = h.session_transaction(c, |tx| {
        calls += 1;
        tx.put_document(&address(), object(1))?;
        tx.submit_input(
            Some("shared".into()),
            "first".into(),
            AdmissionOptions::default(),
            None,
        )?;
        let key = format!("runtime/conversation/{c}").into_bytes();
        let current = other.get(&key)?.unwrap();
        other.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(current.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&winning_state)?,
            }],
        )?;
        Ok(())
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert_eq!(calls, 1);
    let state = h.conversation(c).unwrap();
    assert_eq!(state.submissions.len(), 1);
    assert_eq!(state.submissions[0].id, winner);
    assert!(Documents::new(&other).get(&address()).unwrap().is_none());
}

#[test]
fn rejected_policy_does_not_modify_transaction_candidate() {
    use pi_rs_durable::{runtime::AdmissionOptions, state::AdmissionError};
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    h.session_transaction(c, |tx| {
        let options = AdmissionOptions {
            reject_when_idle: true,
            ..Default::default()
        };
        assert!(matches!(
            tx.submit_input(None, "hello".into(), options, None),
            Err(DocumentError::SessionAdmission(AdmissionError::Idle))
        ));
        tx.submit_write(None, "note".into(), None, None)?;
        Ok(())
    })
    .unwrap();
    let state = h.conversation(c).unwrap();
    assert!(state.submissions.is_empty());
    assert_eq!(state.application_writes.len(), 1);
}

#[tokio::test]
async fn scheduler_admission_commits_notification_with_document() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let c = scheduler.create_conversation(config()).unwrap();
    let task = notification_target(&path, c);
    let id = scheduler
        .session_transaction(c, |tx| {
            tx.put_document(&address(), object(11))?;
            let id = tx.submit_write(None, "note".into(), None, Some(notification(task)))?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    scheduler.shutdown().await.unwrap();
    assert_eq!(receipt(&path, task).unwrap()["id"], json!(id));
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 2);
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value,
        object(11)
    );
}
