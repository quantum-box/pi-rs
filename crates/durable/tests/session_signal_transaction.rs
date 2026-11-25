use async_trait::async_trait;
use durable_store::Store;
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    document::{Address, Documents, Error as DError},
    recovery::UncertainPolicy,
    runtime::{Config, Error, Harness, Options, SubmissionNotify, submission_settled_signal},
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_store::{Save, SignalResult, SignalState, SignalToken, TaskStore},
};
use serde_json::{Value, json};
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
        name: "signal".into(),
        key: "".into(),
    }
}
fn token() -> SignalToken {
    SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap()
}
fn setup(path: &std::path::Path, c: Uuid, deadline: Option<i64>) -> Uuid {
    let store = Store::open(path).unwrap();
    let tasks = TaskStore::new(&store);
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(c),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    tasks.save(&[Save::Create(&task)]).unwrap();
    if let Some(deadline) = deadline {
        tasks
            .reserve(&tasks.load(task.id()).unwrap().unwrap())
            .unwrap();
        tasks
            .update(&tasks.load(task.id()).unwrap().unwrap(), |task| {
                task.wait(WaitFor::TypedSleep {
                    until_millis: Some(deadline),
                    signals: vec![token().key("a").unwrap()],
                })
            })
            .unwrap();
    }
    task.id()
}
fn read(path: &std::path::Path, task: Uuid, token: &SignalToken, key: &str) -> Option<Value> {
    TaskStore::new(&Store::open(path).unwrap())
        .read_typed_signal(task, token, key)
        .unwrap()
}

#[test]
fn signal_document_and_entry_commit_atomically_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = setup(&path, c, None);
    h.session_transaction(c, |tx| {
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        assert_eq!(
            tx.signal(task, &token(), "a", json!(1))?,
            SignalResult::Delivered
        );
        assert!(matches!(
            tx.signal(task, &token(), "a", json!(2)),
            Err(DError::ReadAfterWrite)
        ));
        assert!(matches!(
            tx.submit_write(None, "note".into(), None, None),
            Err(DError::ReadAfterWrite)
        ));
        tx.append_entry("note", None)?;
        assert_eq!(tx.document(&address())?.unwrap()["n"], 1);
        Ok(())
    })
    .unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 1);
    assert_eq!(read(&path, task, &token(), "a"), Some(json!(1)));
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
}

#[test]
fn no_op_signal_observations_compose_with_write_notification() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = setup(&path, c, None);
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    assert_eq!(
        tasks
            .deliver_signal_at(task, &token(), "a", json!(7), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    let id = h
        .session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(task, &token(), "a", json!(8))?,
                SignalResult::Duplicate
            );
            assert_eq!(
                tx.signal(task, &token(), "invalid", json!("bad"))?,
                SignalResult::Invalid
            );
            assert_eq!(
                tx.signal(Uuid::new_v4(), &token(), "x", json!(1))?,
                SignalResult::NotFound
            );
            tx.put_document(&address(), json!({"n":2}).as_object().unwrap().clone())?;
            let id = tx.submit_write(
                Some("request".into()),
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: task,
                    key: "done".into(),
                }),
            )?;
            tx.append_entry("note", None)?;
            assert!(matches!(
                tx.signal(task, &token(), "a", json!(1)),
                Err(DError::ReadAfterWrite)
            ));
            Ok(id)
        })
        .unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 2);
    assert_eq!(read(&path, task, &token(), "a"), Some(json!(7)));
    assert_eq!(
        read(&path, task, &submission_settled_signal(), "done").unwrap()["id"],
        json!(id)
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        2
    );
}

#[test]
fn deadline_expiry_is_a_write_and_commit_time_deadline_is_fenced() {
    for expired in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.set_signal_clock(Arc::new(move || if expired { 20 } else { 0 }));
        let c = h.create_conversation(config()).unwrap();
        let task = setup(&path, c, Some(if expired { 20 } else { 1 }));
        let result = h.session_transaction(c, |tx| {
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            assert_eq!(
                tx.signal(task, &token(), "a", json!(1))?,
                if expired {
                    SignalResult::Closed
                } else {
                    SignalResult::Delivered
                }
            );
            assert!(matches!(
                tx.signal(task, &token(), "b", json!(1)),
                Err(DError::ReadAfterWrite)
            ));
            tx.append_entry("note", None)?;
            if !expired {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Ok(())
        });
        if expired {
            result.unwrap();
            assert_eq!(
                TaskStore::new(&Store::open(&path).unwrap())
                    .read_typed_signal_state(task, &token(), "a")
                    .unwrap(),
                Some(SignalState::Expired)
            );
        } else {
            assert!(matches!(
                result,
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
            ));
            assert!(
                Documents::new(&Store::open(&path).unwrap())
                    .get(&address())
                    .unwrap()
                    .is_none()
            );
            assert!(read(&path, task, &token(), "a").is_none());
        }
    }
}

#[test]
fn target_abort_or_conversation_abort_rolls_back_signal_and_documents() {
    use durable_store::{Check, Expected, Mutation};
    for conversation_abort in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let task = setup(&path, c, None);
        let other = Store::open(&path).unwrap();
        let mut calls = 0;
        let result = h.session_transaction(c, |tx| {
            calls += 1;
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            assert_eq!(
                tx.signal(task, &token(), "a", json!(1))?,
                SignalResult::Delivered
            );
            tx.append_entry("note", None)?;
            if conversation_abort {
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
            } else {
                let tasks = TaskStore::new(&other);
                tasks.update(&tasks.load(task)?.unwrap(), |task| task.cancel())?;
            }
            Ok(())
        });
        assert!(matches!(
            result,
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(calls, 1);
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        assert!(h.conversation(c).unwrap().application_writes.is_empty());
        assert!(read(&path, task, &token(), "a").is_none());
        assert!(Documents::new(&other).get(&address()).unwrap().is_none());
    }
}

#[test]
fn not_found_observation_fences_a_later_task_creation() {
    for write_document in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let other = Store::open(&path).unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(c),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap();
        let result = h.session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(task.id(), &token(), "a", json!(1))?,
                SignalResult::NotFound
            );
            if write_document {
                tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            }
            TaskStore::new(&other).save(&[Save::Create(&task)])?;
            Ok(())
        });
        assert!(matches!(
            result,
            Err(Error::Storage(durable_store::Error::Conflict { .. }))
        ));
        assert!(Documents::new(&other).get(&address()).unwrap().is_none());
    }
}

#[test]
fn callback_error_rolls_back_signal_and_expired_marker() {
    for expired in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.set_signal_clock(Arc::new(|| 20));
        let c = h.create_conversation(config()).unwrap();
        let task = setup(&path, c, expired.then_some(20));
        let result: Result<(), Error> = h.session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(task, &token(), "a", json!(1))?,
                if expired {
                    SignalResult::Closed
                } else {
                    SignalResult::Delivered
                }
            );
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            Err(DError::InvalidSessionEntry)
        });
        assert!(result.is_err());
        let store = Store::open(&path).unwrap();
        assert!(
            TaskStore::new(&store)
                .read_typed_signal_state(task, &token(), "a")
                .unwrap()
                .is_none()
        );
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    }
}

#[tokio::test]
async fn async_session_signal_uses_the_same_order_and_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = setup(&path, c, None);
    h.session_transaction_async(c, move |tx| {
        Box::pin(async move {
            assert_eq!(
                tx.signal(task, &token(), "a", json!(3))?,
                SignalResult::Delivered
            );
            tokio::task::yield_now().await;
            assert!(matches!(
                tx.signal(task, &token(), "b", json!(4)),
                Err(DError::ReadAfterWrite)
            ));
            tx.put_document(&address(), json!({"n":3}).as_object().unwrap().clone())?;
            tx.append_entry("note", None)?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert_eq!(read(&path, task, &token(), "a"), Some(json!(3)));
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        3
    );
}

#[cfg(unix)]
#[test]
fn signal_session_kill_fixture() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_SESSION_SIGNAL_KILL_DB") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let mut h = Harness::open_paused(path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = setup(path, c, None);
    h.session_transaction(c, |tx| {
        tx.put_document(&address(), json!({"n":7}).as_object().unwrap().clone())?;
        assert_eq!(
            tx.signal(task, &token(), "a", json!(7))?,
            SignalResult::Delivered
        );
        tx.append_entry("note", None)?;
        Ok(())
    })
    .unwrap();
    println!("SIGNAL_TX_COMMITTED {c} {task}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn signal_session_survives_process_kill() {
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
            .args(["--exact", "signal_session_kill_fixture", "--nocapture"])
            .env("PI_SESSION_SIGNAL_KILL_DB", &path)
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
            if let Some((_, ids)) = line.split_once("SIGNAL_TX_COMMITTED ") {
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
    assert_eq!(h.conversation(ids[0]).unwrap().application_writes.len(), 1);
    assert_eq!(read(&path, ids[1], &token(), "a"), Some(json!(7)));
    assert_eq!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        7
    );
}

#[test]
fn duplicate_signal_then_queued_input_preserves_notification_on_reopen() {
    use pi_rs_durable::runtime::{AdmissionOptions, WithdrawResult};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = setup(&path, c, None);
    let closed = setup(&path, c, None);
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    tasks
        .update(&tasks.load(closed).unwrap().unwrap(), |task| task.cancel())
        .unwrap();
    assert_eq!(
        tasks
            .deliver_signal_at(task, &token(), "a", json!(7), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    h.submit(c, None, "active".into()).unwrap();
    let id = h
        .session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(closed, &token(), "x", json!(1))?,
                SignalResult::Closed
            );
            assert_eq!(
                tx.signal(task, &token(), "a", json!(9))?,
                SignalResult::Duplicate
            );
            let id = tx.submit_input(
                Some("queued".into()),
                "hello".into(),
                AdmissionOptions::default(),
                Some(SubmissionNotify {
                    task_id: task,
                    key: "input".into(),
                }),
            )?;
            tx.put_document(&address(), json!({"n":4}).as_object().unwrap().clone())?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.withdraw_submission(c, id).unwrap(),
        WithdrawResult::Aborted
    );
    assert_eq!(
        read(&path, task, &submission_settled_signal(), "input").unwrap()["reason"],
        "aborted"
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        4
    );
}

#[test]
fn duplicate_builtin_signal_and_settlement_share_candidate_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let task = setup(&path, c, None);
    let store = Store::open(&path).unwrap();
    let prior = json!({"id":Uuid::new_v4(), "conversationId":c, "type":"write", "status":"done", "entry":Uuid::new_v4()});
    assert_eq!(
        TaskStore::new(&store)
            .deliver_signal_at(task, &submission_settled_signal(), "same", prior.clone(), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    h.session_transaction(c, |tx| {
        assert_eq!(
            tx.signal(task, &submission_settled_signal(), "same", prior.clone())?,
            SignalResult::Duplicate
        );
        tx.submit_write(
            None,
            "note".into(),
            None,
            Some(SubmissionNotify {
                task_id: task,
                key: "same".into(),
            }),
        )?;
        tx.put_document(&address(), json!({"n":5}).as_object().unwrap().clone())?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        read(&path, task, &submission_settled_signal(), "same"),
        Some(prior)
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        5
    );
}
