use async_trait::async_trait;
use durable_store::Store;
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    document::{Address, Documents, Error as DError},
    recovery::UncertainPolicy,
    runtime::{Config, Error, Harness, Options, SubmissionNotify, submission_settled_signal},
    task::{
        Checkpoint, Task, TaskOwner, TaskState, WaitFor,
        scheduler::{Registry, Step},
    },
    task_store::{SignalResult, SignalToken, TaskStore},
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
        name: "task".into(),
        key: "".into(),
    }
}
fn task(owner: Uuid) -> Task {
    Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("job".into(), true)
}
fn registry() -> Registry {
    let mut registry = Registry::default();
    registry.register("job", true, |task: &Task| {
        Ok(Step::Wait {
            checkpoint: task.checkpoint().clone(),
            condition: WaitFor::DeadlineMillis(i64::MAX),
            children: vec![],
        })
    });
    registry
}
fn load(path: &std::path::Path, id: Uuid) -> Option<Task> {
    TaskStore::new(&Store::open(path).unwrap())
        .load(id)
        .unwrap()
        .map(|t| t.task)
}

#[test]
fn creation_composes_signal_observation_submission_notification_and_documents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let driver = h.start_tasks(registry()).unwrap();
    let task = task(c);
    let task_id = task.id();
    let token = SignalToken::new("test", 1).unwrap();
    let write_id = h
        .session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(task_id, &token, "early", json!(1))?,
                SignalResult::NotFound
            );
            let write = tx.submit_write(
                Some("write".into()),
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id,
                    key: "done".into(),
                }),
            )?;
            assert_eq!(tx.create_task(task.clone())?, task_id);
            tx.append_entry("note", None)?;
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            assert!(matches!(
                tx.signal(task_id, &token, "later", json!(1)),
                Err(DError::ReadAfterWrite)
            ));
            Ok(write)
        })
        .unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 2);
    assert_eq!(load(&path, task_id).unwrap().kind(), Some("job"));
    let store = Store::open(&path).unwrap();
    let receipt = TaskStore::new(&store)
        .read_typed_signal(task_id, &submission_settled_signal(), "done")
        .unwrap()
        .unwrap();
    assert_eq!(receipt["id"], json!(write_id));
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(task_id, &token, "early")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
}

#[test]
fn invalid_tasks_and_missing_driver_roll_back_all_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let good = task(c);
    let result = h.session_transaction(c, |tx| {
        tx.create_task(good.clone())?;
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        Ok(())
    });
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert!(load(&path, good.id()).is_none());
    let driver = h.start_tasks(registry()).unwrap();
    for bad in [
        task(c).registered("unknown".into(), true),
        task(c).registered("job".into(), false),
        task(c)
            .registered_versioned("job".into(), true, 2, json!(null))
            .unwrap(),
    ] {
        let id = bad.id();
        let result = h.session_transaction(c, |tx| {
            tx.create_task(bad)?;
            tx.append_entry("note", None)?;
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            Ok(())
        });
        assert!(matches!(result, Err(Error::Invalid(_))));
        assert!(load(&path, id).is_none());
    }
    let result = h.session_transaction(c, |tx| {
        let unregistered = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(c),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap();
        assert!(matches!(
            tx.create_task(unregistered),
            Err(DError::InvalidSessionAdmission(_))
        ));
        assert!(matches!(
            tx.create_task(task(Uuid::new_v4())),
            Err(DError::InvalidSessionAdmission(_))
        ));
        tx.create_task(good.clone())?;
        assert!(matches!(
            tx.create_task(good.clone()),
            Err(DError::DuplicateTask)
        ));
        Err::<(), _>(DError::InvalidSessionEntry)
    });
    assert!(result.is_err());
    assert!(load(&path, good.id()).is_none());
    assert!(h.conversation(c).unwrap().application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}

#[test]
fn creation_after_entry_and_successful_signal_preserves_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let driver = h.start_tasks(registry()).unwrap();
    let existing = task(c).registered("unregistered".into(), true);
    let store = Store::open(&path).unwrap();
    TaskStore::new(&store)
        .save(&[pi_rs_durable::task_store::Save::Create(&existing)])
        .unwrap();
    let token = SignalToken::new("test", 1).unwrap();
    let new = task(c);
    h.session_transaction(c, |tx| {
        assert_eq!(
            tx.signal(existing.id(), &token, "a", json!(1))?,
            SignalResult::Delivered
        );
        tx.append_entry("note", None)?;
        tx.create_task(new.clone())?;
        assert!(matches!(
            tx.submit_continue(None, None),
            Err(DError::ReadAfterWrite)
        ));
        Ok(())
    })
    .unwrap();
    driver.shutdown().unwrap();
    assert!(load(&path, new.id()).is_some());
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(existing.id(), &token, "a")
            .unwrap(),
        Some(json!(1))
    );
    assert_ne!(
        load(&path, new.id()).unwrap().state(),
        &TaskState::Cancelled
    );
}

#[test]
fn driver_replacement_and_duplicate_id_are_checked_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let driver = h.start_tasks(registry()).unwrap();
    let pending = task(c);
    let result = h.session_transaction(c, |tx| {
        tx.create_task(pending.clone())?;
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        driver.replace_registry(Registry::default()).unwrap();
        Ok(())
    });
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert!(load(&path, pending.id()).is_none());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    driver.replace_registry(registry()).unwrap();
    let existing = task(c);
    driver.submit(&existing).unwrap();
    let result = h.session_transaction(c, |tx| {
        tx.create_task(existing.clone())?;
        tx.append_entry("note", None)?;
        Ok(())
    });
    assert!(matches!(result, Err(Error::Invalid(_))));
    assert!(h.conversation(c).unwrap().application_writes.is_empty());
    driver.shutdown().unwrap();
}

#[test]
fn abort_and_document_conflict_roll_back_task_admission() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::document::Transaction;
    for abort in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let driver = h.start_tasks(registry()).unwrap();
        let pending = task(c);
        let other = Store::open(&path).unwrap();
        let mut calls = 0;
        let result = h.session_transaction(c, |tx| {
            calls += 1;
            tx.create_task(pending.clone())?;
            tx.append_entry("note", None)?;
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            if abort {
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
                let mut writer = Transaction::new();
                writer.put(
                    &address(),
                    Expected::Missing,
                    json!({"n":9}).as_object().unwrap(),
                )?;
                writer.commit(&other)?;
            }
            Ok(())
        });
        assert!(matches!(
            result,
            Err(Error::Storage(durable_store::Error::Conflict { .. })) | Err(Error::Invalid(_))
        ));
        assert_eq!(calls, 1);
        assert!(load(&path, pending.id()).is_none());
        assert!(h.conversation(c).unwrap().application_writes.is_empty());
        assert_eq!(
            Documents::new(&other)
                .get(&address())
                .unwrap()
                .map(|d| d.value["n"].clone()),
            if abort { None } else { Some(json!(9)) }
        );
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn async_transaction_creates_registered_task() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let driver = h.start_tasks(registry()).unwrap();
    let pending = task(c);
    let id = pending.id();
    h.session_transaction_async(c, move |tx| {
        Box::pin(async move {
            tx.append_entry("note", None)?;
            tokio::task::yield_now().await;
            tx.create_task(pending)?;
            tx.put_document(&address(), json!({"n":3}).as_object().unwrap().clone())?;
            Ok(())
        })
    })
    .await
    .unwrap();
    driver.shutdown().unwrap();
    assert!(load(&path, id).is_some());
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
fn task_session_kill_fixture() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_SESSION_TASK_KILL_DB") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let mut h = Harness::open_paused(path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let _driver = h.start_tasks(registry()).unwrap();
    let pending = task(c);
    let id = pending.id();
    let write = h
        .session_transaction(c, |tx| {
            let write = tx.submit_write(
                None,
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: id,
                    key: "done".into(),
                }),
            )?;
            tx.create_task(pending)?;
            tx.append_entry("note", None)?;
            tx.put_document(&address(), json!({"n":7}).as_object().unwrap().clone())?;
            Ok(write)
        })
        .unwrap();
    println!("TASK_TX_COMMITTED {c} {id} {write}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn task_session_survives_process_kill() {
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
            .args(["--exact", "task_session_kill_fixture", "--nocapture"])
            .env("PI_SESSION_TASK_KILL_DB", &path)
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
            if let Some((_, ids)) = line.split_once("TASK_TX_COMMITTED ") {
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
    assert!(load(&path, ids[1]).is_some());
    assert_eq!(h.conversation(ids[0]).unwrap().application_writes.len(), 2);
    let store = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(ids[1], &submission_settled_signal(), "done")
            .unwrap()
            .unwrap()["id"],
        json!(ids[2])
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        7
    );
}

#[test]
fn ancestor_signal_deadline_is_not_weakened_by_new_task_graph() {
    use pi_rs_durable::ownership_edge::{Edge, EdgeStore, Mode};
    use pi_rs_durable::task_store::Save;
    for scenario in 0..3 {
        let expires = scenario == 1;
        let changes_version = scenario == 2;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let deadline = now + 5_000;
        let signal_now = if expires { deadline - 1 } else { now };
        h.set_signal_clock(Arc::new(move || signal_now));
        let root = h.create_conversation(config()).unwrap();
        let child = h.create_conversation(config()).unwrap();
        let store = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&store);
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(root),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap();
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let edge = Edge {
            task: parent.id(),
            task_owner: parent.owner().clone(),
            child,
            mode: Mode::Foreground,
        };
        let prepared = EdgeStore::new(&store).prepare_create(&edge).unwrap();
        store.commit(&prepared.checks, &prepared.mutations).unwrap();
        let token = SignalToken::new("deadline", 1).unwrap();
        tasks
            .reserve(&tasks.load(parent.id()).unwrap().unwrap())
            .unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.wait(WaitFor::TypedSleep {
                    until_millis: Some(deadline),
                    signals: vec![token.key("a").unwrap()],
                })
            })
            .unwrap();
        let driver = h.start_tasks(registry()).unwrap();
        let pending = task(child);
        let result = h.session_transaction(child, |tx| {
            assert_eq!(
                tx.signal(parent.id(), &token, "a", json!(1))?,
                SignalResult::Delivered
            );
            if changes_version {
                tasks.update(&tasks.load(parent.id())?.unwrap(), |_| Ok(()))?;
            }
            tx.create_task(pending.clone())?;
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            if expires {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Ok(())
        });
        if expires || changes_version {
            assert!(matches!(
                result,
                Err(Error::Storage(durable_store::Error::Conflict { .. }))
            ));
            assert!(load(&path, pending.id()).is_none());
            assert!(
                tasks
                    .read_typed_signal(parent.id(), &token, "a")
                    .unwrap()
                    .is_none()
            );
            assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        } else {
            result.unwrap();
            assert!(load(&path, pending.id()).is_some());
            assert_eq!(
                tasks.read_typed_signal(parent.id(), &token, "a").unwrap(),
                Some(json!(1))
            );
        }
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn scheduler_driver_executes_only_after_atomic_commit() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut scheduler = Scheduler::open(&path, options()).unwrap();
    let c = scheduler.create_conversation(config()).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let observed_path = path.clone();
    let mut definitions = Registry::default();
    definitions.register("job", true, move |task: &Task| {
        let store = Store::open(&observed_path).map_err(|e| e.to_string())?;
        let document = Documents::new(&store)
            .get(&address())
            .map_err(|e| e.to_string())?
            .ok_or("missing document")?;
        let receipt = TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "done")
            .map_err(|e| e.to_string())?
            .ok_or("missing notification")?;
        tx.send((document.value["n"].clone(), receipt["id"].clone()))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    });
    let driver = scheduler.start_tasks(definitions).unwrap();
    let pending = task(c);
    let id = pending.id();
    let write = scheduler
        .session_transaction(c, |tx| {
            let write = tx.submit_write(
                None,
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: id,
                    key: "done".into(),
                }),
            )?;
            tx.create_task(pending)?;
            tx.put_document(&address(), json!({"n":6}).as_object().unwrap().clone())?;
            Ok(write)
        })
        .unwrap();
    let seen = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert_eq!(seen, (json!(6), json!(write)));
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[test]
fn multiple_registered_tasks_share_one_atomic_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let driver = h.start_tasks(registry()).unwrap();
    let first = task(c);
    let second = task(c);
    let ids = (first.id(), second.id());
    let write = h
        .session_transaction(c, |tx| {
            let write = tx.submit_write(
                None,
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: ids.1,
                    key: "done".into(),
                }),
            )?;
            tx.create_task(first)?;
            tx.create_task(second)?;
            tx.put_document(&address(), json!({"n":8}).as_object().unwrap().clone())?;
            Ok(write)
        })
        .unwrap();
    driver.shutdown().unwrap();
    assert!(load(&path, ids.0).is_some());
    assert!(load(&path, ids.1).is_some());
    let store = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(ids.1, &submission_settled_signal(), "done")
            .unwrap()
            .unwrap()["id"],
        json!(write)
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        8
    );
}
