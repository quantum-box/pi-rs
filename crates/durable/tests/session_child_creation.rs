use async_trait::async_trait;
use durable_store::Store;
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    document::{Address, Documents, Error as DError},
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
    task::{
        Checkpoint, Task, TaskOwner,
        scheduler::{Registry, Step},
    },
    task_store::TaskStore,
};
use serde_json::json;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use uuid::Uuid;
struct Unused;
#[async_trait]
impl Model for Unused {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        panic!("unused")
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
        name: "child".into(),
        key: "".into(),
    }
}
fn task(owner: TaskOwner, kind: &str, input: i32) -> Task {
    Task::new(
        Uuid::new_v4(),
        owner,
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered_versioned(kind.into(), true, 1, json!({"n":input}))
    .unwrap()
}
fn attempt_id(task: &Task) -> serde_json::Value {
    serde_json::to_value(task).unwrap()["attempt_id"].clone()
}
fn driver(
    h: &Harness,
) -> (
    pi_rs_durable::task_driver::Driver,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Arc::new(Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("parent", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    });
    registry.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    (h.start_tasks(registry).unwrap(), started_rx, release_tx)
}
#[test]
fn keyed_child_is_atomic_and_duplicate_identity_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let id = child.id();
    h.session_transaction(c, |tx| {
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        assert_eq!(tx.create_keyed_child(&snapshot, "name", child.clone())?, id);
        let duplicate = task(TaskOwner::Task(parent.id()), "child", 1);
        assert_eq!(tx.create_keyed_child(&snapshot, "name", duplicate)?, id);
        tx.append_entry("note", None)?;
        Ok(())
    })
    .unwrap();
    let latest = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let alternative = task(TaskOwner::Task(parent.id()), "child", 1);
    let head_before_duplicate = h.history_head(c).unwrap();
    let result = h
        .session_transaction(c, |tx| {
            tx.create_keyed_child(&latest, "name", alternative.clone())
        })
        .unwrap();
    assert_eq!(result, id);
    assert_eq!(h.history_head(c).unwrap(), head_before_duplicate);
    assert!(
        TaskStore::new(&store)
            .load(alternative.id())
            .unwrap()
            .is_none()
    );
    let latest = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let mismatch = task(TaskOwner::Task(parent.id()), "child", 2);
    assert!(matches!(
        h.session_transaction(c, |tx| tx.create_keyed_child(&latest, "name", mismatch)),
        Err(pi_rs_durable::runtime::Error::Document(
            DError::InvalidSessionAdmission(_)
        ))
    ));
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
    release.send(()).unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 1);
    assert!(TaskStore::new(&store).load(id).unwrap().is_some());
}

#[test]
fn stale_parent_and_callback_error_discard_child_and_document() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let result: Result<(), _> = h.session_transaction(c, |tx| {
        tx.create_child_task(&snapshot, child.clone())?;
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        Err(DError::InvalidSessionEntry)
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    TaskStore::new(&store)
        .update(&snapshot, |_| Ok(()))
        .unwrap();
    let result = h.session_transaction(c, |tx| tx.create_child_task(&snapshot, child.clone()));
    assert!(result.is_err());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn parent_finishing_after_staging_fences_child_and_document() {
    use pi_rs_durable::task::TaskState;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let result = h.session_transaction(c, |tx| {
        tx.create_keyed_child(&snapshot, "race", child.clone())?;
        tx.put_document(&address(), json!({"n":2}).as_object().unwrap().clone())?;
        release.send(()).unwrap();
        let limit = std::time::Instant::now() + Duration::from_secs(10);
        while TaskStore::new(&store)
            .load(parent.id())?
            .unwrap()
            .task
            .state()
            == &TaskState::Running
        {
            assert!(std::time::Instant::now() < limit);
            std::thread::sleep(Duration::from_millis(2));
        }
        Ok(())
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn child_kill_fixture() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_SESSION_CHILD_KILL_DB") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let mut h = Harness::open_paused(path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    if std::env::var("PI_SESSION_CHILD_OWNED").as_deref() == Ok("1") {
        use pi_rs_durable::{
            ownership_edge::{Edge, EdgeStore, Mode},
            task_store::Save,
        };
        let root = h.create_conversation(config()).unwrap();
        let store = Store::open(path).unwrap();
        let ts = TaskStore::new(&store);
        let owner = task(TaskOwner::Conversation(root), "unknown", 0);
        ts.save(&[Save::Create(&owner)]).unwrap();
        let edge = Edge {
            task: owner.id(),
            task_owner: owner.owner().clone(),
            child: c,
            mode: Mode::Foreground,
        };
        let prepared = EdgeStore::new(&store).prepare_create(&edge).unwrap();
        store.commit(&prepared.checks, &prepared.mutations).unwrap();
        let snapshot = ts.load(owner.id()).unwrap().unwrap();
        ts.update(&snapshot, |task| task.start()).unwrap();
        let snapshot = ts.load(owner.id()).unwrap().unwrap();
        ts.update(&snapshot, |task| task.succeed(json!(null)))
            .unwrap();
    }
    let (driver, started, _release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let id = h
        .session_transaction(c, |tx| {
            let id = tx.create_keyed_child(&snapshot, "restart", child)?;
            tx.put_document(&address(), json!({"n":7}).as_object().unwrap().clone())?;
            tx.append_entry("note", None)?;
            Ok(id)
        })
        .unwrap();
    println!("CHILD_COMMITTED {c} {} {id}", parent.id());
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn keyed_child_survives_kill_and_new_parent_attempt() {
    use std::{
        io::{BufRead, BufReader},
        process::{Child, Command, Stdio},
    };
    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for owned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut process = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "child_kill_fixture", "--nocapture"])
                .env("PI_SESSION_CHILD_KILL_DB", &path)
                .env("PI_SESSION_CHILD_OWNED", if owned { "1" } else { "0" })
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let output = process.0.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let line = line.unwrap();
                if let Some((_, ids)) = line.split_once("CHILD_COMMITTED ") {
                    let _ = tx.send(ids.to_string());
                    break;
                }
            }
        });
        let ids = rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let ids = ids
            .split_whitespace()
            .map(|s| Uuid::parse_str(s).unwrap())
            .collect::<Vec<_>>();
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let (driver, started, release) = driver(&h);
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let store = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&store);
        let attempt = attempt_id(&tasks.load(ids[1]).unwrap().unwrap().task);
        assert!(!attempt.is_null());
        let candidate = task(TaskOwner::Task(ids[1]), "child", 1);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let found = loop {
            let snapshot = tasks.load(ids[1]).unwrap().unwrap();
            assert_eq!(attempt_id(&snapshot.task), attempt);
            let result = h.session_transaction(ids[0], |tx| {
                tx.create_keyed_child(&snapshot, "restart", candidate.clone())
            });
            match result {
                Ok(id) => break id,
                Err(error) => {
                    let fresh = tasks.load(ids[1]).unwrap().unwrap();
                    let parent_changed = fresh.version != snapshot.version
                        && attempt_id(&fresh.task) == attempt
                        && fresh.task.state() == &pi_rs_durable::task::TaskState::Running
                        && !fresh.task.abort_requested();
                    let checked_task_conflict = matches!(&error,
                        pi_rs_durable::runtime::Error::Storage(durable_store::Error::Conflict { key })
                            if key == &{ let mut k = b"durable/task/".to_vec(); k.extend_from_slice(ids[1].as_bytes()); k }
                                || key == &{ let mut k = b"durable/task/".to_vec(); k.extend_from_slice(ids[2].as_bytes()); k });
                    let stale_parent = parent_changed
                        && matches!(
                            &error,
                            pi_rs_durable::runtime::Error::Invalid(_)
                                | pi_rs_durable::runtime::Error::Document(
                                    DError::InvalidSessionAdmission(_)
                                )
                        );
                    assert!(
                        checked_task_conflict || stale_parent,
                        "unexpected admission error: {error:?}"
                    );
                    assert!(
                        std::time::Instant::now() < deadline,
                        "admission did not settle: {error:?}"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        };
        assert_eq!(found, ids[2]);
        assert!(
            TaskStore::new(&store)
                .load(candidate.id())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            Documents::new(&store)
                .get(&address())
                .unwrap()
                .unwrap()
                .value["n"],
            7
        );
        release.send(()).unwrap();
        driver.shutdown().unwrap();
    }
}

#[test]
fn unkeyed_child_composes_signal_submission_notification_and_entry() {
    use pi_rs_durable::{
        runtime::{SubmissionNotify, submission_settled_signal},
        task_store::{SignalResult, SignalToken},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let child_id = child.id();
    let token = SignalToken::new("test", 1).unwrap();
    let write = h
        .session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(child_id, &token, "early", json!(1))?,
                SignalResult::NotFound
            );
            let write = tx.submit_write(
                None,
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: child_id,
                    key: "done".into(),
                }),
            )?;
            tx.create_child_task(&snapshot, child)?;
            tx.append_entry("note", None)?;
            tx.put_document(&address(), json!({"n":4}).as_object().unwrap().clone())?;
            Ok(write)
        })
        .unwrap();
    let signal = TaskStore::new(&store)
        .read_typed_signal(child_id, &submission_settled_signal(), "done")
        .unwrap()
        .unwrap();
    assert_eq!(signal["id"], json!(write));
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        4
    );
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn abort_mark_after_staging_rejects_child() {
    use durable_store::{Check, Expected, Mutation};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let result = h.session_transaction(c, |tx| {
        tx.create_child_task(&snapshot, child.clone())?;
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        let mut key = b"durable/task/".to_vec();
        key.extend_from_slice(parent.id().as_bytes());
        let entry = store.get(&key)?.unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&entry.value)?;
        value["abort_requested"] = json!(true);
        store.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&value)?,
            }],
        )?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn competing_keyed_admission_conflicts_then_finds_winner() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::task_store::Save;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    let snapshot = tasks.load(parent.id()).unwrap().unwrap();
    let candidate = task(TaskOwner::Task(parent.id()), "child", 1);
    let winner = task(TaskOwner::Task(parent.id()), "child", 1);
    let result = h.session_transaction(c, |tx| {
        tx.create_keyed_child(&snapshot, "race", candidate.clone())?;
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        let mut other = tasks.prepare_save(&[Save::Create(&winner)])?;
        let mut parent_key = b"durable/task/".to_vec(); parent_key.extend_from_slice(parent.id().as_bytes());
        let member = "race".as_bytes().iter().map(|b| format!("{b:02x}")).collect::<String>();
        let key = format!("durable/keyed-child/{}/{member}", parent.id()).into_bytes();
        other.checks.push(Check { key: parent_key.clone(), expected: Expected::Version(snapshot.version) });
        other.checks.push(Check { key: key.clone(), expected: Expected::Missing });
        other.mutations.push(Mutation::Put { key: parent_key, value: serde_json::to_vec(&snapshot.task)? });
        other.mutations.push(Mutation::Put { key, value: serde_json::to_vec(&json!({"child":winner.id(),"identity":{"tool":"child","arguments":{"n":1},"replay_safe":true,"task_kind":"child","task_version":1,"initial_checkpoint":winner.checkpoint(),"restart_limit":winner.restart_limit(),"task_tool":false}}))? });
        store.commit(&other.checks, &other.mutations)?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(tasks.load(candidate.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let current = tasks.load(parent.id()).unwrap().unwrap();
    let found = h
        .session_transaction(c, |tx| {
            tx.create_keyed_child(&current, "race", candidate.clone())
        })
        .unwrap();
    assert_eq!(found, winner.id());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn async_child_transaction_uses_same_attempt_fence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let id = child.id();
    h.session_transaction_async(c, move |tx| {
        Box::pin(async move {
            tx.create_child_task(&snapshot, child)?;
            tokio::task::yield_now().await;
            tx.put_document(&address(), json!({"n":5}).as_object().unwrap().clone())?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert!(TaskStore::new(&store).load(id).unwrap().is_some());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn task_owned_conversation_accepts_children_and_recovers() {
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore, Mode},
        task_store::Save,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let root = h.create_conversation(config()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let owner = task(TaskOwner::Conversation(root), "unknown", 0);
    TaskStore::new(&store)
        .save(&[Save::Create(&owner)])
        .unwrap();
    let edge = Edge {
        task: owner.id(),
        task_owner: owner.owner().clone(),
        child: c,
        mode: Mode::Foreground,
    };
    let prepared = EdgeStore::new(&store).prepare_create(&edge).unwrap();
    store.commit(&prepared.checks, &prepared.mutations).unwrap();
    // A successful terminal owner remains a valid anchor for later work.
    // An unresolved missing definition is intentionally fenced at recovery.
    let tasks = TaskStore::new(&store);
    let snapshot = tasks.load(owner.id()).unwrap().unwrap();
    tasks.update(&snapshot, |task| task.start()).unwrap();
    let snapshot = tasks.load(owner.id()).unwrap().unwrap();
    tasks
        .update(&snapshot, |task| task.succeed(json!(null)))
        .unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    // A different persisted attempt must invalidate the callback and all of its writes.
    let mut parent_key = b"durable/task/".to_vec();
    parent_key.extend_from_slice(parent.id().as_bytes());
    let original = store.get(&parent_key).unwrap().unwrap();
    let stale = h.session_transaction(c, |tx| {
        tx.create_child_task(&snapshot, child.clone())?;
        tx.put_document(
            &address(),
            json!({"stale": true}).as_object().unwrap().clone(),
        )?;
        let mut value: serde_json::Value = serde_json::from_slice(&original.value)?;
        value["attempt_id"] = json!(Uuid::new_v4());
        store.commit(
            &[durable_store::Check {
                key: parent_key.clone(),
                expected: durable_store::Expected::Version(original.version),
            }],
            &[durable_store::Mutation::Put {
                key: parent_key.clone(),
                value: serde_json::to_vec(&value)?,
            }],
        )?;
        Ok(())
    });
    assert!(stale.is_err());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let changed = store.get(&parent_key).unwrap().unwrap();
    store
        .commit(
            &[durable_store::Check {
                key: parent_key.clone(),
                expected: durable_store::Expected::Version(changed.version),
            }],
            &[durable_store::Mutation::Put {
                key: parent_key,
                value: original.value,
            }],
        )
        .unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let result = h.session_transaction(c, |tx| {
        let token = pi_rs_durable::task_store::SignalToken::new("test", 1).unwrap();
        assert_eq!(
            tx.signal(child.id(), &token, "early", json!(1))?,
            pi_rs_durable::task_store::SignalResult::NotFound
        );
        let submission = tx.submit_write(
            None,
            "note".into(),
            None,
            Some(pi_rs_durable::runtime::SubmissionNotify {
                task_id: child.id(),
                key: "done".into(),
            }),
        )?;
        tx.create_child_task(&snapshot, child.clone())?;
        tx.put_document(
            &address(),
            json!({"saved": true}).as_object().unwrap().clone(),
        )?;
        Ok(submission)
    });
    let submission = result.unwrap();
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_some());
    let notification = TaskStore::new(&store)
        .read_typed_signal(
            child.id(),
            &pi_rs_durable::runtime::submission_settled_signal(),
            "done",
        )
        .unwrap()
        .unwrap();
    assert_eq!(notification["id"], json!(submission));
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let reopened = Harness::open_paused(&path, options()).unwrap();
    assert!(reopened.conversation(c).is_ok());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_some());
}

#[test]
fn multiple_children_same_attempt_commit_with_notification() {
    use pi_rs_durable::{
        runtime::{SubmissionNotify, submission_settled_signal},
        task_store::{SignalResult, SignalToken},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let first = task(TaskOwner::Task(parent.id()), "child", 1);
    let second = task(TaskOwner::Task(parent.id()), "child", 2);
    let third = task(TaskOwner::Task(parent.id()), "child", 3);
    let ids = (first.id(), second.id(), third.id());
    let token = SignalToken::new("test", 1).unwrap();
    let write = h
        .session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(ids.0, &token, "early", json!(1))?,
                SignalResult::NotFound
            );
            let write = tx.submit_write(
                None,
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: ids.1,
                    key: "done".into(),
                }),
            )?;
            assert_eq!(tx.create_keyed_child(&snapshot, "first", first)?, ids.0);
            assert_eq!(tx.create_keyed_child(&snapshot, "second", second)?, ids.1);
            assert_eq!(tx.create_child_task(&snapshot, third)?, ids.2);
            let duplicate = task(TaskOwner::Task(parent.id()), "child", 1);
            assert_eq!(tx.create_keyed_child(&snapshot, "first", duplicate)?, ids.0);
            tx.put_document(&address(), json!({"n":8}).as_object().unwrap().clone())?;
            tx.append_entry("note", None)?;
            Ok(write)
        })
        .unwrap();
    for id in [ids.0, ids.1, ids.2] {
        assert!(TaskStore::new(&store).load(id).unwrap().is_some());
    }
    let signal = TaskStore::new(&store)
        .read_typed_signal(ids.1, &submission_settled_signal(), "done")
        .unwrap()
        .unwrap();
    assert_eq!(signal["id"], json!(write));
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn child_registration_policy_is_checked_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let bad = [
        task(TaskOwner::Task(parent.id()), "missing", 1),
        task(TaskOwner::Task(parent.id()), "child", 1).registered("child".into(), false),
        task(TaskOwner::Task(parent.id()), "child", 1)
            .registered_versioned("child".into(), true, 2, json!({"n":1}))
            .unwrap(),
    ];
    for child in bad {
        let result = h.session_transaction(c, |tx| {
            tx.create_child_task(&snapshot, child.clone())?;
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            Ok(())
        });
        assert!(result.is_err());
        assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    }
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn signal_deadline_conflict_rolls_back_child() {
    use pi_rs_durable::{
        task::WaitFor,
        task_store::{Save, SignalResult, SignalToken},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    // Keep the scheduler from expiring the key on its own wall clock,
    // while the transaction still observes a short monotonic deadline.
    h.set_signal_clock(Arc::new(|| i64::MAX - 100));
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    let snapshot = tasks.load(parent.id()).unwrap().unwrap();
    let other = task(TaskOwner::Conversation(c), "unregistered", 0);
    tasks.save(&[Save::Create(&other)]).unwrap();
    tasks
        .reserve(&tasks.load(other.id()).unwrap().unwrap())
        .unwrap();
    let token = SignalToken::new("deadline", 1).unwrap();
    tasks
        .update(&tasks.load(other.id()).unwrap().unwrap(), |task| {
            task.wait(WaitFor::TypedSleep {
                until_millis: Some(i64::MAX),
                signals: vec![token.key("a").unwrap()],
            })
        })
        .unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let result = h.session_transaction(c, |tx| {
        assert_eq!(
            tx.signal(other.id(), &token, "a", json!(1))?,
            SignalResult::Delivered
        );
        tx.create_child_task(&snapshot, child.clone())?;
        tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
        std::thread::sleep(Duration::from_millis(150));
        Ok(())
    });
    assert!(result.is_err());
    assert!(tasks.load(child.id()).unwrap().is_none());
    assert!(
        tasks
            .read_typed_signal(other.id(), &token, "a")
            .unwrap()
            .is_none()
    );
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn nested_task_parent_admits_child_with_ancestry_checks() {
    use pi_rs_durable::task::WaitFor;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let root = task(TaskOwner::Conversation(c), "root", 0);
    let parent = task(TaskOwner::Task(root.id()), "parent", 0);
    let parent_id = parent.id();
    let parent_for_handler = parent.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Arc::new(Mutex::new(release_rx));
    let mut definitions = Registry::default();
    definitions.register("root", true, move |task: &Task| {
        Ok(Step::Wait {
            checkpoint: task.checkpoint().clone(),
            condition: WaitFor::Tasks(vec![parent_id]),
            children: vec![parent_for_handler.clone()],
        })
    });
    definitions.register("parent", true, move |_: &Task| {
        started_tx.send(()).unwrap();
        release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    });
    definitions.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let driver = h.start_tasks(definitions).unwrap();
    driver.submit(&root).unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent_id).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent_id), "child", 1);
    let id = child.id();
    h.session_transaction(c, |tx| {
        tx.create_keyed_child(&snapshot, "nested", child)?;
        tx.put_document(&address(), json!({"n":9}).as_object().unwrap().clone())?;
        Ok(())
    })
    .unwrap();
    assert!(TaskStore::new(&store).load(id).unwrap().is_some());
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn non_replay_safe_parent_keeps_result_after_external_child_admission() {
    use pi_rs_durable::task::{TaskState, WaitFor};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Arc::new(Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("parent", false, move |_: &Task| {
        started_tx.send(()).unwrap();
        release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!("result")))
    });
    registry.register("child", true, |task: &Task| {
        Ok(Step::Wait {
            checkpoint: task.checkpoint().clone(),
            condition: WaitFor::DeadlineMillis(i64::MAX),
            children: vec![],
        })
    });
    let driver = h.start_tasks(registry).unwrap();
    let parent = task(TaskOwner::Conversation(c), "parent", 0).registered("parent".into(), false);
    driver.submit(&parent).unwrap();
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    h.session_transaction(c, |tx| tx.create_child_task(&snapshot, child))
        .unwrap();
    release_tx.send(()).unwrap();
    let limit = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let state = TaskStore::new(&store)
            .load(parent.id())
            .unwrap()
            .unwrap()
            .task
            .state()
            .clone();
        if state != TaskState::Running {
            assert!(matches!(state, TaskState::Completing(_)), "{state:?}");
            break;
        }
        assert!(std::time::Instant::now() < limit);
        std::thread::sleep(Duration::from_millis(2));
    }
    driver.shutdown().unwrap();
}

#[test]
fn exhausted_ready_child_is_rejected_for_keyed_and_unkeyed() {
    use pi_rs_durable::task::TransitionError;
    for keyed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let (driver, started, release) = driver(&h);
        let parent = task(TaskOwner::Conversation(c), "parent", 0);
        driver.submit(&parent).unwrap();
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let store = Store::open(&path).unwrap();
        let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
        let mut child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            1,
        )
        .unwrap()
        .registered_versioned("child".into(), true, 1, json!({"n":1}))
        .unwrap();
        child.start().unwrap();
        child.recover().unwrap();
        assert_eq!(child.start(), Err(TransitionError::RestartLimit));
        let result = h.session_transaction(c, |tx| {
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            if keyed {
                tx.create_keyed_child(&snapshot, "exhausted", child.clone())?;
            } else {
                tx.create_child_task(&snapshot, child.clone())?;
            }
            Ok(())
        });
        assert!(result.is_err());
        assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        release.send(()).unwrap();
        driver.shutdown().unwrap();
    }
}

#[test]
fn missing_signal_fences_create_delete_aba_without_blocking_unrelated_puts() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::task_store::{Save, SignalResult, SignalToken};
    for keyed in [false, true] {
        for target_aba in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut h = Harness::open_paused(&path, options()).unwrap();
            let c = h.create_conversation(config()).unwrap();
            let (driver, started, release) = driver(&h);
            let parent = task(TaskOwner::Conversation(c), "parent", 0);
            driver.submit(&parent).unwrap();
            started.recv_timeout(Duration::from_secs(10)).unwrap();
            let store = Store::open(&path).unwrap();
            let tasks = TaskStore::new(&store);
            let snapshot = tasks.load(parent.id()).unwrap().unwrap();
            let child = task(TaskOwner::Task(parent.id()), "child", 1);
            let intervening = if target_aba {
                child.clone()
            } else {
                task(TaskOwner::Task(parent.id()), "child", 9)
            };
            let token = SignalToken::new("aba", 1).unwrap();
            let result = h.session_transaction(c, |tx| {
                assert_eq!(
                    tx.signal(child.id(), &token, "x", json!(1))?,
                    SignalResult::NotFound
                );
                tasks.save(&[Save::Create(&intervening)])?;
                assert_eq!(
                    tasks.deliver_signal_at(intervening.id(), &token, "intruder", json!(9), 0)?,
                    SignalResult::Delivered
                );
                let mut key = b"durable/task/".to_vec();
                key.extend_from_slice(intervening.id().as_bytes());
                let version = store.get(&key)?.unwrap().version;
                store.commit(
                    &[Check {
                        key: key.clone(),
                        expected: Expected::Version(version),
                    }],
                    &[Mutation::Delete { key }],
                )?;
                if keyed {
                    tx.create_keyed_child(&snapshot, "aba", child.clone())?;
                } else {
                    tx.create_child_task(&snapshot, child.clone())?;
                }
                tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
                tx.append_entry("note", None)?;
                Ok(())
            });
            if target_aba {
                assert!(matches!(
                    result,
                    Err(pi_rs_durable::runtime::Error::Storage(
                        durable_store::Error::Conflict { .. }
                    ))
                ));
                assert!(tasks.load(child.id()).unwrap().is_none());
                assert!(Documents::new(&store).get(&address()).unwrap().is_none());
                assert!(h.conversation(c).unwrap().application_writes.is_empty());
            } else {
                result.unwrap();
                assert!(tasks.load(child.id()).unwrap().is_some());
                assert_eq!(
                    Documents::new(&store)
                        .get(&address())
                        .unwrap()
                        .unwrap()
                        .value["n"],
                    1
                );
            }
            release.send(()).unwrap();
            driver.shutdown().unwrap();
        }
    }
}

#[test]
fn scoped_parent_read_and_signal_compose_with_child_and_entry_preview() {
    use pi_rs_durable::task_store::{SignalResult, SignalToken};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let child_id = child.id();
    let token = SignalToken::new("parent-signal", 1).unwrap();
    h.session_transaction(c, |tx| {
        let snapshot = tx.task(parent.id())?.unwrap();
        assert_eq!(
            tx.signal(parent.id(), &token, "a", json!(7))?,
            SignalResult::Delivered
        );
        assert!(matches!(tx.task(parent.id()), Err(DError::ReadAfterWrite)));
        tx.create_keyed_child(&snapshot, "child", child)?;
        let entry = tx.append_entry("note", None)?;
        assert_eq!(tx.entries().last().unwrap().id, entry);
        assert!(matches!(tx.checked_entries(), Err(DError::ReadAfterWrite)));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(parent.id(), &token, "a")
            .unwrap(),
        Some(json!(7))
    );
    assert!(TaskStore::new(&store).load(child_id).unwrap().is_some());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn scoped_missing_child_observation_retains_aba_fence() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::task_store::{Save, SignalResult, SignalToken};
    for keyed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let (driver, started, release) = driver(&h);
        let parent = task(TaskOwner::Conversation(c), "parent", 0);
        driver.submit(&parent).unwrap();
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let store = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&store);
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        let child = task(TaskOwner::Task(parent.id()), "child", 1);
        let token = SignalToken::new("scoped-aba", 1).unwrap();
        let result = h.session_transaction(c, |tx| {
            assert!(tx.task(child.id())?.is_none());
            tasks.save(&[Save::Create(&child)])?;
            assert_eq!(
                tasks.deliver_signal_at(child.id(), &token, "x", json!(1), 0)?,
                SignalResult::Delivered
            );
            let mut key = b"durable/task/".to_vec();
            key.extend_from_slice(child.id().as_bytes());
            let version = store.get(&key)?.unwrap().version;
            store.commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(version),
                }],
                &[Mutation::Delete { key }],
            )?;
            if keyed {
                tx.create_keyed_child(&snapshot, "scoped", child.clone())?;
            } else {
                tx.create_child_task(&snapshot, child.clone())?;
            }
            tx.put_document(&address(), json!({"n":1}).as_object().unwrap().clone())?;
            Ok(())
        });
        assert!(matches!(
            result,
            Err(pi_rs_durable::runtime::Error::Storage(
                durable_store::Error::Conflict { .. }
            ))
        ));
        assert!(tasks.load(child.id()).unwrap().is_none());
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        release.send(()).unwrap();
        driver.shutdown().unwrap();
    }
}

#[test]
fn root_duplicate_error_survives_child_port() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let root = task(TaskOwner::Conversation(c), "child", 1);
    let result: Result<(), _> = h.session_transaction(c, |tx| {
        tx.create_task(root.clone())?;
        assert!(matches!(
            tx.create_task(root.clone()),
            Err(DError::DuplicateTask)
        ));
        Err(DError::InvalidSessionEntry)
    });
    assert!(result.is_err());
    assert!(
        TaskStore::new(&Store::open(&path).unwrap())
            .load(root.id())
            .unwrap()
            .is_none()
    );
}

#[test]
fn forged_internal_child_fields_are_rejected_without_losing_scope_rules() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    for field in [
        "task_tool",
        "attempt_id",
        "retired_attempt_id",
        "delivered_signal",
        "ended_children",
        "wake_children",
    ] {
        let original = task(TaskOwner::Task(parent.id()), "child", 1);
        let mut value = serde_json::to_value(original).unwrap();
        value[field] = match field {
            "task_tool" => json!(true),
            "ended_children" | "wake_children" => json!([]),
            _ => json!(Uuid::new_v4()),
        };
        let forged: Task = serde_json::from_value(value).unwrap();
        let result = h.session_transaction(c, |tx| tx.create_child_task(&snapshot, forged.clone()));
        assert!(
            matches!(
                result,
                Err(pi_rs_durable::runtime::Error::Document(
                    DError::InvalidSessionAdmission(_)
                ))
            ),
            "{field}"
        );
        assert!(TaskStore::new(&store).load(forged.id()).unwrap().is_none());
    }
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn staged_roots_and_children_share_graph_and_keys_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, _started, release) = driver(&h);
    let root_a = task(TaskOwner::Conversation(c), "child", 1);
    let root_b = task(TaskOwner::Conversation(c), "child", 2);
    let child_a = task(TaskOwner::Task(root_a.id()), "child", 3);
    let child_b = task(TaskOwner::Task(root_b.id()), "child", 4);
    let grandchild = task(TaskOwner::Task(child_a.id()), "child", 5);
    use pi_rs_durable::{
        runtime::{SubmissionNotify, submission_settled_signal},
        task_store::{SignalResult, SignalToken},
    };
    let receipt = h
        .session_transaction(c, |tx| {
            assert_eq!(
                tx.signal(
                    child_a.id(),
                    &SignalToken::new("test", 1).unwrap(),
                    "early",
                    json!(1)
                )?,
                SignalResult::NotFound
            );
            let receipt = tx.submit_write(
                None,
                "note".into(),
                None,
                Some(SubmissionNotify {
                    task_id: child_a.id(),
                    key: "done".into(),
                }),
            )?;
            tx.put_document(&address(), json!({"n": 42}).as_object().unwrap().clone())?;
            tx.create_task(root_a.clone())?;
            tx.create_task(root_b.clone())?;
            assert_eq!(
                tx.create_keyed_child_of_staged_task(root_a.id(), "key", child_a.clone())?,
                child_a.id()
            );
            assert_eq!(
                tx.create_keyed_child_of_staged_task(
                    root_a.id(),
                    "key",
                    task(TaskOwner::Task(root_a.id()), "child", 3)
                )?,
                child_a.id()
            );
            let mut invalid = serde_json::to_value(task(TaskOwner::Task(root_a.id()), "child", 3))?;
            invalid["id"] = json!(root_a.id());
            let invalid: Task = serde_json::from_value(invalid)?;
            assert!(
                tx.create_keyed_child_of_staged_task(root_a.id(), "key", invalid)
                    .is_err()
            );
            tx.create_keyed_child_of_staged_task(root_b.id(), "key", child_b.clone())?;
            tx.create_child_of_staged_task(child_a.id(), grandchild.clone())?;
            tx.append_entry("note", None)?;
            Ok(receipt)
        })
        .unwrap();
    release.send(()).unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let store = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(child_a.id(), &submission_settled_signal(), "done")
            .unwrap()
            .unwrap()["id"],
        json!(receipt)
    );
    for item in [&root_a, &root_b, &child_a, &child_b, &grandchild] {
        assert!(TaskStore::new(&store).load(item.id()).unwrap().is_some());
    }
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        42
    );
    assert_eq!(h.conversation(c).unwrap().application_writes.len(), 2);
}

#[test]
fn existing_attempt_and_new_root_commit_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let snapshot = TaskStore::new(&store).load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let root = task(TaskOwner::Conversation(c), "child", 2);
    let nested = task(TaskOwner::Task(root.id()), "child", 3);
    h.session_transaction(c, |tx| {
        tx.create_child_task(&snapshot, child.clone())?;
        tx.create_task(root.clone())?;
        tx.create_child_of_staged_task(root.id(), nested.clone())?;
        Ok(())
    })
    .unwrap();
    for item in [&child, &root, &nested] {
        assert!(TaskStore::new(&store).load(item.id()).unwrap().is_some());
    }
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn two_parent_attempts_use_separate_key_spaces_and_fence_both() {
    use pi_rs_durable::task_store::Save;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let first = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&first).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    // The worker is held in the first handler. Construct a second persisted
    // attempt to exercise the transaction's independent parent fences.
    let mut second = task(TaskOwner::Conversation(c), "parent", 1);
    second.start().unwrap();
    let mut value = serde_json::to_value(second).unwrap();
    value["attempt_id"] = json!(Uuid::new_v4());
    let second: Task = serde_json::from_value(value).unwrap();
    TaskStore::new(&store)
        .save(&[Save::Create(&second)])
        .unwrap();
    let first_snapshot = TaskStore::new(&store).load(first.id()).unwrap().unwrap();
    let second_snapshot = TaskStore::new(&store).load(second.id()).unwrap().unwrap();
    let a = task(TaskOwner::Task(first.id()), "child", 1);
    let b = task(TaskOwner::Task(second.id()), "child", 2);
    h.session_transaction(c, |tx| {
        tx.create_keyed_child(&first_snapshot, "key", a.clone())?;
        tx.create_keyed_child(&second_snapshot, "key", b.clone())?;
        Ok(())
    })
    .unwrap();
    assert!(TaskStore::new(&store).load(a.id()).unwrap().is_some());
    assert!(TaskStore::new(&store).load(b.id()).unwrap().is_some());
    let first_current = TaskStore::new(&store).load(first.id()).unwrap().unwrap();
    let second_current = TaskStore::new(&store).load(second.id()).unwrap().unwrap();
    let a2 = task(TaskOwner::Task(first.id()), "child", 3);
    let b2 = task(TaskOwner::Task(second.id()), "child", 4);
    let result = h.session_transaction(c, |tx| {
        tx.create_child_task(&first_current, a2.clone())?;
        tx.create_child_task(&second_current, b2.clone())?;
        tx.put_document(&address(), json!({"n": 7}).as_object().unwrap().clone())?;
        let mut changed = second_current.clone();
        let mut value = serde_json::to_value(&changed.task)?;
        value["abort_requested"] = json!(true);
        changed.task = serde_json::from_value(value)?;
        TaskStore::new(&store).save(&[Save::Update(&changed)])?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(a2.id()).unwrap().is_none());
    assert!(TaskStore::new(&store).load(b2.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn staged_graph_with_orphan_waiting_on_new_parent_is_rejected_atomically() {
    use pi_rs_durable::{task::WaitFor, task_store::Save};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let blocker = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&blocker).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let root = task(TaskOwner::Conversation(c), "child", 1);
    let child = task(TaskOwner::Task(root.id()), "child", 2);
    let mut orphan = task(TaskOwner::Task(root.id()), "child", 3);
    orphan.start().unwrap();
    orphan.wait(WaitFor::Tasks(vec![root.id()])).unwrap();
    let store = Store::open(&path).unwrap();
    TaskStore::new(&store)
        .save(&[Save::Create(&orphan)])
        .unwrap();
    let result = h.session_transaction(c, |tx| {
        tx.create_task(root.clone())?;
        tx.create_child_of_staged_task(root.id(), child.clone())?;
        tx.put_document(&address(), json!({"n": 1}).as_object().unwrap().clone())?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(root.id()).unwrap().is_none());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    // Remove the deliberately corrupt orphan before allowing the worker to resume.
    let mut key = b"durable/task/".to_vec();
    key.extend_from_slice(orphan.id().as_bytes());
    let entry = store.get(&key).unwrap().unwrap();
    store
        .commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: durable_store::Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Delete { key }],
        )
        .unwrap();
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn staged_child_signal_missing_observation_rejects_create_delete_aba() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::task_store::{Save, SignalResult, SignalToken};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let blocker = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&blocker).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let root = task(TaskOwner::Conversation(c), "child", 1);
    let child = task(TaskOwner::Task(root.id()), "child", 2);
    let result = h.session_transaction(c, |tx| {
        assert_eq!(
            tx.signal(
                child.id(),
                &SignalToken::new("aba", 1).unwrap(),
                "x",
                json!(1)
            )?,
            SignalResult::NotFound
        );
        TaskStore::new(&store).save(&[Save::Create(&child)])?;
        let mut key = b"durable/task/".to_vec();
        key.extend_from_slice(child.id().as_bytes());
        let version = store.get(&key)?.unwrap().version;
        store.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(version),
            }],
            &[Mutation::Delete { key }],
        )?;
        tx.create_task(root.clone())?;
        tx.create_keyed_child_of_staged_task(root.id(), "key", child.clone())?;
        tx.put_document(&address(), json!({"n": 1}).as_object().unwrap().clone())?;
        Ok(())
    });
    assert!(result.is_err());
    assert!(TaskStore::new(&store).load(root.id()).unwrap().is_none());
    assert!(TaskStore::new(&store).load(child.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn owned_conversation_admission_observes_failure_abort_and_background_boundary() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore, Mode},
        task_store::Save,
    };
    for (mode, failed, succeeds) in [
        (Mode::Foreground, true, false),
        (Mode::Foreground, false, false),
        (Mode::Background, true, true),
        (Mode::Background, false, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let root = h.create_conversation(config()).unwrap();
        let c = h.create_conversation(config()).unwrap();
        let store = Store::open(&path).unwrap();
        let owner = task(TaskOwner::Conversation(root), "unknown", 0);
        let ts = TaskStore::new(&store);
        ts.save(&[Save::Create(&owner)]).unwrap();
        let edge = Edge {
            task: owner.id(),
            task_owner: owner.owner().clone(),
            child: c,
            mode,
        };
        let prepared = EdgeStore::new(&store).prepare_create(&edge).unwrap();
        store.commit(&prepared.checks, &prepared.mutations).unwrap();
        let snapshot = ts.load(owner.id()).unwrap().unwrap();
        ts.update(&snapshot, |task| task.start()).unwrap();
        let snapshot = ts.load(owner.id()).unwrap().unwrap();
        ts.update(&snapshot, |task| task.succeed(json!(null)))
            .unwrap();
        let (driver, started, release) = driver(&h);
        let parent = task(TaskOwner::Conversation(c), "parent", 0);
        driver.submit(&parent).unwrap();
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let snapshot = ts.load(parent.id()).unwrap().unwrap();
        let child = task(TaskOwner::Task(parent.id()), "child", 1);
        let mut owner_key = b"durable/task/".to_vec();
        owner_key.extend_from_slice(owner.id().as_bytes());
        let original = store.get(&owner_key).unwrap().unwrap();
        let result = h.session_transaction(c, |tx| {
            let token = pi_rs_durable::task_store::SignalToken::new("test", 1).unwrap();
            assert_eq!(
                tx.signal(child.id(), &token, "early", json!(1))?,
                pi_rs_durable::task_store::SignalResult::NotFound
            );
            tx.submit_write(
                None,
                "notice".into(),
                None,
                Some(pi_rs_durable::runtime::SubmissionNotify {
                    task_id: child.id(),
                    key: "done".into(),
                }),
            )?;
            tx.create_keyed_child(&snapshot, "child", child.clone())?;
            tx.put_document(&address(), json!({"value": 1}).as_object().unwrap().clone())?;
            tx.append_entry("note", None)?;
            let value = if failed {
                let mut failed_owner = owner.clone();
                failed_owner.start().unwrap();
                failed_owner.fail("failed".into()).unwrap();
                serde_json::to_vec(&failed_owner)?
            } else {
                let mut value: serde_json::Value = serde_json::from_slice(&original.value)?;
                value["abort_requested"] = json!(true);
                serde_json::to_vec(&value)?
            };
            store.commit(
                &[Check {
                    key: owner_key.clone(),
                    expected: Expected::Version(original.version),
                }],
                &[Mutation::Put {
                    key: owner_key.clone(),
                    value,
                }],
            )?;
            Ok(())
        });
        assert_eq!(
            result.is_ok(),
            succeeds,
            "{mode:?}, failed={failed}: {result:?}"
        );
        assert_eq!(ts.load(child.id()).unwrap().is_some(), succeeds);
        assert_eq!(
            ts.read_typed_signal(
                child.id(),
                &pi_rs_durable::runtime::submission_settled_signal(),
                "done"
            )
            .unwrap()
            .is_some(),
            succeeds
        );
        assert_eq!(
            Documents::new(&store).get(&address()).unwrap().is_some(),
            succeeds
        );
        assert_eq!(
            !h.conversation(c).unwrap().application_writes.is_empty(),
            succeeds
        );
        // Restore the anchor so the driver can finish without an unrelated cleanup race.
        let current = store.get(&owner_key).unwrap().unwrap();
        store
            .commit(
                &[Check {
                    key: owner_key.clone(),
                    expected: Expected::Version(current.version),
                }],
                &[Mutation::Put {
                    key: owner_key,
                    value: original.value,
                }],
            )
            .unwrap();
        release.send(()).unwrap();
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn async_owned_conversation_transaction_commits_child_and_document() {
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore, Mode},
        task_store::Save,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let root = h.create_conversation(config()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let owner = task(TaskOwner::Conversation(root), "unknown", 0);
    ts.save(&[Save::Create(&owner)]).unwrap();
    let edge = Edge {
        task: owner.id(),
        task_owner: owner.owner().clone(),
        child: c,
        mode: Mode::Foreground,
    };
    let prepared = EdgeStore::new(&store).prepare_create(&edge).unwrap();
    store.commit(&prepared.checks, &prepared.mutations).unwrap();
    let snapshot = ts.load(owner.id()).unwrap().unwrap();
    ts.update(&snapshot, |task| task.start()).unwrap();
    let snapshot = ts.load(owner.id()).unwrap().unwrap();
    ts.update(&snapshot, |task| task.succeed(json!(null)))
        .unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let snapshot = ts.load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let child_id = child.id();
    h.session_transaction_async(c, move |tx| {
        Box::pin(async move {
            tx.create_child_task(&snapshot, child)?;
            tokio::task::yield_now().await;
            tx.put_document(
                &address(),
                json!({"async": true}).as_object().unwrap().clone(),
            )?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert!(ts.load(child_id).unwrap().is_some());
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}

#[test]
fn succeeded_multilevel_owners_allow_admission_after_reopen() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore, Mode},
        task_store::Save,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let root = h.create_conversation(config()).unwrap();
    let conversation = h.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    let ancestor = task(TaskOwner::Conversation(root), "unknown", 0);
    let owner = task(TaskOwner::Task(ancestor.id()), "unknown", 1);
    tasks
        .save(&[Save::Create(&ancestor), Save::Create(&owner)])
        .unwrap();
    let edge = Edge {
        task: owner.id(),
        task_owner: owner.owner().clone(),
        child: conversation,
        mode: Mode::Foreground,
    };
    let prepared = EdgeStore::new(&store).prepare_create(&edge).unwrap();
    store.commit(&prepared.checks, &prepared.mutations).unwrap();
    for id in [owner.id(), ancestor.id()] {
        let snapshot = tasks.load(id).unwrap().unwrap();
        tasks.update(&snapshot, |task| task.start()).unwrap();
        let snapshot = tasks.load(id).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.succeed(json!(null)))
            .unwrap();
    }
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(conversation), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let snapshot = tasks.load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let mut ancestor_key = b"durable/task/".to_vec();
    ancestor_key.extend_from_slice(ancestor.id().as_bytes());
    for mode in 0..3 {
        let original = store.get(&ancestor_key).unwrap().unwrap();
        let result = h.session_transaction(conversation, |tx| {
            tx.create_child_task(&snapshot, child.clone())?;
            tx.put_document(
                &address(),
                json!({"staged": true}).as_object().unwrap().clone(),
            )?;
            let value = match mode {
                0 => {
                    let mut bad = ancestor.clone();
                    bad.start().unwrap();
                    bad.fail("failure".into()).unwrap();
                    serde_json::to_vec(&bad)?
                }
                1 => {
                    let mut bad = ancestor.clone();
                    bad.cancel().unwrap();
                    serde_json::to_vec(&bad)?
                }
                _ => {
                    let mut value: serde_json::Value = serde_json::from_slice(&original.value)?;
                    value["abort_requested"] = json!(true);
                    serde_json::to_vec(&value)?
                }
            };
            store.commit(
                &[Check {
                    key: ancestor_key.clone(),
                    expected: Expected::Version(original.version),
                }],
                &[Mutation::Put {
                    key: ancestor_key.clone(),
                    value,
                }],
            )?;
            Ok(())
        });
        assert!(result.is_err());
        assert!(tasks.load(child.id()).unwrap().is_none());
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        let current = store.get(&ancestor_key).unwrap().unwrap();
        store
            .commit(
                &[Check {
                    key: ancestor_key.clone(),
                    expected: Expected::Version(current.version),
                }],
                &[Mutation::Put {
                    key: ancestor_key.clone(),
                    value: original.value,
                }],
            )
            .unwrap();
    }
    h.session_transaction(conversation, |tx| {
        tx.create_child_task(&snapshot, child.clone())?;
        tx.put_document(
            &address(),
            json!({"saved": true}).as_object().unwrap().clone(),
        )?;
        Ok(())
    })
    .unwrap();
    assert!(tasks.load(child.id()).unwrap().is_some());
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    release.send(()).unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let reopened = Harness::open_paused(&path, options()).unwrap();
    assert!(reopened.conversation(conversation).is_ok());
    assert!(tasks.load(child.id()).unwrap().is_some());
}

#[test]
fn duplicate_key_child_version_change_requires_fresh_transaction() {
    use durable_store::{Check, Expected, Mutation};
    use pi_rs_durable::runtime::Error;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.create_conversation(config()).unwrap();
    let (driver, started, release) = driver(&h);
    let parent = task(TaskOwner::Conversation(c), "parent", 0);
    driver.submit(&parent).unwrap();
    started.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    let snapshot = tasks.load(parent.id()).unwrap().unwrap();
    let child = task(TaskOwner::Task(parent.id()), "child", 1);
    let id = h
        .session_transaction(c, |tx| {
            tx.create_keyed_child(&snapshot, "restart", child.clone())
        })
        .unwrap();
    let snapshot = tasks.load(parent.id()).unwrap().unwrap();
    let candidate = task(TaskOwner::Task(parent.id()), "child", 1);
    let mut key = b"durable/task/".to_vec();
    key.extend_from_slice(id.as_bytes());
    let result = h.session_transaction(c, |tx| {
        assert_eq!(
            tx.create_keyed_child(&snapshot, "restart", candidate.clone())?,
            id
        );
        let old = store.get(&key)?.unwrap();
        store.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(old.version),
            }],
            &[Mutation::Put {
                key: key.clone(),
                value: old.value,
            }],
        )?;
        Ok(())
    });
    assert!(
        matches!(result, Err(Error::Storage(durable_store::Error::Conflict { key: ref conflict })) if *conflict == key)
    );
    let fresh = tasks.load(parent.id()).unwrap().unwrap();
    assert_eq!(attempt_id(&fresh.task), attempt_id(&snapshot.task));
    assert_eq!(
        h.session_transaction(c, |tx| tx.create_keyed_child(&fresh, "restart", candidate))
            .unwrap(),
        id
    );
    release.send(()).unwrap();
    driver.shutdown().unwrap();
}
