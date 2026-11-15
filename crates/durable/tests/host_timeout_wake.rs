use durable_store::{Expected, Store};
use pi_rs_durable::{
    document::{Address, Documents, HostTransaction},
    task::{Checkpoint, Task, TaskOwner, TaskState, TypedWake, WaitFor},
    task_store::{Save, SignalResult, SignalState, SignalToken, TaskStore},
};
use serde_json::json;
use uuid::Uuid;

fn token() -> SignalToken {
    SignalToken::new("answer", 1).unwrap()
}
fn addr() -> Address {
    Address {
        scope: "host-timeout".into(),
        name: "state".into(),
        key: "".into(),
    }
}
fn setup(store: &Store, keys: &[&str]) -> Uuid {
    setup_at(store, keys, 100)
}
fn setup_at(store: &Store, keys: &[&str], deadline: i64) -> Uuid {
    let task = Task::new(
        Uuid::from_u128(31),
        TaskOwner::Conversation(Uuid::from_u128(32)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("timeout".into(), true);
    let ts = TaskStore::new(store);
    ts.save(&[Save::Create(&task)]).unwrap();
    ts.reserve(&ts.load(task.id()).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(task.id()).unwrap().unwrap(), |task| {
        task.wait(WaitFor::TypedSleep {
            until_millis: Some(deadline),
            signals: keys.iter().map(|key| token().key(*key).unwrap()).collect(),
        })
    })
    .unwrap();
    task.id()
}
fn put(tx: &mut HostTransaction<'_>) {
    tx.put(
        &addr(),
        Expected::Missing,
        json!({"committed":true}).as_object().unwrap(),
    )
    .unwrap();
}
fn signal_state(store: &Store, id: Uuid, key: &str) -> Option<SignalState> {
    TaskStore::new(store)
        .read_typed_signal_state(id, &token(), key)
        .unwrap()
}

#[test]
fn timeout_boundary_marks_missing_keys_and_preserves_staged_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, &["a", "b", "c"]);
    let mut tx = HostTransaction::new(&store);
    put(&mut tx);
    assert!(tx.wake_at(id, 99).is_err());
    assert_eq!(
        tx.signal_at(id, &token(), "b", json!(2), 100).unwrap(),
        SignalResult::Closed
    );
    tx.wake_at(id, 100).unwrap();
    tx.commit().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap().task;
    assert_eq!(task.state(), &TaskState::Ready);
    assert_eq!(task.typed_wake(), Some(&TypedWake::Expired));
    for key in ["a", "b", "c"] {
        assert_eq!(signal_state(&store, id, key), Some(SignalState::Expired));
    }
    let mut prefix = b"durable/signal/".to_vec();
    prefix.extend_from_slice(id.as_bytes());
    assert_eq!(store.list(&prefix).unwrap().len(), 3);
    assert!(Documents::new(&store).get(&addr()).unwrap().is_some());
}

#[test]
fn committed_value_has_priority_at_and_after_deadline() {
    for now in [99, 100, 101] {
        let store = Store::memory().unwrap();
        let id = setup(&store, &["a", "b", "c"]);
        let ts = TaskStore::new(&store);
        assert_eq!(
            ts.deliver_signal_at(id, &token(), "a", json!(7), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        let mut tx = HostTransaction::new(&store);
        assert_eq!(
            tx.signal_at(id, &token(), "b", json!(2), 100).unwrap(),
            SignalResult::Closed
        );
        tx.wake_at(id, now).unwrap();
        tx.commit().unwrap();
        let task = ts.load(id).unwrap().unwrap().task;
        assert_eq!(
            task.typed_wake(),
            Some(&TypedWake::Signal(token().key("a").unwrap()))
        );
        assert!(
            ts.signal(id, task.delivered_signal().unwrap())
                .unwrap()
                .unwrap()
                .is_consumed()
        );
        assert_eq!(
            signal_state(&store, id, "a"),
            Some(SignalState::Value(json!(7)))
        );
        assert_eq!(signal_state(&store, id, "b"), Some(SignalState::Expired));
        assert_eq!(signal_state(&store, id, "c"), None);
    }
}

#[test]
fn time_only_sleep_wakes_at_boundary_without_signal_records() {
    let store = Store::memory().unwrap();
    let id = setup(&store, &[]);
    let mut tx = HostTransaction::new(&store);
    assert!(tx.wake_at(id, 99).is_err());
    tx.wake_at(id, 100).unwrap();
    tx.commit().unwrap();
    let task = TaskStore::new(&store).load(id).unwrap().unwrap().task;
    assert_eq!(task.state(), &TaskState::Ready);
    assert_eq!(task.typed_wake(), Some(&TypedWake::Time));
    let mut prefix = b"durable/signal/".to_vec();
    prefix.extend_from_slice(id.as_bytes());
    assert!(store.list(&prefix).unwrap().is_empty());
}

#[test]
fn timeout_wake_races_delivery_without_partial_effects() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, &["a", "b"]);
    drop(store);
    let barrier = Arc::new(Barrier::new(3));
    let host_path = path.clone();
    let host_barrier = barrier.clone();
    let host = std::thread::spawn(move || {
        let store = Store::open(host_path).unwrap();
        let mut tx = HostTransaction::new(&store);
        put(&mut tx);
        tx.wake_at(id, 100).unwrap();
        host_barrier.wait();
        tx.commit()
    });
    let other_path = path.clone();
    let other_barrier = barrier.clone();
    let other = std::thread::spawn(move || {
        let store = Store::open(other_path).unwrap();
        other_barrier.wait();
        TaskStore::new(&store)
            .deliver_signal_at(id, &token(), "a", json!(7), 0)
            .unwrap()
    });
    barrier.wait();
    let host = host.join().unwrap();
    let other = other.join().unwrap();
    let store = Store::open(&path).unwrap();
    let task = TaskStore::new(&store).load(id).unwrap().unwrap().task;
    if host.is_ok() {
        assert_eq!(other, SignalResult::Closed);
        assert_eq!(task.typed_wake(), Some(&TypedWake::Expired));
        assert_eq!(signal_state(&store, id, "a"), Some(SignalState::Expired));
        assert!(Documents::new(&store).get(&addr()).unwrap().is_some());
    } else {
        assert!(matches!(
            host,
            Err(pi_rs_durable::document::Error::Store(
                durable_store::Error::Conflict { .. }
            ))
        ));
        assert_eq!(other, SignalResult::Delivered);
        assert!(matches!(task.state(), TaskState::Waiting(_)));
        assert_eq!(
            signal_state(&store, id, "a"),
            Some(SignalState::Value(json!(7)))
        );
        assert_eq!(signal_state(&store, id, "b"), None);
        assert!(Documents::new(&store).get(&addr()).unwrap().is_none());
    }
}

#[cfg(unix)]
#[test]
fn host_timeout_process_child() {
    use std::io::Write;
    let Some(path) = std::env::var_os("PI_HOST_TIMEOUT_DB") else {
        return;
    };
    let store = Store::open(path).unwrap();
    let mut tx = HostTransaction::new(&store);
    put(&mut tx);
    tx.wake_at(Uuid::from_u128(31), 100).unwrap();
    if std::env::var_os("PI_HOST_TIMEOUT_COMMIT").is_some() {
        tx.commit().unwrap();
    }
    println!("HOST_TIMEOUT_BOUNDARY_REACHED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn timeout_staging_and_commit_survive_kill_reopen() {
    use std::{
        io::{BufRead, BufReader},
        os::unix::process::ExitStatusExt,
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
    for committed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup(&store, &["a", "b"]);
        drop(store);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "host_timeout_process_child", "--nocapture"])
            .env("PI_HOST_TIMEOUT_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if committed {
            command.env("PI_HOST_TIMEOUT_COMMIT", "1");
        }
        let mut child = KillOnDrop(command.spawn().unwrap());
        let stdout = child.0.stdout.take().unwrap();
        let (send, recv) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("HOST_TIMEOUT_BOUNDARY_REACHED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        recv.recv_timeout(Duration::from_secs(20))
            .expect("timeout boundary not reached");
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        let store = Store::open(&path).unwrap();
        let ts = TaskStore::new(&store);
        let task = ts.load(id).unwrap().unwrap().task;
        if committed {
            assert_eq!(task.state(), &TaskState::Ready);
            assert_eq!(task.typed_wake(), Some(&TypedWake::Expired));
            for key in ["a", "b"] {
                assert_eq!(signal_state(&store, id, key), Some(SignalState::Expired));
            }
            assert!(Documents::new(&store).get(&addr()).unwrap().is_some());
        } else {
            assert!(matches!(task.state(), TaskState::Waiting(_)));
            for key in ["a", "b"] {
                assert_eq!(signal_state(&store, id, key), None);
            }
            assert!(Documents::new(&store).get(&addr()).unwrap().is_none());
            let mut tx = HostTransaction::new(&store);
            put(&mut tx);
            tx.wake_at(id, 100).unwrap();
            tx.commit().unwrap();
        }
    }
}

#[test]
fn staged_value_takes_priority_at_deadline() {
    let store = Store::memory().unwrap();
    let id = setup_at(&store, &["a", "b"], 60_000);
    let mut tx = HostTransaction::new(&store);
    assert_eq!(
        tx.signal_at(id, &token(), "b", json!(8), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(7), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_at(id, 60_000).unwrap();
    tx.commit().unwrap();
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap().task;
    assert_eq!(
        task.typed_wake(),
        Some(&TypedWake::Signal(token().key("a").unwrap()))
    );
    assert_eq!(
        ts.signal(id, task.delivered_signal().unwrap())
            .unwrap()
            .unwrap()
            .payload,
        json!(7)
    );
    assert_eq!(
        signal_state(&store, id, "b"),
        Some(SignalState::Value(json!(8)))
    );
}

#[test]
fn timeout_wake_conflicts_with_concurrent_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, &["a", "b"]);
    let mut tx = HostTransaction::new(&store);
    put(&mut tx);
    tx.wake_at(id, 100).unwrap();
    let other = Store::open(&path).unwrap();
    let ts = TaskStore::new(&other);
    let task = ts.load(id).unwrap().unwrap();
    ts.update(&task, |task| task.cancel()).unwrap();
    assert!(matches!(
        tx.commit(),
        Err(pi_rs_durable::document::Error::Store(
            durable_store::Error::Conflict { .. }
        ))
    ));
    drop(other);
    drop(store);
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&addr()).unwrap().is_none());
    for key in ["a", "b"] {
        assert_eq!(signal_state(&store, id, key), None);
    }
    assert_eq!(
        TaskStore::new(&store)
            .load(id)
            .unwrap()
            .unwrap()
            .task
            .state(),
        &TaskState::Cancelled
    );
}
