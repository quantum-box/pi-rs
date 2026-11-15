use durable_store::{Mutation, Store};
use pi_rs_durable::{
    task::{
        Checkpoint, Task, TaskOwner, TaskState, TypedWake, WaitFor,
        scheduler::{Error as SchedulerError, Registry, Scheduler, Step},
    },
    task_store::{
        Error as StoreError, Save, Signal, SignalResult, SignalState, SignalToken, TaskStore,
    },
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

fn token() -> SignalToken {
    SignalToken::new("answer", 1).unwrap()
}
fn setup(store: &Store, typed_signal: bool, deadline: i64) -> Uuid {
    let id = Uuid::from_u128(71);
    let task = Task::new(
        id,
        TaskOwner::Conversation(Uuid::from_u128(72)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("wake-test".into(), true);
    let ts = TaskStore::new(store);
    ts.save(&[Save::Create(&task)]).unwrap();
    ts.reserve(&ts.load(id).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(id).unwrap().unwrap(), |task| {
        task.wait(if typed_signal {
            WaitFor::TypedSignal(token().key("a").unwrap())
        } else {
            WaitFor::TypedSleep {
                until_millis: Some(deadline),
                signals: vec![token().key("a").unwrap(), token().key("b").unwrap()],
            }
        })
    })
    .unwrap();
    id
}
fn registry(seen: Arc<Mutex<Vec<Option<TypedWake>>>>) -> Registry {
    let mut registry = Registry::default();
    registry.register("wake-test", true, move |task: &Task| {
        seen.lock().unwrap().push(task.typed_wake().cloned());
        Ok(Step::Succeed(json!("done")))
    });
    registry
}
fn receipt_key(task: Uuid, id: Uuid) -> Vec<u8> {
    let mut key = b"durable/signal/".to_vec();
    key.extend_from_slice(task.as_bytes());
    key.extend_from_slice(id.as_bytes());
    key
}
fn revision_key(task: Uuid) -> Vec<u8> {
    let mut key = b"durable/signal-revision/".to_vec();
    key.extend_from_slice(task.as_bytes());
    key
}

#[test]
fn scheduler_fails_closed_on_duplicate_or_mismatched_typed_receipts() {
    for typed_signal in [false, true] {
        for mismatch in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let store = Store::open(&path).unwrap();
            let id = setup(&store, typed_signal, 100);
            let one = Signal::new_typed(Uuid::from_u128(1), id, &token(), "a", json!(1)).unwrap();
            let two = Signal::new_typed(Uuid::from_u128(2), id, &token(), "a", json!(2)).unwrap();
            let mut writes = vec![
                Mutation::Put {
                    key: receipt_key(id, if mismatch { Uuid::from_u128(3) } else { one.id }),
                    value: serde_json::to_vec(&one).unwrap(),
                },
                Mutation::Put {
                    key: revision_key(id),
                    value: vec![],
                },
            ];
            if !mismatch {
                writes.push(Mutation::Put {
                    key: receipt_key(id, two.id),
                    value: serde_json::to_vec(&two).unwrap(),
                });
            }
            store.commit(&[], &writes).unwrap();
            let before = TaskStore::new(&store).load(id).unwrap().unwrap();
            drop(store);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let mut scheduler = Scheduler::open(&path, registry(seen.clone())).unwrap();
            let result = scheduler.tick(100);
            if mismatch {
                assert!(matches!(
                    result,
                    Err(SchedulerError::Store(StoreError::InvalidKey))
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(SchedulerError::Store(StoreError::SignalKeyMismatch))
                ));
            }
            assert!(seen.lock().unwrap().is_empty());
            drop(scheduler);
            let store = Store::open(&path).unwrap();
            let after = TaskStore::new(&store).load(id).unwrap().unwrap();
            assert_eq!(before.version, after.version);
            assert!(matches!(after.task.state(), TaskState::Waiting(_)));
        }
    }
}

#[test]
fn scheduler_consumption_advances_revision_and_preserves_wait_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, false, 100);
    let ts = TaskStore::new(&store);
    for key in ["b", "a"] {
        assert_eq!(
            ts.deliver_signal_at(id, &token(), key, json!(key), 0)
                .unwrap(),
            SignalResult::Delivered
        );
    }
    let old_revision = store.get(&revision_key(id)).unwrap().unwrap().version;
    drop(store);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut scheduler = Scheduler::open(&path, registry(seen.clone())).unwrap();
    assert!(scheduler.tick(100).unwrap());
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Some(TypedWake::Signal(token().key("a").unwrap()))]
    );
    drop(scheduler);
    let store = Store::open(&path).unwrap();
    assert!(store.get(&revision_key(id)).unwrap().unwrap().version > old_revision);
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal_state(id, &token(), "b")
            .unwrap(),
        Some(SignalState::Value(json!("b")))
    );
}

#[test]
fn driver_delivers_deadline_wake_and_reopens() {
    use pi_rs_durable::task_driver::Driver;
    use std::time::{Duration, Instant};
    for early_signal in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup(&store, false, if early_signal { 60_000 } else { 1 });
        if early_signal {
            assert_eq!(
                TaskStore::new(&store)
                    .deliver_signal_at(id, &token(), "a", json!(7), 0)
                    .unwrap(),
                SignalResult::Delivered
            );
        }
        drop(store);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let scheduler = Scheduler::open(&path, registry(seen.clone())).unwrap();
        let driver = Driver::start(scheduler);
        let limit = Instant::now() + Duration::from_secs(10);
        loop {
            if matches!(
                driver.get(id).unwrap().unwrap().state(),
                TaskState::Succeeded(_)
            ) {
                break;
            }
            assert!(Instant::now() < limit);
            std::thread::sleep(Duration::from_millis(10));
        }
        driver.shutdown().unwrap();
        let expected = if early_signal {
            TypedWake::Signal(token().key("a").unwrap())
        } else {
            TypedWake::Expired
        };
        assert_eq!(*seen.lock().unwrap(), vec![Some(expected)]);
        let store = Store::open(&path).unwrap();
        let ts = TaskStore::new(&store);
        assert_eq!(
            ts.read_typed_signal_state(id, &token(), "a").unwrap(),
            Some(if early_signal {
                SignalState::Value(json!(7))
            } else {
                SignalState::Expired
            })
        );
        assert_eq!(
            ts.read_typed_signal_state(id, &token(), "b").unwrap(),
            if early_signal {
                None
            } else {
                Some(SignalState::Expired)
            }
        );
    }
}

#[test]
fn scheduler_deadline_races_external_delivery_atomically() {
    use std::sync::Barrier;
    for _ in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup(&store, false, 100);
        drop(store);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut scheduler = Scheduler::open(&path, registry(seen.clone())).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let other_barrier = barrier.clone();
        let other_path = path.clone();
        let other = std::thread::spawn(move || {
            let store = Store::open(other_path).unwrap();
            other_barrier.wait();
            TaskStore::new(&store)
                .deliver_signal_at(id, &token(), "a", json!(7), 0)
                .unwrap()
        });
        barrier.wait();
        for _ in 0..5 {
            scheduler.tick(100).unwrap();
            if !seen.lock().unwrap().is_empty() {
                break;
            }
        }
        let result = other.join().unwrap();
        for _ in 0..5 {
            if !seen.lock().unwrap().is_empty() {
                break;
            }
            scheduler.tick(100).unwrap();
        }
        drop(scheduler);
        let store = Store::open(&path).unwrap();
        let ts = TaskStore::new(&store);
        match result {
            SignalResult::Delivered => {
                assert_eq!(
                    *seen.lock().unwrap(),
                    vec![Some(TypedWake::Signal(token().key("a").unwrap()))]
                );
                assert_eq!(
                    ts.read_typed_signal_state(id, &token(), "a").unwrap(),
                    Some(SignalState::Value(json!(7)))
                );
            }
            SignalResult::Closed => {
                assert_eq!(*seen.lock().unwrap(), vec![Some(TypedWake::Expired)]);
                assert_eq!(
                    ts.read_typed_signal_state(id, &token(), "a").unwrap(),
                    Some(SignalState::Expired)
                );
                assert_eq!(
                    ts.read_typed_signal_state(id, &token(), "b").unwrap(),
                    Some(SignalState::Expired)
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[cfg(unix)]
#[test]
fn scheduler_wake_process_child() {
    use std::io::Write;
    let Some(path) = std::env::var_os("PI_SCHEDULER_WAKE_DB") else {
        return;
    };
    let mut registry = Registry::default();
    registry.register("wake-test", true, |_: &Task| -> Result<Step, String> {
        println!("SCHEDULER_WAKE_RESERVED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    });
    let mut scheduler = Scheduler::open(path, registry).unwrap();
    scheduler.tick(100).unwrap();
}

#[cfg(unix)]
#[test]
fn scheduler_wake_reason_and_receipts_survive_kill_reopen() {
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
    for early in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup(&store, false, 100);
        if early {
            assert_eq!(
                TaskStore::new(&store)
                    .deliver_signal_at(id, &token(), "a", json!(7), 0)
                    .unwrap(),
                SignalResult::Delivered
            );
        }
        drop(store);
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "scheduler_wake_process_child", "--nocapture"])
                .env("PI_SCHEDULER_WAKE_DB", &path)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (send, recv) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("SCHEDULER_WAKE_RESERVED") {
                    let _ = send.send(());
                    break;
                }
            }
        });
        recv.recv_timeout(Duration::from_secs(20))
            .expect("wake not reserved");
        child.0.kill().unwrap();
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut scheduler = Scheduler::open(&path, registry(seen.clone())).unwrap();
        for _ in 0..5 {
            scheduler.tick(100).unwrap();
            if !seen.lock().unwrap().is_empty() {
                break;
            }
        }
        let expected = if early {
            TypedWake::Signal(token().key("a").unwrap())
        } else {
            TypedWake::Expired
        };
        assert_eq!(*seen.lock().unwrap(), vec![Some(expected)]);
        drop(scheduler);
        let store = Store::open(&path).unwrap();
        let ts = TaskStore::new(&store);
        assert_eq!(
            ts.read_typed_signal_state(id, &token(), "a").unwrap(),
            Some(if early {
                SignalState::Value(json!(7))
            } else {
                SignalState::Expired
            })
        );
        if !early {
            assert_eq!(
                ts.read_typed_signal_state(id, &token(), "b").unwrap(),
                Some(SignalState::Expired)
            );
        }
    }
}
