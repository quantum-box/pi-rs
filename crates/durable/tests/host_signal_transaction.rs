use durable_store::{Expected, Store};
use pi_rs_durable::{
    document::{Address, Documents, Error, HostTransaction},
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_store::{Save, SignalResult, SignalToken, TaskStore},
};
use serde_json::json;
use uuid::Uuid;

fn address() -> Address {
    Address {
        scope: "host".into(),
        name: "state".into(),
        key: "".into(),
    }
}
fn token() -> SignalToken {
    SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap()
}
fn setup(store: &Store, deadline: Option<i64>) -> Uuid {
    let ts = TaskStore::new(store);
    let task = Task::new(
        Uuid::from_u128(5),
        TaskOwner::Conversation(Uuid::from_u128(9)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    ts.save(&[Save::Create(&task)]).unwrap();
    ts.reserve(&ts.load(task.id()).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(task.id()).unwrap().unwrap(), |task| {
        task.wait(WaitFor::TypedSleep {
            until_millis: deadline,
            signals: vec![token().key("a").unwrap(), token().key("b").unwrap()],
        })
    })
    .unwrap();
    task.id()
}
fn put(tx: &mut HostTransaction<'_>, n: i32) {
    tx.put(
        &address(),
        Expected::Missing,
        json!({"n":n}).as_object().unwrap(),
    )
    .unwrap();
}

#[test]
fn host_signal_and_document_commit_together_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, None);
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(2), 0).unwrap(),
        SignalResult::Duplicate
    );
    tx.commit().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(id, &token(), "a")
            .unwrap(),
        Some(json!(1))
    );
}

#[test]
fn host_signal_conflict_and_abort_roll_back_document() {
    for mode in [0, 1] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup(&store, None);
        let mut tx = HostTransaction::new(&store);
        put(&mut tx, 1);
        assert_eq!(
            tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
            SignalResult::Delivered
        );
        let other = Store::open(&path).unwrap();
        let ts = TaskStore::new(&other);
        if mode == 0 {
            assert_eq!(
                ts.deliver_signal_at(id, &token(), "a", json!(7), 0)
                    .unwrap(),
                SignalResult::Delivered
            );
        } else {
            let task = ts.load(id).unwrap().unwrap();
            ts.update(&task, |task| task.cancel()).unwrap();
        }
        assert!(matches!(
            tx.commit(),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        assert_eq!(
            TaskStore::new(&store)
                .read_typed_signal(id, &token(), "a")
                .unwrap(),
            if mode == 0 { Some(json!(7)) } else { None }
        );
        if mode == 0 {
            let mut retry = HostTransaction::new(&store);
            put(&mut retry, 2);
            assert_eq!(
                retry.signal_at(id, &token(), "a", json!(2), 0).unwrap(),
                SignalResult::Duplicate
            );
            retry.commit().unwrap();
            assert_eq!(
                Documents::new(&store)
                    .get(&address())
                    .unwrap()
                    .unwrap()
                    .value["n"],
                2
            );
        }
    }
}

#[test]
fn host_signal_deadline_and_overlapping_task_writes_fail_closed() {
    let store = Store::memory().unwrap();
    let id = setup(&store, Some(20));
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(id, &token(), "a")
            .unwrap(),
        None
    );
    // Raw signal namespace writes cannot be mixed around the typed API.
    for after_signal in [false, true] {
        let mut mixed = HostTransaction::new(&store);
        if after_signal {
            mixed.signal_at(id, &token(), "a", json!(1), 0).unwrap();
        }
        let key = b"durable/signal-revision/forged".to_vec();
        let batch = pi_rs_durable::task_store::OwnedBatch {
            checks: vec![durable_store::Check {
                key: key.clone(),
                expected: Expected::Missing,
            }],
            mutations: vec![durable_store::Mutation::Put { key, value: vec![] }],
        };
        assert!(matches!(mixed.append(batch), Err(Error::InvalidBatch)));
    }
    let ts = TaskStore::new(&store);
    let mut task = ts.load(id).unwrap().unwrap();
    task.task.cancel().unwrap();
    let graph = ts.prepare_save(&[Save::Update(&task)]).unwrap();
    let mut before = HostTransaction::new(&store);
    before.append(graph).unwrap();
    assert_eq!(
        before.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Closed
    );
    let mut after = HostTransaction::new(&store);
    after.signal_at(id, &token(), "a", json!(1), 0).unwrap();
    let graph = ts.prepare_save(&[Save::Update(&task)]).unwrap();
    assert!(matches!(after.append(graph), Err(Error::InvalidBatch)));
}

#[test]
fn host_multiple_keys_duplicates_and_invalid_values_reopen_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, None);
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    for (key, value, expected) in [
        ("a", json!(1), SignalResult::Delivered),
        ("a", json!("bad"), SignalResult::Invalid),
        ("a", json!(2), SignalResult::Duplicate),
        ("b", json!("bad"), SignalResult::Invalid),
        ("b", json!(3), SignalResult::Delivered),
        ("b", json!(4), SignalResult::Duplicate),
    ] {
        assert_eq!(tx.signal_at(id, &token(), key, value, 0).unwrap(), expected);
    }
    tx.commit().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    assert_eq!(
        ts.read_typed_signal(id, &token(), "a").unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        ts.read_typed_signal(id, &token(), "b").unwrap(),
        Some(json!(3))
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
fn host_multiple_signals_fence_concurrent_delivery_between_calls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, None);
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    let other = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&other)
            .deliver_signal_at(id, &token(), "b", json!(7), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(id, &token(), "c", json!(3), 0).unwrap(),
        SignalResult::Delivered
    );
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    let ts = TaskStore::new(&store);
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert_eq!(ts.read_typed_signal(id, &token(), "a").unwrap(), None);
    assert_eq!(ts.read_typed_signal(id, &token(), "c").unwrap(), None);
    let mut retry = HostTransaction::new(&store);
    put(&mut retry, 2);
    assert_eq!(
        retry.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        retry.signal_at(id, &token(), "b", json!(8), 0).unwrap(),
        SignalResult::Duplicate
    );
    assert_eq!(
        retry.signal_at(id, &token(), "c", json!(3), 0).unwrap(),
        SignalResult::Delivered
    );
    retry.commit().unwrap();
    drop(other);
    drop(store);
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    for (key, value) in [("a", 1), ("b", 7), ("c", 3)] {
        assert_eq!(
            ts.read_typed_signal(id, &token(), key).unwrap(),
            Some(json!(value))
        );
    }
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
fn host_repeat_closed_and_missing_preserves_upstream_validation_order() {
    use pi_rs_durable::task_store::SignalState;
    let store = Store::memory().unwrap();
    let id = setup(&store, Some(100));
    let mut tx = HostTransaction::new(&store);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 100).unwrap(),
        SignalResult::Closed
    );
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!("bad"), 100).unwrap(),
        SignalResult::Invalid
    );
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(2), 100).unwrap(),
        SignalResult::Closed
    );
    let missing = Uuid::from_u128(99);
    assert_eq!(
        tx.signal_at(missing, &token(), "x", json!("bad"), 0)
            .unwrap(),
        SignalResult::NotFound
    );
    assert_eq!(
        tx.signal_at(missing, &token(), "x", json!(1), 0).unwrap(),
        SignalResult::NotFound
    );
    tx.commit().unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal_state(id, &token(), "a")
            .unwrap(),
        Some(SignalState::Expired)
    );
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap();
    ts.update(&task, |task| task.cancel()).unwrap();
    let mut closed = HostTransaction::new(&store);
    assert_eq!(
        closed
            .signal_at(id, &token(), "a", json!("bad"), 0)
            .unwrap(),
        SignalResult::Closed
    );
    assert_eq!(
        closed.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Closed
    );
    closed.commit().unwrap();
}

#[test]
fn host_multiple_signal_checks_keep_earliest_deadline_and_abort_fence() {
    let store = Store::memory().unwrap();
    let id = setup(&store, Some(1_000));
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(id, &token(), "b", json!(2), 990).unwrap(),
        SignalResult::Delivered
    );
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    let ts = TaskStore::new(&store);
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    for key in ["a", "b"] {
        assert_eq!(ts.read_typed_signal(id, &token(), key).unwrap(), None);
    }
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 2);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    let current = ts.load(id).unwrap().unwrap();
    ts.update(&current, |task| task.cancel()).unwrap();
    assert_eq!(
        tx.signal_at(id, &token(), "b", json!("bad"), 0).unwrap(),
        SignalResult::Closed
    );
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert_eq!(ts.read_typed_signal(id, &token(), "a").unwrap(), None);
}

#[test]
fn host_signal_reconciles_existing_target_read_fence() {
    let store = Store::memory().unwrap();
    let id = setup(&store, None);
    let mut key = b"durable/task/".to_vec();
    key.extend_from_slice(id.as_bytes());
    let entry = store.get(&key).unwrap().unwrap();
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    tx.append(pi_rs_durable::task_store::OwnedBatch {
        checks: vec![durable_store::Check {
            key: key.clone(),
            expected: Expected::Version(entry.version),
        }],
        mutations: vec![],
    })
    .unwrap();
    // A version bump between the independent reads must be a commit conflict,
    // not an invalid composition or a successful stale transaction.
    store
        .commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: entry.value,
            }],
        )
        .unwrap();
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(id, &token(), "a")
            .unwrap(),
        None
    );
}

#[test]
fn host_multi_signal_races_external_delivery_without_partial_commit() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, None);
    drop(store);
    let barrier = Arc::new(Barrier::new(3));
    let task_path = path.clone();
    let first_barrier = barrier.clone();
    let host = std::thread::spawn(move || {
        let store = Store::open(task_path).unwrap();
        let mut tx = HostTransaction::new(&store);
        put(&mut tx, 1);
        assert_eq!(
            tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
            SignalResult::Delivered
        );
        assert_eq!(
            tx.signal_at(id, &token(), "b", json!(2), 0).unwrap(),
            SignalResult::Delivered
        );
        first_barrier.wait();
        tx.commit()
    });
    let task_path = path.clone();
    let second_barrier = barrier.clone();
    let competing = std::thread::spawn(move || {
        let store = Store::open(task_path).unwrap();
        second_barrier.wait();
        TaskStore::new(&store)
            .deliver_signal_at(id, &token(), "a", json!(7), 0)
            .unwrap()
    });
    barrier.wait();
    let host_result = host.join().unwrap();
    let competing_result = competing.join().unwrap();
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let a = ts.read_typed_signal(id, &token(), "a").unwrap();
    let b = ts.read_typed_signal(id, &token(), "b").unwrap();
    let document = Documents::new(&store).get(&address()).unwrap();
    if host_result.is_ok() {
        assert_eq!(competing_result, SignalResult::Duplicate);
        assert_eq!(a, Some(json!(1)));
        assert_eq!(b, Some(json!(2)));
        assert_eq!(document.unwrap().value["n"], 1);
    } else {
        assert!(matches!(
            host_result,
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(competing_result, SignalResult::Delivered);
        assert_eq!(a, Some(json!(7)));
        assert_eq!(b, None);
        assert!(document.is_none());
    }
}

fn setup_single(store: &Store, deadline: Option<i64>) -> Uuid {
    let ts = TaskStore::new(store);
    let task = Task::new(
        Uuid::from_u128(11),
        TaskOwner::Conversation(Uuid::from_u128(9)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("host-wake".into(), true);
    ts.save(&[Save::Create(&task)]).unwrap();
    ts.reserve(&ts.load(task.id()).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(task.id()).unwrap().unwrap(), |task| {
        task.wait(WaitFor::TypedSleep {
            until_millis: deadline,
            signals: vec![token().key("a").unwrap()],
        })
    })
    .unwrap();
    task.id()
}

#[test]
fn host_staged_signal_atomically_wakes_single_key_and_reopens() {
    use pi_rs_durable::task::{TaskState, TypedWake};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup_single(&store, Some(60_000));
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_with_staged_signal(id).unwrap();
    assert!(matches!(
        tx.signal_at(id, &token(), "a", json!(2), 0),
        Err(Error::SignalComposition)
    ));
    tx.commit().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap().task;
    assert_eq!(task.state(), &TaskState::Ready);
    assert_eq!(
        task.typed_wake(),
        Some(&TypedWake::Signal(token().key("a").unwrap()))
    );
    let receipt = ts
        .signal(id, task.delivered_signal().unwrap())
        .unwrap()
        .unwrap();
    assert!(receipt.is_consumed());
    assert_eq!(
        ts.read_typed_signal(id, &token(), "a").unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        Documents::new(&store)
            .get(&address())
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
    drop(store);
    let mut registry = pi_rs_durable::task::scheduler::Registry::default();
    registry.register("host-wake", true, |task: &Task| {
        assert_eq!(
            task.typed_wake(),
            Some(&TypedWake::Signal(token().key("a").unwrap()))
        );
        Ok(pi_rs_durable::task::scheduler::Step::Succeed(json!(
            "resumed"
        )))
    });
    let mut scheduler = pi_rs_durable::task::scheduler::Scheduler::open(&path, registry).unwrap();
    assert!(scheduler.tick(60_001).unwrap());
    assert!(
        matches!(scheduler.get(id).unwrap().unwrap().state(), TaskState::Succeeded(value) if *value == json!("resumed"))
    );
}

#[test]
fn host_wake_rejects_when_no_eligible_signal_without_mutating_transaction() {
    let store = Store::memory().unwrap();
    let id = setup(&store, None);
    let mut tx = HostTransaction::new(&store);
    assert!(tx.wake_with_signal(id).is_err());
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_with_signal(id).unwrap();
    tx.commit().unwrap();
    let ts = TaskStore::new(&store);
    assert_eq!(
        ts.load(id).unwrap().unwrap().task.state(),
        &pi_rs_durable::task::TaskState::Ready
    );
    assert_eq!(
        ts.read_typed_signal(id, &token(), "a").unwrap(),
        Some(json!(1))
    );
}

#[test]
fn host_staged_wake_conflicts_on_abort_or_competing_delivery() {
    for mode in [0, 1] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup_single(&store, None);
        let mut tx = HostTransaction::new(&store);
        put(&mut tx, 1);
        assert_eq!(
            tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
            SignalResult::Delivered
        );
        tx.wake_with_staged_signal(id).unwrap();
        let other = Store::open(&path).unwrap();
        let ts = TaskStore::new(&other);
        if mode == 0 {
            let task = ts.load(id).unwrap().unwrap();
            ts.update(&task, |task| task.cancel()).unwrap();
        } else {
            assert_eq!(
                ts.deliver_signal_at(id, &token(), "a", json!(7), 0)
                    .unwrap(),
                SignalResult::Delivered
            );
        }
        assert!(matches!(
            tx.commit(),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        drop(other);
        drop(store);
        let store = Store::open(&path).unwrap();
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        assert_eq!(
            TaskStore::new(&store)
                .read_typed_signal(id, &token(), "a")
                .unwrap(),
            if mode == 0 { None } else { Some(json!(7)) }
        );
    }
}

#[test]
fn host_staged_wake_cannot_cross_deadline() {
    let store = Store::memory().unwrap();
    let id = setup_single(&store, Some(20));
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_with_staged_signal(id).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(id, &token(), "a")
            .unwrap(),
        None
    );
}

#[test]
fn host_staged_signal_wakes_single_typed_signal_wait() {
    let store = Store::memory().unwrap();
    let ts = TaskStore::new(&store);
    let task = Task::new(
        Uuid::from_u128(12),
        TaskOwner::Conversation(Uuid::from_u128(9)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    ts.save(&[Save::Create(&task)]).unwrap();
    ts.reserve(&ts.load(task.id()).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(task.id()).unwrap().unwrap(), |task| {
        task.wait(WaitFor::TypedSignal(token().key("a").unwrap()))
    })
    .unwrap();
    let mut tx = HostTransaction::new(&store);
    assert_eq!(
        tx.signal_at(task.id(), &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_with_staged_signal(task.id()).unwrap();
    tx.commit().unwrap();
    let task = ts.load(task.id()).unwrap().unwrap().task;
    assert_eq!(task.state(), &pi_rs_durable::task::TaskState::Ready);
    assert_eq!(task.typed_wake(), None);
    assert!(
        ts.signal(task.id(), task.delivered_signal().unwrap())
            .unwrap()
            .unwrap()
            .is_consumed()
    );
}

#[test]
fn host_staged_wake_races_external_delivery_atomically() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup_single(&store, None);
    drop(store);
    let barrier = Arc::new(Barrier::new(3));
    let host_path = path.clone();
    let host_barrier = barrier.clone();
    let host = std::thread::spawn(move || {
        let store = Store::open(host_path).unwrap();
        let mut tx = HostTransaction::new(&store);
        put(&mut tx, 1);
        assert_eq!(
            tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
            SignalResult::Delivered
        );
        tx.wake_with_staged_signal(id).unwrap();
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
    let result = host.join().unwrap();
    let other = other.join().unwrap();
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap().task;
    let signal = ts.read_typed_signal(id, &token(), "a").unwrap();
    let doc = Documents::new(&store).get(&address()).unwrap();
    if result.is_ok() {
        assert_eq!(other, SignalResult::Duplicate);
        assert_eq!(signal, Some(json!(1)));
        assert_eq!(task.state(), &pi_rs_durable::task::TaskState::Ready);
        assert!(
            ts.signal(id, task.delivered_signal().unwrap())
                .unwrap()
                .unwrap()
                .is_consumed()
        );
        assert!(doc.is_some());
    } else {
        assert!(matches!(
            result,
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(other, SignalResult::Delivered);
        assert_eq!(signal, Some(json!(7)));
        assert!(matches!(
            task.state(),
            pi_rs_durable::task::TaskState::Waiting(_)
        ));
        assert!(doc.is_none());
    }
}

#[test]
fn host_staged_wake_reconciles_prior_stale_task_read() {
    let store = Store::memory().unwrap();
    let id = setup_single(&store, None);
    let mut key = b"durable/task/".to_vec();
    key.extend_from_slice(id.as_bytes());
    let entry = store.get(&key).unwrap().unwrap();
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    tx.append(pi_rs_durable::task_store::OwnedBatch {
        checks: vec![durable_store::Check {
            key: key.clone(),
            expected: Expected::Version(entry.version),
        }],
        mutations: vec![],
    })
    .unwrap();
    store
        .commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: entry.value,
            }],
        )
        .unwrap();
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_with_staged_signal(id).unwrap();
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let ts = TaskStore::new(&store);
    assert!(matches!(
        ts.load(id).unwrap().unwrap().task.state(),
        pi_rs_durable::task::TaskState::Waiting(_)
    ));
    assert_eq!(ts.read_typed_signal(id, &token(), "a").unwrap(), None);
}

#[test]
fn host_wake_chooses_first_wait_key_across_committed_and_staged_signals() {
    use pi_rs_durable::task::{TaskState, TypedWake};
    for mode in 0..=2 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let id = setup(&store, None);
        let ts = TaskStore::new(&store);
        let committed = if mode == 0 { "b" } else { "a" };
        assert_eq!(
            ts.deliver_signal_at(id, &token(), committed, json!(7), 0)
                .unwrap(),
            SignalResult::Delivered
        );
        let mut tx = HostTransaction::new(&store);
        put(&mut tx, 1);
        if mode != 2 {
            let staged = if mode == 0 { "a" } else { "b" };
            assert_eq!(
                tx.signal_at(id, &token(), staged, json!(8), 0).unwrap(),
                SignalResult::Delivered
            );
        }
        tx.wake_with_signal(id).unwrap();
        tx.commit().unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        let ts = TaskStore::new(&store);
        let task = ts.load(id).unwrap().unwrap().task;
        assert_eq!(task.state(), &TaskState::Ready);
        assert_eq!(
            task.typed_wake(),
            Some(&TypedWake::Signal(token().key("a").unwrap()))
        );
        let receipt = ts
            .signal(id, task.delivered_signal().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(receipt.payload, if mode == 0 { json!(8) } else { json!(7) });
        assert!(receipt.is_consumed());
        if mode != 2 {
            assert_eq!(
                ts.read_typed_signal(id, &token(), "b").unwrap(),
                Some(if mode == 0 { json!(7) } else { json!(8) })
            );
            let mut prefix = b"durable/signal/".to_vec();
            prefix.extend_from_slice(id.as_bytes());
            let remaining: Vec<pi_rs_durable::task_store::Signal> = store
                .list(&prefix)
                .unwrap()
                .into_iter()
                .map(|(_, entry)| serde_json::from_slice(&entry.value).unwrap())
                .filter(|signal: &pi_rs_durable::task_store::Signal| {
                    signal.typed_key() == Some(&token().key("b").unwrap())
                })
                .collect();
            assert_eq!(remaining.len(), 1);
            assert!(!remaining[0].is_consumed());
        }
        assert_eq!(
            Documents::new(&store)
                .get(&address())
                .unwrap()
                .unwrap()
                .value["n"],
            1
        );
    }
}

#[test]
fn two_host_wakes_compete_for_existing_receipt_without_partial_writes() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, None);
    assert_eq!(
        TaskStore::new(&store)
            .deliver_signal_at(id, &token(), "a", json!(7), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    let mut revision_key = b"durable/signal-revision/".to_vec();
    revision_key.extend_from_slice(id.as_bytes());
    let old_revision = store.get(&revision_key).unwrap().unwrap().version;
    drop(store);
    let barrier = Arc::new(Barrier::new(3));
    let threads: Vec<_> = (1..=2)
        .map(|n| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let store = Store::open(path).unwrap();
                let mut tx = HostTransaction::new(&store);
                put(&mut tx, n);
                tx.wake_with_signal(id).unwrap();
                barrier.wait();
                tx.commit()
            })
        })
        .collect();
    barrier.wait();
    let outcomes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(
                result,
                Err(Error::Store(durable_store::Error::Conflict { .. }))
            ))
            .count(),
        1
    );
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap().task;
    assert_eq!(task.state(), &pi_rs_durable::task::TaskState::Ready);
    assert!(
        ts.signal(id, task.delivered_signal().unwrap())
            .unwrap()
            .unwrap()
            .is_consumed()
    );
    assert_eq!(
        ts.read_typed_signal(id, &token(), "a").unwrap(),
        Some(json!(7))
    );
    assert!(store.get(&revision_key).unwrap().unwrap().version > old_revision);
    let n = Documents::new(&store)
        .get(&address())
        .unwrap()
        .unwrap()
        .value["n"]
        .as_i64()
        .unwrap();
    assert!(n == 1 || n == 2);
}

#[test]
fn host_multi_key_wake_uses_wait_order_not_staging_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup(&store, None);
    let mut tx = HostTransaction::new(&store);
    assert_eq!(
        tx.signal_at(id, &token(), "b", json!(2), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.wake_with_signal(id).unwrap();
    tx.commit().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let task = ts.load(id).unwrap().unwrap().task;
    assert_eq!(
        task.typed_wake(),
        Some(&pi_rs_durable::task::TypedWake::Signal(
            token().key("a").unwrap()
        ))
    );
    let selected = ts
        .signal(id, task.delivered_signal().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(selected.payload, json!(1));
    assert!(selected.is_consumed());
    let mut prefix = b"durable/signal/".to_vec();
    prefix.extend_from_slice(id.as_bytes());
    let rest: Vec<pi_rs_durable::task_store::Signal> = store
        .list(&prefix)
        .unwrap()
        .into_iter()
        .map(|(_, entry)| serde_json::from_slice(&entry.value).unwrap())
        .filter(|signal: &pi_rs_durable::task_store::Signal| {
            signal.typed_key() == Some(&token().key("b").unwrap())
        })
        .collect();
    assert_eq!(rest.len(), 1);
    assert!(!rest[0].is_consumed());
}

#[test]
fn host_wakes_committed_signal_and_scheduler_resumes_after_reopen() {
    use pi_rs_durable::task::{
        TaskState, TypedWake,
        scheduler::{Registry, Scheduler, Step},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let store = Store::open(&path).unwrap();
    let id = setup_single(&store, None);
    assert_eq!(
        TaskStore::new(&store)
            .deliver_signal_at(id, &token(), "a", json!(7), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    let mut tx = HostTransaction::new(&store);
    tx.wake_with_signal(id).unwrap();
    tx.commit().unwrap();
    drop(store);
    let mut registry = Registry::default();
    registry.register("host-wake", true, |task: &Task| {
        assert_eq!(
            task.typed_wake(),
            Some(&TypedWake::Signal(token().key("a").unwrap()))
        );
        Ok(Step::Succeed(json!("resumed")))
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(
        matches!(scheduler.get(id).unwrap().unwrap().state(), TaskState::Succeeded(value) if *value == json!("resumed"))
    );
}

#[test]
fn candidate_task_signals_commit_together_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("candidate.db");
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let task = Task::new(
        Uuid::from_u128(44),
        TaskOwner::Conversation(Uuid::from_u128(9)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    let mut tx = HostTransaction::new(&store);
    tx.append(ts.prepare_save(&[Save::Create(&task)]).unwrap())
        .unwrap();
    assert_eq!(
        tx.signal_at(task.id(), &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(task.id(), &token(), "a", json!(2), 0).unwrap(),
        SignalResult::Duplicate
    );
    assert_eq!(
        tx.signal_at(task.id(), &token(), "b", json!(3), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.commit().unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    assert!(ts.load(task.id()).unwrap().is_some());
    assert_eq!(
        ts.read_typed_signal(task.id(), &token(), "a").unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        ts.read_typed_signal(task.id(), &token(), "b").unwrap(),
        Some(json!(3))
    );
}

#[test]
fn builtin_notification_rechecks_staged_generic_version() {
    let store = Store::memory().unwrap();
    let id = setup(&store, None);
    let generic = SignalToken::new("pi.submission-settled", 2).unwrap();
    let builtin = pi_rs_durable::runtime::submission_settled_signal();
    let value = json!({"id":Uuid::new_v4(), "conversationId":Uuid::new_v4(), "type":"input", "status":"unanswered", "reason":"failed"});
    let mut tx = HostTransaction::new(&store);
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(id, &generic, "same", json!({"legacy":true}), 0)
            .unwrap(),
        SignalResult::Delivered
    );
    assert!(matches!(
        tx.signal_at(id, &builtin, "same", value, 0),
        Err(Error::Signal(
            pi_rs_durable::task_store::Error::NewerSignalVersion {
                stored: 2,
                current: 1
            }
        ))
    ));
    drop(tx);
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(id, &generic, "same")
            .unwrap()
            .is_none()
    );
}

#[test]
fn candidate_terminal_and_abort_override_stored_live_task() {
    for abort in [false, true] {
        let store = Store::memory().unwrap();
        let id = setup(&store, None);
        let ts = TaskStore::new(&store);
        let mut candidate = ts.load(id).unwrap().unwrap();
        if abort {
            let mut encoded = serde_json::to_value(&candidate.task).unwrap();
            encoded["abort_requested"] = json!(true);
            candidate.task = serde_json::from_value(encoded).unwrap();
        } else {
            candidate.task.cancel().unwrap();
        }
        let mut tx = HostTransaction::new(&store);
        tx.append(ts.prepare_save(&[Save::Update(&candidate)]).unwrap())
            .unwrap();
        assert_eq!(
            tx.signal_at(id, &token(), "a", json!(1), 0).unwrap(),
            SignalResult::Closed
        );
        tx.commit().unwrap();
        assert_eq!(ts.read_typed_signal(id, &token(), "a").unwrap(), None);
        assert!(
            ts.load(id).unwrap().unwrap().task.abort_requested()
                || ts.load(id).unwrap().unwrap().task.is_terminal()
        );
    }
}

#[test]
fn candidate_created_deadline_conflict_rolls_back_everything() {
    let store = Store::memory().unwrap();
    let ts = TaskStore::new(&store);
    let mut task = Task::new(
        Uuid::from_u128(45),
        TaskOwner::Conversation(Uuid::from_u128(9)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    task.start().unwrap();
    task.wait(WaitFor::TypedSleep {
        until_millis: Some(1_000),
        signals: vec![token().key("a").unwrap()],
    })
    .unwrap();
    let mut tx = HostTransaction::new(&store);
    tx.append(ts.prepare_save(&[Save::Create(&task)]).unwrap())
        .unwrap();
    put(&mut tx, 1);
    assert_eq!(
        tx.signal_at(task.id(), &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    std::thread::sleep(std::time::Duration::from_millis(1_100));
    assert!(matches!(
        tx.commit(),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(ts.load(task.id()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert_eq!(
        ts.read_typed_signal(task.id(), &token(), "a").unwrap(),
        None
    );
}

#[cfg(unix)]
#[test]
fn candidate_signal_kill_fixture() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_CANDIDATE_SIGNAL_DB") else {
        return;
    };
    let store = Store::open(path).unwrap();
    let ts = TaskStore::new(&store);
    let task = Task::new(
        Uuid::from_u128(46),
        TaskOwner::Conversation(Uuid::from_u128(9)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    let mut tx = HostTransaction::new(&store);
    tx.append(ts.prepare_save(&[Save::Create(&task)]).unwrap())
        .unwrap();
    assert_eq!(
        tx.signal_at(task.id(), &token(), "a", json!(1), 0).unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        tx.signal_at(task.id(), &token(), "b", json!(2), 0).unwrap(),
        SignalResult::Delivered
    );
    tx.commit().unwrap();
    println!("CANDIDATE_SIGNAL_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn candidate_signal_survives_process_kill() {
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
            .args(["--exact", "candidate_signal_kill_fixture", "--nocapture"])
            .env("PI_CANDIDATE_SIGNAL_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let output = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if line.unwrap().contains("CANDIDATE_SIGNAL_COMMITTED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(30))
        .expect("candidate transaction did not commit");
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let store = Store::open(&path).unwrap();
    let ts = TaskStore::new(&store);
    let id = Uuid::from_u128(46);
    assert!(ts.load(id).unwrap().is_some());
    assert_eq!(
        ts.read_typed_signal(id, &token(), "a").unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        ts.read_typed_signal(id, &token(), "b").unwrap(),
        Some(json!(2))
    );
}
