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
    assert!(matches!(
        before.signal_at(id, &token(), "a", json!(1), 0),
        Err(Error::SignalComposition)
    ));
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
