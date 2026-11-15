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
            signals: vec![token().key("a").unwrap()],
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
    assert!(matches!(
        tx.signal_at(id, &token(), "a", json!(2), 0),
        Err(Error::SignalComposition)
    ));
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
