use durable_store::Store;
use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_store::{Error, Save, Signal, SignalToken, TaskStore, WakeResult},
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn typed_keys_survive_reopen_and_isolate_names_keys_and_legacy_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    let task_id = Uuid::from_u128(1);
    let v1 = SignalToken::new("app.answer", 1).unwrap();
    let v2 = SignalToken::new("app.answer", 2).unwrap();
    let other = SignalToken::new("app.other", 1).unwrap();
    let receipt = Uuid::from_u128(4);
    {
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        let task = Task::new(
            task_id,
            TaskOwner::Conversation(Uuid::from_u128(2)),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap();
        store.save(&[Save::Create(&task)]).unwrap();
        store
            .send_signal(&Signal::new(
                Uuid::from_u128(1),
                task_id,
                "app.answer".into(),
                json!("legacy"),
            ))
            .unwrap();
        store
            .send_signal(
                &Signal::new_typed(Uuid::from_u128(2), task_id, &other, "first", json!(0)).unwrap(),
            )
            .unwrap();
        store
            .send_signal(
                &Signal::new_typed(Uuid::from_u128(3), task_id, &v1, "second", json!(0)).unwrap(),
            )
            .unwrap();
        let signal =
            Signal::new_typed(receipt, task_id, &v1, "first", json!({"answer": 42})).unwrap();
        store.send_signal(&signal).unwrap();
        store.send_signal(&signal).unwrap();
        assert!(matches!(
            store.send_signal(
                &Signal::new_typed(receipt, task_id, &v2, "first", json!({"answer": 42})).unwrap()
            ),
            Err(Error::SignalMismatch)
        ));
        store
            .reserve(&store.load(task_id).unwrap().unwrap())
            .unwrap();
        store
            .update(&store.load(task_id).unwrap().unwrap(), |task| {
                task.wait(WaitFor::TypedSignal(v2.key("first").unwrap()))
            })
            .unwrap();
    }
    {
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        let result = store
            .poll_wait(&store.load(task_id).unwrap().unwrap(), 0)
            .unwrap();
        let WakeResult::Woken {
            signal: Some(signal),
        } = result
        else {
            panic!("expected typed signal")
        };
        assert_eq!(signal.id, receipt);
        assert_eq!(signal.typed_version(), Some(1));
        assert_eq!(signal.typed_key(), Some(&v2.key("first").unwrap()));
        assert!(
            store
                .signal(task_id, receipt)
                .unwrap()
                .unwrap()
                .is_consumed()
        );
        store
            .reserve(&store.load(task_id).unwrap().unwrap())
            .unwrap();
        store
            .update(&store.load(task_id).unwrap().unwrap(), |task| {
                task.wait(WaitFor::Signal("app.answer".into()))
            })
            .unwrap();
        let WakeResult::Woken {
            signal: Some(signal),
        } = store
            .poll_wait(&store.load(task_id).unwrap().unwrap(), 0)
            .unwrap()
        else {
            panic!("expected legacy signal")
        };
        assert_eq!(signal.payload, json!("legacy"));
        assert_eq!(signal.typed_version(), None);
    }
}

#[test]
fn token_validation_and_legacy_record_deserialization() {
    for (name, version) in [("", 1), ("bad/name", 1), ("valid", 0)] {
        assert!(matches!(
            SignalToken::new(name, version),
            Err(Error::InvalidSignalToken)
        ));
    }
    let token = SignalToken::new("valid", 1).unwrap();
    assert!(matches!(token.key(""), Err(Error::InvalidSignalToken)));
    let id = Uuid::from_u128(10);
    let old = json!({"id": id, "task_id": id, "name": "old", "payload": 7, "consumed": false});
    let signal: Signal = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(signal.typed_key(), None);
    assert_eq!(serde_json::to_value(signal).unwrap(), old);
    assert_eq!(
        serde_json::to_value(token.key("a/b").unwrap()).unwrap(),
        json!({"signal":"valid", "key":"a/b"})
    );
}

#[test]
fn unrelated_signal_does_not_wake_typed_wait() {
    let db = Store::memory().unwrap();
    let store = TaskStore::new(&db);
    let id = Uuid::from_u128(11);
    let task = Task::new(
        id,
        TaskOwner::Conversation(Uuid::from_u128(12)),
        Checkpoint {
            phase: "x".into(),
            data: json!(null),
        },
        1,
    )
    .unwrap();
    store.save(&[Save::Create(&task)]).unwrap();
    let token = SignalToken::new("answer", 1).unwrap();
    store
        .send_signal(
            &Signal::new_typed(Uuid::from_u128(13), id, &token, "other", json!(1)).unwrap(),
        )
        .unwrap();
    store.reserve(&store.load(id).unwrap().unwrap()).unwrap();
    store
        .update(&store.load(id).unwrap().unwrap(), |task| {
            task.wait(WaitFor::TypedSignal(token.key("wanted").unwrap()))
        })
        .unwrap();
    assert_eq!(
        store
            .poll_wait(&store.load(id).unwrap().unwrap(), 0)
            .unwrap(),
        WakeResult::Pending
    );
}

#[test]
fn typed_key_is_single_value_and_closed_after_wake_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    let token = SignalToken::new("answer", 1).unwrap();
    let task_id = Uuid::from_u128(101);
    let first =
        Signal::new_typed(Uuid::from_u128(102), task_id, &token, "k", json!({"v": 1})).unwrap();
    let retry =
        Signal::new_typed(Uuid::from_u128(103), task_id, &token, "k", json!({"v": 1})).unwrap();
    {
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        for id in [task_id, Uuid::from_u128(104)] {
            let task = Task::new(
                id,
                TaskOwner::Conversation(Uuid::from_u128(1)),
                Checkpoint {
                    phase: "start".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap();
            store.save(&[Save::Create(&task)]).unwrap();
        }
        store.send_signal(&first).unwrap();
        store.send_signal(&retry).unwrap();
        assert!(store.signal(task_id, retry.id).unwrap().is_none());
        assert!(matches!(
            store.send_signal(
                &Signal::new_typed(Uuid::from_u128(105), task_id, &token, "k", json!(2)).unwrap()
            ),
            Err(Error::SignalKeyMismatch)
        ));
        store
            .send_signal(
                &Signal::new_typed(first.id, Uuid::from_u128(104), &token, "k", json!(2)).unwrap(),
            )
            .unwrap();
        store
            .reserve(&store.load(task_id).unwrap().unwrap())
            .unwrap();
        store
            .update(&store.load(task_id).unwrap().unwrap(), |t| {
                t.wait(WaitFor::TypedSignal(token.key("k").unwrap()))
            })
            .unwrap();
        assert!(matches!(
            store
                .poll_wait(&store.load(task_id).unwrap().unwrap(), 0)
                .unwrap(),
            WakeResult::Woken { .. }
        ));
    }
    {
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        store.send_signal(&first).unwrap();
        store.send_signal(&retry).unwrap();
        assert!(matches!(
            store.send_signal(
                &Signal::new_typed(Uuid::from_u128(106), task_id, &token, "k", json!(3)).unwrap()
            ),
            Err(Error::SignalKeyMismatch)
        ));
        store
            .reserve(&store.load(task_id).unwrap().unwrap())
            .unwrap();
        assert!(matches!(
            store.update(&store.load(task_id).unwrap().unwrap(), |t| t
                .wait(WaitFor::TypedSignal(token.key("k").unwrap()))),
            Err(Error::SignalKeyConsumed)
        ));
        store
            .update(&store.load(task_id).unwrap().unwrap(), |t| {
                t.wait(WaitFor::TypedSignal(token.key("next").unwrap()))
            })
            .unwrap();
    }
}

#[test]
fn concurrent_typed_deliveries_accept_only_one_value() {
    use std::sync::{Arc, Barrier};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    let db = Store::open(&path).unwrap();
    let task_id = Uuid::new_v4();
    let task = Task::new(
        task_id,
        TaskOwner::Conversation(Uuid::new_v4()),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    TaskStore::new(&db).save(&[Save::Create(&task)]).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|i| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let db = Store::open(&path).unwrap();
                let token = SignalToken::new("race", 1).unwrap();
                let signal =
                    Signal::new_typed(Uuid::new_v4(), task_id, &token, "key", json!(i)).unwrap();
                barrier.wait();
                TaskStore::new(&db).send_signal(&signal)
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::SignalKeyMismatch)))
            .count(),
        1
    );
}
