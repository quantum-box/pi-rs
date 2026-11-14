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
