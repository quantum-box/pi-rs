use durable_store::Store;
use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, WaitFor},
    task_store::{Save, SignalResult, SignalState, SignalToken, TaskStore, WakeResult},
};
use serde_json::json;
use uuid::Uuid;

fn setup(store: &TaskStore<'_>, id: Uuid, token: &SignalToken) {
    let task = Task::new(
        id,
        TaskOwner::Conversation(Uuid::new_v4()),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    store.save(&[Save::Create(&task)]).unwrap();
    store.reserve(&store.load(id).unwrap().unwrap()).unwrap();
    store
        .update(&store.load(id).unwrap().unwrap(), |t| {
            t.wait(WaitFor::TypedSleep {
                until_millis: Some(100),
                signals: vec![token.key("a").unwrap(), token.key("b").unwrap()],
            })
        })
        .unwrap();
}

#[test]
fn expiry_and_early_values_survive_reopen() {
    for early in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let id = Uuid::new_v4();
        let token = SignalToken::new("answer", 1).unwrap();
        {
            let db = Store::open(&path).unwrap();
            let store = TaskStore::new(&db);
            setup(&store, id, &token);
            assert_eq!(
                store
                    .poll_wait(&store.load(id).unwrap().unwrap(), 99)
                    .unwrap(),
                WakeResult::Pending
            );
            if early {
                assert_eq!(
                    store
                        .deliver_signal_at(id, &token, "b", json!(7), 99)
                        .unwrap(),
                    SignalResult::Delivered
                );
            }
        }
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        assert!(matches!(
            store
                .poll_wait(&store.load(id).unwrap().unwrap(), 100)
                .unwrap(),
            WakeResult::Woken { .. }
        ));
        drop(db);
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        assert_eq!(
            store.load(id).unwrap().unwrap().task.typed_wake(),
            Some(&if early {
                pi_rs_durable::task::TypedWake::Signal(token.key("b").unwrap())
            } else {
                pi_rs_durable::task::TypedWake::Expired
            })
        );
        if early {
            assert_eq!(
                store.read_typed_signal_state(id, &token, "b").unwrap(),
                Some(SignalState::Value(json!(7)))
            );
            assert_eq!(
                store
                    .deliver_signal_at(id, &token, "a", json!(1), 101)
                    .unwrap(),
                SignalResult::Delivered
            );
            assert_eq!(
                store
                    .deliver_signal_at(id, &token, "b", json!(1), 101)
                    .unwrap(),
                SignalResult::Duplicate
            );
        } else {
            for key in ["a", "b"] {
                assert_eq!(
                    store.read_typed_signal_state(id, &token, key).unwrap(),
                    Some(SignalState::Expired)
                );
                assert_eq!(
                    store
                        .deliver_signal_at(id, &token, key, json!(1), 101)
                        .unwrap(),
                    SignalResult::Closed
                );
            }
        }
    }
}

#[test]
fn late_delivery_expires_key_and_wake_expires_remaining_keys() {
    let db = Store::memory().unwrap();
    let store = TaskStore::new(&db);
    let id = Uuid::new_v4();
    let token = SignalToken::new("answer", 1).unwrap();
    setup(&store, id, &token);
    assert_eq!(
        store
            .deliver_signal_at(id, &token, "a", json!(1), 100)
            .unwrap(),
        SignalResult::Closed
    );
    store
        .poll_wait(&store.load(id).unwrap().unwrap(), 100)
        .unwrap();
    assert_eq!(
        store.read_typed_signal_state(id, &token, "b").unwrap(),
        Some(SignalState::Expired)
    );
}

#[test]
fn delivery_races_deadline_without_lost_values() {
    use std::sync::{Arc, Barrier};
    for _ in 0..30 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Store::open(&path).unwrap();
        let id = Uuid::new_v4();
        let token = SignalToken::new("answer", 1).unwrap();
        setup(&TaskStore::new(&db), id, &token);
        let barrier = Arc::new(Barrier::new(2));
        let p = path.clone();
        let b = barrier.clone();
        let delivery = std::thread::spawn(move || {
            let db = Store::open(p).unwrap();
            b.wait();
            TaskStore::new(&db)
                .deliver_signal_at(
                    id,
                    &SignalToken::new("answer", 1).unwrap(),
                    "a",
                    json!(5),
                    99,
                )
                .unwrap()
        });
        barrier.wait();
        let store = TaskStore::new(&db);
        loop {
            let snapshot = store.load(id).unwrap().unwrap();
            match store.poll_wait(&snapshot, 100) {
                Ok(_) => break,
                Err(pi_rs_durable::task_store::Error::Store(durable_store::Error::Conflict {
                    ..
                })) => continue,
                Err(e) => panic!("{e:?}"),
            }
        }
        let result = delivery.join().unwrap();
        let state = store.read_typed_signal_state(id, &token, "a").unwrap();
        match result {
            SignalResult::Delivered => assert_eq!(state, Some(SignalState::Value(json!(5)))),
            SignalResult::Closed => assert_eq!(state, Some(SignalState::Expired)),
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn first_listed_value_wins_and_consumed_or_expired_keys_reject_waits() {
    use pi_rs_durable::{task::TypedWake, task_store::Error};
    let db = Store::memory().unwrap();
    let store = TaskStore::new(&db);
    let id = Uuid::new_v4();
    let token = SignalToken::new("answer", 1).unwrap();
    setup(&store, id, &token);
    for key in ["b", "a"] {
        assert_eq!(
            store
                .deliver_signal_at(id, &token, key, json!(key), 99)
                .unwrap(),
            SignalResult::Delivered
        );
    }
    store
        .poll_wait(&store.load(id).unwrap().unwrap(), 100)
        .unwrap();
    assert_eq!(
        store.load(id).unwrap().unwrap().task.typed_wake(),
        Some(&TypedWake::Signal(token.key("a").unwrap()))
    );
    store.reserve(&store.load(id).unwrap().unwrap()).unwrap();
    assert!(matches!(
        store.update(&store.load(id).unwrap().unwrap(), |t| t.wait(
            WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![token.key("a").unwrap()]
            }
        )),
        Err(Error::SignalKeyConsumed)
    ));
    store
        .update(&store.load(id).unwrap().unwrap(), |t| {
            t.wait(WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![token.key("b").unwrap()],
            })
        })
        .unwrap();
    assert!(matches!(
        store
            .poll_wait(&store.load(id).unwrap().unwrap(), 101)
            .unwrap(),
        WakeResult::Woken { signal: Some(_) }
    ));
}
