use durable_store::Store;
use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner},
    task_store::{Error, Save, Signal, SignalToken, TaskStore},
};
use serde_json::json;
use uuid::Uuid;

fn schema() -> serde_json::Value {
    json!({"type":"object", "properties":{"answer":{"type":"integer"}}, "required":["answer"]})
}

#[test]
fn validation_and_migration_are_safe_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("signals.db");
    let task_id = Uuid::new_v4();
    let receipt_id = Uuid::new_v4();
    let old = SignalToken::new("answer", 1).unwrap();
    let checked = SignalToken::with_schema("answer", 2, schema()).unwrap();
    assert!(SignalToken::with_schema("answer", 1, json!({"type":"nonsense"})).is_err());
    assert!(matches!(
        Signal::new_typed(Uuid::new_v4(), task_id, &checked, "bad", json!({})),
        Err(Error::InvalidSignalPayload)
    ));
    {
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
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
        store.save(&[Save::Create(&task)]).unwrap();
        store
            .send_signal(&Signal::new_typed(receipt_id, task_id, &old, "key", json!(42)).unwrap())
            .unwrap();
        assert!(
            store
                .read_typed_signal(task_id, &checked, "absent")
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store.read_typed_signal(task_id, &checked, "key"),
            Err(Error::SignalMigration(_))
        ));
        let invalid = checked.clone().with_migration(|_, _| Ok(json!({})));
        assert!(matches!(
            store.read_typed_signal(task_id, &invalid, "key"),
            Err(Error::InvalidSignalPayload)
        ));
        let failing = checked
            .clone()
            .with_migration(|_, _| Err("cannot migrate".into()));
        assert!(matches!(
            store.read_typed_signal(task_id, &failing, "key"),
            Err(Error::SignalMigration(_))
        ));
        let panicking = checked
            .clone()
            .with_migration(|_, _| panic!("bad migration"));
        assert!(matches!(
            store.read_typed_signal(task_id, &panicking, "key"),
            Err(Error::SignalMigration(_))
        ));
    }
    {
        let db = Store::open(&path).unwrap();
        let store = TaskStore::new(&db);
        let migrated = checked.with_migration(|value, version| {
            assert_eq!(version, 1);
            Ok(json!({"answer":value}))
        });
        assert_eq!(
            store.read_typed_signal(task_id, &migrated, "key").unwrap(),
            Some(json!({"answer":42}))
        );
        assert_eq!(
            store.signal(task_id, receipt_id).unwrap().unwrap().payload,
            json!(42)
        );
        let older = SignalToken::new("answer", 1).unwrap();
        let newer = SignalToken::new("newer", 2).unwrap();
        store
            .send_signal(
                &Signal::new_typed(Uuid::new_v4(), task_id, &newer, "key", json!(1)).unwrap(),
            )
            .unwrap();
        let older_newer = SignalToken::new("newer", 1).unwrap();
        assert!(matches!(
            store.read_typed_signal(task_id, &older_newer, "key"),
            Err(Error::NewerSignalVersion { .. })
        ));
        let current = SignalToken::with_schema("current", 1, schema()).unwrap();
        store
            .send_signal(
                &Signal::new_typed(
                    Uuid::new_v4(),
                    task_id,
                    &current,
                    "key",
                    json!({"answer": 7}),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(
            store.read_typed_signal(task_id, &current, "key").unwrap(),
            Some(json!({"answer": 7}))
        );
        let mismatch = SignalToken::with_schema("answer", 1, schema()).unwrap();
        assert!(matches!(
            store.read_typed_signal(task_id, &mismatch, "key"),
            Err(Error::InvalidSignalPayload)
        ));
        assert_eq!(
            store.read_typed_signal(task_id, &older, "key").unwrap(),
            Some(json!(42))
        );
    }
}
