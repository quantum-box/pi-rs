use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_durable::{
    document::{Address, Documents, Error, Transaction},
    task::{Checkpoint, Task, TaskOwner},
    task_store::{OwnedBatch, Save, TaskStore},
};
use serde_json::{Map, Value, json};
use std::sync::{Arc, Barrier};
use uuid::Uuid;

fn address(name: &str) -> Address {
    Address {
        scope: "session".into(),
        name: name.into(),
        key: "".into(),
    }
}
fn object(n: i32) -> Map<String, Value> {
    json!({"n": n}).as_object().unwrap().clone()
}
fn task() -> Task {
    Task::new(
        Uuid::from_u128(1),
        TaskOwner::Conversation(Uuid::from_u128(2)),
        Checkpoint {
            phase: "start".into(),
            data: json!({}),
        },
        3,
    )
    .unwrap()
}

#[test]
fn atomic_documents_and_task_rollback_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (a, b) = (address("a"), address("b"));
    let t = task();
    {
        let db = Store::open(&path).unwrap();
        let mut tx = Transaction::new();
        tx.put(&a, Expected::Missing, &object(1)).unwrap();
        tx.put(&b, Expected::Missing, &object(2)).unwrap();
        tx.append(
            TaskStore::new(&db)
                .prepare_save(&[Save::Create(&t)])
                .unwrap(),
        )
        .unwrap();
        tx.commit(&db).unwrap();
        let docs = Documents::new(&db);
        let old = docs.get(&a).unwrap().unwrap();
        let mut stale = Transaction::new();
        stale
            .put(&a, Expected::Version(old.version), &object(3))
            .unwrap();
        stale.put(&b, Expected::Missing, &object(4)).unwrap();
        assert!(matches!(
            stale.commit(&db),
            Err(Error::Store(durable_store::Error::Conflict { .. }))
        ));
        assert_eq!(docs.get(&a).unwrap().unwrap(), old);
    }
    let db = Store::open(&path).unwrap();
    assert_eq!(
        Documents::new(&db).get(&b).unwrap().unwrap().value,
        object(2)
    );
    assert_eq!(TaskStore::new(&db).load(t.id()).unwrap().unwrap().task, t);
    let old = Documents::new(&db).get(&a).unwrap().unwrap();
    let mut tx = Transaction::new();
    tx.delete(&a, old.version).unwrap();
    tx.commit(&db).unwrap();
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(5)).unwrap();
    tx.commit(&db).unwrap();
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Version(old.version), &object(6))
        .unwrap();
    assert!(matches!(
        tx.commit(&db),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
}

#[test]
fn concurrent_writers_have_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let a = address("race");
    let db = Store::open(&path).unwrap();
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(0)).unwrap();
    tx.commit(&db).unwrap();
    let version = Documents::new(&db).get(&a).unwrap().unwrap().version;
    let db2 = Store::open(&path).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = [db, db2]
        .into_iter()
        .enumerate()
        .map(|(i, db)| {
            let barrier = barrier.clone();
            let a = a.clone();
            std::thread::spawn(move || {
                let mut tx = Transaction::new();
                tx.put(&a, Expected::Version(version), &object(i as i32 + 1))
                    .unwrap();
                barrier.wait();
                tx.commit(&db)
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Store(durable_store::Error::Conflict { .. }))))
            .count(),
        1
    );
    assert_ne!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&a)
            .unwrap()
            .unwrap()
            .version,
        version
    );
}

#[test]
fn invalid_composition_does_not_add_partial_writes() {
    let db = Store::memory().unwrap();
    let mut tx = Transaction::new();
    let a = address("a");
    tx.put(&a, Expected::Missing, &object(1)).unwrap();
    assert!(matches!(
        tx.put(&a, Expected::Missing, &object(2)),
        Err(Error::InvalidBatch)
    ));
    let batch = OwnedBatch {
        checks: vec![Check {
            key: b"x".to_vec(),
            expected: Expected::Missing,
        }],
        mutations: vec![Mutation::Put {
            key: b"y".to_vec(),
            value: vec![],
        }],
    };
    assert!(matches!(tx.append(batch), Err(Error::InvalidBatch)));
    tx.commit(&db).unwrap();
    assert!(db.get(b"x").unwrap().is_none());
    assert!(db.get(b"y").unwrap().is_none());
    assert_eq!(
        Documents::new(&db).get(&a).unwrap().unwrap().value,
        object(1)
    );
}

#[test]
fn conflicting_task_prevents_document_commit() {
    let db = Store::memory().unwrap();
    let task = task();
    let tasks = TaskStore::new(&db);
    tasks.save(&[Save::Create(&task)]).unwrap();
    let a = address("rollback");
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(1)).unwrap();
    tx.append(tasks.prepare_save(&[Save::Create(&task)]).unwrap())
        .unwrap();
    assert!(matches!(
        tx.commit(&db),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&db).get(&a).unwrap().is_none());
    assert_eq!(tasks.load(task.id()).unwrap().unwrap().task, task);
}
