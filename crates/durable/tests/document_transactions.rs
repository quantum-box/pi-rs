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
    let before = Documents::new(&db).snapshot().unwrap();
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(1)).unwrap();
    tx.append(tasks.prepare_save(&[Save::Create(&task)]).unwrap())
        .unwrap();
    assert!(matches!(
        tx.commit(&db),
        Err(Error::Store(durable_store::Error::Conflict { .. }))
    ));
    assert!(Documents::new(&db).get(&a).unwrap().is_none());
    assert_eq!(Documents::new(&db).snapshot().unwrap(), before);
    assert!(Documents::new(&db).get_at(&a, before).unwrap().is_none());
    assert_eq!(tasks.load(task.id()).unwrap().unwrap().task, task);
}

#[test]
fn historical_snapshots_survive_updates_delete_recreate_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (a, b) = (address("history-a"), address("history-b"));
    let db = Store::open(&path).unwrap();
    let docs = Documents::new(&db);
    let empty = docs.snapshot().unwrap();
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(1)).unwrap();
    tx.put(&b, Expected::Missing, &object(2)).unwrap();
    tx.commit(&db).unwrap();
    let first = docs.snapshot().unwrap();
    let old_a = docs.get(&a).unwrap().unwrap();
    let old_b = docs.get(&b).unwrap().unwrap();
    let mut tx = Transaction::new();
    tx.delete(&a, old_a.version).unwrap();
    tx.put(&b, Expected::Version(old_b.version), &object(3))
        .unwrap();
    tx.commit(&db).unwrap();
    let deleted = docs.snapshot().unwrap();
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(4)).unwrap();
    tx.commit(&db).unwrap();
    let recreated = docs.snapshot().unwrap();
    drop(db);
    let db = Store::open(&path).unwrap();
    let docs = Documents::new(&db);
    assert_eq!(docs.get_at(&a, empty).unwrap(), None);
    assert_eq!(docs.get_at(&a, first).unwrap(), Some(old_a.clone()));
    assert_eq!(docs.get_at(&b, first).unwrap(), Some(old_b));
    assert_eq!(docs.get_at(&a, deleted).unwrap(), None);
    assert_eq!(docs.get_at(&b, deleted).unwrap().unwrap().value, object(3));
    assert_eq!(
        docs.get_at(&a, recreated).unwrap().unwrap().value,
        object(4)
    );
    assert!(docs.get_at(&a, recreated + 1).is_err());
    let mut stale = Transaction::new();
    stale
        .put(&a, Expected::Version(old_a.version), &object(5))
        .unwrap();
    assert!(stale.commit(&db).is_err());
    assert_eq!(docs.snapshot().unwrap(), recreated);
}

#[test]
fn snapshots_are_atomic_across_competing_multi_document_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Store::open(&path).unwrap();
    let (a, b) = (address("atomic-a"), address("atomic-b"));
    let mut tx = Transaction::new();
    tx.put(&a, Expected::Missing, &object(0)).unwrap();
    tx.put(&b, Expected::Missing, &object(0)).unwrap();
    tx.commit(&db).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let path2 = path.clone();
    let a2 = a.clone();
    let b2 = b.clone();
    let barrier2 = barrier.clone();
    let writer = std::thread::spawn(move || {
        let db = Store::open(path2).unwrap();
        barrier2.wait();
        for n in 1..=50 {
            let docs = Documents::new(&db);
            let mut tx = Transaction::new();
            tx.put(
                &a2,
                Expected::Version(docs.get(&a2).unwrap().unwrap().version),
                &object(n),
            )
            .unwrap();
            tx.put(
                &b2,
                Expected::Version(docs.get(&b2).unwrap().unwrap().version),
                &object(n),
            )
            .unwrap();
            tx.commit(&db).unwrap();
        }
    });
    barrier.wait();
    for _ in 0..100 {
        let docs = Documents::new(&db);
        let snapshot = docs.snapshot().unwrap();
        assert_eq!(
            docs.get_at(&a, snapshot).unwrap().unwrap().value,
            docs.get_at(&b, snapshot).unwrap().unwrap().value
        );
    }
    writer.join().unwrap();
}

#[test]
fn concurrent_document_commit_cannot_move_a_conversation_cut() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let a = address("race-cut");
    let key = a_key(&a);
    let cut = b"runtime/fork/test/0001".to_vec();
    let barrier = Arc::new(Barrier::new(2));
    let db1 = Store::open(&path).unwrap();
    let db2 = Store::open(&path).unwrap();
    let barrier2 = barrier.clone();
    let key2 = key.clone();
    let writer = std::thread::spawn(move || {
        barrier2.wait();
        db2.commit(
            &[],
            &[Mutation::Put {
                key: key2,
                value: br#"{"n":2}"#.to_vec(),
            }],
        )
        .unwrap();
    });
    barrier.wait();
    db1.commit(
        &[],
        &[
            Mutation::Put {
                key: cut.clone(),
                value: b"snapshot".to_vec(),
            },
            Mutation::Put {
                key: key.clone(),
                value: br#"{"n":1}"#.to_vec(),
            },
        ],
    )
    .unwrap();
    writer.join().unwrap();
    let revision = db1.document_cut(&cut).unwrap().unwrap();
    let at_cut = db1.document_at(&key, revision).unwrap().unwrap();
    assert_eq!(at_cut.value, br#"{"n":1}"#);
    drop(db1);
    let reopened = Store::open(&path).unwrap();
    assert_eq!(reopened.document_cut(&cut).unwrap(), Some(revision));
    assert_eq!(
        reopened.document_at(&key, revision).unwrap().unwrap().value,
        br#"{"n":1}"#
    );
}

fn a_key(a: &Address) -> Vec<u8> {
    let mut key = b"durable/document/".to_vec();
    key.extend(serde_json::to_vec(&[&a.scope, &a.name, &a.key]).unwrap());
    key
}
