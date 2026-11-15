use super::*;
use serde_json::json;

fn cp(phase: &str) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data: json!(null),
    }
}
fn task(id: u128, owner: TaskOwner, kind: &str, safe: bool, limit: u32) -> Task {
    Task::new(Uuid::from_u128(id), owner, cp("start"), limit)
        .unwrap()
        .registered(kind.into(), safe)
}

#[test]
fn acquired_documents_follow_completing_abort_failure_reopen_and_watch() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Expected, Mutation, Store};
    for mode in ["success", "abort", "failure"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        let child = task(2, TaskOwner::Task(root.id()), "child", true, 4);
        let registry = || {
            let mut r = Registry::default();
            r.register("parent", true, move |_: &Task| {
                if mode == "failure" {
                    Ok(Step::Fail("failure".into()))
                } else {
                    Ok(Step::Succeed(json!(7)))
                }
            });
            r.register_with_abort(
                "child",
                true,
                |_: &Task| Ok(Step::Succeed(json!(0))),
                |_: &Task| Ok(Step::Succeed(json!(null))),
            );
            r
        };
        let mut scheduler = Scheduler::open(&path, registry()).unwrap();
        scheduler
            .ts()
            .save(&[Save::Create(&root), Save::Create(&child)])
            .unwrap();
        let db = Store::open(&path).unwrap();
        let address = Address {
            scope: "opaque".into(),
            name: "owned".into(),
            key: mode.into(),
        };
        let mut tx = Transaction::new();
        tx.acquire_task_document(&address, root.id()).unwrap();
        tx.put(
            &address,
            Expected::Missing,
            json!({"v": 1}).as_object().unwrap(),
        )
        .unwrap();
        tx.commit(&db).unwrap();
        let (initial, mut watch) = Documents::new(&db).watch_task_document(&address).unwrap();
        assert!(initial.document.is_some());
        scheduler.tick(0).unwrap();
        assert!(scheduler.get(root.id()).unwrap().unwrap().is_completing());
        assert!(Documents::new(&db).get(&address).unwrap().is_some());
        assert!(watch.poll(10).unwrap().is_empty());
        assert!(!watch.ended());
        if mode == "abort" {
            scheduler.request_abort(root.id()).unwrap();
        }
        let cursor = watch.cursor();
        drop(watch);
        drop(scheduler);
        drop(db);
        let mut scheduler = Scheduler::open(&path, registry()).unwrap();
        for _ in 0..10 {
            if scheduler.get(root.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            scheduler.tick(0).unwrap();
        }
        assert!(scheduler.get(root.id()).unwrap().unwrap().is_terminal());
        let db = Store::open(&path).unwrap();
        let docs = Documents::new(&db);
        assert!(docs.get(&address).unwrap().is_none());
        let mut watch = docs.resume_task_document_watch(&address, cursor).unwrap();
        let frames = watch.poll(10).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(frames[0].retired);
        assert_eq!(frames[0].document.as_ref().unwrap().value["v"], 1);
        assert!(watch.ended());
        assert!(watch.poll(10).unwrap().is_empty());
        let key = address.storage_key().unwrap();
        assert!(
            db.commit(
                &[],
                &[Mutation::Put {
                    key,
                    value: br#"{}"#.to_vec()
                }]
            )
            .is_err()
        );
    }
}

#[test]
fn acquired_handler_document_is_retired_with_immediate_or_held_success() {
    use crate::document::{Address, Documents, TaskDocuments};
    struct Parent;
    impl Handler for Parent {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            _: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            let address = Address {
                scope: "scope".into(),
                name: "owned".into(),
                key: "".into(),
            };
            docs.acquire_owned(&address).map_err(|e| e.to_string())?;
            docs.put(&address, json!({"value": 1}).as_object().unwrap().clone())
                .map_err(|e| e.to_string())?;
            Ok(Step::Succeed(json!(0)))
        }
    }
    for held in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut r = Registry::default();
        r.register("parent", true, Parent);
        r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(0))));
        let mut scheduler = Scheduler::open(dir.path().join("tasks.db"), r).unwrap();
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "parent",
            true,
            4,
        );
        let child = task(2, TaskOwner::Task(root.id()), "child", true, 4);
        if held {
            scheduler
                .ts()
                .save(&[Save::Create(&root), Save::Create(&child)])
                .unwrap();
        } else {
            scheduler.ts().save(&[Save::Create(&root)]).unwrap();
        }
        scheduler.tick(0).unwrap();
        let address = Address {
            scope: "scope".into(),
            name: "owned".into(),
            key: "".into(),
        };
        assert_eq!(
            Documents::new(scheduler.owner().store())
                .get(&address)
                .unwrap()
                .is_some(),
            held
        );
        if held {
            scheduler.tick(0).unwrap();
            scheduler.tick(0).unwrap();
            assert!(
                Documents::new(scheduler.owner().store())
                    .get(&address)
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn acquired_document_fences_raw_writes_and_rolls_back_with_task() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Check, Expected, Mutation, Store};
    let db = Store::memory().unwrap();
    let ts = TaskStore::new(&db);
    let root = task(
        1,
        TaskOwner::Conversation(Uuid::from_u128(9)),
        "job",
        true,
        4,
    );
    ts.save(&[Save::Create(&root)]).unwrap();
    let address = Address {
        scope: "unreserved".into(),
        name: "owned".into(),
        key: "".into(),
    };
    let mut tx = Transaction::new();
    tx.acquire_task_document(&address, root.id()).unwrap();
    tx.put(
        &address,
        Expected::Missing,
        json!({"v": 1}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
    let mut next = ts.load(root.id()).unwrap().unwrap();
    next.task.start().unwrap();
    next.task.succeed(json!(1)).unwrap();
    let batch = ts.prepare_save(&[Save::Update(&next)]).unwrap();
    let mut checks = batch.checks.clone();
    checks.push(Check {
        key: b"absent".to_vec(),
        expected: Expected::Version(42),
    });
    assert!(db.commit(&checks, &batch.mutations).is_err());
    assert!(!ts.load(root.id()).unwrap().unwrap().task.is_terminal());
    assert!(Documents::new(&db).get(&address).unwrap().is_some());
    let (_, mut watch) = Documents::new(&db).watch_task_document(&address).unwrap();
    db.commit(&batch.checks, &batch.mutations).unwrap();
    let stored = ts.load(root.id()).unwrap().unwrap();
    let frames = watch.poll(10).unwrap();
    assert!(watch.ended());
    assert_eq!(frames.len(), 1);
    assert_eq!(
        db.commit_for_version(&crate::task_store::key(root.id()), stored.version)
            .unwrap(),
        db.document_commit_sequence(frames[0].revision).unwrap()
    );
    let key = address.storage_key().unwrap();
    assert!(
        db.commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: br#"{}"#.to_vec()
            }]
        )
        .is_err()
    );
    let mut checked = Transaction::new();
    checked
        .put(&address, Expected::Missing, json!({}).as_object().unwrap())
        .unwrap();
    assert!(checked.commit(&db).is_err());
    let task_key = crate::task_store::key(root.id());
    assert!(
        db.commit(
            &[],
            &[Mutation::Put {
                key: task_key,
                value: serde_json::to_vec(&root).unwrap()
            }]
        )
        .is_err()
    );
}

#[test]
fn terminal_commit_hides_staged_update_and_ends_watch_in_one_frame() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Expected, Store};
    let db = Store::memory().unwrap();
    let ts = TaskStore::new(&db);
    let root = task(
        1,
        TaskOwner::Conversation(Uuid::from_u128(9)),
        "job",
        true,
        4,
    );
    ts.save(&[Save::Create(&root)]).unwrap();
    let address = Address {
        scope: "legacy-compatible".into(),
        name: "doc".into(),
        key: "".into(),
    };
    let mut tx = Transaction::new();
    tx.acquire_task_document(&address, root.id()).unwrap();
    tx.put(
        &address,
        Expected::Missing,
        json!({"v": 1}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
    let docs = Documents::new(&db);
    let (initial, mut watch) = docs.watch_task_document(&address).unwrap();
    let mut next = ts.load(root.id()).unwrap().unwrap();
    next.task.start().unwrap();
    next.task.succeed(json!(0)).unwrap();
    let mut tx = Transaction::new();
    tx.put(
        &address,
        Expected::Version(initial.document.unwrap().version),
        json!({"v": 2}).as_object().unwrap(),
    )
    .unwrap();
    tx.append(ts.prepare_save(&[Save::Update(&next)]).unwrap())
        .unwrap();
    tx.commit(&db).unwrap();
    let frames = watch.poll(1).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(frames[0].retired);
    assert_eq!(frames[0].document.as_ref().unwrap().value["v"], 1);
    assert!(watch.ended());
    assert!(docs.get(&address).unwrap().is_none());
}

#[test]
fn acquire_create_task_and_terminal_in_one_checked_batch() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Expected, Store};
    let db = Store::memory().unwrap();
    let ts = TaskStore::new(&db);
    let mut root = task(
        1,
        TaskOwner::Conversation(Uuid::from_u128(9)),
        "job",
        true,
        4,
    );
    root.start().unwrap();
    root.succeed(json!(1)).unwrap();
    let address = Address {
        scope: "ordinary".into(),
        name: "same-commit".into(),
        key: "".into(),
    };
    let mut tx = Transaction::new();
    tx.acquire_task_document(&address, root.id()).unwrap();
    tx.put(
        &address,
        Expected::Missing,
        json!({"never_live": true}).as_object().unwrap(),
    )
    .unwrap();
    tx.append(ts.prepare_save(&[Save::Create(&root)]).unwrap())
        .unwrap();
    tx.commit(&db).unwrap();
    assert!(ts.load(root.id()).unwrap().unwrap().task.is_terminal());
    assert!(Documents::new(&db).get(&address).unwrap().is_none());
    let (_, mut watch) = Documents::new(&db).watch_task_document(&address).unwrap();
    assert!(watch.poll(1).unwrap().is_empty());
    assert!(watch.ended());
}

#[test]
fn acquisition_prepared_before_terminal_or_host_creation_fails_atomically() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Expected, Mutation, Store};
    for terminal in [false, true] {
        let db = Store::memory().unwrap();
        let ts = TaskStore::new(&db);
        let root = task(
            1,
            TaskOwner::Conversation(Uuid::from_u128(9)),
            "job",
            true,
            4,
        );
        ts.save(&[Save::Create(&root)]).unwrap();
        let address = Address {
            scope: "existing-host-scope".into(),
            name: "race".into(),
            key: "".into(),
        };
        let mut pending = Transaction::new();
        pending.acquire_task_document(&address, root.id()).unwrap();
        pending
            .put(
                &address,
                Expected::Missing,
                json!({"task": true}).as_object().unwrap(),
            )
            .unwrap();
        if terminal {
            let record = ts.load(root.id()).unwrap().unwrap();
            ts.update(&record, |t| {
                t.start()?;
                t.succeed(json!(0))
            })
            .unwrap();
        } else {
            db.commit(
                &[],
                &[Mutation::Put {
                    key: address.storage_key().unwrap(),
                    value: br#"{"host":true}"#.to_vec(),
                }],
            )
            .unwrap();
        }
        assert!(pending.commit(&db).is_err());
        let doc = Documents::new(&db).get(&address).unwrap();
        if terminal {
            assert!(doc.is_none());
        } else {
            assert_eq!(doc.unwrap().value["host"], true);
        }
        let marker =
            durable_store::task_document_owner_key(&address.storage_key().unwrap()).unwrap();
        assert!(db.get(&marker).unwrap().is_none());
    }
}

#[test]
fn acquisition_never_steals_existing_host_document() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Expected, Store};
    let db = Store::memory().unwrap();
    let root = task(
        1,
        TaskOwner::Conversation(Uuid::from_u128(9)),
        "job",
        true,
        4,
    );
    TaskStore::new(&db).save(&[Save::Create(&root)]).unwrap();
    let address = Address {
        scope: "pi-rs/task/00000000-0000-0000-0000-000000000001".into(),
        name: "legacy".into(),
        key: "".into(),
    };
    let mut tx = Transaction::new();
    tx.put(
        &address,
        Expected::Missing,
        json!({"safe": true}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
    let mut tx = Transaction::new();
    tx.acquire_task_document(&address, root.id()).unwrap();
    assert!(tx.commit(&db).is_err());
    assert_eq!(
        Documents::new(&db).get(&address).unwrap().unwrap().value["safe"],
        true
    );
}

#[test]
fn acquired_document_write_racing_terminal_is_retired_or_rejected() {
    use crate::document::{Address, Documents, Transaction};
    use durable_store::{Expected, Mutation, Store};
    use std::sync::Barrier;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("race.db");
    for n in 1..=12 {
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        let root = task(
            n,
            TaskOwner::Conversation(Uuid::from_u128(99)),
            "job",
            true,
            4,
        );
        ts.save(&[Save::Create(&root)]).unwrap();
        let address = Address {
            scope: "race".into(),
            name: n.to_string(),
            key: "".into(),
        };
        let mut tx = Transaction::new();
        tx.acquire_task_document(&address, root.id()).unwrap();
        tx.put(&address, Expected::Missing, json!({}).as_object().unwrap())
            .unwrap();
        tx.commit(&db).unwrap();
        let mut next = ts.load(root.id()).unwrap().unwrap();
        next.task.start().unwrap();
        next.task.succeed(json!(0)).unwrap();
        let batch = ts.prepare_save(&[Save::Update(&next)]).unwrap();
        let key = address.storage_key().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let other = barrier.clone();
        let path = path.clone();
        let writer = std::thread::spawn(move || {
            let db = Store::open(path).unwrap();
            other.wait();
            db.commit(
                &[],
                &[Mutation::Put {
                    key,
                    value: br#"{"v":2}"#.to_vec(),
                }],
            )
        });
        barrier.wait();
        db.commit(&batch.checks, &batch.mutations).unwrap();
        let result = writer.join().unwrap();
        assert!(result.is_ok() || matches!(result, Err(durable_store::Error::Conflict { .. })));
        assert!(Documents::new(&db).get(&address).unwrap().is_none());
    }
}
