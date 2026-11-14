use async_trait::async_trait;
use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    ownership_edge::{Edge, EdgeStore, Mode},
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
    task::{Checkpoint, Task, TaskOwner},
    task_store::{Save, TaskStore},
};
use std::sync::Arc;
use uuid::Uuid;

struct Stub;
#[async_trait]
impl Model for Stub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        unreachable!()
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Stub),
        tools: vec![],
        restart_limit: 1,
        max_turns: 1,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    }
}
fn task(owner: Uuid) -> Task {
    Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        },
        1,
    )
    .unwrap()
}

#[test]
fn atomic_composition_conflicts_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (parent, child, other) = {
        let mut harness = Harness::open_paused(&path, options()).unwrap();
        (
            harness.create_conversation(config()).unwrap(),
            harness.create_conversation(config()).unwrap(),
            harness.create_conversation(config()).unwrap(),
        )
    };
    let t = task(parent);
    let first = Edge {
        task: t.id(),
        task_owner: t.owner().clone(),
        child,
        mode: Mode::Foreground,
    };
    let second = Edge {
        child: other,
        mode: Mode::Background,
        ..first.clone()
    };
    {
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&t)]).unwrap();
        let edges = EdgeStore::new(&db);
        let a = edges.prepare_create(&first).unwrap();
        let mut checks = a.checks;
        let mut mutations = a.mutations;
        checks.push(Check {
            key: b"checkpoint".to_vec(),
            expected: Expected::Version(42),
        });
        mutations.push(Mutation::Put {
            key: b"checkpoint".to_vec(),
            value: b"committed".to_vec(),
        });
        assert!(matches!(
            db.commit(&checks, &mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(edges.get(child).unwrap().is_none());
        assert!(edges.get(other).unwrap().is_none());
        assert!(db.get(b"checkpoint").unwrap().is_none());
        checks.pop();
        db.commit(&checks, &mutations).unwrap();
        let b = edges.prepare_create(&second).unwrap();
        db.commit(&b.checks, &b.mutations).unwrap();
        assert!(matches!(
            db.commit(&checks, &mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
    }
    let db = Store::open(&path).unwrap();
    let edges = EdgeStore::new(&db);
    assert_eq!(edges.get(child).unwrap().unwrap().edge, first);
    assert_eq!(edges.get(other).unwrap().unwrap().edge, second);
    assert_eq!(edges.children(t.id()).unwrap().len(), 2);
    let rival = task(parent);
    TaskStore::new(&db).save(&[Save::Create(&rival)]).unwrap();
    let competing = Edge {
        task: rival.id(),
        task_owner: rival.owner().clone(),
        child,
        mode: Mode::Background,
    };
    let batch = edges.prepare_create(&competing).unwrap();
    assert!(matches!(
        db.commit(&batch.checks, &batch.mutations),
        Err(durable_store::Error::Conflict { .. })
    ));
    assert_eq!(edges.get(child).unwrap().unwrap().edge, first);
    assert_eq!(db.get(b"checkpoint").unwrap().unwrap().value, b"committed");
}

#[test]
fn indirect_cycles_across_task_and_background_edges_are_rejected_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (a, b, c) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        (
            h.create_conversation(config()).unwrap(),
            h.create_conversation(config()).unwrap(),
            h.create_conversation(config()).unwrap(),
        )
    };
    let ta = task(a);
    let nested = Task::new(
        Uuid::new_v4(),
        TaskOwner::Task(ta.id()),
        ta.checkpoint().clone(),
        1,
    )
    .unwrap();
    let tb = task(b);
    let tc = task(c);
    {
        let db = Store::open(&path).unwrap();
        TaskStore::new(&db)
            .save(&[
                Save::Create(&ta),
                Save::Create(&nested),
                Save::Create(&tb),
                Save::Create(&tc),
            ])
            .unwrap();
        for edge in [
            Edge {
                task: nested.id(),
                task_owner: nested.owner().clone(),
                child: b,
                mode: Mode::Background,
            },
            Edge {
                task: tb.id(),
                task_owner: tb.owner().clone(),
                child: c,
                mode: Mode::Foreground,
            },
        ] {
            let batch = EdgeStore::new(&db).prepare_create(&edge).unwrap();
            db.commit(&batch.checks, &batch.mutations).unwrap();
        }
    }
    let db = Store::open(&path).unwrap();
    let closing = Edge {
        task: tc.id(),
        task_owner: tc.owner().clone(),
        child: a,
        mode: Mode::Foreground,
    };
    assert!(matches!(
        EdgeStore::new(&db).prepare_create(&closing),
        Err(pi_rs_durable::ownership_edge::Error::Invalid)
    ));
    assert!(EdgeStore::new(&db).get(a).unwrap().is_none());
}

#[test]
fn concurrently_prepared_opposite_edges_cannot_both_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (a, b) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        (
            h.create_conversation(config()).unwrap(),
            h.create_conversation(config()).unwrap(),
        )
    };
    let db = Store::open(&path).unwrap();
    let ta = task(a);
    let tb = task(b);
    TaskStore::new(&db)
        .save(&[Save::Create(&ta), Save::Create(&tb)])
        .unwrap();
    let ab = Edge {
        task: ta.id(),
        task_owner: ta.owner().clone(),
        child: b,
        mode: Mode::Foreground,
    };
    let ba = Edge {
        task: tb.id(),
        task_owner: tb.owner().clone(),
        child: a,
        mode: Mode::Background,
    };
    let first = EdgeStore::new(&db).prepare_create(&ab).unwrap();
    let second = EdgeStore::new(&db).prepare_create(&ba).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = [first, second]
        .into_iter()
        .map(|batch| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let db = Store::open(path).unwrap();
                barrier.wait();
                db.commit(&batch.checks, &batch.mutations)
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(durable_store::Error::Conflict { .. })))
            .count(),
        1
    );
    drop(db);
    let db = Store::open(&path).unwrap();
    let edges = EdgeStore::new(&db);
    let loser = if edges.get(b).unwrap().is_some() {
        &ba
    } else {
        &ab
    };
    assert!(matches!(
        edges.prepare_create(loser),
        Err(pi_rs_durable::ownership_edge::Error::Invalid)
    ));
}

#[test]
fn task_store_rejects_replacing_an_owner() {
    let db = Store::memory().unwrap();
    let tasks = TaskStore::new(&db);
    let original = task(Uuid::new_v4());
    tasks.save(&[Save::Create(&original)]).unwrap();
    let mut replacement = tasks.load(original.id()).unwrap().unwrap();
    replacement.task = Task::new(
        original.id(),
        TaskOwner::Conversation(Uuid::new_v4()),
        original.checkpoint().clone(),
        1,
    )
    .unwrap();
    assert!(matches!(
        tasks.save(&[Save::Update(&replacement)]),
        Err(pi_rs_durable::task_store::Error::OwnerChanged)
    ));
    assert_eq!(tasks.load(original.id()).unwrap().unwrap().task, original);
}

#[test]
fn identity_owner_and_endpoint_versions_are_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (parent, child) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        (
            h.create_conversation(config()).unwrap(),
            h.create_conversation(config()).unwrap(),
        )
    };
    let db = Store::open(path).unwrap();
    let tasks = TaskStore::new(&db);
    let t = task(parent);
    tasks.save(&[Save::Create(&t)]).unwrap();
    let edges = EdgeStore::new(&db);
    let edge = Edge {
        task: t.id(),
        task_owner: t.owner().clone(),
        child,
        mode: Mode::Foreground,
    };
    let mut bad = edge.clone();
    bad.task_owner = TaskOwner::Conversation(child);
    assert!(edges.prepare_create(&bad).is_err());
    bad = edge.clone();
    bad.child = parent;
    assert!(edges.prepare_create(&bad).is_err());
    bad = edge.clone();
    bad.child = Uuid::new_v4();
    assert!(edges.prepare_create(&bad).is_err());
    let batch = edges.prepare_create(&edge).unwrap();
    let snapshot = tasks.load(t.id()).unwrap().unwrap();
    tasks.update(&snapshot, |t| t.start()).unwrap();
    assert!(matches!(
        db.commit(&batch.checks, &batch.mutations),
        Err(durable_store::Error::Conflict { .. })
    ));
    let batch = edges.prepare_create(&edge).unwrap();
    let key = format!("runtime/conversation/{child}").into_bytes();
    let entry = db.get(&key).unwrap().unwrap();
    db.commit(
        &[Check {
            key: key.clone(),
            expected: Expected::Version(entry.version),
        }],
        &[Mutation::Put {
            key: key.clone(),
            value: entry.value.clone(),
        }],
    )
    .unwrap();
    assert!(matches!(
        db.commit(&batch.checks, &batch.mutations),
        Err(durable_store::Error::Conflict { .. })
    ));
    let mut corrupted: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    corrupted["id"] = serde_json::json!(parent);
    db.commit(
        &[],
        &[Mutation::Put {
            key,
            value: serde_json::to_vec(&corrupted).unwrap(),
        }],
    )
    .unwrap();
    assert!(edges.prepare_create(&edge).is_err());
    assert!(edges.get(child).unwrap().is_none());
}

#[test]
fn composed_admission_advances_task_fence_and_recovers_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (parent, child) = {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        (
            h.create_conversation(config()).unwrap(),
            h.create_conversation(config()).unwrap(),
        )
    };
    let t = task(parent);
    let edge = Edge {
        task: t.id(),
        task_owner: t.owner().clone(),
        child,
        mode: Mode::Foreground,
    };
    let mut task_key = b"durable/task/".to_vec();
    task_key.extend_from_slice(t.id().as_bytes());
    let admission_key = b"test/child-admission".to_vec();
    {
        let db = Store::open(&path).unwrap();
        let tasks = TaskStore::new(&db);
        tasks.save(&[Save::Create(&t)]).unwrap();
        // A traversal that read this version before admission must fail its
        // subsequent abort CAS, even if it did not see the new edge.
        let stale = db.get(&task_key).unwrap().unwrap();
        let prepared = EdgeStore::new(&db).prepare_create(&edge).unwrap();
        let mut checks = prepared.checks;
        checks.push(Check {
            key: admission_key.clone(),
            expected: Expected::Missing,
        });
        let mut mutations = prepared.mutations;
        // The edge preparation itself advances the task fence.
        mutations.push(Mutation::Put {
            key: admission_key.clone(),
            value: b"admitted".to_vec(),
        });
        db.commit(&checks, &mutations).unwrap();
        assert!(matches!(
            db.commit(
                &[Check {
                    key: task_key.clone(),
                    expected: Expected::Version(stale.version)
                }],
                &[Mutation::Put {
                    key: task_key.clone(),
                    value: stale.value
                }],
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
    }
    let db = Store::open(&path).unwrap();
    assert_eq!(EdgeStore::new(&db).get(child).unwrap().unwrap().edge, edge);
    assert_eq!(db.get(&admission_key).unwrap().unwrap().value, b"admitted");
}
