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
        let b = edges.prepare_create(&second).unwrap();
        let mut checks = [a.checks, b.checks].concat();
        let mut mutations = [a.mutations, b.mutations].concat();
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
