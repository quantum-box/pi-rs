use durable_store::{Expected, Store};
use pi_rs_durable::{
    document::{Address, Documents, TaskDocuments, Transaction},
    task::{
        Checkpoint, Task, TaskOwner, TaskState, WaitFor,
        scheduler::{Error, Handler, Registry, Scheduler, Step},
    },
    task_store::TaskStore,
};
use serde_json::json;
use std::path::PathBuf;
use uuid::Uuid;

fn cp(phase: &str) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data: json!({}),
    }
}
fn address(name: &str) -> Address {
    Address {
        scope: "test".into(),
        name: name.into(),
        key: "".into(),
    }
}
fn parent() -> Task {
    Task::new(
        Uuid::from_u128(1),
        TaskOwner::Conversation(Uuid::from_u128(2)),
        cp("start"),
        3,
    )
    .unwrap()
    .registered("parent".into(), true)
}
fn child() -> Task {
    Task::new(
        Uuid::from_u128(3),
        TaskOwner::Task(parent().id()),
        cp("child"),
        3,
    )
    .unwrap()
    .registered("child".into(), true)
}
struct Work {
    conflict_path: Option<PathBuf>,
}
impl Handler for Work {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let old = docs.get(&address("write")).unwrap().unwrap();
        docs.get(&address("dependency")).unwrap();
        docs.put(
            &address("write"),
            json!({"n": 1}).as_object().unwrap().clone(),
        )
        .unwrap();
        if let Some(path) = &self.conflict_path {
            // Deterministically change a read-only dependency after it was read.
            let db = Store::open(path).unwrap();
            let mut tx = Transaction::new();
            tx.put(
                &address("dependency"),
                Expected::Missing,
                json!({"changed": true}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(&db).unwrap();
        }
        assert_eq!(old.value["n"], 0);
        Ok(Step::Wait {
            checkpoint: cp("next"),
            condition: WaitFor::Tasks(vec![child().id()]),
            children: vec![child()],
        })
    }
}
fn registry(path: Option<PathBuf>) -> Registry {
    let mut r = Registry::default();
    r.register(
        "parent",
        true,
        Work {
            conflict_path: path,
        },
    );
    r.register("child", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    r
}
fn initialize(path: &std::path::Path) {
    let db = Store::open(path).unwrap();
    let mut tx = Transaction::new();
    tx.put(
        &address("write"),
        Expected::Missing,
        json!({"n": 0}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
}
#[test]
fn checkpoint_documents_and_child_survive_reopen_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut scheduler = Scheduler::open(&path, registry(None)).unwrap();
    scheduler.submit(&parent()).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&db);
    let p = tasks.load(parent().id()).unwrap().unwrap().task;
    assert_eq!(p.checkpoint(), &cp("next"));
    assert_eq!(
        p.state(),
        &TaskState::Waiting(WaitFor::Tasks(vec![child().id()]))
    );
    assert_eq!(tasks.load(child().id()).unwrap().unwrap().task, child());
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
}
#[test]
fn read_only_conflict_rolls_back_step_and_reopen_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    initialize(&path);
    let mut scheduler = Scheduler::open(&path, registry(Some(path.clone()))).unwrap();
    scheduler.submit(&parent()).unwrap();
    assert!(matches!(
        scheduler.tick(0),
        Err(Error::Document(pi_rs_durable::document::Error::Store(
            durable_store::Error::Conflict { .. }
        )))
    ));
    assert!(matches!(scheduler.tick(0), Err(Error::Poisoned)));
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&db);
    let p = tasks.load(parent().id()).unwrap().unwrap().task;
    assert_eq!(p.checkpoint(), &cp("start"));
    assert_eq!(p.state(), &TaskState::Running);
    assert!(tasks.load(child().id()).unwrap().is_none());
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        0
    );
    drop(db);
    let mut recovered = Scheduler::open(&path, registry(None)).unwrap();
    recovered.tick(0).unwrap();
    drop(recovered);
    let db = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .load(parent().id())
            .unwrap()
            .unwrap()
            .task
            .checkpoint(),
        &cp("next")
    );
    assert!(TaskStore::new(&db).load(child().id()).unwrap().is_some());
    assert_eq!(
        Documents::new(&db)
            .get(&address("write"))
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
}
