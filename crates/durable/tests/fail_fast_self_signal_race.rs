use durable_store::{Expected, Store};
use pi_rs_durable::{
    document::{Address, Documents, TaskDocuments, Transaction},
    task::{
        Checkpoint, Task, TaskOwner, TaskState, WaitFor,
        scheduler::{Handler, Registry, Scheduler, Step},
    },
    task_driver::Driver,
    task_store::{Save, SignalResult, SignalToken, TaskStore},
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn address(name: &str) -> Address {
    Address {
        scope: "compound-race".into(),
        name: name.into(),
        key: "".into(),
    }
}
fn token() -> SignalToken {
    SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap()
}
struct Running {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    calls: Arc<AtomicUsize>,
}
impl Handler for Running {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            docs.signal_at(task.id(), &token(), "a", json!(1), 99)
                .unwrap(),
            SignalResult::Delivered
        );
        assert_eq!(
            docs.signal_at(task.id(), &token(), "b", json!(2), 99)
                .unwrap(),
            SignalResult::Delivered
        );
        docs.acquire_owned(&address("owned")).unwrap();
        docs.put(
            &address("owned"),
            json!({"value":"staged"}).as_object().unwrap().clone(),
        )
        .unwrap();
        docs.get(&address("staged")).unwrap();
        docs.put(
            &address("staged"),
            json!({"value":"staged"}).as_object().unwrap().clone(),
        )
        .unwrap();
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .unwrap();
        Ok(Step::Succeed(json!("normal")))
    }
}

#[test]
fn fail_fast_after_equivalent_rewrite_discards_multiple_self_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (cleanup_tx, cleanup_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register("parent", false, |_: &Task| {
        Ok(Step::Succeed(json!("parent")))
    });
    let finish_rx = Mutex::new(finish_rx);
    registry.register_with_abort(
        "running",
        false,
        Running {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            calls: calls.clone(),
        },
        move |_: &Task| {
            cleanup_tx.send(()).unwrap();
            finish_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .unwrap();
            Ok(Step::Succeed(json!(null)))
        },
    );
    let scheduler = Scheduler::open(&path, registry).unwrap();
    let db = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&db);
    let (parent_id, running_id, failed_id) =
        (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3));
    let cp = Checkpoint {
        phase: "start".into(),
        data: json!(null),
    };
    let mut parent = Task::new(
        parent_id,
        TaskOwner::Conversation(Uuid::from_u128(9)),
        cp.clone(),
        3,
    )
    .unwrap()
    .registered("parent".into(), false);
    parent.start().unwrap();
    parent
        .wait(WaitFor::TasksFailFast(vec![running_id, failed_id]))
        .unwrap();
    let running = Task::new(running_id, TaskOwner::Task(parent_id), cp.clone(), 3)
        .unwrap()
        .registered("running".into(), false);
    let failed = Task::new(failed_id, TaskOwner::Task(parent_id), cp, 3)
        .unwrap()
        .registered("failed".into(), false);
    tasks
        .save(&[
            Save::Create(&parent),
            Save::Create(&running),
            Save::Create(&failed),
        ])
        .unwrap();
    let mut tx = Transaction::new();
    tx.acquire_task_document(&address("owned"), running_id)
        .unwrap();
    tx.put(
        &address("owned"),
        Expected::Missing,
        json!({"value":"original"}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(&db).unwrap();
    let docs = Documents::new(&db);
    let (_, mut watch) = docs.watch_task_document(&address("owned")).unwrap();
    let driver = Driver::start(scheduler);
    entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let snapshot = tasks.load(running_id).unwrap().unwrap();
    tasks.update(&snapshot, |_| Ok(())).unwrap();
    let failed = tasks.load(failed_id).unwrap().unwrap();
    tasks
        .update(&failed, |task| {
            task.start()?;
            task.fail("failure".into())
        })
        .unwrap();
    release_tx.send(()).unwrap();
    cleanup_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let current = driver.get(running_id).unwrap().unwrap();
    assert_eq!(current.state(), &TaskState::Running);
    assert!(current.abort_requested());
    assert_eq!(
        docs.get(&address("owned")).unwrap().unwrap().value["value"],
        "original"
    );
    assert!(docs.get(&address("staged")).unwrap().is_none());
    for key in ["a", "b"] {
        assert_eq!(
            tasks.read_typed_signal(running_id, &token(), key).unwrap(),
            None
        );
    }
    assert!(watch.poll(10).unwrap().is_empty());
    finish_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while !driver.get(running_id).unwrap().unwrap().is_terminal() {
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(
        driver.get(running_id).unwrap().unwrap().state(),
        &TaskState::Cancelled
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(docs.get(&address("owned")).unwrap().is_none());
    assert!(docs.get(&address("staged")).unwrap().is_none());
    for key in ["a", "b"] {
        assert_eq!(
            tasks.read_typed_signal(running_id, &token(), key).unwrap(),
            None
        );
    }
    let frames = watch.poll(10).unwrap();
    assert_eq!(frames.len(), 1);
    assert!(frames[0].retired);
    assert_eq!(
        frames[0].document.as_ref().unwrap().value["value"],
        "original"
    );
    driver.shutdown().unwrap();
}
