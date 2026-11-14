use durable_store::Store;
use pi_rs_durable::{
    document::{Address, Documents, TaskDocuments},
    task::{
        Checkpoint, Task, TaskOwner, TaskState, WaitFor,
        scheduler::{Handler, Registry, Scheduler, Step},
    },
    task_store::{Save, SignalResult, SignalToken, TaskStore},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

fn cp(s: &str) -> Checkpoint {
    Checkpoint {
        phase: s.into(),
        data: json!(null),
    }
}
fn task(id: u128, name: &str) -> Task {
    Task::new(
        Uuid::from_u128(id),
        TaskOwner::Conversation(Uuid::from_u128(90)),
        cp("start"),
        3,
    )
    .unwrap()
    .registered(name.into(), true)
}
fn addr() -> Address {
    Address {
        scope: "s".into(),
        name: "atomic".into(),
        key: "".into(),
    }
}
struct Send {
    path: std::path::PathBuf,
    race: u8,
    results: Arc<Mutex<Vec<SignalResult>>>,
}
impl Handler for Send {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "done" {
            return Ok(Step::Succeed(json!(null)));
        }
        let token = SignalToken::new("answer", 1).unwrap();
        let result = docs
            .signal_at(
                Uuid::from_u128(2),
                &token,
                "a",
                json!(1),
                if self.race == 3 || (self.race == 4 && !self.results.lock().unwrap().is_empty()) {
                    100
                } else {
                    99
                },
            )
            .unwrap();
        self.results.lock().unwrap().push(result);
        docs.get(&addr()).unwrap();
        docs.put(&addr(), json!({"done":true}).as_object().unwrap().clone())
            .unwrap();
        if (self.race == 1 || self.race == 2) && self.results.lock().unwrap().len() == 1 {
            let db = Store::open(&self.path).unwrap();
            let ts = TaskStore::new(&db);
            if self.race == 1 {
                assert_eq!(
                    ts.deliver_signal_at(Uuid::from_u128(2), &token, "a", json!(2), 99)
                        .unwrap(),
                    SignalResult::Delivered
                );
            } else {
                let target = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
                ts.update(&target, |t| t.cancel()).unwrap();
            }
        }
        if self.race == 4 && self.results.lock().unwrap().len() == 1 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(Step::Continue(cp("done")))
    }
}
fn registry(path: &std::path::Path, race: u8, results: Arc<Mutex<Vec<SignalResult>>>) -> Registry {
    let mut r = Registry::default();
    r.register(
        "sender",
        true,
        Send {
            path: path.into(),
            race,
            results,
        },
    );
    r.register("receiver", true, |_: &Task| {
        Ok(Step::Succeed(json!("woken")))
    });
    r
}
fn setup(path: &std::path::Path, deadline: Option<i64>) {
    let db = Store::open(path).unwrap();
    let ts = TaskStore::new(&db);
    let t = task(2, "receiver");
    ts.save(&[Save::Create(&t)]).unwrap();
    ts.reserve(&ts.load(t.id()).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(t.id()).unwrap().unwrap(), |t| {
        t.wait(WaitFor::TypedSleep {
            until_millis: deadline,
            signals: vec![SignalToken::new("answer", 1).unwrap().key("a").unwrap()],
        })
    })
    .unwrap();
}
#[test]
fn commit_reopen_and_wake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, Some(100));
    let results = Arc::new(Mutex::new(vec![]));
    let mut s = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    assert!(s.tick(99).unwrap());
    drop(s);
    let db = Store::open(&path).unwrap();
    assert_eq!(*results.lock().unwrap(), vec![SignalResult::Delivered]);
    assert_eq!(
        Documents::new(&db).get(&addr()).unwrap().unwrap().value["done"],
        true
    );
    assert_eq!(
        TaskStore::new(&db)
            .load(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .task
            .checkpoint()
            .phase,
        "done"
    );
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(
                Uuid::from_u128(2),
                &SignalToken::new("answer", 1).unwrap(),
                "a"
            )
            .unwrap(),
        Some(json!(1))
    );
    drop(db);
    let mut s = Scheduler::open(&path, registry(&path, 0, results)).unwrap();
    for _ in 0..5 {
        s.tick(99).unwrap();
        if matches!(
            s.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ) {
            return;
        }
    }
    panic!("receiver did not wake");
}
#[test]
fn competing_delivery_rolls_back_then_replays_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, Some(100));
    let results = Arc::new(Mutex::new(vec![]));
    let mut s = Scheduler::open(&path, registry(&path, 1, results.clone())).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    s.tick(99).unwrap();
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    assert_eq!(
        TaskStore::new(&db)
            .load(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .task
            .checkpoint()
            .phase,
        "start"
    );
    drop(db);
    s.tick(99).unwrap();
    assert_eq!(
        *results.lock().unwrap(),
        vec![SignalResult::Delivered, SignalResult::Duplicate]
    );
    let db = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(
                Uuid::from_u128(2),
                &SignalToken::new("answer", 1).unwrap(),
                "a"
            )
            .unwrap(),
        Some(json!(2))
    );
}

#[test]
fn closing_target_rolls_back_then_replays_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, Some(100));
    let results = Arc::new(Mutex::new(vec![]));
    let mut s = Scheduler::open(&path, registry(&path, 2, results.clone())).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    s.tick(99).unwrap();
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    assert_eq!(
        TaskStore::new(&db)
            .load(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .task
            .checkpoint()
            .phase,
        "start"
    );
    drop(db);
    // Drive the sender directly before abort cleanup can consume another tick.
    for _ in 0..4 {
        s.tick(99).unwrap();
        if results.lock().unwrap().len() > 1 {
            break;
        }
    }
    assert_eq!(
        *results.lock().unwrap(),
        vec![SignalResult::Delivered, SignalResult::Closed]
    );
}

#[test]
fn expired_delivery_commits_closed_with_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, Some(100));
    let results = Arc::new(Mutex::new(vec![]));
    let mut s = Scheduler::open(&path, registry(&path, 3, results.clone())).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    s.tick(99).unwrap();
    drop(s);
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    assert_eq!(*results.lock().unwrap(), vec![SignalResult::Closed]);
    assert_eq!(
        ts.read_typed_signal_state(
            Uuid::from_u128(2),
            &SignalToken::new("answer", 1).unwrap(),
            "a"
        )
        .unwrap(),
        Some(pi_rs_durable::task_store::SignalState::Expired)
    );
    assert_eq!(
        ts.load(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .task
            .checkpoint()
            .phase,
        "done"
    );
    assert!(Documents::new(&db).get(&addr()).unwrap().is_some());
}

#[test]
fn driver_runs_receiver_after_atomic_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, None);
    let results = Arc::new(Mutex::new(vec![]));
    let s = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    let driver = pi_rs_durable::task_driver::Driver::start(s);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if matches!(
            driver.get(Uuid::from_u128(2)).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "driver did not wake receiver"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(*results.lock().unwrap(), vec![SignalResult::Delivered]);
}

#[test]
fn deadline_crossing_rolls_back_entire_attempt_and_replays_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, Some(100));
    let results = Arc::new(Mutex::new(vec![]));
    let mut s = Scheduler::open(&path, registry(&path, 4, results.clone())).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    s.tick(99).unwrap();
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    assert_eq!(
        ts.load(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .task
            .checkpoint()
            .phase,
        "start"
    );
    let token = SignalToken::new("answer", 1).unwrap();
    assert_eq!(
        ts.read_typed_signal_state(Uuid::from_u128(2), &token, "a")
            .unwrap(),
        None
    );
    drop(db);
    s.tick(99).unwrap();
    assert_eq!(
        *results.lock().unwrap(),
        vec![SignalResult::Delivered, SignalResult::Closed]
    );
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    assert_eq!(
        ts.read_typed_signal_state(Uuid::from_u128(2), &token, "a")
            .unwrap(),
        Some(pi_rs_durable::task_store::SignalState::Expired)
    );
    assert!(Documents::new(&db).get(&addr()).unwrap().is_some());
    assert_eq!(
        ts.deliver_signal_at(Uuid::from_u128(2), &token, "a", json!(3), 101)
            .unwrap(),
        SignalResult::Closed
    );
}
