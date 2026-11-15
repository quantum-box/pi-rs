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
    setup(&path, None);
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

struct MultiSend {
    path: std::path::PathBuf,
    mode: u8,
    attempts: Arc<Mutex<Vec<Vec<SignalResult>>>>,
}
impl Handler for MultiSend {
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
        let attempt = self.attempts.lock().unwrap().len();
        let now = if self.mode == 2 && attempt > 0 {
            100
        } else {
            99
        };
        let mut results = Vec::new();
        for (id, key) in [(2, "a"), (2, "b"), (3, "a")] {
            results.push(
                docs.signal_at(Uuid::from_u128(id), &token, key, json!(key), now)
                    .unwrap(),
            );
            if self.mode == 6 && attempt == 0 && id == 2 && key == "a" {
                let db = Store::open(&self.path).unwrap();
                assert_eq!(
                    TaskStore::new(&db)
                        .deliver_signal_at(Uuid::from_u128(2), &token, "b", json!("rival"), 99)
                        .unwrap(),
                    SignalResult::Delivered
                );
            }
        }
        let repeated = docs.signal_at(Uuid::from_u128(2), &token, "a", json!(4), now);
        if matches!(
            results[0],
            SignalResult::Delivered | SignalResult::Duplicate
        ) {
            assert_eq!(repeated.unwrap(), SignalResult::Duplicate);
        } else {
            assert_eq!(repeated.unwrap(), results[0]);
        }
        self.attempts.lock().unwrap().push(results);
        docs.get(&addr()).unwrap();
        docs.put(&addr(), json!({"done":true}).as_object().unwrap().clone())
            .unwrap();
        if self.mode == 1 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            assert_eq!(
                TaskStore::new(&db)
                    .deliver_signal_at(Uuid::from_u128(2), &token, "b", json!("rival"), 99)
                    .unwrap(),
                SignalResult::Delivered
            );
        }
        if self.mode == 5 && attempt == 0 {
            // Simulate an abort intent committed by the driver during the invocation.
            let db = Store::open(&self.path).unwrap();
            let mut task_key = b"durable/task/".to_vec();
            task_key.extend_from_slice(Uuid::from_u128(1).as_bytes());
            let record = db.get(&task_key).unwrap().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
            value["abort_requested"] = json!(true);
            db.commit(
                &[durable_store::Check {
                    key: task_key.clone(),
                    expected: durable_store::Expected::Version(record.version),
                }],
                &[durable_store::Mutation::Put {
                    key: task_key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        }
        if self.mode == 4 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            let ts = TaskStore::new(&db);
            let sender = ts.load(Uuid::from_u128(1)).unwrap().unwrap();
            ts.update(&sender, |task| task.cancel()).unwrap();
        }
        if self.mode == 2 && attempt == 0 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        if self.mode == 3 {
            return Ok(Step::Fail("abort staged writes".into()));
        }
        Ok(Step::Continue(cp("done")))
    }
}
fn multi_setup(
    path: &std::path::Path,
    deadline: Option<i64>,
    mode: u8,
    attempts: Arc<Mutex<Vec<Vec<SignalResult>>>>,
) -> Scheduler {
    setup(path, deadline);
    let db = Store::open(path).unwrap();
    let target = task(3, "receiver");
    let ts = TaskStore::new(&db);
    ts.save(&[Save::Create(&target)]).unwrap();
    ts.reserve(&ts.load(target.id()).unwrap().unwrap()).unwrap();
    ts.update(&ts.load(target.id()).unwrap().unwrap(), |t| {
        t.wait(WaitFor::TypedSleep {
            until_millis: None,
            signals: vec![SignalToken::new("answer", 1).unwrap().key("a").unwrap()],
        })
    })
    .unwrap();
    drop(db);
    let mut registry = Registry::default();
    registry.register(
        "sender",
        true,
        MultiSend {
            path: path.into(),
            mode,
            attempts,
        },
    );
    registry.register("receiver", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let s = Scheduler::open(path, registry).unwrap();
    s.submit(&task(1, "sender")).unwrap();
    s
}
fn signal_value(path: &std::path::Path, id: u128, key: &str) -> Option<serde_json::Value> {
    let db = Store::open(path).unwrap();
    TaskStore::new(&db)
        .read_typed_signal(
            Uuid::from_u128(id),
            &SignalToken::new("answer", 1).unwrap(),
            key,
        )
        .unwrap()
}
#[test]
fn multiple_targets_and_keys_commit_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let attempts = Arc::new(Mutex::new(vec![]));
    let mut s = multi_setup(&path, None, 0, attempts.clone());
    s.tick(99).unwrap();
    drop(s);
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![vec![SignalResult::Delivered; 3]]
    );
    for (id, key) in [(2, "a"), (2, "b"), (3, "a")] {
        assert_eq!(signal_value(&path, id, key), Some(json!(key)));
    }
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_some());
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
    drop(db);
    let mut registry = Registry::default();
    registry.register("receiver", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let mut reopened = Scheduler::open(&path, registry).unwrap();
    for _ in 0..8 {
        reopened.tick(99).unwrap();
        if [2, 3].iter().all(|id| {
            matches!(
                reopened.get(Uuid::from_u128(*id)).unwrap().unwrap().state(),
                TaskState::Succeeded(_)
            )
        }) {
            return;
        }
    }
    panic!("both receivers should wake after reopen");
}
#[test]
fn multi_signal_conflict_rolls_back_everything_then_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let attempts = Arc::new(Mutex::new(vec![]));
    let mut s = multi_setup(&path, None, 1, attempts.clone());
    s.tick(99).unwrap();
    assert_eq!(signal_value(&path, 2, "a"), None);
    assert_eq!(signal_value(&path, 3, "a"), None);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&addr())
            .unwrap()
            .is_none()
    );
    s.tick(99).unwrap();
    assert_eq!(
        attempts.lock().unwrap()[1],
        vec![
            SignalResult::Delivered,
            SignalResult::Duplicate,
            SignalResult::Delivered
        ]
    );
    assert_eq!(signal_value(&path, 2, "b"), Some(json!("rival")));
    assert_eq!(signal_value(&path, 3, "a"), Some(json!("a")));
}
#[test]
fn multi_signal_expiry_rolls_back_other_targets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let attempts = Arc::new(Mutex::new(vec![]));
    let mut s = multi_setup(&path, Some(100), 2, attempts.clone());
    s.tick(99).unwrap();
    assert_eq!(signal_value(&path, 2, "b"), None);
    assert_eq!(signal_value(&path, 3, "a"), None);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&addr())
            .unwrap()
            .is_none()
    );
    s.tick(99).unwrap();
    assert_eq!(
        attempts.lock().unwrap()[1],
        vec![
            SignalResult::Closed,
            SignalResult::Delivered,
            SignalResult::Delivered
        ]
    );
}
#[test]
fn failed_step_discards_all_staged_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut s = multi_setup(&path, None, 3, Arc::new(Mutex::new(vec![])));
    s.tick(99).unwrap();
    drop(s);
    for (id, key) in [(2, "a"), (2, "b"), (3, "a")] {
        assert_eq!(signal_value(&path, id, key), None);
    }
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&addr())
            .unwrap()
            .is_none()
    );
}

#[test]
fn concurrent_sender_cancellation_discards_all_staged_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut s = multi_setup(&path, None, 4, Arc::new(Mutex::new(vec![])));
    assert!(s.tick(99).is_err());
    drop(s);
    for (id, key) in [(2, "a"), (2, "b"), (3, "a")] {
        assert_eq!(signal_value(&path, id, key), None);
    }
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    assert_ne!(
        TaskStore::new(&db)
            .load(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .task
            .checkpoint()
            .phase,
        "done"
    );
}

#[test]
fn concurrent_abort_intent_discards_all_staged_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut s = multi_setup(&path, None, 5, Arc::new(Mutex::new(vec![])));
    s.tick(99).unwrap();
    drop(s);
    for (id, key) in [(2, "a"), (2, "b"), (3, "a")] {
        assert_eq!(signal_value(&path, id, key), None);
    }
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    let sender = TaskStore::new(&db)
        .load(Uuid::from_u128(1))
        .unwrap()
        .unwrap()
        .task;
    assert!(sender.abort_requested());
    assert_eq!(sender.checkpoint().phase, "start");
}

#[test]
fn external_delivery_between_staging_calls_conflicts_and_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let attempts = Arc::new(Mutex::new(vec![]));
    let mut s = multi_setup(&path, None, 6, attempts.clone());
    s.tick(99).unwrap();
    assert_eq!(signal_value(&path, 2, "a"), None);
    assert_eq!(signal_value(&path, 2, "b"), Some(json!("rival")));
    assert_eq!(signal_value(&path, 3, "a"), None);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&addr())
            .unwrap()
            .is_none()
    );
    s.tick(99).unwrap();
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![
            vec![
                SignalResult::Delivered,
                SignalResult::Duplicate,
                SignalResult::Delivered
            ],
            vec![
                SignalResult::Delivered,
                SignalResult::Duplicate,
                SignalResult::Delivered
            ],
        ]
    );
    assert_eq!(signal_value(&path, 2, "a"), Some(json!("a")));
    assert_eq!(signal_value(&path, 3, "a"), Some(json!("a")));
}

#[test]
fn driver_runs_all_receivers_after_multiple_atomic_deliveries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let attempts = Arc::new(Mutex::new(vec![]));
    let s = multi_setup(&path, None, 0, attempts.clone());
    let driver = pi_rs_durable::task_driver::Driver::start(s);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if [2, 3].iter().all(|id| {
            matches!(
                driver.get(Uuid::from_u128(*id)).unwrap().unwrap().state(),
                TaskState::Succeeded(_)
            )
        }) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "driver did not run both receivers"
        );
        std::thread::yield_now();
    }
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![vec![SignalResult::Delivered; 3]]
    );
    for (id, key) in [(2, "a"), (2, "b"), (3, "a")] {
        assert_eq!(signal_value(&path, id, key), Some(json!(key)));
    }
}

// The parent kills this process only after tick has committed the sender's
// checkpoint, document and signal, but before any receiver poll can occur.
#[cfg(unix)]
#[test]
fn signal_commit_process_child() {
    use std::io::Write;
    let Some(path) = std::env::var_os("PI_SIGNAL_DEATH_DB") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    setup(&path, Some(60_000));
    let results = Arc::new(Mutex::new(vec![]));
    let mut scheduler = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    scheduler.submit(&task(1, "sender")).unwrap();
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(*results.lock().unwrap(), vec![SignalResult::Delivered]);
    assert_eq!(
        scheduler
            .get(Uuid::from_u128(1))
            .unwrap()
            .unwrap()
            .checkpoint()
            .phase,
        "done",
        "staged delivery did not commit"
    );
    println!("SIGNAL_COMMITTED_BEFORE_WAIT_POLL");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn committed_typed_signal_survives_process_death_before_wait_poll() {
    use pi_rs_durable::{
        task::TypedWake,
        task_store::{SignalState, WakeResult},
    };
    use std::{
        io::{BufRead, BufReader},
        os::unix::process::ExitStatusExt,
        process::{Child, Command, Stdio},
        sync::mpsc,
        time::Duration,
    };

    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "signal_commit_process_child", "--nocapture"])
            .env("PI_SIGNAL_DEATH_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("SIGNAL_COMMITTED_BEFORE_WAIT_POLL") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("child did not commit signal");
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));

    let token = SignalToken::new("answer", 1).unwrap();
    let receiver = Uuid::from_u128(2);
    {
        let db = Store::open(&path).unwrap();
        let ts = TaskStore::new(&db);
        assert_eq!(
            ts.load(Uuid::from_u128(1))
                .unwrap()
                .unwrap()
                .task
                .checkpoint()
                .phase,
            "done"
        );
        assert_eq!(
            Documents::new(&db).get(&addr()).unwrap().unwrap().value["done"],
            true
        );
        let waiting = ts.load(receiver).unwrap().unwrap();
        assert!(matches!(
            waiting.task.state(),
            TaskState::Waiting(WaitFor::TypedSleep { .. })
        ));
        assert_eq!(
            ts.read_typed_signal_state(receiver, &token, "a").unwrap(),
            Some(SignalState::Value(json!(1)))
        );
        assert_eq!(
            ts.deliver_signal_at(receiver, &token, "a", json!(1), 60_000)
                .unwrap(),
            SignalResult::Duplicate
        );
        // A value committed before the deadline wins even when polling after it.
        assert!(matches!(
            ts.poll_wait(&ts.load(receiver).unwrap().unwrap(), 60_000)
                .unwrap(),
            WakeResult::Woken { signal: Some(_) }
        ));
    }
    let db = Store::open(&path).unwrap();
    let ts = TaskStore::new(&db);
    let recovered = ts.load(receiver).unwrap().unwrap();
    assert_eq!(
        recovered.task.typed_wake(),
        Some(&TypedWake::Signal(token.key("a").unwrap()))
    );
    assert_eq!(
        ts.read_typed_signal_state(receiver, &token, "a").unwrap(),
        Some(SignalState::Value(json!(1)))
    );
    assert_eq!(
        ts.deliver_signal_at(receiver, &token, "a", json!(2), 60_001)
            .unwrap(),
        SignalResult::Duplicate
    );
}

struct RepeatedTypedSignal {
    results: Arc<Mutex<Vec<SignalResult>>>,
}
impl Handler for RepeatedTypedSignal {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
        let target = Uuid::from_u128(2);
        let results = vec![
            docs.signal_at(target, &token, "a", json!(1), 99).unwrap(),
            docs.signal_at(target, &token, "a", json!("invalid"), 99)
                .unwrap(),
            docs.signal_at(target, &token, "a", json!(2), 99).unwrap(),
            docs.signal_at(target, &token, "b", json!("invalid"), 99)
                .unwrap(),
            docs.signal_at(target, &token, "b", json!(3), 99).unwrap(),
            docs.signal_at(target, &token, "b", json!(4), 99).unwrap(),
        ];
        *self.results.lock().unwrap() = results;
        docs.get(&addr()).unwrap();
        docs.put(
            &addr(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .unwrap();
        Ok(Step::Continue(cp("done")))
    }
}

#[test]
fn repeated_staged_signal_validates_each_value_and_commits_one_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, None);
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry.register(
        "sender",
        true,
        RepeatedTypedSignal {
            results: results.clone(),
        },
    );
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&task(1, "sender")).unwrap();
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(
        *results.lock().unwrap(),
        vec![
            SignalResult::Delivered,
            SignalResult::Invalid,
            SignalResult::Duplicate,
            SignalResult::Invalid,
            SignalResult::Delivered,
            SignalResult::Duplicate
        ]
    );
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(Uuid::from_u128(2), &token, "a")
            .unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(Uuid::from_u128(2), &token, "b")
            .unwrap(),
        Some(json!(3))
    );
    assert_eq!(
        Documents::new(&db).get(&addr()).unwrap().unwrap().value["committed"],
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
}

struct RepeatClosedOrMissing {
    path: std::path::PathBuf,
    mode: u8,
    attempts: Arc<Mutex<Vec<Vec<SignalResult>>>>,
}
impl Handler for RepeatClosedOrMissing {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        current_task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if current_task.checkpoint().phase == "done" {
            return Ok(Step::Succeed(json!(null)));
        }
        let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
        let target = Uuid::from_u128(if self.mode == 2 || self.mode == 3 {
            4
        } else {
            2
        });
        let now = if self.mode == 0 || self.mode == 4 {
            100
        } else {
            99
        };
        let first = docs.signal_at(target, &token, "a", json!(1), now).unwrap();
        let attempt = self.attempts.lock().unwrap().len();
        if self.mode == 3 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            let target = task(4, "receiver");
            TaskStore::new(&db).save(&[Save::Create(&target)]).unwrap();
        }
        if self.mode == 4 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            assert_eq!(
                TaskStore::new(&db)
                    .deliver_signal_at(target, &token, "a", json!(7), 99)
                    .unwrap(),
                SignalResult::Delivered
            );
        }
        let second = docs
            .signal_at(target, &token, "a", json!("bad"), now)
            .unwrap();
        let third = docs.signal_at(target, &token, "a", json!(2), now).unwrap();
        self.attempts
            .lock()
            .unwrap()
            .push(vec![first, second, third]);
        docs.get(&addr()).unwrap();
        docs.put(
            &addr(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .unwrap();
        Ok(Step::Continue(cp("done")))
    }
}

#[test]
fn repeated_closed_and_missing_obey_validation_order_and_reopen() {
    use pi_rs_durable::task_store::SignalState;
    for mode in 0..=2 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        setup(&path, if mode == 0 { Some(100) } else { None });
        if mode == 1 {
            let db = Store::open(&path).unwrap();
            let ts = TaskStore::new(&db);
            let target = ts.load(Uuid::from_u128(2)).unwrap().unwrap();
            ts.update(&target, |task| task.cancel()).unwrap();
        }
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry.register(
            "sender",
            true,
            RepeatClosedOrMissing {
                path: path.clone(),
                mode,
                attempts: attempts.clone(),
            },
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        scheduler.submit(&task(1, "sender")).unwrap();
        assert!(scheduler.tick(99).unwrap());
        let expected = match mode {
            0 => vec![
                SignalResult::Closed,
                SignalResult::Invalid,
                SignalResult::Closed,
            ],
            1 => vec![SignalResult::Closed; 3],
            _ => vec![SignalResult::NotFound; 3],
        };
        assert_eq!(*attempts.lock().unwrap(), vec![expected]);
        drop(scheduler);
        let db = Store::open(&path).unwrap();
        assert_eq!(
            Documents::new(&db).get(&addr()).unwrap().unwrap().value["committed"],
            true
        );
        if mode == 0 {
            let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
            assert_eq!(
                TaskStore::new(&db)
                    .read_typed_signal_state(Uuid::from_u128(2), &token, "a")
                    .unwrap(),
                Some(SignalState::Expired)
            );
        }
    }
}

#[test]
fn repeated_missing_is_fenced_against_concurrent_creation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry.register(
        "sender",
        true,
        RepeatClosedOrMissing {
            path: path.clone(),
            mode: 3,
            attempts: attempts.clone(),
        },
    );
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&task(1, "sender")).unwrap();
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(
        *attempts.lock().unwrap(),
        vec![vec![SignalResult::NotFound; 3]]
    );
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    drop(db);
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(
        attempts.lock().unwrap()[1],
        vec![
            SignalResult::Delivered,
            SignalResult::Invalid,
            SignalResult::Duplicate
        ]
    );
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(Uuid::from_u128(4), &token, "a")
            .unwrap(),
        Some(json!(1))
    );
    assert_eq!(
        Documents::new(&db).get(&addr()).unwrap().unwrap().value["committed"],
        true
    );
}

#[test]
fn repeated_expired_is_fenced_against_concurrent_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    setup(&path, Some(100));
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    registry.register(
        "sender",
        true,
        RepeatClosedOrMissing {
            path: path.clone(),
            mode: 4,
            attempts: attempts.clone(),
        },
    );
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    scheduler.submit(&task(1, "sender")).unwrap();
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(
        attempts.lock().unwrap()[0],
        vec![
            SignalResult::Closed,
            SignalResult::Invalid,
            SignalResult::Closed
        ]
    );
    let db = Store::open(&path).unwrap();
    assert!(Documents::new(&db).get(&addr()).unwrap().is_none());
    drop(db);
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(
        attempts.lock().unwrap()[1],
        vec![
            SignalResult::Duplicate,
            SignalResult::Invalid,
            SignalResult::Duplicate
        ]
    );
    drop(scheduler);
    let db = Store::open(&path).unwrap();
    let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
    assert_eq!(
        TaskStore::new(&db)
            .read_typed_signal(Uuid::from_u128(2), &token, "a")
            .unwrap(),
        Some(json!(7))
    );
    assert_eq!(
        Documents::new(&db).get(&addr()).unwrap().unwrap().value["committed"],
        true
    );
}

struct SignalAfterDocument {
    path: std::path::PathBuf,
    mode: u8,
    attempts: Arc<Mutex<Vec<Vec<SignalResult>>>>,
}
impl Handler for SignalAfterDocument {
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
        let attempt = self.attempts.lock().unwrap().len();
        let old = docs.get(&addr()).unwrap();
        let count = old
            .as_ref()
            .and_then(|d| d.value.get("count"))
            .and_then(|n| n.as_i64())
            .unwrap_or(0);
        docs.put(
            &addr(),
            json!({"count":count + 1}).as_object().unwrap().clone(),
        )
        .unwrap();
        if self.mode == 1 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            let mut tx = pi_rs_durable::document::Transaction::new();
            tx.put(
                &addr(),
                durable_store::Expected::Missing,
                json!({"count":5}).as_object().unwrap(),
            )
            .unwrap();
            tx.commit(&db).unwrap();
        }
        let token = SignalToken::new("answer", 1).unwrap();
        let target = Uuid::from_u128(2);
        let first = docs.signal_at(target, &token, "a", json!(1), 99).unwrap();
        let repeated = docs.signal_at(target, &token, "a", json!(2), 99).unwrap();
        self.attempts.lock().unwrap().push(vec![first, repeated]);
        if self.mode == 2 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            assert_eq!(
                TaskStore::new(&db)
                    .deliver_signal_at(target, &token, "a", json!(7), 99)
                    .unwrap(),
                SignalResult::Delivered
            );
        }
        if self.mode == 3 && attempt == 0 {
            let db = Store::open(&self.path).unwrap();
            let mut key = b"durable/task/".to_vec();
            key.extend_from_slice(task.id().as_bytes());
            let entry = db.get(&key).unwrap().unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            value["abort_requested"] = json!(true);
            db.commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        }
        if self.mode == 4 {
            return Ok(Step::Fail("discard".into()));
        }
        Ok(Step::Continue(cp("done")))
    }
}

#[test]
fn document_write_before_signal_is_atomic_and_replays_conflicts() {
    for mode in 0..=4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        setup(&path, None);
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let mut registry = Registry::default();
        registry.register(
            "sender",
            true,
            SignalAfterDocument {
                path: path.clone(),
                mode,
                attempts: attempts.clone(),
            },
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        scheduler.submit(&task(1, "sender")).unwrap();
        assert!(scheduler.tick(99).unwrap());
        assert_eq!(
            attempts.lock().unwrap()[0],
            vec![SignalResult::Delivered, SignalResult::Duplicate]
        );
        if mode == 1 || mode == 2 {
            let db = Store::open(&path).unwrap();
            let doc = Documents::new(&db).get(&addr()).unwrap();
            if mode == 1 {
                assert_eq!(doc.unwrap().value["count"], 5);
            } else {
                assert!(doc.is_none());
            }
            assert_eq!(
                signal_value(&path, 2, "a"),
                if mode == 2 { Some(json!(7)) } else { None }
            );
            drop(db);
            for _ in 0..4 {
                if attempts.lock().unwrap().len() >= 2 {
                    break;
                }
                scheduler.tick(99).unwrap();
            }
            assert_eq!(
                attempts.lock().unwrap()[1],
                vec![
                    if mode == 2 {
                        SignalResult::Duplicate
                    } else {
                        SignalResult::Delivered
                    },
                    SignalResult::Duplicate
                ]
            );
        }
        drop(scheduler);
        let db = Store::open(&path).unwrap();
        let doc = Documents::new(&db).get(&addr()).unwrap();
        if mode >= 3 {
            assert!(doc.is_none());
            assert_eq!(signal_value(&path, 2, "a"), None);
        } else {
            assert_eq!(doc.unwrap().value["count"], if mode == 1 { 6 } else { 1 });
            assert_eq!(
                signal_value(&path, 2, "a"),
                Some(if mode == 2 { json!(7) } else { json!(1) })
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
        }
    }
}
