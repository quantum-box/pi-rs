use durable_store::Store;
use pi_rs_durable::{
    document::{Address, Documents, TaskDocuments},
    task::{
        Checkpoint, Task, TaskOwner, TaskState, TypedWake, WaitFor,
        scheduler::{Handler, Registry, Scheduler, Step},
    },
    task_store::{SignalResult, SignalToken, TaskStore},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

fn cp(phase: &str) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data: json!(null),
    }
}
fn address() -> Address {
    Address {
        scope: "self-signal-test".into(),
        name: "checkpoint".into(),
        key: "".into(),
    }
}
fn task() -> Task {
    Task::new(
        Uuid::from_u128(1),
        TaskOwner::Conversation(Uuid::from_u128(90)),
        cp("start"),
        3,
    )
    .unwrap()
    .registered("self-signal".into(), true)
}
struct SelfSignal {
    path: std::path::PathBuf,
    mode: u8,
    results: Arc<Mutex<Vec<Vec<SignalResult>>>>,
}
impl Handler for SelfSignal {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
        if task.checkpoint().phase == "waiting" {
            assert_eq!(
                task.typed_wake(),
                Some(&TypedWake::Signal(token.key("a").unwrap()))
            );
            return Ok(Step::Succeed(json!("woken")));
        }
        let attempt = self.results.lock().unwrap().len();
        let first = docs
            .signal_at(task.id(), &token, "a", json!(1), 99)
            .unwrap();
        let invalid = docs
            .signal_at(task.id(), &token, "a", json!("bad"), 99)
            .unwrap();
        let repeat = docs
            .signal_at(task.id(), &token, "a", json!(2), 99)
            .unwrap();
        self.results
            .lock()
            .unwrap()
            .push(vec![first, invalid, repeat]);
        if std::env::var_os("PI_SELF_SIGNAL_PAUSE_STAGED").is_some() {
            use std::io::Write;
            println!("SELF_SIGNAL_STAGED_ONLY");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }
        docs.get(&address()).unwrap();
        docs.put(
            &address(),
            json!({"saved":true}).as_object().unwrap().clone(),
        )
        .unwrap();
        if attempt == 0 && self.mode != 0 {
            let db = Store::open(&self.path).unwrap();
            let ts = TaskStore::new(&db);
            match self.mode {
                1 | 4 => {
                    let key = if self.mode == 1 { "other" } else { "a" };
                    assert_eq!(
                        ts.deliver_signal_at(task.id(), &token, key, json!(7), 99)
                            .unwrap(),
                        SignalResult::Delivered
                    );
                }
                2 => {
                    let mut key = b"durable/task/".to_vec();
                    key.extend_from_slice(task.id().as_bytes());
                    let entry = db.get(&key).unwrap().unwrap();
                    let mut stored: serde_json::Value =
                        serde_json::from_slice(&entry.value).unwrap();
                    stored["abort_requested"] = json!(true);
                    db.commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: serde_json::to_vec(&stored).unwrap(),
                        }],
                    )
                    .unwrap();
                }
                3 => return Ok(Step::Fail("discard".into())),
                _ => unreachable!(),
            }
        }
        Ok(Step::Wait {
            checkpoint: cp("waiting"),
            condition: WaitFor::TypedSleep {
                until_millis: Some(100),
                signals: vec![token.key("a").unwrap()],
            },
            children: vec![],
        })
    }
}
fn registry(
    path: &std::path::Path,
    mode: u8,
    results: Arc<Mutex<Vec<Vec<SignalResult>>>>,
) -> Registry {
    let mut registry = Registry::default();
    registry.register(
        "self-signal",
        true,
        SelfSignal {
            path: path.into(),
            mode,
            results,
        },
    );
    registry
}
fn read_signal(path: &std::path::Path, key: &str) -> Option<serde_json::Value> {
    let db = Store::open(path).unwrap();
    let token = SignalToken::with_schema("answer", 1, json!({"type":"integer"})).unwrap();
    TaskStore::new(&db)
        .read_typed_signal(Uuid::from_u128(1), &token, key)
        .unwrap()
}

#[test]
fn self_delivery_wait_reopens_and_wins_over_expired_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut scheduler = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    scheduler.submit(&task()).unwrap();
    assert!(scheduler.tick(99).unwrap());
    assert_eq!(
        results.lock().unwrap()[0],
        vec![
            SignalResult::Delivered,
            SignalResult::Invalid,
            SignalResult::Duplicate
        ]
    );
    assert_eq!(read_signal(&path, "a"), Some(json!(1)));
    assert!(matches!(
        scheduler.get(task().id()).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    drop(scheduler);
    let mut scheduler = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    for _ in 0..4 {
        scheduler.tick(101).unwrap();
        if matches!(
            scheduler.get(task().id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ) {
            break;
        }
    }
    assert!(matches!(
        scheduler.get(task().id()).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    assert_eq!(results.lock().unwrap().len(), 1);
}

#[test]
fn self_delivery_conflicts_retry_without_losing_external_signals() {
    for mode in [1, 4] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut scheduler = Scheduler::open(&path, registry(&path, mode, results.clone())).unwrap();
        scheduler.submit(&task()).unwrap();
        assert!(scheduler.tick(99).unwrap());
        assert!(
            Documents::new(&Store::open(&path).unwrap())
                .get(&address())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_signal(&path, "a"),
            if mode == 4 { Some(json!(7)) } else { None }
        );
        assert!(scheduler.tick(99).unwrap());
        assert_eq!(
            results.lock().unwrap()[1],
            vec![
                if mode == 4 {
                    SignalResult::Duplicate
                } else {
                    SignalResult::Delivered
                },
                SignalResult::Invalid,
                SignalResult::Duplicate
            ]
        );
        assert_eq!(
            read_signal(&path, "a"),
            Some(if mode == 4 { json!(7) } else { json!(1) })
        );
        drop(scheduler);
        assert_eq!(
            Documents::new(&Store::open(&path).unwrap())
                .get(&address())
                .unwrap()
                .unwrap()
                .value["saved"],
            true
        );
        if mode == 1 {
            assert_eq!(read_signal(&path, "other"), Some(json!(7)));
        }
    }
}

#[test]
fn abort_and_failed_step_discard_self_signal_and_document() {
    for mode in [2, 3] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut scheduler = Scheduler::open(
            &path,
            registry(&path, mode, Arc::new(Mutex::new(Vec::new()))),
        )
        .unwrap();
        scheduler.submit(&task()).unwrap();
        assert!(scheduler.tick(99).unwrap());
        drop(scheduler);
        let db = Store::open(&path).unwrap();
        assert!(Documents::new(&db).get(&address()).unwrap().is_none());
        assert_eq!(read_signal(&path, "a"), None);
    }
}

#[cfg(unix)]
#[test]
fn self_signal_process_child() {
    use std::io::Write;
    let Some(path) = std::env::var_os("PI_SELF_SIGNAL_DB") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut scheduler = Scheduler::open(&path, registry(&path, 0, results)).unwrap();
    scheduler.submit(&task()).unwrap();
    assert!(scheduler.tick(99).unwrap());
    assert!(matches!(
        scheduler.get(task().id()).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    println!("SELF_SIGNAL_WAIT_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn self_signal_and_wait_survive_process_kill() {
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
            .args(["--exact", "self_signal_process_child", "--nocapture"])
            .env("PI_SELF_SIGNAL_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, recv) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("SELF_SIGNAL_WAIT_COMMITTED") {
                let _ = send.send(());
                break;
            }
        }
    });
    recv.recv_timeout(Duration::from_secs(20))
        .expect("self signal did not commit");
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    assert_eq!(read_signal(&path, "a"), Some(json!(1)));
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut scheduler = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    for _ in 0..4 {
        scheduler.tick(101).unwrap();
        if matches!(
            scheduler.get(task().id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ) {
            break;
        }
    }
    assert!(matches!(
        scheduler.get(task().id()).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    assert!(results.lock().unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn staged_self_signal_is_discarded_on_kill_and_replayed() {
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
            .args(["--exact", "self_signal_process_child", "--nocapture"])
            .env("PI_SELF_SIGNAL_DB", &path)
            .env("PI_SELF_SIGNAL_PAUSE_STAGED", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, recv) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("SELF_SIGNAL_STAGED_ONLY") {
                let _ = send.send(());
                break;
            }
        }
    });
    recv.recv_timeout(Duration::from_secs(20))
        .expect("self signal was not staged");
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    assert_eq!(read_signal(&path, "a"), None);
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut scheduler = Scheduler::open(&path, registry(&path, 0, results.clone())).unwrap();
    for _ in 0..6 {
        scheduler.tick(99).unwrap();
        if matches!(
            scheduler.get(task().id()).unwrap().unwrap().state(),
            TaskState::Waiting(_)
        ) {
            break;
        }
    }
    assert!(matches!(
        scheduler.get(task().id()).unwrap().unwrap().state(),
        TaskState::Waiting(_)
    ));
    assert_eq!(
        results.lock().unwrap()[0],
        vec![
            SignalResult::Delivered,
            SignalResult::Invalid,
            SignalResult::Duplicate
        ]
    );
    assert_eq!(read_signal(&path, "a"), Some(json!(1)));
    for _ in 0..4 {
        scheduler.tick(101).unwrap();
        if matches!(
            scheduler.get(task().id()).unwrap().unwrap().state(),
            TaskState::Succeeded(_)
        ) {
            break;
        }
    }
    assert!(matches!(
        scheduler.get(task().id()).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
}
