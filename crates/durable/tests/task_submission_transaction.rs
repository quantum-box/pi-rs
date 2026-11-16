use async_trait::async_trait;
use durable_store::Store;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error as ModelError, Model, ModelEvent, ModelRequest, ModelStream,
};
use pi_rs_durable::{
    document::{Address, Documents, TaskDocuments},
    recovery::UncertainPolicy,
    runtime::{
        AdmissionOptions, Config, Harness, Options, Status, SubmissionNotify,
        submission_settled_signal,
    },
    task::{
        Checkpoint, Task, TaskOwner, TaskState, WaitFor,
        scheduler::{Handler, Registry, Step},
    },
    task_store::TaskStore,
};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
struct Answer;
#[async_trait]
impl Model for Answer {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Answer),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn address() -> Address {
    Address {
        scope: "test".into(),
        name: "submission-effect".into(),
        key: "".into(),
    }
}
fn task(conversation: Uuid, kind: &str) -> Task {
    Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered(kind.into(), true)
}
fn wait_task(path: &std::path::Path, id: Uuid, predicate: impl Fn(&TaskState) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let store = Store::open(path).unwrap();
        let Some(state) = TaskStore::new(&store).load(id).unwrap() else {
            assert!(Instant::now() < deadline, "task was not created");
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        if predicate(state.task.state()) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "task remained {:?}",
            state.task.state()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
struct Stage {
    write: bool,
    fail: bool,
}
impl Handler for Stage {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        let notify = Some(SubmissionNotify {
            task_id: task.id(),
            key: "settled".into(),
        });
        if self.write {
            docs.submit_write(
                "request".into(),
                "app.note".into(),
                Some(json!({"v":1})),
                notify,
            )
            .map_err(|e| e.to_string())?;
        } else {
            docs.submit_input_with_options(
                "request".into(),
                "followup".into(),
                AdmissionOptions::default(),
                notify,
            )
            .map_err(|e| e.to_string())?;
        }
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.fail {
            return Ok(Step::Fail("discard".into()));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("settled").unwrap()],
            },
            children: vec![],
        })
    }
}
fn setup(path: &std::path::Path) -> (Harness, Uuid) {
    let mut h = Harness::open_paused(path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    (h, id)
}

struct UnkeyedBatch {
    fail: bool,
    notify: bool,
}
impl Handler for UnkeyedBatch {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        for _ in 0..2 {
            docs.submit_write_without_request_id("app.unkeyed".into(), Some(json!(1)), None)
                .map_err(|e| e.to_string())?;
        }
        let notify = self.notify.then(|| SubmissionNotify {
            task_id: task.id(),
            key: "continue".into(),
        });
        docs.submit_continue_without_request_id(notify)
            .map_err(|e| e.to_string())?;
        assert!(docs.submit_continue_without_request_id(None).is_err());
        for _ in 0..2 {
            docs.submit_input_without_request_id(
                "follow up".into(),
                AdmissionOptions::default(),
                None,
            )
            .map_err(|e| e.to_string())?;
        }
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"unkeyed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.fail {
            return Ok(Step::Fail("rollback".into()));
        }
        if !self.notify {
            return Ok(Step::Succeed(json!(null)));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("continue").unwrap()],
            },
            children: vec![],
        })
    }
}

#[tokio::test]
async fn unkeyed_task_batch_is_distinct_and_notifies_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let parent = task(id, "hold");
    let child = descendant(parent.id(), "unkeyed");
    let mut registry = Registry::default();
    registry.register(
        "hold",
        true,
        Hold(std::collections::HashMap::from([(
            parent.id(),
            vec![child.clone()],
        )])),
    );
    registry.register(
        "unkeyed",
        true,
        UnkeyedBatch {
            fail: false,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&parent).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Waiting(_)));
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 3);
    assert!(state.submissions.iter().all(|s| s.request_id.is_none()));
    assert_ne!(state.submissions[1].id, state.submissions[2].id);
    assert_eq!(state.application_writes.len(), 2);
    assert!(
        state
            .application_writes
            .iter()
            .all(|w| w.request_id.is_none())
    );
    assert_ne!(
        state.application_writes[0].id,
        state.application_writes[1].id
    );
    let store = Store::open(&path).unwrap();
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "continue")
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("hold", true, Hold(std::collections::HashMap::new()));
    registry.register(
        "unkeyed",
        true,
        UnkeyedBatch {
            fail: false,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
    let signal = TaskStore::new(&store)
        .read_typed_signal(child.id(), &submission_settled_signal(), "continue")
        .unwrap()
        .unwrap();
    assert_eq!(signal["id"], state.submissions[0].id.to_string());
    assert_eq!(signal["reason"], "nothing_to_answer");
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 3);
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
}

#[test]
fn failed_unkeyed_batch_discards_all_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "unkeyed",
        true,
        UnkeyedBatch {
            fail: true,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "unkeyed");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Failed(_)));
    let state = h.conversation(id).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "continue")
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}

struct UnkeyedRace {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Handler for UnkeyedRace {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let result = UnkeyedBatch {
            fail: false,
            notify: false,
        }
        .run_with_documents(task, docs)?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.staged.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|e| e.to_string())?;
        }
        Ok(result)
    }
}
#[test]
fn unkeyed_batch_retries_after_conflict_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        UnkeyedRace {
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.submit_write(id, None, "app.concurrent".into(), None)
        .unwrap();
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    release_tx.send(()).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 3);
    assert_eq!(state.application_writes.len(), 3);
    driver.shutdown().unwrap();
}

#[test]
fn unkeyed_batch_is_discarded_on_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        UnkeyedRace {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    driver.request_abort(task.id()).unwrap();
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    let state = h.conversation(id).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[cfg(unix)]
struct KillUnkeyed;
#[cfg(unix)]
impl Handler for KillUnkeyed {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        UnkeyedBatch {
            fail: false,
            notify: false,
        }
        .run_with_documents(task, docs)?;
        println!("UNKEYED_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(unix)]
#[test]
fn unkeyed_kill_worker() {
    let Ok(path) = std::env::var("PI_UNKEYED_KILL_DB") else {
        return;
    };
    let id = Uuid::parse_str(&std::env::var("PI_UNKEYED_KILL_CONVERSATION").unwrap()).unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("unkeyed", true, KillUnkeyed);
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::from_u128(8847),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("unkeyed".into(), true);
    driver.submit(&task).unwrap();
    loop {
        std::thread::park();
    }
}
#[cfg(unix)]
#[test]
fn killed_unkeyed_batch_recovers_once() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "unkeyed_kill_worker", "--nocapture"])
        .env("PI_UNKEYED_KILL_DB", &path)
        .env("PI_UNKEYED_KILL_CONVERSATION", id.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if line.contains("UNKEYED_STAGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let reached = rx.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    reached.expect("worker did not stage batch");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    let mut registry = Registry::default();
    registry.register(
        "unkeyed",
        true,
        UnkeyedBatch {
            fail: false,
            notify: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    wait_task(&path, Uuid::from_u128(8847), |s| {
        matches!(s, TaskState::Succeeded(_))
    });
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 3);
    assert_eq!(state.application_writes.len(), 2);
    assert!(state.submissions.iter().all(|s| s.request_id.is_none()));
    assert!(
        state
            .application_writes
            .iter()
            .all(|w| w.request_id.is_none())
    );
}

struct ContinueBatch {
    fail: bool,
    notify: bool,
}
impl Handler for ContinueBatch {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        docs.submit_write("before".into(), "app.before".into(), None, None)
            .map_err(|e| e.to_string())?;
        let notify = self.notify.then(|| SubmissionNotify {
            task_id: task.id(),
            key: "continue".into(),
        });
        docs.submit_continue("continue".into(), notify)
            .map_err(|e| e.to_string())?;
        docs.submit_continue("continue".into(), None)
            .map_err(|e| e.to_string())?;
        assert!(docs.submit_continue("other-continue".into(), None).is_err());
        assert!(
            docs.submit_input("continue".into(), "wrong type".into())
                .is_err()
        );
        docs.submit_input("after".into(), "after continue".into())
            .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"continue":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.fail {
            return Ok(Step::Fail("rollback".into()));
        }
        if !self.notify {
            return Ok(Step::Succeed(json!(null)));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("continue").unwrap()],
            },
            children: vec![],
        })
    }
}

#[tokio::test]
async fn task_continue_batch_deduplicates_orders_and_notifies_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let parent = task(id, "hold");
    let child = descendant(parent.id(), "continue");
    let mut registry = Registry::default();
    registry.register(
        "hold",
        true,
        Hold(std::collections::HashMap::from([(
            parent.id(),
            vec![child.clone()],
        )])),
    );
    registry.register(
        "continue",
        true,
        ContinueBatch {
            fail: false,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&parent).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Waiting(_)));
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.submissions[0].request_id.as_deref(), Some("continue"));
    assert_eq!(state.submissions[0].status, Status::Running);
    assert_eq!(state.submissions[1].request_id.as_deref(), Some("after"));
    assert_eq!(state.submissions[1].status, Status::Queued);
    assert_eq!(state.application_writes.len(), 1);
    let store = Store::open(&path).unwrap();
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "continue")
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("hold", true, Hold(std::collections::HashMap::new()));
    registry.register(
        "continue",
        true,
        ContinueBatch {
            fail: false,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
    let receipt = TaskStore::new(&store)
        .read_typed_signal(child.id(), &submission_settled_signal(), "continue")
        .unwrap()
        .unwrap();
    assert_eq!(receipt["id"], state.submissions[0].id.to_string());
    assert_eq!(receipt["type"], "continue");
    assert_eq!(receipt["status"], "unanswered");
    assert_eq!(receipt["reason"], "nothing_to_answer");
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 2);
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    driver.shutdown().unwrap();
}

#[test]
fn failed_task_rolls_back_continue_and_other_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "continue",
        true,
        ContinueBatch {
            fail: true,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "continue");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Failed(_)));
    let state = h.conversation(id).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "continue")
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}

struct BusyContinue;
impl Handler for BusyContinue {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        assert!(matches!(
            docs.submit_continue("busy".into(), None),
            Err(pi_rs_durable::document::Error::SessionAdmission(
                pi_rs_durable::state::AdmissionError::Busy
            ))
        ));
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn task_continue_rejects_busy_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    h.submit(id, None, "busy".into()).unwrap();
    let mut registry = Registry::default();
    registry.register("busy", true, BusyContinue);
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "busy");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 1);
    driver.shutdown().unwrap();
}

struct RacingContinue {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Handler for RacingContinue {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let result = ContinueBatch {
            fail: false,
            notify: false,
        }
        .run_with_documents(task, docs)?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.staged.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|e| e.to_string())?;
        }
        Ok(result)
    }
}
#[test]
fn task_continue_conflict_replays_entire_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        RacingContinue {
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.submit_write(id, None, "app.concurrent".into(), None)
        .unwrap();
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    release_tx.send(()).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.application_writes.len(), 2);
    driver.shutdown().unwrap();
}

#[test]
fn abort_discards_staged_task_continue_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        RacingContinue {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    driver.request_abort(task.id()).unwrap();
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    let state = h.conversation(id).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[cfg(unix)]
struct KillContinue;
#[cfg(unix)]
impl Handler for KillContinue {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        ContinueBatch {
            fail: false,
            notify: false,
        }
        .run_with_documents(task, docs)?;
        println!("CONTINUE_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(unix)]
#[test]
fn continue_kill_worker() {
    let Ok(path) = std::env::var("PI_TASK_CONTINUE_KILL_DB") else {
        return;
    };
    let id =
        Uuid::parse_str(&std::env::var("PI_TASK_CONTINUE_KILL_CONVERSATION").unwrap()).unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("continue", true, KillContinue);
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::from_u128(8846),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("continue".into(), true);
    driver.submit(&task).unwrap();
    loop {
        std::thread::park();
    }
}
#[cfg(unix)]
#[test]
fn killed_staged_continue_replays_once_after_reopen() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "continue_kill_worker", "--nocapture"])
        .env("PI_TASK_CONTINUE_KILL_DB", &path)
        .env("PI_TASK_CONTINUE_KILL_CONVERSATION", id.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if line.contains("CONTINUE_STAGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let reached = rx.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    reached.expect("worker did not stage continuation");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    let mut registry = Registry::default();
    registry.register(
        "continue",
        true,
        ContinueBatch {
            fail: false,
            notify: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    wait_task(&path, Uuid::from_u128(8846), |s| {
        matches!(s, TaskState::Succeeded(_))
    });
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.submissions[0].request_id.as_deref(), Some("continue"));
    assert_eq!(state.application_writes.len(), 1);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_some()
    );
}

struct Multiple {
    fail: bool,
    notify: bool,
}
impl Handler for Multiple {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        let destination = |key: &str| {
            self.notify.then(|| SubmissionNotify {
                task_id: task.id(),
                key: key.into(),
            })
        };
        docs.submit_input_with_options(
            "first".into(),
            "first".into(),
            AdmissionOptions::default(),
            destination("input"),
        )
        .map_err(|e| e.to_string())?;
        let reject = AdmissionOptions {
            reject_when_busy: true,
            ..Default::default()
        };
        assert!(
            docs.submit_input_with_options("reject".into(), "must reject".into(), reject, None)
                .is_err()
        );
        docs.submit_input("second".into(), "second".into())
            .map_err(|e| e.to_string())?;
        docs.submit_input("first".into(), "ignored duplicate".into())
            .map_err(|e| e.to_string())?;
        assert!(
            docs.submit_write("first".into(), "app.wrong-type".into(), None, None)
                .is_err()
        );
        docs.submit_write(
            "write".into(),
            "app.multi".into(),
            Some(json!(1)),
            destination("write"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_write("write".into(), "pi.ignored".into(), None, None)
            .map_err(|e| e.to_string())?;
        docs.submit_write(
            "write2".into(),
            "app.second".into(),
            Some(json!(2)),
            destination("write2"),
        )
        .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"batch":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.fail {
            return Ok(Step::Fail("rollback".into()));
        }
        if !self.notify {
            return Ok(Step::Succeed(json!(null)));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("input").unwrap()],
            },
            children: vec![],
        })
    }
}

#[tokio::test]
async fn multiple_mixed_admissions_deduplicate_notify_and_drive_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let parent = task(id, "hold");
    let task = descendant(parent.id(), "multiple");
    let mut registry = Registry::default();
    registry.register(
        "hold",
        true,
        Hold(std::collections::HashMap::from([(
            parent.id(),
            vec![task.clone()],
        )])),
    );
    registry.register(
        "multiple",
        true,
        Multiple {
            fail: false,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&parent).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Waiting(_)));
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.submissions[0].content, "first");
    assert_eq!(state.submissions[0].status, Status::Running);
    assert_eq!(state.submissions[1].content, "second");
    assert_eq!(state.submissions[1].status, Status::Queued);
    assert_eq!(state.application_writes.len(), 2);
    assert_eq!(state.application_writes[0].data, Some(json!(1)));
    assert_eq!(state.application_writes[1].data, Some(json!(2)));
    let store = Store::open(&path).unwrap();
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "write")
            .unwrap()
            .unwrap()["status"],
        "done"
    );
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "write2")
            .unwrap()
            .unwrap()["entry"],
        state.application_writes[1].id.to_string()
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(
        TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "input")
            .unwrap()
            .unwrap()["id"],
        state.submissions[0].id.to_string()
    );
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.application_writes.len(), 2);
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
}

#[test]
fn multiple_admissions_roll_back_with_failed_task() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "multiple",
        true,
        Multiple {
            fail: true,
            notify: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "multiple");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Failed(_)));
    let state = h.conversation(id).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    for key in ["input", "write", "write2"] {
        assert!(
            TaskStore::new(&store)
                .read_typed_signal(task.id(), &submission_settled_signal(), key)
                .unwrap()
                .is_none()
        );
    }
    driver.shutdown().unwrap();
}

struct MultipleRace {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

struct AdmissionLimit;
impl Handler for AdmissionLimit {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        for n in 0..16 {
            docs.submit_write(
                format!("limit-{n}"),
                "app.limit".into(),
                Some(json!(n)),
                None,
            )
            .map_err(|e| e.to_string())?;
        }
        assert!(
            docs.submit_write("overflow".into(), "app.limit".into(), None, None)
                .is_err()
        );
        Ok(Step::Succeed(json!(null)))
    }
}

#[test]
fn admission_batch_bound_preserves_first_sixteen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register("limit", true, AdmissionLimit);
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "limit");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    let writes = h.conversation(id).unwrap().application_writes;
    assert_eq!(writes.len(), 16);
    for (n, write) in writes.iter().enumerate() {
        assert_eq!(write.data, Some(json!(n)));
    }
    driver.shutdown().unwrap();
}
impl Handler for MultipleRace {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let result = Multiple {
            fail: false,
            notify: false,
        }
        .run_with_documents(task, docs)?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.staged.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|e| e.to_string())?;
        }
        Ok(result)
    }
}

#[test]
fn multiple_admissions_replay_as_one_after_conversation_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        MultipleRace {
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.submit_write(id, None, "app.concurrent".into(), None)
        .unwrap();
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    release_tx.send(()).unwrap();
    wait_task(&path, task.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.application_writes.len(), 3);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_some()
    );
    driver.shutdown().unwrap();
}

#[test]
fn abort_discards_all_staged_admissions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        MultipleRace {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    driver.request_abort(task.id()).unwrap();
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    let state = h.conversation(id).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}

#[cfg(unix)]
struct KillMultiple;
#[cfg(unix)]
impl Handler for KillMultiple {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        Multiple {
            fail: false,
            notify: false,
        }
        .run_with_documents(task, docs)?;
        println!("MULTIPLE_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(unix)]
#[test]
fn multiple_kill_worker() {
    let Ok(path) = std::env::var("PI_MULTI_KILL_DB") else {
        return;
    };
    let id = Uuid::parse_str(&std::env::var("PI_MULTI_KILL_CONVERSATION").unwrap()).unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("multiple", true, KillMultiple);
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::from_u128(8844),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("multiple".into(), true);
    driver.submit(&task).unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn killed_multiple_admissions_replay_once_after_reopen() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "multiple_kill_worker", "--nocapture"])
        .env("PI_MULTI_KILL_DB", &path)
        .env("PI_MULTI_KILL_CONVERSATION", id.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if line.contains("MULTIPLE_STAGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let reached = rx.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    reached.expect("worker did not stage admissions");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    let mut registry = Registry::default();
    registry.register(
        "multiple",
        true,
        Multiple {
            fail: false,
            notify: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    wait_task(&path, Uuid::from_u128(8844), |s| {
        matches!(s, TaskState::Succeeded(_))
    });
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 2);
    assert_eq!(h.conversation(id).unwrap().application_writes.len(), 2);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_some()
    );
}

struct Hold(std::collections::HashMap<Uuid, Vec<Task>>);
impl Handler for Hold {
    fn run(&self, task: &Task) -> Result<Step, String> {
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "hold".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("hold").unwrap()],
            },
            children: self.0.get(&task.id()).cloned().unwrap_or_default(),
        })
    }
}

struct DescendantWrite;
impl Handler for DescendantWrite {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        docs.submit_write(
            "write-request".into(),
            "app.note".into(),
            Some(json!({"v":1})),
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "settled".into(),
            }),
        )
        .map_err(|e| e.to_string())?;
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("settled").unwrap()],
            },
            children: vec![],
        })
    }
}

fn descendant(parent: Uuid, kind: &str) -> Task {
    Task::new(
        Uuid::new_v4(),
        TaskOwner::Task(parent),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered(kind.into(), true)
}

#[tokio::test]
async fn descendant_input_and_write_commit_notify_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    h.submit(id, None, "first".into()).unwrap();
    let root = task(id, "hold");
    let middle = descendant(root.id(), "hold");
    let input = descendant(middle.id(), "input");
    let write = descendant(middle.id(), "write");
    let mut registry = Registry::default();
    registry.register(
        "hold",
        true,
        Hold(std::collections::HashMap::from([
            (root.id(), vec![middle.clone()]),
            (middle.id(), vec![input.clone(), write.clone()]),
        ])),
    );
    registry.register(
        "input",
        true,
        Stage {
            write: false,
            fail: false,
        },
    );
    registry.register("write", true, DescendantWrite);
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&root).unwrap();
    wait_task(&path, input.id(), |s| matches!(s, TaskState::Waiting(_)));
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 2);
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, input.id(), |s| matches!(s, TaskState::Succeeded(_)));
    wait_task(&path, write.id(), |s| matches!(s, TaskState::Succeeded(_)));
    let store = Store::open(&path).unwrap();
    for id in [input.id(), write.id()] {
        assert!(
            TaskStore::new(&store)
                .read_typed_signal(id, &submission_settled_signal(), "settled")
                .unwrap()
                .is_some()
        );
    }
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.application_writes.len(), 1);
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
}

struct RacingDescendant {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Handler for RacingDescendant {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.submit_write("ancestry-race".into(), "app.race".into(), None, None)
            .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.staged.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|e| e.to_string())?;
        }
        Ok(Step::Succeed(json!(null)))
    }
}

#[test]
fn descendant_ancestry_rewrite_replays_without_partial_effects() {
    use durable_store::{Check, Expected, Mutation};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, original) = setup(&path);
    let other = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let root = task(original, "hold");
    let child = descendant(root.id(), "race");
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "hold",
        true,
        Hold(std::collections::HashMap::from([(
            root.id(),
            vec![child.clone()],
        )])),
    );
    registry.register(
        "race",
        true,
        RacingDescendant {
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&root).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let mut key = b"durable/task/".to_vec();
    key.extend_from_slice(root.id().as_bytes());
    let entry = store.get(&key).unwrap().unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    // Simulate an out-of-band reparent: the public task store rejects it.
    value["owner"] = json!({"Conversation": other});
    store
        .commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
    assert!(
        h.conversation(original)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        h.conversation(original)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert_eq!(h.conversation(other).unwrap().application_writes.len(), 1);
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    driver.shutdown().unwrap();
}

#[test]
fn ancestor_abort_discards_staged_descendant_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let root = task(id, "hold");
    let child = descendant(root.id(), "race");
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "hold",
        true,
        Hold(std::collections::HashMap::from([(root.id(), vec![child])])),
    );
    registry.register(
        "race",
        true,
        RacingDescendant {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&root).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    driver.request_abort(root.id()).unwrap();
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn busy_input_and_notification_commit_with_task_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    h.submit(id, None, "first".into()).unwrap();
    let mut registry = Registry::default();
    registry.register(
        "input",
        true,
        Stage {
            write: false,
            fail: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "input");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Waiting(_))
    });
    let state = h.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 2);
    assert_eq!(state.submissions[1].status, Status::Queued);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_some()
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    let store = Store::open(&path).unwrap();
    let value = TaskStore::new(&store)
        .read_typed_signal(task.id(), &submission_settled_signal(), "settled")
        .unwrap()
        .unwrap();
    assert_eq!(value["id"], state.submissions[1].id.to_string());
    assert_eq!(value["status"], "done");
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 2);
}
#[test]
fn immediate_write_notification_composes_with_task_wait_and_document() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "write",
        true,
        Stage {
            write: true,
            fail: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "write");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    let state = h.conversation(id).unwrap();
    assert_eq!(state.application_writes.len(), 1);
    assert!(state.application_writes[0].placed);
    let store = Store::open(&path).unwrap();
    let value = TaskStore::new(&store)
        .read_typed_signal(task.id(), &submission_settled_signal(), "settled")
        .unwrap()
        .unwrap();
    assert_eq!(value["entry"], state.application_writes[0].id.to_string());
    assert_eq!(value["status"], "done");
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    driver.shutdown().unwrap();
}
#[test]
fn failed_write_discards_admission_notification_and_documents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "write",
        true,
        Stage {
            write: true,
            fail: true,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "write");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Failed(_))
    });
    let store = Store::open(&path).unwrap();
    let binding = format!("durable/owned-subagent-notify/{}/736574746c6564", task.id());
    assert!(store.get(binding.as_bytes()).unwrap().is_none());
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(task.id(), &submission_settled_signal(), "settled")
            .unwrap()
            .is_none()
    );
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

struct DuplicateWrite;
impl Handler for DuplicateWrite {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        docs.submit_write(
            "request".into(),
            "pi.invalid".into(),
            Some(json!(2)),
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "different".into(),
            }),
        )
        .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn duplicate_write_preserves_original_and_notification() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "write",
        true,
        Stage {
            write: true,
            fail: false,
        },
    );
    registry.register("duplicate", true, DuplicateWrite);
    let driver = h.start_tasks(registry).unwrap();
    let first = task(id, "write");
    driver.submit(&first).unwrap();
    wait_task(&path, first.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    let before = h.conversation(id).unwrap().application_writes[0].clone();
    let second = task(id, "duplicate");
    driver.submit(&second).unwrap();
    wait_task(&path, second.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    let writes = h.conversation(id).unwrap().application_writes;
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].id, before.id);
    assert_eq!(writes[0].data, before.data);
    let store = Store::open(&path).unwrap();
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(second.id(), &submission_settled_signal(), "different")
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}
struct RejectBusy;
impl Handler for RejectBusy {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let opts = AdmissionOptions {
            reject_when_busy: true,
            ..Default::default()
        };
        assert!(matches!(
            docs.submit_input_with_options("only-idle".into(), "test".into(), opts, None),
            Err(pi_rs_durable::document::Error::SessionAdmission(
                pi_rs_durable::runtime::AdmissionError::Busy
            ))
        ));
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn busy_rejection_is_reported_in_handler() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    h.submit(id, None, "first".into()).unwrap();
    let mut registry = Registry::default();
    registry.register("reject", true, RejectBusy);
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "reject");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 1);
    driver.shutdown().unwrap();
}

struct GatedWrite {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Handler for GatedWrite {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.submit_write("staged".into(), "app.staged".into(), None, None)
            .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            self.staged.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|e| e.to_string())?;
        }
        Ok(Step::Succeed(json!(null)))
    }
}
fn gated_write_race(abort: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "gated",
        true,
        GatedWrite {
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "gated");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    if abort {
        driver.request_abort(task.id()).unwrap();
    } else {
        h.submit_write(id, None, "app.concurrent".into(), None)
            .unwrap();
    }
    release_tx.send(()).unwrap();
    if abort {
        driver.shutdown().unwrap();
    } else {
        wait_task(&path, task.id(), |state| {
            matches!(state, TaskState::Succeeded(_))
        });
        driver.shutdown().unwrap();
    }
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let writes = h.conversation(id).unwrap().application_writes;
    assert_eq!(
        writes
            .iter()
            .filter(|w| w.request_id.as_deref() == Some("staged"))
            .count(),
        usize::from(!abort)
    );
    let doc = Documents::new(&Store::open(&path).unwrap())
        .get(&address())
        .unwrap();
    assert_eq!(doc.is_some(), !abort);
    if !abort {
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
#[test]
fn write_conflict_replays_whole_step() {
    gated_write_race(false);
}
#[test]
fn write_abort_discards_whole_step() {
    gated_write_race(true);
}

#[tokio::test]
async fn write_behind_reset_queues_then_delivers_notification() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    h.submit(id, None, "first".into()).unwrap();
    h.reset(id, None).unwrap();
    let mut registry = Registry::default();
    registry.register(
        "write",
        true,
        Stage {
            write: true,
            fail: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "write");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Waiting(_))
    });
    let before = h.conversation(id).unwrap();
    assert_eq!(before.application_writes.len(), 1);
    assert!(!before.application_writes[0].placed);
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert!(h.conversation(id).unwrap().application_writes[0].placed);
    driver.shutdown().unwrap();
}

struct TwoResetBatch {
    fail: bool,
}
impl Handler for TwoResetBatch {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        let notify = |key: &str| {
            Some(SubmissionNotify {
                task_id: task.id(),
                key: key.into(),
            })
        };
        docs.submit_write(
            "reset-write-a".into(),
            "app.a".into(),
            Some(json!(1)),
            notify("a"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_input_with_options(
            "reset-input".into(),
            "after resets".into(),
            AdmissionOptions::default(),
            notify("input"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_write(
            "reset-write-b".into(),
            "app.b".into(),
            Some(json!(2)),
            notify("b"),
        )
        .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"reset_batch":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.fail {
            return Ok(Step::Fail("discard batch".into()));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("input").unwrap()],
            },
            children: vec![],
        })
    }
}

#[tokio::test]
async fn mixed_batch_waits_for_two_resets_and_rolls_back_on_failure() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, id) = setup(&path);
        h.submit(id, None, "initial".into()).unwrap();
        let first = h.reset(id, None).unwrap();
        let second = h.reset(id, None).unwrap();
        let mut registry = Registry::default();
        registry.register("two-reset-batch", true, TwoResetBatch { fail });
        let driver = h.start_tasks(registry).unwrap();
        let task = task(id, "two-reset-batch");
        driver.submit(&task).unwrap();
        wait_task(&path, task.id(), |state| {
            if fail {
                matches!(state, TaskState::Failed(_))
            } else {
                matches!(state, TaskState::Waiting(_))
            }
        });
        let store = Store::open(&path).unwrap();
        let before = h.conversation(id).unwrap();
        let pending = serde_json::to_value(&before).unwrap()["pending_resets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(pending, [first, second]);
        assert_eq!(before.submissions.len(), if fail { 1 } else { 2 });
        assert_eq!(before.application_writes.len(), if fail { 0 } else { 2 });
        if !fail {
            assert_eq!(before.submissions[1].status, Status::Queued);
            assert_eq!(
                before
                    .application_writes
                    .iter()
                    .map(|w| (w.kind.as_str(), w.placed, w.after_reset))
                    .collect::<Vec<_>>(),
                [
                    ("app.a", false, Some(second)),
                    ("app.b", false, Some(second))
                ]
            );
        }
        assert_eq!(
            Documents::new(&store).get(&address()).unwrap().is_some(),
            !fail
        );
        for key in ["a", "b", "input"] {
            assert!(
                TaskStore::new(&store)
                    .read_typed_signal(task.id(), &submission_settled_signal(), key)
                    .unwrap()
                    .is_none()
            );
        }
        driver.shutdown().unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let reopened = h.conversation(id).unwrap();
        let pending = serde_json::to_value(&reopened).unwrap()["pending_resets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap().parse::<Uuid>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(pending, [first, second]);
        assert_eq!(reopened.application_writes.len(), if fail { 0 } else { 2 });
        let mut registry = Registry::default();
        registry.register("two-reset-batch", true, TwoResetBatch { fail });
        let driver = h.start_tasks(registry).unwrap();
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .unwrap();
        if !fail {
            wait_task(&path, task.id(), |s| matches!(s, TaskState::Succeeded(_)));
        }
        let after = h.conversation(id).unwrap();
        assert_eq!(
            after.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
            [first, second]
        );
        assert!(
            serde_json::to_value(&after).unwrap()["pending_resets"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(after.application_writes.len(), if fail { 0 } else { 2 });
        if !fail {
            assert!(matches!(after.submissions[1].status, Status::Done { .. }));
            assert!(after.messages.iter().any(|message| matches!(message, pi_rs_core::Message::User { content } if content == "after resets")));
            assert!(!after.messages.iter().any(|message| matches!(message, pi_rs_core::Message::User { content } if content == "initial")));
            for (index, key) in ["a", "b"].iter().enumerate() {
                let write = &after.application_writes[index];
                assert!(write.placed);
                assert_eq!(write.after_reset, Some(second));
                let signal = TaskStore::new(&store)
                    .read_typed_signal(task.id(), &submission_settled_signal(), *key)
                    .unwrap()
                    .unwrap();
                assert_eq!(signal["entry"], write.id.to_string());
                assert_eq!(signal["status"], "done");
            }
            let signal = TaskStore::new(&store)
                .read_typed_signal(task.id(), &submission_settled_signal(), "input")
                .unwrap()
                .unwrap();
            assert_eq!(signal["id"], after.submissions[1].id.to_string());
        } else {
            assert!(Documents::new(&store).get(&address()).unwrap().is_none());
            for key in ["a", "b", "input"] {
                assert!(
                    TaskStore::new(&store)
                        .read_typed_signal(task.id(), &submission_settled_signal(), key)
                        .unwrap()
                        .is_none()
                );
            }
        }
        driver.shutdown().unwrap();
    }
}

#[cfg(unix)]
struct KillWrite;
#[cfg(unix)]
impl Handler for KillWrite {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        docs.submit_write(
            "request".into(),
            "app.note".into(),
            Some(json!({"v":1})),
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "settled".into(),
            }),
        )
        .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        println!("TASK_WRITE_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(unix)]
#[test]
fn task_write_kill_child() {
    let Ok(path) = std::env::var("PI_TASK_WRITE_KILL_DB") else {
        return;
    };
    let id = Uuid::parse_str(&std::env::var("PI_TASK_WRITE_CONVERSATION").unwrap()).unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("write", true, KillWrite);
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::from_u128(2001),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("write".into(), true);
    driver.submit(&task).unwrap();
    loop {
        std::thread::park();
    }
}
#[cfg(unix)]
#[test]
fn killed_staged_write_recovers_once_with_notification() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "task_write_kill_child", "--nocapture"])
        .env("PI_TASK_WRITE_KILL_DB", &path)
        .env("PI_TASK_WRITE_CONVERSATION", id.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("TASK_WRITE_STAGED") {
                let _ = sent.send(());
                break;
            }
        }
    });
    let observed = received.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    observed.expect("write was not staged");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    let store = Store::open(&path).unwrap();
    let binding = format!("durable/owned-subagent-notify/{}/736574746c6564", Uuid::from_u128(2001));
    assert!(store.get(binding.as_bytes()).unwrap().is_none());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    drop(store);
    let mut registry = Registry::default();
    registry.register(
        "write",
        true,
        Stage {
            write: true,
            fail: false,
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    wait_task(&path, Uuid::from_u128(2001), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let writes = h.conversation(id).unwrap().application_writes;
    assert_eq!(writes.len(), 1);
    assert!(writes[0].placed);
    let store = Store::open(&path).unwrap();
    let binding = store.get(binding.as_bytes()).unwrap().unwrap();
    let bound: (Uuid, Uuid) = serde_json::from_slice(&binding.value).unwrap();
    assert_eq!(bound, (id, writes[0].id));
    let value = TaskStore::new(&store)
        .read_typed_signal(
            Uuid::from_u128(2001),
            &submission_settled_signal(),
            "settled",
        )
        .unwrap()
        .unwrap();
    assert_eq!(value["entry"], writes[0].id.to_string());
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
}

struct ReservedTarget {
    target: Uuid,
}
impl Handler for ReservedTarget {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let result = docs.submit_write(
            "reserved".into(),
            "app.note".into(),
            None,
            Some(SubmissionNotify {
                task_id: self.target,
                key: "reserved".into(),
            }),
        );
        assert!(
            matches!(result,Err(pi_rs_durable::document::Error::InvalidSessionAdmission(message)) if message.contains("reserved"))
        );
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn task_submission_rejects_foreground_reserved_notification() {
    use pi_rs_durable::task_store::Save;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let parent = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "parent".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    let store = Store::open(&path).unwrap();
    TaskStore::new(&store)
        .save(&[Save::Create(&parent)])
        .unwrap();
    drop(store);
    h.submit_task_owned_subagent_notified(
        parent.id(),
        parent.owner().clone(),
        "child",
        Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        },
        "work".into(),
        None,
        "reserved".into(),
    )
    .unwrap();
    let mut registry = Registry::default();
    registry.register(
        "reserved",
        true,
        ReservedTarget {
            target: parent.id(),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "reserved");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    driver.shutdown().unwrap();
}

struct RacingReservation {
    target: Uuid,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Handler for RacingReservation {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let attempt = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let result = docs.submit_write(
            "race-reservation".into(),
            "app.note".into(),
            None,
            Some(SubmissionNotify {
                task_id: self.target,
                key: "reserved".into(),
            }),
        );
        if attempt == 0 {
            result.map_err(|e| e.to_string())?;
            self.staged.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .map_err(|e| e.to_string())?;
        } else {
            assert!(
                matches!(result,Err(pi_rs_durable::document::Error::InvalidSessionAdmission(message)) if message.contains("reserved"))
            );
        }
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn concurrent_foreground_reservation_replays_before_rejection() {
    use pi_rs_durable::task_store::Save;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, id) = setup(&path);
    let parent = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "parent".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap();
    let store = Store::open(&path).unwrap();
    TaskStore::new(&store)
        .save(&[Save::Create(&parent)])
        .unwrap();
    drop(store);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "race-reservation",
        true,
        RacingReservation {
            target: parent.id(),
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "race-reservation");
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.submit_task_owned_subagent_notified(
        parent.id(),
        parent.owner().clone(),
        "child",
        Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        },
        "work".into(),
        None,
        "reserved".into(),
    )
    .unwrap();
    release_tx.send(()).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(h.conversation(id).unwrap().application_writes.is_empty());
    driver.shutdown().unwrap();
}

struct ConflictingSignal;
impl Handler for ConflictingSignal {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        let id = Uuid::from_u128(777);
        let conversation = match task.owner() {
            TaskOwner::Conversation(id) => *id,
            _ => unreachable!(),
        };
        docs.submit_write(
            "conflict".into(),
            "app.note".into(),
            None,
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "settled".into(),
            }),
        )
        .map_err(|e| e.to_string())?;
        let value = json!({"id":id,"conversationId":conversation,"type":"write","status":"done","entry":id});
        assert!(docs.signal_at(task.id(), &submission_settled_signal(), "settled", value, 0).is_err());
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("settled").unwrap()],
            },
            children: vec![],
        })
    }
}
#[test]
fn staged_signal_cannot_replace_write_notification() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, id) = setup(&path);
    let mut registry = Registry::default();
    registry.register("conflict", true, ConflictingSignal);
    registry.register("duplicate", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let driver = h.start_tasks(registry).unwrap();
    let task = task(id, "conflict");
    driver.submit(&task).unwrap();
    wait_task(&path, task.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert_eq!(h.conversation(id).unwrap().application_writes.len(), 1);
    let store = Store::open(&path).unwrap();
    let receipt = TaskStore::new(&store)
        .read_typed_signal(task.id(), &submission_settled_signal(), "settled")
        .unwrap()
        .unwrap();
    assert_eq!(receipt["id"], h.conversation(id).unwrap().application_writes[0].id.to_string());
    let other = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("duplicate".into(), true);
    driver.submit(&other).unwrap();
    wait_task(&path, other.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    driver.shutdown().unwrap();
}

#[test]
fn ignored_write_validation_storage_error_discards_staged_effects() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct IgnoreWriteError {
        path: std::path::PathBuf,
        saw_storage_error: Arc<AtomicBool>,
    }
    impl Handler for IgnoreWriteError {
        fn run(&self, _: &Task) -> Result<Step, String> {
            unreachable!()
        }
        fn run_with_documents(
            &self,
            task: &Task,
            docs: &mut TaskDocuments<'_>,
        ) -> Result<Step, String> {
            docs.get(&address()).map_err(|e| e.to_string())?;
            docs.put(
                &address(),
                json!({"must_not_commit": true})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .map_err(|e| e.to_string())?;
            let conn = rusqlite::Connection::open(&self.path).map_err(|e| e.to_string())?;
            conn.execute_batch(
                "ALTER TABLE durable_document_history_meta RENAME TO disabled_document_history_meta",
            )
            .map_err(|e| e.to_string())?;
            // The conversation lookup succeeds, then the write draft's validation
            // fails when it opens a document-history snapshot.
            let result = docs.submit_write(
                "write-storage-failure".into(),
                "app.note".into(),
                Some(json!({"must_not_commit": true})),
                Some(SubmissionNotify {
                    task_id: task.id(),
                    key: "settled".into(),
                }),
            );
            self.saw_storage_error.store(
                matches!(&result, Err(pi_rs_durable::document::Error::TaskInputStore(error))
                    if matches!(error.as_ref(), durable_store::Error::Storage(_))
                        && error.to_string().contains("durable_document_history_meta")),
                Ordering::SeqCst,
            );
            conn.execute_batch(
                "ALTER TABLE disabled_document_history_meta RENAME TO durable_document_history_meta",
            )
            .map_err(|e| e.to_string())?;
            // Intentionally ignore the failure and claim success.
            Ok(Step::Succeed(json!({"must_not_commit": true})))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, conversation) = setup(&path);
    let saw_storage_error = Arc::new(AtomicBool::new(false));
    let mut registry = Registry::default();
    registry.register(
        "write-storage-error",
        true,
        IgnoreWriteError {
            path: path.clone(),
            saw_storage_error: saw_storage_error.clone(),
        },
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = task(conversation, "write-storage-error");
    scheduler.submit(&task).unwrap();
    let error = scheduler.tick(0).unwrap_err();
    assert!(saw_storage_error.load(Ordering::SeqCst));
    assert!(matches!(
        error,
        pi_rs_durable::task::scheduler::Error::Document(
            pi_rs_durable::document::Error::TaskInputStore(error)
        ) if matches!(error.as_ref(), durable_store::Error::Storage(_))
            && error.to_string().contains("durable_document_history_meta")
    ));
    let store = Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    assert_eq!(
        tasks.load(task.id()).unwrap().unwrap().task.state(),
        &TaskState::Running
    );
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let state = h.conversation(conversation).unwrap();
    assert!(state.application_writes.is_empty());
    assert!(state.submissions.is_empty());
    assert!(
        tasks
            .read_typed_signal(task.id(), &submission_settled_signal(), "settled")
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        scheduler.tick(0),
        Err(pi_rs_durable::task::scheduler::Error::Poisoned)
    ));
}

struct CrossSessionInput {
    target: Uuid,
    fail: bool,
    staged: Option<std::sync::mpsc::Sender<()>>,
    release: Option<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl Handler for CrossSessionInput {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        if let Err(error) = docs.submit_input_to_session_conversation(
            self.target,
            "cross-request".into(),
            "cross input".into(),
            AdmissionOptions::default(),
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "cross-settled".into(),
            }),
        ) {
            return Ok(Step::Fail(error.to_string()));
        }
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            if let Some(staged) = &self.staged {
                staged.send(()).unwrap();
            }
            if let Some(release) = &self.release {
                release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| e.to_string())?;
            }
        }
        if self.fail {
            return Ok(Step::Fail("deliberate failure".into()));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("cross-settled").unwrap()],
            },
            children: vec![],
        })
    }
}
fn cross_handler(target: Uuid) -> CrossSessionInput {
    CrossSessionInput {
        target,
        fail: false,
        staged: None,
        release: None,
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }
}
fn cross_setup(path: &std::path::Path) -> (Harness, Uuid, Uuid, Uuid) {
    let (mut h, owner) = setup(path);
    let target = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let store = Store::open(path).unwrap();
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    let session = membership.create().unwrap();
    membership.enroll(session, owner).unwrap();
    membership.enroll(session, target).unwrap();
    (h, owner, target, session)
}

#[tokio::test]
async fn cross_session_commit_notification_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Waiting(_)));
    assert!(h.conversation(owner).unwrap().submissions.is_empty());
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    h.resume_conversation(target, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "cross-settled")
            .unwrap()
            .is_some()
    );
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
}

#[test]
fn cross_session_rejects_unscoped_and_rolls_back_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, session) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    membership.revoke(session, target).unwrap();
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    let other = membership.create().unwrap();
    membership.enroll(other, target).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    membership.revoke(other, target).unwrap();
    driver.shutdown().unwrap();
    membership.enroll(session, target).unwrap();
    let mut handler = cross_handler(target);
    handler.fail = true;
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_revocation_reenrollment_replays_atomic_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, session) = cross_setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let calls = handler.calls.clone();
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    membership.revoke(session, target).unwrap();
    membership.enroll(session, target).unwrap();
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Waiting(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_target_abort_discards_prepared_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.request_abort(target).unwrap();
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
    let store = Store::open(&path).unwrap();
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_unsafe_conflict_does_not_replay_or_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, session) = cross_setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let calls = handler.calls.clone();
    let mut registry = Registry::default();
    registry.register("cross-unsafe", false, handler);
    let driver = h.start_tasks(registry).unwrap();
    let mut child = task(owner, "cross-unsafe");
    child = child.registered("cross-unsafe".into(), false);
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    membership.revoke(session, target).unwrap();
    membership.enroll(session, target).unwrap();
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_owner_abort_discards_prepared_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.request_abort(owner).unwrap();
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |s| {
        matches!(s, TaskState::Cancelled | TaskState::Failed(_))
    });
    let store = Store::open(&path).unwrap();
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[cfg(unix)]
struct CrossSessionKill(Uuid);
#[cfg(unix)]
impl Handler for CrossSessionKill {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        cross_handler(self.0).run_with_documents(task, docs)?;
        println!("CROSS_SESSION_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(unix)]
#[test]
fn cross_session_kill_worker() {
    let Ok(path) = std::env::var("CROSS_SESSION_DB") else {
        return;
    };
    let owner = std::env::var("CROSS_SESSION_OWNER")
        .unwrap()
        .parse()
        .unwrap();
    let target = std::env::var("CROSS_SESSION_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("cross", true, CrossSessionKill(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = Task::new(
        Uuid::from_u128(9931),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("cross".into(), true);
    driver.submit(&child).unwrap();
    loop {
        std::thread::park();
    }
}
#[cfg(unix)]
#[test]
fn cross_session_killed_step_recovers_once() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "cross_session_kill_worker", "--nocapture"])
        .env("CROSS_SESSION_DB", &path)
        .env("CROSS_SESSION_OWNER", owner.to_string())
        .env("CROSS_SESSION_TARGET", target.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let out = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(out).lines().map_while(Result::ok) {
            if line.contains("CROSS_SESSION_STAGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let reached = rx.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    reached.expect("worker did not stage input");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    wait_task(&path, Uuid::from_u128(9931), |s| {
        matches!(s, TaskState::Waiting(_))
    });
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
}

struct CrossForeignNotification(Uuid);
impl Handler for CrossForeignNotification {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let error = docs
            .submit_input_to_session_conversation(
                self.0,
                "foreign-notify".into(),
                "input".into(),
                AdmissionOptions::default(),
                Some(SubmissionNotify {
                    task_id: Uuid::new_v4(),
                    key: "foreign".into(),
                }),
            )
            .unwrap_err();
        Ok(Step::Fail(error.to_string()))
    }
}
#[test]
fn cross_session_rejects_foreign_notification_target() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let mut registry = Registry::default();
    registry.register("foreign", true, CrossForeignNotification(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "foreign");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn cross_session_scheduler_drives_target_and_settles_notification() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut scheduler = pi_rs_durable::runtime::Scheduler::open(&path, options()).unwrap();
    let config = || Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 10,
    };
    let owner = scheduler.create_conversation(config()).unwrap();
    let target = scheduler.create_conversation(config()).unwrap();
    let store = Store::open(&path).unwrap();
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    let session = membership.create().unwrap();
    membership.enroll(session, owner).unwrap();
    membership.enroll(session, target).unwrap();
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = scheduler.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let done = scheduler
                .conversation(target)
                .unwrap()
                .submissions
                .iter()
                .any(|submission| matches!(submission.status, Status::Done { .. }));
            let task_done = TaskStore::new(&store)
                .load(child.id())
                .unwrap()
                .is_some_and(|task| matches!(task.task.state(), TaskState::Succeeded(_)));
            if done && task_done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("target admission or notification did not settle");
    assert!(
        scheduler
            .conversation(owner)
            .unwrap()
            .submissions
            .is_empty()
    );
    assert_eq!(scheduler.conversation(target).unwrap().submissions.len(), 1);
    tokio::task::spawn_blocking(move || driver.shutdown())
        .await
        .unwrap()
        .unwrap();
    scheduler.shutdown().await.unwrap();
}

#[test]
fn cross_session_rejects_missing_abort_evidence_on_both_conversations() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    for id in [owner, target] {
        let key = format!("runtime/conversation/{id}").into_bytes();
        let original = store.get(&key).unwrap().unwrap();
        let mut damaged: serde_json::Value = serde_json::from_slice(&original.value).unwrap();
        damaged.as_object_mut().unwrap().remove("abort_requested");
        store
            .commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Version(original.version),
                }],
                &[durable_store::Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&damaged).unwrap(),
                }],
            )
            .unwrap();
        let child = task(owner, "cross");
        driver.submit(&child).unwrap();
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Failed(_))
        });
        assert!(h.conversation(target).unwrap().submissions.is_empty());
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        let damaged = store.get(&key).unwrap().unwrap();
        store
            .commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Version(damaged.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: original.value,
                }],
            )
            .unwrap();
    }
    driver.shutdown().unwrap();
}

fn cross_attach_target_owner(
    store: &Store,
    owner: Uuid,
    target: Uuid,
    mode: pi_rs_durable::ownership_edge::Mode,
) -> Uuid {
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore},
        task_store::Save,
    };
    let parent = task(owner, "not-registered");
    TaskStore::new(store)
        .save(&[Save::Create(&parent)])
        .unwrap();
    let prepared = EdgeStore::new(store)
        .prepare_create(&Edge {
            task: parent.id(),
            task_owner: TaskOwner::Conversation(owner),
            child: target,
            mode,
        })
        .unwrap();
    store.commit(&prepared.checks, &prepared.mutations).unwrap();
    parent.id()
}
fn cross_mark_completing_failed(store: &Store, id: Uuid) {
    let key = format!("durable/task/{id}").into_bytes();
    // Task keys are binary UUID suffixes; find the precise persisted key.
    let (key, entry) = store
        .list(b"durable/task/")
        .unwrap()
        .into_iter()
        .find(|(_, entry)| {
            serde_json::from_slice::<Task>(&entry.value).is_ok_and(|task| task.id() == id)
        })
        .unwrap_or_else(|| panic!("missing task {key:?}"));
    let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    value["state"] = json!({"CompletingFailed":"pending cascade"});
    store
        .commit(
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

#[test]
fn cross_session_rejects_target_with_failed_foreground_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let parent = cross_attach_target_owner(
        &store,
        owner,
        target,
        pi_rs_durable::ownership_edge::Mode::Foreground,
    );
    cross_mark_completing_failed(&store, parent);
    assert!(!h.conversation(target).unwrap().abort_requested);
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Failed(_) | TaskState::Cancelled)
    });
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_target_owner_failure_race_fences_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let parent = cross_attach_target_owner(
        &store,
        owner,
        target,
        pi_rs_durable::ownership_edge::Mode::Foreground,
    );
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    cross_mark_completing_failed(&store, parent);
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Failed(_) | TaskState::Cancelled)
    });
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_background_target_does_not_inherit_failed_parent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let parent = cross_attach_target_owner(
        &store,
        owner,
        target,
        pi_rs_durable::ownership_edge::Mode::Background,
    );
    cross_mark_completing_failed(&store, parent);
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Waiting(_))
    });
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_rejects_duplicate_with_different_notification_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    h.submit(target, Some("cross-request".into()), "host input".into())
        .unwrap();
    drop(h);
    for _ in 0..2 {
        let h = Harness::open_paused(&path, options()).unwrap();
        let mut registry = Registry::default();
        registry.register("cross", true, cross_handler(target));
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "cross");
        driver.submit(&child).unwrap();
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Failed(_))
        });
        assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
        assert!(
            Documents::new(&Store::open(&path).unwrap())
                .get(&address())
                .unwrap()
                .is_none()
        );
        driver.shutdown().unwrap();
    }
}

#[test]
fn cross_session_pending_fail_fast_sibling_fences_target() {
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore, Mode},
        task_store::Save,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let root = task(owner, "not-registered");
    let branch = descendant(root.id(), "not-registered");
    let sibling = descendant(root.id(), "not-registered");
    TaskStore::new(&store)
        .save(&[
            Save::Create(&root),
            Save::Create(&branch),
            Save::Create(&sibling),
        ])
        .unwrap();
    let edge = EdgeStore::new(&store)
        .prepare_create(&Edge {
            task: branch.id(),
            task_owner: TaskOwner::Task(root.id()),
            child: target,
            mode: Mode::Foreground,
        })
        .unwrap();
    store.commit(&edge.checks, &edge.mutations).unwrap();
    let (key, entry) = store
        .list(b"durable/task/")
        .unwrap()
        .into_iter()
        .find(|(_, entry)| {
            serde_json::from_slice::<Task>(&entry.value).is_ok_and(|task| task.id() == root.id())
        })
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    value["state"] = json!({"Waiting":{"TasksFailFast":[branch.id(),sibling.id()]}});
    store
        .commit(
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
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    cross_mark_completing_failed(&store, sibling.id());
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Failed(_) | TaskState::Cancelled)
    });
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_target_cleanup_marker_fences_prepared_input() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_handler(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("cross", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let key = format!("runtime/owned-cleanup-attempt/{target}").into_bytes();
    store
        .commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: durable_store::Expected::Missing,
            }],
            &[durable_store::Mutation::Put {
                key,
                value: b"{}".to_vec(),
            }],
        )
        .unwrap();
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Failed(_))
    });
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

struct CrossAction {
    target: Uuid,
    write: bool,
    fail: bool,
    staged: Option<std::sync::mpsc::Sender<()>>,
    release: Option<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
fn cross_action(target: Uuid, write: bool) -> CrossAction {
    CrossAction {
        target,
        write,
        fail: false,
        staged: None,
        release: None,
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }
}
impl Handler for CrossAction {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        let notify = Some(SubmissionNotify {
            task_id: task.id(),
            key: "action-settled".into(),
        });
        let result = if self.write {
            docs.submit_write_to_session_conversation(
                self.target,
                "cross-action".into(),
                "app.cross".into(),
                Some(json!({"n":1})),
                notify,
            )
        } else {
            docs.submit_continue_to_session_conversation(self.target, "cross-action".into(), notify)
        };
        if let Err(error) = result {
            return Ok(Step::Fail(error.to_string()));
        }
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"action":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            if let Some(staged) = &self.staged {
                staged.send(()).unwrap();
            }
            if let Some(release) = &self.release {
                release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| e.to_string())?;
            }
        }
        if self.fail {
            return Ok(Step::Fail("rollback".into()));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("action-settled").unwrap()],
            },
            children: vec![],
        })
    }
}

#[tokio::test]
async fn cross_session_continue_and_write_settle_atomically_and_reopen() {
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, _) = cross_setup(&path);
        let mut registry = Registry::default();
        registry.register("action", true, cross_action(target, write));
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "action");
        driver.submit(&child).unwrap();
        wait_task(&path, child.id(), |s| {
            if write {
                matches!(s, TaskState::Succeeded(_))
            } else {
                matches!(s, TaskState::Waiting(_))
            }
        });
        let store = Store::open(&path).unwrap();
        assert!(Documents::new(&store).get(&address()).unwrap().is_some());
        assert!(h.conversation(owner).unwrap().submissions.is_empty());
        if write {
            assert_eq!(h.conversation(target).unwrap().application_writes.len(), 1);
        } else {
            assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
        }
        driver.shutdown().unwrap();
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        if !write {
            let mut registry = Registry::default();
            registry.register("action", true, cross_action(target, write));
            let driver = h.start_tasks(registry).unwrap();
            h.resume_conversation(target, &CancellationToken::new())
                .await
                .unwrap();
            wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
            driver.shutdown().unwrap();
        }
        let signal = TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "action-settled")
            .unwrap()
            .unwrap();
        assert_eq!(signal["type"], if write { "write" } else { "continue" });
        assert_eq!(
            h.conversation(target).unwrap().application_writes.len(),
            usize::from(write)
        );
        assert_eq!(
            h.conversation(target).unwrap().submissions.len(),
            usize::from(!write)
        );
    }
}

#[tokio::test]
async fn cross_session_continue_busy_and_write_after_two_resets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    h.submit(target, None, "initial".into()).unwrap();
    let first = h.reset(target, None).unwrap();
    let second = h.reset(target, None).unwrap();
    let mut registry = Registry::default();
    registry.register("continue", true, cross_action(target, false));
    registry.register("write", true, cross_action(target, true));
    let driver = h.start_tasks(registry).unwrap();
    let rejected = task(owner, "continue");
    driver.submit(&rejected).unwrap();
    wait_task(&path, rejected.id(), |s| matches!(s, TaskState::Failed(_)));
    let child = task(owner, "write");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Waiting(_)));
    let state = h.conversation(target).unwrap();
    assert_eq!(state.submissions.len(), 1);
    assert_eq!(state.application_writes.len(), 1);
    assert!(!state.application_writes[0].placed);
    assert_eq!(state.application_writes[0].after_reset, Some(second));
    driver.shutdown().unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("write", true, cross_action(target, true));
    let driver = h.start_tasks(registry).unwrap();
    h.resume_conversation(target, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
    let state = h.conversation(target).unwrap();
    assert_eq!(
        state.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        [first, second]
    );
    assert!(state.application_writes[0].placed);
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_action_rollback_and_duplicate_notification_rejection() {
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, owner, target, _) = cross_setup(&path);
        let mut handler = cross_action(target, write);
        handler.fail = true;
        let mut registry = Registry::default();
        registry.register("action", true, handler);
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "action");
        driver.submit(&child).unwrap();
        wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
        let store = Store::open(&path).unwrap();
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        assert!(h.conversation(target).unwrap().submissions.is_empty());
        assert!(
            h.conversation(target)
                .unwrap()
                .application_writes
                .is_empty()
        );
        driver.shutdown().unwrap();
        if write {
            h.submit_write(target, Some("cross-action".into()), "app.host".into(), None)
                .unwrap();
        } else {
            h.submit_continue(target, Some("cross-action".into()))
                .unwrap();
        }
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let mut registry = Registry::default();
        registry.register("action", true, cross_action(target, write));
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "action");
        driver.submit(&child).unwrap();
        wait_task(&path, child.id(), |s| matches!(s, TaskState::Failed(_)));
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        let state = h.conversation(target).unwrap();
        assert_eq!(state.application_writes.len(), usize::from(write));
        assert_eq!(state.submissions.len(), usize::from(!write));
        driver.shutdown().unwrap();
    }
}

#[test]
fn cross_session_actions_replay_revocation_and_reject_abort() {
    for write in [false, true] {
        for abort in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (mut h, owner, target, session) = cross_setup(&path);
            let (staged_tx, staged_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let mut handler = cross_action(target, write);
            handler.staged = Some(staged_tx);
            handler.release = Some(std::sync::Mutex::new(release_rx));
            let calls = handler.calls.clone();
            let mut registry = Registry::default();
            registry.register("action", true, handler);
            let driver = h.start_tasks(registry).unwrap();
            let child = task(owner, "action");
            driver.submit(&child).unwrap();
            staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let store = Store::open(&path).unwrap();
            if abort {
                h.request_abort(target).unwrap();
            } else {
                let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
                membership.revoke(session, target).unwrap();
                membership.enroll(session, target).unwrap();
            }
            assert!(Documents::new(&store).get(&address()).unwrap().is_none());
            release_tx.send(()).unwrap();
            wait_task(&path, child.id(), |s| {
                if abort {
                    matches!(s, TaskState::Failed(_))
                } else if write {
                    matches!(s, TaskState::Succeeded(_))
                } else {
                    matches!(s, TaskState::Waiting(_))
                }
            });
            if abort {
                assert!(h.conversation(target).unwrap().submissions.is_empty());
                assert!(
                    h.conversation(target)
                        .unwrap()
                        .application_writes
                        .is_empty()
                );
                assert!(Documents::new(&store).get(&address()).unwrap().is_none());
            } else {
                assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
                let state = h.conversation(target).unwrap();
                assert_eq!(state.submissions.len(), usize::from(!write));
                assert_eq!(state.application_writes.len(), usize::from(write));
            }
            driver.shutdown().unwrap();
        }
    }
}

#[cfg(unix)]
struct KillCrossAction {
    target: Uuid,
    write: bool,
}
#[cfg(unix)]
impl Handler for KillCrossAction {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        cross_action(self.target, self.write).run_with_documents(task, docs)?;
        println!("CROSS_ACTION_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(unix)]
#[test]
fn cross_session_action_kill_worker() {
    let Ok(path) = std::env::var("CROSS_ACTION_DB") else {
        return;
    };
    let owner = std::env::var("CROSS_ACTION_OWNER")
        .unwrap()
        .parse()
        .unwrap();
    let target = std::env::var("CROSS_ACTION_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let write = std::env::var("CROSS_ACTION_WRITE").unwrap() == "1";
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("action", true, KillCrossAction { target, write });
    let driver = h.start_tasks(registry).unwrap();
    let child = Task::new(
        Uuid::from_u128(9998),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("action".into(), true);
    driver.submit(&child).unwrap();
    loop {
        std::thread::park();
    }
}
#[cfg(unix)]
#[test]
fn cross_session_actions_recover_once_after_process_kill() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, _) = cross_setup(&path);
        drop(h);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "cross_session_action_kill_worker", "--nocapture"])
            .env("CROSS_ACTION_DB", &path)
            .env("CROSS_ACTION_OWNER", owner.to_string())
            .env("CROSS_ACTION_TARGET", target.to_string())
            .env("CROSS_ACTION_WRITE", if write { "1" } else { "0" })
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines().map_while(Result::ok) {
                if line.contains("CROSS_ACTION_STAGED") {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        let reached = rx.recv_timeout(Duration::from_secs(20));
        let _ = child.kill();
        let _ = child.wait();
        reached.expect("worker did not stage action");
        let h = Harness::open_paused(&path, options()).unwrap();
        let state = h.conversation(target).unwrap();
        assert!(state.submissions.is_empty());
        assert!(state.application_writes.is_empty());
        let mut registry = Registry::default();
        registry.register("action", true, cross_action(target, write));
        let driver = h.start_tasks(registry).unwrap();
        wait_task(&path, Uuid::from_u128(9998), |s| {
            if write {
                matches!(s, TaskState::Succeeded(_))
            } else {
                matches!(s, TaskState::Waiting(_))
            }
        });
        let state = h.conversation(target).unwrap();
        assert_eq!(state.submissions.len(), usize::from(!write));
        assert_eq!(state.application_writes.len(), usize::from(write));
        driver.shutdown().unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        let state = h.conversation(target).unwrap();
        assert_eq!(state.submissions.len(), usize::from(!write));
        assert_eq!(state.application_writes.len(), usize::from(write));
    }
}

#[test]
fn cross_session_actions_fence_target_foreground_failure() {
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, _) = cross_setup(&path);
        let store = Store::open(&path).unwrap();
        let parent = cross_attach_target_owner(
            &store,
            owner,
            target,
            pi_rs_durable::ownership_edge::Mode::Foreground,
        );
        let (staged_tx, staged_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut handler = cross_action(target, write);
        handler.staged = Some(staged_tx);
        handler.release = Some(std::sync::Mutex::new(release_rx));
        let mut registry = Registry::default();
        registry.register("action", true, handler);
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "action");
        driver.submit(&child).unwrap();
        staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        cross_mark_completing_failed(&store, parent);
        release_tx.send(()).unwrap();
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Failed(_) | TaskState::Cancelled)
        });
        let target_state = h.conversation(target).unwrap();
        assert!(target_state.submissions.is_empty());
        assert!(target_state.application_writes.is_empty());
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        driver.shutdown().unwrap();
    }
}

#[test]
fn cross_session_actions_unsafe_attempts_do_not_replay_on_conflict() {
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, session) = cross_setup(&path);
        let store = Store::open(&path).unwrap();
        let (staged_tx, staged_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut handler = cross_action(target, write);
        handler.staged = Some(staged_tx);
        handler.release = Some(std::sync::Mutex::new(release_rx));
        let calls = handler.calls.clone();
        let mut registry = Registry::default();
        registry.register("action-unsafe", false, handler);
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "action-unsafe").registered("action-unsafe".into(), false);
        driver.submit(&child).unwrap();
        staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
        membership.revoke(session, target).unwrap();
        membership.enroll(session, target).unwrap();
        release_tx.send(()).unwrap();
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Failed(_))
        });
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let state = h.conversation(target).unwrap();
        assert!(state.submissions.is_empty());
        assert!(state.application_writes.is_empty());
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        driver.shutdown().unwrap();
    }
}

struct CrossActionForeign {
    target: Uuid,
    write: bool,
}
impl Handler for CrossActionForeign {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        let notify = Some(SubmissionNotify {
            task_id: Uuid::new_v4(),
            key: "foreign".into(),
        });
        let result = if self.write {
            docs.submit_write_to_session_conversation(
                self.target,
                "action".into(),
                "app.cross".into(),
                None,
                notify,
            )
        } else {
            docs.submit_continue_to_session_conversation(self.target, "action".into(), notify)
        };
        assert!(result.is_err());
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn cross_session_actions_reject_foreign_notification_destination() {
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, _) = cross_setup(&path);
        let mut registry = Registry::default();
        registry.register("foreign", true, CrossActionForeign { target, write });
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "foreign");
        driver.submit(&child).unwrap();
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Succeeded(_))
        });
        let state = h.conversation(target).unwrap();
        assert!(state.submissions.is_empty());
        assert!(state.application_writes.is_empty());
        driver.shutdown().unwrap();
    }
}

#[tokio::test]
async fn cross_session_actions_scheduler_drives_wake_and_notification() {
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut scheduler = pi_rs_durable::runtime::Scheduler::open(&path, options()).unwrap();
        let config = || Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        };
        let owner = scheduler.create_conversation(config()).unwrap();
        let target = scheduler.create_conversation(config()).unwrap();
        let store = Store::open(&path).unwrap();
        let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
        let session = membership.create().unwrap();
        membership.enroll(session, owner).unwrap();
        membership.enroll(session, target).unwrap();
        let mut registry = Registry::default();
        registry.register("action", true, cross_action(target, write));
        let driver = scheduler.start_tasks(registry).unwrap();
        let child = task(owner, "action");
        driver.submit(&child).unwrap();
        let wake = format!("runtime/task-input-wake/{target}");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let state = scheduler.conversation(target).unwrap();
                let settled = if write {
                    state.application_writes.len() == 1 && state.application_writes[0].placed
                } else {
                    state.submissions.len() == 1
                        && matches!(
                            state.submissions[0].status,
                            Status::Done { .. } | Status::Unanswered { .. }
                        )
                };
                let task_done = TaskStore::new(&store)
                    .load(child.id())
                    .unwrap()
                    .is_some_and(|item| matches!(item.task.state(), TaskState::Succeeded(_)));
                if settled && task_done && store.get(wake.as_bytes()).unwrap().is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("scheduler did not drive target wake and task notification");
        let state = scheduler.conversation(target).unwrap();
        let receipt = TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "action-settled")
            .unwrap()
            .unwrap();
        assert_eq!(receipt["conversationId"], target.to_string());
        assert_eq!(receipt["type"], if write { "write" } else { "continue" });
        if write {
            assert_eq!(receipt["entry"], state.application_writes[0].id.to_string());
            assert!(state.submissions.is_empty());
        } else {
            assert_eq!(receipt["id"], state.submissions[0].id.to_string());
            assert!(state.application_writes.is_empty());
        }
        assert!(
            scheduler
                .conversation(owner)
                .unwrap()
                .submissions
                .is_empty()
        );
        tokio::task::spawn_blocking(move || driver.shutdown())
            .await
            .unwrap()
            .unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[test]
fn cross_session_committed_action_wake_kill_worker() {
    use std::io::Write;
    let Ok(path) = std::env::var("CROSS_ACTION_WAKE_DB") else {
        return;
    };
    let owner: Uuid = std::env::var("CROSS_ACTION_WAKE_OWNER")
        .unwrap()
        .parse()
        .unwrap();
    let target: Uuid = std::env::var("CROSS_ACTION_WAKE_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let write = std::env::var("CROSS_ACTION_WAKE_WRITE").unwrap() == "1";
    let h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("action", true, cross_action(target, write));
    let driver = h.start_tasks(registry).unwrap();
    let child = Task::new(
        Uuid::from_u128(10019),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("action".into(), true);
    driver.submit(&child).unwrap();
    wait_task(std::path::Path::new(&path), child.id(), |state| {
        matches!(state, TaskState::Waiting(_))
    });
    let store = Store::open(&path).unwrap();
    assert!(
        store
            .get(format!("runtime/task-input-wake/{target}").as_bytes())
            .unwrap()
            .is_some()
    );
    println!("CROSS_ACTION_WAKE_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cross_session_committed_action_wake_survives_kill_and_drives_after_reopen() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    for write in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, owner, target, _) = cross_setup(&path);
        let last_reset = if write {
            h.submit(target, None, "initial".into()).unwrap();
            h.reset(target, None).unwrap();
            Some(h.reset(target, None).unwrap())
        } else {
            None
        };
        drop(h);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "cross_session_committed_action_wake_kill_worker",
                "--nocapture",
            ])
            .env("CROSS_ACTION_WAKE_DB", &path)
            .env("CROSS_ACTION_WAKE_OWNER", owner.to_string())
            .env("CROSS_ACTION_WAKE_TARGET", target.to_string())
            .env("CROSS_ACTION_WAKE_WRITE", if write { "1" } else { "0" })
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line.contains("CROSS_ACTION_WAKE_COMMITTED") {
                    let _ = sent.send(());
                    break;
                }
            }
        });
        let reached = received.recv_timeout(Duration::from_secs(20));
        let _ = child.kill();
        let _ = child.wait();
        reached.expect("child did not commit action and wake");
        let store = Store::open(&path).unwrap();
        let wake = format!("runtime/task-input-wake/{target}");
        assert!(store.get(wake.as_bytes()).unwrap().is_some());
        let hex: String = "action-settled"
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let binding_key = format!(
            "durable/owned-subagent-notify/{}/{hex}",
            Uuid::from_u128(10019)
        );
        let binding: (Uuid, Uuid) =
            serde_json::from_slice(&store.get(binding_key.as_bytes()).unwrap().unwrap().value)
                .unwrap();
        assert_eq!(binding.0, target);
        assert!(
            TaskStore::new(&store)
                .read_typed_signal(
                    Uuid::from_u128(10019),
                    &submission_settled_signal(),
                    "action-settled"
                )
                .unwrap()
                .is_none()
        );
        let scheduler = pi_rs_durable::runtime::Scheduler::open(&path, options()).unwrap();
        let mut registry = Registry::default();
        registry.register("action", true, cross_action(target, write));
        let driver = scheduler.start_tasks(registry).unwrap();
        let task_id = Uuid::from_u128(10019);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let state = scheduler.conversation(target).unwrap();
                let settled = if write {
                    state.application_writes.len() == 1 && state.application_writes[0].placed
                } else {
                    state.submissions.len() == 1
                        && matches!(
                            state.submissions[0].status,
                            Status::Done { .. } | Status::Unanswered { .. }
                        )
                };
                let task_done = TaskStore::new(&store)
                    .load(task_id)
                    .unwrap()
                    .is_some_and(|item| matches!(item.task.state(), TaskState::Succeeded(_)));
                if settled && task_done && store.get(wake.as_bytes()).unwrap().is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("reopened scheduler did not drive committed target wake");
        let state = scheduler.conversation(target).unwrap();
        let receipt = TaskStore::new(&store)
            .read_typed_signal(task_id, &submission_settled_signal(), "action-settled")
            .unwrap()
            .unwrap();
        assert_eq!(receipt["conversationId"], target.to_string());
        assert_eq!(receipt["id"], binding.1.to_string());
        assert_eq!(receipt["type"], if write { "write" } else { "continue" });
        if write {
            assert_eq!(state.application_writes.len(), 1);
            assert_eq!(receipt["entry"], state.application_writes[0].id.to_string());
            assert_eq!(state.application_writes[0].after_reset, last_reset);
            assert_eq!(state.resets.last().map(|reset| reset.id), last_reset);
            assert_eq!(state.submissions.len(), 1);
        } else {
            assert_eq!(state.submissions.len(), 1);
            assert_eq!(receipt["id"], state.submissions[0].id.to_string());
            assert!(state.application_writes.is_empty());
        }
        assert!(
            scheduler
                .conversation(owner)
                .unwrap()
                .submissions
                .is_empty()
        );
        tokio::task::spawn_blocking(move || driver.shutdown())
            .await
            .unwrap()
            .unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

struct CrossUnkeyedBatch {
    target: Uuid,
    fail: bool,
    staged: Option<std::sync::mpsc::Sender<()>>,
    release: Option<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
fn cross_unkeyed_batch(target: Uuid) -> CrossUnkeyedBatch {
    CrossUnkeyedBatch {
        target,
        fail: false,
        staged: None,
        release: None,
        calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }
}
impl Handler for CrossUnkeyedBatch {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        let notify = |key: &str| {
            Some(SubmissionNotify {
                task_id: task.id(),
                key: key.into(),
            })
        };
        if let Err(error) = docs.submit_write_to_session_conversation_without_request_id(
            self.target,
            "app.one".into(),
            Some(json!(1)),
            notify("one"),
        ) {
            return Ok(Step::Fail(error.to_string()));
        }
        docs.submit_write_to_session_conversation_without_request_id(
            self.target,
            "app.two".into(),
            Some(json!(2)),
            notify("two"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_write_to_session_conversation(
            self.target,
            "stable".into(),
            "app.stable".into(),
            Some(json!(3)),
            notify("stable"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_write_to_session_conversation(
            self.target,
            "stable".into(),
            "app.ignored".into(),
            Some(json!(4)),
            notify("stable"),
        )
        .map_err(|e| e.to_string())?;
        assert!(
            docs.submit_write_to_session_conversation(
                self.target,
                "stable".into(),
                "app.ignored".into(),
                None,
                notify("different")
            )
            .is_err()
        );
        assert!(
            docs.submit_write_to_session_conversation_without_request_id(
                self.target,
                "app.collision".into(),
                None,
                notify("one")
            )
            .is_err()
        );
        docs.submit_continue_to_session_conversation_without_request_id(
            self.target,
            notify("continue"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_input_to_session_conversation_without_request_id(
            self.target,
            "input one".into(),
            AdmissionOptions::default(),
            notify("input-one"),
        )
        .map_err(|e| e.to_string())?;
        docs.submit_input_to_session_conversation_without_request_id(
            self.target,
            "input two".into(),
            AdmissionOptions::default(),
            notify("input-two"),
        )
        .map_err(|e| e.to_string())?;
        assert!(
            docs.submit_continue_to_session_conversation_without_request_id(self.target, None)
                .is_err()
        );
        assert!(
            docs.submit_write_to_session_conversation_without_request_id(
                Uuid::new_v4(),
                "app.other".into(),
                None,
                None
            )
            .is_err()
        );
        assert!(
            docs.submit_input_without_request_id("owner".into(), AdmissionOptions::default(), None)
                .is_err()
        );
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"batch":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            if let Some(staged) = &self.staged {
                staged.send(()).unwrap();
            }
            if let Some(release) = &self.release {
                release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))
                    .map_err(|e| e.to_string())?;
            }
        }
        if self.fail {
            return Ok(Step::Fail("rollback".into()));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("input-two").unwrap()],
            },
            children: vec![],
        })
    }
}

#[tokio::test]
async fn cross_session_unkeyed_batch_orders_notifies_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    h.set_after_failed_run(target, pi_rs_durable::runtime::AfterFailedRun::Continue)
        .unwrap();
    let mut registry = Registry::default();
    registry.register("batch", true, cross_unkeyed_batch(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "batch");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Waiting(_)));
    let before = h.conversation(target).unwrap();
    assert_eq!(before.application_writes.len(), 3);
    assert_eq!(before.submissions.len(), 3);
    assert!(before.application_writes[0].request_id.is_none());
    assert!(before.application_writes[1].request_id.is_none());
    assert_ne!(
        before.application_writes[0].id,
        before.application_writes[1].id
    );
    assert_eq!(
        before.application_writes[2].request_id.as_deref(),
        Some("stable")
    );
    assert_eq!(before.application_writes[2].data, Some(json!(3)));
    assert!(before.submissions.iter().all(|s| s.request_id.is_none()));
    assert_ne!(before.submissions[1].id, before.submissions[2].id);
    assert_eq!(before.submissions[0].status, Status::Running);
    assert_eq!(before.submissions[1].status, Status::Queued);
    assert_eq!(before.submissions[2].status, Status::Queued);
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&address()).unwrap().is_some());
    for key in ["one", "two", "stable"] {
        assert!(
            TaskStore::new(&store)
                .read_typed_signal(child.id(), &submission_settled_signal(), key)
                .unwrap()
                .is_some()
        );
    }
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "input-two")
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("batch", true, cross_unkeyed_batch(target));
    let driver = h.start_tasks(registry).unwrap();
    h.resume_conversation(target, &CancellationToken::new())
        .await
        .unwrap();
    wait_task(&path, child.id(), |s| matches!(s, TaskState::Succeeded(_)));
    for key in ["one", "two", "stable", "continue", "input-one", "input-two"] {
        assert!(
            TaskStore::new(&store)
                .read_typed_signal(child.id(), &submission_settled_signal(), key)
                .unwrap()
                .is_some(),
            "missing {key}"
        );
    }
    let after = h.conversation(target).unwrap();
    assert_eq!(after.application_writes.len(), 3);
    assert_eq!(after.submissions.len(), 3);
    assert!(h.conversation(owner).unwrap().submissions.is_empty());
    driver.shutdown().unwrap();
}

#[test]
fn cross_session_unkeyed_batch_rollback_and_membership_replay() {
    for fail in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, session) = cross_setup(&path);
        let (staged_tx, staged_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut handler = cross_unkeyed_batch(target);
        handler.fail = fail;
        handler.staged = Some(staged_tx);
        handler.release = Some(std::sync::Mutex::new(release_rx));
        let calls = handler.calls.clone();
        let mut registry = Registry::default();
        registry.register("batch", true, handler);
        let driver = h.start_tasks(registry).unwrap();
        let child = task(owner, "batch");
        driver.submit(&child).unwrap();
        staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let store = Store::open(&path).unwrap();
        assert!(h.conversation(target).unwrap().submissions.is_empty());
        assert!(
            h.conversation(target)
                .unwrap()
                .application_writes
                .is_empty()
        );
        assert!(Documents::new(&store).get(&address()).unwrap().is_none());
        if !fail {
            let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
            membership.revoke(session, target).unwrap();
            membership.enroll(session, target).unwrap();
        }
        release_tx.send(()).unwrap();
        wait_task(&path, child.id(), |s| {
            if fail {
                matches!(s, TaskState::Failed(_))
            } else {
                matches!(s, TaskState::Waiting(_))
            }
        });
        let state = h.conversation(target).unwrap();
        assert_eq!(state.application_writes.len(), if fail { 0 } else { 3 });
        assert_eq!(state.submissions.len(), if fail { 0 } else { 3 });
        assert_eq!(
            Documents::new(&store).get(&address()).unwrap().is_some(),
            !fail
        );
        if !fail {
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        }
        driver.shutdown().unwrap();
    }
}

#[cfg(unix)]
struct KillCrossUnkeyed(Uuid);
#[cfg(unix)]
impl Handler for KillCrossUnkeyed {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        use std::io::Write;
        cross_unkeyed_batch(self.0).run_with_documents(task, docs)?;
        println!("CROSS_UNKEYED_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
#[cfg(unix)]
#[test]
fn cross_unkeyed_kill_worker() {
    let Ok(path) = std::env::var("CROSS_UNKEYED_DB") else {
        return;
    };
    let owner = std::env::var("CROSS_UNKEYED_OWNER")
        .unwrap()
        .parse()
        .unwrap();
    let target = std::env::var("CROSS_UNKEYED_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("batch", true, KillCrossUnkeyed(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = Task::new(
        Uuid::from_u128(10031),
        TaskOwner::Conversation(owner),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("batch".into(), true);
    driver.submit(&child).unwrap();
    loop {
        std::thread::park();
    }
}
#[cfg(unix)]
#[test]
fn cross_session_unkeyed_batch_kill_replays_once() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, session) = cross_setup(&path);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "cross_unkeyed_kill_worker", "--nocapture"])
        .env("CROSS_UNKEYED_DB", &path)
        .env("CROSS_UNKEYED_OWNER", owner.to_string())
        .env("CROSS_UNKEYED_TARGET", target.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("CROSS_UNKEYED_STAGED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let reached = rx.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    reached.expect("child did not stage batch");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(target).unwrap().submissions.is_empty());
    assert!(
        h.conversation(target)
            .unwrap()
            .application_writes
            .is_empty()
    );
    let store = Store::open(&path).unwrap();
    let binding_key = |name: &str| {
        let hex: String = name.bytes().map(|byte| format!("{byte:02x}")).collect();
        format!(
            "durable/owned-subagent-notify/{}/{hex}",
            Uuid::from_u128(10031)
        )
        .into_bytes()
    };
    assert!(store.get(&binding_key("one")).unwrap().is_none());
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    membership.revoke(session, target).unwrap();
    membership.enroll(session, target).unwrap();
    let mut registry = Registry::default();
    registry.register("batch", true, cross_unkeyed_batch(target));
    let driver = h.start_tasks(registry).unwrap();
    wait_task(&path, Uuid::from_u128(10031), |s| {
        matches!(s, TaskState::Waiting(_))
    });
    let state = h.conversation(target).unwrap();
    assert_eq!(state.application_writes.len(), 3);
    assert_eq!(state.submissions.len(), 3);
    assert!(state.submissions.iter().all(|s| s.request_id.is_none()));
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(target).unwrap();
    assert_eq!(state.application_writes.len(), 3);
    assert_eq!(state.submissions.len(), 3);
    let binding = store.get(&binding_key("one")).unwrap().unwrap();
    let bound: (Uuid, Uuid) = serde_json::from_slice(&binding.value).unwrap();
    assert_eq!(bound, (target, state.application_writes[0].id));
}

#[test]
fn cross_session_unkeyed_batch_abort_discards_all_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut handler = cross_unkeyed_batch(target);
    handler.staged = Some(staged_tx);
    handler.release = Some(std::sync::Mutex::new(release_rx));
    let mut registry = Registry::default();
    registry.register("batch", true, handler);
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "batch");
    driver.submit(&child).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.request_abort(target).unwrap();
    release_tx.send(()).unwrap();
    wait_task(&path, child.id(), |s| {
        matches!(s, TaskState::Failed(_) | TaskState::Cancelled)
    });
    let state = h.conversation(target).unwrap();
    assert!(state.submissions.is_empty());
    assert!(state.application_writes.is_empty());
    let store = Store::open(&path).unwrap();
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

struct CrossUnkeyedLimit(Uuid);
impl Handler for CrossUnkeyedLimit {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        for n in 0..16 {
            docs.submit_write_to_session_conversation_without_request_id(
                self.0,
                "app.limit".into(),
                Some(json!(n)),
                None,
            )
            .map_err(|e| e.to_string())?;
        }
        assert!(
            docs.submit_write_to_session_conversation_without_request_id(
                self.0,
                "app.overflow".into(),
                None,
                None
            )
            .is_err()
        );
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn cross_session_unkeyed_batch_limit_retains_first_sixteen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let mut registry = Registry::default();
    registry.register("limit", true, CrossUnkeyedLimit(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "limit");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    let state = h.conversation(target).unwrap();
    assert_eq!(state.application_writes.len(), 16);
    assert!(
        state
            .application_writes
            .iter()
            .all(|write| write.request_id.is_none())
    );
    assert_eq!(
        state
            .application_writes
            .iter()
            .map(|write| write.id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        16
    );
    assert_eq!(
        state
            .application_writes
            .iter()
            .map(|write| write.data.clone())
            .collect::<Vec<_>>(),
        (0..16).map(|n| Some(json!(n))).collect::<Vec<_>>()
    );
    driver.shutdown().unwrap();
}

fn cross_two_succeeded_owners(
    store: &Store,
    owner: Uuid,
    target: Uuid,
    mode: pi_rs_durable::ownership_edge::Mode,
) -> (Uuid, Uuid) {
    use pi_rs_durable::{
        ownership_edge::{Edge, EdgeStore},
        task_store::Save,
    };
    let root = task(owner, "completed-root");
    let middle = descendant(root.id(), "completed-middle");
    let tasks = TaskStore::new(store);
    tasks
        .save(&[Save::Create(&root), Save::Create(&middle)])
        .unwrap();
    let edge = EdgeStore::new(store)
        .prepare_create(&Edge {
            task: middle.id(),
            task_owner: TaskOwner::Task(root.id()),
            child: target,
            mode,
        })
        .unwrap();
    store.commit(&edge.checks, &edge.mutations).unwrap();
    for id in [middle.id(), root.id()] {
        let snapshot = tasks.load(id).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| {
                task.start()?;
                task.succeed(json!("completed"))
            })
            .unwrap();
    }
    (root.id(), middle.id())
}

#[tokio::test]
async fn cross_session_multilevel_successful_target_owners_survive_reopen() {
    for operation in ["input", "continue", "write"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (h, owner, target, session) = cross_setup(&path);
        let store = Store::open(&path).unwrap();
        let (root, middle) = cross_two_succeeded_owners(
            &store,
            owner,
            target,
            pi_rs_durable::ownership_edge::Mode::Foreground,
        );
        for id in [root, middle] {
            assert!(matches!(
                TaskStore::new(&store)
                    .load(id)
                    .unwrap()
                    .unwrap()
                    .task
                    .state(),
                TaskState::Succeeded(_)
            ));
        }
        drop(h);
        let mut h = Harness::open_paused(&path, options()).unwrap();
        pi_rs_durable::session_membership::SessionMembership::new(&store)
            .authorize_pair(session, owner, target)
            .unwrap()
            .commit(&[], &[])
            .unwrap();
        let mut registry = Registry::default();
        if operation == "input" {
            registry.register("cross", true, cross_handler(target));
        } else {
            registry.register("action", true, cross_action(target, operation == "write"));
        }
        let driver = h.start_tasks(registry).unwrap();
        let child = task(
            owner,
            if operation == "input" {
                "cross"
            } else {
                "action"
            },
        );
        driver.submit(&child).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state = TaskStore::new(&store)
                .load(child.id())
                .unwrap()
                .unwrap()
                .task
                .state()
                .clone();
            if matches!(state, TaskState::Waiting(_) | TaskState::Succeeded(_)) {
                break;
            }
            assert!(
                !matches!(state, TaskState::Failed(_) | TaskState::Cancelled),
                "{operation} rejected: {state:?}"
            );
            assert!(
                Instant::now() < deadline,
                "{operation} did not settle: {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if operation != "write" {
            h.resume_conversation(target, &CancellationToken::new())
                .await
                .unwrap();
        }
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Succeeded(_))
        });
        let result = h.conversation(target).unwrap();
        if operation == "write" {
            assert_eq!(result.application_writes.len(), 1);
            assert!(result.application_writes[0].placed);
        } else {
            assert_eq!(result.submissions.len(), 1);
            assert!(matches!(
                result.submissions[0].status,
                Status::Done { .. } | Status::Unanswered { .. }
            ));
        }
        let key = if operation == "input" {
            "cross-settled"
        } else {
            "action-settled"
        };
        let receipt = TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), key)
            .unwrap()
            .unwrap();
        assert_eq!(receipt["conversationId"], target.to_string());
        assert_eq!(receipt["type"], operation);
        driver.shutdown().unwrap();
        drop(h);
        let h = Harness::open_paused(&path, options()).unwrap();
        pi_rs_durable::session_membership::SessionMembership::new(&store)
            .authorize_pair(session, owner, target)
            .unwrap()
            .commit(&[], &[])
            .unwrap();
        assert_eq!(
            h.conversation(target).unwrap().application_writes.len(),
            usize::from(operation == "write")
        );
        assert_eq!(
            h.conversation(target).unwrap().submissions.len(),
            usize::from(operation != "write")
        );
    }
}

fn cross_rewrite_owner_state(store: &Store, id: Uuid, state: serde_json::Value, abort: bool) {
    let (key, entry) = store
        .list(b"durable/task/")
        .unwrap()
        .into_iter()
        .find(|(_, entry)| {
            serde_json::from_slice::<Task>(&entry.value).is_ok_and(|task| task.id() == id)
        })
        .unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
    value["state"] = state;
    value["abort_requested"] = abort.into();
    store
        .commit(
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

#[test]
fn cross_session_multilevel_failed_cancelled_and_aborting_ancestors_block() {
    for index in [0, 1] {
        for mode in ["failed", "cancelled", "completing", "abort"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let (h, owner, target, _) = cross_setup(&path);
            let store = Store::open(&path).unwrap();
            let (root, middle) = cross_two_succeeded_owners(
                &store,
                owner,
                target,
                pi_rs_durable::ownership_edge::Mode::Foreground,
            );
            let ancestor = if index == 0 { root } else { middle };
            let state = match mode {
                "failed" => json!({"Failed":"failed"}),
                "cancelled" => json!("Cancelled"),
                "completing" => json!({"CompletingFailed":"pending"}),
                "abort" => json!({"Succeeded":null}),
                _ => unreachable!(),
            };
            cross_rewrite_owner_state(&store, ancestor, state, mode == "abort");
            let mut registry = Registry::default();
            registry.register("cross", true, cross_handler(target));
            let driver = h.start_tasks(registry).unwrap();
            let child = task(owner, "cross");
            driver.submit(&child).unwrap();
            wait_task(&path, child.id(), |state| {
                matches!(state, TaskState::Failed(_) | TaskState::Cancelled)
            });
            assert!(
                h.conversation(target).unwrap().submissions.is_empty(),
                "{index} {mode}"
            );
            assert!(Documents::new(&store).get(&address()).unwrap().is_none());
            driver.shutdown().unwrap();
        }
    }
}

#[test]
fn cross_session_multilevel_background_boundary_ignores_failed_outer_owner_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let store = Store::open(&path).unwrap();
    let (root, _) = cross_two_succeeded_owners(
        &store,
        owner,
        target,
        pi_rs_durable::ownership_edge::Mode::Background,
    );
    cross_rewrite_owner_state(&store, root, json!({"Failed":"outside boundary"}), false);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("cross", true, cross_handler(target));
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "cross");
    driver.submit(&child).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Waiting(_))
    });
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    driver.shutdown().unwrap();
}

struct CrossReservationSteps(Uuid);
impl Handler for CrossReservationSteps {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let notify = |key: &str| {
            Some(SubmissionNotify {
                task_id: task.id(),
                key: key.into(),
            })
        };
        if task.checkpoint().phase == "start" {
            docs.submit_write_to_session_conversation(
                self.0,
                "stable-reservation".into(),
                "app.first".into(),
                None,
                notify("receipt"),
            )
            .map_err(|e| e.to_string())?;
            docs.submit_input_to_session_conversation_without_request_id(
                self.0,
                "pending".into(),
                AdmissionOptions::default(),
                notify("pending"),
            )
            .map_err(|e| e.to_string())?;
            let unrelated = Uuid::new_v4();
            let value = json!({"id": unrelated, "conversationId": self.0, "type":"write", "status":"done", "entry":unrelated});
            docs.signal_at(
                task.id(),
                &submission_settled_signal(),
                "unrelated",
                value,
                0,
            )
            .map_err(|e| e.to_string())?;
            return Ok(Step::Continue(Checkpoint {
                phase: "second".into(),
                data: json!(null),
            }));
        }
        for key in ["receipt", "pending", "unrelated"] {
            assert!(
                docs.submit_write_to_session_conversation_without_request_id(
                    self.0,
                    "app.collision".into(),
                    None,
                    notify(key)
                )
                .is_err(),
                "{key}"
            );
        }
        docs.submit_write_to_session_conversation(
            self.0,
            "stable-reservation".into(),
            "app.ignored".into(),
            None,
            notify("receipt"),
        )
        .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    }
}

#[test]
fn task_notification_reservations_survive_steps_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let mut registry = Registry::default();
    registry.register("reservations", true, CrossReservationSteps(target));
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let child = task(owner, "reservations");
    scheduler.submit(&child).unwrap();
    assert!(scheduler.tick(0).unwrap());
    let store = Store::open(&path).unwrap();
    assert_eq!(h.conversation(target).unwrap().application_writes.len(), 1);
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "receipt")
            .unwrap()
            .is_some()
    );
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "unrelated")
            .unwrap()
            .is_some()
    );
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(child.id(), &submission_settled_signal(), "pending")
            .unwrap()
            .is_none()
    );
    drop(scheduler);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("reservations", true, CrossReservationSteps(target));
    let mut scheduler = h.attach_tasks(registry).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(matches!(
        TaskStore::new(&store)
            .load(child.id())
            .unwrap()
            .unwrap()
            .task
            .state(),
        TaskState::Succeeded(_)
    ));
    let state = h.conversation(target).unwrap();
    assert_eq!(state.application_writes.len(), 1);
    assert_eq!(state.submissions.len(), 1);
}

struct ReservationRace {
    target: Uuid,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl Handler for ReservationRace {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let result = docs.submit_write_to_session_conversation_without_request_id(
            self.target,
            "app.race".into(),
            None,
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "raced".into(),
            }),
        );
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call > 0 {
            assert!(result.is_err());
            return Ok(Step::Succeed(json!("collision")));
        }
        result.map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"should_not_commit":true})
                .as_object()
                .unwrap()
                .clone(),
        )
        .map_err(|e| e.to_string())?;
        self.staged.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!("unexpected")))
    }
}

#[test]
fn task_notification_reservation_races_legacy_pending_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, owner, target, _) = cross_setup(&path);
    let (staged, observed) = std::sync::mpsc::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "reservation-race",
        true,
        ReservationRace {
            target,
            staged,
            release: std::sync::Mutex::new(wait),
            calls: calls.clone(),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "reservation-race");
    driver.submit(&child).unwrap();
    observed.recv_timeout(Duration::from_secs(10)).unwrap();
    h.submit_with_notification(
        owner,
        None,
        "host pending".into(),
        AdmissionOptions::default(),
        SubmissionNotify {
            task_id: child.id(),
            key: "raced".into(),
        },
    )
    .unwrap();
    release.send(()).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        h.conversation(target)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    driver.shutdown().unwrap();
}

#[test]
fn task_notification_reservation_races_retained_signal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, owner, target, _) = cross_setup(&path);
    let (staged, observed) = std::sync::mpsc::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "reservation-race",
        true,
        ReservationRace {
            target,
            staged,
            release: std::sync::Mutex::new(wait),
            calls: calls.clone(),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let child = task(owner, "reservation-race");
    driver.submit(&child).unwrap();
    observed.recv_timeout(Duration::from_secs(10)).unwrap();
    let store = Store::open(&path).unwrap();
    let id = Uuid::new_v4();
    let value = json!({"id":id,"conversationId":owner,"type":"write","status":"done","entry":id});
    let signal = pi_rs_durable::task_store::Signal::new_typed(
        Uuid::new_v4(),
        child.id(),
        &submission_settled_signal(),
        "raced",
        value,
    )
    .unwrap();
    TaskStore::new(&store).send_signal(&signal).unwrap();
    release.send(()).unwrap();
    wait_task(&path, child.id(), |state| {
        matches!(state, TaskState::Succeeded(_))
    });
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        h.conversation(target)
            .unwrap()
            .application_writes
            .is_empty()
    );
    assert!(Documents::new(&store).get(&address()).unwrap().is_none());
    driver.shutdown().unwrap();
}

struct LocalReservationSteps;
impl Handler for LocalReservationSteps {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let notify = |key: &str| {
            Some(SubmissionNotify {
                task_id: task.id(),
                key: key.into(),
            })
        };
        if task.checkpoint().phase == "start" {
            docs.submit_write(
                "local-stable".into(),
                "app.first".into(),
                None,
                notify("receipt"),
            )
            .map_err(|e| e.to_string())?;
            docs.submit_input_without_request_id(
                "pending".into(),
                AdmissionOptions::default(),
                notify("pending"),
            )
            .map_err(|e| e.to_string())?;
            let id = Uuid::new_v4();
            let owner = match task.owner() {
                TaskOwner::Conversation(id) => *id,
                _ => unreachable!(),
            };
            let value = json!({"id":id,"conversationId":owner,"type":"write","status":"done","entry":id});
            docs.signal_at(
                task.id(),
                &submission_settled_signal(),
                "unrelated",
                value,
                0,
            )
            .map_err(|e| e.to_string())?;
            return Ok(Step::Continue(Checkpoint {
                phase: "second".into(),
                data: json!(null),
            }));
        }
        for key in ["receipt", "pending", "unrelated"] {
            assert!(
                docs.submit_write_without_request_id(
                    "app.collision".into(),
                    None,
                    notify(key)
                )
                .is_err(),
                "{key}"
            );
        }
        docs.submit_write(
            "local-stable".into(),
            "app.ignored".into(),
            None,
            notify("receipt"),
        )
        .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    }
}

#[test]
fn local_notification_reservations_survive_steps_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, conversation) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "local-reservations",
        true,
        LocalReservationSteps,
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let child = task(conversation, "local-reservations");
    scheduler.submit(&child).unwrap();
    assert!(scheduler.tick(0).unwrap());
    let store = Store::open(&path).unwrap();
    let before = h.conversation(conversation).unwrap();
    assert_eq!(before.application_writes.len(), 1);
    assert_eq!(before.submissions.len(), 1);
    for key in ["receipt", "unrelated"] {
        assert!(
            TaskStore::new(&store)
                .read_typed_signal(
                    child.id(),
                    &submission_settled_signal(),
                    key
                )
                .unwrap()
                .is_some()
        );
    }
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(
                child.id(),
                &submission_settled_signal(),
                "pending"
            )
            .unwrap()
            .is_none()
    );
    drop(scheduler);
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register(
        "local-reservations",
        true,
        LocalReservationSteps,
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(matches!(
        TaskStore::new(&store)
            .load(child.id())
            .unwrap()
            .unwrap()
            .task
            .state(),
        TaskState::Succeeded(_)
    ));
    let state = h.conversation(conversation).unwrap();
    assert_eq!(state.application_writes.len(), 1);
    assert_eq!(state.submissions.len(), 1);
}

struct LocalReservationRace {
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
impl Handler for LocalReservationRace {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        let result = docs.submit_write_without_request_id(
            "app.race".into(),
            None,
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "local-race".into(),
            }),
        );
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            > 0
        {
            assert!(result.is_err());
            return Ok(Step::Succeed(json!(null)));
        }
        result.map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"no":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        self.staged.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!("unexpected")))
    }
}
#[test]
fn local_reservation_races_existing_pending_and_receipt() {
    for receipt in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut h, conversation) = setup(&path);
        let (staged, observed) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register(
            "local-race",
            true,
            LocalReservationRace {
                staged,
                release: std::sync::Mutex::new(wait),
                calls: calls.clone(),
            },
        );
        let driver = h.start_tasks(registry).unwrap();
        let child = task(conversation, "local-race");
        driver.submit(&child).unwrap();
        observed.recv_timeout(Duration::from_secs(10)).unwrap();
        let store = Store::open(&path).unwrap();
        if receipt {
            let id = Uuid::new_v4();
            let value = json!({"id":id,"conversationId":conversation,"type":"write","status":"done","entry":id});
            let signal = pi_rs_durable::task_store::Signal::new_typed(
                Uuid::new_v4(),
                child.id(),
                &submission_settled_signal(),
                "local-race",
                value,
            )
            .unwrap();
            TaskStore::new(&store).send_signal(&signal).unwrap();
        } else {
            h.submit_with_notification(
                conversation,
                None,
                "pending".into(),
                AdmissionOptions::default(),
                SubmissionNotify {
                    task_id: child.id(),
                    key: "local-race".into(),
                },
            )
            .unwrap();
        }
        release.send(()).unwrap();
        wait_task(&path, child.id(), |state| {
            matches!(state, TaskState::Succeeded(_))
        });
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(
            h.conversation(conversation)
                .unwrap()
                .application_writes
                .is_empty()
        );
        assert!(
            Documents::new(&store).get(&address()).unwrap().is_none()
        );
        driver.shutdown().unwrap();
    }
}

struct LegacyDuplicate;
impl Handler for LegacyDuplicate {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        docs.submit_write(
            "legacy-key".into(),
            "app.ignored".into(),
            None,
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "changed".into(),
            }),
        )
        .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn local_keyed_duplicate_preserves_legacy_notification_without_binding()
{
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut h, conversation) = setup(&path);
    let mut registry = Registry::default();
    registry.register("legacy-duplicate", true, LegacyDuplicate);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let child = task(conversation, "legacy-duplicate");
    scheduler.submit(&child).unwrap();
    let original = h
        .submit_write_with_notification(
            conversation,
            Some("legacy-key".into()),
            "app.original".into(),
            None,
            SubmissionNotify {
                task_id: child.id(),
                key: "legacy".into(),
            },
        )
        .unwrap();
    let store = Store::open(&path).unwrap();
    let key = format!(
        "durable/owned-subagent-notify/{}/6c6567616379",
        child.id()
    );
    assert!(store.get(key.as_bytes()).unwrap().is_none());
    assert!(scheduler.tick(0).unwrap());
    assert!(matches!(
        scheduler.get(child.id()).unwrap().unwrap().state(),
        TaskState::Succeeded(_)
    ));
    let state = h.conversation(conversation).unwrap();
    assert_eq!(state.application_writes.len(), 1);
    assert_eq!(state.application_writes[0].id, original);
    assert_eq!(
        state.submission_notification(original).unwrap().key,
        "legacy"
    );
    assert!(store.get(key.as_bytes()).unwrap().is_none());
}

struct RejectMalformedReceipt;
impl Handler for RejectMalformedReceipt {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        task: &Task,
        docs: &mut TaskDocuments<'_>,
    ) -> Result<Step, String> {
        assert!(
            docs.submit_write_without_request_id(
                "app.must-not-commit".into(),
                None,
                Some(SubmissionNotify {
                    task_id: task.id(),
                    key: "malformed".into()
                })
            )
            .is_err()
        );
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn local_reservation_fails_closed_on_malformed_signal_storage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (h, conversation) = setup(&path);
    let mut registry = Registry::default();
    registry.register(
        "malformed-receipt",
        true,
        RejectMalformedReceipt,
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let child = task(conversation, "malformed-receipt");
    scheduler.submit(&child).unwrap();
    let store = Store::open(&path).unwrap();
    let mut signal_key = b"durable/signal/".to_vec();
    signal_key.extend_from_slice(child.id().as_bytes());
    signal_key.extend_from_slice(Uuid::new_v4().as_bytes());
    store
        .commit(
            &[durable_store::Check {
                key: signal_key.clone(),
                expected: durable_store::Expected::Missing,
            }],
            &[durable_store::Mutation::Put {
                key: signal_key,
                value: b"{".to_vec(),
            }],
        )
        .unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(
        h.conversation(conversation)
            .unwrap()
            .application_writes
            .is_empty()
    );
    let binding = format!(
        "durable/owned-subagent-notify/{}/6d616c666f726d6564",
        child.id()
    );
    assert!(store.get(binding.as_bytes()).unwrap().is_none());
}
