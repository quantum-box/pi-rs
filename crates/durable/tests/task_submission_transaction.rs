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
        let state = TaskStore::new(&store).load(id).unwrap().unwrap();
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
        docs.signal_at(task.id(), &submission_settled_signal(), "settled", value, 0)
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
#[test]
fn staged_signal_wins_over_duplicate_write_notification() {
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
    assert_eq!(receipt["id"], Uuid::from_u128(777).to_string());
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
