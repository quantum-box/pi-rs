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
