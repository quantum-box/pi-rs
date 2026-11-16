use async_trait::async_trait;
use durable_store::Store;
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    document::{Address, Documents, TaskDocuments},
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
    task::{
        Checkpoint, Task, TaskOwner,
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

struct Unused;
#[async_trait]
impl Model for Unused {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        panic!("unexpected model")
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Unused),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn address() -> Address {
    Address {
        scope: "test".into(),
        name: "task-input".into(),
        key: "".into(),
    }
}
struct Submit {
    fail: bool,
}
impl Handler for Submit {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.submit_input("stable-request".into(), "hello".into())
            .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        Ok(if self.fail {
            Step::Fail("intentional".into())
        } else {
            Step::Succeed(json!(null))
        })
    }
}
fn run(fail: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let mut registry = Registry::default();
    registry.register("submit", true, Submit { fail });
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("submit".into(), true);
    driver.submit(&task).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let db = Store::open(&path).unwrap();
        if TaskStore::new(&db)
            .load(task.id())
            .unwrap()
            .unwrap()
            .task
            .is_terminal()
        {
            break;
        }
        assert!(Instant::now() < deadline, "task did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
    if !fail {
        let duplicate = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("submit".into(), true);
        driver.submit(&duplicate).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let db = Store::open(&path).unwrap();
            if TaskStore::new(&db)
                .load(duplicate.id())
                .unwrap()
                .unwrap()
                .task
                .is_terminal()
            {
                break;
            }
            assert!(Instant::now() < deadline, "duplicate task did not finish");
            std::thread::sleep(Duration::from_millis(10));
        }
        let db = Store::open(&path).unwrap();
        let stored = TaskStore::new(&db).load(duplicate.id()).unwrap().unwrap();
        assert!(matches!(
            stored.task.state(),
            pi_rs_durable::task::TaskState::Succeeded(_)
        ));
    }
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(conversation).unwrap();
    let doc = Documents::new(&Store::open(&path).unwrap())
        .get(&address())
        .unwrap();
    assert_eq!(state.submissions.len(), usize::from(!fail));
    assert_eq!(doc.is_some(), !fail);
    if !fail {
        assert_eq!(
            state.submissions[0].request_id.as_deref(),
            Some("stable-request")
        );
    }
}
#[test]
fn input_and_document_commit_together() {
    run(false);
}
#[test]
fn failed_step_discards_input_and_document() {
    run(true);
}

struct RacingSubmit {
    calls: Arc<std::sync::atomic::AtomicUsize>,
    staged: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Handler for RacingSubmit {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.submit_input("race".into(), "hello".into())
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
fn changed_conversation_retries_whole_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        RacingSubmit {
            calls: calls.clone(),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("race".into(), true);
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.submit_write(conversation, None, "note".into(), None)
        .unwrap();
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    release_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TaskStore::new(&Store::open(&path).unwrap())
            .load(task.id())
            .unwrap()
            .unwrap()
            .task
            .is_terminal()
        {
            break;
        }
        assert!(Instant::now() < deadline, "task did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(conversation).unwrap().submissions.len(), 1);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_some()
    );
}

#[test]
fn abort_discards_staged_input_and_document() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (staged_tx, staged_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut registry = Registry::default();
    registry.register(
        "race",
        true,
        RacingSubmit {
            calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged: staged_tx,
            release: std::sync::Mutex::new(release_rx),
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("race".into(), true);
    driver.submit(&task).unwrap();
    staged_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    driver.request_abort(task.id()).unwrap();
    release_tx.send(()).unwrap();
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(conversation).unwrap().submissions.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
}
