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

#[cfg(unix)]
struct KillAfterStage;
#[cfg(unix)]
struct SpawnKill(Task);
#[cfg(unix)]
impl Handler for SpawnKill {
    fn run(&self, task: &Task) -> Result<Step, String> {
        if task.checkpoint().phase == "waiting" {
            return Ok(Step::Succeed(json!(null)));
        }
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: json!(null),
            },
            condition: pi_rs_durable::task::WaitFor::Tasks(vec![self.0.id()]),
            children: vec![self.0.clone()],
        })
    }
}
#[cfg(unix)]
impl Handler for KillAfterStage {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        use std::io::Write;
        docs.submit_input("stable-request".into(), "hello".into())
            .map_err(|e| e.to_string())?;
        docs.get(&address()).map_err(|e| e.to_string())?;
        docs.put(
            &address(),
            json!({"committed":true}).as_object().unwrap().clone(),
        )
        .map_err(|e| e.to_string())?;
        println!("TASK_INPUT_STAGED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(unix)]
#[test]
fn task_input_kill_child() {
    let Ok(path) = std::env::var("PI_TASK_INPUT_KILL_DB") else {
        return;
    };
    let conversation =
        Uuid::parse_str(&std::env::var("PI_TASK_INPUT_KILL_CONVERSATION").unwrap()).unwrap();
    let h = Harness::open_paused(path, options()).unwrap();
    let parent = Uuid::from_u128(1000);
    let task = Task::new(
        Uuid::from_u128(1001),
        TaskOwner::Task(parent),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("submit".into(), true);
    let mut registry = Registry::default();
    registry.register("submit", true, KillAfterStage);
    registry.register("spawn", true, SpawnKill(task));
    let driver = h.start_tasks(registry).unwrap();
    let root = Task::new(
        parent,
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("spawn".into(), true);
    driver.submit(&root).unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn killed_staged_input_recovers_once() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
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
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "task_input_kill_child", "--nocapture"])
        .env("PI_TASK_INPUT_KILL_DB", &path)
        .env("PI_TASK_INPUT_KILL_CONVERSATION", conversation.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("TASK_INPUT_STAGED") {
                let _ = sent.send(());
                break;
            }
        }
    });
    let observed = received.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    observed.expect("child did not stage input");
    let h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(conversation).unwrap().submissions.is_empty());
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_none()
    );
    let mut registry = Registry::default();
    registry.register("submit", true, Submit { fail: false });
    // The root stays parked until its child finishes; register its definition
    // for the wake following recovery.
    let child = Task::new(
        Uuid::from_u128(1001),
        TaskOwner::Task(Uuid::from_u128(1000)),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("submit".into(), true);
    registry.register("spawn", true, SpawnKill(child));
    let driver = h.start_tasks(registry).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let db = Store::open(&path).unwrap();
        if TaskStore::new(&db)
            .load(Uuid::from_u128(1001))
            .unwrap()
            .unwrap()
            .task
            .is_terminal()
        {
            break;
        }
        assert!(Instant::now() < deadline, "recovered task did not finish");
        std::thread::sleep(Duration::from_millis(10));
    }
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(conversation).unwrap();
    assert_eq!(state.submissions.len(), 1);
    assert_eq!(
        state.submissions[0].request_id.as_deref(),
        Some("stable-request")
    );
    assert!(
        Documents::new(&Store::open(&path).unwrap())
            .get(&address())
            .unwrap()
            .is_some()
    );
}

struct CheckpointOrder;
impl Handler for CheckpointOrder {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.submit_input("order".into(), "hello".into())
            .map_err(|e| e.to_string())?;
        docs.append_entry("app.note", json!(null))
            .map_err(|e| e.to_string())?;
        assert!(matches!(
            docs.checkpoint_runtime(),
            Err(pi_rs_durable::document::Error::InvalidApplicationEntry)
        ));
        Ok(Step::Succeed(json!(null)))
    }
}
#[test]
fn task_input_rejects_conflicting_checkpoint_without_stopping_driver() {
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
    registry.register("order", true, CheckpointOrder);
    let mut scheduler = h.attach_tasks(registry).unwrap();
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
    .registered("order".into(), true);
    scheduler.submit(&task).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(matches!(
        scheduler.get(task.id()).unwrap().unwrap().state(),
        pi_rs_durable::task::TaskState::Succeeded(_)
    ));
    assert_eq!(h.conversation(conversation).unwrap().submissions.len(), 1);
}

struct Answer;
#[async_trait]
impl Model for Answer {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        use futures_util::{StreamExt, stream};
        Ok(stream::iter(vec![
            Ok(pi_rs_core::ModelEvent::Text("answer".into())),
            Ok(pi_rs_core::ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn task_admission_automatically_drives_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = options();
    opts.model = Arc::new(Answer);
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let mut registry = Registry::default();
    registry.register("submit", true, Submit { fail: false });
    let driver = scheduler.start_tasks(registry).unwrap();
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
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = scheduler.conversation(conversation).unwrap();
            if state
                .submissions
                .iter()
                .any(|s| matches!(s.status, pi_rs_durable::runtime::Status::Done { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("task input was not driven");
    tokio::task::spawn_blocking(move || driver.shutdown())
        .await
        .unwrap()
        .unwrap();
    scheduler.shutdown().await.unwrap();
}

struct ReverseCheckpointOrder;
impl Handler for ReverseCheckpointOrder {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.append_entry("app.before", json!(null))
            .map_err(|e| e.to_string())?;
        docs.checkpoint_runtime().map_err(|e| e.to_string())?;
        assert!(matches!(
            docs.submit_input("reverse".into(), "hello".into()),
            Err(pi_rs_durable::document::Error::ReadAfterWrite)
        ));
        Ok(Step::Fail("discard".into()))
    }
}
#[test]
fn checkpoint_then_task_input_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let mut registry = Registry::default();
    registry.register("reverse", true, ReverseCheckpointOrder);
    let mut scheduler = h.attach_tasks(registry).unwrap();
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
    .registered("reverse".into(), true);
    scheduler.submit(&task).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(matches!(
        scheduler.get(task.id()).unwrap().unwrap().state(),
        pi_rs_durable::task::TaskState::Failed(_)
    ));
    assert!(h.conversation(conversation).unwrap().submissions.is_empty());
}

#[cfg(unix)]
#[test]
fn committed_task_input_kill_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_TASK_INPUT_KILL_DB") else {
        return;
    };
    let conversation =
        Uuid::parse_str(&std::env::var("PI_TASK_INPUT_KILL_CONVERSATION").unwrap()).unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    let mut registry = Registry::default();
    registry.register("submit", true, Submit { fail: false });
    let driver = h.start_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::from_u128(1002),
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
        std::thread::sleep(Duration::from_millis(10));
    }
    println!("TASK_INPUT_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn committed_task_input_drives_after_process_kill() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
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
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "committed_task_input_kill_child", "--nocapture"])
        .env("PI_TASK_INPUT_KILL_DB", &path)
        .env("PI_TASK_INPUT_KILL_CONVERSATION", conversation.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("TASK_INPUT_COMMITTED") {
                let _ = sent.send(());
                break;
            }
        }
    });
    let observed = received.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    observed.expect("child did not commit input");
    assert_eq!(
        Store::open(&path)
            .unwrap()
            .list(b"runtime/task-input-wake/")
            .unwrap()
            .len(),
        1
    );
    let mut opts = options();
    opts.model = Arc::new(Answer);
    let scheduler = pi_rs_durable::runtime::Scheduler::open(&path, opts).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = scheduler.conversation(conversation).unwrap();
            if state
                .submissions
                .iter()
                .any(|s| matches!(s.status, pi_rs_durable::runtime::Status::Done { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("committed input did not recover");
    scheduler.shutdown().await.unwrap();
    let state = Harness::open_paused(&path, options())
        .unwrap()
        .conversation(conversation)
        .unwrap();
    assert_eq!(state.submissions.len(), 1);
}
