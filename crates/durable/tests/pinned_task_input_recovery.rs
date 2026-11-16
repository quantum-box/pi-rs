#![cfg(unix)]

use async_trait::async_trait;
use durable_store::Store;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error as ModelError, Message, Model, ModelEvent, ModelRequest, ModelStream, ToolCall, ToolSpec,
};
use pi_rs_durable::{
    document::TaskDocuments,
    model::{ProviderAdapter, ProviderAttempt},
    recovery::UncertainPolicy,
    runtime::{
        Config, Harness, Options, Scheduler, Status, SubmissionNotify, submission_settled_signal,
    },
    task::{
        Checkpoint, Task, TaskOwner,
        scheduler::{Handler, Registry, Step},
    },
    task_store::{Save, TaskStore},
    task_tool::TaskTool,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

struct Unused;
#[async_trait]
impl Model for Unused {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        panic!("core model must not run")
    }
}
struct Gate {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl ProviderAdapter for Gate {
    fn id(&self) -> &str {
        "recovery-adapter/v1"
    }
    async fn prepare(
        &self,
        _: &ProviderAttempt,
        request: &ModelRequest,
    ) -> Result<Value, ModelError> {
        Ok(json!({"done": request.messages.iter().any(|m| matches!(m, Message::Tool { .. }))}))
    }
    fn validate(&self, _: &ProviderAttempt, _: &ModelRequest, _: &Value) -> Result<(), ModelError> {
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &ProviderAttempt,
        _: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream, ModelError> {
        let done = body["done"].as_bool().unwrap();
        if !done {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        let mut events = Vec::new();
        if done {
            events.push(Ok(ModelEvent::Text("recovered".into())));
        }
        events.push(Ok(ModelEvent::Complete {
            calls: if done {
                vec![]
            } else {
                vec![ToolCall {
                    id: "call".into(),
                    name: "job".into(),
                    arguments: "{}".into(),
                }]
            },
            usage: None,
        }));
        Ok(stream::iter(events).boxed())
    }
}
fn options(tool: Option<Arc<TaskTool>>) -> Options {
    Options {
        model: Arc::new(Unused),
        tools: tool.into_iter().map(|t| t.register()).collect(),
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
struct Submit(Uuid);
impl Handler for Submit {
    fn run(&self, _: &Task) -> Result<Step, String> {
        unreachable!()
    }
    fn run_with_documents(&self, _: &Task, docs: &mut TaskDocuments<'_>) -> Result<Step, String> {
        docs.submit_input_with_options(
            "wake-request".into(),
            "hello".into(),
            Default::default(),
            Some(SubmissionNotify {
                task_id: self.0,
                key: "settled".into(),
            }),
        )
        .map_err(|e| e.to_string())?;
        Ok(Step::Succeed(json!(null)))
    }
}
fn task(id: Uuid, conversation: Uuid) -> Task {
    Task::new(
        id,
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
}
fn provider() -> Arc<Gate> {
    Arc::new(Gate {
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
    })
}

#[test]
fn persisted_wake_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_PINNED_WAKE_DB") else {
        return;
    };
    let conversation =
        Uuid::parse_str(&std::env::var("PI_PINNED_WAKE_CONVERSATION").unwrap()).unwrap();
    let recipient = Uuid::parse_str(&std::env::var("PI_PINNED_WAKE_RECIPIENT").unwrap()).unwrap();
    let mut h = Harness::open_paused(&path, options(None)).unwrap();
    h.install_provider_adapter(provider()).unwrap();
    let mut registry = Registry::default();
    registry.register("submit", true, Submit(recipient));
    let driver = h.start_tasks(registry).unwrap();
    let sender = task(Uuid::from_u128(123456), conversation).registered("submit".into(), true);
    driver.submit(&sender).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let stored = TaskStore::new(&Store::open(&path).unwrap())
            .load(sender.id())
            .unwrap()
            .unwrap();
        if stored.task.is_terminal() {
            assert!(matches!(
                stored.task.state(),
                pi_rs_durable::task::TaskState::Succeeded(_)
            ));
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        Store::open(&path)
            .unwrap()
            .list(b"runtime/task-input-wake/")
            .unwrap()
            .len(),
        1
    );
    println!("PINNED_WAKE_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn persisted_wake_recovers_with_pinned_tasks_and_notification_after_kill() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options(None)).unwrap();
    h.install_provider_adapter(provider()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.select_provider_adapter(conversation, true).unwrap();
    let recipient = task(Uuid::new_v4(), conversation);
    TaskStore::new(&Store::open(&path).unwrap())
        .save(&[Save::Create(&recipient)])
        .unwrap();
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "persisted_wake_child", "--nocapture"])
        .env("PI_PINNED_WAKE_DB", &path)
        .env("PI_PINNED_WAKE_CONVERSATION", conversation.to_string())
        .env("PI_PINNED_WAKE_RECIPIENT", recipient.id().to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("PINNED_WAKE_COMMITTED") {
                let _ = sent.send(());
                break;
            }
        }
    });
    let observed = received.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    child.wait().unwrap();
    observed.expect("child did not commit task input");
    assert_eq!(
        Store::open(&path)
            .unwrap()
            .list(b"runtime/task-input-wake/")
            .unwrap()
            .len(),
        1
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("task result")))
    });
    let tool = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler);
    let gate = provider();
    let (scheduler, driver) = Scheduler::open_with_pinned_provider_adapter_and_tasks(
        &path,
        options(Some(tool)),
        Default::default(),
        gate.clone(),
        registry,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let state = scheduler.conversation(conversation).unwrap();
    assert_eq!(state.submissions.len(), 1);
    let submission = state.submissions[0].id;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    gate.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(15),
        scheduler.wait_for_submission(
            conversation,
            submission,
            &pi_rs_core::CancellationToken::new(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let state = scheduler.conversation(conversation).unwrap();
    assert!(matches!(state.submissions[0].status, Status::Done { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let receipt = TaskStore::new(&Store::open(&path).unwrap())
        .read_typed_signal(recipient.id(), &submission_settled_signal(), "settled")
        .unwrap()
        .expect("settlement notification missing");
    assert_eq!(receipt["id"], submission.to_string());
    assert_eq!(receipt["conversationId"], conversation.to_string());
    assert_eq!(receipt["status"], "done");
    assert_eq!(receipt["answer"], "recovered");
    scheduler.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || driver.shutdown())
        .await
        .unwrap()
        .unwrap();
}

struct CrossSubmit(Uuid);
impl Handler for CrossSubmit {
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
        docs.submit_input_to_session_conversation(
            self.0,
            "cross-pinned-wake".into(),
            "hello".into(),
            Default::default(),
            Some(SubmissionNotify {
                task_id: task.id(),
                key: "cross-settled".into(),
            }),
        )
        .map_err(|error| error.to_string())?;
        Ok(Step::Wait {
            checkpoint: Checkpoint {
                phase: "waiting".into(),
                data: Value::Null,
            },
            condition: pi_rs_durable::task::WaitFor::TypedSleep {
                until_millis: None,
                signals: vec![submission_settled_signal().key("cross-settled").unwrap()],
            },
            children: vec![],
        })
    }
}

#[test]
fn cross_pinned_committed_wake_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_CROSS_PINNED_WAKE_DB") else {
        return;
    };
    let owner: Uuid = std::env::var("PI_CROSS_PINNED_WAKE_OWNER")
        .unwrap()
        .parse()
        .unwrap();
    let target: Uuid = std::env::var("PI_CROSS_PINNED_WAKE_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let mut h = Harness::open_paused(&path, options(None)).unwrap();
    h.install_provider_adapter(provider()).unwrap();
    let mut registry = Registry::default();
    registry.register("cross-submit", true, CrossSubmit(target));
    let driver = h.start_tasks(registry).unwrap();
    let sender = task(Uuid::from_u128(123457), owner).registered("cross-submit".into(), true);
    driver.submit(&sender).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let store = Store::open(&path).unwrap();
    loop {
        let state = TaskStore::new(&store).load(sender.id()).unwrap().unwrap();
        if matches!(
            state.task.state(),
            pi_rs_durable::task::TaskState::Waiting(_)
        ) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sender did not commit: {:?}",
            state.task.state()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        store
            .get(format!("runtime/task-input-wake/{target}").as_bytes())
            .unwrap()
            .is_some()
    );
    assert_eq!(h.conversation(target).unwrap().submissions.len(), 1);
    assert!(
        TaskStore::new(&store)
            .list()
            .unwrap()
            .iter()
            .all(|task| task.task.owner() != &TaskOwner::Conversation(target))
    );
    assert!(
        TaskStore::new(&store)
            .read_typed_signal(sender.id(), &submission_settled_signal(), "cross-settled")
            .unwrap()
            .is_none()
    );
    println!("CROSS_PINNED_WAKE_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[tokio::test]
async fn cross_session_pinned_wake_recovers_task_tool_and_notification_after_kill() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options(None)).unwrap();
    h.install_provider_adapter(provider()).unwrap();
    let config = || Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let owner = h.create_conversation(config()).unwrap();
    let target = h.create_conversation(config()).unwrap();
    h.select_provider_adapter(target, true).unwrap();
    let store = Store::open(&path).unwrap();
    let membership = pi_rs_durable::session_membership::SessionMembership::new(&store);
    let session = membership.create().unwrap();
    membership.enroll(session, owner).unwrap();
    membership.enroll(session, target).unwrap();
    drop(store);
    drop(h);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cross_pinned_committed_wake_child",
            "--nocapture",
        ])
        .env("PI_CROSS_PINNED_WAKE_DB", &path)
        .env("PI_CROSS_PINNED_WAKE_OWNER", owner.to_string())
        .env("PI_CROSS_PINNED_WAKE_TARGET", target.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sent, received) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("CROSS_PINNED_WAKE_COMMITTED") {
                let _ = sent.send(());
                break;
            }
        }
    });
    let observed = received.recv_timeout(Duration::from_secs(20));
    let _ = child.kill();
    let _ = child.wait();
    observed.expect("child did not commit cross-session wake");
    let store = Store::open(&path).unwrap();
    let wake = format!("runtime/task-input-wake/{target}");
    assert!(store.get(wake.as_bytes()).unwrap().is_some());
    pi_rs_durable::session_membership::SessionMembership::new(&store)
        .authorize_pair(session, owner, target)
        .unwrap()
        .commit(&[], &[])
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let handler: Arc<dyn Handler> = Arc::new(move |_: &Task| {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(Step::Succeed(json!("task result")))
    });
    let tool = Arc::new(TaskTool::new(
        ToolSpec {
            name: "job".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        },
        "job-task",
        true,
        1,
        handler.clone(),
    ));
    let mut registry = Registry::default();
    registry.register_shared("job-task", true, handler);
    registry.register("cross-submit", true, CrossSubmit(target));
    let gate = provider();
    let (scheduler, driver) = Scheduler::open_with_pinned_provider_adapter_and_tasks(
        &path,
        options(Some(tool)),
        Default::default(),
        gate.clone(),
        registry,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let state = scheduler.conversation(target).unwrap();
    assert_eq!(state.submissions.len(), 1);
    let submission = state.submissions[0].id;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        scheduler
            .conversation(owner)
            .unwrap()
            .submissions
            .is_empty()
    );
    gate.release.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(15),
        scheduler.wait_for_submission(target, submission, &pi_rs_core::CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    let sender = Uuid::from_u128(123457);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let done = TaskStore::new(&store)
                .load(sender)
                .unwrap()
                .is_some_and(|task| {
                    matches!(
                        task.task.state(),
                        pi_rs_durable::task::TaskState::Succeeded(_)
                    )
                });
            if done && store.get(wake.as_bytes()).unwrap().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cross-session notification or wake did not settle");
    let state = scheduler.conversation(target).unwrap();
    assert_eq!(state.submissions.len(), 1);
    assert!(matches!(state.submissions[0].status, Status::Done { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let receipt = TaskStore::new(&store)
        .read_typed_signal(sender, &submission_settled_signal(), "cross-settled")
        .unwrap()
        .expect("cross-session notification missing");
    assert_eq!(receipt["id"], submission.to_string());
    assert_eq!(receipt["conversationId"], target.to_string());
    assert_eq!(receipt["status"], "done");
    assert_eq!(receipt["answer"], "recovered");
    let owned = TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .filter(|task| task.task.owner() == &TaskOwner::Conversation(target))
        .collect::<Vec<_>>();
    assert_eq!(owned.len(), 1);
    assert!(matches!(
        owned[0].task.state(),
        pi_rs_durable::task::TaskState::Succeeded(_)
    ));
    scheduler.shutdown().await.unwrap();
    tokio::task::spawn_blocking(move || driver.shutdown())
        .await
        .unwrap()
        .unwrap();
}
