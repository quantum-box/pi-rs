use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
        DurableToolStep, DurableWait, ReplaySafeAuthorization,
        register_conversation_replay_safe_durable_tool,
    },
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler},
    task_scheduler::Registry,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

struct ModelStub {
    arguments: String,
}
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let done = request
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }));
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: if done {
                vec![]
            } else {
                vec![ToolCall {
                    id: "call".into(),
                    name: "root".into(),
                    arguments: self.arguments.clone(),
                }]
            },
            usage: None,
        })])
        .boxed())
    }
}
struct AsyncTool {
    name: &'static str,
    child: Option<&'static str>,
    gate: Option<Arc<std::sync::atomic::AtomicBool>>,
    early: bool,
}
#[async_trait]
impl Tool for AsyncTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        panic!("ordinary invoke")
    }
}
#[async_trait]
impl DurableAsyncTool for AsyncTool {
    async fn execute_durable(
        &self,
        _: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        let Some(child_name) = self.child else {
            if let Some(gate) = &self.gate {
                gate.store(true, std::sync::atomic::Ordering::SeqCst);
                loop {
                    tokio::select! {
                        _ = context.cancellation.cancelled() => return Err("cancelled".into()),
                        _ = tokio::time::sleep(Duration::from_millis(10)) => if !gate.load(std::sync::atomic::Ordering::SeqCst) { break; },
                    }
                }
            }
            if let Some(path) = std::env::var_os("PI_CONV_ASYNC_KILL_READY") {
                std::fs::write(path, "leaf-running").unwrap();
                std::future::pending::<()>().await;
            }
            return Ok(DurableToolStep::Complete(Outcome::Succeeded("leaf".into())));
        };
        if let Some(checkpoint) = resume.checkpoint {
            let child: AsyncTaskCall =
                serde_json::from_value(checkpoint).map_err(|e| e.to_string())?;
            if !resume.ended.contains(&child.task_id()) {
                return Err("missing wake".into());
            }
            if !matches!(
                host.child_result(&child).await?,
                Some(AsyncTaskResult::Outcome(Outcome::Succeeded(_)))
            ) {
                return Err("missing result".into());
            }
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                self.name.into(),
            )))
        } else {
            let child = host.start_child("key", child_name, json!({})).await?;
            if self.early {
                return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                    self.name.into(),
                )));
            }
            Ok(DurableToolStep::Park {
                checkpoint: serde_json::to_value(&child).unwrap(),
                wait: DurableWait::All(vec![child]),
            })
        }
    }
}
fn setup() -> (Options, Registry) {
    setup_with_gate(None, false, "{}")
}
fn setup_with_gate(
    gate: Option<Arc<std::sync::atomic::AtomicBool>>,
    early: bool,
    arguments: &str,
) -> (Options, Registry) {
    let mut registry = Registry::default();
    let mut adapters = vec![];
    let mut child = None;
    for (name, next) in [
        ("leaf", None),
        ("middle", Some("leaf")),
        ("root", Some("middle")),
    ] {
        let adapter = register_conversation_replay_safe_durable_tool(
            &mut registry,
            Arc::new(AsyncTool {
                name,
                child: next,
                early: early && name == "root",
                gate: if next.is_none() { gate.clone() } else { None },
            }),
            name,
            1,
            child.into_iter().collect(),
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
        adapters.push(adapter.register());
        child = Some(adapter);
    }
    (
        Options {
            model: Arc::new(ModelStub {
                arguments: arguments.into(),
            }),
            tools: adapters,
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
    )
}
#[tokio::test]
async fn conversation_executes_two_park_levels() {
    let dir = tempfile::tempdir().unwrap();
    let (options, registry) = setup();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    let done = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c = scheduler.conversation(id).unwrap();
            if c.messages
                .iter()
                .any(|m| matches!(m,Message::Tool { content,.. } if content == "root"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if let Err(error) = done {
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        panic!("{error}: {tasks:?}; failure: {:?}", report.failure());
    }
    driver.shutdown().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn conversation_checkpointable_kill_worker() {
    let Some(directory) = std::env::var_os("PI_CONV_ASYNC_KILL_DIR") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let (options, registry) = setup();
    let mut scheduler = Scheduler::open(directory.join("db"), options).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(directory.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}

#[cfg(unix)]
#[tokio::test]
async fn conversation_two_park_levels_survive_kill_and_reopen() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut child = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "conversation_checkpointable_kill_worker"])
            .env("PI_CONV_ASYNC_KILL_DIR", dir.path())
            .env("PI_CONV_ASYNC_KILL_READY", dir.path().join("ready"))
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !dir.path().join("ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let id = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert!(
        tasks
            .iter()
            .filter(|t| matches!(t.task.state(), pi_rs_durable::task::TaskState::Waiting(_)))
            .count()
            >= 2
    );
    drop(store);
    let (options, registry) = setup();
    let scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let c = scheduler.conversation(id).unwrap();
            if c.messages
                .iter()
                .any(|m| matches!(m, Message::Tool { content, .. } if content == "root"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn conversation_abort_cancels_running_child_and_drains_two_parks() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (options, registry) = setup_with_gate(Some(gate.clone()), false, "{}");
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !gate.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
        .await
        .unwrap()
        .unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert_eq!(tasks.len(), 3);
    assert!(tasks.iter().all(|t| t.task.is_terminal()));
    assert!(
        tasks
            .iter()
            .all(|t| !matches!(t.task.state(), pi_rs_durable::task::TaskState::Succeeded(_)))
    );
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn retained_children_complete_after_parent_success() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (options, registry) = setup_with_gate(Some(gate.clone()), true, "{}");
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !gate.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert!(tasks.iter().any(|t| matches!(t.task.owner(), pi_rs_durable::task::TaskOwner::Conversation(owner) if *owner == id) && matches!(t.task.state(), pi_rs_durable::task::TaskState::Completing(_))));
    gate.store(false, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c = scheduler.conversation(id).unwrap();
            if c.messages
                .iter()
                .any(|m| matches!(m, Message::Tool { content, .. } if content == "root"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn invalid_arguments_return_failed_tool_result_without_task_execution() {
    for arguments in [
        "{",
        "[]",
        &format!("{{\"value\":\"{}\"}}", "x".repeat(16 * 1024 * 1024)),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (options, registry) = setup_with_gate(None, false, arguments);
        let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async { loop { let c = scheduler.conversation(id).unwrap(); if let Some(position) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content.contains("invalid checkpointable arguments"))) && c.messages[position + 1..].iter().any(|m| matches!(m, Message::Assistant { .. })) { break; } tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        assert!(
            pi_rs_durable::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .is_empty()
        );
        driver.shutdown().unwrap();
    }
}
