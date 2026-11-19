use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::task::TaskState;
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
        DurableToolStep, DurableWait, ReplaySafeAuthorization, UnsafeDurableAuthorization,
        register_conversation_replay_safe_durable_tool, register_conversation_unsafe_durable_tool,
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
            if self.name == "root"
                && let Some(path) = std::env::var_os("PI_CONV_ASYNC_KILL_WAKE")
            {
                std::fs::write(path, "root-woken").unwrap();
                std::future::pending::<()>().await;
            }
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
fn setup_unsafe_tree(unsafe_leaf: bool) -> (Options, Registry) {
    setup_unsafe_tree_with_gate(unsafe_leaf, None)
}
fn setup_unsafe_tree_with_gate(
    unsafe_leaf: bool,
    gate: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> (Options, Registry) {
    let mut registry = Registry::default();
    let mut adapters = vec![];
    let mut child = None;
    for (name, next) in [
        ("leaf", None),
        ("middle", Some("leaf")),
        ("root", Some("middle")),
    ] {
        let tool = Arc::new(AsyncTool {
            name,
            child: next,
            gate: if name == "leaf" { gate.clone() } else { None },
            early: false,
        });
        let adapter = if name != "leaf" || unsafe_leaf {
            register_conversation_unsafe_durable_tool(
                &mut registry,
                tool,
                name,
                1,
                child.into_iter().collect(),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
            .unwrap()
        } else {
            register_conversation_replay_safe_durable_tool(
                &mut registry,
                tool,
                name,
                1,
                child.into_iter().collect(),
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap()
        };
        adapters.push(adapter.register());
        child = Some(adapter);
    }
    (
        Options {
            model: Arc::new(ModelStub {
                arguments: "{}".into(),
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
async fn conversation_unsafe_tree_executes_two_park_levels() {
    let dir = tempfile::tempdir().unwrap();
    let (options, registry) = setup_unsafe_tree(true);
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
    let result = tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id)).await;
    if !matches!(result, Ok(Ok(_))) {
        let tasks = pi_rs_durable::task_store::TaskStore::new(
            &durable_store::Store::open(dir.path().join("db")).unwrap(),
        )
        .list()
        .unwrap();
        panic!("result: {result:?}, tasks: {tasks:?}");
    }
    let conversation = scheduler.conversation(id).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(
        &durable_store::Store::open(dir.path().join("db")).unwrap(),
    )
    .list()
    .unwrap();
    assert!(
        conversation
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "root")),
        "{conversation:?}, {tasks:?}"
    );
    driver.shutdown().unwrap();
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

#[tokio::test]
async fn unsafe_conversation_rejects_selected_extension_before_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = Registry::default();
    let adapter = register_conversation_unsafe_durable_tool(
        &mut registry,
        Arc::new(UncertainUnsafeTool {
            late: false,
            started: started.clone(),
        }),
        "unhooked-root",
        1,
        vec![],
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![adapter.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install(pi_rs_durable::extension::Extension {
            name: "policy".into(),
            ..Default::default()
        })
        .unwrap();
    let mut scheduler =
        Scheduler::open_with_extensions(dir.path().join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
            .await
            .unwrap()
            .is_err()
    );
    assert!(!started.load(std::sync::atomic::Ordering::SeqCst));
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    assert!(
        pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap()
            .is_empty()
    );
    drop(store);
    let _ = driver.shutdown_with_pending().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn conversation_checkpointable_kill_worker() {
    let Some(directory) = std::env::var_os("PI_CONV_ASYNC_KILL_DIR") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let (options, registry) = match std::env::var("PI_CONV_UNSAFE_TREE").ok().as_deref() {
        Some("unsafe-leaf") => setup_unsafe_tree(true),
        Some("safe-leaf") => setup_unsafe_tree(false),
        _ => setup(),
    };
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
async fn unsafe_park_wakes_after_kill_but_interrupted_unsafe_leaf_is_not_replayed() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for unsafe_leaf in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "conversation_checkpointable_kill_worker"])
                .env("PI_CONV_ASYNC_KILL_DIR", dir.path())
                .env("PI_CONV_ASYNC_KILL_READY", dir.path().join("ready"))
                .env(
                    "PI_CONV_UNSAFE_TREE",
                    if unsafe_leaf {
                        "unsafe-leaf"
                    } else {
                        "safe-leaf"
                    },
                )
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
        assert_eq!(
            tasks
                .iter()
                .filter(|t| matches!(t.task.state(), TaskState::Waiting(_)))
                .count(),
            2
        );
        let leaf = tasks
            .iter()
            .find(|t| t.task.state() == &TaskState::Running)
            .unwrap()
            .task
            .id();
        drop(store);
        let (options, registry) = setup_unsafe_tree(unsafe_leaf);
        let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        let conversation = scheduler.conversation(id).unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        let leaf_task = tasks.iter().find(|t| t.task.id() == leaf).unwrap();
        if unsafe_leaf {
            assert!(
                matches!(leaf_task.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation")),
                "{tasks:?}"
            );
            assert!(
                !conversation
                    .messages
                    .iter()
                    .any(|m| matches!(m, Message::Tool { .. })),
                "{conversation:?}"
            );
        } else {
            assert!(
                matches!(leaf_task.task.state(), TaskState::Succeeded(_)),
                "{tasks:?}"
            );
            assert!(
                conversation
                    .messages
                    .iter()
                    .any(|m| matches!(m, Message::Tool {content,..} if content == "root")),
                "{conversation:?}"
            );
        }
        drop(store);
        let _ = driver.shutdown_with_pending().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn interrupted_woken_unsafe_conversation_root_never_replays() {
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
            .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
            .env("PI_CONV_UNSAFE_TREE", "unsafe-leaf")
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !dir.path().join("wake").exists() {
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
    let (options, registry) = setup_unsafe_tree(true);
    let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    let conversation = scheduler.conversation(id).unwrap();
    assert!(
        !conversation
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. })),
        "{conversation:?}"
    );
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    let root = tasks
        .iter()
        .find(|t| {
            matches!(
                t.task.owner(),
                pi_rs_durable::task::TaskOwner::Conversation(_)
            )
        })
        .unwrap();
    assert!(
        matches!(root.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation")),
        "{tasks:?}"
    );
    assert_eq!(
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root.task.id())
            .unwrap()
            .len(),
        0
    );
    drop(store);
    let _ = driver.shutdown_with_pending().unwrap();
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
async fn unsafe_conversation_abort_drains_children_without_publishing_success() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (options, registry) = setup_unsafe_tree_with_gate(true, Some(gate.clone()));
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
    let abort = tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
        .await
        .unwrap();
    assert!(
        abort.is_err(),
        "unsafe aborted work requires reconciliation"
    );
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert_eq!(tasks.len(), 3);
    assert!(tasks.iter().all(|t| t.task.is_terminal()), "{tasks:?}");
    assert!(
        tasks
            .iter()
            .all(|t| !matches!(t.task.state(), TaskState::Succeeded(_))),
        "{tasks:?}"
    );
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool {content,..} if content == "root"))
    );
    driver.shutdown().unwrap();
}

struct UncertainUnsafeTool {
    late: bool,
    started: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl Tool for UncertainUnsafeTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for UncertainUnsafeTool {
    async fn execute_durable(
        &self,
        _: Value,
        ctx: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.started
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if self.late {
            ctx.cancellation.cancelled().await;
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "external-effect-id".into(),
            )))
        } else {
            Ok(DurableToolStep::Complete(Outcome::Pending {
                reference: "remote-job-id".into(),
            }))
        }
    }
}
#[tokio::test]
async fn unsafe_conversation_retains_pending_and_late_abort_receipts() {
    for late in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut registry = Registry::default();
        let adapter = register_conversation_unsafe_durable_tool(
            &mut registry,
            Arc::new(UncertainUnsafeTool {
                late,
                started: started.clone(),
            }),
            "uncertain-root",
            1,
            vec![],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
        let options = Options {
            model: Arc::new(ModelStub {
                arguments: "{}".into(),
            }),
            tools: vec![adapter.register()],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !started.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if late {
            let _ = tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
                .await
                .unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
                .await
                .unwrap()
                .unwrap();
        }
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        let receipts = pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(
            &store,
            tasks[0].task.id(),
        )
        .unwrap();
        let expected = if late {
            Outcome::Succeeded("external-effect-id".into())
        } else {
            Outcome::Pending {
                reference: "remote-job-id".into(),
            }
        };
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].outcome, expected);
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. }))
        );
        drop(store);
        let _ = driver.shutdown_with_pending().unwrap();
    }
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

struct InvalidPreparedTool(Arc<std::sync::atomic::AtomicUsize>, bool);
#[async_trait]
impl Tool for InvalidPreparedTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: "".into(),
            parameters: json!({"type":"object","required":["required"]}),
        }
    }
    async fn prepare_arguments(&self, _: Value) -> pi_rs_core::ExtensionResult<Value> {
        if let Some(path) = std::env::var_os("PI_UNSAFE_PREP_CUT") {
            std::fs::write(path, "entered").unwrap();
            std::future::pending::<()>().await;
        }
        if self.1 {
            Err("private preparation details".into())
        } else {
            Ok(json!({"wrong":true}))
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for InvalidPreparedTool {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(DurableToolStep::Complete(Outcome::Succeeded(
            "must not execute".into(),
        )))
    }
}
fn invalid_prepared_setup(
    count: Arc<std::sync::atomic::AtomicUsize>,
    fail: bool,
) -> (Options, Registry) {
    let mut registry = Registry::default();
    let adapter = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(InvalidPreparedTool(count, fail)),
        "invalid-prepared",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub {
                arguments: "{}".into(),
            }),
            tools: vec![adapter.register()],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
    )
}
#[tokio::test]
async fn invalid_prepared_arguments_fail_without_tool_execution_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (options, registry) = invalid_prepared_setup(count.clone(), false);
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(10),async { loop { let c=scheduler.conversation(id).unwrap(); if let Some(pos)=c.messages.iter().position(|m| matches!(m,Message::Tool {content,..} if content.contains("normalized arguments do not match schema"))) && c.messages[pos+1..].iter().any(|m| matches!(m,Message::Assistant {..})) { break; } tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
    let (options, registry) = invalid_prepared_setup(count.clone(), false);
    let scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    assert!(scheduler.conversation(id).unwrap().messages.iter().any(|m| matches!(m,Message::Tool {content,..} if content.contains("normalized arguments do not match schema"))));
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}

struct ValidatingRoot(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl Tool for ValidatingRoot {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: "".into(),
            parameters: json!({"type":"object","required":["required"]}),
        }
    }
    async fn prepare_arguments(&self, _: Value) -> pi_rs_core::ExtensionResult<Value> {
        Ok(json!({"required":true}))
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for ValidatingRoot {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if resume.checkpoint.is_some() {
            return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "unexpected".into(),
            )));
        }
        let child = host.start_child("leaf", "leaf", json!({})).await?;
        Ok(DurableToolStep::Park {
            checkpoint: serde_json::to_value(&child).unwrap(),
            wait: DurableWait::All(vec![child]),
        })
    }
}
#[tokio::test]
async fn malformed_resumed_arguments_do_not_run_tool_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut registry = Registry::default();
    let leaf = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(AsyncTool {
            name: "leaf",
            child: None,
            early: false,
            gate: Some(gate.clone()),
        }),
        "leaf",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let root = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(ValidatingRoot(count.clone())),
        "valid-root",
        1,
        vec![leaf.clone()],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root.register(), leaf.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options).unwrap();
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
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    let root=tasks.iter().find(|t| matches!(t.task.owner(),pi_rs_durable::task::TaskOwner::Conversation(owner) if *owner==id)).unwrap();
    assert!(matches!(root.task.state(), TaskState::Waiting(_)));
    let root_id = root.task.id();
    let mut value = serde_json::to_value(&root.task).unwrap();
    value["checkpoint"]["data"]["arguments"] = json!({});
    let mut key = b"durable/task/".to_vec();
    key.extend_from_slice(root_id.as_bytes());
    store
        .commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: durable_store::Expected::Version(root.version),
            }],
            &[durable_store::Mutation::Put {
                key,
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
    gate.store(false, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let task = pi_rs_durable::task_store::TaskStore::new(&store)
                .load(root_id)
                .unwrap()
                .unwrap();
            if matches!(task.task.state(), TaskState::Failed(_)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn prepare_error_is_known_failure_without_execution_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (options, registry) = invalid_prepared_setup(count.clone(), true);
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(10),async { loop { let c=scheduler.conversation(id).unwrap(); if let Some(pos)=c.messages.iter().position(|m| matches!(m,Message::Tool {content,..} if content.contains("argument preparation failed"))) && c.messages[pos+1..].iter().any(|m| matches!(m,Message::Assistant {..})) { break; } tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
    let (options, registry) = invalid_prepared_setup(count.clone(), true);
    let scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let c = scheduler.conversation(id).unwrap();
    assert!(c.messages.iter().any(|m| matches!(m,Message::Tool {content,..} if content.contains("argument preparation failed") && !content.contains("private preparation details"))));
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}

#[cfg(unix)]
struct CrashThenInvalid {
    directory: std::path::PathBuf,
    fail: bool,
}
#[cfg(unix)]
#[async_trait]
impl Tool for CrashThenInvalid {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: "".into(),
            parameters: json!({"type":"object","required":["valid"]}),
        }
    }
    async fn prepare_arguments(&self, _: Value) -> pi_rs_core::ExtensionResult<Value> {
        if self.directory.join("executed").exists() {
            if self.fail {
                Err("retry preparation error".into())
            } else {
                Ok(json!({"invalid":true}))
            }
        } else {
            Ok(json!({"valid":true}))
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[cfg(unix)]
#[async_trait]
impl DurableAsyncTool for CrashThenInvalid {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        std::fs::write(self.directory.join("executed"), "started").unwrap();
        std::future::pending::<()>().await;
        unreachable!()
    }
}
#[cfg(unix)]
fn crash_then_invalid_setup(directory: &std::path::Path, fail: bool) -> (Options, Registry) {
    let mut registry = Registry::default();
    let root = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(CrashThenInvalid {
            directory: directory.to_owned(),
            fail,
        }),
        "crash-then-invalid",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub {
                arguments: "{}".into(),
            }),
            tools: vec![root.register()],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
    )
}
#[cfg(unix)]
#[tokio::test]
async fn crash_then_invalid_prepare_worker() {
    let Some(path) = std::env::var_os("PI_CONV_PREPARE_CRASH_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(path);
    let fail = std::env::var_os("PI_CONV_PREPARE_CRASH_ERROR").is_some();
    let (options, registry) = crash_then_invalid_setup(&dir, fail);
    let mut scheduler = Scheduler::open(dir.join("db"), options).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn crash_after_execution_then_prepare_failure_remains_uncertain() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "crash_then_invalid_prepare_worker"])
            .env("PI_CONV_PREPARE_CRASH_DIR", dir.path());
        if fail {
            cmd.env("PI_CONV_PREPARE_CRASH_ERROR", "1");
        }
        let mut process = Kill(cmd.spawn().unwrap());
        tokio::time::timeout(Duration::from_secs(10), async {
            while !dir.path().join("executed").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let id: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let (options, registry) = crash_then_invalid_setup(dir.path(), fail);
        let scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some((_, reason)) = scheduler.blocked(id).unwrap()
                    && !reason.starts_with("pending task:")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let c = scheduler.conversation(id).unwrap();
        assert!(
            !c.messages.iter().any(|m| matches!(m, Message::Tool { .. })),
            "previous external effect must not be reported as a known failure"
        );
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert!(matches!(tasks[0].task.state(), TaskState::Failed(_)));
        driver.shutdown().unwrap();
    }
}

fn unsafe_invalid_prepared_setup(
    count: Arc<std::sync::atomic::AtomicUsize>,
    fail: bool,
) -> (Options, Registry) {
    let mut registry = Registry::default();
    let adapter = register_conversation_unsafe_durable_tool(
        &mut registry,
        Arc::new(InvalidPreparedTool(count, fail)),
        "unsafe-invalid-prepared",
        1,
        vec![],
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub {
                arguments: "{}".into(),
            }),
            tools: vec![adapter.register()],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
    )
}
#[tokio::test]
async fn unsafe_first_prepare_rejection_reaches_model_without_dispatch() {
    for (fail, expected) in [
        (true, "argument preparation failed"),
        (false, "normalized arguments do not match schema"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (options, registry) = unsafe_invalid_prepared_setup(calls.clone(), fail);
        let mut scheduler = Scheduler::open(&path, options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let c = scheduler.conversation(id).unwrap();
                if let Some(i) = c.messages.iter().position(
                    |m| matches!(m, Message::Tool { content, .. } if content.contains(expected)),
                ) && c.messages[i + 1..]
                    .iter()
                    .any(|m| matches!(m, Message::Assistant { .. }))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(matches!(tasks[0].task.state(), TaskState::Succeeded(_)));
        assert!(
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(
                &store,
                tasks[0].task.id()
            )
            .unwrap()
            .is_empty()
        );
        let entries = store
            .list(format!("durable/unsafe-park-async/{}/", tasks[0].task.id()).as_bytes())
            .unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|(k, _)| k.ends_with(b"/no-dispatch"))
                .count(),
            1
        );
        drop(store);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
        let (options, registry) = unsafe_invalid_prepared_setup(calls.clone(), fail);
        let scheduler = Scheduler::open(&path, options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        assert!(
            scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { content, .. } if content.contains(expected)))
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_prepare_interruption_worker() {
    let Some(dir) = std::env::var_os("PI_UNSAFE_PREP_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (options, registry) =
        unsafe_invalid_prepared_setup(Arc::new(std::sync::atomic::AtomicUsize::new(0)), true);
    let mut scheduler = Scheduler::open(dir.join("db"), options).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn unsafe_prepare_kill_with_existing_intent_remains_unknown() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "unsafe_prepare_interruption_worker"])
            .env("PI_UNSAFE_PREP_DIR", dir.path())
            .env("PI_UNSAFE_PREP_CUT", dir.path().join("entered"))
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !dir.path().join("entered").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let id: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].task.id();
    assert!(
        store
            .list(format!("durable/unsafe-park-async/{task_id}/").as_bytes())
            .unwrap()
            .iter()
            .any(|(k, _)| k.ends_with(b"/intent"))
    );
    drop(store);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (options, registry) = unsafe_invalid_prepared_setup(calls.clone(), true);
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let store = durable_store::Store::open(&path).unwrap();
            let task = pi_rs_durable::task_store::TaskStore::new(&store)
                .load(task_id)
                .unwrap()
                .unwrap()
                .task;
            if task.is_terminal() {
                assert!(matches!(task.state(), TaskState::Failed(_)));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert!(
        scheduler
            .blocked(id)
            .unwrap()
            .is_some_and(|(_, reason)| reason.contains("reconciliation"))
    );
    let c = scheduler.conversation(id).unwrap();
    assert!(!c.messages.iter().any(|m| matches!(m, Message::Tool { .. })));
    let store = durable_store::Store::open(&path).unwrap();
    assert!(
        !store
            .list(format!("durable/unsafe-park-async/{task_id}/").as_bytes())
            .unwrap()
            .iter()
            .any(|(k, _)| k.ends_with(b"/no-dispatch"))
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}

struct UnsafeBadChild(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl Tool for UnsafeBadChild {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "bad".into(),
            description: "".into(),
            parameters: json!({"type":"object","required":["required"]}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for UnsafeBadChild {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        unreachable!()
    }
}
struct ParentOfInvalid;
#[async_trait]
impl Tool for ParentOfInvalid {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for ParentOfInvalid {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if let Some(checkpoint) = resume.checkpoint {
            let child: AsyncTaskCall =
                serde_json::from_value(checkpoint).map_err(|e| e.to_string())?;
            if !matches!(
                host.child_result(&child).await?,
                Some(AsyncTaskResult::Outcome(Outcome::Failed(_)))
            ) {
                return Err("expected known child rejection".into());
            }
            return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "known-child-rejection".into(),
            )));
        }
        let child = host.start_child("bad", "bad", json!({})).await?;
        Ok(DurableToolStep::Park {
            checkpoint: serde_json::to_value(&child).unwrap(),
            wait: DurableWait::All(vec![child]),
        })
    }
}
#[tokio::test]
async fn unsafe_invalid_child_is_known_failure_to_parked_parent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    let child = register_conversation_unsafe_durable_tool(
        &mut registry,
        Arc::new(UnsafeBadChild(calls.clone())),
        "bad-child",
        1,
        vec![],
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    let root = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(ParentOfInvalid),
        "parent-invalid",
        1,
        vec![child.clone()],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root.register(), child.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async { loop { if scheduler.conversation(id).unwrap().messages.iter().any(|m| matches!(m, Message::Tool { content, .. } if content == "known-child-rejection")) { break; } tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

struct BlockingUnsafePrepare {
    entered: Arc<std::sync::atomic::AtomicBool>,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait]
impl Tool for BlockingUnsafePrepare {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn prepare_arguments(&self, _: Value) -> pi_rs_core::ExtensionResult<Value> {
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        std::future::pending::<pi_rs_core::ExtensionResult<Value>>().await
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for BlockingUnsafePrepare {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        unreachable!()
    }
}
#[tokio::test]
async fn abort_during_unsafe_preparation_does_not_publish_no_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    let adapter = register_conversation_unsafe_durable_tool(
        &mut registry,
        Arc::new(BlockingUnsafePrepare {
            entered: entered.clone(),
            calls: calls.clone(),
        }),
        "blocking-prepare",
        1,
        vec![],
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![adapter.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options).unwrap();
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
        while !entered.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
        .await
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(
        !store
            .list(format!("durable/unsafe-park-async/{}/", tasks[0].task.id()).as_bytes())
            .unwrap()
            .iter()
            .any(|(key, _)| key.ends_with(b"/no-dispatch"))
    );
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}

#[tokio::test]
async fn conversation_park_cancel_claim_does_not_dispatch_callback() {
    exercise_conversation_park_claim(false).await;
}

#[tokio::test]
async fn conversation_park_cancel_claim_rejects_changed_conversation_attempt() {
    exercise_conversation_park_claim(true).await;
}

async fn exercise_conversation_park_claim(replace_attempt: bool) {
    use pi_rs_durable::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_park_cancel,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Cancel(Arc<AtomicUsize>);
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    let leaf = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(AsyncTool {
            name: "leaf",
            child: None,
            gate: Some(gate),
            early: false,
        }),
        "leaf",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let root_tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity_tool: Arc<dyn Tool> = root_tool.clone();
    let _identity_handle =
        pi_rs_durable::nested_identity::register(&identity_tool, Some("backend-v1".into()))
            .unwrap();
    let root = register_conversation_unsafe_durable_tool_with_park_cancel(
        &mut registry,
        root_tool,
        "root",
        1,
        vec![leaf.clone()],
        "cancel-v1",
        Arc::new(Cancel(calls.clone())),
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root.register(), leaf.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open(&path, options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store);
    let root_id = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(record) = tasks.list().unwrap().into_iter().find(|r| {
                r.task.owner() == &pi_rs_durable::task::TaskOwner::Conversation(conversation)
                    && matches!(r.task.state(), TaskState::Waiting(_))
            }) {
                break record.task.id();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if replace_attempt {
        let conversation_key = format!("runtime/conversation/{conversation}").into_bytes();
        let entry = store.get(&conversation_key).unwrap().unwrap();
        let mut record: Value = serde_json::from_slice(&entry.value).unwrap();
        record["tool_attempt"]["attempt_id"] = json!(uuid::Uuid::new_v4());
        store
            .commit(
                &[durable_store::Check {
                    key: conversation_key.clone(),
                    expected: durable_store::Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Put {
                    key: conversation_key,
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
    }
    scheduler.abort_task(root_id).unwrap();
    let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store.get(key.as_bytes()).unwrap().is_some()
                || (replace_attempt && tasks.load(root_id).unwrap().unwrap().task.is_terminal())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap().is_none(),
        replace_attempt
    );
    if !replace_attempt {
        let entry = store.get(key.as_bytes()).unwrap().unwrap();
        let claim: Value = serde_json::from_slice(&entry.value).unwrap();
        assert_eq!(claim["task"], json!(root_id));
        assert_eq!(claim["definition_version"], 1);
        assert!(claim["attempt"].as_str().is_some());
        assert!(claim["lease"].as_str().is_some());
        let final_state = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let state = tasks.load(root_id).unwrap().unwrap().task.state().clone();
                if matches!(state, TaskState::Failed(_)) {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(final_state, TaskState::Failed(reason) if reason.contains("reconciliation required"))
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending().unwrap();
    if !replace_attempt {
        // A claim alone is evidence of an unresolved cleanup, never a callback
        // receipt. Reopening must not turn it into a completed callback.
        let reopened = durable_store::Store::open(&path).unwrap();
        assert!(reopened.get(key.as_bytes()).unwrap().is_some());
        let receipt_key = format!("durable/replay-safe-async/{root_id}/abort-after");
        assert!(reopened.get(receipt_key.as_bytes()).unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
