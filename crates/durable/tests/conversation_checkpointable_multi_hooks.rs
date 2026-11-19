use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::{
    async_task_tool::{
        DurableAsyncTool, DurableTaskHost, DurableToolResume, DurableToolStep,
        ReplaySafeAuthorization, ReplaySafeConversationHooksAuthorization,
        register_conversation_replay_safe_durable_tool_with_selected_hooks,
    },
    extension::{BeforeTool, Context, Extension, ExtensionIdentity, Hook},
    recovery::UncertainPolicy,
    runtime::{Config, Options, Scheduler},
    task_scheduler::Registry,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct ModelStub;
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
                    arguments: "{}".into(),
                }]
            },
            usage: None,
        })])
        .boxed())
    }
}
struct OrderedHook {
    id: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    block: bool,
}
#[async_trait]
impl Hook for OrderedHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if ctx.memo.get_or_insert("shared", json!("stable")).await? != "stable" {
            return Err("shared memo changed".into());
        }
        ctx.memo
            .get_or_insert(&format!("before-{}", self.id), json!(self.id))
            .await?;
        self.events
            .lock()
            .unwrap()
            .push(format!("before-{}", self.id));
        cut("before", self.id).await;
        if self.block && self.id == "b2" {
            return Ok(BeforeTool::Block("denied".into()));
        }
        let mut args = call.arguments.clone();
        args.as_object_mut()
            .unwrap()
            .entry("trace")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .unwrap()
            .push(json!(self.id));
        Ok(BeforeTool::Rewrite(args))
    }
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if ctx.memo.get_or_insert("shared", json!("different")).await? != "stable" {
            return Err("shared memo changed".into());
        }
        ctx.memo
            .get_or_insert(&format!("after-{}", self.id), json!(self.id))
            .await?;
        self.events
            .lock()
            .unwrap()
            .push(format!("after-{}", self.id));
        cut("after", self.id).await;
        Ok(Some(json!(format!(
            "{}/{}",
            result.as_str().ok_or("missing text")?,
            self.id
        ))))
    }
}
async fn cut(phase: &str, id: &str) {
    if id == "b2" && std::env::var("PI_MULTI_HOOK_CUT").ok().as_deref() == Some(phase) {
        std::fs::write(std::env::var_os("PI_MULTI_HOOK_READY").unwrap(), phase).unwrap();
        std::future::pending::<()>().await;
    }
}
struct Job {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for Job {
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
impl DurableAsyncTool for Job {
    async fn execute_durable(
        &self,
        args: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if args["trace"] != json!(["b1", "b2", "a"]) {
            return Err("before order or rewrite lost".into());
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(DurableToolStep::Complete(Outcome::Succeeded("tool".into())))
    }
}
fn setup(
    events: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
    block: bool,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    setup_order(events, calls, block, &["b".into(), "a".into()])
}
fn setup_order(
    events: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
    block: bool,
    names: &[String],
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let hook = |id| {
        Arc::new(OrderedHook {
            id,
            events: events.clone(),
            block,
        }) as Arc<dyn Hook>
    };
    let mut extensions = pi_rs_durable::extension::Registry::default();
    for (name, hooks, ids) in [
        ("a", vec![hook("a")], vec!["a"]),
        ("b", vec![hook("b1"), hook("b2")], vec!["b1", "b2"]),
    ] {
        extensions
            .install_identified(
                Extension {
                    name: name.into(),
                    hooks,
                    ..Default::default()
                },
                ExtensionIdentity::new(
                    1,
                    name.into(),
                    ids.into_iter().map(str::to_owned).collect(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let mut registry = Registry::default();
    let adapter = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job { calls }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        names,
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub),
            tools: vec![adapter.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
        extensions,
    )
}
async fn start(scheduler: &mut Scheduler) -> uuid::Uuid {
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["b".into(), "a".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    id
}
async fn wait_task(path: &std::path::Path) -> pi_rs_durable::task::Task {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let store = durable_store::Store::open(path).unwrap();
            if let Some(task) = pi_rs_durable::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .into_iter()
                .find(|t| t.task.is_terminal())
            {
                break task.task;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
async fn wait_result(scheduler: &Scheduler, id: uuid::Uuid) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let c = scheduler.conversation(id).unwrap();
            if let Some(i) = c.messages.iter().position(
                |m| matches!(m, Message::Tool { content, .. } if content == "tool/b1/b2/a"),
            ) && c.messages[i + 1..]
                .iter()
                .any(|m| matches!(m, Message::Assistant { .. }))
            {
                assert_eq!(
                    c.messages
                        .iter()
                        .filter(|m| matches!(m, Message::Tool { .. }))
                        .count(),
                    1
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn ordered_rewrites_share_memo_and_publish_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup(events.clone(), calls.clone(), false);
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    wait_result(&scheduler, id).await;
    assert_eq!(
        *events.lock().unwrap(),
        [
            "before-b1",
            "before-b2",
            "before-a",
            "after-b1",
            "after-b2",
            "after-a"
        ]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn block_short_circuits_before_but_runs_ordered_after() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup(events.clone(), calls.clone(), true);
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    start(&mut scheduler).await;
    let task = wait_task(&path).await;
    let pi_rs_durable::task::TaskState::Succeeded(result) = task.state() else {
        panic!("unexpected state {:?}", task.state());
    };
    assert_eq!(result["task_content_v1"]["isError"], true);
    assert!(
        result["task_content_v1"]["output"][0]["text"]
            .as_str()
            .unwrap()
            .ends_with("/b1/b2/a")
    );
    assert_eq!(
        *events.lock().unwrap(),
        ["before-b1", "before-b2", "after-b1", "after-b2", "after-a"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn kill_worker() {
    let Some(dir) = std::env::var_os("PI_MULTI_HOOK_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (options, registry, extensions) = setup(
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
        false,
    );
    let mut scheduler =
        Scheduler::open_with_extensions(dir.join("db"), options, extensions).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["b".into(), "a".into()])
        .unwrap();
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn interrupted_chain_replays_memo_and_publishes_once() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "kill_worker"])
                .env("PI_MULTI_HOOK_DIR", dir.path())
                .env("PI_MULTI_HOOK_CUT", stage)
                .env("PI_MULTI_HOOK_READY", dir.path().join("ready"))
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
        let id: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        let key = format!("runtime/extension-memo/{}/shared", tasks[0].task.id());
        let memo = store.get(key.as_bytes()).unwrap().unwrap().value;
        drop(store);
        let (wrong_options, wrong_registry, wrong_extensions) = setup_order(
            Arc::new(Mutex::new(vec![])),
            Arc::new(AtomicUsize::new(0)),
            false,
            &["a".into(), "b".into()],
        );
        let wrong =
            Scheduler::open_with_extensions(&path, wrong_options, wrong_extensions).unwrap();
        assert!(wrong.start_tasks(wrong_registry).is_err());
        wrong.shutdown().await.unwrap();
        let events = Arc::new(Mutex::new(vec![]));
        let calls = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup(events.clone(), calls.clone(), false);
        let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        wait_result(&scheduler, id).await;
        let expected = if stage == "before" {
            vec![
                "before-b1",
                "before-b2",
                "before-a",
                "after-b1",
                "after-b2",
                "after-a",
            ]
        } else {
            vec!["after-b1", "after-b2", "after-a"]
        };
        assert_eq!(*events.lock().unwrap(), expected);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            if stage == "before" { 1 } else { 0 }
        );
        let store = durable_store::Store::open(&path).unwrap();
        assert_eq!(store.get(key.as_bytes()).unwrap().unwrap().value, memo);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[test]
fn repeated_selection_is_rejected() {
    let (_, _, extensions) = setup(
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
        false,
    );
    let mut registry = Registry::default();
    let result = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        &["b".into(), "b".into()],
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    );
    assert!(result.is_err());
}

struct SleepHook {
    id: &'static str,
    deadline: i64,
    events: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl Hook for SleepHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if self.id == "b1"
            && let Some(path) = std::env::var_os("PI_REVIEW_KILL_BEFORE_SLEEP")
        {
            std::fs::write(path, b"running").unwrap();
            std::future::pending::<()>().await;
        }
        self.events
            .lock()
            .unwrap()
            .push(format!("before-{}", self.id));
        let saved = ctx
            .memo
            .get_or_insert("sleep-deadline", json!(self.deadline))
            .await?;
        let deadline = saved.as_i64().ok_or("invalid memo")?;
        if self.id == "b2" && deadline == -1 {
            return Ok(BeforeTool::Sleep(pi_rs_durable::extension::Sleep {
                signals: vec![],
                until_millis: None,
            }));
        }
        if self.id == "b2" && now_ms() < deadline {
            return Ok(BeforeTool::Sleep(pi_rs_durable::extension::Sleep {
                signals: vec![],
                until_millis: Some(deadline),
            }));
        }
        let mut args = call.arguments.clone();
        args.as_object_mut()
            .unwrap()
            .entry("trace")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .unwrap()
            .push(json!(self.id));
        Ok(BeforeTool::Rewrite(args))
    }
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        self.events
            .lock()
            .unwrap()
            .push(format!("after-{}", self.id));
        if self.id == "b1" && self.deadline == -1 {
            return Ok(Some(json!("redacted")));
        }
        Ok(Some(json!(format!(
            "{}/{}",
            result.as_str().ok_or("missing text")?,
            self.id
        ))))
    }
}

#[tokio::test]
async fn invalid_sleep_runs_after_without_tool_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_sleep(-1, events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_task(&path).await;
    let pi_rs_durable::task::TaskState::Succeeded(result) = task.state() else {
        panic!("unexpected state {:?}", task.state());
    };
    assert_eq!(
        result["task_content_v1"]["diagnostics"][0]["code"],
        "invalid_park"
    );
    assert_eq!(
        *events.lock().unwrap(),
        ["before-b1", "before-b2", "after-b1", "after-b2", "after-a"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c = scheduler.conversation(id).unwrap();
            if let Some(index) = c.messages.iter().position(
                |m| matches!(m, Message::Tool { content, .. } if content == "redacted/b2/a\n[tool error]"),
            ) && c.messages[index + 1..]
                .iter()
                .any(|m| matches!(m, Message::Assistant { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("conversation: {:?}", scheduler.conversation(id).unwrap().messages));
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
fn setup_sleep(
    deadline: i64,
    events: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let hook = |id| {
        Arc::new(SleepHook {
            id,
            deadline,
            events: events.clone(),
        }) as Arc<dyn Hook>
    };
    let mut extensions = pi_rs_durable::extension::Registry::default();
    for (name, hooks, ids) in [
        ("a", vec![hook("a")], vec!["a"]),
        ("b", vec![hook("b1"), hook("b2")], vec!["b1", "b2"]),
    ] {
        extensions
            .install_identified(
                Extension {
                    name: name.into(),
                    hooks,
                    ..Default::default()
                },
                ExtensionIdentity::new(
                    1,
                    name.into(),
                    ids.into_iter().map(str::to_owned).collect(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let mut registry = Registry::default();
    let adapter = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job { calls }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        &["b".into(), "a".into()],
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub),
            tools: vec![adapter.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
        extensions,
    )
}
async fn wait_sleep(path: &std::path::Path) -> pi_rs_durable::task::Task {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let store = durable_store::Store::open(path).unwrap();
            if let Some(item) = pi_rs_durable::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .into_iter()
                .find(|t| {
                    matches!(
                        t.task.state(),
                        pi_rs_durable::task::TaskState::Waiting(
                            pi_rs_durable::task::WaitFor::Sleep { .. }
                        )
                    )
                })
            {
                break item.task;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn before_sleep_replays_whole_chain_with_memo_and_wakes_at_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let deadline = now_ms() + 1200;
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_sleep(deadline, events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_sleep(&path).await;
    assert_eq!(task.checkpoint().phase, "replay-safe-async-hook-gate");
    assert_eq!(*events.lock().unwrap(), ["before-b1", "before-b2"]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    wait_result(&scheduler, id).await;
    assert_eq!(
        *events.lock().unwrap(),
        [
            "before-b1",
            "before-b2",
            "before-b1",
            "before-b2",
            "before-a",
            "after-b1",
            "after-b2",
            "after-a"
        ]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn sleep_worker() {
    let Some(dir) = std::env::var_os("PI_MULTI_SLEEP_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let deadline = std::env::var("PI_MULTI_SLEEP_DEADLINE")
        .unwrap()
        .parse()
        .unwrap();
    let (options, registry, extensions) = setup_sleep(
        deadline,
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
    );
    let mut scheduler =
        Scheduler::open_with_extensions(dir.join("db"), options, extensions).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["b".into(), "a".into()])
        .unwrap();
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn before_sleep_survives_process_kill_and_wakes_automatically() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let deadline = now_ms() + 4000;
    let mut child = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sleep_worker"])
            .env("PI_MULTI_SLEEP_DIR", dir.path())
            .env("PI_MULTI_SLEEP_DEADLINE", deadline.to_string())
            .spawn()
            .unwrap(),
    );
    let task = wait_sleep(&path).await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let id: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let key = format!("runtime/extension-memo/{}/sleep-deadline", task.id());
    let memo = store.get(key.as_bytes()).unwrap().unwrap().value;
    drop(store);
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_sleep(deadline, events.clone(), calls.clone());
    let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while events.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wait_result(&scheduler, id).await;
    assert_eq!(
        *events.lock().unwrap(),
        [
            "before-b1",
            "before-b2",
            "before-b1",
            "before-b2",
            "before-a",
            "after-b1",
            "after-b2",
            "after-a"
        ]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(store.get(key.as_bytes()).unwrap().unwrap().value, memo);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_while_before_sleeping_never_dispatches_tool() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_sleep(now_ms() + 60_000, events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_sleep(&path).await;
    tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
        .await
        .unwrap()
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .load(task.id())
        .unwrap()
        .unwrap()
        .task;
    assert!(matches!(
        task.state(),
        pi_rs_durable::task::TaskState::Cancelled
    ));
    assert_eq!(*events.lock().unwrap(), ["before-b1", "before-b2"]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}

struct SignalHook {
    id: &'static str,
    deadline: Option<i64>,
    events: Arc<Mutex<Vec<String>>>,
    gate: Option<Arc<AtomicUsize>>,
}
#[async_trait]
impl Hook for SignalHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        self.events
            .lock()
            .unwrap()
            .push(format!("before-{}", self.id));
        if ctx
            .memo
            .get_or_insert("shared-signal-memo", json!("same"))
            .await?
            != "same"
        {
            return Err("memo changed".into());
        }
        if self.id == "b2" {
            if let Some(gate) = &self.gate
                && gate.load(Ordering::SeqCst) == 20
            {
                gate.store(21, Ordering::SeqCst);
                while gate.load(Ordering::SeqCst) != 22 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            match ctx.read_signal("first").await? {
                None => {
                    if let Some(gate) = &self.gate
                        && gate.load(Ordering::SeqCst) == 10
                    {
                        gate.store(11, Ordering::SeqCst);
                        while gate.load(Ordering::SeqCst) != 12 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        ctx.memo
                            .get_or_insert("review-decision", json!("denied-before-signal"))
                            .await?;
                    }
                    if let Some(gate) = &self.gate
                        && gate.load(Ordering::SeqCst) == 0
                    {
                        gate.store(1, Ordering::SeqCst);
                        while gate.load(Ordering::SeqCst) != 2 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                    return Ok(BeforeTool::Sleep(pi_rs_durable::extension::Sleep {
                        signals: vec!["first".into()],
                        until_millis: self.deadline,
                    }));
                }
                Some(pi_rs_durable::task_store::SignalState::Value(v)) if v == json!("one") => {
                    if let Some(gate) = &self.gate
                        && gate.load(Ordering::SeqCst) == 12
                        && ctx.memo.get("review-decision").await?
                            == Some(json!("denied-before-signal"))
                    {
                        return Ok(BeforeTool::Block("stale memo denied approval".into()));
                    }
                    if self.deadline == Some(i64::MAX) {
                        return Ok(BeforeTool::Sleep(pi_rs_durable::extension::Sleep {
                            signals: vec!["first".into()],
                            until_millis: None,
                        }));
                    }
                    if ctx.read_signal("second").await?.is_none() {
                        return Ok(BeforeTool::Sleep(pi_rs_durable::extension::Sleep {
                            signals: vec!["second".into()],
                            until_millis: self.deadline,
                        }));
                    }
                }
                Some(pi_rs_durable::task_store::SignalState::Expired) => (),
                _ => return Err("unexpected first signal".into()),
            }
        }
        let mut args = call.arguments.clone();
        args.as_object_mut()
            .unwrap()
            .entry("trace")
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .unwrap()
            .push(json!(self.id));
        Ok(BeforeTool::Rewrite(args))
    }
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        self.events
            .lock()
            .unwrap()
            .push(format!("after-{}", self.id));
        Ok(Some(json!(format!(
            "{}/{}",
            result.as_str().ok_or("missing text")?,
            self.id
        ))))
    }
}

#[tokio::test]
async fn used_signal_key_is_invalid_park_and_runs_after_hooks() {
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_signal(Some(i64::MAX), Arc::new(Mutex::new(vec![])), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    let task = wait_task(&path).await;
    let pi_rs_durable::task::TaskState::Succeeded(value) = task.state() else {
        panic!("unexpected state {:?}", task.state());
    };
    assert_eq!(
        value["task_content_v1"]["diagnostics"][0]["code"],
        "invalid_park"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}

#[tokio::test]
async fn abort_while_signal_sleeping_closes_target_without_dispatch() {
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_signal(None, Arc::new(Mutex::new(vec![])), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    scheduler.abort_and_wait(id).await.unwrap();
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Closed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}
fn setup_signal(
    deadline: Option<i64>,
    events: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    setup_signal_gate(deadline, events, calls, None)
}
fn setup_signal_gate(
    deadline: Option<i64>,
    events: Arc<Mutex<Vec<String>>>,
    calls: Arc<AtomicUsize>,
    gate: Option<Arc<AtomicUsize>>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let hook = |id| {
        Arc::new(SignalHook {
            id,
            deadline,
            events: events.clone(),
            gate: gate.clone(),
        }) as Arc<dyn Hook>
    };
    let mut extensions = pi_rs_durable::extension::Registry::default();
    for (name, hooks, ids) in [
        ("a", vec![hook("a")], vec!["a"]),
        ("b", vec![hook("b1"), hook("b2")], vec!["b1", "b2"]),
    ] {
        extensions
            .install_identified(
                Extension {
                    name: name.into(),
                    hooks,
                    ..Default::default()
                },
                ExtensionIdentity::new(
                    1,
                    name.into(),
                    ids.into_iter().map(str::to_owned).collect(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let mut registry = Registry::default();
    let adapter = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job { calls }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        &["b".into(), "a".into()],
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub),
            tools: vec![adapter.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
        extensions,
    )
}
async fn wait_signal_sleep(path: &std::path::Path, key: &str) -> pi_rs_durable::task::Task {
    tokio::time::timeout(Duration::from_secs(10), async { loop {
        let store = durable_store::Store::open(path).unwrap();
        if let Some(item) = pi_rs_durable::task_store::TaskStore::new(&store).list().unwrap().into_iter().find(|t| matches!(t.task.state(), pi_rs_durable::task::TaskState::Waiting(pi_rs_durable::task::WaitFor::TypedSleep { signals, .. }) if signals.iter().any(|k| k.key() == key))) { break item.task; }
        tokio::time::sleep(Duration::from_millis(10)).await;
    } }).await.unwrap()
}
#[cfg(unix)]
#[tokio::test]
async fn signal_sleep_worker() {
    let Some(dir) = std::env::var_os("PI_MULTI_SIGNAL_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (options, registry, extensions) = setup_signal(
        None,
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
    );
    let mut scheduler =
        Scheduler::open_with_extensions(dir.join("db"), options, extensions).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["b".into(), "a".into()])
        .unwrap();
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn signal_sleep_reopens_and_handles_two_distinct_keys_once() {
    use pi_rs_durable::task_store::SignalResult;
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
            .args(["--exact", "signal_sleep_worker"])
            .env("PI_MULTI_SIGNAL_DIR", dir.path())
            .spawn()
            .unwrap(),
    );
    let task = wait_signal_sleep(&path, "first").await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let id: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal(None, events.clone(), calls.clone());
    let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let store = durable_store::Store::open(&path).unwrap();
            let current = pi_rs_durable::task_store::TaskStore::new(&store)
                .load(task.id())
                .unwrap()
                .unwrap()
                .task;
            if matches!(current.state(), pi_rs_durable::task::TaskState::Waiting(_))
                && current.checkpoint().data != task.checkpoint().data
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Duplicate
    );
    wait_signal_sleep(&path, "second").await;
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    assert_eq!(
        *events.lock().unwrap(),
        [
            "before-b1",
            "before-b2",
            "before-b1",
            "before-b2",
            "before-b1",
            "before-b2",
            "before-a",
            "after-b1",
            "after-b2",
            "after-a"
        ]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        pi_rs_durable::task_store::TaskStore::new(&store)
            .read_typed_signal_state(task.id(), &token, "first")
            .unwrap(),
        Some(pi_rs_durable::task_store::SignalState::Value(json!("one")))
    );
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn signal_sleep_expiry_is_durable_and_rejects_late_delivery() {
    use pi_rs_durable::task_store::{SignalResult, SignalState};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_signal(Some(now_ms() + 1000), events, calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    wait_result(&scheduler, id).await;
    let store = durable_store::Store::open(&path).unwrap();
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        pi_rs_durable::task_store::TaskStore::new(&store)
            .read_typed_signal_state(task.id(), &token, "first")
            .unwrap(),
        Some(SignalState::Expired)
    );
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Closed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn signal_arriving_after_hook_read_retries_before_dispatch() {
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert_eq!(tasks.len(), 1);
    let task_id = tasks[0].task.id();
    drop(store);
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task_id, &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    gate.store(2, Ordering::SeqCst);
    wait_signal_sleep(&path, "second").await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        driver
            .deliver_signal(task_id, &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_races_signal_during_paused_hook() {
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    let task_id = tasks[0].task.id();
    drop(store);
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task_id, &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
        .await
        .unwrap()
        .unwrap();
    gate.store(2, Ordering::SeqCst);
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .load(task_id)
        .unwrap()
        .unwrap()
        .task;
    assert!(matches!(
        task.state(),
        pi_rs_durable::task::TaskState::Cancelled
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn signal_competing_with_timeout_has_one_durable_outcome() {
    use pi_rs_durable::task_store::{SignalResult, SignalState};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let deadline = now_ms() + 1500;
    let (options, registry, extensions) =
        setup_signal(Some(deadline), Arc::new(Mutex::new(vec![])), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    while now_ms() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let result = driver
        .deliver_signal(task.id(), &token, "first", json!("one"))
        .unwrap();
    assert!(matches!(
        result,
        SignalResult::Delivered | SignalResult::Closed
    ));
    if result == SignalResult::Delivered {
        assert_eq!(
            driver
                .deliver_signal(task.id(), &token, "second", json!("two"))
                .unwrap(),
            SignalResult::Delivered
        );
    }
    wait_result(&scheduler, id).await;
    let store = durable_store::Store::open(&path).unwrap();
    let state = pi_rs_durable::task_store::TaskStore::new(&store)
        .read_typed_signal_state(task.id(), &token, "first")
        .unwrap();
    assert_eq!(
        state,
        Some(if result == SignalResult::Delivered {
            SignalState::Value(json!("one"))
        } else {
            SignalState::Expired
        })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

fn publish_unselected(scheduler: &Scheduler, name: &str) {
    scheduler
        .install_extension(Extension {
            name: name.into(),
            ..Default::default()
        })
        .unwrap();
}
async fn wait_gate_generation(
    path: &std::path::Path,
    task_id: uuid::Uuid,
    prior: &Value,
) -> pi_rs_durable::task::Task {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let store = durable_store::Store::open(path).unwrap();
            let task = pi_rs_durable::task_store::TaskStore::new(&store)
                .load(task_id)
                .unwrap()
                .unwrap()
                .task;
            if matches!(task.state(), pi_rs_durable::task::TaskState::Waiting(_))
                && &task.checkpoint().data != prior
            {
                break task;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn registry_publication_wakes_signal_gate_and_preserves_signal_and_memo() {
    use pi_rs_durable::task_store::{SignalResult, SignalState};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal(None, events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let first = wait_signal_sleep(&path, "first").await;
    let store = durable_store::Store::open(&path).unwrap();
    let memo_key = format!("runtime/extension-memo/{}/shared-signal-memo", first.id());
    let memo = store.get(memo_key.as_bytes()).unwrap().unwrap().value;
    drop(store);
    publish_unselected(&scheduler, "other-one");
    let next = wait_gate_generation(&path, first.id(), &first.checkpoint().data).await;
    assert_eq!(
        *events.lock().unwrap(),
        ["before-b1", "before-b2", "before-b1", "before-b2"]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(next.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    publish_unselected(&scheduler, "other-two");
    wait_signal_sleep(&path, "second").await;
    assert_eq!(
        driver
            .deliver_signal(next.id(), &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(store.get(memo_key.as_bytes()).unwrap().unwrap().value, memo);
    assert_eq!(
        pi_rs_durable::task_store::TaskStore::new(&store)
            .read_typed_signal_state(next.id(), &token, "first")
            .unwrap(),
        Some(SignalState::Value(json!("one")))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
#[tokio::test]
async fn removed_selected_registration_wakes_but_never_dispatches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal(None, events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    scheduler.uninstall_extension("b").unwrap();
    let terminal = wait_task(&path).await;
    assert!(!matches!(
        terminal.state(),
        pi_rs_durable::task::TaskState::Succeeded(_)
    ));
    assert_eq!(*events.lock().unwrap(), ["before-b1", "before-b2"]);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let store = durable_store::Store::open(&path).unwrap();
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        pi_rs_durable::task_store::TaskStore::new(&store)
            .read_typed_signal_state(task.id(), &token, "first")
            .unwrap(),
        None
    );
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[tokio::test]
async fn registry_change_during_hook_rechecks_just_committed_sleep() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_signal_gate(None, events.clone(), calls.clone(), Some(gate.clone()));
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    publish_unselected(&scheduler, "while-running");
    gate.store(2, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        while events
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == "before-b2")
            .count()
            < 2
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let task = wait_signal_sleep(&path, "first").await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    driver
        .deliver_signal(task.id(), &token, "first", json!("one"))
        .unwrap();
    wait_signal_sleep(&path, "second").await;
    driver
        .deliver_signal(task.id(), &token, "second", json!("two"))
        .unwrap();
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn registry_and_signal_during_hook_preserve_signal_and_selection() {
    use pi_rs_durable::task_store::{SignalResult, SignalState};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_signal_gate(None, events.clone(), calls.clone(), Some(gate.clone()));
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    let task_id = tasks[0].task.id();
    drop(store);
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task_id, &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    publish_unselected(&scheduler, "concurrent-change");
    gate.store(2, Ordering::SeqCst);
    wait_signal_sleep(&path, "second").await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        driver
            .deliver_signal(task_id, &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        pi_rs_durable::task_store::TaskStore::new(&store)
            .read_typed_signal_state(task_id, &token, "first")
            .unwrap(),
        Some(SignalState::Value(json!("one")))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn task_registry_publication_wakes_sleeping_hook() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal(None, events.clone(), calls.clone());
    let replacement = registry.clone();
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    driver.replace_registry(replacement).unwrap();
    let task = wait_gate_generation(&path, task.id(), &task.checkpoint().data).await;
    assert_eq!(
        *events.lock().unwrap(),
        ["before-b1", "before-b2", "before-b1", "before-b2"]
    );
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    driver
        .deliver_signal(task.id(), &token, "first", json!("one"))
        .unwrap();
    wait_signal_sleep(&path, "second").await;
    driver
        .deliver_signal(task.id(), &token, "second", json!("two"))
        .unwrap();
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn successful_sleep_clears_interrupted_attempt_count() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let deadline = now_ms() + 30_000;
    let mut child = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sleep_worker"])
            .env("PI_MULTI_SLEEP_DIR", root)
            .env("PI_MULTI_SLEEP_DEADLINE", deadline.to_string())
            .env("PI_REVIEW_KILL_BEFORE_SLEEP", root.join("running"))
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !root.join("running").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let (options, registry, extensions) = setup_sleep(
        deadline,
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
    );
    let scheduler = Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let task = wait_sleep(&root.join("db")).await;
    assert_eq!(
        task.restarts(),
        0,
        "committed sleep must reset interrupted attempt count"
    );
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn review_sleep_resume_worker() {
    let Some(root) = std::env::var_os("PI_REVIEW_SLEEP_RESUME") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let deadline = std::env::var("PI_MULTI_SLEEP_DEADLINE")
        .unwrap()
        .parse()
        .unwrap();
    let (options, registry, extensions) = setup_sleep(
        deadline,
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
    );
    let scheduler = Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    std::hint::black_box((&scheduler, &driver));
    std::future::pending::<()>().await;
}

#[cfg(unix)]
#[tokio::test]
async fn prior_interruptions_do_not_exhaust_budget_after_committed_sleep() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let deadline = now_ms() + 12_000;
    for index in 0..4 {
        let _ = std::fs::remove_file(root.join("running"));
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                if index == 0 {
                    "sleep_worker"
                } else {
                    "review_sleep_resume_worker"
                },
            ])
            .env("PI_MULTI_SLEEP_DIR", root)
            .env("PI_REVIEW_SLEEP_RESUME", root)
            .env("PI_MULTI_SLEEP_DEADLINE", deadline.to_string())
            .env("PI_REVIEW_KILL_BEFORE_SLEEP", root.join("running"));
        let mut process = Kill(command.spawn().unwrap());
        tokio::time::timeout(Duration::from_secs(15), async {
            while !root.join("running").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        process.0.kill().unwrap();
        process.0.wait().unwrap();
    }
    let (options, registry, extensions) = setup_sleep(
        deadline,
        Arc::new(Mutex::new(vec![])),
        Arc::new(AtomicUsize::new(0)),
    );
    let scheduler = Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let waiting = wait_sleep(&root.join("db")).await;
    assert_eq!(waiting.restarts(), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
    let _ = std::fs::remove_file(root.join("running"));
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "review_sleep_resume_worker"])
            .env("PI_REVIEW_SLEEP_RESUME", root)
            .env("PI_MULTI_SLEEP_DEADLINE", deadline.to_string())
            .env("PI_REVIEW_KILL_BEFORE_SLEEP", root.join("running"))
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !root.join("running").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_sleep(deadline, events.clone(), calls.clone());
    let scheduler = Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let task = wait_task(&root.join("db")).await;
    assert!(
        matches!(task.state(), pi_rs_durable::task::TaskState::Succeeded(_)),
        "{:?}",
        task.state()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "after-a")
    );
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn review_sleep_wake_rejects_removed_selection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let events = Arc::new(Mutex::new(vec![]));
    let calls = Arc::new(AtomicUsize::new(0));
    let deadline = now_ms() + 1800;
    let (options, registry, extensions) = setup_sleep(deadline, events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let _ = wait_sleep(&path).await;
    scheduler.uninstall_extension("a").unwrap();
    while now_ms() < deadline + 400 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(*events.lock().unwrap(), ["before-b1", "before-b2"]);
    assert!(scheduler.conversation(id).unwrap().tool_results.is_empty());
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}

#[tokio::test]
async fn review_abort_sleep_result_is_not_duplicated_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let deadline = now_ms() + 60_000;
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_sleep(deadline, Arc::new(Mutex::new(vec![])), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    wait_sleep(&path).await;
    tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
        .await
        .unwrap()
        .unwrap();
    let before = scheduler.conversation(id).unwrap();
    let results = before.tool_results.len();
    let messages = before
        .messages
        .iter()
        .filter(|m| matches!(m, Message::Tool { .. }))
        .count();
    assert_eq!(results, 1);
    assert_eq!(messages, 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
    let (options, registry, extensions) =
        setup_sleep(deadline, Arc::new(Mutex::new(vec![])), calls.clone());
    let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = scheduler.conversation(id).unwrap();
    assert_eq!(after.tool_results.len(), results);
    assert_eq!(
        after
            .messages
            .iter()
            .filter(|m| matches!(m, Message::Tool { .. }))
            .count(),
        messages
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn signal_revision_fences_memo_and_retries_fresh() {
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(10));
    let calls = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 11 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap()[0]
        .task
        .id();
    drop(store);
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task, &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    gate.store(12, Ordering::SeqCst);
    let task = wait_signal_sleep(&path, "second").await;
    let store = durable_store::Store::open(&path).unwrap();
    assert!(
        store
            .get(format!("runtime/extension-memo/{}/review-decision", task.id()).as_bytes())
            .unwrap()
            .is_none()
    );
    drop(store);
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

// The hook marks the exact registry-wake commit, so an unrelated write cannot
// satisfy these contention tests. The writer is released on a separate thread.
#[cfg(feature = "test-hooks")]
async fn registry_wake_writer_race(lock_past_busy_timeout: bool) {
    use pi_rs_durable::task_scheduler::registry_wake_test_hook;
    use pi_rs_durable::task_store::{SignalResult, SignalState};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let deadline = now_ms()
        + if lock_past_busy_timeout {
            30_000
        } else {
            3_000
        };
    let calls = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(Mutex::new(vec![]));
    let (options, registry, extensions) =
        setup_signal(Some(deadline), events.clone(), calls.clone());
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    let task = wait_signal_sleep(&path, "first").await;
    let (at_tx, at_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    if lock_past_busy_timeout {
        registry_wake_test_hook::install_busy(task.id(), move || {
            busy_tx.send(()).unwrap();
        });
    }
    registry_wake_test_hook::install(task.id(), move || {
        at_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    publish_unselected(&scheduler, "deadline-race");
    at_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_path = path.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let connection = rusqlite::Connection::open(writer_path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        if lock_past_busy_timeout {
            std::thread::sleep(Duration::from_millis(7000));
        } else {
            while now_ms() <= deadline + 50 {
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        connection.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    if lock_past_busy_timeout {
        busy_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("target wake did not reach the SQLite busy timeout");
    }
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    if lock_past_busy_timeout {
        // The first transaction could not start. Its retry must remain live and
        // preserve the still-valid signal rather than poisoning the driver.
        assert_eq!(
            driver
                .deliver_signal(task.id(), &token, "first", json!("one"))
                .unwrap(),
            SignalResult::Delivered
        );
        wait_signal_sleep(&path, "second").await;
        assert_eq!(
            driver
                .deliver_signal(task.id(), &token, "second", json!("two"))
                .unwrap(),
            SignalResult::Delivered
        );
    }
    wait_result(&scheduler, id).await;
    let store = durable_store::Store::open(&path).unwrap();
    if !lock_past_busy_timeout {
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| *event == "before-b2")
                .count(),
            2,
            "expired registry wake must not rerun the hook before expiration is recorded"
        );
        assert_eq!(
            pi_rs_durable::task_store::TaskStore::new(&store)
                .read_typed_signal_state(task.id(), &token, "first")
                .unwrap(),
            Some(SignalState::Expired)
        );
        assert_eq!(
            driver
                .deliver_signal(task.id(), &token, "first", json!("one"))
                .unwrap(),
            SignalResult::Closed
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn registry_wake_waiting_for_writer_cannot_cross_deadline() {
    registry_wake_writer_race(false).await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn registry_wake_retries_writer_lock_timeout() {
    registry_wake_writer_race(true).await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn memo_busy_after_signal_read_defers_and_rechecks_lease() {
    use pi_rs_durable::task_scheduler::hook_fence_test_hook;
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(10));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 11 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .task;
    let (at_tx, at_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    hook_fence_test_hook::install(task.id(), "memo", move || {
        at_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    hook_fence_test_hook::install(task.id(), "busy", move || {
        busy_tx.send(()).unwrap();
    });
    gate.store(12, Ordering::SeqCst);
    at_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_path = path.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(writer_path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(6500));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(7))
        .expect("memo commit never reached busy timeout");
    writer.join().unwrap();
    let task = wait_signal_sleep(&path, "first").await;
    assert!(
        store
            .get(format!("runtime/extension-memo/{}/review-decision", task.id()).as_bytes())
            .unwrap()
            .is_none()
    );
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_signal_sleep(&path, "second").await;
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn busy_park_settlement_rechecks_abort_before_retry() {
    use pi_rs_durable::task_scheduler::hook_fence_test_hook;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .task;
    let (at_tx, at_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    hook_fence_test_hook::install(task.id(), "settlement", move || {
        at_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    hook_fence_test_hook::install(task.id(), "settlement-busy", move || {
        busy_tx.send(()).unwrap();
    });
    gate.store(2, Ordering::SeqCst);
    at_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_path = path.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(writer_path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(7000));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(7))
        .expect("settlement did not hit busy timeout");
    driver.request_abort(task.id()).unwrap();
    writer.join().unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let task = driver.get(task.id()).unwrap().unwrap();
            if task.is_terminal() && task.abort_requested() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn busy_park_settlement_revalidates_and_resumes() {
    use pi_rs_durable::task_scheduler::hook_fence_test_hook;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .task;
    let (at_tx, at_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    hook_fence_test_hook::install(task.id(), "settlement", move || {
        at_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    hook_fence_test_hook::install(task.id(), "settlement-busy", move || {
        busy_tx.send(()).unwrap();
    });
    gate.store(2, Ordering::SeqCst);
    at_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_path = path.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(writer_path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(7000));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(7))
        .expect("settlement did not hit busy timeout");
    writer.join().unwrap();
    let task = wait_signal_sleep(&path, "first").await;
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        pi_rs_durable::task_store::SignalResult::Delivered
    );
    wait_signal_sleep(&path, "second").await;
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "second", json!("two"))
            .unwrap(),
        pi_rs_durable::task_store::SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn signal_read_busy_defers_and_rechecks_lease() {
    use pi_rs_durable::task_scheduler::hook_fence_test_hook;
    use pi_rs_durable::task_store::SignalResult;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicUsize::new(20));
    let (options, registry, extensions) = setup_signal_gate(
        None,
        Arc::new(Mutex::new(vec![])),
        calls.clone(),
        Some(gate.clone()),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while gate.load(Ordering::SeqCst) != 21 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .task;
    let (at_tx, at_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    hook_fence_test_hook::install(task.id(), "signal", move || {
        at_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    hook_fence_test_hook::install(task.id(), "busy", move || {
        busy_tx.send(()).unwrap();
    });
    gate.store(22, Ordering::SeqCst);
    at_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let writer_path = path.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(writer_path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(6500));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(7))
        .expect("signal read never reached busy timeout");
    writer.join().unwrap();
    let task = wait_signal_sleep(&path, "first").await;
    let token = pi_rs_durable::extension::checkpointable_hook_signal_token();
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "first", json!("one"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_signal_sleep(&path, "second").await;
    assert_eq!(
        driver
            .deliver_signal(task.id(), &token, "second", json!("two"))
            .unwrap(),
        SignalResult::Delivered
    );
    wait_result(&scheduler, id).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
