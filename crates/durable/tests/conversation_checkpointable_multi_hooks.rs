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
                |m| matches!(m, Message::Tool { content, .. } if content == "redacted/b2/a"),
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
    .unwrap();
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
    assert_eq!(task.checkpoint().phase, "initial");
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
