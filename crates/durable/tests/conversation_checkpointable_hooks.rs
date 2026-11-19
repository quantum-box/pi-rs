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
        ReplaySafeConversationHooksAuthorization,
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
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
struct ModelStub;
struct InvalidModel(&'static str);
#[async_trait]
impl Model for InvalidModel {
    async fn stream(&self, r: ModelRequest) -> Result<ModelStream, Error> {
        let done = r.messages.iter().any(|m| matches!(m, Message::Tool { .. }));
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: if done {
                vec![]
            } else {
                vec![ToolCall {
                    id: "call".into(),
                    name: "root".into(),
                    arguments: self.0.into(),
                }]
            },
            usage: None,
        })])
        .boxed())
    }
}
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, r: ModelRequest) -> Result<ModelStream, Error> {
        let done = r.messages.iter().any(|m| matches!(m, Message::Tool { .. }));
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
struct OneHook {
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
    gate: Option<(&'static str, Arc<AtomicUsize>)>,
}
#[async_trait]
impl Hook for OneHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if ctx.snapshot.get("id").is_none() {
            return Err("missing conversation".into());
        }
        if ctx.memo.get_or_insert("before", json!("stable")).await? != "stable" {
            return Err("memo changed".into());
        }
        if call.name == "root"
            && let Some(("before", gate)) = &self.gate
        {
            gate.store(1, Ordering::SeqCst);
            while gate.load(Ordering::SeqCst) != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            ctx.memo
                .get_or_insert("late", json!("must-not-commit"))
                .await?;
        }
        if call.name == "root"
            && std::env::var("PI_SELECTED_HOOK_CUT").ok().as_deref() == Some("before")
            && let Some(path) = std::env::var_os("PI_SELECTED_HOOK_READY")
        {
            std::fs::write(path, "before").unwrap();
            std::future::pending::<()>().await;
        }
        self.before.fetch_add(1, Ordering::SeqCst);
        let mut args = call.arguments.clone();
        args["prepared_by_hook"] = json!(true);
        Ok(BeforeTool::Rewrite(args))
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if ctx.snapshot.get("id").is_none() {
            return Err("missing conversation".into());
        }
        if ctx.memo.get_or_insert("after", json!("stable")).await? != "stable" {
            return Err("memo changed".into());
        }
        if call.name == "root"
            && let Some(("after", gate)) = &self.gate
        {
            gate.store(1, Ordering::SeqCst);
            while gate.load(Ordering::SeqCst) != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            ctx.memo
                .get_or_insert("late", json!("must-not-commit"))
                .await?;
        }
        if call.name == "root"
            && std::env::var("PI_SELECTED_HOOK_CUT").ok().as_deref() == Some("after")
            && let Some(path) = std::env::var_os("PI_SELECTED_HOOK_READY")
        {
            std::fs::write(path, "after").unwrap();
            std::future::pending::<()>().await;
        }
        self.after.fetch_add(1, Ordering::SeqCst);
        Ok(Some(json!(format!(
            "{}-after",
            result.as_str().ok_or("text required")?
        ))))
    }
}
struct Job {
    name: &'static str,
    child: Option<&'static str>,
    executions: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for Job {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
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
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        if args["prepared_by_hook"] != true {
            return Err("before rewrite missing".into());
        }
        self.executions.fetch_add(1, Ordering::SeqCst);
        if let Some(name) = self.child {
            if let Some(saved) = resume.checkpoint {
                let child: AsyncTaskCall =
                    serde_json::from_value(saved).map_err(|e| e.to_string())?;
                if !resume.ended.contains(&child.task_id()) {
                    return Err("wake missing".into());
                }
                if !matches!(
                    host.child_result(&child).await?,
                    Some(AsyncTaskResult::Rich(_))
                ) {
                    return Err("after result missing".into());
                }
                return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                    self.name.into(),
                )));
            }
            let child = host.start_child("child", name, json!({})).await?;
            Ok(DurableToolStep::Park {
                checkpoint: serde_json::to_value(&child).unwrap(),
                wait: DurableWait::All(vec![child]),
            })
        } else {
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                self.name.into(),
            )))
        }
    }
}
fn setup(
    depth: usize,
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
    executions: Arc<AtomicUsize>,
    fingerprint: &str,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    setup_gated(depth, before, after, executions, fingerprint, None)
}
fn setup_gated(
    depth: usize,
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
    executions: Arc<AtomicUsize>,
    fingerprint: &str,
    gate: Option<(&'static str, Arc<AtomicUsize>)>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(OneHook {
                    before,
                    after,
                    gate,
                })],
                ..Default::default()
            },
            ExtensionIdentity::new(1, fingerprint.into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let snapshot = extensions.snapshot();
    let mut registry = Registry::default();
    let mut adapters = vec![];
    let mut child = None;
    let jobs: Vec<(&'static str, Option<&'static str>)> = if depth == 1 {
        vec![("root", None)]
    } else {
        vec![
            ("leaf", None),
            ("middle", Some("leaf")),
            ("root", Some("middle")),
        ]
    };
    for (name, next) in jobs {
        let adapter = register_conversation_replay_safe_durable_tool_with_selected_hooks(
            &mut registry,
            Arc::new(Job {
                name,
                child: next,
                executions: executions.clone(),
            }),
            name,
            1,
            child.into_iter().collect(),
            &snapshot,
            &["policy".into()],
            ReplaySafeAuthorization::host_attests_replay_safe(),
            ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
        .unwrap();
        adapters.push(adapter.register());
        child = Some(adapter);
    }
    (
        Options {
            model: Arc::new(ModelStub),
            tools: adapters,
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
        extensions,
    )
}
async fn until_result(scheduler: &Scheduler, id: uuid::Uuid) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let c = scheduler.conversation(id).unwrap();
            if let Some(pos) = c
                .messages
                .iter()
                .position(|m| matches!(m,Message::Tool {content,..} if content=="root-after"))
                && c.messages[pos + 1..]
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
}

#[tokio::test]
async fn invalid_raw_runs_only_after_hook_and_survives_reopen() {
    for raw in ["{invalid", "[1,2]"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (mut options, registry, extensions) =
            setup(1, before.clone(), after.clone(), executions.clone(), "v1");
        options.model = Arc::new(InvalidModel(raw));
        let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler
            .select_extensions(id, vec!["policy".into()])
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        let task = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let store = durable_store::Store::open(&path).unwrap();
                if let Some(task) = pi_rs_durable::task_store::TaskStore::new(&store)
                    .list()
                    .unwrap()
                    .into_iter()
                    .find(|task| task.task.is_terminal())
                {
                    break task.task;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(before.load(Ordering::SeqCst), 0);
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert_eq!(task.checkpoint().phase, "replay-safe-async-after");
        let pi_rs_durable::task::TaskState::Succeeded(value) = task.state() else {
            panic!("expected after result: {:?}", task.state());
        };
        assert_eq!(value["task_content_v1"]["isError"], true);
        assert!(
            value["task_content_v1"]["output"][0]["text"]
                .as_str()
                .unwrap()
                .ends_with("-after")
        );
        let store = durable_store::Store::open(&path).unwrap();
        let memo_key = format!("runtime/extension-memo/{}/after", task.id());
        assert!(store.get(memo_key.as_bytes()).unwrap().is_some());
        drop(store);
        let _ = driver.shutdown_with_pending();
        let _ = scheduler.shutdown().await;

        let (mut options, registry, extensions) =
            setup(1, before.clone(), after.clone(), executions.clone(), "v1");
        options.model = Arc::new(InvalidModel(raw));
        let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let recovered = pi_rs_durable::task_store::TaskStore::new(&store)
            .load(task.id())
            .unwrap()
            .unwrap()
            .task;
        assert_eq!(recovered.state(), task.state());
        assert!(store.get(memo_key.as_bytes()).unwrap().is_some());
        assert_eq!(after.load(Ordering::SeqCst), 1);
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending();
        let _ = scheduler.shutdown().await;
    }
}

#[tokio::test]
async fn invalid_raw_selection_change_fences_after_memo() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(0));
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (mut options, registry, extensions) = setup_gated(
        1,
        before.clone(),
        after.clone(),
        executions.clone(),
        "v1",
        Some(("after", gate.clone())),
    );
    options.model = Arc::new(InvalidModel("[1,2]"));
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    scheduler.uninstall_extension("policy").unwrap();
    gate.store(2, Ordering::SeqCst);
    let task = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let store = durable_store::Store::open(&path).unwrap();
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
    .unwrap();
    assert!(!matches!(
        task.state(),
        pi_rs_durable::task::TaskState::Succeeded(_)
    ));
    let store = durable_store::Store::open(&path).unwrap();
    let late = format!("runtime/extension-memo/{}/late", task.id());
    assert!(store.get(late.as_bytes()).unwrap().is_none());
    assert_eq!(before.load(Ordering::SeqCst), 0);
    assert_eq!(after.load(Ordering::SeqCst), 0);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[tokio::test]
async fn selected_hook_runs_once_per_phase_and_publishes_result() {
    let dir = tempfile::tempdir().unwrap();
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup(1, before.clone(), after.clone(), executions.clone(), "v1");
    let mut scheduler =
        Scheduler::open_with_extensions(dir.path().join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until_result(&scheduler, id).await;
    assert_eq!(before.load(Ordering::SeqCst), 1);
    assert_eq!(after.load(Ordering::SeqCst), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
}
#[tokio::test]
async fn selected_hook_two_level_children_wake_and_finish() {
    let dir = tempfile::tempdir().unwrap();
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup(3, before.clone(), after.clone(), executions.clone(), "v1");
    let mut scheduler =
        Scheduler::open_with_extensions(dir.path().join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    until_result(&scheduler, id).await;
    assert_eq!(before.load(Ordering::SeqCst), 3);
    assert_eq!(after.load(Ordering::SeqCst), 3);
    assert_eq!(executions.load(Ordering::SeqCst), 5);
    driver.shutdown().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn selected_hook_kill_worker() {
    let Some(dir) = std::env::var_os("PI_SELECTED_HOOK_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (mut options, registry, extensions) = setup(
        1,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        "v1",
    );
    if std::env::var_os("PI_SELECTED_HOOK_INVALID_RAW").is_some() {
        options.model = Arc::new(InvalidModel("{invalid"));
    }
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
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_raw_after_recovers_after_process_kill() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut process = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "selected_hook_kill_worker"])
            .env("PI_SELECTED_HOOK_DIR", dir.path())
            .env("PI_SELECTED_HOOK_INVALID_RAW", "1")
            .env("PI_SELECTED_HOOK_CUT", "after")
            .env("PI_SELECTED_HOOK_READY", dir.path().join("ready"))
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
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let path = dir.path().join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert_eq!(tasks.len(), 1);
    let task = &tasks[0].task;
    assert_eq!(task.checkpoint().phase, "replay-safe-async-after");
    let memo_key = format!("runtime/extension-memo/{}/after", task.id());
    let memo = store.get(memo_key.as_bytes()).unwrap().unwrap().value;
    let task_id = task.id();
    drop(store);
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (mut options, registry, extensions) =
        setup(1, before.clone(), after.clone(), executions.clone(), "v1");
    options.model = Arc::new(InvalidModel("{invalid"));
    let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let store = durable_store::Store::open(&path).unwrap();
            let task = pi_rs_durable::task_store::TaskStore::new(&store)
                .load(task_id)
                .unwrap()
                .unwrap()
                .task;
            if matches!(task.state(), pi_rs_durable::task::TaskState::Succeeded(_)) {
                assert_eq!(store.get(memo_key.as_bytes()).unwrap().unwrap().value, memo);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(before.load(Ordering::SeqCst), 0);
    assert_eq!(after.load(Ordering::SeqCst), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn selected_hook_before_and_after_survive_kill_and_reopen() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let mut process = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "selected_hook_kill_worker"])
                .env("PI_SELECTED_HOOK_DIR", dir.path())
                .env("PI_SELECTED_HOOK_CUT", stage)
                .env("PI_SELECTED_HOOK_READY", dir.path().join("ready"))
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while !dir.path().join("ready").exists() {
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
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        let root = tasks[0].task.id();
        let memo_key = format!("runtime/extension-memo/{root}/{stage}");
        let saved = store.get(memo_key.as_bytes()).unwrap().unwrap().value;
        if stage == "after" {
            assert_eq!(tasks[0].task.checkpoint().phase, "replay-safe-async-after");
        }
        drop(store);
        let (options, registry, extensions) = setup(
            1,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "changed",
        );
        let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        assert!(scheduler.start_tasks(registry).is_err());
        scheduler.shutdown().await.unwrap();
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) =
            setup(1, before.clone(), after.clone(), executions.clone(), "v1");
        let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        until_result(&scheduler, id).await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            if stage == "after" { 0 } else { 1 }
        );
        assert_eq!(after.load(Ordering::SeqCst), 1);
        let store = durable_store::Store::open(&path).unwrap();
        assert_eq!(
            store.get(memo_key.as_bytes()).unwrap().unwrap().value,
            saved
        );
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn abort_during_before_or_after_fences_hook_and_tool_result() {
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let gate = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup_gated(
            1,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            executions.clone(),
            "v1",
            Some((stage, gate.clone())),
        );
        let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler
            .select_extensions(id, vec!["policy".into()])
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while gate.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(10), scheduler.abort_and_wait(id))
            .await
            .unwrap()
            .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(matches!(
            tasks[0].task.state(),
            pi_rs_durable::task::TaskState::Cancelled
        ));
        let late = format!("runtime/extension-memo/{}/late", tasks[0].task.id());
        assert!(store.get(late.as_bytes()).unwrap().is_none());
        assert_eq!(
            executions.load(Ordering::SeqCst),
            if stage == "before" { 0 } else { 1 }
        );
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m,Message::Tool {content,..} if content=="root-after"))
        );
        driver.shutdown().unwrap();
    }
}
#[tokio::test]
async fn selection_change_during_hook_fences_memo_and_publication() {
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let gate = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup_gated(
            1,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            executions.clone(),
            "v1",
            Some((stage, gate.clone())),
        );
        let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler
            .select_extensions(id, vec!["policy".into()])
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while gate.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        scheduler.uninstall_extension("policy").unwrap();
        gate.store(2, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let store = durable_store::Store::open(&path).unwrap();
                let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
                    .list()
                    .unwrap();
                if tasks.iter().any(|t| t.task.is_terminal()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        let late = format!("runtime/extension-memo/{}/late", tasks[0].task.id());
        assert!(store.get(late.as_bytes()).unwrap().is_none());
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m,Message::Tool {content,..} if content=="root-after"))
        );
        let _ = driver.shutdown_with_pending();
    }
}

struct RichHook;
#[async_trait]
impl Hook for RichHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        let mut args = call.arguments.clone();
        args["prepared_by_hook"] = json!(true);
        Ok(BeforeTool::Rewrite(args))
    }
    async fn after_tool_blocks(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        _: bool,
        _: &Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        let mut result = result.clone();
        result.output.push(json!({"type":"text","text":"second"}));
        Ok(Some(result))
    }
}
#[tokio::test]
async fn unsupported_multi_block_result_is_not_published_as_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(RichHook)],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "rich-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let mut registry = Registry::default();
    let root = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job {
            name: "root",
            child: None,
            executions: Arc::new(AtomicUsize::new(0)),
        }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        &["policy".into()],
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![root.register()],
        restart_limit: 5,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap();
    assert!(result.is_err());
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    let _ = driver.shutdown_with_pending();
}
