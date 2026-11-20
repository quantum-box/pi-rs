use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolCall,
    ToolContext, ToolSpec,
};
use pi_rs_durable::{
    async_task_tool::{
        AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableParkCancel, DurableTaskHost,
        DurableToolResume, DurableToolStep, DurableWait, ReplaySafeAuthorization,
        ReplaySafeConversationHooksAuthorization, UnsafeDurableAuthorization,
        register_conversation_replay_safe_durable_tool_with_selected_hooks,
        register_conversation_unsafe_durable_tool_with_selected_hooks,
        register_conversation_unsafe_durable_tool_with_selected_hooks_and_park_cancel,
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
struct ReviewModel(Arc<std::sync::Mutex<Vec<String>>>, &'static str);
#[async_trait]
impl Model for ReviewModel {
    async fn stream(&self, r: ModelRequest) -> Result<ModelStream, Error> {
        self.0
            .lock()
            .unwrap()
            .extend(r.messages.iter().filter_map(|m| match m {
                Message::Tool { content, .. } => Some(content.clone()),
                _ => None,
            }));
        if r.messages.iter().any(|m| matches!(m, Message::Tool { .. }))
            && let Some(path) = std::env::var_os("PI_REVIEW_PUBLICATION_READY")
        {
            std::fs::write(path, b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        InvalidModel(self.1).stream(r).await
    }
}
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
        if std::env::var_os("PI_SELECTED_HOOK_TEST_BLOCK").is_some() {
            return Ok(BeforeTool::Block("policy denied".into()));
        }
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
        if ((call.name == "root"
            && std::env::var("PI_SELECTED_HOOK_CUT").ok().as_deref() == Some("after"))
            || (call.name == "leaf"
                && std::env::var("PI_SELECTED_HOOK_CUT").ok().as_deref() == Some("leaf-after")))
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

struct PendingLeaf;
#[async_trait]
impl Tool for PendingLeaf {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "leaf".into(),
            description: "".into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for PendingLeaf {
    async fn execute_durable(
        &self,
        _: Value,
        context: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        context.cancellation.cancelled().await;
        Err("cancelled".into())
    }
}
struct CancelCounter(Arc<AtomicUsize>);
#[async_trait]
impl DurableParkCancel for CancelCounter {
    async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CancelHooks {
    before: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
    identity: Arc<std::sync::Mutex<Option<pi_rs_durable::nested_identity::IdentityHandle>>>,
    invalid: u8,
}
#[async_trait]
impl Hook for CancelHooks {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if call.name == "root" {
            assert_eq!(
                ctx.memo
                    .get_or_insert("cancel-before", json!("stable"))
                    .await?,
                "stable"
            );
            self.before.fetch_add(1, Ordering::SeqCst);
            return Ok(BeforeTool::Rewrite(
                json!({"before":"prepared", "prepared_by_hook":true}),
            ));
        }
        Ok(BeforeTool::Pass)
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name == "root" {
            assert_eq!(call.arguments["before"], "prepared");
            let identity = self.identity.lock().unwrap().clone().unwrap();
            assert!(
                identity
                    .update(Some("changed".into()), || panic!(
                        "identity update ran during abort hook"
                    ))
                    .await
                    .is_err()
            );
            assert_eq!(ctx.memo.get("cancel-before").await?, Some(json!("stable")));
            assert_eq!(
                ctx.memo
                    .get_or_insert("cancel-after", json!("saved"))
                    .await?,
                "saved"
            );
            assert_eq!(
                ctx.memo
                    .get_or_insert("cancel-after", json!("other"))
                    .await?,
                "saved"
            );
            if self.invalid != 0 {
                let invalid_key = "k".repeat(4097);
                let error = match self.invalid {
                    1 => ctx.memo.get("").await,
                    2 => ctx.memo.get(&invalid_key).await,
                    _ => ctx
                        .memo
                        .get_or_insert("oversize", json!("v".repeat(65535)))
                        .await
                        .map(Some),
                };
                assert!(error.is_err());
                assert!(
                    ctx.memo
                        .get_or_insert("after-invalid", json!("not saved"))
                        .await
                        .is_err()
                );
                self.after.fetch_add(1, Ordering::SeqCst);
                return Ok(Some(json!("must not publish")));
            }
            let runtime = tokio::runtime::Handle::current();
            let barrier = std::sync::Barrier::new(2);
            let (first, second) = std::thread::scope(|scope| {
                let first = scope.spawn(|| {
                    barrier.wait();
                    runtime.block_on(ctx.memo.get_or_insert("concurrent", json!("first")))
                });
                let second = scope.spawn(|| {
                    barrier.wait();
                    runtime.block_on(ctx.memo.get_or_insert("concurrent", json!("second")))
                });
                (first.join().unwrap(), second.join().unwrap())
            });
            assert_eq!(first?, second?);
            let max_key = "k".repeat(4096);
            let max_value = "v".repeat(65534); // JSON string uses 65536 bytes.
            assert_eq!(
                ctx.memo.get_or_insert(&max_key, json!(max_value)).await?,
                json!(max_value)
            );
            // An existing value wins without serializing a replacement.
            assert_eq!(
                ctx.memo
                    .get_or_insert("cancel-after", json!("v".repeat(65535)))
                    .await?,
                json!("saved")
            );
            self.after.fetch_add(1, Ordering::SeqCst);
            return Ok(Some(json!("public cancel after")));
        }
        Ok(None)
    }
}

#[tokio::test]
async fn selected_hook_public_cancel_reaches_conversation() {
    exercise_selected_hook_cancel_registration("enabled").await;
}

#[tokio::test]
async fn selected_abort_memo_invalid_key_and_value_fail_closed() {
    for change in ["invalid-empty", "invalid-key", "invalid-value"] {
        exercise_selected_hook_cancel_registration(change).await;
    }
}

#[tokio::test]
async fn selected_hook_cancel_registration_parks_and_remains_fail_closed() {
    exercise_selected_hook_cancel_registration("unchanged").await;
}

#[tokio::test]
async fn selected_hook_cancel_registration_rejects_changed_identity() {
    exercise_selected_hook_cancel_registration("identity").await;
}

#[tokio::test]
async fn selected_hook_cancel_registration_rejects_changed_selection() {
    exercise_selected_hook_cancel_registration("selection").await;
}

async fn exercise_selected_hook_cancel_registration(change: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let cancellations = Arc::new(AtomicUsize::new(0));
    let hook_identity = Arc::new(std::sync::Mutex::new(None));
    let enabled = change == "enabled" || change.starts_with("invalid-");
    let hook: Arc<dyn Hook> = if enabled {
        Arc::new(CancelHooks {
            before: before.clone(),
            after: after.clone(),
            identity: hook_identity.clone(),
            invalid: match change {
                "invalid-empty" => 1,
                "invalid-key" => 2,
                "invalid-value" => 3,
                _ => 0,
            },
        })
    } else {
        Arc::new(OneHook {
            before: before.clone(),
            after: after.clone(),
            gate: None,
        })
    };
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![hook],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "policy-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let mut registry = Registry::default();
    let snapshot = extensions.snapshot();
    let names = vec!["policy".to_string()];
    let leaf = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(PendingLeaf),
        "leaf",
        1,
        vec![],
        &snapshot,
        &names,
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    let tool = Arc::new(Job {
        name: "root",
        child: Some("leaf"),
        executions: executions.clone(),
    });
    let identity_tool: Arc<dyn Tool> = tool.clone();
    let identity =
        pi_rs_durable::nested_identity::register(&identity_tool, Some("backend".into())).unwrap();
    *hook_identity.lock().unwrap() = Some(identity.clone());
    let root = if enabled {
        pi_rs_durable::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(
            &mut registry, tool, "root", 1, vec![leaf.clone()], &snapshot, &names, "cancel-v1",
            Arc::new(CancelCounter(cancellations.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    } else {
        register_conversation_unsafe_durable_tool_with_selected_hooks_and_park_cancel(
        &mut registry,
        tool,
        "root",
        1,
        vec![leaf.clone()],
        &snapshot,
        &names,
        "cancel-v1",
        Arc::new(CancelCounter(cancellations.clone())),
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    }.unwrap();
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![root.register(), leaf.register()],
        restart_limit: 5,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.select_extensions(conversation, names).unwrap();
    scheduler.submit(conversation, None, "work".into()).unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store);
    let root_id = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(record) = tasks.list().unwrap().into_iter().find(|record| {
                record.task.owner() == &pi_rs_durable::task::TaskOwner::Conversation(conversation)
                    && matches!(
                        record.task.state(),
                        pi_rs_durable::task::TaskState::Waiting(_)
                    )
            }) {
                break record.task.id();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(before.load(Ordering::SeqCst) >= 1);
    if change == "identity" {
        identity
            .update(Some("different-backend".into()), || {})
            .await
            .unwrap();
    }
    if change == "selection" {
        scheduler
            .install_extension(Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(BlockHook)],
                ..Default::default()
            })
            .unwrap();
    }
    if let Err(error) = scheduler.abort_task(root_id) {
        assert!(
            change == "selection"
                && matches!(&error, pi_rs_durable::runtime::Error::Invalid(reason) if reason == "task is terminal")
                && tasks.load(root_id).unwrap().unwrap().task.is_terminal(),
            "abort failed: {error}"
        );
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(cancellations.load(Ordering::SeqCst), usize::from(enabled));
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    let claim = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
    assert_eq!(
        store.get(claim.as_bytes()).unwrap().is_none(),
        change != "unchanged" && !enabled
    );
    if change.starts_with("invalid-") {
        let state = tasks.load(root_id).unwrap().unwrap();
        assert!(
            matches!(state.task.state(), pi_rs_durable::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert_eq!(after.load(Ordering::SeqCst), 1);
        let key = format!("runtime/extension-memo/{root_id}/after-invalid");
        assert!(store.get(key.as_bytes()).unwrap().is_none());
        assert!(!scheduler.conversation(conversation).unwrap().messages.iter().any(|m| matches!(m, Message::Tool { content, .. } if content.contains("must not publish"))));
    }
    if change == "enabled" {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let c = scheduler.conversation(conversation).unwrap();
                if c.messages.iter().any(|m| matches!(m, Message::Tool { content, .. } if content.contains("public cancel after"))) { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert_eq!(after.load(Ordering::SeqCst), 1);
        let key = format!("runtime/extension-memo/{root_id}/cancel-after");
        assert_eq!(
            serde_json::from_slice::<Value>(&store.get(key.as_bytes()).unwrap().unwrap().value)
                .unwrap(),
            json!("saved")
        );
    }
    let report = driver.shutdown_with_pending().unwrap();
    assert!(report.pending().is_empty());
    let _ = scheduler.shutdown().await;
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
        if self.name == "root"
            && std::env::var("PI_SELECTED_HOOK_CUT").ok().as_deref() == Some("execute")
            && let Some(path) = std::env::var_os("PI_SELECTED_HOOK_READY")
        {
            std::fs::write(path, "effect-started").unwrap();
            std::future::pending::<()>().await;
        }
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
struct BlockHook;
#[async_trait]
impl Hook for BlockHook {
    async fn before_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        Ok(BeforeTool::Block("denied".into()))
    }
}
#[tokio::test]
async fn unsafe_selected_before_block_runs_after_without_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(BlockHook)],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "block-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let mut registry = Registry::default();
    let adapter = register_conversation_unsafe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job {
            name: "root",
            child: None,
            executions: executions.clone(),
        }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        &["policy".into()],
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 5,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
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
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert!(matches!(
        tasks[0].task.state(),
        pi_rs_durable::task::TaskState::Succeeded(_)
    ));
    assert!(
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, tasks[0].task.id())
            .unwrap()
            .is_empty()
    );
    drop(store);
    let _ = driver.shutdown_with_pending();
}

struct PriorIntentGate {
    gate: Arc<AtomicUsize>,
    after: Arc<AtomicUsize>,
    pass: bool,
}
#[async_trait]
impl Hook for PriorIntentGate {
    async fn before_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        ctx: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        self.gate.store(1, Ordering::SeqCst);
        while self.gate.load(Ordering::SeqCst) != 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The hook ignores the error; a rejected access still fences its phase.
        let _ = ctx
            .memo
            .get_or_insert("late-after-prior", json!(true))
            .await;
        if self.pass {
            Ok(BeforeTool::Rewrite(json!({"prepared_by_hook":true})))
        } else {
            Ok(BeforeTool::Block("blocked".into()))
        }
    }
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        self.after.fetch_add(1, Ordering::SeqCst);
        Ok(None)
    }
}
#[tokio::test]
async fn unsafe_before_rejects_prior_intent_without_publishing_known_failure() {
    for pass in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let gate = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let mut extensions = pi_rs_durable::extension::Registry::default();
        extensions
            .install_identified(
                Extension {
                    name: "policy".into(),
                    hooks: vec![Arc::new(PriorIntentGate {
                        gate: gate.clone(),
                        after: after.clone(),
                        pass,
                    })],
                    ..Default::default()
                },
                ExtensionIdentity::new(1, "prior-v1".into(), vec!["hook-v1".into()]).unwrap(),
            )
            .unwrap();
        let mut registry = Registry::default();
        let adapter = register_conversation_unsafe_durable_tool_with_selected_hooks(
            &mut registry,
            Arc::new(Job {
                name: "root",
                child: None,
                executions: executions.clone(),
            }),
            "root",
            1,
            vec![],
            &extensions.snapshot(),
            &["policy".into()],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
        .unwrap();
        let options = Options {
            model: Arc::new(ModelStub),
            tools: vec![adapter.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
        let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
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
        tokio::time::timeout(Duration::from_secs(15), async {
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
        let old_attempt = uuid::Uuid::new_v4();
        let mut prior = serde_json::to_value(&tasks[0].task).unwrap();
        prior["attempt_id"] = json!(old_attempt);
        let key = format!(
            "durable/unsafe-park-async/{}/{old_attempt}/intent",
            tasks[0].task.id()
        )
        .into_bytes();
        store
            .commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Missing,
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&prior).unwrap(),
                }],
            )
            .unwrap();
        drop(store);
        gate.store(2, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert_eq!(after.load(Ordering::SeqCst), 0);
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. }))
        );
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert!(
            matches!(tasks[0].task.state(),pi_rs_durable::task::TaskState::Failed(reason) if reason.contains("reconciliation")),
            "{tasks:?}"
        );
        assert!(
            store
                .get(
                    format!(
                        "durable/unsafe-park-async/{}/no-dispatch",
                        tasks[0].task.id()
                    )
                    .as_bytes()
                )
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(
                    format!(
                        "runtime/extension-memo/{}/late-after-prior",
                        tasks[0].task.id()
                    )
                    .as_bytes()
                )
                .unwrap()
                .is_none()
        );
        drop(store);
        let _ = driver.shutdown_with_pending();
    }
}
struct LeafBlockHook(Arc<AtomicUsize>);
#[async_trait]
impl Hook for LeafBlockHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if call.name == "leaf" {
            Ok(BeforeTool::Block("leaf denied".into()))
        } else {
            let mut args = call.arguments.clone();
            args["prepared_by_hook"] = json!(true);
            Ok(BeforeTool::Rewrite(args))
        }
    }
    async fn after_tool(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &Value,
        _: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(json!(format!(
            "{}-after",
            result.as_str().ok_or("text")?
        ))))
    }
}
#[tokio::test]
async fn unsafe_nested_child_block_runs_after_and_wakes_parent() {
    let dir = tempfile::tempdir().unwrap();
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(LeafBlockHook(after.clone()))],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "leaf-block-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let mut registry = Registry::default();
    let mut child = None;
    let mut tools = vec![];
    for (name, next) in [
        ("leaf", None),
        ("middle", Some("leaf")),
        ("root", Some("middle")),
    ] {
        let adapter = register_conversation_unsafe_durable_tool_with_selected_hooks(
            &mut registry,
            Arc::new(Job {
                name,
                child: next,
                executions: executions.clone(),
            }),
            name,
            1,
            child.into_iter().collect(),
            &extensions.snapshot(),
            &["policy".into()],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
        .unwrap();
        tools.push(adapter.register());
        child = Some(adapter);
    }
    let options = Options {
        model: Arc::new(ModelStub),
        tools,
        restart_limit: 5,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
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
    until_result(&scheduler, id).await;
    assert_eq!(after.load(Ordering::SeqCst), 3);
    assert_eq!(executions.load(Ordering::SeqCst), 4);
    driver.shutdown().unwrap();
}

#[tokio::test]
async fn unsafe_no_dispatch_after_rejects_racing_intent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (mut options, registry, extensions) = setup_unsafe_gated(
        1,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        executions.clone(),
        "v1",
        Some(("after", gate.clone())),
    );
    options.model = Arc::new(InvalidModel("{invalid"));
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
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
    tokio::time::timeout(Duration::from_secs(15), async {
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
    let task = tasks[0].task.id();
    assert!(
        store
            .get(format!("durable/unsafe-park-async/{task}/no-dispatch").as_bytes())
            .unwrap()
            .is_some()
    );
    let key = format!(
        "durable/unsafe-park-async/{task}/{}/intent",
        uuid::Uuid::new_v4()
    )
    .into_bytes();
    store
        .commit(
            &[durable_store::Check {
                key: key.clone(),
                expected: durable_store::Expected::Missing,
            }],
            &[durable_store::Mutation::Put {
                key,
                value: b"prior".to_vec(),
            }],
        )
        .unwrap();
    drop(store);
    gate.store(2, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    let store = durable_store::Store::open(&path).unwrap();
    let task = pi_rs_durable::task_store::TaskStore::new(&store)
        .load(task)
        .unwrap()
        .unwrap()
        .task;
    assert!(
        matches!(task.state(), pi_rs_durable::task::TaskState::Failed(_)),
        "{task:?}"
    );
    drop(store);
    let _ = driver.shutdown_with_pending();
}

struct PendingJob(bool);
#[async_trait]
impl Tool for PendingJob {
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
impl DurableAsyncTool for PendingJob {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        Ok(DurableToolStep::Complete(if self.0 {
            Outcome::Unknown("effect uncertain".into())
        } else {
            Outcome::Pending {
                reference: "remote-job-id".into(),
            }
        }))
    }
}
#[tokio::test]
async fn unsafe_selected_hook_pending_reference_stays_uncertain() {
    for unknown in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let mut extensions = pi_rs_durable::extension::Registry::default();
        extensions
            .install_identified(
                Extension {
                    name: "policy".into(),
                    hooks: vec![Arc::new(OneHook {
                        before: before.clone(),
                        after: after.clone(),
                        gate: None,
                    })],
                    ..Default::default()
                },
                ExtensionIdentity::new(1, "v1".into(), vec!["hook-v1".into()]).unwrap(),
            )
            .unwrap();
        let mut registry = Registry::default();
        let adapter = register_conversation_unsafe_durable_tool_with_selected_hooks(
            &mut registry,
            Arc::new(PendingJob(unknown)),
            "pending",
            1,
            vec![],
            &extensions.snapshot(),
            &["policy".into()],
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
        .unwrap();
        let options = Options {
            model: Arc::new(ModelStub),
            tools: vec![adapter.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        };
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
        tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.load(Ordering::SeqCst), 1);
        assert_eq!(after.load(Ordering::SeqCst), 0);
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m, Message::Tool { .. }))
        );
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        let receipt = pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(
            &store,
            tasks[0].task.id(),
        )
        .unwrap();
        assert_eq!(receipt.len(), 1);
        assert_eq!(
            receipt[0].outcome,
            if unknown {
                Outcome::Unknown("effect uncertain".into())
            } else {
                Outcome::Pending {
                    reference: "remote-job-id".into(),
                }
            }
        );
        drop(store);
        let _ = driver.shutdown_with_pending();
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
fn setup_unsafe_gated(
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
        let adapter = register_conversation_unsafe_durable_tool_with_selected_hooks(
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
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
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
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let c = scheduler.conversation(id).unwrap();
                if let Some(pos) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content.contains("invalid checkpointable arguments") && content.contains("-after")))
                    && c.messages[pos + 1..].iter().any(|m| matches!(m, Message::Assistant { .. })) {
                    assert_eq!(c.messages.iter().filter(|m| matches!(m, Message::Tool { .. })).count(), 1);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert_eq!(after.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending();
        let _ = scheduler.shutdown().await;
    }
}

#[tokio::test]
async fn unsafe_invalid_raw_runs_after_and_survives_reopen() {
    for raw in ["{invalid", "[1,2]"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (mut options, registry, extensions) = setup_unsafe_gated(
            1,
            before.clone(),
            after.clone(),
            executions.clone(),
            "v1",
            None,
        );
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

        let (mut options, registry, extensions) = setup_unsafe_gated(
            1,
            before.clone(),
            after.clone(),
            executions.clone(),
            "v1",
            None,
        );
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
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let c = scheduler.conversation(id).unwrap();
                if let Some(pos) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content.contains("invalid checkpointable arguments") && content.contains("-after")))
                    && c.messages[pos + 1..].iter().any(|m| matches!(m, Message::Assistant { .. })) {
                    assert_eq!(c.messages.iter().filter(|m| matches!(m, Message::Tool { .. })).count(), 1);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert_eq!(after.load(Ordering::SeqCst), 1);
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
async fn unsafe_selected_hooks_two_park_levels_finish() {
    let dir = tempfile::tempdir().unwrap();
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_unsafe_gated(
        3,
        before.clone(),
        after.clone(),
        executions.clone(),
        "v1",
        None,
    );
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
    let wait = tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert!(wait.is_ok(), "{wait:?} {tasks:?}");
    let conversation = scheduler.conversation(id).unwrap();
    assert!(
        conversation
            .messages
            .iter()
            .any(|m| matches!(m,Message::Tool {content,..} if content == "root-after")),
        "{conversation:?} {tasks:?}"
    );
    assert_eq!(before.load(Ordering::SeqCst), 3);
    assert_eq!(after.load(Ordering::SeqCst), 3);
    assert_eq!(executions.load(Ordering::SeqCst), 5);
    drop(store);
    driver.shutdown().unwrap();
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
    let (mut options, registry, extensions) =
        if std::env::var_os("PI_SELECTED_HOOK_UNSAFE").is_some() {
            setup_unsafe_gated(
                if std::env::var_os("PI_SELECTED_HOOK_DEPTH3").is_some() {
                    3
                } else {
                    1
                },
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                "v1",
                None,
            )
        } else {
            setup(
                1,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                "v1",
            )
        };
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
    let conversation: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
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
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let c = scheduler.conversation(conversation).unwrap();
            if let Some(pos) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content.contains("invalid checkpointable arguments") && content.contains("-after")))
                && c.messages[pos+1..].iter().any(|m| matches!(m, Message::Assistant { .. })) {
                assert_eq!(c.messages.iter().filter(|m| matches!(m, Message::Tool { .. })).count(), 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(after.load(Ordering::SeqCst), 1);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn unsafe_invalid_raw_after_recovers_after_process_kill() {
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
            .env("PI_SELECTED_HOOK_UNSAFE", "1")
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
    let conversation: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
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
    let (mut options, registry, extensions) = setup_unsafe_gated(
        1,
        before.clone(),
        after.clone(),
        executions.clone(),
        "v1",
        None,
    );
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
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let c = scheduler.conversation(conversation).unwrap();
            if let Some(pos) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content.contains("invalid checkpointable arguments") && content.contains("-after")))
                && c.messages[pos+1..].iter().any(|m| matches!(m, Message::Assistant { .. })) {
                assert_eq!(c.messages.iter().filter(|m| matches!(m, Message::Tool { .. })).count(), 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(after.load(Ordering::SeqCst), 1);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn unsafe_before_block_after_recovers_after_process_kill() {
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
            .env("PI_SELECTED_HOOK_TEST_BLOCK", "1")
            .env("PI_SELECTED_HOOK_UNSAFE", "1")
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
    let conversation: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
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
    let (options, registry, extensions) = setup_unsafe_gated(
        1,
        before.clone(),
        after.clone(),
        executions.clone(),
        "v1",
        None,
    );
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
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let c = scheduler.conversation(conversation).unwrap();
            if let Some(pos) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content.contains("policy denied") && content.contains("-after")))
                && c.messages[pos+1..].iter().any(|m| matches!(m, Message::Assistant { .. })) {
                assert_eq!(c.messages.iter().filter(|m| matches!(m, Message::Tool { .. })).count(), 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(after.load(Ordering::SeqCst), 1);
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}
#[cfg(unix)]
#[tokio::test]
async fn unsafe_selected_hooks_resume_before_and_after_without_replaying_tool() {
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
                .args(["--exact", "selected_hook_kill_worker"])
                .env("PI_SELECTED_HOOK_DIR", dir.path())
                .env("PI_SELECTED_HOOK_UNSAFE", "1")
                .env("PI_SELECTED_HOOK_CUT", stage)
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
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let id = std::fs::read_to_string(dir.path().join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let before = Arc::new(AtomicUsize::new(0));
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup_unsafe_gated(
            1,
            before.clone(),
            after.clone(),
            executions.clone(),
            "v1",
            None,
        );
        let scheduler =
            Scheduler::open_with_extensions(dir.path().join("db"), options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        until_result(&scheduler, id).await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            if stage == "after" { 0 } else { 1 }
        );
        assert_eq!(after.load(Ordering::SeqCst), 1);
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(
                &store,
                tasks[0].task.id()
            )
            .unwrap()
            .len(),
            1
        );
        drop(store);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_selected_multilevel_park_recovers_leaf_after_hook() {
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
            .args(["--exact", "selected_hook_kill_worker"])
            .env("PI_SELECTED_HOOK_DIR", dir.path())
            .env("PI_SELECTED_HOOK_UNSAFE", "1")
            .env("PI_SELECTED_HOOK_DEPTH3", "1")
            .env("PI_SELECTED_HOOK_CUT", "leaf-after")
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
            .filter(|t| matches!(t.task.state(), pi_rs_durable::task::TaskState::Waiting(_)))
            .count(),
        2
    );
    drop(store);
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_unsafe_gated(
        3,
        before.clone(),
        after.clone(),
        executions.clone(),
        "v1",
        None,
    );
    let scheduler =
        Scheduler::open_with_extensions(dir.path().join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    until_result(&scheduler, id).await;
    assert_eq!(before.load(Ordering::SeqCst), 0);
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    assert_eq!(after.load(Ordering::SeqCst), 3);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_selected_hook_interrupted_tool_is_not_reexecuted() {
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
            .args(["--exact", "selected_hook_kill_worker"])
            .env("PI_SELECTED_HOOK_DIR", dir.path())
            .env("PI_SELECTED_HOOK_UNSAFE", "1")
            .env("PI_SELECTED_HOOK_CUT", "execute")
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
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let id = std::fs::read_to_string(dir.path().join("conversation"))
        .unwrap()
        .parse()
        .unwrap();
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_unsafe_gated(
        1,
        before.clone(),
        after.clone(),
        executions.clone(),
        "v1",
        None,
    );
    let mut scheduler =
        Scheduler::open_with_extensions(dir.path().join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(after.load(Ordering::SeqCst), 0);
    assert!(
        !scheduler
            .conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { .. }))
    );
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
        .list()
        .unwrap();
    assert!(
        matches!(tasks[0].task.state(), pi_rs_durable::task::TaskState::Failed(reason) if reason.contains("reconciliation"))
    );
    drop(store);
    let _ = driver.shutdown_with_pending();
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
async fn unsafe_selected_hook_abort_fences_before_and_after() {
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup_unsafe_gated(
            1,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            executions.clone(),
            "v1",
            Some((stage, gate.clone())),
        );
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
        tokio::time::timeout(Duration::from_secs(15), async {
            while gate.load(Ordering::SeqCst) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(15), scheduler.abort_and_wait(id))
            .await
            .unwrap();
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap();
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].task.is_terminal());
        assert_eq!(
            executions.load(Ordering::SeqCst),
            if stage == "before" { 0 } else { 1 }
        );
        assert!(
            store
                .get(format!("runtime/extension-memo/{}/late", tasks[0].task.id()).as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|m| matches!(m,Message::Tool {content,..} if content == "root-after"))
        );
        let receipts = pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(
            &store,
            tasks[0].task.id(),
        )
        .unwrap();
        assert_eq!(receipts.len(), if stage == "before" { 0 } else { 1 });
        drop(store);
        let _ = driver.shutdown_with_pending();
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

#[tokio::test]
async fn unsafe_selection_change_fences_hook_and_publication() {
    for stage in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let gate = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup_unsafe_gated(
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

struct RichAdapter;
#[async_trait]
impl pi_rs_durable::model::ProviderAdapter for RichAdapter {
    fn id(&self) -> &str {
        "rich-fixture/v1"
    }
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> Result<Value, Error> {
        Ok(json!({"tool_blocks":{}}))
    }
    fn validate(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: &Value,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        r: &pi_rs_durable::model::DurableToolRequest,
    ) -> Result<Value, Error> {
        r.validate_bindings()?;
        Ok(json!({"tool_blocks":r.tool_blocks}))
    }
    fn validate_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        r: &pi_rs_durable::model::DurableToolRequest,
        body: &Value,
    ) -> Result<(), Error> {
        r.validate_bindings()?;
        if body["tool_blocks"] != json!(r.tool_blocks) {
            return Err(Error::Config("blocks changed".into()));
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream, Error> {
        let done = !body["tool_blocks"].as_object().unwrap().is_empty();
        if done {
            let binding = body["tool_blocks"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            assert_eq!(binding["blocks"][0]["text"], "first");
            assert!(
                binding["blocks"][1]["text"]
                    .as_str()
                    .unwrap()
                    .starts_with("second")
            );
            let encoded = binding.to_string();
            assert!(encoded.contains("hook diagnostic"));
            assert!(!encoded.contains("blocked by policy"));
            assert!(!encoded.contains("tool failure"));
        }
        if done && let Some(path) = std::env::var_os("PI_RICH_AFTER_READY") {
            std::fs::write(path, "ready").unwrap();
            std::future::pending::<()>().await;
        }
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
struct RichHook {
    mode: &'static str,
    after: Arc<AtomicUsize>,
}
#[async_trait]
impl Hook for RichHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if self.mode == "block" {
            return Ok(BeforeTool::Block("blocked by policy".into()));
        }
        let mut args = call.arguments.clone();
        args["prepared_by_hook"] = json!(true);
        Ok(BeforeTool::Rewrite(args))
    }
    async fn after_tool_blocks(
        &self,
        _: &pi_rs_durable::extension::ToolCall,
        result: &pi_rs_durable::extension::ToolBlockResult,
        _: bool,
        ctx: &Context<'_>,
    ) -> Result<Option<pi_rs_durable::extension::ToolBlockResult>, String> {
        if self.mode == "invalid-structured-append"
            && let Some(path) = std::env::var_os("PI_REVIEW_AFTER_GATE")
        {
            std::fs::write(path, b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        if self.mode == "invalid-structured" {
            self.after.fetch_add(1, Ordering::SeqCst);
            return Ok(None);
        }
        if self.mode == "invalid-structured-append" {
            self.after.fetch_add(1, Ordering::SeqCst);
            let mut replaced = result.clone();
            let prior = replaced.output[0]["text"].as_str().unwrap();
            assert!(!prior.contains("schema-secret"));
            replaced.output[0]["text"] = format!("{prior}\nintentional hook text").into();
            return Ok(Some(replaced));
        }
        if self.mode == "invalid-structured-legacy" {
            self.after.fetch_add(1, Ordering::SeqCst);
            let mut replaced = result.clone();
            let projection =
                json!({"data":result.extra["structuredOutput"], "attachments":[]}).to_string();
            replaced.output[0]["text"] = format!("{projection}\nintentional hook text").into();
            return Ok(Some(replaced));
        }
        if self.mode == "invalid-simple" {
            self.after.fetch_add(1, Ordering::SeqCst);
            let mut replaced = result.clone();
            replaced.output = vec![json!({"type":"text", "text":"looks successful"})];
            return Ok(Some(replaced));
        }
        assert_eq!(
            ctx.memo
                .get_or_insert("rich-after", json!("stable"))
                .await?,
            json!("stable")
        );
        self.after.fetch_add(1, Ordering::SeqCst);
        let mut result = result.clone();
        result.output = vec![
            json!({"type":"text","text":"first"}),
            json!({"type":"text","text":"second"}),
        ];
        result.extra.insert("details".into(), json!({"kept":true}));
        result
            .extra
            .insert("structuredOutput".into(), json!({"value":7}));
        result.extra.insert(
            "attachments".into(),
            json!([{"id":"ref-1","media_type":"text/plain"}]),
        );
        result
            .diagnostics
            .push(pi_rs_durable::runtime::ToolDiagnostic {
                severity: pi_rs_durable::runtime::DiagnosticSeverity::Warn,
                message: "hook diagnostic".into(),
                code: Some("hook_note".into()),
            });
        if self.mode == "diagnostic-collision" {
            result
                .diagnostics
                .push(pi_rs_durable::runtime::ToolDiagnostic {
                    severity: pi_rs_durable::runtime::DiagnosticSeverity::Error,
                    message: "explicit hook warning must reach the model".into(),
                    code: Some("tool_failed".into()),
                });
        }
        Ok(Some(result))
    }
}
struct ErrorJob {
    mode: &'static str,
    executions: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for ErrorJob {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "root".into(),
            description: String::new(),
            parameters: if self.mode == "schema" {
                json!({"type":"object","required":["required"]})
            } else {
                json!({"type":"object"})
            },
        }
    }
    async fn prepare_arguments(&self, args: Value) -> pi_rs_core::ExtensionResult<Value> {
        if self.mode == "prepare" {
            Err("preparation failure".into())
        } else {
            Ok(args)
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        unreachable!()
    }
}
#[async_trait]
impl DurableAsyncTool for ErrorJob {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Ok(DurableToolStep::Complete(
            if self.mode.starts_with("invalid-structured") {
                Outcome::Structured {
                    data: json!({"secret":"schema-secret"}),
                    attachments: vec![],
                }
            } else if self.mode == "failure" {
                Outcome::Failed("tool failure".into())
            } else {
                Outcome::Succeeded("ok".into())
            },
        ))
    }
}

#[tokio::test]
async fn invalid_structured_projection_is_not_sent_to_model() {
    for mode in [
        "invalid-structured",
        "invalid-structured-append",
        "invalid-structured-legacy",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (mut options, registry, extensions) = setup_rich(
            mode,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        options.model = Arc::new(ReviewModel(seen.clone(), "{}"));
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
        scheduler
            .set_tool_output_schema(
                id,
                "root".into(),
                Some(json!({"type":"object","required":["value"]})),
            )
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        let c = scheduler.conversation(id).unwrap();
        assert!(c.tool_results[0].structured_output.is_none());
        assert!(
            c.tool_results[0]
                .diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("invalid_structured_output"))
        );
        assert!(!c.messages.iter().any(
            |m| matches!(m, Message::Tool { content, .. } if content.contains("schema-secret"))
        ));
        assert!(
            !seen
                .lock()
                .unwrap()
                .iter()
                .any(|s| s.contains("schema-secret"))
        );
        if mode != "invalid-structured" {
            assert!(
                seen.lock()
                    .unwrap()
                    .iter()
                    .any(|s| s.contains("intentional hook text"))
            );
        }
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_error_status_reaches_model_without_status_adapter() {
    let dir = tempfile::tempdir().unwrap();
    let (mut options, registry, extensions) = setup_rich(
        "invalid-simple",
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    options.model = Arc::new(ReviewModel(seen.clone(), "[1,2]"));
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
    tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    let c = scheduler.conversation(id).unwrap();
    assert!(c.tool_results[0].is_error);
    assert!(
        c.tool_results[0]
            .diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("invalid_arguments"))
    );
    assert!(c.messages.iter().any(
        |m| matches!(m, Message::Tool {content,..} if content == "looks successful\n[tool error]")
    ));
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|s| s == "looks successful\n[tool error]")
    );
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn result_publication_reopen_worker() {
    let Some(root) = std::env::var_os("PI_RESULT_PUBLICATION_DIR") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mode = if root.join("structured").exists() {
        "invalid-structured-append"
    } else {
        "invalid-simple"
    };
    let raw = if mode == "invalid-simple" {
        "[1,2]"
    } else {
        "{}"
    };
    let (mut options, registry, extensions) = setup_rich(
        mode,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    options.model = Arc::new(ReviewModel(
        Arc::new(std::sync::Mutex::new(Vec::new())),
        raw,
    ));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
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
    if mode != "invalid-simple" {
        scheduler
            .set_tool_output_schema(
                id,
                "root".into(),
                Some(json!({"type":"object","required":["value"]})),
            )
            .unwrap();
    }
    std::fs::write(root.join("id"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}

#[cfg(unix)]
#[tokio::test]
async fn validated_publication_and_error_status_survive_kill_reopen() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for mode in ["invalid-structured-append", "invalid-simple"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        if mode != "invalid-simple" {
            std::fs::write(root.join("structured"), b"yes").unwrap();
        }
        let mut child = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "result_publication_reopen_worker"])
                .env("PI_RESULT_PUBLICATION_DIR", root)
                .env("PI_REVIEW_PUBLICATION_READY", root.join("ready"))
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(15), async {
            while !root.join("ready").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let id = std::fs::read_to_string(root.join("id"))
            .unwrap()
            .parse()
            .unwrap();
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (mut options, registry, extensions) =
            setup_rich(mode, after.clone(), executions.clone());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        options.model = Arc::new(ReviewModel(
            seen.clone(),
            if mode == "invalid-simple" {
                "[1,2]"
            } else {
                "{}"
            },
        ));
        let mut scheduler =
            Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        let c = scheduler.conversation(id).unwrap();
        assert_eq!(c.tool_results.len(), 1);
        assert_eq!(
            c.messages
                .iter()
                .filter(|m| matches!(m, Message::Tool { .. }))
                .count(),
            1
        );
        let text = c
            .messages
            .iter()
            .find_map(|m| {
                if let Message::Tool { content, .. } = m {
                    Some(content.as_str())
                } else {
                    None
                }
            })
            .unwrap();
        if mode == "invalid-simple" {
            assert_eq!(text, "looks successful\n[tool error]");
            assert!(c.tool_results[0].is_error);
        } else {
            assert!(!text.contains("schema-secret"));
            assert!(text.contains("intentional hook text"));
        }
        assert_eq!(after.load(Ordering::SeqCst), 0);
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_structured_checkpoint_replays_after_hook_without_tool() {
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::write(root.join("structured"), b"yes").unwrap();
    let mut child = Kill(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "result_publication_reopen_worker"])
            .env("PI_RESULT_PUBLICATION_DIR", root)
            .env("PI_REVIEW_AFTER_GATE", root.join("ready"))
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !root.join("ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let id = std::fs::read_to_string(root.join("id"))
        .unwrap()
        .parse()
        .unwrap();
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (mut options, registry, extensions) = setup_rich(
        "invalid-structured-append",
        after.clone(),
        executions.clone(),
    );
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    options.model = Arc::new(ReviewModel(seen.clone(), "{}"));
    let mut scheduler =
        Scheduler::open_with_extensions(root.join("db"), options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    let c = scheduler.conversation(id).unwrap();
    assert_eq!(c.tool_results.len(), 1);
    assert!(
        seen.lock()
            .unwrap()
            .iter()
            .any(|s| s.contains("intentional hook text"))
    );
    assert!(
        !seen
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.contains("schema-secret"))
    );
    assert_eq!(after.load(Ordering::SeqCst), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
fn setup_rich(
    mode: &'static str,
    after: Arc<AtomicUsize>,
    executions: Arc<AtomicUsize>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(RichHook {
                    mode,
                    after: after.clone(),
                })],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "rich-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let mut registry = Registry::default();
    let root = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(ErrorJob {
            mode,
            executions: executions.clone(),
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
    (options, registry, extensions)
}
fn setup_rich_unsafe(
    mode: &'static str,
    after: Arc<AtomicUsize>,
    executions: Arc<AtomicUsize>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(RichHook {
                    mode,
                    after: after.clone(),
                })],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "rich-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let mut registry = Registry::default();
    let root = register_conversation_unsafe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(ErrorJob {
            mode,
            executions: executions.clone(),
        }),
        "root",
        1,
        vec![],
        &extensions.snapshot(),
        &["policy".into()],
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
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
    (options, registry, extensions)
}
#[tokio::test]
async fn selected_after_rich_and_error_results_resume_conversation() {
    for mode in ["success", "failure", "block", "schema"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) = setup_rich(mode, after.clone(), executions.clone());
        let mut scheduler = Scheduler::open_with_provider_adapter(
            &path,
            options,
            extensions,
            Arc::new(RichAdapter),
        )
        .unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.select_provider_adapter(id, true).unwrap();
        scheduler
            .select_extensions(id, vec!["policy".into()])
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        let c = scheduler.conversation(id).unwrap();
        let meta = &c.tool_results[0];
        assert_eq!(meta.is_error, mode != "success", "{mode}");
        assert_eq!(meta.details, Some(json!({"kept":true})), "{mode}");
        assert_eq!(meta.structured_output, Some(json!({"value":7})), "{mode}");
        assert_eq!(meta.attachments.len(), 1, "{mode}");
        assert!(
            meta.diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("hook_note")),
            "{mode}"
        );
        if mode == "block" || mode == "schema" {
            assert!(
                meta.diagnostics
                    .iter()
                    .any(|d| d.severity == pi_rs_durable::runtime::DiagnosticSeverity::Error),
                "{mode}"
            );
        }
        let blocks = c
            .content_blocks
            .values()
            .find(|b| Some(b.message_id) == meta.content_block_id)
            .unwrap();
        assert_eq!(blocks.blocks[0]["text"], "first");
        assert_eq!(blocks.blocks[1]["text"], "second");
        assert_eq!(
            serde_json::to_string(&blocks.blocks)
                .unwrap()
                .contains("[tool error]"),
            mode != "success"
        );
        assert!(
            c.messages
                .iter()
                .any(|m| matches!(m, Message::Assistant { .. }))
        );
        assert_eq!(after.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(
            executions.load(Ordering::SeqCst),
            usize::from(mode == "success" || mode == "failure"),
            "{mode}"
        );
        let store = durable_store::Store::open(&path).unwrap();
        let root = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap()[0]
            .task
            .id();
        assert!(
            store
                .get(format!("runtime/extension-memo/{root}/rich-after").as_bytes())
                .unwrap()
                .is_some()
        );
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_selected_after_rich_and_error_results_resume_conversation() {
    for mode in ["success", "failure", "block", "schema", "prepare"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let after = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let (options, registry, extensions) =
            setup_rich_unsafe(mode, after.clone(), executions.clone());
        let mut scheduler = Scheduler::open_with_provider_adapter(
            &path,
            options,
            extensions,
            Arc::new(RichAdapter),
        )
        .unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        scheduler.select_provider_adapter(id, true).unwrap();
        scheduler
            .select_extensions(id, vec!["policy".into()])
            .unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        let c = scheduler.conversation(id).unwrap();
        let meta = &c.tool_results[0];
        assert_eq!(meta.is_error, mode != "success", "{mode}");
        assert_eq!(meta.details, Some(json!({"kept":true})), "{mode}");
        assert_eq!(meta.structured_output, Some(json!({"value":7})), "{mode}");
        assert_eq!(meta.attachments.len(), 1, "{mode}");
        assert!(
            meta.diagnostics
                .iter()
                .any(|d| d.code.as_deref() == Some("hook_note")),
            "{mode}"
        );
        if mode == "block" || mode == "schema" {
            assert!(
                meta.diagnostics
                    .iter()
                    .any(|d| d.severity == pi_rs_durable::runtime::DiagnosticSeverity::Error),
                "{mode}"
            );
        }
        let blocks = c
            .content_blocks
            .values()
            .find(|b| Some(b.message_id) == meta.content_block_id)
            .unwrap();
        assert_eq!(blocks.blocks[0]["text"], "first");
        assert_eq!(blocks.blocks[1]["text"], "second");
        assert_eq!(
            serde_json::to_string(&blocks.blocks)
                .unwrap()
                .contains("[tool error]"),
            mode != "success"
        );
        assert!(
            c.messages
                .iter()
                .any(|m| matches!(m, Message::Assistant { .. }))
        );
        assert_eq!(after.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(
            executions.load(Ordering::SeqCst),
            usize::from(mode == "success" || mode == "failure"),
            "{mode}"
        );
        let store = durable_store::Store::open(&path).unwrap();
        let root = pi_rs_durable::task_store::TaskStore::new(&store)
            .list()
            .unwrap()[0]
            .task
            .id();
        assert!(
            store
                .get(format!("runtime/extension-memo/{root}/rich-after").as_bytes())
                .unwrap()
                .is_some()
        );
        driver.shutdown().unwrap();
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn review_explicit_hook_diagnostic_with_reserved_code_is_omitted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_rich("diagnostic-collision", after, executions);
    let mut scheduler =
        Scheduler::open_with_provider_adapter(&path, options, extensions, Arc::new(RichAdapter))
            .unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.select_provider_adapter(id, true).unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    let c = scheduler.conversation(id).unwrap();
    assert!(
        c.tool_results[0]
            .diagnostics
            .iter()
            .any(|d| d.message == "explicit hook warning must reach the model")
    );
    let blocks = c
        .content_blocks
        .values()
        .find(|b| Some(b.message_id) == c.tool_results[0].content_block_id)
        .unwrap();
    assert!(
        !serde_json::to_string(&blocks.blocks)
            .unwrap()
            .contains("explicit hook warning must reach the model")
    );
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rich_after_kill_worker() {
    let Some(dir) = std::env::var_os("PI_RICH_AFTER_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (options, registry, extensions) = setup_rich(
        "failure",
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let mut scheduler = Scheduler::open_with_provider_adapter(
        dir.join("db"),
        options,
        extensions,
        Arc::new(RichAdapter),
    )
    .unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.select_provider_adapter(id, true).unwrap();
    scheduler
        .select_extensions(id, vec!["policy".into()])
        .unwrap();
    std::fs::write(dir.join("id"), id.to_string()).unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn rich_after_published_result_survives_process_kill() {
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
            .args(["--exact", "rich_after_kill_worker"])
            .env("PI_RICH_AFTER_DIR", dir.path())
            .env("PI_RICH_AFTER_READY", dir.path().join("ready"))
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
    let id = std::fs::read_to_string(dir.path().join("id"))
        .unwrap()
        .parse()
        .unwrap();
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_rich("failure", after.clone(), executions.clone());
    let (mut scheduler, driver) = Scheduler::open_with_pinned_provider_adapter_and_tasks(
        dir.path().join("db"),
        options,
        extensions,
        Arc::new(RichAdapter),
        registry,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    let c = scheduler.conversation(id).unwrap();
    let meta = &c.tool_results[0];
    assert!(meta.is_error);
    assert_eq!(meta.details, Some(json!({"kept":true})));
    assert_eq!(meta.structured_output, Some(json!({"value":7})));
    assert_eq!(meta.attachments.len(), 1);
    assert!(
        meta.diagnostics
            .iter()
            .any(|d| d.code.as_deref() == Some("hook_note"))
    );
    let blocks = c
        .content_blocks
        .values()
        .find(|b| Some(b.message_id) == meta.content_block_id)
        .unwrap();
    assert_eq!(blocks.blocks[0]["text"], "first");
    assert_eq!(blocks.blocks[1]["text"], "second");
    assert!(
        serde_json::to_string(&blocks.blocks)
            .unwrap()
            .contains("[tool error]")
    );
    assert_eq!(after.load(Ordering::SeqCst), 0);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

struct IncompleteHook {
    gate: Option<Arc<AtomicUsize>>,
}
#[async_trait]
impl Hook for IncompleteHook {
    async fn before_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Context<'_>,
    ) -> Result<BeforeTool, String> {
        if call.name == "leaf" {
            Ok(BeforeTool::Block("denied-leaf".into()))
        } else {
            Ok(BeforeTool::Pass)
        }
    }
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &Value,
        ctx: &Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name != "leaf" {
            return Ok(None);
        }
        ctx.memo
            .get_or_insert("incomplete-after", json!("kept"))
            .await?;
        if let Some(path) = std::env::var_os("PI_INCOMPLETE_HOOK_CUT") {
            std::fs::write(path, "entered").unwrap();
            std::future::pending::<()>().await;
        }
        if let Some(gate) = &self.gate {
            gate.store(1, Ordering::SeqCst);
            while gate.load(Ordering::SeqCst) != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Err("after hook failed".into())
    }
}
struct IncompleteParent {
    seen: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for IncompleteParent {
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
impl DurableAsyncTool for IncompleteParent {
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
            match host.child_result(&child).await? {
                Some(AsyncTaskResult::AfterIncomplete(Outcome::Failed(reason)))
                    if reason.contains("denied-leaf") =>
                {
                    self.seen.fetch_add(1, Ordering::SeqCst);
                    return Ok(DurableToolStep::Complete(Outcome::Succeeded(
                        "parent-saw-incomplete".into(),
                    )));
                }
                other => return Err(format!("expected known incomplete host failure: {other:?}")),
            }
        }
        let child = host.start_child("leaf", "leaf", json!({})).await?;
        Ok(DurableToolStep::Park {
            checkpoint: serde_json::to_value(&child).unwrap(),
            wait: DurableWait::All(vec![child]),
        })
    }
}
fn setup_incomplete(
    seen: Arc<AtomicUsize>,
    executions: Arc<AtomicUsize>,
    gate: Option<Arc<AtomicUsize>>,
) -> (Options, Registry, pi_rs_durable::extension::Registry) {
    let mut extensions = pi_rs_durable::extension::Registry::default();
    extensions
        .install_identified(
            Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(IncompleteHook { gate })],
                ..Default::default()
            },
            ExtensionIdentity::new(1, "incomplete-v1".into(), vec!["hook-v1".into()]).unwrap(),
        )
        .unwrap();
    let snapshot = extensions.snapshot();
    let names = ["policy".into()];
    let mut registry = Registry::default();
    let child = register_conversation_unsafe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(Job {
            name: "leaf",
            child: None,
            executions,
        }),
        "incomplete-leaf",
        1,
        vec![],
        &snapshot,
        &names,
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    let root = register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry,
        Arc::new(IncompleteParent { seen }),
        "incomplete-parent",
        1,
        vec![child.clone()],
        &snapshot,
        &names,
        ReplaySafeAuthorization::host_attests_replay_safe(),
        ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    (
        Options {
            model: Arc::new(ModelStub),
            tools: vec![root.register(), child.register()],
            restart_limit: 5,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
        registry,
        extensions,
    )
}
async fn start_incomplete(scheduler: &mut Scheduler) -> uuid::Uuid {
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
    id
}
async fn wait_incomplete_parent(scheduler: &Scheduler, id: uuid::Uuid) {
    tokio::time::timeout(Duration::from_secs(15), async { loop {
        let c = scheduler.conversation(id).unwrap();
        if let Some(i) = c.messages.iter().position(|m| matches!(m, Message::Tool { content, .. } if content == "parent-saw-incomplete")) && c.messages[i+1..].iter().any(|m| matches!(m, Message::Assistant { .. })) { break; }
        tokio::time::sleep(Duration::from_millis(10)).await;
    } }).await.unwrap();
}
#[tokio::test]
async fn unsafe_child_no_dispatch_after_failure_reaches_parent_as_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let seen = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_incomplete(seen.clone(), executions.clone(), None);
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start_incomplete(&mut scheduler).await;
    wait_incomplete_parent(&scheduler, id).await;
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn unsafe_child_incomplete_worker() {
    let Some(dir) = std::env::var_os("PI_INCOMPLETE_HOOK_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let (options, registry, extensions) = setup_incomplete(
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        None,
    );
    let mut scheduler =
        Scheduler::open_with_extensions(dir.join("db"), options, extensions).unwrap();
    let _driver = scheduler.start_tasks(registry).unwrap();
    let id = start_incomplete(&mut scheduler).await;
    std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
    std::future::pending::<()>().await;
}
#[cfg(unix)]
#[tokio::test]
async fn unsafe_child_incomplete_after_survives_kill_and_reopen() {
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
            .args(["--exact", "unsafe_child_incomplete_worker"])
            .env("PI_INCOMPLETE_HOOK_DIR", dir.path())
            .env("PI_INCOMPLETE_HOOK_CUT", dir.path().join("cut"))
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        while !dir.path().join("cut").exists() || !dir.path().join("conversation").exists() {
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
    let leaf = tasks
        .iter()
        .find(|t| matches!(t.task.owner(), pi_rs_durable::task::TaskOwner::Task(_)))
        .unwrap()
        .task
        .id();
    let memo_key = format!("runtime/extension-memo/{leaf}/incomplete-after");
    let memo = store.get(memo_key.as_bytes()).unwrap().unwrap().value;
    drop(store);
    let seen = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_incomplete(seen.clone(), executions.clone(), None);
    let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    wait_incomplete_parent(&scheduler, id).await;
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(store.get(memo_key.as_bytes()).unwrap().unwrap().value, memo);
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_receipt_conflicting_with_no_dispatch_never_reaches_parent_as_known() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) =
        setup_incomplete(seen.clone(), executions.clone(), Some(gate.clone()));
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let id = start_incomplete(&mut scheduler).await;
    tokio::time::timeout(Duration::from_secs(15), async {
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
    let leaf = &tasks
        .iter()
        .find(|t| matches!(t.task.owner(), pi_rs_durable::task::TaskOwner::Task(_)))
        .unwrap()
        .task;
    let task_id = leaf.id();
    let prior = uuid::Uuid::new_v4();
    let mut recorded = serde_json::to_value(leaf).unwrap();
    recorded["attempt_id"] = json!(prior);
    let intent = format!("durable/unsafe-park-async/{task_id}/{prior}/intent").into_bytes();
    let mut receipt = intent.clone();
    receipt.extend_from_slice(b"/receipt");
    store.commit(&[durable_store::Check { key: intent.clone(), expected: durable_store::Expected::Missing }, durable_store::Check { key: receipt.clone(), expected: durable_store::Expected::Missing }], &[durable_store::Mutation::Put { key: intent, value: serde_json::to_vec(&recorded).unwrap() }, durable_store::Mutation::Put { key: receipt, value: serde_json::to_vec(&json!({"task_id":task_id,"attempt_id":prior,"outcome":Outcome::Succeeded("late-effect".into())})).unwrap() }]).unwrap();
    drop(store);
    gate.store(2, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seen.load(Ordering::SeqCst), 0);
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(
        !scheduler.conversation(id).unwrap().messages.iter().any(
            |m| matches!(m, Message::Tool { content, .. } if content == "parent-saw-incomplete")
        )
    );
    let store = durable_store::Store::open(&path).unwrap();
    assert_eq!(
        pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, task_id)
            .unwrap()
            .len(),
        1
    );
    let _ = driver.shutdown_with_pending();
    let _ = scheduler.shutdown().await;
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn unsafe_initial_memo_busy_retries_without_dispatching_early() {
    use pi_rs_durable::task_scheduler::hook_fence_test_hook;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let gate = Arc::new(AtomicUsize::new(0));
    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let executions = Arc::new(AtomicUsize::new(0));
    let (options, registry, extensions) = setup_unsafe_gated(
        1,
        before.clone(),
        after.clone(),
        executions.clone(),
        "busy-v1",
        Some(("before", gate.clone())),
    );
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
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
    tokio::time::timeout(Duration::from_secs(15), async {
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
    hook_fence_test_hook::install(task.id(), "memo", move || {
        at_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    hook_fence_test_hook::install(task.id(), "busy", move || {
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
        std::thread::sleep(Duration::from_millis(6500));
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    resume_tx.send(()).unwrap();
    busy_rx
        .recv_timeout(Duration::from_secs(7))
        .expect("unsafe initial memo did not reach busy timeout");
    writer.join().unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while gate.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(
        store
            .list(format!("durable/unsafe-park-async/{}/", task.id()).as_bytes())
            .unwrap()
            .is_empty()
    );
    gate.store(2, Ordering::SeqCst);
    until_result(&scheduler, id).await;
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(before.load(Ordering::SeqCst), 1);
    assert_eq!(after.load(Ordering::SeqCst), 1);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}
