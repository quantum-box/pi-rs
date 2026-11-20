// Real driver tests with a deterministic competing writer at claim commit.
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
struct ClaimPassHook(Option<Arc<std::sync::atomic::AtomicUsize>>, bool);
#[async_trait]
impl crate::extension::Hook for ClaimPassHook {
    async fn before_tool(
        &self,
        call: &crate::extension::ToolCall,
        _: &crate::extension::Context<'_>,
    ) -> Result<crate::extension::BeforeTool, String> {
        if call.name == "root" {
            Ok(crate::extension::BeforeTool::Rewrite(
                json!({"before":"prepared"}),
            ))
        } else {
            Ok(crate::extension::BeforeTool::Pass)
        }
    }
    async fn after_tool(
        &self,
        call: &crate::extension::ToolCall,
        _: &Value,
        ctx: &crate::extension::Context<'_>,
    ) -> Result<Option<Value>, String> {
        if call.name == "leaf" && self.1 {
            assert_eq!(
                ctx.memo
                    .get_or_insert("nested-abort", json!("saved"))
                    .await?,
                json!("saved")
            );
            return Ok(Some(json!("nested after cancel")));
        }
        if call.name == "root"
            && let Some(count) = &self.0
        {
            assert!(ctx.snapshot.get("id").is_some());
            assert_eq!(call.arguments["before"], "prepared");
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.1 {
                assert_eq!(
                    ctx.memo.get_or_insert("abort-memo", json!("saved")).await?,
                    json!("saved")
                );
                if std::env::var("PI_SELECTED_CANCEL_CUT").ok().as_deref() == Some("after_memo") {
                    let dir = std::path::PathBuf::from(
                        std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
                    );
                    let store = durable_store::Store::open(dir.join("db")).unwrap();
                    let (_, claim) = store
                        .list(b"durable/replay-safe-async/")
                        .unwrap()
                        .into_iter()
                        .find(|(key, _)| key.ends_with(b"/conversation-park-cancel-claim"))
                        .unwrap();
                    std::fs::write(dir.join("ready"), claim.value).unwrap();
                    loop {
                        std::thread::park();
                    }
                }
            }
            return Ok(Some(json!("after cancel")));
        }
        Ok(None)
    }
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
async fn exercise_conversation_park_claim(case: &str) {
    use pi_rs_durable::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_park_cancel,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Cancel(
        Arc<AtomicUsize>,
        String,
        std::path::PathBuf,
        pi_rs_durable::nested_identity::IdentityHandle,
        Arc<std::sync::Mutex<Option<Scheduler>>>,
    );
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel_scoped(
            &self,
            arguments: Value,
            checkpoint: Value,
            context: ToolContext,
            scope: durable_store::controls::CallbackScope,
        ) -> Result<(), String> {
            if self.1 == "dispatch_spawn_unscoped_cancel" {
                let path = self.2.clone();
                let cancellation = context.cancellation.clone();
                let child = tokio::spawn(async move {
                    let store = durable_store::Store::open(path).unwrap();
                    let task = pi_rs_durable::task_store::TaskStore::new(&store)
                        .load(context.operation_id)
                        .unwrap()
                        .unwrap();
                    let pi_rs_durable::task::TaskOwner::Conversation(conversation) =
                        task.task.owner()
                    else {
                        panic!("not a root")
                    };
                    let signal = crate::conversation_signal::Signal::new(
                        uuid::Uuid::new_v4(),
                        *conversation,
                        "unscoped".into(),
                        json!(1),
                    );
                    crate::conversation_signal::Inbox::new(&store)
                        .send_with_retry(&signal, &context.cancellation)
                        .await
                });
                tokio::time::sleep(Duration::from_millis(30)).await;
                cancellation.cancel();
                assert!(matches!(
                    child.await.unwrap(),
                    Err(crate::conversation_signal::Error::Cancelled)
                ));
                self.0.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
            if self.1.starts_with("dispatch_spawn_") {
                let this = self.clone();
                return tokio::spawn(async move {
                    scope
                        .scope(this.cancel(arguments, checkpoint, context))
                        .await
                })
                .await
                .map_err(|error| error.to_string())?;
            }
            self.cancel(arguments, checkpoint, context).await
        }
        async fn cancel(
            &self,
            arguments: Value,
            _: Value,
            context: ToolContext,
        ) -> Result<(), String> {
            if self.1.starts_with("selection_hook") {
                assert_eq!(arguments["before"], "prepared");
            }
            self.0.fetch_add(1, Ordering::SeqCst);
            if self.1 == "nested_claim_kill" {
                let dir = std::path::PathBuf::from(
                    std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
                );
                std::fs::write(dir.join("ready"), context.operation_id.to_string()).unwrap();
                std::future::pending::<()>().await;
            }
            if self.1 == "dispatch_late" || self.1 == "nested_claim_late_kill" {
                let store = durable_store::Store::open(&self.2).unwrap();
                let prefix = format!("durable/unsafe-park-async/{}/", context.operation_id);
                let (mut key, entry) = store
                    .list(prefix.as_bytes())
                    .unwrap()
                    .into_iter()
                    .find(|(key, _)| key.ends_with(b"/intent"))
                    .unwrap();
                let intent: pi_rs_durable::task::Task =
                    serde_json::from_slice(&entry.value).unwrap();
                let receipt = pi_rs_durable::async_task_tool::UnsafeDurableReceipt {
                    task_id: context.operation_id,
                    attempt_id: intent.attempt_id().unwrap(),
                    outcome: Outcome::Pending {
                        reference: "late-job".into(),
                    },
                };
                key.extend_from_slice(b"/receipt");
                store
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Missing,
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: serde_json::to_vec(&receipt).unwrap(),
                        }],
                    )
                    .unwrap();
                if self.1 == "nested_claim_late_kill" {
                    let dir = std::path::PathBuf::from(
                        std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
                    );
                    std::fs::write(dir.join("ready"), context.operation_id.to_string()).unwrap();
                    std::future::pending::<()>().await;
                }
            }
            if self.1 == "dispatch_hold" || self.1 == "dispatch_hold_cleanup" {
                while !self.2.with_extension("release").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
            if self.1 == "dispatch_panic" {
                panic!("cancel panic");
            }
            if self.1 == "dispatch_reentrant" || self.1 == "dispatch_spawn_signal" {
                // Test after a yield as well as the first poll: the callback
                // must never wait for the marker it owns.
                tokio::task::yield_now().await;
                let store = durable_store::Store::open(&self.2).unwrap();
                let task = pi_rs_durable::task_store::TaskStore::new(&store)
                    .load(context.operation_id)
                    .unwrap()
                    .unwrap();
                let pi_rs_durable::task::TaskOwner::Conversation(conversation) = task.task.owner()
                else {
                    panic!("not a root")
                };
                let signal = crate::conversation_signal::Signal::new(
                    uuid::Uuid::new_v4(),
                    *conversation,
                    "reentrant".into(),
                    json!(1),
                );
                let result = tokio::time::timeout(
                    Duration::from_secs(1),
                    crate::conversation_signal::Inbox::new(&store)
                        .send_with_retry(&signal, &context.cancellation),
                )
                .await;
                assert!(matches!(
                    result,
                    Ok(Err(crate::conversation_signal::Error::ReentrantCallback))
                ));
                assert!(
                    crate::task_store::reentrant_callback(&store, context.operation_id).unwrap()
                );
            }
            if self.1 == "dispatch_reentrant_abort" || self.1 == "dispatch_spawn_abort" {
                let store = durable_store::Store::open(&self.2).unwrap();
                let task = pi_rs_durable::task_store::TaskStore::new(&store)
                    .load(context.operation_id)
                    .unwrap()
                    .unwrap();
                let pi_rs_durable::task::TaskOwner::Conversation(conversation) = task.task.owner()
                else {
                    panic!("not a root")
                };
                let mut scheduler = loop {
                    let next = self.4.lock().unwrap().take();
                    if let Some(scheduler) = next {
                        break scheduler;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                };
                let result = tokio::time::timeout(
                    Duration::from_secs(1),
                    scheduler.abort_and_wait(*conversation),
                )
                .await;
                assert!(matches!(
                    result,
                    Ok(Err(crate::runtime::Error::ReentrantAbort))
                ));
                *self.4.lock().unwrap() = Some(scheduler);
            }
            if self.1 == "dispatch_guard" {
                use futures_util::FutureExt;
                let update = self
                    .3
                    .update(Some("different".into()), || {})
                    .now_or_never()
                    .unwrap();
                assert!(update.is_err(), "identity lease must cover first poll");
                let store = durable_store::Store::open(&self.2).unwrap();
                let task = pi_rs_durable::task_store::TaskStore::new(&store)
                    .load(context.operation_id)
                    .unwrap()
                    .unwrap();
                let pi_rs_durable::task::TaskOwner::Conversation(conversation) = task.task.owner()
                else {
                    panic!("not a conversation root")
                };
                let signal = crate::conversation_signal::Signal::new(
                    uuid::Uuid::new_v4(),
                    *conversation,
                    "during-first-poll".into(),
                    json!(1),
                );
                assert!(matches!(
                    crate::conversation_signal::Inbox::new(&store).send(&signal),
                    Err(crate::conversation_signal::Error::CallbackStartBusy)
                ));
            }
            if std::env::var("PI_TEST_CONV_CALLBACK_KILL").ok().as_deref() == Some("1") {
                let dir = std::path::PathBuf::from(
                    std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
                );
                let store = durable_store::Store::open(dir.join("db")).unwrap();
                let key = format!(
                    "durable/replay-safe-async/{}/conversation-park-cancel-claim",
                    context.operation_id
                );
                let claim = store.get(key.as_bytes()).unwrap().unwrap();
                std::fs::write(dir.join("ready"), claim.value).unwrap();
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| dir.path().to_path_buf())
        .join("db");
    let gate = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let after_calls = Arc::new(AtomicUsize::new(0));
    let callback_scheduler = Arc::new(std::sync::Mutex::new(None));
    let selected = matches!(
        case,
        "nested_claim_selected"
            | "selection_hook_fence"
            | "selection_hook_test_dispatch"
            | "selection_hook_kill"
            | "selection_hook_marker_conflict"
            | "selection_hook_multilevel"
            | "selection_hook_after_memo"
            | "selection_hook_memo_conflict"
            | "selection_hook_output_schema"
            | "selection_hook_snapshot_conflict"
            | "selection_hook_receipt_changed"
            | "selection_hook_unsafe_descendant"
            | "selection_hook_conversation_abort"
    );
    let mut extensions = crate::extension::Registry::default();
    if selected {
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "policy".into(),
                    hooks: vec![Arc::new(ClaimPassHook(
                        Some(after_calls.clone()),
                        matches!(
                            case,
                            "nested_claim_selected"
                                | "selection_hook_after_memo"
                                | "selection_hook_memo_conflict"
                                | "selection_hook_kill"
                        ),
                    ))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "policy-v1".into(),
                    vec!["hook-v1".into()],
                )
                .unwrap(),
            )
            .unwrap();
    }
    let snapshot = extensions.snapshot();
    let selection_names = vec!["policy".to_owned()];
    let mut registry = Registry::default();
    registry.register(
        "independent-maintenance",
        true,
        |_: &pi_rs_durable::task::Task| {
            Ok(pi_rs_durable::task_scheduler::Step::Succeed(json!("done")))
        },
    );
    let multilevel = case == "selection_hook_multilevel" || case.starts_with("nested_claim_");
    let grandchild = if multilevel && selected {
        Some(crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(
            &mut registry, Arc::new(AsyncTool { name: "grandchild", child: None, gate: Some(gate.clone()), early: false }),
            "grandchild", 1, vec![], &snapshot, &selection_names,
            ReplaySafeAuthorization::host_attests_replay_safe(),
            crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        ).unwrap())
    } else if multilevel {
        Some(
            register_conversation_replay_safe_durable_tool(
                &mut registry,
                Arc::new(AsyncTool {
                    name: "grandchild",
                    child: None,
                    gate: Some(gate.clone()),
                    early: false,
                }),
                "grandchild",
                1,
                vec![],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap(),
        )
    } else {
        None
    };
    let leaf_tool = Arc::new(AsyncTool {
        name: "leaf",
        child: multilevel.then_some("grandchild"),
        gate: if multilevel { None } else { Some(gate.clone()) },
        early: false,
    });
    let leaf_identity_tool: Arc<dyn Tool> = leaf_tool.clone();
    let leaf_identity = if case.starts_with("nested_claim_") {
        Some(
            pi_rs_durable::nested_identity::register(
                &leaf_identity_tool,
                Some("leaf-backend".into()),
            )
            .unwrap(),
        )
    } else {
        None
    };
    let leaf = if case == "nested_claim_selected" {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(
            &mut registry, leaf_tool, "leaf", 1, grandchild.clone().into_iter().collect(), &snapshot, &selection_names,
            "cancel-v1", Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), leaf_identity.clone().unwrap(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    } else if case == "nested_claim_success" || case == "nested_claim_success_late" {
        pi_rs_durable::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(
            &mut registry, leaf_tool, "leaf", 1, grandchild.clone().into_iter().collect(), "cancel-v1",
            Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), leaf_identity.clone().unwrap(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    } else if case.starts_with("nested_claim_") {
        register_conversation_unsafe_durable_tool_with_park_cancel(
            &mut registry, leaf_tool, "leaf", 1, grandchild.clone().into_iter().collect(), "cancel-v1",
            Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), leaf_identity.clone().unwrap(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    } else if selected {
        crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(
            &mut registry, leaf_tool, "leaf", 1, grandchild.clone().into_iter().collect(), &snapshot, &selection_names,
            ReplaySafeAuthorization::host_attests_replay_safe(),
            crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    } else { register_conversation_replay_safe_durable_tool(
        &mut registry, leaf_tool,
        "leaf",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    ) }
    .unwrap();
    let root_tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity_tool: Arc<dyn Tool> = root_tool.clone();
    let identity_handle =
        pi_rs_durable::nested_identity::register(&identity_tool, Some("backend-v1".into()))
            .unwrap();
    let root = if selected && case != "selection_hook_fence" {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(
            &mut registry, root_tool, "root", 1, vec![leaf.clone()], &snapshot, &selection_names,
            "cancel-v1", Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity_handle.clone(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    } else if selected {
        crate::async_task_tool::register_selected_unsafe_cancel_candidate(
            &mut registry, root_tool, "root", 1, vec![leaf.clone()], &snapshot, &selection_names,
            "cancel-v1", Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity_handle.clone(), callback_scheduler.clone())),
            false,
        )
    } else if case.starts_with("dispatch_") {
        pi_rs_durable::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(
            &mut registry, root_tool, "root", 1, vec![leaf.clone()], "cancel-v1",
            Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity_handle.clone(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    } else {
        register_conversation_unsafe_durable_tool_with_park_cancel(
            &mut registry, root_tool, "root", 1, vec![leaf.clone()], "cancel-v1",
            Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity_handle.clone(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    }.unwrap();
    let mut registered_tools = vec![root.register(), leaf.register()];
    if let Some(grandchild) = &grandchild {
        registered_tools.push(grandchild.register());
    }
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: registered_tools,
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let recovery_options = options.clone();
    let recovery_registry = registry.clone();
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    if selected {
        scheduler
            .select_extensions(conversation, selection_names)
            .unwrap();
    }
    if case == "selection_hook_output_schema" {
        scheduler
            .set_tool_output_schema(conversation, "root".into(), Some(json!({"type":"object"})))
            .unwrap();
    }
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
    if case.starts_with("nested_claim_") {
        let child_id = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(child) = tasks.list().unwrap().into_iter().find(|item| {
                    item.task.owner() == &pi_rs_durable::task::TaskOwner::Task(root_id)
                        && matches!(item.task.state(), TaskState::Waiting(_))
                }) {
                    break child.task.id();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        crate::task_scheduler::enable_test_nested_claim(child_id);
        if case == "nested_claim_callback"
            || case == "nested_claim_kill"
            || case == "nested_claim_late_kill"
            || case == "nested_claim_selected"
            || case == "nested_claim_success"
            || case == "nested_claim_success_late"
        {
            crate::task_scheduler::enable_test_nested_callback(child_id);
        }
        if case == "nested_claim_success" || case == "nested_claim_success_late" {
            crate::task_scheduler::enable_test_nested_success(child_id);
        }
        if case == "nested_claim_success_late" {
            let probe_path = path.clone();
            crate::task_scheduler::on_conversation_cancel_settlement(child_id, move || {
                let probe_store = durable_store::Store::open(&probe_path).unwrap();
                let prefix = format!("durable/unsafe-park-async/{child_id}/");
                let (mut key, entry) = probe_store
                    .list(prefix.as_bytes())
                    .unwrap()
                    .into_iter()
                    .find(|(key, _)| key.ends_with(b"/intent"))
                    .unwrap();
                let intent: pi_rs_durable::task::Task =
                    serde_json::from_slice(&entry.value).unwrap();
                let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                    task_id: child_id,
                    attempt_id: intent.attempt_id().unwrap(),
                    outcome: Outcome::Pending {
                        reference: "late-after-cancel".into(),
                    },
                };
                key.extend_from_slice(b"/receipt");
                probe_store
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Missing,
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: serde_json::to_vec(&receipt).unwrap(),
                        }],
                    )
                    .unwrap();
            });
        }
        if case == "nested_claim_signal" || case == "nested_claim_late" {
            let probe_path = path.clone();
            let signal_race = case == "nested_claim_signal";
            crate::task_scheduler::on_conversation_claim_after_commit(child_id, move || {
                let probe_store = durable_store::Store::open(&probe_path).unwrap();
                if signal_race {
                    let key = crate::task_store::signal_revision_key(root_id);
                    let old = probe_store.get(&key).unwrap();
                    probe_store
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: old
                                    .as_ref()
                                    .map_or(durable_store::Expected::Missing, |entry| {
                                        durable_store::Expected::Version(entry.version)
                                    }),
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: vec![1],
                            }],
                        )
                        .unwrap();
                } else {
                    let prefix = format!("durable/unsafe-park-async/{child_id}/");
                    let (mut key, entry) = probe_store
                        .list(prefix.as_bytes())
                        .unwrap()
                        .into_iter()
                        .find(|(key, _)| key.ends_with(b"/intent"))
                        .unwrap();
                    let intent: pi_rs_durable::task::Task =
                        serde_json::from_slice(&entry.value).unwrap();
                    let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                        task_id: child_id,
                        attempt_id: intent.attempt_id().unwrap(),
                        outcome: Outcome::Pending {
                            reference: "late".into(),
                        },
                    };
                    key.extend_from_slice(b"/receipt");
                    probe_store
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Missing,
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: serde_json::to_vec(&receipt).unwrap(),
                            }],
                        )
                        .unwrap();
                }
            });
        }
        scheduler.abort_task(child_id).unwrap();
        let claim_key =
            format!("durable/replay-safe-async/{child_id}/conversation-park-cancel-claim");
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.get(claim_key.as_bytes()).unwrap().is_none() {
                let task = tasks.load(child_id).unwrap().unwrap();
                assert!(
                    !task.task.is_terminal(),
                    "nested claim failed: {:?}",
                    task.task.state()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            store
                .get(format!("durable/task-callback-claim-ancestry/{child_id}").as_bytes())
                .unwrap()
                .is_some()
        );
        if case == "nested_claim_signal" || case == "nested_claim_late" {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !tasks.load(child_id).unwrap().unwrap().task.is_terminal() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                store
                    .get(format!("durable/task-callback-start/{child_id}").as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .get(
                        format!(
                            "durable/replay-safe-async/{child_id}/conversation-park-cancel-receipt"
                        )
                        .as_bytes()
                    )
                    .unwrap()
                    .is_none()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let _ = driver.shutdown_with_pending().unwrap();
            return;
        }
        let receipt_key =
            format!("durable/replay-safe-async/{child_id}/conversation-park-cancel-receipt");
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.get(receipt_key.as_bytes()).unwrap().is_none() {
                let task = tasks.load(child_id).unwrap().unwrap();
                assert!(
                    !task.task.is_terminal(),
                    "nested receipt failed: {:?}",
                    task.task.state()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            store
                .get(format!("durable/task-callback-start/{child_id}").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(format!("durable/task-callback-start/{root_id}").as_bytes())
                .unwrap()
                .is_none()
        );
        if case == "nested_claim_success_late" {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !tasks.load(child_id).unwrap().unwrap().task.is_terminal() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                matches!(tasks.load(child_id).unwrap().unwrap().task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
            );
            let key = format!("durable/replay-safe-async/{child_id}/nested-cancel-terminal-v1");
            assert!(store.get(key.as_bytes()).unwrap().is_none());
        }
        if case == "nested_claim_success" {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !tasks.load(child_id).unwrap().unwrap().task.is_terminal() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                tasks.load(child_id).unwrap().unwrap().task.state(),
                &TaskState::Cancelled
            );
            let evidence_key =
                format!("durable/replay-safe-async/{child_id}/nested-cancel-terminal-v1");
            let evidence = store.get(evidence_key.as_bytes()).unwrap().unwrap();
            let terminal = tasks.load(child_id).unwrap().unwrap();
            assert_eq!(
                store
                    .commit_for_version(evidence_key.as_bytes(), evidence.version)
                    .unwrap(),
                store
                    .commit_for_version(&crate::task_store::key(child_id), terminal.version)
                    .unwrap()
            );
        }
        if case == "nested_claim_selected" {
            let receipt = store.get(receipt_key.as_bytes()).unwrap().unwrap();
            let value: Value = serde_json::from_slice(&receipt.value).unwrap();
            assert_eq!(value["version"], 2);
            assert!(value.get("after_result").is_some());
            let memo = store
                .get(format!("runtime/extension-memo/{child_id}/nested-abort").as_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&memo.value).unwrap(),
                json!("saved")
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(
                case == "nested_claim_callback"
                    || case == "nested_claim_selected"
                    || case == "nested_claim_success"
                    || case == "nested_claim_success_late"
            )
        );
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    if case == "selection_hook_unsafe_descendant" {
        let child_id = uuid::Uuid::new_v4();
        let attempt = uuid::Uuid::new_v4();
        let mut child = pi_rs_durable::task::Task::new(
            child_id,
            pi_rs_durable::task::TaskOwner::Task(root_id),
            pi_rs_durable::task::Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered(
            "pi-rs.unsafe-park-async.v1.hook.test-uncertain".into(),
            false,
        );
        child.start_attempt(attempt).unwrap();
        let intent = child.clone();
        child.fail("external result unknown".into()).unwrap();
        let child_key = crate::task_store::key(child_id);
        let intent_key =
            format!("durable/unsafe-park-async/{child_id}/{attempt}/intent").into_bytes();
        store
            .commit(
                &[
                    durable_store::Check {
                        key: child_key.clone(),
                        expected: durable_store::Expected::Missing,
                    },
                    durable_store::Check {
                        key: intent_key.clone(),
                        expected: durable_store::Expected::Missing,
                    },
                ],
                &[
                    durable_store::Mutation::Put {
                        key: child_key,
                        value: serde_json::to_vec(&child).unwrap(),
                    },
                    durable_store::Mutation::Put {
                        key: intent_key,
                        value: serde_json::to_vec(&intent).unwrap(),
                    },
                ],
            )
            .unwrap();
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            store
                .get(
                    format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim")
                        .as_bytes()
                )
                .unwrap()
                .is_none()
        );
        assert!(
            matches!(tasks.load(root_id).unwrap().unwrap().task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    if case == "selection_hook_output_schema" {
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
        assert!(store.get(key.as_bytes()).unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(after_calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    if matches!(
        case,
        "selection_hook_test_dispatch"
            | "selection_hook_kill"
            | "selection_hook_marker_conflict"
            | "selection_hook_multilevel"
            | "selection_hook_after_memo"
            | "selection_hook_memo_conflict"
            | "selection_hook_snapshot_conflict"
            | "selection_hook_receipt_changed"
            | "selection_hook_conversation_abort"
    ) {
        if multilevel {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let all = tasks.list().unwrap();
                    let waiting_child = all.iter().any(|record| {
                        record.task.owner() == &pi_rs_durable::task::TaskOwner::Task(root_id)
                            && matches!(record.task.state(), TaskState::Waiting(_))
                    });
                    if waiting_child && gate.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("nested child did not park");
        }
        let worker = scheduler.subagent_harness().worker();
        let observed = Arc::new(AtomicBool::new(false));
        let observed_hook = observed.clone();
        let kill_before = case == "selection_hook_kill"
            && std::env::var("PI_SELECTED_CANCEL_CUT").ok().as_deref() == Some("before");
        let before_path = path.clone();
        crate::task_scheduler::on_conversation_start_result(root_id, move || {
            assert!(matches!(
                worker.install_extension(crate::extension::Extension {
                    name: "policy".into(),
                    ..Default::default()
                }),
                Err(crate::runtime::Error::TaskPublicationBusy)
            ));
            worker
                .install_extension(crate::extension::Extension {
                    name: "unrelated".into(),
                    ..Default::default()
                })
                .unwrap();
            observed_hook.store(true, Ordering::SeqCst);
            if kill_before {
                let store = durable_store::Store::open(&before_path).unwrap();
                let key =
                    format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
                let claim = store.get(key.as_bytes()).unwrap().unwrap();
                std::fs::write(before_path.parent().unwrap().join("ready"), claim.value).unwrap();
                loop {
                    std::thread::park();
                }
            }
        });
        if matches!(
            case,
            "selection_hook_marker_conflict"
                | "selection_hook_snapshot_conflict"
                | "selection_hook_memo_conflict"
        ) {
            let path = path.clone();
            let snapshot_conflict = matches!(
                case,
                "selection_hook_snapshot_conflict" | "selection_hook_memo_conflict"
            );
            crate::task_scheduler::on_conversation_receipt_before_commit(root_id, move || {
                let writer = durable_store::Store::open(path).unwrap();
                let key = if snapshot_conflict {
                    format!("runtime/conversation/{conversation}").into_bytes()
                } else {
                    format!("durable/task-callback-start/{root_id}").into_bytes()
                };
                let entry = writer.get(&key).unwrap().unwrap();
                writer
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: entry.value,
                        }],
                    )
                    .unwrap();
            });
        }
        if case == "selection_hook_kill"
            && std::env::var("PI_SELECTED_CANCEL_CUT").ok().as_deref() == Some("after_receipt")
        {
            let path = path.clone();
            crate::task_scheduler::on_conversation_receipt_saved(root_id, move || {
                let store = durable_store::Store::open(&path).unwrap();
                let key =
                    format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
                let claim = store.get(key.as_bytes()).unwrap().unwrap();
                std::fs::write(path.parent().unwrap().join("ready"), claim.value).unwrap();
                loop {
                    std::thread::park();
                }
            });
        }
        let changed = Arc::new(AtomicBool::new(false));
        if case == "selection_hook_receipt_changed" {
            let path = path.clone();
            let changed_hook = changed.clone();
            crate::task_scheduler::on_conversation_cancel_after_settlement(root_id, move || {
                let writer = durable_store::Store::open(path).unwrap();
                let key =
                    format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt")
                        .into_bytes();
                let entry = writer.get(&key).unwrap().unwrap();
                writer
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: entry.value,
                        }],
                    )
                    .unwrap();
                changed_hook.store(true, Ordering::SeqCst);
            });
        }
        if case == "selection_hook_conversation_abort" {
            scheduler.abort(conversation).unwrap();
        } else {
            scheduler.abort_task(root_id).unwrap();
        }
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        if matches!(
            case,
            "selection_hook_marker_conflict"
                | "selection_hook_snapshot_conflict"
                | "selection_hook_memo_conflict"
        ) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let receipt = store.get(key.as_bytes()).unwrap();
            let receipt: crate::task_scheduler::ConversationCancelReceipt =
                serde_json::from_slice(&receipt.unwrap().value).unwrap();
            assert_eq!(receipt.version, 1);
            if case == "selection_hook_memo_conflict" {
                let memo_key = format!("runtime/extension-memo/{root_id}/abort-memo");
                assert!(store.get(memo_key.as_bytes()).unwrap().is_none());
            }
            assert_eq!(receipt.result, "callback returned");
            assert!(receipt.after_result.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(after_calls.load(Ordering::SeqCst), 1);
            assert!(
                matches!(tasks.load(root_id).unwrap().unwrap().task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
            );
            let _ = driver.shutdown_with_pending().unwrap();
            return;
        }
        if case == "selection_hook_receipt_changed" {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !changed.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let worker_errors = scheduler
                .subagent_harness()
                .owner
                .conversation_controls()
                .worker_errors();
            tokio::time::timeout(Duration::from_secs(10), async {
                while worker_errors.lock().unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("receipt replacement was not rejected");
            assert!(
                worker_errors
                    .lock()
                    .unwrap()
                    .values()
                    .any(|error| error.contains("cancel receipt version changed"))
            );
            assert!(!scheduler.conversation(conversation).unwrap().messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("after cancel"))));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let _ = driver.shutdown_with_pending().unwrap();
            return;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.get(key.as_bytes()).unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(after_calls.load(Ordering::SeqCst), 1);
        if case == "selection_hook_after_memo" {
            let memo_key = format!("runtime/extension-memo/{root_id}/abort-memo");
            let memo = store.get(memo_key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&memo.value).unwrap(),
                json!("saved")
            );
        }
        let entry = store.get(key.as_bytes()).unwrap().unwrap();
        let receipt: crate::task_scheduler::ConversationCancelReceipt =
            serde_json::from_slice(&entry.value).unwrap();
        assert_eq!(receipt.version, 2);
        assert_eq!(
            receipt.after_result.as_ref().unwrap()["task_content_v1"]["output"][0]["text"],
            "after cancel"
        );
        crate::async_task_tool::validate_aborted_after_result(
            receipt.after_result.as_ref().unwrap(),
        )
        .unwrap();
        let published = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let conversation = scheduler.conversation(conversation).unwrap();
                if conversation.messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("after cancel"))) {
                    assert!(conversation.tool_results[0].is_error);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if published.is_err() {
            let errors = scheduler
                .subagent_harness()
                .owner
                .conversation_controls()
                .worker_errors();
            panic!(
                "after result did not reach conversation: errors={:?} conversation={:?}",
                errors.lock().unwrap(),
                scheduler.conversation(conversation).unwrap()
            );
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let terminal = tasks.load(root_id).unwrap().unwrap();
        assert_eq!(terminal.task.state(), &TaskState::Cancelled);
        let marker_key = crate::task_scheduler::conversation_cancel_terminal_key(root_id);
        let marker = store.get(&marker_key).unwrap().unwrap();
        let marker: crate::task_scheduler::ConversationCancelTerminalMarker =
            serde_json::from_slice(&marker.value).unwrap();
        assert_eq!(marker.claim_id, receipt.claim.claim_id);
        assert_eq!(marker.attempt, receipt.claim.attempt);
        assert_eq!(marker.terminal_task, terminal.task);
        assert_eq!(after_calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    if case == "dispatch_signal_before" || case == "dispatch_abort_before" {
        let finished = Arc::new(AtomicBool::new(false));
        let finished_hook = finished.clone();
        crate::task_scheduler::on_conversation_start_result(root_id, move || {
            finished_hook.store(true, Ordering::SeqCst)
        });
        let after_path = path.clone();
        let abort_first = case == "dispatch_abort_before";
        crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
            let writer = durable_store::Store::open(after_path).unwrap();
            if abort_first {
                let key = format!("runtime/conversation/{conversation}").into_bytes();
                let entry = writer.get(&key).unwrap().unwrap();
                let mut value: Value = serde_json::from_slice(&entry.value).unwrap();
                value["abort_requested"] = json!(true);
                writer
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Put {
                            key,
                            value: serde_json::to_vec(&value).unwrap(),
                        }],
                    )
                    .unwrap();
            } else {
                let signal = crate::task_store::Signal::new(
                    uuid::Uuid::new_v4(),
                    root_id,
                    "before-first-poll".into(),
                    json!(1),
                );
                crate::task_store::TaskStore::new(&writer)
                    .send_signal(&signal)
                    .unwrap();
            }
        });
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !finished.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        assert!(store.get(key.as_bytes()).unwrap().is_none());
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        return;
    }
    if case == "dispatch_kill" {
        scheduler.abort_task(root_id).unwrap();
        std::future::pending::<()>().await;
        unreachable!();
    }
    if case == "dispatch_late_after_receipt" || case == "dispatch_late_during_settlement" {
        let writer_path = path.clone();
        let inject = move || {
            let writer = durable_store::Store::open(writer_path).unwrap();
            let prefix = format!("durable/unsafe-park-async/{root_id}/");
            let (mut key, entry) = writer
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent"))
                .unwrap();
            let intent: pi_rs_durable::task::Task = serde_json::from_slice(&entry.value).unwrap();
            let receipt = pi_rs_durable::async_task_tool::UnsafeDurableReceipt {
                task_id: root_id,
                attempt_id: intent.attempt_id().unwrap(),
                outcome: Outcome::Pending {
                    reference: "late-after-receipt".into(),
                },
            };
            key.extend_from_slice(b"/receipt");
            writer
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&receipt).unwrap(),
                    }],
                )
                .unwrap();
        };
        if case == "dispatch_late_during_settlement" {
            crate::task_scheduler::on_conversation_cancel_settlement(root_id, inject);
        } else {
            crate::task_scheduler::on_conversation_receipt_saved(root_id, inject);
        }
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let task = tasks.load(root_id).unwrap().unwrap();
        assert!(
            matches!(task.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        return;
    }
    if case == "dispatch_late" || case == "dispatch_panic" {
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        assert!(store.get(key.as_bytes()).unwrap().is_none());
        assert!(
            store
                .get(format!("runtime/conversation-callback-start/{conversation}").as_bytes())
                .unwrap()
                .is_none()
        );
        let unknown =
            format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-unknown");
        assert!(store.get(unknown.as_bytes()).unwrap().is_some());
        let evidence =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root_id).unwrap();
        if case == "dispatch_late" {
            assert!(evidence.iter().any(
                |r| matches!(&r.outcome, Outcome::Pending { reference } if reference == "late-job")
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        return;
    }
    if case == "dispatch_maintenance" {
        use pi_rs_durable::task::{Checkpoint, Task, TaskOwner};
        let parent_id = uuid::Uuid::new_v4();
        let mut parent = Task::new(
            parent_id,
            TaskOwner::Conversation(uuid::Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("independent-maintenance".into(), true);
        parent.start().unwrap();
        let child = Task::new(
            uuid::Uuid::new_v4(),
            TaskOwner::Task(parent_id),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("independent-maintenance".into(), true);
        crate::task_scheduler::set_maintenance_busy_for_test(parent_id, true);
        let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
        crate::task_driver::on_maintenance_reserved_for_test(parent_id, move || {
            reserved_tx.send(()).unwrap();
        });
        {
            tasks
                .save(&[
                    pi_rs_durable::task_store::Save::Create(&parent),
                    pi_rs_durable::task_store::Save::Create(&child),
                ])
                .unwrap();
            tasks
                .update(&tasks.load(parent_id).unwrap().unwrap(), |task| {
                    task.hold_failure("held".into())
                })
                .unwrap();
        }
        let trigger_owner = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let trigger = Task::new(
            uuid::Uuid::new_v4(),
            TaskOwner::Conversation(trigger_owner),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("independent-maintenance".into(), true);
        driver.submit(&trigger).unwrap();
        reserved_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("maintenance not reserved");
        let graph_changed = Arc::new(AtomicBool::new(false));
        let graph_changed_hook = graph_changed.clone();
        let graph_writer_path = path.clone();
        crate::task_scheduler::on_conversation_cancel_settlement(root_id, move || {
            let writer = durable_store::Store::open(graph_writer_path).unwrap();
            let task = Task::new(
                uuid::Uuid::new_v4(),
                TaskOwner::Conversation(uuid::Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap()
            .registered("independent-maintenance".into(), true);
            pi_rs_durable::task_store::TaskStore::new(&writer)
                .save(&[pi_rs_durable::task_store::Save::Create(&task)])
                .unwrap();
            graph_changed_hook.store(true, Ordering::SeqCst);
        });
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            graph_changed.load(Ordering::SeqCst),
            "settlement graph conflict was not exercised"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            tasks.load(parent_id).unwrap().unwrap().task.state(),
            TaskState::CompletingFailed(_)
        ));
        crate::task_scheduler::set_maintenance_busy_for_test(parent_id, false);
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        return;
    }
    if case == "dispatch_reentrant_abort" || case == "dispatch_spawn_abort" {
        scheduler.abort_task(root_id).unwrap();
        *callback_scheduler.lock().unwrap() = Some(scheduler);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        let scheduler = callback_scheduler.lock().unwrap().take().unwrap();
        scheduler.shutdown().await.unwrap();
        return;
    }
    if case == "dispatch_hold" || case == "dispatch_hold_cleanup" {
        let other = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let cleanup_ready = Arc::new(tokio::sync::Semaphore::new(0));
        if case == "dispatch_hold_cleanup" {
            crate::runtime::on_callback_cleanup_committed(conversation, cleanup_ready.clone());
            scheduler.abort(conversation).unwrap();
        } else {
            scheduler.abort_task(root_id).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if case == "dispatch_hold_cleanup" {
            tokio::time::timeout(Duration::from_secs(10), cleanup_ready.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
        let same_signal = crate::conversation_signal::Signal::new(
            uuid::Uuid::new_v4(),
            conversation,
            "blocked-by-callback".into(),
            json!(1),
        );
        assert!(matches!(
            scheduler.send_signal(&same_signal),
            Err(crate::runtime::Error::TaskPublicationBusy)
        ));
        let modes = crate::runtime::QueueModes::default();
        if case == "dispatch_hold" {
            assert!(matches!(
                scheduler.set_queue_modes(conversation, modes),
                Err(crate::runtime::Error::TaskPublicationBusy)
            ));
            assert!(matches!(
                scheduler.reset_with_blocks(
                    conversation,
                    None,
                    vec![vec![json!({"type":"text","text":"held"})]]
                ),
                Err(crate::runtime::Error::TaskPublicationBusy)
            ));
        }
        scheduler.set_queue_modes(other, modes).unwrap();
        let signal = crate::conversation_signal::Signal::new(
            uuid::Uuid::new_v4(),
            other,
            "other-graph".into(),
            json!(1),
        );
        scheduler.send_signal(&signal).unwrap();
        std::fs::write(path.with_extension("release"), b"release").unwrap();
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.get(key.as_bytes()).unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if case == "dispatch_hold" {
            scheduler.set_queue_modes(conversation, modes).unwrap();
            scheduler
                .reset_with_blocks(
                    conversation,
                    None,
                    vec![vec![json!({"type":"text","text":"released"})]],
                )
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        return;
    }
    if case == "dispatch_ok"
        || case == "dispatch_guard"
        || case == "dispatch_reentrant"
        || case == "dispatch_spawn_signal"
        || case == "dispatch_spawn_unscoped_cancel"
    {
        scheduler.abort_task(root_id).unwrap();
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store.get(key.as_bytes()).unwrap().is_some() {
                    break;
                }
                if tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                    panic!(
                        "callback terminated without receipt: {:?}",
                        tasks.load(root_id).unwrap().unwrap().task
                    );
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            store
                .get(format!("runtime/conversation-callback-start/{conversation}").as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(format!("durable/task-callback-start/{root_id}").as_bytes())
                .unwrap()
                .is_none()
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        return;
    }
    if case.starts_with("start_") {
        use std::sync::atomic::{AtomicBool, Ordering};
        let case_name = case.to_owned();
        let writer_path = path.clone();
        let hook_reached = Arc::new(AtomicBool::new(false));
        let reached = hook_reached.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_hook = finished.clone();
        crate::task_scheduler::on_conversation_start_result(root_id, move || {
            finished_hook.store(true, Ordering::SeqCst);
        });
        let identity = identity_handle.clone();
        let other = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        crate::task_scheduler::enable_test_conversation_start(root_id);
        if case == "start_retire" {
            crate::task_scheduler::enable_test_conversation_finish(root_id);
        }
        if case == "start_legacy" {
            let after_path = path.clone();
            let legacy_reached = hook_reached.clone();
            crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
                let writer = durable_store::Store::open(&after_path).unwrap();
                let key = format!(
                    "durable/replay-safe-async/{root_id}/conversation-park-cancel-signals-v1"
                )
                .into_bytes();
                let entry = writer.get(&key).unwrap().unwrap();
                writer
                    .commit(
                        &[durable_store::Check {
                            key: key.clone(),
                            expected: durable_store::Expected::Version(entry.version),
                        }],
                        &[durable_store::Mutation::Delete { key }],
                    )
                    .unwrap();
                legacy_reached.store(true, Ordering::SeqCst);
            });
        }
        if case == "start_signal_after_claim" || case == "start_task_signal_after_claim" {
            let after_path = path.clone();
            let after_case = case.to_owned();
            crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
                let writer = durable_store::Store::open(&after_path).unwrap();
                if after_case == "start_signal_after_claim" {
                    let signal = crate::conversation_signal::Signal::new(
                        uuid::Uuid::new_v4(),
                        conversation,
                        "after-claim".into(),
                        json!(1),
                    );
                    let result = crate::conversation_signal::Inbox::new(&writer).send(&signal);
                    assert!(
                        result.is_ok()
                            || matches!(result, Err(crate::conversation_signal::Error::Closed))
                    );
                } else {
                    let signal = crate::task_store::Signal::new(
                        uuid::Uuid::new_v4(),
                        root_id,
                        "after-claim".into(),
                        json!(1),
                    );
                    crate::task_store::TaskStore::new(&writer)
                        .send_signal(&signal)
                        .unwrap();
                }
            });
        }
        crate::task_scheduler::on_conversation_start_before_commit(root_id, move || {
            let writer = durable_store::Store::open(&writer_path).unwrap();
            match case_name.as_str() {
                "start_signal_after_claim" | "start_task_signal_after_claim" => (),
                "start_ok" | "start_retire" => {
                    use futures_util::FutureExt;
                    let result = identity
                        .update(Some("changed".into()), || {})
                        .now_or_never()
                        .expect("identity update must not wait");
                    assert!(result.is_err());
                }
                "start_owner" => {
                    let key = crate::task_store::key(root_id);
                    let entry = writer.get(&key).unwrap().unwrap();
                    let mut value: Value = serde_json::from_slice(&entry.value).unwrap();
                    value["owner"] = json!({"Conversation": uuid::Uuid::new_v4()});
                    writer
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Version(entry.version),
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: serde_json::to_vec(&value).unwrap(),
                            }],
                        )
                        .unwrap();
                }
                "start_abort" => {
                    let key = format!("runtime/conversation/{conversation}").into_bytes();
                    let entry = writer.get(&key).unwrap().unwrap();
                    let mut value: Value = serde_json::from_slice(&entry.value).unwrap();
                    value["abort_requested"] = json!(true);
                    writer
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Version(entry.version),
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: serde_json::to_vec(&value).unwrap(),
                            }],
                        )
                        .unwrap();
                }
                "start_signal" => {
                    let signal = crate::conversation_signal::Signal::new(
                        uuid::Uuid::new_v4(),
                        conversation,
                        "start-race".into(),
                        json!(1),
                    );
                    let result = crate::conversation_signal::Inbox::new(&writer).send(&signal);
                    assert!(
                        result.is_ok()
                            || matches!(result, Err(crate::conversation_signal::Error::Closed))
                    );
                }
                "start_task_signal" => {
                    let signal = crate::task_store::Signal::new(
                        uuid::Uuid::new_v4(),
                        root_id,
                        "start-race".into(),
                        json!(1),
                    );
                    crate::task_store::TaskStore::new(&writer)
                        .send_signal(&signal)
                        .unwrap();
                }
                "start_child" => {
                    let child = pi_rs_durable::task::Task::new(
                        uuid::Uuid::new_v4(),
                        pi_rs_durable::task::TaskOwner::Task(root_id),
                        pi_rs_durable::task::Checkpoint {
                            phase: "initial".into(),
                            data: json!({}),
                        },
                        3,
                    )
                    .unwrap()
                    .registered("foreign".into(), true);
                    let key = crate::task_store::key(child.id());
                    writer
                        .commit(
                            &[durable_store::Check {
                                key: key.clone(),
                                expected: durable_store::Expected::Missing,
                            }],
                            &[durable_store::Mutation::Put {
                                key,
                                value: serde_json::to_vec(&child).unwrap(),
                            }],
                        )
                        .unwrap();
                }
                _ => panic!("unknown start case"),
            }
            reached.store(true, Ordering::SeqCst);
        });
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !hook_reached.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let conversation_marker_key = format!("runtime/conversation-callback-start/{conversation}");
        let task_marker_key = format!("durable/task-callback-start/{root_id}");
        if case == "start_ok" {
            let marker = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(entry) = store.get(conversation_marker_key.as_bytes()).unwrap() {
                        break entry.value;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                store
                    .get(task_marker_key.as_bytes())
                    .unwrap()
                    .unwrap()
                    .value,
                marker
            );
            if std::env::var_os("PI_TEST_START_MARKER_KILL").is_some() {
                let directory = std::path::PathBuf::from(
                    std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
                );
                let temporary = directory.join("start-ready.tmp");
                std::fs::write(
                    &temporary,
                    serde_json::to_vec(&json!({"conversation": conversation, "task": root_id}))
                        .unwrap(),
                )
                .unwrap();
                std::fs::rename(temporary, directory.join("start-ready")).unwrap();
                std::future::pending::<()>().await;
                unreachable!();
            }
            let signal = crate::conversation_signal::Signal::new(
                uuid::Uuid::new_v4(),
                conversation,
                "blocked".into(),
                json!(1),
            );
            assert!(matches!(
                crate::conversation_signal::Inbox::new(&store).send(&signal),
                Err(crate::conversation_signal::Error::CallbackStartBusy)
            ));
            let task_signal = crate::task_store::Signal::new(
                uuid::Uuid::new_v4(),
                root_id,
                "blocked".into(),
                json!(1),
            );
            assert!(matches!(
                tasks.send_signal(&task_signal),
                Err(crate::task_store::Error::CallbackStartBusy)
            ));
            let other_signal = crate::conversation_signal::Signal::new(
                uuid::Uuid::new_v4(),
                other,
                "allowed".into(),
                json!(1),
            );
            scheduler.send_signal(&other_signal).unwrap();
            let conv_finish = crate::conversation_signal::prepare_callback_finish_signal_fence(
                &store,
                conversation,
                &marker,
            )
            .unwrap();
            let task_finish =
                crate::task_store::prepare_callback_finish_task_fence(&store, root_id, &marker)
                    .unwrap();
            let mut checks = conv_finish.checks;
            checks.extend(task_finish.checks);
            let mut mutations = conv_finish.mutations;
            mutations.extend(task_finish.mutations);
            store.commit(&checks, &mutations).unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !finished.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert!(
                store
                    .get(conversation_marker_key.as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(store.get(task_marker_key.as_bytes()).unwrap().is_none());
        }
        if case == "start_retire" {
            let key =
                format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
            assert!(store.get(key.as_bytes()).unwrap().is_some());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    if case == "kill_after_claim" {
        let directory = std::path::PathBuf::from(
            std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
        );
        let claim_path = path.clone();
        crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
            let store = durable_store::Store::open(claim_path).unwrap();
            let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
            let claim = store.get(key.as_bytes()).unwrap().expect("claim committed");
            std::fs::write(directory.join("ready"), claim.value).unwrap();
            loop {
                std::thread::park();
            }
        });
        scheduler.abort_task(root_id).unwrap();
        std::future::pending::<()>().await;
        unreachable!();
    }
    if case == "late_after_claim" {
        crate::task_scheduler::enable_test_conversation_receipt(root_id);
        let mutation_path = path.clone();
        crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
            let writer = durable_store::Store::open(mutation_path).unwrap();
            let prefix = format!("durable/unsafe-park-async/{root_id}/");
            let (mut key, entry) = writer
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent"))
                .expect("park intent");
            let intent: pi_rs_durable::task::Task = serde_json::from_slice(&entry.value).unwrap();
            let attempt = serde_json::to_value(intent).unwrap()["attempt_id"]
                .as_str()
                .unwrap()
                .parse::<uuid::Uuid>()
                .unwrap();
            key.extend_from_slice(b"/receipt");
            let late = pi_rs_durable::async_task_tool::UnsafeDurableReceipt {
                task_id: root_id,
                attempt_id: attempt,
                outcome: Outcome::Pending {
                    reference: "remote-job".into(),
                },
            };
            writer
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&late).unwrap(),
                    }],
                )
                .unwrap();
        });
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let claim_key =
            format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
        let receipt_key =
            format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        assert!(store.get(claim_key.as_bytes()).unwrap().is_some());
        assert!(store.get(receipt_key.as_bytes()).unwrap().is_none());
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root_id).unwrap();
        assert!(receipts.iter().any(
            |r| matches!(&r.outcome, Outcome::Pending { reference } if reference == "remote-job")
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    if case.starts_with("save_receipt") {
        crate::task_scheduler::enable_test_conversation_receipt(root_id);
        scheduler.abort_task(root_id).unwrap();
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-receipt");
        let entry = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(entry) = store.get(key.as_bytes()).unwrap() {
                    break entry;
                }
                if tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                    panic!("receipt not stored");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let receipt: crate::task_scheduler::ConversationCancelReceipt =
            serde_json::from_slice(&entry.value).unwrap();
        assert_eq!(receipt.claim.task, root_id);
        assert_eq!(receipt.result, "test evidence only");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if let Some(directory) = std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR") {
            std::fs::write(
                std::path::PathBuf::from(directory).join("ready"),
                serde_json::to_vec(&receipt).unwrap(),
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        let _ = driver.shutdown_with_pending().unwrap();
        drop(store);
        let _ = scheduler.shutdown().await;
        let h = pi_rs_durable::runtime::Harness::open_paused(&path, recovery_options).unwrap();
        let read = h.read_stopped_conversation_cancel_receipt(
            root_id,
            receipt.claim.attempt,
            receipt.claim.claim_id,
            &recovery_registry,
        );
        assert_eq!(read.unwrap(), Some(receipt.clone()));
        assert!(
            h.read_stopped_conversation_cancel_receipt(
                root_id,
                uuid::Uuid::new_v4(),
                receipt.claim.claim_id,
                &recovery_registry
            )
            .is_err()
        );
        assert!(
            h.read_stopped_conversation_cancel_receipt(
                root_id,
                receipt.claim.attempt,
                uuid::Uuid::new_v4(),
                &recovery_registry
            )
            .is_err()
        );
        if case == "save_receipt_owner" {
            let owner_writer = durable_store::Store::open(&path).unwrap();
            let task_key = crate::task_store::key(root_id);
            let original = owner_writer.get(&task_key).unwrap().unwrap();
            let mut altered: Value = serde_json::from_slice(&original.value).unwrap();
            altered["owner"] = json!({"Conversation": uuid::Uuid::new_v4()});
            owner_writer
                .commit(
                    &[durable_store::Check {
                        key: task_key.clone(),
                        expected: durable_store::Expected::Version(original.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key: task_key.clone(),
                        value: serde_json::to_vec(&altered).unwrap(),
                    }],
                )
                .unwrap();
            assert!(
                h.read_stopped_conversation_cancel_receipt(
                    root_id,
                    receipt.claim.attempt,
                    receipt.claim.claim_id,
                    &recovery_registry
                )
                .is_err()
            );
            return;
        }
        identity_handle
            .update(Some("backend-v2".into()), || {})
            .await
            .unwrap();
        assert!(
            h.read_stopped_conversation_cancel_receipt(
                root_id,
                receipt.claim.attempt,
                receipt.claim.claim_id,
                &recovery_registry
            )
            .is_err()
        );
        identity_handle
            .update(Some("backend-v1".into()), || {})
            .await
            .unwrap();
        assert_eq!(
            h.read_stopped_conversation_cancel_receipt(
                root_id,
                receipt.claim.attempt,
                receipt.claim.claim_id,
                &recovery_registry
            )
            .unwrap(),
            Some(receipt.clone())
        );
        if case == "save_receipt_marker" {
            let writer = durable_store::Store::open(&path).unwrap();
            let key = crate::task_scheduler::conversation_cancel_terminal_key(root_id);
            let entry = writer.get(&key).unwrap().unwrap();
            let mut marker: Value = serde_json::from_slice(&entry.value).unwrap();
            marker["attempt"] = json!(uuid::Uuid::new_v4());
            writer
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&marker).unwrap(),
                    }],
                )
                .unwrap();
            assert!(
                h.read_stopped_conversation_cancel_receipt(
                    root_id,
                    receipt.claim.attempt,
                    receipt.claim.claim_id,
                    &recovery_registry
                )
                .is_err()
            );
            return;
        }
        if case != "save_receipt_late" {
            return;
        }
        let mutation_path = path.clone();
        crate::runtime::on_stopped_receipt_read_before_commit(root_id, move || {
            let writer = durable_store::Store::open(&mutation_path).unwrap();
            let prefix = format!("durable/unsafe-park-async/{root_id}/");
            let (intent_key, intent_entry) = writer
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent"))
                .expect("park intent");
            let intent: pi_rs_durable::task::Task =
                serde_json::from_slice(&intent_entry.value).unwrap();
            let attempt = serde_json::to_value(&intent).unwrap()["attempt_id"]
                .as_str()
                .unwrap()
                .parse::<uuid::Uuid>()
                .unwrap();
            let mut key = intent_key;
            key.extend_from_slice(b"/receipt");
            let late = pi_rs_durable::async_task_tool::UnsafeDurableReceipt {
                task_id: root_id,
                attempt_id: attempt,
                outcome: Outcome::Pending {
                    reference: "remote-job".into(),
                },
            };
            writer
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&late).unwrap(),
                    }],
                )
                .unwrap();
        });
        assert!(
            h.read_stopped_conversation_cancel_receipt(
                root_id,
                receipt.claim.attempt,
                receipt.claim.claim_id,
                &recovery_registry
            )
            .is_err()
        );
        return;
    }
    if case == "mismatch" {
        identity_handle
            .update(Some("backend-v2".into()), || {})
            .await
            .unwrap();
        scheduler.abort_task(root_id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
        assert!(store.get(key.as_bytes()).unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        return;
    }
    let is_owner = case == "owner";
    let is_reentrant = case == "reentrant";
    let is_hook_selection_fence = case == "selection_hook_fence";
    let is_selection_fence = case == "selection_fence" || is_hook_selection_fence;
    let selection_worker = is_selection_fence.then(|| scheduler.subagent_harness().worker());
    let selection_updated = Arc::new(AtomicBool::new(false));
    let selection_updated_hook = selection_updated.clone();
    let is_late_receipt = case == "late_receipt";
    let reentrant_handle = identity_handle.clone();
    let mutation_path = path.clone();
    let reached = Arc::new(AtomicBool::new(false));
    let reached_hook = reached.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let finished_hook = finished.clone();
    crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
        finished_hook.store(true, Ordering::SeqCst)
    });
    let case = case.to_owned();
    crate::task_scheduler::on_conversation_claim_before_commit(root_id, move || {
        if let Some(worker) = selection_worker {
            let gate = worker.checkpointable_extension_gate();
            assert!(matches!(
                gate.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            if is_hook_selection_fence {
                assert!(matches!(
                    worker.install_extension(crate::extension::Extension {
                        name: "policy".into(),
                        ..Default::default()
                    }),
                    Err(crate::runtime::Error::TaskPublicationBusy)
                ));
                selection_updated_hook.store(true, Ordering::SeqCst);
                reached_hook.store(true, Ordering::SeqCst);
                return;
            }
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let updated = selection_updated_hook.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                worker
                    .install_extension(crate::extension::Extension {
                        name: "claim-race".into(),
                        ..Default::default()
                    })
                    .unwrap();
                updated.store(true, Ordering::SeqCst);
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(!selection_updated_hook.load(Ordering::SeqCst));
            reached_hook.store(true, Ordering::SeqCst);
            return;
        }
        if case == "reentrant" {
            use futures_util::FutureExt;
            let applied = Arc::new(AtomicBool::new(false));
            let applied_update = applied.clone();
            let result = reentrant_handle
                .update(Some("backend-v2".into()), move || {
                    applied_update.store(true, Ordering::SeqCst)
                })
                .now_or_never()
                .expect("identity update must not block");
            assert!(result.is_err());
            assert!(!applied.load(Ordering::SeqCst));
            reached_hook.store(true, Ordering::SeqCst);
            return;
        }
        let writer = durable_store::Store::open(&mutation_path).unwrap();
        if case == "late_receipt" {
            let prefix = format!("durable/unsafe-park-async/{root_id}/");
            let (intent_key, intent_entry) = writer
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent"))
                .expect("park intent");
            let intent: pi_rs_durable::task::Task =
                serde_json::from_slice(&intent_entry.value).unwrap();
            let attempt = serde_json::to_value(&intent).unwrap()["attempt_id"]
                .as_str()
                .unwrap()
                .parse::<uuid::Uuid>()
                .unwrap();
            let mut key = intent_key;
            key.extend_from_slice(b"/receipt");
            let receipt = pi_rs_durable::async_task_tool::UnsafeDurableReceipt {
                task_id: root_id,
                attempt_id: attempt,
                outcome: Outcome::Pending {
                    reference: "remote-job".into(),
                },
            };
            writer
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&receipt).unwrap(),
                    }],
                )
                .unwrap();
            reached_hook.store(true, Ordering::SeqCst);
            return;
        }
        if case == "conversation_signal" {
            let signal = crate::conversation_signal::Signal::new(
                uuid::Uuid::new_v4(),
                conversation,
                "claim-race".into(),
                json!(1),
            );
            let result = crate::conversation_signal::Inbox::new(&writer).send(&signal);
            assert!(
                result.is_ok() || matches!(result, Err(crate::conversation_signal::Error::Closed))
            );
            assert!(
                writer
                    .get(&crate::conversation_signal::revision(conversation))
                    .unwrap()
                    .is_some()
            );
            reached_hook.store(true, Ordering::SeqCst);
            return;
        }
        if case == "task_signal" {
            let signal = crate::task_store::Signal::new(
                uuid::Uuid::new_v4(),
                root_id,
                "claim-race".into(),
                json!(1),
            );
            crate::task_store::TaskStore::new(&writer)
                .send_signal(&signal)
                .unwrap();
            reached_hook.store(true, Ordering::SeqCst);
            return;
        }
        let key = if case == "owner" {
            crate::task_store::key(root_id)
        } else {
            format!("runtime/conversation/{conversation}").into_bytes()
        };
        let entry = writer.get(&key).unwrap().unwrap();
        let mut value: Value = serde_json::from_slice(&entry.value).unwrap();
        match case.as_str() {
            "owner" => {
                value["owner"] = json!({"Conversation": uuid::Uuid::new_v4()});
            }
            "identity" => {
                value["tool_attempt"]["attempt_id"] = json!(uuid::Uuid::new_v4());
            }
            "abort" => {
                value["abort_requested"] = json!(true);
            }
            _ => panic!("unknown case"),
        }
        writer
            .commit(
                &[durable_store::Check {
                    key: key.clone(),
                    expected: durable_store::Expected::Version(entry.version),
                }],
                &[durable_store::Mutation::Put {
                    key,
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        reached_hook.store(true, Ordering::SeqCst);
    });
    scheduler.abort_task(root_id).unwrap();
    let key = format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !reached.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !finished.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap().is_some(),
        is_reentrant || is_selection_fence
    );
    if is_selection_fence {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !selection_updated.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    if is_late_receipt {
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root_id).unwrap();
        assert!(receipts.iter().any(|receipt| matches!(&receipt.outcome, Outcome::Pending { reference } if reference == "remote-job")));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // Bound shutdown separately: it is synchronous and must not block the
    // current-thread runtime, which also drives the timeout.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = shutdown_tx.send(driver.shutdown_with_pending());
    });
    let report = tokio::time::timeout(Duration::from_secs(10), shutdown_rx)
        .await
        .expect("driver shutdown timed out")
        .expect("driver shutdown thread exited")
        .unwrap();
    if is_owner {
        assert!(
            report.failure().is_some(),
            "stale ownership must be rejected"
        );
        assert!(
            report
                .pending()
                .iter()
                .any(|pending| pending.task_id() == root_id)
        );
        assert_eq!(
            tasks.load(root_id).unwrap().unwrap().task.state(),
            &TaskState::Running
        );
    }
}

#[tokio::test]
async fn conversation_start_marker_kill_worker() {
    if std::env::var_os("PI_TEST_START_MARKER_KILL").is_some() {
        exercise_conversation_park_claim("start_ok").await;
    }
}

#[tokio::test]
async fn conversation_start_markers_survive_process_kill() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("conversation_claim_boundary_tests::conversation_start_marker_kill_worker")
        .arg("--nocapture")
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .env("PI_TEST_START_MARKER_KILL", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let ready = dir.path().join("start-ready");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("marker worker exited before ready: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "marker worker timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let value: Value = serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
    let conversation: uuid::Uuid = serde_json::from_value(value["conversation"].clone()).unwrap();
    let task: uuid::Uuid = serde_json::from_value(value["task"].clone()).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let conv_marker = store
        .get(format!("runtime/conversation-callback-start/{conversation}").as_bytes())
        .unwrap()
        .unwrap();
    let task_marker = store
        .get(format!("durable/task-callback-start/{task}").as_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(conv_marker.value, task_marker.value);
    let marker: Value = serde_json::from_slice(&conv_marker.value).unwrap();
    let evidence_key =
        format!("durable/replay-safe-async/{task}/conversation-park-cancel-signals-v1");
    let evidence: Value =
        serde_json::from_slice(&store.get(evidence_key.as_bytes()).unwrap().unwrap().value)
            .unwrap();
    assert_eq!(evidence["version"], json!(1));
    assert_eq!(evidence["claim_id"], marker["claim_id"]);
    assert_eq!(evidence["task"], json!(task));
    assert_eq!(evidence["conversation"], json!(conversation));
    let signal = crate::conversation_signal::Signal::new(
        uuid::Uuid::new_v4(),
        conversation,
        "after-kill".into(),
        json!(1),
    );
    assert!(matches!(
        crate::conversation_signal::Inbox::new(&store).send(&signal),
        Err(crate::conversation_signal::Error::CallbackStartBusy)
    ));
    let task_signal =
        crate::task_store::Signal::new(uuid::Uuid::new_v4(), task, "after-kill".into(), json!(1));
    assert!(matches!(
        crate::task_store::TaskStore::new(&store).send_signal(&task_signal),
        Err(crate::task_store::Error::CallbackStartBusy)
    ));
}

#[tokio::test]
async fn conversation_cancel_panic_preserves_unknown_and_releases_marker() {
    exercise_conversation_park_claim("dispatch_panic").await;
}

#[tokio::test]
async fn conversation_cancel_late_outcome_at_settlement_retries_and_reconciles() {
    exercise_conversation_park_claim("dispatch_late_during_settlement").await;
}

#[tokio::test]
async fn conversation_cancel_late_outcome_after_receipt_requires_reconciliation() {
    exercise_conversation_park_claim("dispatch_late_after_receipt").await;
}

#[tokio::test]
async fn conversation_cancel_late_outcome_prevents_success_receipt() {
    exercise_conversation_park_claim("dispatch_late").await;
}

#[tokio::test]
async fn conversation_cancel_abort_before_first_poll_prevents_dispatch() {
    exercise_conversation_park_claim("dispatch_abort_before").await;
}

#[tokio::test]
async fn conversation_cancel_signal_before_first_poll_prevents_dispatch() {
    exercise_conversation_park_claim("dispatch_signal_before").await;
}

#[tokio::test]
async fn conversation_cancel_reentrant_abort_fails_without_waiting() {
    exercise_conversation_park_claim("dispatch_reentrant_abort").await;
}

#[tokio::test]
async fn conversation_cancel_reentrant_signal_fails_after_yield() {
    exercise_conversation_park_claim("dispatch_reentrant").await;
}

#[tokio::test]
async fn conversation_cancel_scoped_spawn_rejects_signal_self_wait() {
    exercise_conversation_park_claim("dispatch_spawn_signal").await;
}

#[tokio::test]
async fn conversation_cancel_scoped_spawn_rejects_abort_self_wait() {
    exercise_conversation_park_claim("dispatch_spawn_abort").await;
}

#[tokio::test]
async fn conversation_cancel_unscoped_spawn_retry_can_be_cancelled() {
    exercise_conversation_park_claim("dispatch_spawn_unscoped_cancel").await;
}

#[tokio::test]
async fn conversation_cancel_first_poll_fences_signal_and_identity_update() {
    exercise_conversation_park_claim("dispatch_guard").await;
}

#[tokio::test]
async fn conversation_cancel_settles_while_independent_maintenance_is_pending() {
    exercise_conversation_park_claim("dispatch_maintenance").await;
}

#[tokio::test]
async fn conversation_cancel_pending_callback_does_not_block_other_graph() {
    exercise_conversation_park_claim("dispatch_hold").await;
}

#[tokio::test]
async fn conversation_abort_cleanup_claim_is_fenced_during_callback() {
    exercise_conversation_park_claim("dispatch_hold_cleanup").await;
}

#[tokio::test]
async fn conversation_cancel_claim_keeps_selection_stable_through_commit() {
    exercise_conversation_park_claim("selection_fence").await;
}

#[tokio::test]
async fn selected_hook_cancel_claim_rejects_selection_change() {
    exercise_conversation_park_claim("selection_hook_fence").await;
}

#[tokio::test]
async fn selected_hook_callback_pins_selection_and_publishes_after_result() {
    exercise_conversation_park_claim("selection_hook_test_dispatch").await;
}

#[tokio::test]
async fn selected_hook_cancel_marker_conflict_preserves_callback_without_after() {
    exercise_conversation_park_claim("selection_hook_marker_conflict").await;
}

#[tokio::test]
async fn selected_hook_cancel_snapshot_conflict_preserves_callback_without_after() {
    exercise_conversation_park_claim("selection_hook_snapshot_conflict").await;
}

#[tokio::test]
async fn selected_hook_replaced_receipt_is_not_published() {
    exercise_conversation_park_claim("selection_hook_receipt_changed").await;
}

#[tokio::test]
async fn selected_hook_cancel_drains_two_park_levels() {
    exercise_conversation_park_claim("selection_hook_multilevel").await;
}

#[tokio::test]
async fn selected_hook_conversation_abort_publishes_after_result() {
    exercise_conversation_park_claim("selection_hook_conversation_abort").await;
}

#[tokio::test]
async fn selected_hook_abort_memo_is_persisted() {
    exercise_conversation_park_claim("selection_hook_after_memo").await;
}

#[tokio::test]
async fn selected_hook_abort_memo_snapshot_conflict_fails_closed() {
    exercise_conversation_park_claim("selection_hook_memo_conflict").await;
}

#[tokio::test]
async fn selected_hook_output_schema_is_rejected_before_cancel() {
    exercise_conversation_park_claim("selection_hook_output_schema").await;
}

#[tokio::test]
async fn selected_hook_cancel_rejects_uncertain_unsafe_descendant() {
    exercise_conversation_park_claim("selection_hook_unsafe_descendant").await;
}

#[tokio::test]
async fn selected_hook_cancel_kill_worker() {
    if std::env::var_os("PI_SELECTED_CANCEL_CUT").is_some() {
        exercise_conversation_park_claim("selection_hook_kill").await;
    }
}

#[tokio::test]
async fn selected_hook_cancel_kill_reopens_with_fenced_result() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Cancel(Arc<AtomicUsize>);
    #[async_trait]
    impl crate::async_task_tool::DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for (cut, late) in [
        ("before", false),
        ("after", false),
        ("after", true),
        ("after_memo", false),
        ("after_receipt", false),
        ("after_receipt", true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::selected_hook_cancel_kill_worker",
                "--nocapture",
            ])
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env("PI_SELECTED_CANCEL_CUT", cut)
            .env(
                "PI_TEST_CONV_CALLBACK_KILL",
                if cut == "after" { "1" } else { "0" },
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let ready = dir.path().join("ready");
        let reached = tokio::time::timeout(Duration::from_secs(20), async {
            while !ready.exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "worker exited before {cut} barrier"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if reached.is_err() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{cut} barrier timed out");
        }
        let claim: crate::task_scheduler::ConversationCancelClaim =
            serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        if matches!(cut, "after_memo" | "after_receipt") {
            let key = format!("runtime/extension-memo/{}/abort-memo", claim.task);
            let memo = store.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&memo.value).unwrap(),
                json!("saved")
            );
        }
        let prefix = format!("durable/unsafe-park-async/{}/", claim.task);
        let intents: Vec<_> = store
            .list(prefix.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|(key, _)| key.ends_with(b"/intent").then_some(key))
            .collect();
        assert!(!intents.is_empty());
        if late {
            let intent_entry = store.get(&intents[0]).unwrap().unwrap();
            let intent: pi_rs_durable::task::Task =
                serde_json::from_slice(&intent_entry.value).unwrap();
            let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                task_id: claim.task,
                attempt_id: intent.attempt_id().unwrap(),
                outcome: Outcome::Pending {
                    reference: "late-remote-job".into(),
                },
            };
            let mut receipt_key = intents[0].clone();
            receipt_key.extend_from_slice(b"/receipt");
            store
                .commit(
                    &[durable_store::Check {
                        key: receipt_key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key: receipt_key,
                        value: serde_json::to_vec(&receipt).unwrap(),
                    }],
                )
                .unwrap();
        }
        drop(store);

        let recovery_after_calls = Arc::new(AtomicUsize::new(0));
        let mut extensions = crate::extension::Registry::default();
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "policy".into(),
                    hooks: vec![Arc::new(ClaimPassHook(
                        Some(recovery_after_calls.clone()),
                        false,
                    ))],
                    ..Default::default()
                },
                crate::extension::ExtensionIdentity::new(
                    1,
                    "policy-v1".into(),
                    vec!["hook-v1".into()],
                )
                .unwrap(),
            )
            .unwrap();
        let snapshot = extensions.snapshot();
        let names = vec!["policy".to_owned()];
        let hooks = crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe;
        let mut registry = Registry::default();
        let leaf = crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(
            &mut registry, Arc::new(AsyncTool { name: "leaf", child: None, gate: None, early: false }), "leaf", 1, vec![], &snapshot, &names,
            ReplaySafeAuthorization::host_attests_replay_safe(), hooks(),
        ).unwrap();
        let tool = Arc::new(AsyncTool {
            name: "root",
            child: Some("leaf"),
            gate: None,
            early: false,
        });
        let identity_tool: Arc<dyn Tool> = tool.clone();
        let _identity =
            crate::nested_identity::register(&identity_tool, Some("backend-v1".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let root = crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(
            &mut registry,
            tool,
            "root",
            1,
            vec![leaf.clone()],
            &snapshot,
            &names,
            "cancel-v1",
            Arc::new(Cancel(calls.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(), hooks(),
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
        let scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store);
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let current = tasks.load(claim.task).unwrap().unwrap();
                if current.task.is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("recovery did not settle");
        let current = tasks.load(claim.task).unwrap().unwrap();
        if cut == "after_receipt" && !late {
            assert_eq!(current.task.state(), &TaskState::Cancelled);
            let pi_rs_durable::task::TaskOwner::Conversation(conversation) = current.task.owner()
            else {
                panic!("not conversation owned")
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let c = scheduler.conversation(*conversation).unwrap();
                    if c.messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("after cancel"))) { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("recovered after result was not published");
        } else {
            assert!(
                matches!(current.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation")),
                "{cut}: {:?}",
                current.task.state()
            );
        }
        let after: Vec<_> = store
            .list(prefix.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|(key, _)| key.ends_with(b"/intent").then_some(key))
            .collect();
        assert_eq!(intents, after);
        if late {
            let receipts =
                crate::async_task_tool::read_unsafe_durable_receipts(&store, claim.task).unwrap();
            assert!(receipts.iter().any(|receipt| matches!(&receipt.outcome, Outcome::Pending { reference } if reference == "late-remote-job")));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(recovery_after_calls.load(Ordering::SeqCst), 0);
        let _ = driver.shutdown_with_pending().unwrap();
        let _ = scheduler.shutdown().await;
    }
}

#[tokio::test]
async fn conversation_cancel_dispatches_once_and_stores_receipt() {
    exercise_conversation_park_claim("dispatch_ok").await;
}

#[tokio::test]
async fn conversation_start_marker_transaction_fences_real_driver_races() {
    for case in [
        "start_ok",
        "start_retire",
        "start_owner",
        "start_abort",
        "start_signal",
        "start_task_signal",
        "start_signal_after_claim",
        "start_task_signal_after_claim",
        "start_child",
        "start_legacy",
    ] {
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn late_outcome_after_claim_blocks_pre_dispatch_check() {
    exercise_conversation_park_claim("late_after_claim").await;
}

#[tokio::test]
async fn conversation_cancel_test_receipt_is_stored_by_driver() {
    exercise_conversation_park_claim("save_receipt").await;
}

#[tokio::test]
async fn stopped_receipt_rejects_replaced_terminal_attempt() {
    exercise_conversation_park_claim("save_receipt_marker").await;
}
#[tokio::test]
async fn stopped_receipt_rejects_replaced_owner() {
    exercise_conversation_park_claim("save_receipt_owner").await;
}
#[tokio::test]
async fn stopped_receipt_rejects_late_outcome_at_commit() {
    exercise_conversation_park_claim("save_receipt_late").await;
}

#[tokio::test]
async fn conversation_claim_rejects_owner_identity_and_abort_races() {
    for case in [
        "owner",
        "identity",
        "abort",
        "reentrant",
        "mismatch",
        "late_receipt",
        "conversation_signal",
        "task_signal",
    ] {
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn conversation_receipt_terminal_marker_survives_process_kill() {
    use pi_rs_durable::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_park_cancel,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Cancel(Arc<AtomicUsize>);
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact").arg("conversation_claim_boundary_tests::conversation_cancel_test_receipt_is_stored_by_driver")
        .arg("--nocapture").env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
    let ready = dir.path().join("ready");
    let wait = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if ready.exists() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("fixture exited: {status}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if wait.is_err() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("fixture timed out");
    }
    let receipt: crate::task_scheduler::ConversationCancelReceipt =
        serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    let leaf = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(AsyncTool {
            name: "leaf",
            child: None,
            gate: None,
            early: false,
        }),
        "leaf",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity_tool: Arc<dyn Tool> = tool.clone();
    let _identity =
        pi_rs_durable::nested_identity::register(&identity_tool, Some("backend-v1".into()))
            .unwrap();
    let root = register_conversation_unsafe_durable_tool_with_park_cancel(
        &mut registry,
        tool,
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
    let h = pi_rs_durable::runtime::Harness::open_paused(dir.path().join("db"), options).unwrap();
    assert_eq!(
        h.read_stopped_conversation_cancel_receipt(
            receipt.claim.task,
            receipt.claim.attempt,
            receipt.claim.claim_id,
            &registry
        )
        .unwrap(),
        Some(receipt.clone())
    );
    assert!(
        h.read_stopped_conversation_cancel_receipt(
            receipt.claim.task,
            uuid::Uuid::new_v4(),
            receipt.claim.claim_id,
            &registry
        )
        .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn conversation_cancel_claim_kill_worker() {
    if std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").is_some() {
        let case = if std::env::var("PI_TEST_CONV_CALLBACK_KILL").ok().as_deref() == Some("1") {
            "dispatch_kill"
        } else {
            "kill_after_claim"
        };
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn interrupted_conversation_claim_settles_original_attempt_and_other_graph_runs() {
    use pi_rs_durable::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_park_cancel,
    };
    use pi_rs_durable::task::{Checkpoint, Task, TaskOwner};
    use pi_rs_durable::task_scheduler::Step;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Cancel(Arc<AtomicUsize>);
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for (late, busy, uncertain, dispatch) in [
        (false, false, false, false),
        (true, false, false, false),
        (true, true, false, false),
        (false, false, true, false),
        (false, false, false, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::conversation_cancel_claim_kill_worker",
                "--nocapture",
            ])
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env(
                "PI_TEST_CONV_CALLBACK_KILL",
                if dispatch { "1" } else { "0" },
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let ready = dir.path().join("ready");
        let wait = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if ready.exists() {
                    break;
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("claim worker exited: {status}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if wait.is_err() {
            let _ = child.kill();
            let _ = child.wait();
            panic!("claim barrier timed out");
        }
        let claim_bytes = std::fs::read(ready).unwrap();
        let claim: crate::task_scheduler::ConversationCancelClaim =
            serde_json::from_slice(&claim_bytes).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store);
        let running = tasks.load(claim.task).unwrap().unwrap();
        assert_eq!(running.task.state(), &TaskState::Running);
        assert_eq!(running.task.attempt_id(), Some(claim.attempt));
        assert!(
            store
                .get(&crate::task_scheduler::conversation_cancel_terminal_key(
                    claim.task
                ))
                .unwrap()
                .is_none()
        );
        if late {
            let prefix = format!("durable/unsafe-park-async/{}/", claim.task);
            let (mut key, entry) = store
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent"))
                .expect("park intent");
            let intent: Task = serde_json::from_slice(&entry.value).unwrap();
            let receipt = pi_rs_durable::async_task_tool::UnsafeDurableReceipt {
                task_id: claim.task,
                attempt_id: intent.attempt_id().unwrap(),
                outcome: Outcome::Pending {
                    reference: "late-remote-job".into(),
                },
            };
            key.extend_from_slice(b"/receipt");
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&receipt).unwrap(),
                    }],
                )
                .unwrap();
        }
        let unsafe_prefix = format!("durable/unsafe-park-async/{}/", claim.task);
        let intents_before: Vec<_> = store
            .list(unsafe_prefix.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|(key, _)| key.ends_with(b"/intent").then_some(key))
            .collect();
        assert!(!intents_before.is_empty());
        drop(store);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = Registry::default();
        registry.register("independent", true, |_: &Task| {
            Ok(Step::Succeed(json!("ok")))
        });
        let leaf = register_conversation_replay_safe_durable_tool(
            &mut registry,
            Arc::new(AsyncTool {
                name: "leaf",
                child: None,
                gate: None,
                early: false,
            }),
            "leaf",
            1,
            vec![],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
        .unwrap();
        let tool = Arc::new(AsyncTool {
            name: "root",
            child: Some("leaf"),
            gate: None,
            early: false,
        });
        let identity_tool: Arc<dyn Tool> = tool.clone();
        let _identity =
            pi_rs_durable::nested_identity::register(&identity_tool, Some("backend-v1".into()))
                .unwrap();
        let root = if dispatch {
            pi_rs_durable::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(
                &mut registry, tool, "root", 1, vec![leaf.clone()], "cancel-v1",
                Arc::new(Cancel(calls.clone())),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
        } else {
            register_conversation_unsafe_durable_tool_with_park_cancel(
                &mut registry, tool, "root", 1, vec![leaf.clone()], "cancel-v1",
                Arc::new(Cancel(calls.clone())),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
        }.unwrap();
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
        let busy_writer = if busy {
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (writer_tx, writer_rx) = std::sync::mpsc::channel();
            let writer_path = path.clone();
            let busy_signal = Arc::new(tokio::sync::Semaphore::new(0));
            let notify = busy_signal.clone();
            crate::task_scheduler::on_conversation_claim_recovery_busy(claim.task, move || {
                notify.add_permits(1)
            });
            crate::task_scheduler::on_conversation_claim_recovery_before_commit(
                claim.task,
                move || {
                    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
                    let writer = std::thread::spawn(move || {
                        let conn = rusqlite::Connection::open(writer_path).unwrap();
                        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                        locked_tx.send(()).unwrap();
                        release_rx.recv_timeout(Duration::from_secs(20)).unwrap();
                        conn.execute_batch("COMMIT").unwrap();
                    });
                    locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    writer_tx.send(writer).unwrap();
                },
            );
            Some((release_tx, writer_rx, busy_signal))
        } else {
            None
        };
        if uncertain {
            crate::task_scheduler::inject_conversation_claim_recovery_uncertain(claim.task);
        }
        let driver = scheduler.start_tasks(registry).unwrap();
        if uncertain {
            tokio::time::timeout(Duration::from_secs(10), async {
                while driver.get(claim.task).is_ok() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("indeterminate recovery must stop the driver");
            let report = driver.shutdown_with_pending().unwrap();
            assert!(report.failure().is_some());
            let store = durable_store::Store::open(&path).unwrap();
            let terminal = pi_rs_durable::task_store::TaskStore::new(&store)
                .load(claim.task)
                .unwrap()
                .unwrap();
            assert!(
                matches!(terminal.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation required"))
            );
            let entry = store
                .get(&crate::task_scheduler::conversation_cancel_terminal_key(
                    claim.task,
                ))
                .unwrap()
                .unwrap();
            let marker: crate::task_scheduler::ConversationCancelTerminalMarker =
                serde_json::from_slice(&entry.value).unwrap();
            assert_eq!(marker.attempt, claim.attempt);
            assert_eq!(marker.claim_id, claim.claim_id);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            let _ = scheduler.shutdown().await;
            continue;
        }
        if let Some((release, writer, signal)) = busy_writer {
            tokio::time::timeout(Duration::from_secs(12), signal.acquire())
                .await
                .expect("recovery must reach the locked commit")
                .unwrap()
                .forget();
            assert!(driver.get(claim.task).unwrap().unwrap().state() == &TaskState::Running);
            release.send(()).unwrap();
            writer
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .join()
                .unwrap();
        }
        let other = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        let other_id = uuid::Uuid::new_v4();
        let other_task = Task::new(
            other_id,
            TaskOwner::Conversation(other),
            Checkpoint {
                phase: "start".into(),
                data: json!({}),
            },
            4,
        )
        .unwrap()
        .registered("independent".into(), true);
        driver.submit(&other_task).unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = pi_rs_durable::task_store::TaskStore::new(&store);
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let root = tasks.load(claim.task).unwrap().unwrap();
                let other = tasks.load(other_id).unwrap().unwrap();
                if root.task.is_terminal() && other.task.is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("recovery and unrelated graph must progress");
        let terminal = tasks.load(claim.task).unwrap().unwrap();
        assert!(
            matches!(terminal.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation required"))
        );
        assert!(matches!(
            tasks.load(other_id).unwrap().unwrap().task.state(),
            TaskState::Succeeded(_)
        ));
        let marker_key = crate::task_scheduler::conversation_cancel_terminal_key(claim.task);
        let marker_entry = store.get(&marker_key).unwrap().unwrap();
        let marker: crate::task_scheduler::ConversationCancelTerminalMarker =
            serde_json::from_slice(&marker_entry.value).unwrap();
        assert_eq!(marker.claim_id, claim.claim_id);
        assert_eq!(marker.attempt, claim.attempt);
        assert_eq!(marker.running_task_version, running.version);
        assert_eq!(marker.terminal_task, terminal.task);
        let task_commit = store
            .commit_for_version(&crate::task_store::key(claim.task), terminal.version)
            .unwrap();
        assert!(task_commit.is_some());
        assert_eq!(
            task_commit,
            store
                .commit_for_version(&marker_key, marker_entry.version)
                .unwrap()
        );
        let claim_key = format!(
            "durable/replay-safe-async/{}/conversation-park-cancel-claim",
            claim.task
        );
        assert_eq!(
            store.get(claim_key.as_bytes()).unwrap().unwrap().value,
            claim_bytes
        );
        let intents_after: Vec<_> = store
            .list(unsafe_prefix.as_bytes())
            .unwrap()
            .into_iter()
            .filter_map(|(key, _)| key.ends_with(b"/intent").then_some(key))
            .collect();
        assert_eq!(
            intents_after, intents_before,
            "recovery must not redispatch the Tool"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        if late {
            let receipts =
                pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, claim.task)
                    .unwrap();
            assert!(receipts.iter().any(|r| matches!(&r.outcome, Outcome::Pending { reference } if reference == "late-remote-job")));
        }
        let report = driver.shutdown_with_pending().unwrap();
        assert!(
            report.failure().is_none(),
            "driver failed: {:?}",
            report.failure()
        );
        let _ = scheduler.shutdown().await;
    }
}

#[test]
fn claim_recovery_progresses_during_independent_maintenance() {
    exercise_claim_recovery_during_maintenance(false);
}

#[test]
fn repeated_claim_conflicts_yield_to_independent_signal_and_maintenance() {
    exercise_claim_recovery_during_maintenance(true);
}

fn exercise_claim_recovery_during_maintenance(conflicts: bool) {
    use crate::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_park_cancel,
    };
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_scheduler::Step;
    use crate::task_store::{Save, TaskStore};
    use std::time::Instant;
    struct Cancel;
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            panic!("callback is disabled")
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let root = std::env::var_os("PI_TEST_CLAIM_CONFLICT_KILL_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| dir.path().to_path_buf());
    let mut worker = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::conversation_cancel_claim_kill_worker",
        ])
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", root.as_path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    while !root.as_path().join("ready").exists() {
        assert!(worker.try_wait().unwrap().is_none(), "claim worker exited");
        assert!(Instant::now() < until, "claim worker timed out");
        std::thread::sleep(Duration::from_millis(5));
    }
    let claim_bytes = std::fs::read(root.as_path().join("ready")).unwrap();
    let claim: crate::task_scheduler::ConversationCancelClaim =
        serde_json::from_slice(&claim_bytes).unwrap();
    worker.kill().unwrap();
    worker.wait().unwrap();
    let path = root.join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = TaskStore::new(&store);
    let original = tasks.load(claim.task).unwrap().unwrap();
    let mut parent = Task::new(
        uuid::Uuid::new_v4(),
        TaskOwner::Conversation(uuid::Uuid::new_v4()),
        Checkpoint {
            phase: "initial".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("independent".into(), true);
    parent.start().unwrap();
    let child = Task::new(
        uuid::Uuid::new_v4(),
        TaskOwner::Task(parent.id()),
        Checkpoint {
            phase: "initial".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("independent".into(), true);
    drop(store);
    let mut registry = Registry::default();
    registry.register("independent", true, |_: &Task| {
        Ok(Step::Succeed(json!("ok")))
    });
    let leaf = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(AsyncTool {
            name: "leaf",
            child: None,
            gate: None,
            early: false,
        }),
        "leaf",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity: Arc<dyn Tool> = tool.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let root = register_conversation_unsafe_durable_tool_with_park_cancel(
        &mut registry,
        tool,
        "root",
        1,
        vec![leaf.clone()],
        "cancel-v1",
        Arc::new(Cancel),
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    let harness = crate::runtime::Harness::open_paused(
        &path,
        Options {
            model: Arc::new(ModelStub {
                arguments: "{}".into(),
            }),
            tools: vec![root.register(), leaf.register()],
            restart_limit: 4,
            max_turns: 4,
            uncertain_policy: UncertainPolicy::RequireReconciliation,
        },
    )
    .unwrap();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (writer_tx, writer_rx) = std::sync::mpsc::channel();
    let (busy_tx, busy_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let writer_path = path.clone();
    crate::task_scheduler::on_conversation_claim_recovery_before_commit(claim.task, move || {
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let db = rusqlite::Connection::open(writer_path).unwrap();
            db.execute_batch("BEGIN IMMEDIATE").unwrap();
            locked_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(20)).unwrap();
            db.execute_batch("COMMIT").unwrap();
        });
        locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        writer_tx.send(writer).unwrap();
    });
    crate::task_scheduler::on_conversation_claim_recovery_busy(claim.task, move || {
        busy_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    });
    let task_scheduler = harness.attach_tasks(registry).unwrap();
    let owner = task_scheduler.owner();
    let control = owner.conversation_controls().get(uuid::Uuid::new_v4());
    let poll = control.gate.lock().unwrap();
    crate::task_scheduler::set_maintenance_busy_for_test(parent.id(), true);
    let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
    crate::task_driver::on_maintenance_reserved_for_test(parent.id(), move || {
        reserved_tx.send(()).unwrap()
    });
    let driver = crate::task_driver::Driver::start_with_harness(task_scheduler, harness.worker());
    busy_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("claim recovery did not encounter writer lock");
    release_tx.send(()).unwrap();
    writer_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .join()
        .unwrap();
    {
        // The driver is paused in its test-only busy hook while holding the
        // task gate. Insert the independent graph through the store seam.
        let tasks = TaskStore::new(owner.store());
        tasks
            .save(&[Save::Create(&parent), Save::Create(&child)])
            .unwrap();
        tasks
            .update(&tasks.load(parent.id()).unwrap().unwrap(), |task| {
                task.hold_failure("maintenance".into())
            })
            .unwrap();
    }
    let signal_id = uuid::Uuid::new_v4();
    let mut signal_task = Task::new(
        signal_id,
        TaskOwner::Conversation(uuid::Uuid::new_v4()),
        Checkpoint {
            phase: "initial".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("independent".into(), true);
    signal_task.start().unwrap();
    signal_task
        .wait(crate::task::WaitFor::Signal("go".into()))
        .unwrap();
    let conflict_id = uuid::Uuid::new_v4();
    let mut conflict_task = Task::new(
        conflict_id,
        TaskOwner::Conversation(uuid::Uuid::new_v4()),
        Checkpoint {
            phase: "initial".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("independent".into(), true);
    conflict_task.start().unwrap();
    conflict_task.succeed(json!(null)).unwrap();
    let conflict_enabled = Arc::new(std::sync::atomic::AtomicBool::new(conflicts));
    let conflict_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    if conflicts {
        let tasks = TaskStore::new(owner.store());
        tasks
            .save(&[Save::Create(&signal_task), Save::Create(&conflict_task)])
            .unwrap();
        tasks
            .send_signal(&crate::task_store::Signal::new(
                uuid::Uuid::new_v4(),
                signal_id,
                "go".into(),
                json!(null),
            ))
            .unwrap();
        fn install_conflict(
            id: uuid::Uuid,
            conflict_id: uuid::Uuid,
            path: std::path::PathBuf,
            enabled: Arc<std::sync::atomic::AtomicBool>,
            count: Arc<std::sync::atomic::AtomicUsize>,
        ) {
            crate::task_scheduler::on_conversation_claim_recovery_before_commit(id, move || {
                if !enabled.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let store = durable_store::Store::open(&path).unwrap();
                let tasks = TaskStore::new(&store);
                let current = tasks.load(conflict_id).unwrap().unwrap();
                tasks.save(&[Save::Update(&current)]).unwrap();
                let previous = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if previous >= 2
                    && let Some(root) = std::env::var_os("PI_TEST_CLAIM_CONFLICT_KILL_DIR")
                {
                    std::fs::write(
                        std::path::PathBuf::from(root).join("conflict-ready"),
                        b"yes",
                    )
                    .unwrap();
                    loop {
                        std::thread::park();
                    }
                }
                install_conflict(id, conflict_id, path, enabled, count);
            });
        }
        install_conflict(
            claim.task,
            conflict_id,
            path.clone(),
            conflict_enabled.clone(),
            conflict_count.clone(),
        );
    }
    resume_tx.send(()).unwrap();
    reserved_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("maintenance was not retained");
    if conflicts {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if conflict_count.load(std::sync::atomic::Ordering::SeqCst) >= 3
                && driver.get(signal_id).unwrap().unwrap().is_terminal()
            {
                break;
            }
            assert!(
                Instant::now() < until,
                "signal was starved by repeated recovery conflicts: count {}, signal {:?}, claim {:?}",
                conflict_count.load(std::sync::atomic::Ordering::SeqCst),
                driver.get(signal_id),
                driver.get(claim.task)
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!driver.get(claim.task).unwrap().unwrap().is_terminal());
        assert!(matches!(
            driver.get(parent.id()).unwrap().unwrap().state(),
            TaskState::CompletingFailed(_)
        ));
        conflict_enabled.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let current = driver.get(claim.task).unwrap().unwrap();
        if current.is_terminal() {
            break;
        }
        assert!(
            Instant::now() < until,
            "claim remained blocked behind unrelated maintenance"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(matches!(
        driver.get(parent.id()).unwrap().unwrap().state(),
        TaskState::CompletingFailed(_)
    ));
    let marker_key = crate::task_scheduler::conversation_cancel_terminal_key(claim.task);
    let marker_entry = owner.store().get(&marker_key).unwrap().unwrap();
    let marker: crate::task_scheduler::ConversationCancelTerminalMarker =
        serde_json::from_slice(&marker_entry.value).unwrap();
    assert_eq!(marker.claim_id, claim.claim_id);
    assert_eq!(marker.attempt, claim.attempt);
    assert_eq!(marker.running_task_version, original.version);
    let terminal = TaskStore::new(owner.store())
        .load(claim.task)
        .unwrap()
        .unwrap();
    assert_eq!(
        owner
            .store()
            .commit_for_version(&crate::task_store::key(claim.task), terminal.version)
            .unwrap(),
        owner
            .store()
            .commit_for_version(&marker_key, marker_entry.version)
            .unwrap()
    );
    drop(poll);
    crate::task_scheduler::set_maintenance_busy_for_test(parent.id(), false);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done_tx.send(driver.shutdown_with_pending());
    });
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .failure()
            .is_none()
    );
}

#[test]
fn claim_recovery_repeated_conflicts_survive_process_kill() {
    if std::env::var_os("PI_TEST_CLAIM_CONFLICT_KILL_DIR").is_some() {
        return;
    }
    use crate::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_park_cancel,
    };
    struct Cancel;
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            panic!("callback disabled")
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "conversation_claim_boundary_tests::repeated_claim_conflicts_yield_to_independent_signal_and_maintenance"])
        .env("PI_TEST_CLAIM_CONFLICT_KILL_DIR", dir.path())
        .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(30);
    while !dir.path().join("conflict-ready").exists() {
        assert!(
            process.try_wait().unwrap().is_none(),
            "conflict worker exited"
        );
        assert!(
            std::time::Instant::now() < until,
            "conflict worker timed out"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let claim: crate::task_scheduler::ConversationCancelClaim =
        serde_json::from_slice(&std::fs::read(dir.path().join("ready")).unwrap()).unwrap();
    let path = dir.path().join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let original = crate::task_store::TaskStore::new(&store)
        .load(claim.task)
        .unwrap()
        .unwrap();
    assert_eq!(original.task.attempt_id(), Some(claim.attempt));
    assert!(
        store
            .get(&crate::task_scheduler::conversation_cancel_terminal_key(
                claim.task
            ))
            .unwrap()
            .is_none()
    );
    drop(store);
    let mut registry = Registry::default();
    registry.register("independent", true, |_: &crate::task::Task| {
        Ok(crate::task_scheduler::Step::Succeed(json!("ok")))
    });
    let leaf = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(AsyncTool {
            name: "leaf",
            child: None,
            gate: None,
            early: false,
        }),
        "leaf",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity: Arc<dyn Tool> = tool.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let root = register_conversation_unsafe_durable_tool_with_park_cancel(
        &mut registry,
        tool,
        "root",
        1,
        vec![leaf.clone()],
        "cancel-v1",
        Arc::new(Cancel),
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
    let harness = crate::runtime::Harness::open_paused(&path, options).unwrap();
    let scheduler = harness.attach_tasks(registry).unwrap();
    let driver = crate::task_driver::Driver::start_with_harness(scheduler, harness.worker());
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if driver.get(claim.task).unwrap().unwrap().is_terminal() {
            break;
        }
        assert!(std::time::Instant::now() < until, "claim did not recover");
        std::thread::sleep(Duration::from_millis(5));
    }
    let store = durable_store::Store::open(&path).unwrap();
    let entry = store
        .get(&crate::task_scheduler::conversation_cancel_terminal_key(
            claim.task,
        ))
        .unwrap()
        .unwrap();
    let marker: crate::task_scheduler::ConversationCancelTerminalMarker =
        serde_json::from_slice(&entry.value).unwrap();
    assert_eq!(marker.claim_id, claim.claim_id);
    assert_eq!(marker.attempt, claim.attempt);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(driver.shutdown_with_pending());
    });
    assert!(
        rx.recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .failure()
            .is_none()
    );
}

#[tokio::test]
async fn nested_claim_binds_real_conversation_and_ancestry() {
    exercise_conversation_park_claim("nested_claim_probe").await;
}

#[tokio::test]
async fn nested_claim_rejects_late_ancestor_signal_and_receipt() {
    for case in ["nested_claim_signal", "nested_claim_late"] {
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn nested_callback_probe_commits_receipt_once() {
    exercise_conversation_park_claim("nested_claim_callback").await;
}

#[tokio::test]
async fn nested_callback_kill_worker() {
    if std::env::var_os("PI_TEST_NESTED_CALLBACK_KILL").is_some() {
        let case = if std::env::var_os("PI_TEST_NESTED_CALLBACK_LATE").is_some() {
            "nested_claim_late_kill"
        } else {
            "nested_claim_kill"
        };
        exercise_conversation_park_claim(case).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn nested_callback_kill_preserves_claim_and_ancestor_markers() {
    for late in [false, true] {
        exercise_nested_callback_kill(late).await;
    }
}

#[cfg(unix)]
async fn exercise_nested_callback_kill(late: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    if late {
        command.env("PI_TEST_NESTED_CALLBACK_LATE", "1");
    }
    let mut process = command
        .arg("--exact")
        .arg("conversation_claim_boundary_tests::nested_callback_kill_worker")
        .env("PI_TEST_NESTED_CALLBACK_KILL", "1")
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let task = loop {
        if let Ok(value) = std::fs::read_to_string(dir.path().join("ready")) {
            if let Ok(id) = value.parse::<uuid::Uuid>() {
                break id;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nested callback did not start"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    process.kill().unwrap();
    process.wait().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = crate::task_store::TaskStore::new(&store);
    let child = tasks.load(task).unwrap().unwrap();
    let pi_rs_durable::task::TaskOwner::Task(root) = child.task.owner() else {
        panic!("child owner missing")
    };
    for id in [task, *root] {
        assert!(
            store
                .get(format!("durable/task-callback-start/{id}").as_bytes())
                .unwrap()
                .is_some()
        );
    }
    assert!(
        store
            .get(
                format!("durable/replay-safe-async/{task}/conversation-park-cancel-claim")
                    .as_bytes()
            )
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .get(
                format!("durable/replay-safe-async/{task}/conversation-park-cancel-receipt")
                    .as_bytes()
            )
            .unwrap()
            .is_none()
    );

    let root = *root;
    drop(store);
    struct NeverCancel(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait]
    impl crate::async_task_tool::DurableParkCancel for NeverCancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = Registry::default();
    let grandchild = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(AsyncTool {
            name: "grandchild",
            child: None,
            gate: None,
            early: false,
        }),
        "grandchild",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let leaf_tool = Arc::new(AsyncTool {
        name: "leaf",
        child: Some("grandchild"),
        gate: None,
        early: false,
    });
    let leaf_identity_tool: Arc<dyn Tool> = leaf_tool.clone();
    let _leaf_identity =
        pi_rs_durable::nested_identity::register(&leaf_identity_tool, Some("leaf-backend".into()))
            .unwrap();
    let leaf = crate::async_task_tool::register_conversation_unsafe_durable_tool_with_park_cancel(
        &mut registry,
        leaf_tool,
        "leaf",
        1,
        vec![grandchild.clone()],
        "cancel-v1",
        Arc::new(NeverCancel(calls.clone())),
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
    )
    .unwrap();
    let root_tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let root_identity_tool: Arc<dyn Tool> = root_tool.clone();
    let _root_identity =
        pi_rs_durable::nested_identity::register(&root_identity_tool, Some("backend-v1".into()))
            .unwrap();
    let root_tool =
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_park_cancel(
            &mut registry,
            root_tool,
            "root",
            1,
            vec![leaf.clone()],
            "cancel-v1",
            Arc::new(NeverCancel(calls.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root_tool.register(), leaf.register(), grandchild.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let scheduler = Scheduler::open(dir.path().join("db"), options).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let item = crate::task_store::TaskStore::new(&store).load(task).unwrap().unwrap();
            if item.task.is_terminal() {
                assert!(matches!(item.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation")));
                for id in [task, root] { assert!(store.get(format!("durable/task-callback-start/{id}").as_bytes()).unwrap().is_none()); }
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    if late {
        let store = durable_store::Store::open(dir.path().join("db")).unwrap();
        let prefix = format!("durable/unsafe-park-async/{task}/");
        assert!(
            store
                .list(prefix.as_bytes())
                .unwrap()
                .iter()
                .any(|(key, _)| key.ends_with(b"/intent/receipt"))
        );
    }
    let _ = driver.shutdown_with_pending().unwrap();
    let _ = scheduler.shutdown().await;
}

#[tokio::test]
async fn nested_selected_callback_runs_after_and_memo() {
    exercise_conversation_park_claim("nested_claim_selected").await;
}

#[tokio::test]
async fn nested_success_commits_terminal_evidence() {
    exercise_conversation_park_claim("nested_claim_success").await;
}

#[tokio::test]
async fn nested_success_rejects_late_tool_receipt_before_settlement() {
    exercise_conversation_park_claim("nested_claim_success_late").await;
}
