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
            if let Some(dir) = std::env::var_os("PI_TEST_SELECTED_RECORD_AFTER") {
                use std::io::Write;
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(std::path::Path::new(&dir).join("selected-after-count"))
                    .unwrap()
                    .write_all(b"x")
                    .unwrap();
            }
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
struct SchemaClaimHook(ClaimPassHook);
#[async_trait]
impl crate::extension::Hook for SchemaClaimHook {
    async fn before_tool(
        &self,
        call: &crate::extension::ToolCall,
        ctx: &crate::extension::Context<'_>,
    ) -> Result<crate::extension::BeforeTool, String> {
        self.0.before_tool(call, ctx).await
    }
    async fn after_tool_blocks(
        &self,
        call: &crate::extension::ToolCall,
        result: &crate::extension::ToolBlockResult,
        _nested: bool,
        ctx: &crate::extension::Context<'_>,
    ) -> Result<Option<crate::extension::ToolBlockResult>, String> {
        let Some(value) = self.0.after_tool(call, &json!(null), ctx).await? else {
            return Ok(None);
        };
        let mut result = result.clone();
        result.output = vec![json!({"type":"text", "text": value.as_str().unwrap()})];
        result
            .extra
            .insert("structuredOutput".into(), json!({"must":"be discarded"}));
        Ok(Some(result))
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
                if std::env::var_os("PI_TEST_WOKEN_LIVE_DIR").is_some() {
                    context.cancellation.cancelled().await;
                    return Err("cancelled".into());
                }
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
struct MutableSpecTool {
    inner: AsyncTool,
    changed: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl Tool for MutableSpecTool {
    fn spec(&self) -> ToolSpec {
        let mut spec = self.inner.spec();
        if self.changed.load(std::sync::atomic::Ordering::SeqCst) {
            spec.description = "updating".into();
        }
        spec
    }
    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        self.inner.invoke(args, context).await
    }
}
#[async_trait]
impl DurableAsyncTool for MutableSpecTool {
    async fn execute_durable(
        &self,
        args: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.inner
            .execute_durable(args, context, resume, host)
            .await
    }
}
async fn exercise_conversation_park_claim(case: &str) {
    exercise_conversation_park_claim_with_late_publication(case, false, false).await;
}
async fn exercise_conversation_park_claim_with_late_publication(
    case: &str,
    late_publication: bool,
    graph_aba: bool,
) {
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
            if self.1 == "nested_claim_kill" || self.1 == "nested_claim_actual_kill" {
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
    let selected = std::env::var_os("PI_TEST_NESTED_UNHOOKED").is_none()
        && matches!(
            case,
            "nested_claim_selected"
                | "nested_claim_actual_selected"
                | "nested_claim_actual_selected_kill_parent"
                | "selection_hook_fence"
                | "selection_hook_test_dispatch"
                | "selection_hook_writer_busy"
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
                | "selection_hook_woken"
                | "selection_hook_woken_live"
                | "selection_hook_woken_schema"
        );

    let mut extensions = crate::extension::Registry::default();
    if selected {
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "policy".into(),
                    hooks: vec![if case == "selection_hook_woken_schema" {
                        Arc::new(SchemaClaimHook(ClaimPassHook(
                            Some(after_calls.clone()),
                            true,
                        ))) as Arc<dyn crate::extension::Hook>
                    } else {
                        Arc::new(ClaimPassHook(
                            Some(after_calls.clone()),
                            matches!(
                                case,
                                "nested_claim_selected"
                                    | "nested_claim_actual_selected"
                                    | "nested_claim_actual_selected_kill_parent"
                                    | "selection_hook_after_memo"
                                    | "selection_hook_memo_conflict"
                                    | "selection_hook_kill"
                                    | "selection_hook_woken"
                                    | "selection_hook_woken_live"
                            ),
                        ))
                    }],
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
    let deep_bottom = if case.starts_with("nested_claim_actual_deep") {
        Some(
            register_conversation_replay_safe_durable_tool(
                &mut registry,
                Arc::new(AsyncTool {
                    name: "bottom",
                    child: None,
                    gate: Some(gate.clone()),
                    early: false,
                }),
                "bottom",
                1,
                vec![],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap(),
        )
    } else {
        None
    };
    let grandchild = if case.starts_with("nested_claim_actual_deep") {
        let tool = Arc::new(AsyncTool {
            name: "grandchild",
            child: Some("bottom"),
            gate: None,
            early: false,
        });
        let identity_tool: Arc<dyn Tool> = tool.clone();
        let identity = pi_rs_durable::nested_identity::register(
            &identity_tool,
            Some("grandchild-backend".into()),
        )
        .unwrap();
        Some(pi_rs_durable::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(
            &mut registry, tool, "grandchild", 1, vec![deep_bottom.as_ref().unwrap().clone()], "cancel-v1",
            Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity, callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        ).unwrap())
    } else if multilevel && selected {
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
    let leaf_spec_changed = Arc::new(AtomicBool::new(false));
    let leaf_base = AsyncTool {
        name: "leaf",
        child: multilevel.then_some("grandchild"),
        gate: if multilevel { None } else { Some(gate.clone()) },
        early: false,
    };
    let leaf_tool: Arc<dyn DurableAsyncTool> = if case == "nested_claim_actual_busy_spec" {
        Arc::new(MutableSpecTool {
            inner: leaf_base,
            changed: leaf_spec_changed.clone(),
        })
    } else {
        Arc::new(leaf_base)
    };
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
    let leaf = if selected && (case == "nested_claim_selected" || case == "nested_claim_actual_selected" || case == "nested_claim_actual_selected_kill_parent") {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(
            &mut registry, leaf_tool, "leaf", 1, grandchild.clone().into_iter().collect(), &snapshot, &selection_names,
            "cancel-v1", Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), leaf_identity.clone().unwrap(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    } else if case == "nested_claim_success" || case == "nested_claim_success_late" || case == "nested_claim_parent" || case == "nested_claim_actual" || case == "nested_claim_actual_kill" || case == "nested_claim_actual_signal" || case == "nested_claim_actual_late" || case.starts_with("nested_claim_actual_busy") || case == "nested_claim_actual_selected_kill_parent" || case.starts_with("nested_claim_actual_deep") {
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
    let root = if matches!(case, "selection_hook_woken" | "selection_hook_woken_live" | "selection_hook_woken_schema") {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_woken_cancel(
            &mut registry, root_tool, "root", 1, vec![leaf.clone()], &snapshot, &selection_names,
            "cancel-v1", Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity_handle.clone(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
        )
    } else if selected && case != "selection_hook_fence" {
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
    } else if matches!(case, "dispatch_woken_claim" | "dispatch_woken_live_abort") {
        pi_rs_durable::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel(
            &mut registry, root_tool, "root", 1, vec![leaf.clone()], "cancel-v1",
            Arc::new(Cancel(calls.clone(), case.to_owned(), path.clone(), identity_handle.clone(), callback_scheduler.clone())),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    } else if case.starts_with("dispatch_") || case == "nested_claim_parent" || case == "nested_claim_actual" || case.starts_with("nested_claim_actual_busy") || case == "nested_claim_actual_selected_kill_parent" || case.starts_with("nested_claim_actual_deep") {
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
    if let Some(bottom) = &deep_bottom {
        registered_tools.push(bottom.register());
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
    if matches!(
        case,
        "selection_hook_output_schema" | "selection_hook_woken_schema"
    ) {
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
        if case != "nested_claim_actual"
            && case != "nested_claim_actual_selected"
            && case != "nested_claim_actual_selected_kill_parent"
            && case != "nested_claim_actual_kill"
            && case != "nested_claim_actual_signal"
            && case != "nested_claim_actual_late"
            && !case.starts_with("nested_claim_actual_busy")
            && !case.starts_with("nested_claim_actual_deep")
        {
            crate::task_scheduler::enable_test_nested_claim(child_id);
        }
        let (busy_tx, busy_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        if case.starts_with("nested_claim_actual_busy") {
            let identity = leaf_identity.as_ref().unwrap().clone();
            let changed = leaf_spec_changed.clone();
            let change_spec = case == "nested_claim_actual_busy_spec";
            crate::task_scheduler::on_nested_preflight_for_test(root_id, move || {
                let (held_tx, held_rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    rt.block_on(identity.update(Some("leaf-backend".into()), || {
                        if change_spec {
                            changed.store(true, Ordering::SeqCst);
                        }
                        held_tx.send(()).unwrap();
                        busy_tx.send(()).unwrap();
                        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                        if change_spec {
                            changed.store(false, Ordering::SeqCst);
                        }
                    }))
                    .unwrap();
                });
                held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
        }
        if case == "nested_claim_callback"
            || case == "nested_claim_kill"
            || case == "nested_claim_late_kill"
            || case == "nested_claim_selected"
            || case == "nested_claim_success"
            || case == "nested_claim_success_late"
            || case == "nested_claim_parent"
        {
            crate::task_scheduler::enable_test_nested_callback(child_id);
        }
        if case == "nested_claim_success"
            || case == "nested_claim_success_late"
            || case == "nested_claim_parent"
        {
            crate::task_scheduler::enable_test_nested_success(child_id);
        }
        if case == "nested_claim_actual_deep_late_tamper" {
            let probe_path = path.clone();
            crate::task_scheduler::on_conversation_claim_after_commit(root_id, move || {
                let store = durable_store::Store::open(probe_path).unwrap();
                let tasks = crate::task_store::TaskStore::new(&store);
                let grandchild = tasks
                    .list()
                    .unwrap()
                    .into_iter()
                    .find(|item| {
                        item.task.owner() == &pi_rs_durable::task::TaskOwner::Task(child_id)
                    })
                    .unwrap();
                let key = format!(
                    "durable/replay-safe-async/{}/nested-cancel-terminal-v1",
                    grandchild.task.id()
                )
                .into_bytes();
                let entry = store.get(&key).unwrap().unwrap();
                let mut value: Value = serde_json::from_slice(&entry.value).unwrap();
                value["claim"]["claim_id"] = json!(uuid::Uuid::new_v4());
                store
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
            });
        }
        if case == "nested_claim_actual_deep_tamper" {
            let probe_path = path.clone();
            crate::task_scheduler::on_conversation_cancel_after_settlement(child_id, move || {
                let store = durable_store::Store::open(probe_path).unwrap();
                let key = format!("durable/replay-safe-async/{child_id}/nested-cancel-terminal-v1")
                    .into_bytes();
                let entry = store.get(&key).unwrap().unwrap();
                let mut value: Value = serde_json::from_slice(&entry.value).unwrap();
                value["claim"]["claim_id"] = json!(uuid::Uuid::new_v4());
                store
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
            });
        }
        let (proof_tx, proof_rx) = std::sync::mpsc::channel();
        if case == "nested_claim_success" {
            let harness = scheduler.subagent_harness().worker();
            let probe_registry = recovery_registry.clone();
            let probe_path = path.clone();
            crate::task_scheduler::on_conversation_cancel_after_settlement(child_id, move || {
                let result = (|| -> Result<(), String> {
                    let gate = harness.checkpointable_extension_gate();
                    let _selection = gate.lock().map_err(|e| e.to_string())?;
                    let store =
                        durable_store::Store::open(&probe_path).map_err(|e| e.to_string())?;
                    let records = crate::task_store::TaskStore::new(&store)
                        .list()
                        .map_err(|e| e.to_string())?;
                    let (unresolved, checks, leases) =
                        crate::task_scheduler::authenticated_unsafe_descendant_reconciliation(
                            &store,
                            root_id,
                            &records,
                            store.put_version().map_err(|e| e.to_string())?,
                            &probe_registry,
                            &harness,
                            None,
                        )
                        .map_err(|e| e.to_string())?;
                    if unresolved.is_some() || leases.len() != 1 {
                        return Err(format!(
                            "unexpected nested proof: {unresolved:?}, {} leases",
                            leases.len()
                        ));
                    }
                    store.commit(&checks, &[]).map_err(|e| e.to_string())?;
                    Ok(())
                })();
                let _ = proof_tx.send(result);
            });
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
        if matches!(
            case,
            "nested_claim_signal"
                | "nested_claim_late"
                | "nested_claim_actual_signal"
                | "nested_claim_actual_late"
        ) {
            let probe_path = path.clone();
            let signal_race = case == "nested_claim_signal" || case == "nested_claim_actual_signal";
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
        if case == "nested_claim_actual_selected_kill_parent"
            && std::env::var_os("PI_TEST_NESTED_CHILD_RECEIPT").is_some()
        {
            let probe_path = path.clone();
            let directory = std::path::PathBuf::from(
                std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
            );
            crate::task_scheduler::on_conversation_receipt_saved(child_id, move || {
                let store = durable_store::Store::open(probe_path).unwrap();
                let key =
                    format!("durable/replay-safe-async/{child_id}/conversation-park-cancel-claim");
                let claim = store.get(key.as_bytes()).unwrap().unwrap();
                std::fs::write(directory.join("ready.tmp"), claim.value).unwrap();
                std::fs::rename(directory.join("ready.tmp"), directory.join("ready")).unwrap();
                loop {
                    std::thread::park();
                }
            });
        }
        if case == "nested_claim_actual_selected_kill_parent" {
            let probe_path = path.clone();
            let directory = std::path::PathBuf::from(
                std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR").unwrap(),
            );
            crate::task_scheduler::on_conversation_receipt_saved(root_id, move || {
                let store = durable_store::Store::open(probe_path).unwrap();
                let key =
                    format!("durable/replay-safe-async/{root_id}/conversation-park-cancel-claim");
                let claim = store.get(key.as_bytes()).unwrap().unwrap();
                std::fs::write(directory.join("ready"), claim.value).unwrap();
                loop {
                    std::thread::park();
                }
            });
        }
        if case.starts_with("nested_claim_actual_deep") {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if tasks.list().unwrap().iter().any(|item| {
                        item.task.owner() == &pi_rs_durable::task::TaskOwner::Task(child_id)
                            && matches!(item.task.state(), TaskState::Waiting(_))
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        if late_publication {
            let probe_path = path.clone();
            crate::task_scheduler::on_conversation_cancel_after_settlement(root_id, move || {
                let store = durable_store::Store::open(probe_path).unwrap();
                let prefix = format!("durable/unsafe-park-async/{child_id}/");
                let (mut key, entry) = store
                    .list(prefix.as_bytes())
                    .unwrap()
                    .into_iter()
                    .find(|(key, _)| key.ends_with(b"/intent"))
                    .unwrap();
                let intent: crate::task::Task = serde_json::from_slice(&entry.value).unwrap();
                let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                    task_id: child_id,
                    attempt_id: intent.attempt_id().unwrap(),
                    outcome: Outcome::Pending {
                        reference: "late-publication".into(),
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
            });
        }
        if case == "nested_claim_parent"
            || case == "nested_claim_actual"
            || case == "nested_claim_actual_selected"
            || case == "nested_claim_actual_selected_kill_parent"
            || case.starts_with("nested_claim_actual_busy")
            || case.starts_with("nested_claim_actual_deep")
        {
            scheduler.abort_task(root_id).unwrap();
        } else {
            scheduler.abort_task(child_id).unwrap();
        }
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
        if matches!(
            case,
            "nested_claim_signal"
                | "nested_claim_late"
                | "nested_claim_actual_signal"
                | "nested_claim_actual_late"
        ) {
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
        if !matches!(
            case,
            "nested_claim_parent"
                | "nested_claim_actual"
                | "nested_claim_actual_selected"
                | "nested_claim_actual_selected_kill_parent"
                | "nested_claim_actual_busy"
                | "nested_claim_actual_busy_spec"
                | "nested_claim_actual_deep"
                | "nested_claim_actual_deep_tamper"
                | "nested_claim_actual_deep_late_tamper"
        ) {
            assert!(
                store
                    .get(format!("durable/task-callback-start/{root_id}").as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
        if case == "nested_claim_parent"
            || case == "nested_claim_actual"
            || case == "nested_claim_actual_selected"
            || case == "nested_claim_actual_selected_kill_parent"
            || case.starts_with("nested_claim_actual_busy")
            || case.starts_with("nested_claim_actual_deep")
        {
            if case.starts_with("nested_claim_actual_busy") {
                busy_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    1,
                    "parent callback ran while child identity was busy"
                );
                assert_eq!(
                    tasks.load(root_id).unwrap().unwrap().task.state(),
                    &TaskState::Ready
                );
                let independent_owner = scheduler
                    .create_conversation(Config {
                        model: "test".into(),
                        system_prompt: "".into(),
                        max_output_tokens: 100,
                    })
                    .unwrap();
                let independent = crate::task::Task::new(
                    uuid::Uuid::new_v4(),
                    crate::task::TaskOwner::Conversation(independent_owner),
                    crate::task::Checkpoint {
                        phase: "initial".into(),
                        data: json!({}),
                    },
                    3,
                )
                .unwrap()
                .registered("independent-maintenance".into(), true);
                driver.submit(&independent).unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if driver.get(independent.id()).unwrap().unwrap().is_terminal() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("independent task stalled while identity was busy");
                assert_eq!(
                    driver.get(independent.id()).unwrap().unwrap().state(),
                    &TaskState::Succeeded(json!("done"))
                );
                release_tx.send(()).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                tasks.load(child_id).unwrap().unwrap().task.state(),
                &TaskState::Cancelled
            );
            if matches!(
                case,
                "nested_claim_actual_deep_tamper" | "nested_claim_actual_deep_late_tamper"
            ) {
                assert!(
                    matches!(tasks.load(root_id).unwrap().unwrap().task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
                );
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            } else {
                assert_eq!(
                    tasks.load(root_id).unwrap().unwrap().task.state(),
                    &TaskState::Cancelled
                );
                assert_eq!(
                    calls.load(Ordering::SeqCst),
                    if case.starts_with("nested_claim_actual_deep") {
                        3
                    } else {
                        2
                    }
                );
            }
            if case == "nested_claim_actual_selected" {
                scheduler.schedule(conversation);
                let result = scheduler.wait(conversation).await;
                if late_publication {
                    assert!(
                        result.is_err(),
                        "late child outcome must prevent after publication"
                    );
                } else {
                    result.unwrap();
                    let state = scheduler.conversation(conversation).unwrap();
                    assert!(format!("{state:?}").contains("after cancel"), "{state:?}");
                }
            }
            let _ = driver.shutdown_with_pending().unwrap();
            return;
        }
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
            proof_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
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
            let boundary = store.put_version().unwrap();
            let (checks, _identity) = crate::task_scheduler::nested_cancel_terminal_checks(
                &store,
                &terminal,
                &recovery_registry,
                boundary,
                None,
            )
            .unwrap();
            assert!(
                leaf_identity
                    .as_ref()
                    .unwrap()
                    .update(Some("changed".into()), || {})
                    .await
                    .is_err()
            );
            let prefix = format!("durable/unsafe-park-async/{child_id}/");
            let (mut key, entry) = store
                .list(prefix.as_bytes())
                .unwrap()
                .into_iter()
                .find(|(key, _)| key.ends_with(b"/intent"))
                .unwrap();
            let intent: pi_rs_durable::task::Task = serde_json::from_slice(&entry.value).unwrap();
            let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                task_id: child_id,
                attempt_id: intent.attempt_id().unwrap(),
                outcome: Outcome::Pending {
                    reference: "late-after-terminal".into(),
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
            assert!(matches!(
                store.commit(&checks, &[]),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(
                crate::task_scheduler::nested_cancel_terminal_checks(
                    &store,
                    &terminal,
                    &recovery_registry,
                    store.put_version().unwrap(),
                    None,
                )
                .is_err()
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
    if matches!(
        case,
        "dispatch_woken_claim" | "selection_hook_woken" | "selection_hook_woken_schema"
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !gate.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        gate.store(false, Ordering::SeqCst);
        std::future::pending::<()>().await;
        unreachable!();
    }
    if matches!(
        case,
        "dispatch_woken_live_abort" | "selection_hook_woken_live"
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !gate.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        gate.store(false, Ordering::SeqCst);
        let dir = std::path::PathBuf::from(std::env::var_os("PI_TEST_WOKEN_LIVE_DIR").unwrap());
        tokio::time::timeout(Duration::from_secs(10), async {
            while !dir.join("wake").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let abort_result = tokio::time::timeout(
            Duration::from_secs(10),
            scheduler.abort_and_wait(conversation),
        )
        .await
        .unwrap();
        assert!(
            abort_result.is_err(),
            "the interrupted outcome still requires reconciliation"
        );
        let current = driver.get(root_id).unwrap().unwrap();
        std::fs::write(
            dir.join("live-result"),
            serde_json::to_vec(&(calls.load(Ordering::SeqCst), current)).unwrap(),
        )
        .unwrap();
        if case == "selection_hook_woken_live" {
            std::fs::write(
                dir.join("selected-live-result"),
                format!(
                    "{} {:?}",
                    after_calls.load(Ordering::SeqCst),
                    scheduler.conversation(conversation)
                ),
            )
            .unwrap();
        }
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
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
    if matches!(
        case,
        "selection_hook_test_dispatch"
            | "selection_hook_writer_busy"
            | "selection_hook_output_schema"
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
        if case == "selection_hook_writer_busy" && graph_aba {
            let path = path.clone();
            crate::task_scheduler::on_conversation_receipt_before_commit(root_id, move || {
                let store = durable_store::Store::open(path).unwrap();
                let child = crate::task::Task::new(
                    uuid::Uuid::new_v4(),
                    crate::task::TaskOwner::Task(root_id),
                    crate::task::Checkpoint {
                        phase: "initial".into(),
                        data: json!({}),
                    },
                    3,
                )
                .unwrap();
                let tasks = crate::task_store::TaskStore::new(&store);
                // Simulate an older writer bypassing current admission checks.
                let key = crate::task_store::key(child.id());
                store
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
                let ready = tasks.load(child.id()).unwrap().unwrap();
                tasks.update(&ready, |task| task.start()).unwrap();
                let running = tasks.load(child.id()).unwrap().unwrap();
                tasks
                    .update(&running, |task| task.succeed(json!("external work")))
                    .unwrap();
            });
        }
        if case == "selection_hook_writer_busy" && !graph_aba {
            let path = path.clone();
            crate::task_scheduler::on_conversation_receipt_before_commit(root_id, move || {
                let (ready_tx, ready_rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || {
                    let conn = rusqlite::Connection::open(path).unwrap();
                    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                    ready_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_secs(6));
                    conn.execute_batch("ROLLBACK").unwrap();
                });
                ready_rx.recv().unwrap();
            });
        }
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
        if case == "selection_hook_writer_busy" {
            tokio::time::timeout(Duration::from_secs(10), async {
                while after_calls.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let report = driver.shutdown_with_pending().unwrap();
            let known = report
                .pending()
                .iter()
                .filter_map(|p| p.completed())
                .filter_map(|c| c.known_conversation_cancel())
                .next()
                .expect("known cancel result retained");
            assert_eq!(
                known.commit_state(),
                if graph_aba {
                    crate::task_scheduler::KnownCancelCommitState::Stale
                } else {
                    crate::task_scheduler::KnownCancelCommitState::Busy
                }
            );
            assert!(known.after_result().is_some());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(after_calls.load(Ordering::SeqCst), 1);
            assert!(store.get(key.as_bytes()).unwrap().is_none());
            return;
        }
        if matches!(
            case,
            "selection_hook_marker_conflict" | "selection_hook_snapshot_conflict"
        ) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while after_calls.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let report = driver.shutdown_with_pending().unwrap();
            let known = report
                .pending()
                .iter()
                .filter_map(|p| p.completed())
                .filter_map(|c| c.known_conversation_cancel())
                .next()
                .expect("known result retained");
            assert!(known.after_result().is_some());
            assert!(known.has_snapshot_check());
            assert_eq!(
                known.commit_state(),
                crate::task_scheduler::KnownCancelCommitState::Stale
            );
            assert!(known.identity().is_some());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(after_calls.load(Ordering::SeqCst), 1);
            assert!(store.get(key.as_bytes()).unwrap().is_none());
            return;
        }
        if case == "selection_hook_memo_conflict" {
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
        assert_eq!(
            receipt.version,
            2,
            "task: {:?}",
            tasks.load(root_id).unwrap().unwrap().task.state()
        );
        assert_eq!(
            receipt.after_result.as_ref().unwrap()["task_content_v1"]["output"][0]["text"],
            "after cancel"
        );
        if case == "selection_hook_output_schema" {
            assert!(
                receipt.after_result.as_ref().unwrap()["task_content_v1"]["structuredOutput"]
                    .is_null()
            );
        }
        crate::async_task_tool::validate_aborted_after_result(
            receipt.after_result.as_ref().unwrap(),
        )
        .unwrap();
        let published = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let conversation = scheduler.conversation(conversation).unwrap();
                if conversation.messages.iter().any(|message| matches!(message, Message::Tool { content, .. } if content.contains("after cancel"))) {
                    assert!(conversation.tool_results[0].is_error);
                    if case == "selection_hook_output_schema" {
                        assert!(conversation.tool_results[0].structured_output.is_none());
                    }
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
            tokio::time::timeout(Duration::from_secs(10), async {
                while !finished.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
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
            std::fs::write(directory.join("ready.tmp"), claim.value).unwrap();
            std::fs::rename(directory.join("ready.tmp"), directory.join("ready")).unwrap();
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
async fn selected_parked_child_graph_aba_is_retained_without_retry() {
    exercise_conversation_park_claim_with_late_publication(
        "selection_hook_writer_busy",
        false,
        true,
    )
    .await;
}

#[tokio::test]
async fn selected_hook_busy_receipt_is_retained_without_retry() {
    exercise_conversation_park_claim("selection_hook_writer_busy").await;
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
async fn selected_hook_output_schema_allows_cancel_and_after_result() {
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
async fn woken_conversation_claim_worker() {
    if std::env::var_os("PI_TEST_WOKEN_CLAIM_DIR").is_some() {
        exercise_conversation_park_claim("dispatch_woken_claim").await;
    }
}

#[tokio::test]
async fn woken_conversation_claim_binds_interrupted_attempt_and_late_receipt() {
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let mut process = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::woken_conversation_claim_worker",
        ])
        .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let ready = tokio::time::timeout(Duration::from_secs(20), async {
        while !dir.path().join("wake").exists() {
            if let Some(status) = process.try_wait().unwrap() {
                panic!("worker exited: {status}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if ready.is_err() {
        let _ = process.kill();
        let _ = process.wait();
        panic!("wake timed out");
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let store = durable_store::Store::open(dir.path().join("db")).unwrap();
    let tasks = crate::task_store::TaskStore::new(&store).list().unwrap();
    let mut root = tasks
        .iter()
        .find(|r| {
            matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                && r.task.state() == &TaskState::Running
        })
        .unwrap()
        .task
        .clone();
    let original = root.attempt_id().unwrap();
    root.recover_interrupted_woken_for_cleanup().unwrap();
    root.start_attempt(uuid::Uuid::new_v4()).unwrap();
    let lease = uuid::Uuid::new_v4();
    let (claim, checks) = crate::task_scheduler::prepare_woken_conversation_cancel_claim(
        &store,
        &root,
        lease,
        crate::async_task_tool::ConversationCancelPolicy::AtMostOnce,
        store.put_version().unwrap(),
    )
    .unwrap();
    assert_eq!(claim.woken_attempt, original);
    assert_eq!(claim.cleanup_attempt, root.attempt_id().unwrap());
    assert_ne!(claim.cleanup_attempt, claim.woken_attempt);
    assert_eq!(claim.cleanup_lease, lease);
    let claim_key = crate::task_scheduler::woken_conversation_claim_key(root.id());
    let mut claim_checks = checks.clone();
    claim_checks.push(durable_store::Check {
        key: claim_key.clone(),
        expected: durable_store::Expected::Missing,
    });
    store
        .commit(
            &claim_checks,
            &[durable_store::Mutation::Put {
                key: claim_key,
                value: serde_json::to_vec(&claim).unwrap(),
            }],
        )
        .unwrap();
    let rechecked = crate::task_scheduler::woken_conversation_claim_checks(
        &store,
        &root,
        &claim,
        lease,
        store.put_version().unwrap(),
    )
    .unwrap();
    let mut foreign = claim.clone();
    foreign.woken_attempt = uuid::Uuid::new_v4();
    assert!(
        crate::task_scheduler::woken_conversation_claim_checks(
            &store,
            &root,
            &foreign,
            lease,
            store.put_version().unwrap()
        )
        .is_err()
    );
    store.commit(&checks, &[]).unwrap();
    store.commit(&rechecked, &[]).unwrap();
    let key = format!(
        "durable/unsafe-park-async/{}/{original}/intent/receipt",
        root.id()
    )
    .into_bytes();
    let receipt = crate::async_task_tool::UnsafeDurableReceipt {
        task_id: root.id(),
        attempt_id: original,
        outcome: Outcome::Pending {
            reference: "late".into(),
        },
    };
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
    assert!(matches!(
        store.commit(&checks, &[]),
        Err(durable_store::Error::Conflict { .. })
    ));
    assert!(matches!(
        store.commit(&rechecked, &[]),
        Err(durable_store::Error::Conflict { .. })
    ));
    assert!(
        crate::task_scheduler::woken_conversation_claim_checks(
            &store,
            &root,
            &claim,
            lease,
            store.put_version().unwrap()
        )
        .is_err()
    );
    assert!(
        crate::task_scheduler::prepare_woken_conversation_cancel_claim(
            &store,
            &root,
            lease,
            crate::async_task_tool::ConversationCancelPolicy::AtMostOnce,
            store.put_version().unwrap()
        )
        .is_err()
    );
}

#[tokio::test]
async fn interrupted_woken_claim_and_markers_reconcile_through_driver() {
    use crate::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel,
    };
    use std::process::{Command, Stdio};
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
    let mut process = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::woken_conversation_claim_worker",
        ])
        .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !dir.path().join("wake").exists() {
            assert!(process.try_wait().unwrap().is_none(), "worker exited");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    process.kill().unwrap();
    process.wait().unwrap();
    let path = dir.path().join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = crate::task_store::TaskStore::new(&store);
    let snapshot = tasks
        .list()
        .unwrap()
        .into_iter()
        .find(|r| {
            matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                && r.task.state() == &TaskState::Running
        })
        .unwrap();
    let mut task = snapshot.task.clone();
    task.recover_interrupted_woken_for_cleanup().unwrap();
    task.start_attempt(uuid::Uuid::new_v4()).unwrap();
    tasks
        .update(&snapshot, |t| {
            *t = task.clone();
            Ok(())
        })
        .unwrap();
    let (claim, mut checks) = crate::task_scheduler::prepare_woken_conversation_cancel_claim(
        &store,
        &task,
        uuid::Uuid::new_v4(),
        crate::async_task_tool::ConversationCancelPolicy::AtMostOnce,
        store.put_version().unwrap(),
    )
    .unwrap();
    let claim_key = crate::task_scheduler::woken_conversation_claim_key(task.id());
    let task_key = format!("durable/task-callback-start/{}", task.id()).into_bytes();
    let conversation_key =
        format!("runtime/conversation-callback-start/{}", claim.conversation).into_bytes();
    let marker = serde_json::to_vec(&json!({
        "version": 3, "claim_id": claim.claim_id, "task": claim.task,
        "woken_attempt": claim.woken_attempt, "attempt": claim.cleanup_attempt,
        "lease": claim.cleanup_lease, "policy": claim.policy,
    }))
    .unwrap();
    for key in [&claim_key, &task_key, &conversation_key] {
        checks.push(durable_store::Check {
            key: key.clone(),
            expected: durable_store::Expected::Missing,
        });
    }
    store
        .commit(
            &checks,
            &[
                durable_store::Mutation::Put {
                    key: claim_key.clone(),
                    value: serde_json::to_vec(&claim).unwrap(),
                },
                durable_store::Mutation::Put {
                    key: task_key.clone(),
                    value: marker.clone(),
                },
                durable_store::Mutation::Put {
                    key: conversation_key.clone(),
                    value: marker,
                },
            ],
        )
        .unwrap();
    drop(store);
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
    let identity: Arc<dyn Tool> = tool.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let root = register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel(
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
    let harness = crate::runtime::Harness::open_paused(&path, options).unwrap();
    let scheduler = harness.attach_tasks(registry).unwrap();
    let driver = crate::task_driver::Driver::start_with_harness(scheduler, harness.worker());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let current = driver.get(task.id()).unwrap().unwrap();
            if let TaskState::Failed(reason) = current.state() {
                assert!(reason.contains("reconciliation"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let store = durable_store::Store::open(&path).unwrap();
    assert!(store.get(&claim_key).unwrap().is_some());
    assert!(store.get(&task_key).unwrap().is_none());
    assert!(store.get(&conversation_key).unwrap().is_none());
    let report = driver.shutdown_with_pending().unwrap();
    assert!(report.failure().is_none());
}

fn start_woken_test_driver(
    path: &std::path::Path,
) -> (crate::runtime::Harness, crate::task_driver::Driver) {
    use crate::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel,
    };
    struct Cancel(std::path::PathBuf);
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, context: ToolContext) -> Result<(), String> {
            let path = &self.0;
            use std::io::Write;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path.join("woken-callback-count"))
                .unwrap()
                .write_all(b"x")
                .unwrap();
            std::fs::write(path.join("woken-callback.tmp"), b"called").unwrap();
            std::fs::rename(path.join("woken-callback.tmp"), path.join("woken-callback")).unwrap();
            match std::fs::read_to_string(path.join("woken-mode"))
                .unwrap_or_default()
                .as_str()
            {
                "error" => return Err("callback failure".into()),
                "panic" => panic!("callback panic"),
                "hold-file" => {
                    while !path.join("release-callback").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                "late" => {
                    let store = durable_store::Store::open(path.join("db")).unwrap();
                    let task = crate::task_store::TaskStore::new(&store)
                        .load(context.operation_id)
                        .unwrap()
                        .unwrap()
                        .task;
                    let attempt = task.woken_interrupted_attempt().unwrap();
                    let key = format!(
                        "durable/unsafe-park-async/{}/{attempt}/intent/receipt",
                        task.id()
                    )
                    .into_bytes();
                    let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                        task_id: task.id(),
                        attempt_id: attempt,
                        outcome: Outcome::Pending {
                            reference: "late".into(),
                        },
                    };
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
                _ => (),
            }
            if std::env::var_os("PI_TEST_WOKEN_HOLD_CALLBACK").is_some() {
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }
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
    let identity: Arc<dyn Tool> = tool.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let root = register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel(
        &mut registry,
        tool,
        "root",
        1,
        vec![leaf.clone()],
        "cancel-v1",
        Arc::new(Cancel(path.parent().unwrap().to_owned())),
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
    let harness = crate::runtime::Harness::open_paused(path, options).unwrap();
    let scheduler = harness.attach_tasks(registry).unwrap();
    let driver = crate::task_driver::Driver::start_with_harness(scheduler, harness.worker());
    (harness, driver)
}

#[tokio::test]
async fn woken_driver_start_marker_kill_worker() {
    if let Some(dir) = std::env::var_os("PI_TEST_WOKEN_START_DIR") {
        let path = std::path::PathBuf::from(&dir).join("db");
        if std::env::var_os("PI_TEST_WOKEN_LATE_RECEIPT").is_some() {
            let ids: (uuid::Uuid, uuid::Uuid) = serde_json::from_slice(
                &std::fs::read(std::path::Path::new(&dir).join("root-attempt")).unwrap(),
            )
            .unwrap();
            let path2 = path.clone();
            let dir2 = std::path::PathBuf::from(&dir);
            crate::task_scheduler::before_woken_recovery_commit_for_test(ids.0, move || {
                let store = durable_store::Store::open(path2).unwrap();
                let key = format!(
                    "durable/unsafe-park-async/{}/{}/intent/receipt",
                    ids.0, ids.1
                )
                .into_bytes();
                let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                    task_id: ids.0,
                    attempt_id: ids.1,
                    outcome: Outcome::Pending {
                        reference: "late".into(),
                    },
                };
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
                std::fs::write(dir2.join("woken-late"), b"written").unwrap();
            });
        }
        let (_harness, _driver) = start_woken_test_driver(&path);
        std::future::pending::<()>().await;
    }
}

#[tokio::test]
async fn woken_driver_claim_and_start_markers_survive_process_kill() {
    use std::process::{Command, Stdio};
    for mode in [
        "start",
        "callback",
        "receipt",
        "memo",
        "memo-receipt",
        "memo-late",
        "memo-snapshot",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut wake = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_conversation_claim_worker",
            ])
            .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("wake").exists() {
                assert!(wake.try_wait().unwrap().is_none(), "wake worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        wake.kill().unwrap();
        wake.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = crate::task_store::TaskStore::new(&store);
        let root = tasks
            .list()
            .unwrap()
            .into_iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap();
        // The worker's bound Harness performs the recovery transition.
        drop(store);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_driver_start_marker_kill_worker",
            ])
            .env("PI_TEST_WOKEN_START_DIR", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if mode != "start" {
            command.env("PI_TEST_WOKEN_DISPATCH", "1");
        }
        if mode == "callback" {
            command.env("PI_TEST_WOKEN_HOLD_CALLBACK", "1");
        }
        if mode.starts_with("memo") {
            command.env("PI_TEST_WOKEN_MEMO", "1");
        }
        let mut start = command.spawn().unwrap();
        let ready = match mode {
            "start" => "woken-start",
            "callback" => "woken-callback",
            _ => "woken-receipt",
        };
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join(ready).exists() {
                assert!(start.try_wait().unwrap().is_none(), "start worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        start.kill().unwrap();
        start.wait().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let claim_key = crate::task_scheduler::woken_conversation_claim_key(root.task.id());
        let claim: crate::task_scheduler::WokenConversationCancelClaim =
            serde_json::from_slice(&store.get(&claim_key).unwrap().unwrap().value).unwrap();
        let task_key = format!("durable/task-callback-start/{}", root.task.id()).into_bytes();
        let conversation_key =
            format!("runtime/conversation-callback-start/{}", claim.conversation).into_bytes();
        let receipt_key = format!(
            "durable/replay-safe-async/{}/conversation-woken-cancel-receipt-v1",
            root.task.id()
        )
        .into_bytes();
        if mode == "receipt" || mode.starts_with("memo") {
            assert!(store.get(&task_key).unwrap().is_none());
            assert!(store.get(&conversation_key).unwrap().is_none());
            assert!(store.get(&receipt_key).unwrap().is_some());
        } else {
            assert_eq!(
                store.get(&task_key).unwrap().unwrap().value,
                std::fs::read(dir.path().join("woken-start")).unwrap()
            );
            assert_eq!(
                store.get(&task_key).unwrap().unwrap().value,
                store.get(&conversation_key).unwrap().unwrap().value
            );
            assert!(store.get(&receipt_key).unwrap().is_none());
        }
        if mode.starts_with("memo") {
            let tasks = crate::task_store::TaskStore::new(&store);
            let running = tasks.load(root.task.id()).unwrap().unwrap();
            let mut terminal = running.clone();
            terminal
                .task
                .fail("interrupted woken execution; reconciliation required".into())
                .unwrap();
            let receipt_entry = store.get(&receipt_key).unwrap().unwrap();
            let marker = crate::task_scheduler::WokenConversationCancelTerminalMarker {
                version: 1,
                claim_id: claim.claim_id,
                cleanup_attempt: claim.cleanup_attempt,
                woken_attempt: claim.woken_attempt,
                running_task_version: running.version,
                terminal_task: terminal.task.clone(),
                receipt_version: receipt_entry.version,
            };
            let marker_key = crate::task_scheduler::woken_conversation_terminal_key(root.task.id());
            let mut batch = tasks
                .prepare_save(&[crate::task_store::Save::Update(&terminal)])
                .unwrap();
            batch.checks.push(durable_store::Check {
                key: marker_key.clone(),
                expected: durable_store::Expected::Missing,
            });
            batch.mutations.push(durable_store::Mutation::Put {
                key: marker_key.clone(),
                value: serde_json::to_vec(&marker).unwrap(),
            });
            store.commit(&batch.checks, &batch.mutations).unwrap();
            let finished = tasks.load(root.task.id()).unwrap().unwrap();
            let (result, checks) = crate::task_scheduler::woken_terminal_after_checks(
                &store,
                &finished,
                store.put_version().unwrap(),
            )
            .unwrap();
            assert!(result.is_object());
            store.commit(&checks, &[]).unwrap();
            match mode {
                "memo" => {
                    let mut foreign = marker.clone();
                    foreign.woken_attempt = uuid::Uuid::new_v4();
                    let entry = store.get(&marker_key).unwrap().unwrap();
                    store
                        .commit(
                            &[durable_store::Check {
                                key: marker_key.clone(),
                                expected: durable_store::Expected::Version(entry.version),
                            }],
                            &[durable_store::Mutation::Put {
                                key: marker_key.clone(),
                                value: serde_json::to_vec(&foreign).unwrap(),
                            }],
                        )
                        .unwrap();
                    assert!(
                        crate::task_scheduler::woken_terminal_after_checks(
                            &store,
                            &finished,
                            store.put_version().unwrap()
                        )
                        .is_err()
                    );
                }
                "memo-receipt" => {
                    let entry = store.get(&receipt_key).unwrap().unwrap();
                    store
                        .commit(
                            &[durable_store::Check {
                                key: receipt_key.clone(),
                                expected: durable_store::Expected::Version(entry.version),
                            }],
                            &[durable_store::Mutation::Put {
                                key: receipt_key.clone(),
                                value: entry.value,
                            }],
                        )
                        .unwrap();
                    assert!(
                        crate::task_scheduler::woken_terminal_after_checks(
                            &store,
                            &finished,
                            store.put_version().unwrap()
                        )
                        .is_err()
                    );
                }
                "memo-late" => {
                    let key = format!(
                        "durable/unsafe-park-async/{}/{}/intent/receipt",
                        root.task.id(),
                        claim.woken_attempt
                    )
                    .into_bytes();
                    let late = crate::async_task_tool::UnsafeDurableReceipt {
                        task_id: root.task.id(),
                        attempt_id: claim.woken_attempt,
                        outcome: Outcome::Pending {
                            reference: "late".into(),
                        },
                    };
                    store
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
                    assert!(
                        crate::task_scheduler::woken_terminal_after_checks(
                            &store,
                            &finished,
                            store.put_version().unwrap()
                        )
                        .is_err()
                    );
                }
                "memo-snapshot" => {
                    let key = crate::runtime::Harness::key(claim.conversation);
                    let entry = store.get(&key).unwrap().unwrap();
                    store
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
                    let (_, stale_checks) = crate::task_scheduler::woken_terminal_after_checks(
                        &store,
                        &finished,
                        store.put_version().unwrap(),
                    )
                    .unwrap();
                    assert!(matches!(
                        store.commit(&stale_checks, &[]),
                        Err(durable_store::Error::Conflict { .. })
                    ));
                }
                _ => unreachable!(),
            }
        }
        drop(store);
        let (_harness, driver) = start_woken_test_driver(&path);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = driver.get(root.task.id()).unwrap().unwrap();
                if let TaskState::Failed(reason) = current.state() {
                    assert!(reason.contains("reconciliation"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(store.get(&claim_key).unwrap().is_some());
        assert!(store.get(&task_key).unwrap().is_none());
        assert!(store.get(&conversation_key).unwrap().is_none());
        assert_eq!(
            store.get(&receipt_key).unwrap().is_some(),
            mode == "receipt" || mode.starts_with("memo")
        );
        if mode.starts_with("memo") {
            let key = format!("runtime/extension-memo/{}/woken-test", root.task.id());
            assert_eq!(
                serde_json::from_slice::<Value>(&store.get(key.as_bytes()).unwrap().unwrap().value)
                    .unwrap(),
                json!("saved")
            );
            let receipt: crate::task_scheduler::WokenConversationCancelReceipt =
                serde_json::from_slice(&store.get(&receipt_key).unwrap().unwrap().value).unwrap();
            assert_eq!(receipt.version, 2);
            assert!(receipt.after_result.is_some());
        }
        assert_eq!(dir.path().join("woken-callback").exists(), mode != "start");
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }
}

#[tokio::test]
async fn woken_driver_late_receipt_before_recovery_blocks_callback() {
    use std::process::{Command, Stdio};
    for mode in ["late"] {
        let dir = tempfile::tempdir().unwrap();
        let mut wake = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_conversation_claim_worker",
            ])
            .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("wake").exists() {
                assert!(wake.try_wait().unwrap().is_none(), "wake worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        wake.kill().unwrap();
        wake.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = crate::task_store::TaskStore::new(&store);
        let root = tasks
            .list()
            .unwrap()
            .into_iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap();
        std::fs::write(
            dir.path().join("root-attempt"),
            serde_json::to_vec(&(root.task.id(), root.task.attempt_id().unwrap())).unwrap(),
        )
        .unwrap();
        drop(store);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_driver_start_marker_kill_worker",
            ])
            .env("PI_TEST_WOKEN_START_DIR", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.env("PI_TEST_WOKEN_LATE_RECEIPT", "1");
        if mode != "start" {
            command.env("PI_TEST_WOKEN_DISPATCH", "1");
        }
        if mode == "callback" {
            command.env("PI_TEST_WOKEN_HOLD_CALLBACK", "1");
        }
        let mut start = command.spawn().unwrap();
        let ready = match mode {
            "late" => "woken-late",
            "start" => "woken-start",
            "callback" => "woken-callback",
            _ => "woken-receipt",
        };
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join(ready).exists() {
                assert!(start.try_wait().unwrap().is_none(), "start worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        start.kill().unwrap();
        start.wait().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let claim_key = crate::task_scheduler::woken_conversation_claim_key(root.task.id());
        let task_key = format!("durable/task-callback-start/{}", root.task.id()).into_bytes();
        let conversation = match root.task.owner() {
            crate::task::TaskOwner::Conversation(id) => *id,
            _ => unreachable!(),
        };
        let conversation_key =
            format!("runtime/conversation-callback-start/{conversation}").into_bytes();
        let receipt_key = format!(
            "durable/unsafe-park-async/{}/{}/intent/receipt",
            root.task.id(),
            root.task.attempt_id().unwrap()
        )
        .into_bytes();
        assert!(store.get(&claim_key).unwrap().is_none());
        assert!(store.get(&receipt_key).unwrap().is_some());
        assert!(store.get(&task_key).unwrap().is_none());
        assert!(store.get(&conversation_key).unwrap().is_none());
        drop(store);
        let (_harness, driver) = start_woken_test_driver(&path);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = driver.get(root.task.id()).unwrap().unwrap();
                if let TaskState::Failed(reason) = current.state() {
                    assert!(reason.contains("reconciliation"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(store.get(&claim_key).unwrap().is_none());
        assert!(store.get(&task_key).unwrap().is_none());
        assert!(store.get(&conversation_key).unwrap().is_none());
        assert!(store.get(&receipt_key).unwrap().is_some());
        assert!(!dir.path().join("woken-callback").exists());
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }
}

#[tokio::test]
async fn opted_in_woken_conversation_cancel_runs_once_and_keeps_outcome_uncertain() {
    use std::process::{Command, Stdio};
    for mode in ["success", "error", "panic", "late"] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("woken-mode"), mode).unwrap();
        let mut wake = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_conversation_claim_worker",
            ])
            .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("wake").exists() {
                assert!(wake.try_wait().unwrap().is_none(), "wake worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        wake.kill().unwrap();
        wake.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let root = crate::task_store::TaskStore::new(&store)
            .list()
            .unwrap()
            .into_iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap();
        drop(store);
        let (_harness, driver) = start_woken_test_driver(&path);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = driver.get(root.task.id()).unwrap().unwrap();
                if let TaskState::Failed(reason) = current.state() {
                    assert!(reason.contains("reconciliation"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("woken-callback-count")).unwrap(),
            b"x"
        );
        let store = durable_store::Store::open(&path).unwrap();
        let claim_key = crate::task_scheduler::woken_conversation_claim_key(root.task.id());
        let receipt_key = format!(
            "durable/replay-safe-async/{}/conversation-woken-cancel-receipt-v1",
            root.task.id()
        )
        .into_bytes();
        if mode == "success" {
            let receipt: crate::task_scheduler::WokenConversationCancelReceipt =
                serde_json::from_slice(&store.get(&receipt_key).unwrap().unwrap().value).unwrap();
            assert_eq!(receipt.claim.woken_attempt, root.task.attempt_id().unwrap());
            assert_eq!(receipt.result, "callback returned");
        } else {
            assert!(store.get(&receipt_key).unwrap().is_none());
            let unknown_key = format!(
                "durable/replay-safe-async/{}/conversation-woken-cancel-unknown-v1",
                root.task.id()
            );
            assert!(store.get(unknown_key.as_bytes()).unwrap().is_some());
            if mode == "late" {
                let late_key = format!(
                    "durable/unsafe-park-async/{}/{}/intent/receipt",
                    root.task.id(),
                    root.task.attempt_id().unwrap()
                );
                assert!(store.get(late_key.as_bytes()).unwrap().is_some());
            }
        }
        assert!(store.get(&claim_key).unwrap().is_some());
        assert!(
            store
                .get(format!("durable/task-callback-start/{}", root.task.id()).as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }
}

#[tokio::test]
async fn woken_production_callback_and_receipt_survive_process_kill() {
    use std::process::{Command, Stdio};
    for mode in ["callback", "receipt"] {
        let dir = tempfile::tempdir().unwrap();
        let mut wake = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_conversation_claim_worker",
            ])
            .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("wake").exists() {
                assert!(wake.try_wait().unwrap().is_none(), "wake worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        wake.kill().unwrap();
        wake.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let tasks = crate::task_store::TaskStore::new(&store);
        let root = tasks
            .list()
            .unwrap()
            .into_iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap();
        // The worker's bound Harness performs the recovery transition.
        drop(store);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_driver_start_marker_kill_worker",
            ])
            .env("PI_TEST_WOKEN_START_DIR", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.env("PI_TEST_WOKEN_REAL", "1");
        if mode == "receipt" {
            command.env("PI_TEST_WOKEN_HOLD_AFTER_RECEIPT", dir.path());
        }
        if mode != "start" {
            command.env("PI_TEST_WOKEN_DISPATCH", "1");
        }
        if mode == "callback" {
            command.env("PI_TEST_WOKEN_HOLD_CALLBACK", "1");
        }
        let mut start = command.spawn().unwrap();
        let ready = match mode {
            "start" => "woken-start",
            "callback" => "woken-callback",
            _ => "woken-receipt",
        };
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join(ready).exists() {
                assert!(start.try_wait().unwrap().is_none(), "start worker exited");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        start.kill().unwrap();
        start.wait().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let claim_key = crate::task_scheduler::woken_conversation_claim_key(root.task.id());
        let claim: crate::task_scheduler::WokenConversationCancelClaim =
            serde_json::from_slice(&store.get(&claim_key).unwrap().unwrap().value).unwrap();
        let task_key = format!("durable/task-callback-start/{}", root.task.id()).into_bytes();
        let conversation_key =
            format!("runtime/conversation-callback-start/{}", claim.conversation).into_bytes();
        let receipt_key = format!(
            "durable/replay-safe-async/{}/conversation-woken-cancel-receipt-v1",
            root.task.id()
        )
        .into_bytes();
        if mode == "receipt" {
            assert!(store.get(&task_key).unwrap().is_none());
            assert!(store.get(&conversation_key).unwrap().is_none());
            assert!(store.get(&receipt_key).unwrap().is_some());
        } else {
            assert_eq!(
                store.get(&task_key).unwrap().unwrap().value,
                store.get(&conversation_key).unwrap().unwrap().value
            );
            assert!(store.get(&receipt_key).unwrap().is_none());
        }
        drop(store);
        let (_harness, driver) = start_woken_test_driver(&path);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = driver.get(root.task.id()).unwrap().unwrap();
                if let TaskState::Failed(reason) = current.state() {
                    assert!(reason.contains("reconciliation"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(store.get(&claim_key).unwrap().is_some());
        assert!(store.get(&task_key).unwrap().is_none());
        assert!(store.get(&conversation_key).unwrap().is_none());
        assert_eq!(
            store.get(&receipt_key).unwrap().is_some(),
            mode == "receipt"
        );
        assert_eq!(dir.path().join("woken-callback").exists(), mode != "start");
        assert_eq!(
            std::fs::read(dir.path().join("woken-callback-count")).unwrap(),
            b"x"
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }
}

#[tokio::test]
async fn woken_live_abort_worker() {
    if std::env::var_os("PI_TEST_WOKEN_LIVE_DIR").is_some() {
        let case = if std::env::var_os("PI_TEST_SELECTED_WOKEN_LIVE").is_some() {
            "selection_hook_woken_live"
        } else {
            "dispatch_woken_live_abort"
        };
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn opted_in_woken_live_abort_invokes_cancel_once() {
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let status = tokio::task::spawn_blocking({
        let dir = dir.path().to_owned();
        move || {
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "conversation_claim_boundary_tests::woken_live_abort_worker",
                    "--nocapture",
                ])
                .env("PI_TEST_WOKEN_LIVE_DIR", &dir)
                .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", &dir)
                .env("PI_CONV_ASYNC_KILL_WAKE", dir.join("wake"))
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .unwrap()
        }
    })
    .await
    .unwrap();
    assert!(status.success(), "live abort worker failed");
    let (calls, task): (usize, crate::task::Task) =
        serde_json::from_slice(&std::fs::read(dir.path().join("live-result")).unwrap()).unwrap();
    assert_eq!(
        calls,
        1,
        "woken cancel must run once; state {:?}",
        task.state()
    );
}

#[tokio::test]
async fn woken_cancel_shutdown_joins_callback_and_preserves_receipt() {
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("woken-mode"), "hold-file").unwrap();
    let mut wake = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::woken_conversation_claim_worker",
        ])
        .env("PI_TEST_WOKEN_CLAIM_DIR", dir.path())
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !dir.path().join("wake").exists() {
            assert!(wake.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wake.kill().unwrap();
    wake.wait().unwrap();
    let path = dir.path().join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let root = crate::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .find(|r| {
            matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                && r.task.state() == &TaskState::Running
        })
        .unwrap();
    drop(store);
    let (_harness, driver) = start_woken_test_driver(&path);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !dir.path().join("woken-callback").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let shutdown =
        std::thread::spawn(move || done_tx.send(driver.shutdown_with_pending()).unwrap());
    assert!(
        done_rx.recv_timeout(Duration::from_millis(150)).is_err(),
        "shutdown must join the callback"
    );
    std::fs::write(dir.path().join("release-callback"), b"release").unwrap();
    let report = done_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    assert!(report.failure().is_none());
    shutdown.join().unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let receipt_key = format!(
        "durable/replay-safe-async/{}/conversation-woken-cancel-receipt-v1",
        root.task.id()
    );
    assert!(store.get(receipt_key.as_bytes()).unwrap().is_some());
    assert_eq!(
        std::fs::read(dir.path().join("woken-callback-count")).unwrap(),
        b"x"
    );
}

#[tokio::test]
async fn selected_woken_original_worker() {
    if std::env::var_os("PI_TEST_SELECTED_WOKEN_DIR").is_some() {
        let case = if std::env::var_os("PI_TEST_SELECTED_WOKEN_SCHEMA").is_some() {
            "selection_hook_woken_schema"
        } else {
            "selection_hook_woken"
        };
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn selected_woken_after_is_retained_without_resolving_uncertain_effect() {
    exercise_selected_woken_receipt("normal").await;
}

#[tokio::test]
async fn selected_woken_busy_receipt_is_retained_without_retry() {
    exercise_selected_woken_receipt("busy").await;
}

#[tokio::test]
async fn selected_woken_child_graph_aba_is_retained_without_retry() {
    exercise_selected_woken_receipt("graph_aba").await;
}

#[tokio::test]
async fn selected_woken_conflicting_snapshot_retains_known_result() {
    exercise_selected_woken_receipt("stale").await;
}

#[tokio::test]
async fn selected_woken_missing_snapshot_fails_closed_during_settlement() {
    exercise_selected_woken_receipt("missing_snapshot").await;
}

async fn exercise_selected_woken_receipt(mode: &str) {
    use crate::async_task_tool::DurableParkCancel;
    use std::process::{Command, Stdio};
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
    let mut wake = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::selected_woken_original_worker",
        ])
        .env("PI_TEST_SELECTED_WOKEN_DIR", dir.path())
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
        .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !dir.path().join("wake").exists() {
            assert!(
                wake.try_wait().unwrap().is_none(),
                "initial selected worker exited"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wake.kill().unwrap();
    wake.wait().unwrap();
    let path = dir.path().join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let root = crate::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .find(|r| {
            matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                && r.task.state() == &TaskState::Running
        })
        .unwrap();
    let conversation = match root.task.owner() {
        crate::task::TaskOwner::Conversation(id) => *id,
        _ => unreachable!(),
    };
    drop(store);
    let after_calls = Arc::new(AtomicUsize::new(0));
    let mut extensions = crate::extension::Registry::default();
    extensions
        .install_identified(
            crate::extension::Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(ClaimPassHook(Some(after_calls.clone()), true))],
                ..Default::default()
            },
            crate::extension::ExtensionIdentity::new(1, "policy-v1".into(), vec!["hook-v1".into()])
                .unwrap(),
        )
        .unwrap();
    let snapshot = extensions.snapshot();
    let names = vec!["policy".to_owned()];
    let mut registry = Registry::default();
    assert!(
        registry.register("independent-safe", true, |_: &crate::task::Task| Ok(
            crate::task_scheduler::Step::Succeed(json!("independent"))
        ))
    );
    let leaf = crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry, Arc::new(AsyncTool {name:"leaf", child:None, gate:None, early:false}), "leaf", 1, vec![], &snapshot, &names,
        ReplaySafeAuthorization::host_attests_replay_safe(), crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    ).unwrap();
    let tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity: Arc<dyn Tool> = tool.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let cancel_calls = Arc::new(AtomicUsize::new(0));
    let root_tool = crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_woken_cancel(
        &mut registry,
        tool,
        "root",
        1,
        vec![leaf.clone()],
        &snapshot,
        &names,
        "cancel-v1",
        Arc::new(Cancel(cancel_calls.clone())),
        UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    )
    .unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root_tool.register(), leaf.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let independent_conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    let busy_released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let busy_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if mode == "busy" {
        let writer_path = path.clone();
        let busy_ready = busy_ready.clone();
        let busy_released = busy_released.clone();
        crate::task_scheduler::on_conversation_receipt_before_commit(root.task.id(), move || {
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let conn = rusqlite::Connection::open(writer_path).unwrap();
                conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                busy_ready.store(true, Ordering::SeqCst);
                ready_tx.send(()).unwrap();
                std::thread::sleep(Duration::from_secs(6));
                conn.execute_batch("ROLLBACK").unwrap();
                busy_released.store(true, Ordering::SeqCst);
            });
            ready_rx.recv().unwrap();
        });
    }
    if mode == "graph_aba" {
        let writer_path = path.clone();
        let parent = root.task.id();
        crate::task_scheduler::on_conversation_receipt_before_commit(parent, move || {
            let store = durable_store::Store::open(writer_path).unwrap();
            let child = crate::task::Task::new(
                uuid::Uuid::new_v4(),
                crate::task::TaskOwner::Task(parent),
                crate::task::Checkpoint {
                    phase: "initial".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap();
            let tasks = crate::task_store::TaskStore::new(&store);
            // Simulate an older writer bypassing current admission checks.
            let key = crate::task_store::key(child.id());
            store
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
            let ready = tasks.load(child.id()).unwrap().unwrap();
            tasks.update(&ready, |task| task.start()).unwrap();
            let running = tasks.load(child.id()).unwrap().unwrap();
            tasks
                .update(&running, |task| task.succeed(json!("external work")))
                .unwrap();
        });
    }
    if mode == "stale" {
        let writer_path = path.clone();
        crate::task_scheduler::on_conversation_receipt_before_commit(root.task.id(), move || {
            let writer = durable_store::Store::open(writer_path).unwrap();
            let key = format!("runtime/conversation/{conversation}").into_bytes();
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
    if mode == "missing_snapshot" {
        let writer_path = path.clone();
        let task_id = root.task.id();
        crate::task_scheduler::on_conversation_receipt_saved(task_id, move || {
            let store = durable_store::Store::open(writer_path).unwrap();
            let key = crate::task_scheduler::woken_conversation_receipt_key(task_id);
            let entry = store.get(&key).unwrap().unwrap();
            let mut receipt: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            receipt.as_object_mut().unwrap().remove("snapshot_version");
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&receipt).unwrap(),
                    }],
                )
                .unwrap();
        });
    }
    let busy_store = durable_store::Store::open(&path).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    if mode == "busy" {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !busy_ready.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let store = &busy_store;
        assert!(
            store
                .get(&crate::task_scheduler::woken_conversation_receipt_key(
                    root.task.id()
                ))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(format!("durable/task-callback-start/{}", root.task.id()).as_bytes())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get(format!("runtime/conversation-callback-start/{conversation}").as_bytes())
                .unwrap()
                .is_some()
        );
    }
    if mode == "busy" {
        tokio::time::timeout(Duration::from_secs(15), async {
            while !busy_released.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let independent = crate::task::Task::new(
            uuid::Uuid::new_v4(),
            crate::task::TaskOwner::Conversation(independent_conversation),
            crate::task::Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("independent-safe".into(), true);
        driver.submit(&independent).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !driver.get(independent.id()).unwrap().unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            driver.get(independent.id()).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("independent"))
        );
        let report = driver.shutdown_with_pending().unwrap();
        let known = report
            .pending()
            .iter()
            .filter_map(|p| p.completed())
            .filter_map(|c| c.known_woken_cancel())
            .next()
            .expect("known woken result retained");
        assert_eq!(
            known.commit_state(),
            crate::task_scheduler::KnownCancelCommitState::Busy
        );
        assert!(known.after_result().is_some());
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(after_calls.load(Ordering::SeqCst), 1);
        return;
    }
    if mode == "stale" || mode == "graph_aba" {
        tokio::time::timeout(Duration::from_secs(10), async {
            while after_calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        let known = report
            .pending()
            .iter()
            .filter_map(|p| p.completed())
            .filter_map(|c| c.known_woken_cancel())
            .next()
            .expect("known woken result retained");
        assert_eq!(
            known.commit_state(),
            crate::task_scheduler::KnownCancelCommitState::Stale
        );
        assert!(known.after_result().is_some());
        assert!(known.has_snapshot_check());
        assert!(known.identity().is_some());
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            store
                .get(&crate::task_scheduler::woken_conversation_receipt_key(
                    root.task.id()
                ))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(format!("durable/task-callback-start/{}", root.task.id()).as_bytes())
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get(format!("runtime/conversation-callback-start/{conversation}").as_bytes())
                .unwrap()
                .is_some()
        );
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(after_calls.load(Ordering::SeqCst), 1);
        return;
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let current = driver.get(root.task.id()).unwrap().unwrap();
            if matches!(current.state(), TaskState::Failed(_)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
    assert_eq!(after_calls.load(Ordering::SeqCst), 1);
    let store = durable_store::Store::open(&path).unwrap();
    if mode == "busy" {
        assert!(
            store
                .get(&crate::task_scheduler::woken_conversation_receipt_key(
                    root.task.id()
                ))
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .get(format!("durable/task-callback-start/{}", root.task.id()).as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(format!("runtime/conversation-callback-start/{conversation}").as_bytes())
                .unwrap()
                .is_none()
        );
    }
    if mode == "missing_snapshot" {
        assert!(
            store
                .get(&crate::task_scheduler::woken_conversation_terminal_key(
                    root.task.id()
                ))
                .unwrap()
                .is_none()
        );
        assert_eq!(cancel_calls.load(Ordering::SeqCst), 1);
        assert_eq!(after_calls.load(Ordering::SeqCst), 1);
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        return;
    }
    let terminal = store
        .get(&crate::task_scheduler::woken_conversation_terminal_key(
            root.task.id(),
        ))
        .unwrap();
    assert!(
        terminal.is_some(),
        "selected after needs atomic terminal evidence"
    );
    let memo = format!("runtime/extension-memo/{}/abort-memo", root.task.id());
    assert!(store.get(memo.as_bytes()).unwrap().is_some());
    // The supplemental output must never make the uncertain Tool outcome known.
    scheduler.schedule(conversation);
    scheduler.wait(conversation).await.unwrap();
    let state = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = scheduler.conversation(conversation).unwrap();
            if format!("{state:?}").contains("reconciliation") {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("selected state: {:?}", scheduler.conversation(conversation)));
    assert!(format!("{state:?}").contains("after cancel"));
    assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
}

fn start_selected_woken_test_scheduler(
    path: &std::path::Path,
    fingerprint: &str,
) -> (Scheduler, crate::task_driver::Driver) {
    use crate::async_task_tool::DurableParkCancel;
    use std::sync::atomic::AtomicUsize;
    struct Cancel(std::path::PathBuf);
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            use std::io::Write;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.0.join("selected-cancel-count"))
                .unwrap()
                .write_all(b"x")
                .unwrap();
            Ok(())
        }
    }
    let mut extensions = crate::extension::Registry::default();
    extensions
        .install_identified(
            crate::extension::Extension {
                name: "policy".into(),
                hooks: vec![if std::env::var("PI_TEST_SELECTED_SCHEMA_RICH")
                    .ok()
                    .as_deref()
                    == Some("1")
                {
                    Arc::new(SchemaClaimHook(ClaimPassHook(
                        Some(Arc::new(AtomicUsize::new(0))),
                        true,
                    ))) as Arc<dyn crate::extension::Hook>
                } else {
                    Arc::new(ClaimPassHook(Some(Arc::new(AtomicUsize::new(0))), true))
                }],
                ..Default::default()
            },
            crate::extension::ExtensionIdentity::new(1, fingerprint.into(), vec!["hook-v1".into()])
                .unwrap(),
        )
        .unwrap();
    let snapshot = extensions.snapshot();
    let names = vec!["policy".to_owned()];
    let mut registry = Registry::default();
    let leaf = crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(
        &mut registry, Arc::new(AsyncTool {name:"leaf", child:None, gate:None, early:false}), "leaf", 1, vec![], &snapshot, &names,
        ReplaySafeAuthorization::host_attests_replay_safe(), crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    ).unwrap();
    let tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let identity: Arc<dyn Tool> = tool.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let root = crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_woken_cancel(
        &mut registry, tool, "root", 1, vec![leaf.clone()], &snapshot, &names, "cancel-v1", Arc::new(Cancel(path.parent().unwrap().to_owned())),
        UnsafeDurableAuthorization::interruption_requires_reconciliation(), crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe(),
    ).unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root.register(), leaf.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let scheduler = Scheduler::open_with_extensions(path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    (scheduler, driver)
}

#[tokio::test]
async fn selected_woken_resume_worker() {
    if let Some(dir) = std::env::var_os("PI_TEST_SELECTED_RESUME_DIR") {
        let fingerprint =
            std::env::var("PI_TEST_SELECTED_FINGERPRINT").unwrap_or_else(|_| "policy-v1".into());
        let (_scheduler, _driver) = start_selected_woken_test_scheduler(
            &std::path::Path::new(&dir).join("db"),
            &fingerprint,
        );
        std::future::pending::<()>().await;
    }
}

#[tokio::test]
async fn selected_woken_after_receipt_kill_reopens_blocked_with_result() {
    use std::process::{Command, Stdio};
    for mode in ["normal", "late", "selection", "schema", "missing_snapshot"] {
        let dir = tempfile::tempdir().unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "conversation_claim_boundary_tests::selected_woken_original_worker",
            ])
            .env("PI_TEST_SELECTED_WOKEN_DIR", dir.path())
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
            .env("PI_CONV_ASYNC_KILL_WAKE", dir.path().join("wake"))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if mode == "schema" {
            command.env("PI_TEST_SELECTED_WOKEN_SCHEMA", "1");
        }
        let mut wake = command.spawn().unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("wake").exists() {
                assert!(wake.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        wake.kill().unwrap();
        wake.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let root = crate::task_store::TaskStore::new(&store)
            .list()
            .unwrap()
            .into_iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap();
        let conversation = match root.task.owner() {
            crate::task::TaskOwner::Conversation(id) => *id,
            _ => unreachable!(),
        };
        drop(store);
        let mut resume = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::selected_woken_resume_worker",
            ])
            .env("PI_TEST_SELECTED_RESUME_DIR", dir.path())
            .env("PI_TEST_SELECTED_RECORD_AFTER", dir.path())
            .env("PI_TEST_WOKEN_HOLD_AFTER_RECEIPT", dir.path())
            .env(
                "PI_TEST_SELECTED_SCHEMA_RICH",
                if mode == "schema" { "1" } else { "0" },
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("woken-receipt").exists() {
                assert!(
                    resume.try_wait().unwrap().is_none(),
                    "selected resume exited"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        resume.kill().unwrap();
        resume.wait().unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("selected-cancel-count")).unwrap(),
            b"x"
        );
        assert_eq!(
            std::fs::read(dir.path().join("selected-after-count")).unwrap(),
            b"x"
        );
        if mode == "missing_snapshot" {
            let store = durable_store::Store::open(&path).unwrap();
            let key = crate::task_scheduler::woken_conversation_receipt_key(root.task.id());
            let entry = store.get(&key).unwrap().unwrap();
            let mut receipt: serde_json::Value = serde_json::from_slice(&entry.value).unwrap();
            receipt.as_object_mut().unwrap().remove("snapshot_version");
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: serde_json::to_vec(&receipt).unwrap(),
                    }],
                )
                .unwrap();
        }
        if mode == "late" {
            let store = durable_store::Store::open(&path).unwrap();
            let key = format!(
                "durable/unsafe-park-async/{}/{}/intent/receipt",
                root.task.id(),
                root.task.attempt_id().unwrap()
            )
            .into_bytes();
            let receipt = crate::async_task_tool::UnsafeDurableReceipt {
                task_id: root.task.id(),
                attempt_id: root.task.attempt_id().unwrap(),
                outcome: Outcome::Pending {
                    reference: "late".into(),
                },
            };
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
        if mode == "selection" {
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "conversation_claim_boundary_tests::selected_woken_resume_worker",
                ])
                .env("PI_TEST_SELECTED_RESUME_DIR", dir.path())
                .env("PI_TEST_SELECTED_FINGERPRINT", "policy-v2")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert!(!status.success(), "changed selection must fail closed");
            assert_eq!(
                std::fs::read(dir.path().join("selected-cancel-count")).unwrap(),
                b"x"
            );
            assert_eq!(
                std::fs::read(dir.path().join("selected-after-count")).unwrap(),
                b"x"
            );
        }
        let (mut scheduler, driver) = start_selected_woken_test_scheduler(&path, "policy-v1");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if matches!(
                    driver.get(root.task.id()).unwrap().unwrap().state(),
                    TaskState::Failed(_)
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        scheduler.schedule(conversation);
        scheduler.wait(conversation).await.unwrap();
        let state = scheduler.conversation(conversation).unwrap();
        let text = format!("{state:?}");
        assert!(text.contains("reconciliation"), "{text}");
        if mode == "schema" {
            assert_eq!(state.tool_results.len(), 1, "{text}");
            assert!(state.tool_results[0].structured_output.is_none());
        }
        assert_eq!(
            text.contains("after cancel"),
            mode != "late" && mode != "missing_snapshot",
            "{text}"
        );
        let store = durable_store::Store::open(&path).unwrap();
        assert_eq!(
            store
                .get(&crate::task_scheduler::woken_conversation_terminal_key(
                    root.task.id()
                ))
                .unwrap()
                .is_some(),
            mode != "late" && mode != "missing_snapshot"
        );
        assert_eq!(
            std::fs::read(dir.path().join("selected-cancel-count")).unwrap(),
            b"x"
        );
        assert_eq!(
            std::fs::read(dir.path().join("selected-after-count")).unwrap(),
            b"x"
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn selected_woken_live_abort_runs_after_but_stays_blocked() {
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let status = tokio::task::spawn_blocking({
        let dir = dir.path().to_owned();
        move || {
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "conversation_claim_boundary_tests::woken_live_abort_worker",
                    "--nocapture",
                ])
                .env("PI_TEST_WOKEN_LIVE_DIR", &dir)
                .env("PI_TEST_SELECTED_WOKEN_LIVE", "1")
                .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", &dir)
                .env("PI_CONV_ASYNC_KILL_WAKE", dir.join("wake"))
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .unwrap()
        }
    })
    .await
    .unwrap();
    assert!(status.success());
    let (calls, task): (usize, crate::task::Task) =
        serde_json::from_slice(&std::fs::read(dir.path().join("live-result")).unwrap()).unwrap();
    assert_eq!(calls, 1);
    assert!(matches!(task.state(), TaskState::Failed(_)));
    let value = std::fs::read_to_string(dir.path().join("selected-live-result")).unwrap();
    assert!(
        value.starts_with("1 ")
            && value.contains("after cancel")
            && value.contains("reconciliation"),
        "{value}"
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
        } else if std::env::var_os("PI_TEST_NESTED_CALLBACK_ACTUAL").is_some() {
            "nested_claim_actual_kill"
        } else {
            "nested_claim_kill"
        };
        exercise_conversation_park_claim(case).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn nested_callback_kill_preserves_claim_and_ancestor_markers() {
    for (late, actual) in [(false, false), (true, false), (false, true)] {
        exercise_nested_callback_kill(late, actual).await;
    }
}

#[cfg(unix)]
async fn exercise_nested_callback_kill(late: bool, actual: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    if late {
        command.env("PI_TEST_NESTED_CALLBACK_LATE", "1");
    }
    if actual {
        command.env("PI_TEST_NESTED_CALLBACK_ACTUAL", "1");
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
    let leaf = if actual {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(
            &mut registry, leaf_tool, "leaf", 1, vec![grandchild.clone()], "cancel-v1",
            Arc::new(NeverCancel(calls.clone())), UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    } else {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_park_cancel(
            &mut registry, leaf_tool, "leaf", 1, vec![grandchild.clone()], "cancel-v1",
            Arc::new(NeverCancel(calls.clone())), UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
    }.unwrap();
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

#[tokio::test]
async fn nested_cancel_success_allows_parent_cancel() {
    exercise_conversation_park_claim("nested_claim_parent").await;
}

#[tokio::test]
async fn nested_production_cancel_deep_rechecks_evidence_before_parent_callback() {
    exercise_conversation_park_claim("nested_claim_actual_deep_late_tamper").await;
}

#[tokio::test]
async fn nested_production_cancel_deep_rejects_replaced_ancestor_evidence() {
    exercise_conversation_park_claim("nested_claim_actual_deep_tamper").await;
}

#[tokio::test]
async fn nested_production_cancel_deep_unsafe_chain() {
    exercise_conversation_park_claim("nested_claim_actual_deep").await;
}

#[tokio::test]
async fn nested_production_cancel_allows_parent_cancel() {
    exercise_conversation_park_claim("nested_claim_actual").await;
}

#[tokio::test]
async fn nested_selected_production_cancel_allows_parent_cancel() {
    exercise_conversation_park_claim("nested_claim_actual_selected").await;
}

#[tokio::test]
async fn nested_selected_late_child_receipt_prevents_after_publication() {
    exercise_conversation_park_claim_with_late_publication(
        "nested_claim_actual_selected",
        true,
        false,
    )
    .await;
}

#[tokio::test]
async fn nested_production_cancel_rejects_signal_and_late_receipt() {
    for case in ["nested_claim_actual_signal", "nested_claim_actual_late"] {
        exercise_conversation_park_claim(case).await;
    }
}

#[tokio::test]
async fn nested_selected_parent_kill_worker() {
    if std::env::var_os("PI_TEST_NESTED_PARENT_KILL").is_some() {
        exercise_conversation_park_claim("nested_claim_actual_selected_kill_parent").await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn nested_parent_receipt_survives_kill_and_reopen() {
    for mode in ["good", "late"] {
        exercise_nested_parent_receipt_recovery(mode, false).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn nested_child_receipt_survives_kill_and_reopen() {
    for mode in [
        "good",
        "late",
        "replace",
        "snapshot",
        "unhooked",
        "unhooked_late",
    ] {
        exercise_nested_parent_receipt_recovery(mode, true).await;
    }
}

#[cfg(unix)]
async fn exercise_nested_parent_receipt_recovery(mode: &str, child_cut: bool) {
    let unhooked = mode.starts_with("unhooked");
    let late = mode == "late" || mode == "unhooked_late";
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Cancel(Arc<AtomicUsize>);
    #[async_trait]
    impl crate::async_task_tool::DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, _: Value, _: ToolContext) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "conversation_claim_boundary_tests::nested_selected_parent_kill_worker",
        ])
        .env("PI_TEST_NESTED_PARENT_KILL", "1")
        .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path());
    if child_cut {
        command.env("PI_TEST_NESTED_CHILD_RECEIPT", "1");
    }
    if unhooked {
        command.env("PI_TEST_NESTED_UNHOOKED", "1");
    }
    let mut process = command.spawn().unwrap();
    let claim: crate::task_scheduler::ConversationCancelClaim =
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(value) = std::fs::read(dir.path().join("ready")) {
                    if let Ok(claim) = serde_json::from_slice(&value) {
                        break claim;
                    }
                }
                assert!(
                    process.try_wait().unwrap().is_none(),
                    "worker exited before receipt"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    process.kill().unwrap();
    process.wait().unwrap();
    let path = dir.path().join("db");
    let store = durable_store::Store::open(&path).unwrap();
    let children = crate::task_store::TaskStore::new(&store).list().unwrap();
    let (root_id, child) = if child_cut {
        let child = children
            .iter()
            .find(|item| item.task.id() == claim.task)
            .unwrap();
        let crate::task::TaskOwner::Task(root_id) = child.task.owner() else {
            panic!("missing parent")
        };
        (*root_id, child)
    } else {
        let child = children
            .iter()
            .find(|item| item.task.owner() == &crate::task::TaskOwner::Task(claim.task))
            .unwrap();
        (claim.task, child)
    };
    if child_cut {
        assert_eq!(child.task.state(), &TaskState::Running);
    } else {
        assert_eq!(child.task.state(), &TaskState::Cancelled);
    }
    let child_id = child.task.id();
    if mode == "replace" || mode == "snapshot" {
        let key = if mode == "replace" {
            format!("durable/replay-safe-async/{child_id}/conversation-park-cancel-receipt")
                .into_bytes()
        } else {
            let root = children
                .iter()
                .find(|item| item.task.id() == root_id)
                .unwrap();
            let crate::task::TaskOwner::Conversation(id) = root.task.owner() else {
                panic!("missing conversation")
            };
            format!("runtime/conversation/{id}").into_bytes()
        };
        let entry = store.get(&key).unwrap().unwrap();
        // Even an identical rewrite must invalidate the immutable evidence.
        store
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
    }
    if late {
        let prefix = format!("durable/unsafe-park-async/{child_id}/");
        let (mut key, entry) = store
            .list(prefix.as_bytes())
            .unwrap()
            .into_iter()
            .find(|(key, _)| key.ends_with(b"/intent"))
            .unwrap();
        let intent: crate::task::Task = serde_json::from_slice(&entry.value).unwrap();
        let receipt = crate::async_task_tool::UnsafeDurableReceipt {
            task_id: child_id,
            attempt_id: intent.attempt_id().unwrap(),
            outcome: Outcome::Pending {
                reference: "late-after-parent-receipt".into(),
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
    if !child_cut {
        assert!(
            store
                .get(
                    format!(
                        "durable/replay-safe-async/{}/nested-cancel-terminal-v1",
                        child.task.id()
                    )
                    .as_bytes()
                )
                .unwrap()
                .is_some()
        );
    }
    assert!(
        store
            .get(
                format!(
                    "durable/replay-safe-async/{}/conversation-park-cancel-receipt",
                    claim.task
                )
                .as_bytes()
            )
            .unwrap()
            .is_some()
    );
    drop(store);

    let mut extensions = crate::extension::Registry::default();
    extensions
        .install_identified(
            crate::extension::Extension {
                name: "policy".into(),
                hooks: vec![Arc::new(ClaimPassHook(None, false))],
                ..Default::default()
            },
            crate::extension::ExtensionIdentity::new(1, "policy-v1".into(), vec!["hook-v1".into()])
                .unwrap(),
        )
        .unwrap();
    let snapshot = extensions.snapshot();
    let names = vec!["policy".to_owned()];
    let hooks = crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::default();
    let grandchild_tool = Arc::new(AsyncTool {
        name: "grandchild",
        child: None,
        gate: None,
        early: false,
    });
    let grandchild = if unhooked {
        register_conversation_replay_safe_durable_tool(
            &mut registry,
            grandchild_tool,
            "grandchild",
            1,
            vec![],
            ReplaySafeAuthorization::host_attests_replay_safe(),
        )
    } else {
        crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(
            &mut registry,
            grandchild_tool,
            "grandchild",
            1,
            vec![],
            &snapshot,
            &names,
            ReplaySafeAuthorization::host_attests_replay_safe(),
            hooks(),
        )
    }
    .unwrap();
    let leaf_tool = Arc::new(AsyncTool {
        name: "leaf",
        child: Some("grandchild"),
        gate: None,
        early: false,
    });
    let leaf_identity_tool: Arc<dyn Tool> = leaf_tool.clone();
    let leaf_identity =
        crate::nested_identity::register(&leaf_identity_tool, Some("leaf-backend".into())).unwrap();
    let leaf = if unhooked {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(&mut registry, leaf_tool, "leaf", 1, vec![grandchild.clone()], "cancel-v1", Arc::new(Cancel(calls.clone())), UnsafeDurableAuthorization::interruption_requires_reconciliation())
    } else {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(&mut registry, leaf_tool, "leaf", 1, vec![grandchild.clone()], &snapshot, &names, "cancel-v1", Arc::new(Cancel(calls.clone())), UnsafeDurableAuthorization::interruption_requires_reconciliation(), hooks())
    }.unwrap();
    let root_tool = Arc::new(AsyncTool {
        name: "root",
        child: Some("leaf"),
        gate: None,
        early: false,
    });
    let root_identity_tool: Arc<dyn Tool> = root_tool.clone();
    let _root_identity =
        crate::nested_identity::register(&root_identity_tool, Some("backend-v1".into())).unwrap();
    let root = if unhooked {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(&mut registry, root_tool, "root", 1, vec![leaf.clone()], "cancel-v1", Arc::new(Cancel(calls.clone())), UnsafeDurableAuthorization::interruption_requires_reconciliation())
    } else {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(&mut registry, root_tool, "root", 1, vec![leaf.clone()], &snapshot, &names, "cancel-v1", Arc::new(Cancel(calls.clone())), UnsafeDurableAuthorization::interruption_requires_reconciliation(), hooks())
    }.unwrap();
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![root.register(), leaf.register(), grandchild.register()],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let mut scheduler = Scheduler::open_with_extensions(&path, options, extensions).unwrap();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    if mode == "busy" {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            rt.block_on(leaf_identity.update(Some("leaf-backend".into()), || {
                held_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }))
            .unwrap();
        });
        held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let driver = scheduler.start_tasks(registry).unwrap();
    if mode == "busy" {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let store = durable_store::Store::open(&path).unwrap();
        assert_eq!(
            crate::task_store::TaskStore::new(&store)
                .load(child_id)
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        release_tx.send(()).unwrap();
    }
    let store = durable_store::Store::open(&path).unwrap();
    let tasks = crate::task_store::TaskStore::new(&store);
    tokio::time::timeout(Duration::from_secs(15), async {
        while !tasks.load(root_id).unwrap().unwrap().task.is_terminal() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let terminal = tasks.load(root_id).unwrap().unwrap();
    if mode != "good" && mode != "unhooked" && mode != "busy" {
        assert!(
            matches!(terminal.task.state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        if late {
            assert!(crate::async_task_tool::read_unsafe_durable_receipts(&store, child_id).unwrap().iter().any(|r| matches!(&r.outcome, Outcome::Pending { reference } if reference == "late-after-parent-receipt")));
        }
    } else {
        assert_eq!(terminal.task.state(), &TaskState::Cancelled);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        usize::from(child_cut && (mode == "good" || mode == "unhooked" || mode == "busy"))
    );
    if !unhooked && mode == "good" {
        let conversation = match terminal.task.owner() {
            crate::task::TaskOwner::Conversation(id) => *id,
            _ => unreachable!(),
        };
        scheduler.schedule(conversation);
        scheduler.wait(conversation).await.unwrap();
        let state = scheduler.conversation(conversation).unwrap();
        if child_cut {
            assert!(
                format!("{state:?}").contains("Tool was aborted"),
                "{state:?}"
            );
        } else {
            assert!(format!("{state:?}").contains("after cancel"), "{state:?}");
        }
    }
    let _ = driver.shutdown_with_pending().unwrap();
    let _ = scheduler.shutdown().await;
}

#[tokio::test]
async fn nested_busy_identity_defers_even_during_spec_change() {
    exercise_conversation_park_claim("nested_claim_actual_busy_spec").await;
}

#[tokio::test]
async fn nested_busy_identity_defers_parent_callback() {
    exercise_conversation_park_claim("nested_claim_actual_busy").await;
}

struct WokenAnyFixture {
    name: &'static str,
    dir: std::path::PathBuf,
}
#[async_trait]
impl Tool for WokenAnyFixture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        panic!("durable only")
    }
}
#[async_trait]
impl DurableAsyncTool for WokenAnyFixture {
    async fn execute_durable(
        &self,
        _: Value,
        context: ToolContext,
        resume: DurableToolResume,
        host: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        match self.name {
            "slow" if self.dir.join("unsafe-mode").exists() => {
                let blocker = host
                    .start_child("blocker-key", "blocker", json!({}))
                    .await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"blocker": blocker}),
                    wait: DurableWait::All(vec![blocker]),
                })
            }
            "slow" | "blocker" => {
                std::fs::write(self.dir.join("slow-ready"), b"ready").unwrap();
                context.cancellation.cancelled().await;
                Err("cancelled".into())
            }
            "fast" => {
                while !self.dir.join("slow-ready").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                if self.dir.join("unsafe-mode").exists() {
                    loop {
                        let store = durable_store::Store::open(self.dir.join("db"))
                            .map_err(|e| e.to_string())?;
                        let waiting = crate::task_store::TaskStore::new(&store)
                            .list()
                            .map_err(|e| e.to_string())?
                            .iter()
                            .any(|r| {
                                matches!(r.task.owner(), crate::task::TaskOwner::Task(_))
                                    && matches!(r.task.state(), TaskState::Waiting(_))
                            });
                        if waiting {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                Ok(DurableToolStep::Complete(Outcome::Succeeded("fast".into())))
            }
            "root" if resume.checkpoint.is_none() => {
                let slow = host.start_child("slow-key", "slow", json!({})).await?;
                let fast = host.start_child("fast-key", "fast", json!({})).await?;
                Ok(DurableToolStep::Park {
                    checkpoint: json!({"slow":slow,"fast":fast}),
                    wait: DurableWait::Any(vec![slow, fast]),
                })
            }
            "root" => {
                std::fs::write(self.dir.join("any-woken"), b"ready").unwrap();
                if std::env::var_os("PI_TEST_WOKEN_ANY_LIVE").is_some() {
                    context.cancellation.cancelled().await;
                    return Err("cancelled".into());
                }
                std::future::pending::<()>().await;
                unreachable!()
            }
            _ => unreachable!(),
        }
    }
}

fn build_woken_any(path: &std::path::Path) -> (Options, Registry, crate::extension::Registry) {
    use crate::async_task_tool::{
        DurableParkCancel, register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel,
        register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel,
    };
    struct Cancel(std::path::PathBuf, bool);
    #[async_trait]
    impl DurableParkCancel for Cancel {
        async fn cancel(&self, _: Value, checkpoint: Value, _: ToolContext) -> Result<(), String> {
            if self.1 {
                let blocker: AsyncTaskCall =
                    serde_json::from_value(checkpoint["blocker"].clone()).unwrap();
                let store = durable_store::Store::open(self.0.join("db")).unwrap();
                assert!(
                    crate::task_store::TaskStore::new(&store)
                        .load(blocker.task_id())
                        .unwrap()
                        .unwrap()
                        .task
                        .is_terminal()
                );
                std::fs::write(self.0.join("slow-cancel"), b"called").unwrap();
                return Ok(());
            }
            let slow: AsyncTaskCall = serde_json::from_value(checkpoint["slow"].clone()).unwrap();
            let store = durable_store::Store::open(self.0.join("db")).unwrap();
            let slow = crate::task_store::TaskStore::new(&store)
                .load(slow.task_id())
                .unwrap()
                .unwrap();
            assert!(
                slow.task.is_terminal(),
                "child must be drained before parent cancel"
            );
            if self.0.join("unsafe-mode").exists() {
                assert!(
                    self.0.join("slow-cancel").exists(),
                    "unsafe child must cancel first"
                );
            }
            std::fs::write(self.0.join("parent-cancel"), b"called").unwrap();
            Ok(())
        }
    }
    let dir = path.parent().unwrap().to_owned();
    let selected = dir.join("selected-mode").exists();
    let mut extensions = crate::extension::Registry::default();
    if selected {
        extensions
            .install_identified(
                crate::extension::Extension {
                    name: "policy".into(),
                    hooks: vec![Arc::new(ClaimPassHook(
                        Some(Arc::new(std::sync::atomic::AtomicUsize::new(0))),
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
    }
    let mut registry = Registry::default();
    assert!(
        registry.register("owned-independent", true, |_: &crate::task::Task| Ok(
            crate::task_scheduler::Step::Succeed(json!("independent"))
        ))
    );
    let mut extra = Vec::new();
    let register_safe = |registry: &mut Registry, name: &'static str| {
        let tool = Arc::new(WokenAnyFixture {
            name,
            dir: dir.clone(),
        });
        if selected {
            crate::async_task_tool::register_conversation_replay_safe_durable_tool_with_selected_hooks(registry, tool, name, 1, vec![], &extensions.snapshot(), &["policy".to_owned()], ReplaySafeAuthorization::host_attests_replay_safe(), crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe()).unwrap()
        } else {
            register_conversation_replay_safe_durable_tool(
                registry,
                tool,
                name,
                1,
                vec![],
                ReplaySafeAuthorization::host_attests_replay_safe(),
            )
            .unwrap()
        }
    };
    let slow = if dir.join("unsafe-mode").exists() {
        let blocker = register_safe(&mut registry, "blocker");
        let slow = Arc::new(WokenAnyFixture {
            name: "slow",
            dir: dir.clone(),
        });
        let identity: Arc<dyn Tool> = slow.clone();
        let _identity =
            crate::nested_identity::register(&identity, Some("slow-backend".into())).unwrap();
        let slow = if selected {
            crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_park_cancel(&mut registry, slow, "slow", 1, vec![blocker.clone()], &extensions.snapshot(), &["policy".to_owned()], "slow-cancel-v1", Arc::new(Cancel(dir.clone(), true)), UnsafeDurableAuthorization::interruption_requires_reconciliation(), crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe()).unwrap()
        } else {
            register_conversation_unsafe_durable_tool_with_at_most_once_park_cancel(
                &mut registry,
                slow,
                "slow",
                1,
                vec![blocker.clone()],
                "slow-cancel-v1",
                Arc::new(Cancel(dir.clone(), true)),
                UnsafeDurableAuthorization::interruption_requires_reconciliation(),
            )
            .unwrap()
        };
        extra.push(blocker);
        slow
    } else {
        register_safe(&mut registry, "slow")
    };
    let fast = register_safe(&mut registry, "fast");
    let children = vec![slow, fast];
    let root = Arc::new(WokenAnyFixture {
        name: "root",
        dir: dir.clone(),
    });
    let identity: Arc<dyn Tool> = root.clone();
    let _identity = crate::nested_identity::register(&identity, Some("backend-v1".into())).unwrap();
    let root = if selected {
        crate::async_task_tool::register_conversation_unsafe_durable_tool_with_selected_hooks_and_at_most_once_woken_cancel(&mut registry, root, "root", 1, children.clone(), &extensions.snapshot(), &["policy".to_owned()], "cancel-v1", Arc::new(Cancel(dir, false)), UnsafeDurableAuthorization::interruption_requires_reconciliation(), crate::async_task_tool::ReplaySafeConversationHooksAuthorization::host_attests_hooks_replay_safe()).unwrap()
    } else {
        register_conversation_unsafe_durable_tool_with_at_most_once_woken_cancel(
            &mut registry,
            root,
            "root",
            1,
            children.clone(),
            "cancel-v1",
            Arc::new(Cancel(dir, false)),
            UnsafeDurableAuthorization::interruption_requires_reconciliation(),
        )
        .unwrap()
    };
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: std::iter::once(root.register())
            .chain(children.iter().map(|c| c.register()))
            .chain(extra.iter().map(|c| c.register()))
            .collect(),
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    (options, registry, extensions)
}

fn start_woken_any_scheduler(path: &std::path::Path) -> (Scheduler, crate::task_driver::Driver) {
    let (options, registry, extensions) = build_woken_any(path);
    let scheduler = Scheduler::open_with_extensions(path, options, extensions).unwrap();
    let driver = scheduler.start_tasks(registry).unwrap();
    (scheduler, driver)
}

#[tokio::test]
async fn woken_any_child_drain_worker() {
    if let Some(dir) = std::env::var_os("PI_TEST_WOKEN_ANY_DIR") {
        let path = std::path::Path::new(&dir).join("db");
        let (mut scheduler, _driver) = start_woken_any_scheduler(&path);
        if std::env::var_os("PI_TEST_WOKEN_ANY_RESUME").is_some() {
            std::future::pending::<()>().await;
            return;
        }
        let conversation = scheduler
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 100,
            })
            .unwrap();
        if std::path::Path::new(&dir).join("selected-mode").exists() {
            scheduler
                .select_extensions(conversation, vec!["policy".into()])
                .unwrap();
        }
        scheduler.submit(conversation, None, "work".into()).unwrap();
        if std::env::var_os("PI_TEST_WOKEN_ANY_LIVE").is_some() {
            tokio::time::timeout(Duration::from_secs(15), async {
                while !std::path::Path::new(&dir).join("any-woken").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(15),
                scheduler.abort_and_wait(conversation),
            )
            .await
            .unwrap();
            assert!(result.is_err(), "the interrupted outcome remains unknown");
            assert!(
                std::path::Path::new(&dir).join("parent-cancel").exists(),
                "tasks: {:?}",
                crate::task_store::TaskStore::new(&durable_store::Store::open(&path).unwrap())
                    .list()
            );
            assert!(_driver.shutdown_with_pending().unwrap().failure().is_none());
            scheduler.shutdown().await.unwrap();
            return;
        }
        std::future::pending::<()>().await;
    }
}

#[tokio::test]
async fn interrupted_woken_root_drains_any_loser_before_cancel() {
    use std::process::{Command, Stdio};
    for unsafe_child in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        if unsafe_child {
            std::fs::write(dir.path().join("unsafe-mode"), b"yes").unwrap();
        }
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_any_child_drain_worker",
            ])
            .env("PI_TEST_WOKEN_ANY_DIR", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("any-woken").exists() {
                assert!(child.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let roots = crate::task_store::TaskStore::new(&store).list().unwrap();
        let root = roots
            .iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap()
            .task
            .id();
        drop(store);
        let (scheduler, driver) = start_woken_any_scheduler(&path);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if driver.get(root).unwrap().unwrap().is_terminal() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            dir.path().join("parent-cancel").exists(),
            "tasks: {:?}",
            crate::task_store::TaskStore::new(&durable_store::Store::open(&path).unwrap()).list()
        );
        assert!(
            matches!(driver.get(root).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn live_woken_root_drains_any_loser_before_cancel() {
    use std::process::{Command, Stdio};
    for unsafe_child in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        if unsafe_child {
            std::fs::write(dir.path().join("unsafe-mode"), b"yes").unwrap();
        }
        let status = tokio::task::spawn_blocking({
            let path = dir.path().to_owned();
            move || {
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "conversation_claim_boundary_tests::woken_any_child_drain_worker",
                        "--nocapture",
                    ])
                    .env("PI_TEST_WOKEN_ANY_DIR", &path)
                    .env("PI_TEST_WOKEN_ANY_LIVE", "1")
                    .stdout(Stdio::inherit())
                    .stderr(Stdio::inherit())
                    .status()
                    .unwrap()
            }
        })
        .await
        .unwrap();
        assert!(status.success());
        assert!(dir.path().join("parent-cancel").exists());
    }
}

#[tokio::test]
async fn interrupted_woken_graph_abort_commits_wake_and_owned_conversation_together() {
    exercise_woken_owned_graph_receipt(false, 0).await;
}

#[tokio::test]
async fn owned_cleanup_receipt_conflict_preserves_abort_and_marker() {
    exercise_woken_owned_graph_receipt(true, 0).await;
}

#[tokio::test]
async fn owned_graph_child_aba_requires_scoped_revision() {
    exercise_woken_owned_graph_receipt(false, 1).await;
}

#[tokio::test]
async fn owned_graph_terminal_resurrection_requires_scoped_revision() {
    exercise_woken_owned_graph_receipt(false, 2).await;
}

#[tokio::test]
async fn owned_graph_abort_clear_requires_scoped_revision() {
    exercise_woken_owned_graph_receipt(false, 3).await;
}

async fn exercise_woken_owned_graph_receipt(conflict: bool, graph_aba: u8) {
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::woken_any_child_drain_worker",
        ])
        .env("PI_TEST_WOKEN_ANY_DIR", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !dir.path().join("any-woken").exists() {
            assert!(child.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let path = dir.path().join("db");
    let (options, registry, _extensions) = build_woken_any(&path);
    let mut harness = crate::runtime::Harness::open_paused(&path, options).unwrap();
    let conversation = harness
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let records = crate::task_store::TaskStore::new(&store).list().unwrap();
    let root = records
        .into_iter()
        .find(|r| {
            matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                && r.task.state() == &TaskState::Running
        })
        .unwrap();
    let edge = crate::ownership_edge::Edge {
        task: root.task.id(),
        task_owner: root.task.owner().clone(),
        child: conversation,
        mode: crate::ownership_edge::Mode::Foreground,
    };
    let prepared = crate::ownership_edge::EdgeStore::new(&store)
        .prepare_create(&edge)
        .unwrap();
    store.commit(&prepared.checks, &prepared.mutations).unwrap();
    let root = crate::task_store::TaskStore::new(&store)
        .load(root.task.id())
        .unwrap()
        .unwrap();
    let boundary = store.put_version().unwrap();
    let (_, proof) = crate::async_task_tool::unsafe_park_proof::woken_cancel_candidate_checks(
        &store, &root.task, boundary,
    )
    .unwrap();
    assert!(
        harness
            .scoped_woken_abort_graph(&root, &proof, &[])
            .is_err()
    );
    assert!(!harness.conversation(conversation).unwrap().abort_requested);
    assert_eq!(
        crate::task_store::TaskStore::new(&store)
            .load(root.task.id())
            .unwrap()
            .unwrap(),
        root
    );
    // A stale evidence check must not partially abort the graph.
    let mut stale_proof = proof.clone();
    stale_proof.push(durable_store::Check {
        key: b"test/woken-stale".to_vec(),
        expected: durable_store::Expected::Version(1),
    });
    assert!(
        harness
            .scoped_woken_abort_graph(&root, &stale_proof, &[conversation])
            .is_err()
    );
    assert!(!harness.conversation(conversation).unwrap().abort_requested);
    assert_eq!(
        crate::task_store::TaskStore::new(&store)
            .load(root.task.id())
            .unwrap()
            .unwrap(),
        root
    );
    assert!(
        harness
            .authenticated_scoped_woken_abort_graph(&root, &Registry::default(), &[conversation])
            .is_err()
    );
    assert!(!harness.conversation(conversation).unwrap().abort_requested);
    assert_eq!(
        crate::task_store::TaskStore::new(&store)
            .load(root.task.id())
            .unwrap()
            .unwrap(),
        root
    );
    harness
        .authenticated_scoped_woken_abort_graph(&root, &registry, &[conversation])
        .unwrap();
    assert!(harness.conversation(conversation).unwrap().abort_requested);
    let recovered = crate::task_store::TaskStore::new(&store)
        .load(root.task.id())
        .unwrap()
        .unwrap();
    assert!(recovered.task.abort_requested());
    assert_eq!(recovered.task.state(), &TaskState::Ready);
    assert_eq!(
        recovered.task.woken_interrupted_attempt(),
        root.task.attempt_id()
    );
    let evidence_key = crate::runtime::woken_owned_graph_key(root.task.id());
    let evidence_entry = store.get(&evidence_key).unwrap().unwrap();
    let evidence: crate::runtime::WokenOwnedGraphEvidence =
        serde_json::from_slice(&evidence_entry.value).unwrap();
    assert_eq!(evidence.root, root.task.id());
    assert_eq!(Some(evidence.interrupted_attempt), root.task.attempt_id());
    assert_eq!(evidence.edges.len(), 1);
    assert_eq!(evidence.edges[0].edge.child, conversation);
    let graph_commit = store
        .commit_for_version(&evidence_key, evidence_entry.version)
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .commit_for_version(&crate::task_store::key(root.task.id()), recovered.version)
            .unwrap(),
        Some(graph_commit)
    );
    let conversation_key = format!("runtime/conversation/{conversation}").into_bytes();
    let conversation_entry = store.get(&conversation_key).unwrap().unwrap();
    assert_eq!(
        store
            .commit_for_version(&conversation_key, conversation_entry.version)
            .unwrap(),
        Some(graph_commit)
    );

    if conflict {
        let path = path.clone();
        crate::runtime::before_owned_cleanup_final_commit(conversation, move || {
            let store = durable_store::Store::open(path).unwrap();
            let key = crate::runtime::owned_cleanup_key(conversation);
            let marker = store.get(&key).unwrap().unwrap();
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(marker.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: marker.value,
                    }],
                )
                .unwrap();
        });
    }
    let cleanup = harness
        .drive_owned_cleanup_fenced(conversation, &pi_rs_core::CancellationToken::new())
        .await;
    if conflict {
        assert!(cleanup.is_err());
        assert!(harness.conversation(conversation).unwrap().abort_requested);
        assert!(
            store
                .get(&crate::runtime::owned_cleanup_key(conversation))
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .list(format!("runtime/owned-cleanup-receipt/v1/{conversation}/").as_bytes())
                .unwrap()
                .is_empty()
        );
        drop(store);
        drop(harness);
        let (options, _, _) = build_woken_any(&path);
        let reopened = crate::runtime::Harness::open_paused(&path, options).unwrap();
        assert!(reopened.conversation(conversation).unwrap().abort_requested);
        assert!(
            durable_store::Store::open(&path)
                .unwrap()
                .get(&crate::runtime::owned_cleanup_key(conversation))
                .unwrap()
                .is_some()
        );
        return;
    }
    cleanup.unwrap();
    assert!(!harness.conversation(conversation).unwrap().abort_requested);
    assert!(
        store
            .get(&crate::runtime::owned_cleanup_key(conversation))
            .unwrap()
            .is_none()
    );
    let prefix = format!("runtime/owned-cleanup-receipt/v1/{conversation}/");
    let receipts = store.list(prefix.as_bytes()).unwrap();
    assert_eq!(receipts.len(), 1);
    let (receipt_key, receipt_entry) = &receipts[0];
    let receipt: crate::runtime::OwnedCleanupReceipt =
        serde_json::from_slice(&receipt_entry.value).unwrap();
    assert_eq!(receipt.conversation, conversation);
    assert_eq!(receipt.edge.child, conversation);
    assert_eq!(receipt.edge.task, root.task.id());
    let finished = store.get(&conversation_key).unwrap().unwrap();
    let receipt_commit = store
        .commit_for_version(receipt_key, receipt_entry.version)
        .unwrap()
        .unwrap();
    assert_eq!(
        store
            .commit_for_version(&conversation_key, finished.version)
            .unwrap(),
        Some(receipt_commit)
    );
    let start_commit = store
        .commit_for_version(
            &crate::runtime::owned_cleanup_key(conversation),
            receipt.marker_version,
        )
        .unwrap()
        .unwrap();
    assert!(start_commit < receipt_commit);
    #[cfg(not(feature = "experimental-graph-revision"))]
    assert!(
        harness
            .replay_safe_woken_owned_cleanup_checks(&recovered, store.put_version().unwrap())
            .is_err()
    );
    #[cfg(feature = "experimental-graph-revision")]
    {
        // Normal cleanup state changes and a different graph must not
        // invalidate this graph's structural revision.
        let tasks = crate::task_store::TaskStore::new(&store);
        let child = tasks
            .list()
            .unwrap()
            .into_iter()
            .find(|item| {
                item.task.owner() == &crate::task::TaskOwner::Task(root.task.id())
                    && !item.task.is_terminal()
            })
            .unwrap();
        tasks.update(&child, |task| task.cancel()).unwrap();
        let unrelated = harness
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 100,
            })
            .unwrap();
        let unrelated_task = crate::task::Task::new(
            uuid::Uuid::new_v4(),
            crate::task::TaskOwner::Conversation(unrelated),
            crate::task::Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered("other".into(), true);
        tasks
            .save(&[crate::task_store::Save::Create(&unrelated_task)])
            .unwrap();
        let checks = harness
            .replay_safe_woken_owned_cleanup_checks(&recovered, store.put_version().unwrap())
            .unwrap();
        store.commit(&checks, &[]).unwrap();
        if graph_aba >= 2 {
            let key = crate::task_store::key(child.task.id());
            let original = store.get(&key).unwrap().unwrap();
            let mut changed: Value = serde_json::from_slice(&original.value).unwrap();
            if graph_aba == 2 {
                changed["state"] = json!("Running");
            } else {
                assert_eq!(changed["abort_requested"], json!(true));
                changed["abort_requested"] = json!(false);
            }
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(original.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key: key.clone(),
                        value: serde_json::to_vec(&changed).unwrap(),
                    }],
                )
                .unwrap();
            let changed_entry = store.get(&key).unwrap().unwrap();
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(changed_entry.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key,
                        value: original.value,
                    }],
                )
                .unwrap();
        } else if graph_aba == 1 {
            let child = crate::task::Task::new(
                uuid::Uuid::new_v4(),
                crate::task::TaskOwner::Conversation(conversation),
                crate::task::Checkpoint {
                    phase: "initial".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap()
            .registered("legacy-child".into(), true);
            let key = crate::task_store::key(child.id());
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Missing,
                    }],
                    &[durable_store::Mutation::Put {
                        key: key.clone(),
                        value: serde_json::to_vec(&child).unwrap(),
                    }],
                )
                .unwrap();
            let entry = store.get(&key).unwrap().unwrap();
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[durable_store::Mutation::Delete { key }],
                )
                .unwrap();
        } else {
            // Even a byte-identical replacement changes edge authority.
            let edge_key = crate::ownership_edge::edge_key(conversation);
            let edge = store.get(&edge_key).unwrap().unwrap();
            store
                .commit(
                    &[durable_store::Check {
                        key: edge_key.clone(),
                        expected: durable_store::Expected::Version(edge.version),
                    }],
                    &[durable_store::Mutation::Put {
                        key: edge_key,
                        value: edge.value,
                    }],
                )
                .unwrap();
        }
        assert!(matches!(
            store.commit(&checks, &[]),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(
            harness
                .replay_safe_woken_owned_cleanup_checks(&recovered, store.put_version().unwrap())
                .is_err()
        );
    }
    #[cfg(not(feature = "experimental-graph-revision"))]
    let _ = graph_aba;
    drop(store);
    drop(harness);
    let options = Options {
        model: Arc::new(ModelStub {
            arguments: "{}".into(),
        }),
        tools: vec![],
        restart_limit: 4,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    };
    let reopened = crate::runtime::Harness::open_paused(&path, options).unwrap();
    assert!(!reopened.conversation(conversation).unwrap().abort_requested);
    let store = durable_store::Store::open(&path).unwrap();
    let saved = crate::task_store::TaskStore::new(&store)
        .load(root.task.id())
        .unwrap()
        .unwrap();
    assert_eq!(saved.task, recovered.task);
}

#[tokio::test]
async fn selected_woken_unsafe_child_keeps_after_through_settlement_and_reopen() {
    use std::process::{Command, Stdio};
    for scenario in ["normal", "reopen", "changed_child"] {
        let kill_after_receipt = scenario == "reopen";
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("unsafe-mode"), b"yes").unwrap();
        std::fs::write(dir.path().join("selected-mode"), b"yes").unwrap();
        let mut worker = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::woken_any_child_drain_worker",
            ])
            .env("PI_TEST_WOKEN_ANY_DIR", dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !dir.path().join("any-woken").exists() {
                assert!(worker.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            panic!(
                "tasks: {:?}",
                crate::task_store::TaskStore::new(&store).list()
            );
        });
        worker.kill().unwrap();
        worker.wait().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let root = crate::task_store::TaskStore::new(&store)
            .list()
            .unwrap()
            .into_iter()
            .find(|r| {
                matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                    && r.task.state() == &TaskState::Running
            })
            .unwrap();
        let conversation = match root.task.owner() {
            crate::task::TaskOwner::Conversation(id) => *id,
            _ => unreachable!(),
        };
        let root = root.task.id();
        drop(store);
        if kill_after_receipt {
            let mut resume = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "conversation_claim_boundary_tests::woken_any_child_drain_worker",
                ])
                .env("PI_TEST_WOKEN_ANY_DIR", dir.path())
                .env("PI_TEST_WOKEN_ANY_RESUME", "1")
                .env("PI_TEST_WOKEN_HOLD_AFTER_RECEIPT", dir.path())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            tokio::time::timeout(Duration::from_secs(20), async {
                while !dir.path().join("woken-receipt").exists() {
                    assert!(resume.try_wait().unwrap().is_none());
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            resume.kill().unwrap();
            resume.wait().unwrap();
        }
        let (mut scheduler, driver) = start_woken_any_scheduler(&path);
        tokio::time::timeout(Duration::from_secs(15), async {
            while !driver.get(root).unwrap().unwrap().is_terminal() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        assert!(
            store
                .get(&crate::task_scheduler::woken_conversation_terminal_key(
                    root
                ))
                .unwrap()
                .is_some(),
            "kill_after_receipt={kill_after_receipt}"
        );
        if scenario == "changed_child" {
            let child = crate::task_store::TaskStore::new(&store)
                .list()
                .unwrap()
                .into_iter()
                .find(|item| {
                    item.task.owner() == &crate::task::TaskOwner::Task(root)
                        && !item.task.replay_safe()
                })
                .unwrap();
            let key = format!(
                "durable/replay-safe-async/{}/nested-cancel-terminal-v1",
                child.task.id()
            )
            .into_bytes();
            let entry = store.get(&key).unwrap().unwrap();
            store
                .commit(
                    &[durable_store::Check {
                        key: key.clone(),
                        expected: durable_store::Expected::Version(entry.version),
                    }],
                    &[durable_store::Mutation::Delete { key }],
                )
                .unwrap();
        }
        scheduler.schedule(conversation);
        let result = scheduler.wait(conversation).await;
        if scenario == "changed_child" {
            assert!(
                result.is_err(),
                "changed child evidence must not publish after"
            );
            assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
            let _ = scheduler.shutdown().await;
            continue;
        }
        result.unwrap();
        let state = scheduler.conversation(conversation).unwrap();
        let text = format!("{state:?}");
        assert!(
            text.contains("after cancel") && text.contains("reconciliation"),
            "{text}"
        );
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        scheduler.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn interrupted_woken_owned_conversation_is_marked_by_driver_before_refusal() {
    exercise_woken_owned_coordinator(false).await;
}

#[tokio::test]
async fn invalid_woken_owned_proof_does_not_stop_independent_work() {
    exercise_woken_owned_coordinator(true).await;
}

async fn exercise_woken_owned_coordinator(invalid_proof: bool) {
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let mut process = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "conversation_claim_boundary_tests::woken_any_child_drain_worker",
        ])
        .env("PI_TEST_WOKEN_ANY_DIR", dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !dir.path().join("any-woken").exists() {
            assert!(process.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    process.kill().unwrap();
    process.wait().unwrap();
    let path = dir.path().join("db");
    let (options, _, _) = build_woken_any(&path);
    let mut harness = crate::runtime::Harness::open_paused(&path, options).unwrap();
    let conversation = harness
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    let independent_conversation = harness
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    let store = durable_store::Store::open(&path).unwrap();
    let root = crate::task_store::TaskStore::new(&store)
        .list()
        .unwrap()
        .into_iter()
        .find(|r| {
            matches!(r.task.owner(), crate::task::TaskOwner::Conversation(_))
                && r.task.state() == &TaskState::Running
        })
        .unwrap();
    let edge = crate::ownership_edge::Edge {
        task: root.task.id(),
        task_owner: root.task.owner().clone(),
        child: conversation,
        mode: crate::ownership_edge::Mode::Foreground,
    };
    let prepared = crate::ownership_edge::EdgeStore::new(&store)
        .prepare_create(&edge)
        .unwrap();
    store.commit(&prepared.checks, &prepared.mutations).unwrap();
    if invalid_proof {
        let current = crate::task_store::TaskStore::new(&store)
            .load(root.task.id())
            .unwrap()
            .unwrap();
        let (_, checks) = crate::async_task_tool::unsafe_park_proof::woken_cancel_candidate_checks(
            &store,
            &current.task,
            store.put_version().unwrap(),
        )
        .unwrap();
        let key = checks
            .iter()
            .find(|check| matches!(check.expected, durable_store::Expected::Version(_)))
            .unwrap()
            .key
            .clone();
        let entry = store.get(&key).unwrap().unwrap();
        store
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
    }
    drop(store);
    drop(harness);
    let conflict_observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = conflict_observed.clone();
    let writer_path = path.clone();
    let parent = root.task.id();
    if !invalid_proof {
        crate::task_scheduler::before_woken_recovery_commit_for_test(parent, move || {
            use std::sync::atomic::Ordering;
            let store = durable_store::Store::open(writer_path).unwrap();
            let child = crate::task::Task::new(
                uuid::Uuid::new_v4(),
                crate::task::TaskOwner::Task(parent),
                crate::task::Checkpoint {
                    phase: "initial".into(),
                    data: json!({}),
                },
                3,
            )
            .unwrap();
            let key = crate::task_store::key(child.id());
            store
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
            observed.store(true, Ordering::SeqCst);
        });
    }
    let (scheduler, driver) = start_woken_any_scheduler(&path);
    tokio::time::timeout(Duration::from_secs(15), async {
        while !scheduler
            .conversation(conversation)
            .unwrap()
            .abort_requested
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let recovered = driver.get(root.task.id()).unwrap().unwrap();
    if invalid_proof {
        assert!(
            matches!(recovered.state(), TaskState::CompletingFailed(reason) | TaskState::Failed(reason) if reason.contains("reconciliation"))
        );
    } else {
        assert!(conflict_observed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(recovered.abort_requested());
        assert_eq!(
            recovered.woken_interrupted_attempt(),
            root.task.attempt_id()
        );
    }
    assert!(!dir.path().join("parent-cancel").exists());
    let independent = crate::task::Task::new(
        uuid::Uuid::new_v4(),
        crate::task::TaskOwner::Conversation(independent_conversation),
        crate::task::Checkpoint {
            phase: "initial".into(),
            data: json!({}),
        },
        3,
    )
    .unwrap()
    .registered("owned-independent".into(), true);
    driver.submit(&independent).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !driver.get(independent.id()).unwrap().unwrap().is_terminal() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        driver.get(independent.id()).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("independent"))
    );
    assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    let _ = scheduler.shutdown().await;
}
