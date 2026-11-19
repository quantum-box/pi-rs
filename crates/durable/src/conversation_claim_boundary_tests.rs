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
    let identity_handle =
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
    if case == "save_receipt" {
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
        let _ = driver.shutdown_with_pending().unwrap();
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
    let is_reentrant = case == "reentrant";
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
    assert_eq!(store.get(key.as_bytes()).unwrap().is_some(), is_reentrant);
    if is_late_receipt {
        let receipts =
            pi_rs_durable::async_task_tool::read_unsafe_durable_receipts(&store, root_id).unwrap();
        assert!(receipts.iter().any(|receipt| matches!(&receipt.outcome, Outcome::Pending { reference } if reference == "remote-job")));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let _ = driver.shutdown_with_pending().unwrap();
}

#[tokio::test]
async fn conversation_cancel_test_receipt_is_stored_by_driver() {
    exercise_conversation_park_claim("save_receipt").await;
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
    ] {
        exercise_conversation_park_claim(case).await;
    }
}
