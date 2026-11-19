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
    let path = std::env::var_os("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| dir.path().to_path_buf())
        .join("db");
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
    let recovery_options = options.clone();
    let recovery_registry = registry.clone();
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
        exercise_conversation_park_claim("kill_after_claim").await;
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
    for (late, busy, uncertain) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conversation_claim_boundary_tests::conversation_cancel_claim_kill_worker",
                "--nocapture",
            ])
            .env("PI_TEST_CONVERSATION_RECEIPT_KILL_DIR", dir.path())
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
