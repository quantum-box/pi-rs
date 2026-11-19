use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    Error, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    async_task_tool::{
        DurableAsyncTool, DurableTaskHost, DurableToolResume, DurableToolStep,
        ReplaySafeAuthorization, register_conversation_replay_safe_durable_tool,
    },
    recovery::UncertainPolicy,
    runtime::{Harness, Options},
    task_scheduler::Registry,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })])
        .boxed())
    }
}
struct Candidate {
    description: &'static str,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Tool for Candidate {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "candidate".into(),
            description: self.description.into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        panic!("not dispatched")
    }
}
#[async_trait]
impl DurableAsyncTool for Candidate {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(DurableToolStep::Complete(Outcome::Succeeded("done".into())))
    }
}
fn registration(
    description: &'static str,
    version: u32,
    calls: Arc<AtomicUsize>,
) -> (Registry, Arc<pi_rs_durable::task_tool::TaskTool>) {
    let mut registry = Registry::default();
    let adapter = register_conversation_replay_safe_durable_tool(
        &mut registry,
        Arc::new(Candidate { description, calls }),
        "implementation",
        version,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    (registry, adapter)
}
fn options(adapter: &Arc<pi_rs_durable::task_tool::TaskTool>) -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![adapter.register()],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
#[test]
fn matching_registration_reopens_and_rejects_substitution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    {
        let (registry, adapter) = registration("original", 1, calls.clone());
        let h = Harness::open_paused(&path, options(&adapter)).unwrap();
        drop(h.attach_tasks(registry).unwrap());
    }
    for (description, version) in [("original", 1), ("changed", 1), ("original", 2)] {
        let (registry, _) = registration("original", 1, calls.clone());
        let (_, substitute) = registration(description, version, calls.clone());
        let h = Harness::open_paused(&path, options(&substitute)).unwrap();
        assert!(h.attach_tasks(registry).is_err(), "{description} {version}");
    }
    let (registry, adapter) = registration("original", 1, calls.clone());
    let h = Harness::open_paused(&path, options(&adapter)).unwrap();
    drop(h.attach_tasks(registry).unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unconnected_candidate_rejects_invocation_and_missing_driver_registration() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let (_, adapter) = registration("original", 1, calls.clone());
    let h = Harness::open_paused(dir.path().join("db"), options(&adapter)).unwrap();
    assert!(h.attach_tasks(Registry::default()).is_err());
    let result = adapter
        .invoke(
            json!({}),
            ToolContext {
                operation_id: uuid::Uuid::new_v4(),
                cancellation: pi_rs_core::CancellationToken::new(),
            },
        )
        .await;
    assert!(matches!(result, Outcome::Skipped(reason) if reason.contains("not enabled")));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct ChangingSpec(std::sync::atomic::AtomicBool);
#[async_trait]
impl Tool for ChangingSpec {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "candidate".into(),
            description: if self.0.load(Ordering::SeqCst) {
                "changed"
            } else {
                "original"
            }
            .into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        panic!("not dispatched")
    }
}
#[async_trait]
impl DurableAsyncTool for ChangingSpec {
    async fn execute_durable(
        &self,
        _: Value,
        _: ToolContext,
        _: DurableToolResume,
        _: &dyn DurableTaskHost,
    ) -> Result<DurableToolStep, String> {
        panic!("not dispatched")
    }
}
#[test]
fn attachment_rejects_tool_whose_live_spec_changes() {
    let dir = tempfile::tempdir().unwrap();
    let tool = Arc::new(ChangingSpec(std::sync::atomic::AtomicBool::new(false)));
    let mut registry = Registry::default();
    let adapter = register_conversation_replay_safe_durable_tool(
        &mut registry,
        tool.clone(),
        "implementation",
        1,
        vec![],
        ReplaySafeAuthorization::host_attests_replay_safe(),
    )
    .unwrap();
    let h = Harness::open_paused(dir.path().join("db"), options(&adapter)).unwrap();
    tool.0.store(true, Ordering::SeqCst);
    assert!(h.attach_tasks(registry).is_err());
}
