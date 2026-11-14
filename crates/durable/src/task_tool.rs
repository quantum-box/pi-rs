//! Opt-in top-level tool whose work is a conversation-owned scheduler task.
//! Nested and unscoped invocation are rejected before task admission.
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use serde_json::Value;
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// A tool definition backed by one registered generic task handler.
/// The handler receives the prepared tool arguments as its task input.
pub struct TaskTool {
    spec: ToolSpec,
    kind: String,
    replay_safe: bool,
    handler: Arc<dyn crate::task_scheduler::Handler>,
}
impl TaskTool {
    pub fn new(
        spec: ToolSpec,
        kind: impl Into<String>,
        replay_safe: bool,
        handler: Arc<dyn crate::task_scheduler::Handler>,
    ) -> Self {
        Self {
            spec,
            kind: kind.into(),
            replay_safe,
            handler,
        }
    }
    /// Register this exact adapter. The same handler must be registered with the task driver.
    pub fn register(self: &Arc<Self>) -> crate::runtime::RegisteredTool {
        let tool: Arc<dyn Tool> = self.clone();
        let mut entries = ENTRIES
            .get_or_init(Default::default)
            .lock()
            .expect("task tool registry poisoned");
        entries.retain(|(tool, _)| tool.strong_count() > 0);
        if !entries
            .iter()
            .any(|(registered, _)| registered.ptr_eq(&Arc::downgrade(&tool)))
        {
            entries.push((Arc::downgrade(&tool), Arc::downgrade(self)));
        }
        crate::runtime::RegisteredTool {
            tool,
            replay: crate::recovery::ReplayPolicy::Safe,
        }
    }
}
tokio::task_local! { static DISPATCH: (Arc<TaskTool>, uuid::Uuid); }

pub(crate) async fn scope<F: std::future::Future>(
    adapter: Option<Arc<TaskTool>>,
    operation: uuid::Uuid,
    future: F,
) -> F::Output {
    if let Some(adapter) = adapter {
        DISPATCH.scope((adapter, operation), future).await
    } else {
        future.await
    }
}

static ENTRIES: OnceLock<Mutex<Vec<(Weak<dyn Tool>, Weak<TaskTool>)>>> = OnceLock::new();
pub(crate) fn lookup(tool: &Arc<dyn Tool>) -> Option<Arc<TaskTool>> {
    let mut entries = ENTRIES.get_or_init(Default::default).lock().ok()?;
    entries.retain(|(tool, adapter)| tool.strong_count() > 0 && adapter.strong_count() > 0);
    entries.iter().find_map(|(registered, adapter)| {
        let registered = registered.upgrade()?;
        if Arc::ptr_eq(&registered, tool) {
            adapter.upgrade()
        } else {
            None
        }
    })
}
impl TaskTool {
    pub(crate) fn task(
        &self,
        conversation: uuid::Uuid,
        operation: uuid::Uuid,
        input: Value,
        limit: u32,
    ) -> Result<crate::task::Task, String> {
        crate::task::Task::new(
            operation,
            crate::task::TaskOwner::Conversation(conversation),
            crate::task::Checkpoint {
                phase: "initial".into(),
                data: input.clone(),
            },
            limit,
        )
        .and_then(|task| task.registered_versioned(self.kind.clone(), self.replay_safe, 1, input))
        .map_err(|e| format!("task creation: {e:?}"))
    }
    pub(crate) fn handler(&self) -> Arc<dyn crate::task_scheduler::Handler> {
        self.handler.clone()
    }
}
#[async_trait::async_trait]
impl Tool for TaskTool {
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }
    async fn invoke(&self, _: Value, context: ToolContext) -> Outcome {
        if !DISPATCH
            .try_with(|(adapter, operation)| {
                std::ptr::eq(self, adapter.as_ref()) && *operation == context.operation_id
            })
            .unwrap_or(false)
        {
            return Outcome::Skipped("task tool requires top-level durable dispatch".into());
        }
        Outcome::Pending {
            reference: format!("task:{}", context.operation_id),
        }
    }
}
