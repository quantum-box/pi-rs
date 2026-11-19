//! Opt-in tool whose work is a scheduler task. Model-issued invocation is
//! conversation-owned; a task handler can stage another TaskTool as its owned
//! keyed child. Ordinary nested Tool invocation remains unsupported.
use pi_rs_core::{Outcome, Tool, ToolContext, ToolSpec};
use serde_json::Value;
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// A tool definition backed by one registered generic task handler.
/// The handler receives the prepared tool arguments as its task input.
pub struct TaskTool {
    spec: ToolSpec,
    kind: String,
    replay_safe: bool,
    definition_version: u32,
    handler: Arc<dyn crate::task_scheduler::Handler>,
    direct_binding: Option<Value>,
    deferred_arguments: bool,
    conversation_hooks: bool,
}
/// Handle for a keyed task-backed nested tool. It can be stored in a checkpoint
/// and resolved after a child wake. The child is admitted only when the parent
/// handler's transition commits successfully.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NestedTaskCall {
    parent: uuid::Uuid,
    task_id: uuid::Uuid,
}

/// The durable terminal state of a task-backed nested tool.
#[derive(Clone, Debug, PartialEq)]
pub enum NestedTaskResult {
    Succeeded(Value),
    Failed(String),
    Cancelled,
}

impl NestedTaskCall {
    pub(crate) fn belongs_to(&self, parent: uuid::Uuid) -> bool {
        self.parent == parent
    }
    pub(crate) fn new(parent: uuid::Uuid, task_id: uuid::Uuid) -> Self {
        Self { parent, task_id }
    }
    pub fn task_id(&self) -> uuid::Uuid {
        self.task_id
    }

    /// Read the child in this invocation's snapshot. `None` means it has not
    /// settled; a staged child cannot be read until its admission commits.
    /// The read fences the parent transition against concurrent child changes.
    pub fn result(
        &self,
        parent: &crate::task::Task,
        documents: &mut crate::document::TaskDocuments<'_>,
    ) -> Result<Option<NestedTaskResult>, String> {
        if self.parent != parent.id() {
            return Err("nested task handle belongs to a different parent".into());
        }
        let child = documents.get_owned_child(self.task_id)?;
        use crate::task::TaskState;
        Ok(match child.state() {
            TaskState::Succeeded(value) => Some(NestedTaskResult::Succeeded(value.clone())),
            TaskState::Failed(error) => Some(NestedTaskResult::Failed(error.clone())),
            TaskState::Cancelled => Some(NestedTaskResult::Cancelled),
            _ => None,
        })
    }
}

impl TaskTool {
    /// `definition_version` must be nonzero and must change whenever the
    /// handler's behavior or interpretation of its checkpoint changes. Register
    /// the same kind, version, policy and handler with the task driver.
    pub fn new(
        spec: ToolSpec,
        kind: impl Into<String>,
        replay_safe: bool,
        definition_version: u32,
        handler: Arc<dyn crate::task_scheduler::Handler>,
    ) -> Self {
        Self {
            spec,
            kind: kind.into(),
            replay_safe,
            definition_version,
            handler,
            direct_binding: None,
            deferred_arguments: false,
            conversation_hooks: false,
        }
    }
    pub(crate) fn with_direct_binding(mut self, binding: Value) -> Self {
        self.direct_binding = Some(binding);
        self
    }
    pub(crate) fn with_deferred_arguments(mut self) -> Self {
        self.deferred_arguments = true;
        self
    }
    pub(crate) fn is_direct(&self) -> bool {
        self.direct_binding.is_some()
    }
    pub(crate) fn is_checkpointable(&self) -> bool {
        self.deferred_arguments
    }
    pub(crate) fn with_conversation_hooks(mut self) -> Self {
        self.conversation_hooks = true;
        self
    }
    pub(crate) fn has_conversation_hooks(&self) -> bool {
        self.conversation_hooks
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

    /// Start or find a keyed nested task-backed tool and return its durable
    /// handle. Return a `Step::Wait` naming the handle's task ID to release
    /// the parent and receive `Task::ended_children()` on wake.
    pub fn start_child(
        &self,
        parent: &crate::task::Task,
        documents: &mut crate::document::TaskDocuments<'_>,
        key: &str,
        input: Value,
    ) -> Result<NestedTaskCall, String> {
        Ok(NestedTaskCall {
            parent: parent.id(),
            task_id: self.stage_child(parent, documents, key, input)?,
        })
    }

    /// Stage this adapter as a keyed direct child of the running task. The
    /// handler must return a scheduler step; child admission and that step are
    /// committed atomically. Reusing a key requires identical input and task
    /// definition. The child handler must be registered in the task driver.
    /// This is a synchronous task-handler API, not a `KeyedHost` invocation.
    pub fn stage_child(
        &self,
        parent: &crate::task::Task,
        documents: &mut crate::document::TaskDocuments<'_>,
        key: &str,
        input: Value,
    ) -> Result<uuid::Uuid, String> {
        let (identity, child) = self.child_candidate(parent, input)?;
        documents.stage_task_tool_child(
            key,
            &identity,
            child.input(),
            child.clone(),
            self.handler.clone(),
        )
    }

    pub(crate) fn child_candidate(
        &self,
        parent: &crate::task::Task,
        input: Value,
    ) -> Result<(String, crate::task::Task), String> {
        self.child_candidate_inner(parent, input, true)
    }

    /// Sealed async tools normalize arguments inside their durable executor.
    /// Validate the raw object and schema here; validate the normalized value
    /// against that schema before invoking the tool.
    pub(crate) fn sealed_child_candidate(
        &self,
        parent: &crate::task::Task,
        input: Value,
    ) -> Result<(String, crate::task::Task), String> {
        self.child_candidate_inner(parent, input, false)
    }

    fn child_candidate_inner(
        &self,
        parent: &crate::task::Task,
        input: Value,
        validate_input: bool,
    ) -> Result<(String, crate::task::Task), String> {
        let identity = self.identity().ok_or("invalid task tool identity")?;
        let identity = serde_json::to_string(&identity).map_err(|e| e.to_string())?;
        let validator = jsonschema::validator_for(&self.spec.parameters)
            .map_err(|_| "invalid task tool schema")?;
        if identity.len() > 65_536
            || self.spec.name.is_empty()
            || serde_json::to_vec(&input).map_err(|e| e.to_string())?.len() > 16 * 1024 * 1024
            || if validate_input && !self.deferred_arguments {
                !validator.is_valid(&input)
            } else {
                !input.is_object()
            }
        {
            return Err("invalid task tool identity, schema, or input".into());
        }
        let child = crate::task::Task::new(
            uuid::Uuid::new_v4(),
            crate::task::TaskOwner::Task(parent.id()),
            crate::task::Checkpoint {
                phase: "initial".into(),
                data: input.clone(),
            },
            parent.restart_limit(),
        )
        .and_then(|child| {
            child.registered_versioned(
                self.kind.clone(),
                self.replay_safe,
                self.definition_version,
                input.clone(),
            )
        })
        .map_err(|error| format!("task creation: {error:?}"))?
        .into_task_tool();
        let child = if self.deferred_arguments {
            child.retain_task_children_on_success()
        } else {
            child
        };
        Ok((identity, child))
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

type RegistryEntry = (Weak<dyn Tool>, Weak<TaskTool>);
static ENTRIES: OnceLock<Mutex<Vec<RegistryEntry>>> = OnceLock::new();
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
    pub(crate) fn identity(&self) -> Option<Identity> {
        if self.kind.is_empty() || self.definition_version == 0 {
            return None;
        }
        Some(Identity {
            adapter_version: 1,
            spec: self.spec.clone(),
            kind: self.kind.clone(),
            definition_version: self.definition_version,
            replay_safe: self.replay_safe,
            direct_binding: self.direct_binding.clone(),
            deferred_arguments: self.deferred_arguments,
            conversation_hooks: self.conversation_hooks,
        })
    }
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
        .and_then(|task| {
            task.registered_versioned(
                self.kind.clone(),
                self.replay_safe,
                self.definition_version,
                input,
            )
        })
        .map(|task| {
            let task = task.into_task_tool();
            if self.deferred_arguments {
                task.retain_task_children_on_success()
            } else {
                task
            }
        })
        .map_err(|e| format!("task creation: {e:?}"))
    }
    pub(crate) fn handler(&self) -> Arc<dyn crate::task_scheduler::Handler> {
        self.handler.clone()
    }
}
/// Stable host-declared identity. Changing handler behavior requires changing its
/// kind or version; code identity cannot be inferred from a trait object.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Identity {
    adapter_version: u32,
    spec: ToolSpec,
    pub(crate) kind: String,
    pub(crate) definition_version: u32,
    pub(crate) replay_safe: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    direct_binding: Option<Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    deferred_arguments: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    conversation_hooks: bool,
}
impl Identity {
    pub(crate) fn is_checkpointable(&self) -> bool {
        self.deferred_arguments
    }
}
#[async_trait::async_trait]
impl Tool for TaskTool {
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }
    async fn invoke(&self, _: Value, context: ToolContext) -> Outcome {
        if self.deferred_arguments {
            return Outcome::Skipped(
                "conversation checkpointable tool execution is not enabled".into(),
            );
        }
        if self.is_direct() {
            return Outcome::Skipped("foreground task requires direct durable admission".into());
        }
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
