//! Task-backed async leaf tools for standalone hosts with explicit hook policy.
//!
//! Conversation-attached execution and nested async calls remain unsupported.
//! Task-owned dispatch uses its own attempt fence, cancellation and durable
//! receipts rather than relying on a conversation attempt fence.

use crate::task::Task;
use pi_rs_core::{Outcome, Tool, ToolSpec};
use std::sync::Arc;

const UNCERTAIN_PREFIX: &str = "tool outcome requires reconciliation: ";
const UNSUPPORTED_PREFIX: &str = "unsupported tool result: ";

/// A keyed task-backed leaf call. Inspect the typed result after the child wakes.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AsyncTaskCall(crate::task_tool::NestedTaskCall);

/// Preserves the Tool outcome, including pending references and structured output.
#[derive(Debug, PartialEq)]
pub enum AsyncTaskResult {
    Outcome(Outcome),
    /// The external outcome is unresolved and must not be replayed automatically.
    Uncertain(Outcome),
    /// A host operation or state update was unsupported; the output is retained.
    Unsupported {
        reason: String,
        outcome: Outcome,
    },
    Failed(String),
    Cancelled,
}
impl AsyncTaskCall {
    pub fn task_id(&self) -> uuid::Uuid {
        self.0.task_id()
    }

    pub fn result(
        &self,
        parent: &Task,
        documents: &mut crate::document::TaskDocuments<'_>,
    ) -> Result<Option<AsyncTaskResult>, String> {
        use crate::task_tool::NestedTaskResult;
        let Some(result) = self.0.result(parent, documents)? else {
            return Ok(None);
        };
        if let Some(outcome) = documents.read_sealed_child_receipt(self.task_id())? {
            return Ok(Some(
                if matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
                    AsyncTaskResult::Uncertain(outcome)
                } else {
                    AsyncTaskResult::Outcome(outcome)
                },
            ));
        }
        let result = match result {
            NestedTaskResult::Succeeded(_) => {
                return Err("sealed tool result lacks a receipt".into());
            }
            NestedTaskResult::Failed(reason) => match reason.strip_prefix(UNCERTAIN_PREFIX) {
                Some(json) => {
                    let outcome: Outcome = serde_json::from_str(json)
                        .map_err(|_| "invalid stored uncertain tool outcome")?;
                    if !matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
                        return Err("invalid uncertain tool outcome kind".into());
                    }
                    AsyncTaskResult::Uncertain(outcome)
                }
                None => match reason.strip_prefix(UNSUPPORTED_PREFIX) {
                    Some(json) => {
                        let (reason, outcome): (String, Outcome) = serde_json::from_str(json)
                            .map_err(|_| "invalid stored unsupported tool outcome")?;
                        AsyncTaskResult::Unsupported { reason, outcome }
                    }
                    None => AsyncTaskResult::Failed(reason),
                },
            },
            NestedTaskResult::Cancelled => AsyncTaskResult::Cancelled,
        };
        Ok(Some(result))
    }
}

/// The host explicitly attests that its standalone task application does not
/// require tool hooks for authorization or policy. This cannot establish facts
/// about external policy; callers are responsible for that assertion.
pub struct UnhookedAuthorization(());
impl UnhookedAuthorization {
    /// Explicitly attest that hooks are not required by this host's policy.
    pub fn standalone_only() -> Self {
        Self(())
    }
}

/// Owns a scheduler constructed without a conversation runtime. Its fields are
/// private, so a conversation-attached scheduler cannot be substituted.
/// Only explicitly registered sealed leaf tools can be dispatched.
pub struct StandaloneNoHooksHost {
    scheduler: crate::task_scheduler::Scheduler,
}
impl StandaloneNoHooksHost {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        registry: crate::task_scheduler::Registry,
        _authorization: UnhookedAuthorization,
    ) -> Result<Self, crate::task_scheduler::Error> {
        let scheduler = crate::task_scheduler::Scheduler::open(path, registry)?;
        Ok(Self { scheduler })
    }

    /// Register a sealed leaf Tool in this standalone host. The exact Tool
    /// allocation must have a matching `nested_identity` registration. The
    /// returned adapter stages task-owned child calls atomically.
    pub fn register_tool(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        let kind = kind.into();
        let (executor, identity) =
            self.scheduler
                .install_sealed_tool(kind.clone(), tool, implementation, version)?;
        let spec = identity.spec.clone();
        let handler: Arc<dyn crate::task_scheduler::Handler> = executor;
        let adapter = Arc::new(crate::task_tool::TaskTool::new(
            spec, kind, false, version, handler,
        ));
        Ok(Arc::new(AsyncTaskTool { adapter, identity }))
    }

    #[cfg(test)]
    pub(crate) fn into_scheduler_for_test(self) -> crate::task_scheduler::Scheduler {
        self.scheduler
    }

    /// Transfer this standalone scheduler to its real task driver.
    /// Registered sealed tools execute only through this driver.
    pub fn into_driver(self) -> crate::task_driver::Driver {
        crate::task_driver::Driver::start(self.scheduler)
    }
}

/// A standalone host for one replay-safe, stateless before-tool hook per tool.
/// The caller must change `hook_identity` whenever hook code or configuration changes.
/// Hooks must be side-effect-free: preflight can be retried after interruption.
/// Task-fenced memo is supported; sleep, conversation state and after-tool hooks are not.
pub struct StandaloneBeforeHookHost {
    scheduler: crate::task_scheduler::Scheduler,
}
impl StandaloneBeforeHookHost {
    pub fn open(
        path: impl AsRef<std::path::Path>,
        registry: crate::task_scheduler::Registry,
    ) -> Result<Self, crate::task_scheduler::Error> {
        Ok(Self {
            scheduler: crate::task_scheduler::Scheduler::open(path, registry)?,
        })
    }
    pub fn register_tool(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        hook_identity: &str,
        hook: Arc<dyn crate::extension::Hook>,
    ) -> Result<Arc<AsyncTaskTool>, String> {
        let kind = kind.into();
        let (executor, identity) = self.scheduler.install_sealed_tool_with_hook(
            kind.clone(),
            tool,
            implementation,
            version,
            Some((hook_identity.into(), hook)),
        )?;
        let handler: Arc<dyn crate::task_scheduler::Handler> = executor;
        let adapter = Arc::new(crate::task_tool::TaskTool::new(
            identity.spec.clone(),
            kind,
            false,
            version,
            handler,
        ));
        Ok(Arc::new(AsyncTaskTool { adapter, identity }))
    }
    /// Bind an exact, host-identified extension selection to this standalone
    /// task tool. The selection and its order are part of the durable identity.
    /// The selected hooks have the same replay and capability restrictions as
    /// `register_tool`; in particular this phase only invokes before hooks.
    pub fn register_selected_tool(
        &mut self,
        kind: impl Into<String>,
        tool: Arc<dyn Tool>,
        implementation: &str,
        version: u32,
        snapshot: &crate::extension::Snapshot,
        names: &[String],
    ) -> Result<Arc<AsyncTaskTool>, String> {
        let selection = snapshot
            .stable_selection(names)
            .ok_or("extension selection is missing, duplicated, or unidentified")?;
        let resolved = snapshot
            .resolve_stable(names, &selection)
            .ok_or("extension selection changed")?;
        let identity = format!(
            "selection-v1:{}",
            serde_json::to_string(&selection).map_err(|e| e.to_string())?
        );
        self.register_tool(
            kind,
            tool,
            implementation,
            version,
            &identity,
            Arc::new(SelectedBeforeHooks(resolved)),
        )
    }
    pub fn into_driver(self) -> crate::task_driver::Driver {
        crate::task_driver::Driver::start(self.scheduler)
    }
    #[cfg(test)]
    pub(crate) fn into_scheduler_for_test(self) -> crate::task_scheduler::Scheduler {
        self.scheduler
    }
}

struct SelectedBeforeHooks(crate::extension::Resolved);
#[async_trait::async_trait]
impl crate::extension::Hook for SelectedBeforeHooks {
    async fn before_tool(
        &self,
        call: &crate::extension::ToolCall,
        context: &crate::extension::Context<'_>,
    ) -> Result<crate::extension::BeforeTool, String> {
        let (next, decision, _reports) = self.0.before_tool(call.clone(), context).await;
        Ok(match decision {
            crate::extension::BeforeTool::Pass if next.arguments != call.arguments => {
                crate::extension::BeforeTool::Rewrite(next.arguments)
            }
            other => other,
        })
    }
}

/// Persistable registration identity. The host-assigned implementation ID must
/// cover implementation and backend configuration; it is an assertion by the
/// host, not a cryptographic proof of executable code or tenant identity.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolIdentity {
    spec: ToolSpec,
    implementation: String,
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    before_hook: Option<String>,
}
impl ToolIdentity {
    pub fn new(
        tool: &dyn Tool,
        implementation: impl Into<String>,
        version: u32,
    ) -> Result<Self, String> {
        let implementation = implementation.into();
        let spec = tool.spec();
        if implementation.is_empty()
            || implementation.len() > 4096
            || version == 0
            || spec.name.is_empty()
        {
            return Err("invalid tool identity".into());
        }
        Ok(Self {
            spec,
            implementation,
            version,
            before_hook: None,
        })
    }

    pub(crate) fn with_before_hook(mut self, id: String) -> Result<Self, String> {
        if id.trim().is_empty() || id.len() > 4096 {
            return Err("invalid before-hook identity".into());
        }
        self.before_hook = Some(id);
        Ok(self)
    }

    pub(crate) fn is_valid(&self) -> bool {
        !self.implementation.is_empty()
            && self.implementation.len() <= 4096
            && self.version != 0
            && !self.spec.name.is_empty()
            && self
                .before_hook
                .as_ref()
                .is_none_or(|id| !id.trim().is_empty() && id.len() <= 4096)
    }

    /// Compare the stored identity with the current definition. This check is
    /// only a building block and does not acquire an execution or abort fence.
    pub fn matches(&self, tool: &dyn Tool, implementation: &str, version: u32) -> bool {
        self.is_valid()
            && self.spec == tool.spec()
            && self.implementation == implementation
            && self.version == version
    }
}

/// Executor construction is crate-private: no caller can substitute a Handler
/// for the Tool whose identity is checked by the driver.
pub(crate) struct SealedExecutor {
    tool: Arc<dyn Tool>,
    identity: ToolIdentity,
    implementation: String,
    version: u32,
    before_hook: Option<Arc<dyn crate::extension::Hook>>,
}
impl SealedExecutor {
    pub(crate) fn new(
        tool: Arc<dyn Tool>,
        identity: ToolIdentity,
        implementation: String,
        version: u32,
        before_hook: Option<Arc<dyn crate::extension::Hook>>,
    ) -> Self {
        Self {
            tool,
            identity,
            implementation,
            version,
            before_hook,
        }
    }
    pub(crate) fn tool(&self) -> &Arc<dyn Tool> {
        &self.tool
    }

    pub(crate) fn has_before_hook(&self) -> bool {
        self.before_hook.is_some()
    }

    pub(crate) fn checkpoint_arguments(&self, task: &Task) -> Result<serde_json::Value, String> {
        if task.checkpoint().phase != "sealed.execute.v1" {
            return Err("unsupported sealed tool phase".into());
        }
        let data = &task.checkpoint().data;
        if data.get("identity")
            != Some(&serde_json::to_value(&self.identity).map_err(|e| e.to_string())?)
        {
            return Err("before-hook registration changed".into());
        }
        let args = data
            .get("arguments")
            .cloned()
            .ok_or("missing prepared arguments")?;
        self.validate_arguments(&args)?;
        Ok(args)
    }
    fn validate_arguments(&self, args: &serde_json::Value) -> Result<(), String> {
        if serde_json::to_vec(args).map_err(|e| e.to_string())?.len() > 16 * 1024 * 1024
            || !args.is_object()
            || !jsonschema::validator_for(&self.identity.spec.parameters)
                .is_ok_and(|v| v.is_valid(args))
        {
            return Err("arguments exceed 16 MiB or do not match tool schema".into());
        }
        Ok(())
    }
    pub(crate) fn preflight(
        &self,
        task: &Task,
        cancel: &pi_rs_core::CancellationToken,
        memo: &dyn crate::extension::Memo,
    ) -> Result<crate::task_scheduler::Step, String> {
        let hook = self
            .before_hook
            .as_ref()
            .ok_or("missing before hook")?
            .clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        runtime.block_on(async {
            let operation = async {
                let args = self.tool.prepare_arguments(task.input().clone()).await?;
                self.validate_arguments(&args)?;
                if !self.identity.matches(self.tool.as_ref(), &self.implementation, self.version) { return Err("tool identity changed".into()); }
                let mut registry = crate::extension::Registry::default();
                registry.install(crate::extension::Extension { name: "task-before".into(), hooks: vec![hook], ..Default::default() }).map_err(|e| e.to_string())?;
                let selected = registry.snapshot().resolve(None);
                let snapshot = serde_json::Value::Null;
                let context = crate::extension::Context { snapshot: &snapshot, memo };
                let (call, decision, _) = selected.before_tool(crate::extension::ToolCall { name: self.identity.spec.name.clone(), arguments: args }, &context).await;
                match decision {
                    crate::extension::BeforeTool::Pass => (),
                    crate::extension::BeforeTool::Block(reason) => return Ok(crate::task_scheduler::Step::Fail(format!("tool call blocked: {reason}"))),
                    crate::extension::BeforeTool::Sleep(_) => return Err("before-hook sleep is unsupported".into()),
                    crate::extension::BeforeTool::Rewrite(_) => return Err("unexpected hook decision".into()),
                }
                self.validate_arguments(&call.arguments)?;
                let checkpoint = crate::task::Checkpoint { phase: "sealed.execute.v1".into(), data: serde_json::json!({"identity": self.identity, "arguments": call.arguments}) };
                if serde_json::to_vec(&checkpoint).map_err(|e| e.to_string())?.len().saturating_add(1024) > 16 * 1024 * 1024 { return Err("prepared checkpoint exceeds 16 MiB".into()); }
                Ok(crate::task_scheduler::Step::Continue(checkpoint))
            };
            tokio::select! { biased; _ = cancel.cancelled() => Err("before-hook cancelled".into()), result = operation => result }
        })
    }

    /// Called only after the driver has committed its task-owned intent and
    /// acquired the registered identity lease. The worker directly polls and
    /// joins the Tool future. Arbitrary effects spawned by a Tool are outside
    /// that join and require destination-side idempotency or reconciliation.
    pub(crate) fn run(
        &self,
        task: &Task,
        intent: &crate::async_task_protocol::Intent,
        store: &durable_store::Store,
        cancellation: &pi_rs_core::CancellationToken,
        prepared: Option<serde_json::Value>,
    ) -> Result<crate::task_scheduler::Step, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tool runtime: {e}"))?;
        let outcome = runtime.block_on(async {
            if cancellation.is_cancelled() {
                return Outcome::Unknown("cancelled after logical dispatch".into());
            }
            let args = match prepared {
                Some(args) => args,
                None => match self.tool.prepare_arguments(task.input().clone()).await {
                    Ok(args) => args,
                    Err(error) => {
                        return Outcome::Unknown(format!(
                            "argument preparation failed after logical dispatch: {error}"
                        ));
                    }
                },
            };
            if !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
            {
                return Outcome::Unknown("tool identity changed during preparation".into());
            }
            let spec = self.tool.spec();
            if !args.is_object()
                || !jsonschema::validator_for(&spec.parameters).is_ok_and(|v| v.is_valid(&args))
            {
                return Outcome::Unknown(
                    "normalized arguments do not match the tool schema after dispatch".into(),
                );
            }
            let batch =
                match crate::async_task_protocol::prepare_arguments_record(store, intent, &args) {
                    Ok(batch) => batch,
                    Err(error) => {
                        return Outcome::Unknown(format!(
                            "normalized argument persistence failed: {error}"
                        ));
                    }
                };
            if let Err(error) = store.commit(&batch.checks, &batch.mutations) {
                return Outcome::Unknown(format!("normalized argument commit failed: {error}"));
            }
            if cancellation.is_cancelled() {
                return Outcome::Unknown("cancelled after logical dispatch".into());
            }
            let host = Arc::new(SealedLeafHost(std::sync::atomic::AtomicBool::new(false)));
            let reply = self
                .tool
                .invoke_transaction(
                    args,
                    pi_rs_core::ToolContext {
                        operation_id: task.id(),
                        cancellation: cancellation.clone(),
                    },
                    None,
                    host.clone(),
                )
                .await;
            if host.0.load(std::sync::atomic::Ordering::Acquire) || reply.state.is_some() {
                return Outcome::Unknown(format!(
                    "unsupported host operation or state update; returned reply: {}",
                    serde_json::to_string(&reply).unwrap_or_else(|_| "unserializable".into())
                ));
            }
            reply.outcome
        });
        Ok(crate::task_scheduler::Step::Succeed(
            serde_json::to_value(outcome).map_err(|e| e.to_string())?,
        ))
    }
}

impl crate::task_scheduler::Handler for SealedExecutor {
    fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
        Err("sealed executor requires its driver-owned task reservation".into())
    }
}

struct SealedLeafHost(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl pi_rs_core::ToolHost for SealedLeafHost {
    fn tools(&self) -> Vec<ToolSpec> {
        vec![]
    }
    async fn report_progress(&self, _: serde_json::Value) -> pi_rs_core::ExtensionResult<()> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("progress is unavailable".into())
    }
    async fn ui(
        &self,
        _: pi_rs_core::UiRequest,
    ) -> pi_rs_core::ExtensionResult<pi_rs_core::UiResponse> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("UI is unavailable".into())
    }
    async fn call(&self, _: &str, _: serde_json::Value) -> Outcome {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Outcome::Skipped("nested calls are unavailable".into())
    }
}

/// Sealed task-backed Tool registered in a standalone no-hook host.
pub struct AsyncTaskTool {
    adapter: Arc<crate::task_tool::TaskTool>,
    identity: ToolIdentity,
}
impl AsyncTaskTool {
    /// Immediately admit a keyed sealed child using the caller's live task lease.
    /// The returned handle may be persisted in a subsequent checkpoint.
    pub fn start_child_now(
        &self,
        admission: &crate::task_scheduler::AdmissionHandle<'_>,
        key: &str,
        input: serde_json::Value,
    ) -> Result<AsyncTaskCall, String> {
        let (parent, child) = admission
            .admit_sealed_child(&self.adapter, &self.identity, key, input)
            .map_err(|error| error.to_string())?;
        Ok(AsyncTaskCall(crate::task_tool::NestedTaskCall::new(
            parent, child,
        )))
    }

    /// Stage a direct task-owned call. Its identity, admission and parent
    /// transition commit together; it runs only while the parent is active or
    /// durably waiting on this direct child.
    pub fn start_child(
        &self,
        parent: &Task,
        documents: &mut crate::document::TaskDocuments<'_>,
        key: &str,
        input: serde_json::Value,
    ) -> Result<AsyncTaskCall, String> {
        documents.require_standalone_for_sealed_child()?;
        let (identity, child) = self.adapter.sealed_child_candidate(parent, input)?;
        let id = documents.stage_task_tool_child(
            key,
            &identity,
            child.input(),
            child.clone(),
            self.adapter.handler(),
        )?;
        let handle = crate::task_tool::NestedTaskCall::new(parent.id(), id);
        documents.bind_sealed_child_identity(key, &self.identity)?;
        Ok(AsyncTaskCall(handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, TaskOwner};
    use crate::task_scheduler::Registry;
    use crate::task_store::{Save, TaskStore};
    use pi_rs_core::{ToolContext, ToolSpec};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    struct TestTool {
        calls: Arc<AtomicUsize>,
        spec: &'static str,
    }
    #[async_trait::async_trait]
    impl Tool for TestTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.spec.into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Outcome::Pending {
                reference: "remote-job".into(),
            }
        }
    }

    struct BeforeFixture {
        decision: u8,
        entered: Option<std::sync::mpsc::Sender<()>>,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for BeforeFixture {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(sender) = &self.entered {
                sender.send(()).unwrap();
            }
            match self.decision {
                0 => Ok(crate::extension::BeforeTool::Block("denied".into())),
                1 => Ok(crate::extension::BeforeTool::Rewrite(
                    json!({"rewritten": true}),
                )),
                2 => std::future::pending().await,
                3 => Ok(crate::extension::BeforeTool::Rewrite(json!({"bad": true}))),
                _ => Ok(crate::extension::BeforeTool::Rewrite(
                    json!({"huge": "x".repeat(16 * 1024 * 1024)}),
                )),
            }
        }
    }
    struct BeforeToolFixture(std::sync::mpsc::Sender<serde_json::Value>);
    #[async_trait::async_trait]
    impl Tool for BeforeToolFixture {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "before-fixture".into(),
                description: String::new(),
                parameters: json!({"type":"object", "properties":{"bad":false}}),
            }
        }
        async fn prepare_arguments(
            &self,
            mut args: serde_json::Value,
        ) -> pi_rs_core::ExtensionResult<serde_json::Value> {
            args["prepared"] = json!(true);
            Ok(args)
        }
        async fn invoke(&self, args: serde_json::Value, _: ToolContext) -> Outcome {
            self.0.send(args.clone()).unwrap();
            Outcome::Structured {
                data: args,
                attachments: vec![],
            }
        }
    }
    fn before_fixture_host(
        path: &std::path::Path,
        tool: Arc<dyn Tool>,
        hook: Arc<dyn crate::extension::Hook>,
        hook_id: &str,
    ) -> StandaloneBeforeHookHost {
        let cell = Arc::new(std::sync::OnceLock::new());
        let mut registry = Registry::default();
        assert!(registry.register("parent", true, CheckpointParent(cell.clone())));
        let mut host = StandaloneBeforeHookHost::open(path, registry).unwrap();
        let adapter = host
            .register_tool("leaf", tool, "backend", 1, hook_id, hook)
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        host
    }
    fn before_parent() -> Task {
        Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("parent".into(), true)
        .into_task_tool()
    }
    fn wait_terminal(driver: &crate::task_driver::Driver, id: Uuid) -> Task {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let task = driver.get(id).unwrap().unwrap();
            if task.is_terminal() {
                return task;
            }
            assert!(
                std::time::Instant::now() < until,
                "task did not settle: {:?}",
                task.state()
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    #[test]
    fn before_driver_block_rewrite_and_reject_invalid() {
        for mode in [0, 1, 3, 4] {
            let dir = tempfile::tempdir().unwrap();
            let (sent, received) = std::sync::mpsc::channel();
            let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let hook = Arc::new(BeforeFixture {
                decision: mode,
                entered: None,
                calls: calls.clone(),
            });
            let scheduler = before_fixture_host(&dir.path().join("db"), tool, hook, "hook-v1")
                .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let task = wait_terminal(&driver, parent.id());
            if mode == 1 {
                assert_eq!(received.try_recv().unwrap(), json!({"rewritten":true}));
                assert_eq!(
                    task.state(),
                    &crate::task::TaskState::Succeeded(json!({"rewritten":true}))
                );
            } else {
                assert!(received.try_recv().is_err());
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }
    #[test]
    fn before_driver_abort_cancels_hook_before_intent() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let (entered, waiting) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let hook = Arc::new(BeforeFixture {
            decision: 2,
            entered: Some(entered),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let scheduler = before_fixture_host(&dir.path().join("db"), tool, hook, "hook-v1")
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        waiting
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent.id()).unwrap();
        let task = wait_terminal(&driver, parent.id());
        // Neither fixture registers abort cleanup; the scheduler conservatively
        // orphans the parent while still preventing leaf dispatch.
        assert!(
            matches!(task.state(), crate::task::TaskState::Failed(reason) if reason.contains("orphaned")),
            "{:?}",
            task.state()
        );
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending().unwrap();
    }
    #[test]
    fn before_driver_reopen_uses_committed_execute_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(BeforeFixture {
            decision: 1,
            entered: None,
            calls: calls.clone(),
        });
        let mut scheduler = before_fixture_host(&path, tool.clone(), hook.clone(), "hook-v1")
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        let saved: AsyncTaskCall = serde_json::from_value(
            scheduler
                .get(parent.id())
                .unwrap()
                .unwrap()
                .checkpoint()
                .data
                .clone(),
        )
        .unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler
                .get(saved.task_id())
                .unwrap()
                .unwrap()
                .checkpoint()
                .phase,
            "sealed.execute.v1"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(scheduler);
        let driver = before_fixture_host(&path, tool, hook, "hook-v1").into_driver();
        let task = wait_terminal(&driver, parent.id());
        assert_eq!(
            task.state(),
            &crate::task::TaskState::Succeeded(json!({"rewritten":true}))
        );
        assert_eq!(received.try_recv().unwrap(), json!({"rewritten":true}));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        driver.shutdown().unwrap();
    }

    #[test]
    fn before_driver_reopen_rejects_changed_hook_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let hook = Arc::new(BeforeFixture {
            decision: 1,
            entered: None,
            calls: calls.clone(),
        });
        let mut scheduler = before_fixture_host(&path, tool.clone(), hook.clone(), "hook-v1")
            .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(scheduler);
        let driver = before_fixture_host(&path, tool, hook, "hook-v2").into_driver();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match driver.get(parent.id()) {
                Err(error) => {
                    assert!(
                        error
                            .to_string()
                            .contains("before-hook registration changed")
                    );
                    break;
                }
                Ok(_) => {
                    assert!(
                        std::time::Instant::now() < until,
                        "driver did not reject changed hook"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        }
        assert!(received.try_recv().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let _ = driver.shutdown_with_pending();
    }

    struct MemoHook {
        invalid: bool,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for MemoHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            if self.invalid {
                let _ = ctx.memo.get_or_insert("", json!(1)).await;
                return Ok(crate::extension::BeforeTool::Pass);
            }
            let first = ctx.memo.get_or_insert("key", json!("first")).await?;
            let second = ctx.memo.get_or_insert("key", json!("second")).await?;
            let read = ctx.memo.get("key").await?;
            if first != json!("first") || second != first || read != Some(first) {
                return Err("memo did not retain first value".into());
            }
            Ok(crate::extension::BeforeTool::Rewrite(json!({"memo":true})))
        }
    }

    #[test]
    fn driver_fences_memo_and_rejects_ignored_memo_errors() {
        for invalid in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (sent, received) = std::sync::mpsc::channel();
            let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = before_fixture_host(
                &dir.path().join("db"),
                tool,
                Arc::new(MemoHook { invalid }),
                "memo-v1",
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            let driver = crate::task_driver::Driver::start(scheduler);
            let task = wait_terminal(&driver, parent.id());
            if invalid {
                assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
                assert!(received.try_recv().is_err());
            } else {
                assert_eq!(received.try_recv().unwrap(), json!({"memo":true}));
                assert!(matches!(task.state(), crate::task::TaskState::Succeeded(_)));
            }
            let _ = driver.shutdown_with_pending().unwrap();
        }
    }

    struct StaleMemoHook {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        result: std::sync::mpsc::Sender<bool>,
    }
    #[async_trait::async_trait]
    impl crate::extension::Hook for StaleMemoHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            let rejected = ctx.memo.get_or_insert("late", json!(1)).await.is_err();
            self.result.send(rejected).unwrap();
            Ok(crate::extension::BeforeTool::Pass)
        }
    }
    #[test]
    fn driver_rejects_memo_after_ancestor_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (result, result_rx) = std::sync::mpsc::channel();
        let hook = StaleMemoHook {
            entered,
            release: std::sync::Mutex::new(release_rx),
            result,
        };
        let scheduler =
            before_fixture_host(&dir.path().join("db"), tool, Arc::new(hook), "memo-v1")
                .into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent.id()).unwrap();
        release.send(()).unwrap();
        assert!(
            result_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        let _ = wait_terminal(&driver, parent.id());
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending().unwrap();
    }

    struct KillBeforeHook(std::path::PathBuf);
    #[async_trait::async_trait]
    impl crate::extension::Hook for KillBeforeHook {
        async fn before_tool(
            &self,
            _: &crate::extension::ToolCall,
            ctx: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeTool, String> {
            ctx.memo
                .get_or_insert("kill-marker", json!("saved"))
                .await?;
            std::fs::write(self.0.join("hook-ready"), b"ready").map_err(|e| e.to_string())?;
            std::future::pending().await
        }
    }
    #[test]
    fn driver_kill_during_before_reopens_without_dispatch() {
        const ENV: &str = "PI_RS_TASK_BEFORE_KILL_1119";
        if let Ok(path) = std::env::var(ENV) {
            let path = std::path::PathBuf::from(path);
            let (sent, _received) = std::sync::mpsc::channel();
            let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
            let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
            let scheduler = before_fixture_host(
                &path.join("db"),
                tool,
                Arc::new(KillBeforeHook(path.clone())),
                "kill-v1",
            )
            .into_scheduler_for_test();
            let parent = before_parent();
            scheduler.submit(&parent).unwrap();
            std::fs::write(path.join("parent"), parent.id().to_string()).unwrap();
            let _driver = crate::task_driver::Driver::start(scheduler);
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_tool::tests::driver_kill_during_before_reopens_without_dispatch")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("hook-ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("before hook did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let parent: Uuid = std::fs::read_to_string(dir.path().join("parent"))
            .unwrap()
            .parse()
            .unwrap();
        {
            let store = durable_store::Store::open(dir.path().join("db")).unwrap();
            let parent_task = TaskStore::new(&store).load(parent).unwrap().unwrap().task;
            let handle: AsyncTaskCall =
                serde_json::from_value(parent_task.checkpoint().data.clone()).unwrap();
            let key = format!("runtime/extension-memo/{}/kill-marker", handle.task_id());
            let stored = store.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&stored.value).unwrap(),
                json!("saved")
            );
        }
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let driver = before_fixture_host(
            &dir.path().join("db"),
            tool,
            Arc::new(KillBeforeHook(dir.path().to_owned())),
            "kill-v1",
        )
        .into_driver();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match driver.get(parent) {
                Ok(Some(task)) if task.is_terminal() => break,
                Err(_) => break,
                _ => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "reopened task did not fail closed"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
        }
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending();
    }

    #[test]
    fn selected_extensions_execute_in_driver_and_bind_order_to_identity() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(BeforeToolFixture(sent));
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut extensions = crate::extension::Registry::default();
        for (name, mode) in [("first", 1), ("second", 0)] {
            extensions
                .install_identified(
                    crate::extension::Extension {
                        name: name.into(),
                        hooks: vec![Arc::new(BeforeFixture {
                            decision: mode,
                            entered: None,
                            calls: calls.clone(),
                        })],
                        ..Default::default()
                    },
                    crate::extension::ExtensionIdentity::new(1, name.into(), vec!["hook".into()])
                        .unwrap(),
                )
                .unwrap();
        }
        let snapshot = extensions.snapshot();
        let names = vec!["first".into(), "second".into()];
        let mut registry = Registry::default();
        let cell = Arc::new(std::sync::OnceLock::new());
        assert!(registry.register("parent", true, CheckpointParent(cell.clone())));
        let mut host = StandaloneBeforeHookHost::open(dir.path().join("db"), registry).unwrap();
        assert!(
            host.register_selected_tool(
                "invalid",
                tool.clone(),
                "backend",
                1,
                &snapshot,
                &["missing".into()]
            )
            .is_err()
        );
        let adapter = host
            .register_selected_tool("leaf", tool, "backend", 1, &snapshot, &names)
            .unwrap();
        assert!(cell.set(adapter).is_ok());
        let scheduler = host.into_scheduler_for_test();
        let parent = before_parent();
        scheduler.submit(&parent).unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        assert!(matches!(
            wait_terminal(&driver, parent.id()).state(),
            crate::task::TaskState::Failed(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending().unwrap();
    }

    #[test]
    fn serialized_handle_survives_checkpoint_round_trip() {
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        let handle: AsyncTaskCall =
            serde_json::from_value(json!({"parent": parent, "task_id": child})).unwrap();
        let checkpoint = serde_json::to_value(&handle).unwrap();
        let restored: AsyncTaskCall = serde_json::from_value(checkpoint).unwrap();
        assert_eq!(restored, handle);
        assert_eq!(restored.task_id(), child);
    }

    #[test]
    fn driver_blocks_persisted_unavailable_tasks_for_all_tool_specs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("host-db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        for spec in ["original", "changed"] {
            let tool = Arc::new(TestTool {
                calls: calls.clone(),
                spec,
            });
            assert!(
                host.register_tool("async.disabled", tool, "backend", 1)
                    .is_err()
            );
        }
        let id = Uuid::new_v4();
        let task = Task::new(
            id,
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "execute".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("async.disabled".into(), false, 1, json!({}))
        .unwrap()
        .into_task_tool();
        let store = durable_store::Store::open(&path).unwrap();
        TaskStore::new(&store).save(&[Save::Create(&task)]).unwrap();
        drop(store);
        let host = StandaloneNoHooksHost::open(
            &path,
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let driver = host.into_driver();
        let blocked = driver.blocked_tasks().unwrap();
        assert!(blocked.iter().any(|task| task.id == id));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        driver.shutdown().unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let restored = TaskStore::new(&store).load(id).unwrap().unwrap().task;
        assert!(!restored.is_terminal());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    struct CheckpointParent(Arc<std::sync::OnceLock<Arc<AsyncTaskTool>>>, bool);
    impl crate::task_scheduler::Handler for CheckpointParent {
        fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
            unreachable!()
        }
        fn run_with_admission(
            &self,
            parent: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
            admission: &crate::task_scheduler::AdmissionHandle<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            if self.1 && parent.checkpoint().phase == "initial" {
                let handle = self.0.get().ok_or("missing tool")?.start_child_now(
                    admission,
                    "child",
                    json!({}),
                )?;
                let repeated = self.0.get().ok_or("missing tool")?.start_child_now(
                    admission,
                    "child",
                    json!({}),
                )?;
                if repeated != handle {
                    return Err("keyed child did not reconnect".into());
                }
                if self
                    .0
                    .get()
                    .ok_or("missing tool")?
                    .start_child_now(admission, "child", json!({"changed":true}))
                    .is_ok()
                {
                    return Err("keyed child accepted changed input".into());
                }
                let id = handle.task_id();
                return Ok(crate::task_scheduler::Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: serde_json::to_value(handle).map_err(|e| e.to_string())?,
                    },
                    condition: crate::task::WaitFor::Tasks(vec![id]),
                    children: vec![],
                });
            }
            self.run_with_documents(parent, docs)
        }
        fn run_with_documents(
            &self,
            parent: &Task,
            docs: &mut crate::document::TaskDocuments<'_>,
        ) -> Result<crate::task_scheduler::Step, String> {
            use crate::task_scheduler::Step;
            if parent.checkpoint().phase == "initial" {
                let handle = self.0.get().ok_or("missing tool")?.start_child(
                    parent,
                    docs,
                    "child",
                    json!({}),
                )?;
                let id = handle.task_id();
                return Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "waiting".into(),
                        data: serde_json::to_value(&handle).map_err(|e| e.to_string())?,
                    },
                    condition: crate::task::WaitFor::Tasks(vec![id]),
                    children: vec![],
                });
            }
            let handle: AsyncTaskCall = serde_json::from_value(parent.checkpoint().data.clone())
                .map_err(|e| e.to_string())?;
            match handle.result(parent, docs)? {
                Some(AsyncTaskResult::Outcome(Outcome::Structured { data, .. })) => {
                    Ok(Step::Succeed(data))
                }
                other => Err(format!("unexpected child outcome: {other:?}")),
            }
        }
    }

    fn driver_checkpoint_reopen(mode: u8) {
        use std::time::{Duration, Instant};
        struct ReplyTool {
            changed: Arc<std::sync::atomic::AtomicBool>,
            calls: Arc<AtomicUsize>,
            normalize: bool,
        }
        #[async_trait::async_trait]
        impl Tool for ReplyTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "fixture".into(),
                    description: String::new(),
                    parameters: if self.changed.load(Ordering::SeqCst) {
                        json!({"type":"object", "required":["different"]})
                    } else if self.normalize {
                        json!({"type":"object", "required":["normalized"]})
                    } else {
                        json!({"type":"object"})
                    },
                }
            }
            async fn prepare_arguments(
                &self,
                mut args: serde_json::Value,
            ) -> pi_rs_core::ExtensionResult<serde_json::Value> {
                if self.normalize {
                    args["normalized"] = json!(true);
                }
                Ok(args)
            }
            async fn invoke(&self, args: serde_json::Value, _: ToolContext) -> Outcome {
                if self.normalize {
                    assert_eq!(args["normalized"], json!(true));
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                Outcome::Structured {
                    data: json!({"answer":42}),
                    attachments: vec![],
                }
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(ReplyTool {
            changed: changed.clone(),
            calls: calls.clone(),
            normalize: mode >= 4,
        });
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let make_host = || {
            let cell = Arc::new(std::sync::OnceLock::new());
            let mut registry = Registry::default();
            assert!(registry.register(
                "parent",
                true,
                CheckpointParent(cell.clone(), mode == 3 || mode == 5)
            ));
            let mut host = StandaloneNoHooksHost::open(
                &path,
                registry,
                UnhookedAuthorization::standalone_only(),
            )
            .unwrap();
            let adapter = host
                .register_tool("fixture", tool.clone(), "backend", 1)
                .unwrap();
            assert!(cell.set(adapter).is_ok());
            host
        };
        let mut scheduler = make_host().into_scheduler_for_test();
        let id = Uuid::new_v4();
        let parent = Task::new(
            id,
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("parent".into(), true)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(id).unwrap().unwrap().checkpoint().phase,
            "waiting"
        );
        drop(scheduler);
        if mode == 2 {
            let cell = Arc::new(std::sync::OnceLock::new());
            let mut registry = Registry::default();
            assert!(registry.register("parent", true, CheckpointParent(cell, false)));
            let forged_calls = Arc::new(AtomicUsize::new(0));
            let observed = forged_calls.clone();
            assert!(registry.register("fixture", false, move |_: &Task| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(crate::task_scheduler::Step::Succeed(json!(null)))
            }));
            let driver = StandaloneNoHooksHost::open(
                &path,
                registry,
                UnhookedAuthorization::standalone_only(),
            )
            .unwrap()
            .into_driver();
            let deadline = Instant::now() + Duration::from_secs(5);
            while driver.get(id).is_ok() {
                assert!(
                    Instant::now() < deadline,
                    "replacement handler was not rejected"
                );
                std::thread::yield_now();
            }
            assert_eq!(forged_calls.load(Ordering::SeqCst), 0);
            assert!(driver.shutdown_with_pending().unwrap().failure().is_some());
            return;
        }
        changed.store(mode == 1, Ordering::SeqCst);
        let driver = make_host().into_driver();
        if mode == 1 {
            let deadline = Instant::now() + Duration::from_secs(5);
            while driver.get(id).is_ok() {
                assert!(Instant::now() < deadline, "changed spec was not rejected");
                std::thread::yield_now();
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(driver.shutdown_with_pending().unwrap().failure().is_some());
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let task = driver.get(id).unwrap().unwrap();
            if task.is_terminal() {
                assert_eq!(
                    task.state(),
                    &crate::task::TaskState::Succeeded(json!({"answer":42}))
                );
                break;
            }
            assert!(Instant::now() < deadline, "driver did not settle parent");
            std::thread::yield_now();
        }
        driver.shutdown().unwrap();
    }
    #[test]
    fn driver_restores_checkpoint_handle_and_reads_child_after_reopen() {
        driver_checkpoint_reopen(0);
    }
    #[test]
    fn driver_rejects_changed_tool_spec_after_reopen() {
        driver_checkpoint_reopen(1);
    }
    #[test]
    fn driver_rejects_replacement_handler_after_reopen() {
        driver_checkpoint_reopen(2);
    }
    #[test]
    fn immediate_sealed_child_handle_survives_checkpoint_and_reopen() {
        driver_checkpoint_reopen(3);
    }
    #[test]
    fn staged_sealed_child_normalizes_before_schema_validation_after_reopen() {
        driver_checkpoint_reopen(4);
    }
    #[test]
    fn immediate_sealed_child_normalizes_before_schema_validation_after_reopen() {
        driver_checkpoint_reopen(5);
    }
    struct ChangedSchema;
    #[async_trait::async_trait]
    impl Tool for ChangedSchema {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "original".into(),
                description: String::new(),
                parameters: json!({"type":"object", "required":["new"]}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }

    #[test]
    fn stored_tool_identity_detects_spec_implementation_and_version_changes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let original = TestTool {
            calls: calls.clone(),
            spec: "original",
        };
        let changed = TestTool {
            calls,
            spec: "changed",
        };
        let identity = ToolIdentity::new(&original, "backend-tenant-v1", 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = durable_store::Store::open(&path).unwrap();
        let key = b"test/tool-identity".to_vec();
        store
            .commit(
                &[],
                &[durable_store::Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&identity).unwrap(),
                }],
            )
            .unwrap();
        drop(store);
        let store = durable_store::Store::open(&path).unwrap();
        let restored: ToolIdentity =
            serde_json::from_slice(&store.get(&key).unwrap().unwrap().value).unwrap();
        assert!(restored.matches(&original, "backend-tenant-v1", 1));
        assert!(!restored.matches(&changed, "backend-tenant-v1", 1));
        assert!(!restored.matches(&ChangedSchema, "backend-tenant-v1", 1));
        assert!(!restored.matches(&original, "other-tenant", 1));
        assert!(!restored.matches(&original, "backend-tenant-v1", 2));
        assert!(ToolIdentity::new(&original, "", 1).is_err());
        assert!(ToolIdentity::new(&original, "valid", 0).is_err());
    }

    #[test]
    fn unhooked_host_drives_only_its_standalone_scheduler() {
        let dir = tempfile::tempdir().unwrap();
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let tool = Arc::new(TestTool {
            calls: Arc::new(AtomicUsize::new(0)),
            spec: "fixture",
        });
        assert!(host.register_tool("fixture", tool, "backend", 1).is_err());
        host.into_driver().shutdown().unwrap();
    }
    #[test]
    fn unhooked_host_cannot_attach_to_a_live_conversation_runtime() {
        struct ModelStub;
        #[async_trait::async_trait]
        impl pi_rs_core::Model for ModelStub {
            async fn stream(
                &self,
                _: pi_rs_core::ModelRequest,
            ) -> Result<pi_rs_core::ModelStream, pi_rs_core::Error> {
                unreachable!()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let harness = crate::runtime::Harness::open_paused(
            &path,
            crate::runtime::Options {
                model: Arc::new(ModelStub),
                tools: vec![],
                restart_limit: 3,
                max_turns: 3,
                uncertain_policy: crate::recovery::UncertainPolicy::InterruptedResult,
            },
        )
        .unwrap();
        assert!(
            StandaloneNoHooksHost::open(
                &path,
                Registry::default(),
                UnhookedAuthorization::standalone_only()
            )
            .is_err()
        );
        drop(harness);
        let host = StandaloneNoHooksHost::open(
            &path,
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        host.into_driver().shutdown().unwrap();
    }
    #[test]
    fn production_registration_requires_matching_host_tool_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tool: Arc<dyn Tool> = Arc::new(TestTool {
            calls: calls.clone(),
            spec: "leaf",
        });
        assert!(
            host.register_tool("leaf", tool.clone(), "backend", 1)
                .is_err()
        );
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        assert!(
            host.register_tool("leaf", tool.clone(), "other", 1)
                .is_err()
        );
        host.register_tool("leaf", tool.clone(), "backend", 1)
            .unwrap();
        assert!(host.register_tool("leaf", tool, "backend", 1).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        host.into_driver().shutdown().unwrap();
    }
}
