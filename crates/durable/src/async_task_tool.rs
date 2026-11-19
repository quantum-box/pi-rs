//! Task-backed async leaf tools for an explicitly unhooked standalone host.
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

/// Persistable registration identity. The host-assigned implementation ID must
/// cover implementation and backend configuration; it is an assertion by the
/// host, not a cryptographic proof of executable code or tenant identity.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolIdentity {
    spec: ToolSpec,
    implementation: String,
    version: u32,
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
        })
    }

    pub(crate) fn is_valid(&self) -> bool {
        !self.implementation.is_empty()
            && self.implementation.len() <= 4096
            && self.version != 0
            && !self.spec.name.is_empty()
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
}
impl SealedExecutor {
    pub(crate) fn new(
        tool: Arc<dyn Tool>,
        identity: ToolIdentity,
        implementation: String,
        version: u32,
    ) -> Self {
        Self {
            tool,
            identity,
            implementation,
            version,
        }
    }
    pub(crate) fn tool(&self) -> &Arc<dyn Tool> {
        &self.tool
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
    ) -> Result<crate::task_scheduler::Step, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tool runtime: {e}"))?;
        let outcome = runtime.block_on(async {
            if cancellation.is_cancelled() {
                return Outcome::Unknown("cancelled after logical dispatch".into());
            }
            let args = match self.tool.prepare_arguments(task.input().clone()).await {
                Ok(args) => args,
                Err(error) => {
                    return Outcome::Unknown(format!(
                        "argument preparation failed after logical dispatch: {error}"
                    ));
                }
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
        let handle = self.adapter.start_child(parent, documents, key, input)?;
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
    struct CheckpointParent(Arc<std::sync::OnceLock<Arc<AsyncTaskTool>>>);
    impl crate::task_scheduler::Handler for CheckpointParent {
        fn run(&self, _: &Task) -> Result<crate::task_scheduler::Step, String> {
            unreachable!()
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
        }
        #[async_trait::async_trait]
        impl Tool for ReplyTool {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "fixture".into(),
                    description: String::new(),
                    parameters: if self.changed.load(Ordering::SeqCst) {
                        json!({"type":"object", "required":["different"]})
                    } else {
                        json!({"type":"object"})
                    },
                }
            }
            async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
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
        });
        let _binding = crate::nested_identity::register(&tool, Some("backend".into())).unwrap();
        let make_host = || {
            let cell = Arc::new(std::sync::OnceLock::new());
            let mut registry = Registry::default();
            assert!(registry.register("parent", true, CheckpointParent(cell.clone())));
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
            assert!(registry.register("parent", true, CheckpointParent(cell)));
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
