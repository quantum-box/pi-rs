//! Disabled task-backed async tool bridge.
//!
//! The candidate is intentionally unavailable until task-owned dispatch can
//! enforce hook authorization, abort ordering, and durable uncertain results.
//! These boundaries cannot be established by a conversation attempt fence.

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
        let result = match result {
            NestedTaskResult::Succeeded(value) => {
                let outcome: Outcome = serde_json::from_value(value)
                    .map_err(|_| "invalid stored async tool outcome")?;
                if matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
                    return Err("uncertain tool outcome was stored as success".into());
                }
                AsyncTaskResult::Outcome(outcome)
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
/// This does not yet authorize dispatch of async task tools.
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

    /// Transfer this standalone scheduler to its real task driver. Async tool
    /// dispatch remains disabled, including after this transfer.
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

/// Reserved API for the disabled task-backed async tool bridge.
pub struct AsyncTaskTool {
    _private: (),
}
impl AsyncTaskTool {
    /// The bridge is unavailable even with a standalone host capability.
    pub fn new_unhooked(
        _host: &StandaloneNoHooksHost,
        _tool: Arc<dyn Tool>,
        _kind: impl Into<String>,
        _version: u32,
    ) -> Result<Self, String> {
        Err("async task tool dispatch is disabled: hooks, abort fencing, uncertain outcomes and tool identity are not yet integrated".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{Checkpoint, TaskOwner};
    use crate::task_scheduler::{Registry, Scheduler};
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
        let host = StandaloneNoHooksHost::open(
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
            assert!(AsyncTaskTool::new_unhooked(&host, tool, "async.disabled", 1).is_err());
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
    struct CheckpointParent(Arc<crate::task_tool::TaskTool>);
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
                let handle = AsyncTaskCall(self.0.start_child(parent, docs, "child", json!({}))?);
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

    #[test]
    fn driver_restores_checkpoint_handle_and_reads_child_after_reopen() {
        use crate::task_scheduler::{Handler, Step};
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let handler: Arc<dyn Handler> = Arc::new(|_: &Task| {
            Ok(Step::Succeed(
                serde_json::to_value(Outcome::Structured {
                    data: json!({"answer":42}),
                    attachments: vec![],
                })
                .unwrap(),
            ))
        });
        let adapter = Arc::new(crate::task_tool::TaskTool::new(
            ToolSpec {
                name: "fixture".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            },
            "fixture",
            true,
            1,
            handler.clone(),
        ));
        let registry = || {
            let mut registry = Registry::default();
            assert!(registry.register_shared("fixture", true, handler.clone()));
            assert!(registry.register("parent", true, CheckpointParent(adapter.clone())));
            registry
        };
        let mut scheduler = Scheduler::open(&path, registry()).unwrap();
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
        .registered("parent".into(), true);
        scheduler.submit(&parent).unwrap();
        assert!(scheduler.tick(0).unwrap());
        assert_eq!(
            scheduler.get(id).unwrap().unwrap().checkpoint().phase,
            "waiting"
        );
        drop(scheduler);
        let driver = crate::task_driver::Driver::start(Scheduler::open(&path, registry()).unwrap());
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
        let host = StandaloneNoHooksHost::open(
            dir.path().join("db"),
            Registry::default(),
            UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        let tool = Arc::new(TestTool {
            calls: Arc::new(AtomicUsize::new(0)),
            spec: "fixture",
        });
        assert!(AsyncTaskTool::new_unhooked(&host, tool, "fixture", 1).is_err());
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
}
