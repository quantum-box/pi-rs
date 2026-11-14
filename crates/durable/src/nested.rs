//! Persistent nested tool calls. The runtime must retain the returned host,
//! wait for the parent invocation to finish, and call `seal_and_inspect` before
//! committing *any* parent result or state. An error means the parent must be
//! blocked for reconciliation, even if the parent returned success.
//!
//! Exactly one execution of a given root operation may be alive at a time,
//! including detached tasks. The scheduler must cancel and join all old work
//! before replay and keep `OwnedStore` ownership for the whole execution. Calls
//! made concurrently on one host are rejected: only ordered calls, or the
//! explicitly ordered `call_batch`, have stable replay identities.
//!
//! Tool argument preparation must be side-effect free. Nested tools that return
//! extension state are unsupported: their result is treated as uncertain. The
//! caller must not use this host for tools requiring transactional state updates.

use crate::{recovery::ReplayPolicy, runtime::RegisteredTool};
use durable_store::{Check, Expected, Mutation, owner::OwnedStore};
use pi_rs_core::{CancellationToken, Invocation, Outcome, ToolCall, ToolHost, ToolSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    name: String,
    arguments: Value,
    prepared: Value,
    policy: ReplayPolicy,
    operation_id: Uuid,
    outcome: Option<Outcome>,
}

#[derive(Debug, thiserror::Error)]
pub enum InspectionError {
    #[error("nested execution is still active or was sealed")]
    ActiveOrSealed,
    #[error("nested execution cannot be trusted: {0}")]
    Uncertain(String),
    #[error("nested storage: {0}")]
    Storage(#[from] durable_store::Error),
    #[error("nested record encoding: {0}")]
    Encoding(#[from] serde_json::Error),
}

struct Shared {
    owner: Arc<OwnedStore>,
    root: Uuid,
    tools: HashMap<String, RegisteredTool>,
    cancellation: CancellationToken,
    timeout: Duration,
    max_depth: usize,
    // Protects sealing, active calls, and the visited call counts together.
    lifecycle: Mutex<Lifecycle>,
    failed: AtomicBool,
    idle: tokio::sync::Notify,
}
#[derive(Default)]
struct Lifecycle {
    sealed: bool,
    active: usize,
    counts: HashMap<Uuid, u64>,
}

/// One execution of one parent operation. Clone the Arc, not the host itself.
/// `seal_and_inspect` must be called on the root host exactly once after the
/// parent returns, and its success must precede the parent's durable commit.
pub struct NestedHost {
    shared: Arc<Shared>,
    parent: Uuid,
    depth: usize,
    serial: tokio::sync::Mutex<()>,
}

struct Active(Arc<Shared>);
impl Drop for Active {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.lifecycle.lock() {
            state.active -= 1;
            self.0.idle.notify_one();
        } else {
            self.0.failed.store(true, Ordering::Release);
        }
    }
}

impl NestedHost {
    pub fn new(
        owner: Arc<OwnedStore>,
        parent: Uuid,
        tools: Vec<RegisteredTool>,
        cancellation: CancellationToken,
        timeout: Duration,
        max_depth: usize,
    ) -> Result<Arc<Self>, InspectionError> {
        let mut registry = HashMap::new();
        for tool in tools {
            let name = tool.tool.spec().name;
            if registry.insert(name.clone(), tool).is_some() {
                return Err(InspectionError::Uncertain(format!(
                    "duplicate tool: {name}"
                )));
            }
        }
        let mut lifecycle = Lifecycle::default();
        lifecycle.counts.insert(parent, 0);
        Ok(Arc::new(Self {
            shared: Arc::new(Shared {
                owner,
                root: parent,
                tools: registry,
                cancellation,
                timeout,
                max_depth,
                lifecycle: Mutex::new(lifecycle),
                failed: AtomicBool::new(false),
                idle: tokio::sync::Notify::new(),
            }),
            parent,
            depth: 0,
            serial: tokio::sync::Mutex::new(()),
        }))
    }
    fn prefix(&self) -> Vec<u8> {
        format!("nested/{}/", self.shared.root).into_bytes()
    }
    fn key(&self, parent: Uuid, index: u64) -> Vec<u8> {
        format!("nested/{}/{parent}/{index:020}", self.shared.root).into_bytes()
    }
    fn fail(&self, reason: impl Into<String>) -> Outcome {
        self.shared.failed.store(true, Ordering::Release);
        Outcome::Unknown(reason.into())
    }
    fn enter(&self) -> Result<Active, Outcome> {
        let mut l = self
            .shared
            .lifecycle
            .lock()
            .map_err(|_| self.fail("nested lifecycle lock poisoned"))?;
        if l.sealed || self.shared.failed.load(Ordering::Acquire) {
            return Err(self.fail("nested host sealed or failed"));
        }
        l.active += 1;
        Ok(Active(self.shared.clone()))
    }
    /// Seal the entire host tree and check all records, including descendants.
    /// A successful inspection requires no active or detached nested calls, no
    /// uncertain records, and no omitted previously recorded calls in any scope
    /// that was re-executed. Once sealed, later calls cannot dispatch.
    pub fn seal_and_inspect(&self) -> Result<(), InspectionError> {
        self.inspect(false)
    }
    /// Prevent new nested dispatch, cancel outstanding calls and wait for
    /// their futures to stop before the parent can be settled or reconciled.
    pub async fn seal_and_wait(&self) -> Result<(), InspectionError> {
        if self.parent != self.shared.root {
            return Err(InspectionError::Uncertain("only root can inspect".into()));
        }
        {
            let mut state = self
                .shared
                .lifecycle
                .lock()
                .map_err(|_| InspectionError::Uncertain("lifecycle lock poisoned".into()))?;
            if state.sealed {
                return Err(InspectionError::ActiveOrSealed);
            }
            state.sealed = true;
            if state.active > 0 {
                self.shared.cancellation.cancel();
            }
        }
        loop {
            let notified = self.shared.idle.notified();
            let active = self
                .shared
                .lifecycle
                .lock()
                .map_err(|_| InspectionError::Uncertain("lifecycle lock poisoned".into()))?
                .active;
            if active == 0 {
                break;
            }
            notified.await;
        }
        self.inspect(true)
    }
    fn inspect(&self, sealed_by_wait: bool) -> Result<(), InspectionError> {
        if self.parent != self.shared.root {
            return Err(InspectionError::Uncertain("only root can inspect".into()));
        }
        let mut l = self
            .shared
            .lifecycle
            .lock()
            .map_err(|_| InspectionError::Uncertain("lifecycle lock poisoned".into()))?;
        if (l.sealed && !sealed_by_wait) || l.active != 0 {
            return Err(InspectionError::ActiveOrSealed);
        }
        l.sealed = true;
        if self.shared.failed.load(Ordering::Acquire) {
            return Err(InspectionError::Uncertain(
                "nested call failed validation or storage".into(),
            ));
        }
        for (key, entry) in self.shared.owner.store().list(&self.prefix())? {
            let record: Record = serde_json::from_slice(&entry.value)?;
            if matches!(
                record.outcome,
                None | Some(Outcome::Unknown(_)) | Some(Outcome::Pending { .. })
            ) {
                return Err(InspectionError::Uncertain(format!(
                    "unresolved nested operation {}",
                    record.operation_id
                )));
            }
            let s = std::str::from_utf8(&key)
                .map_err(|_| InspectionError::Uncertain("invalid nested key".into()))?;
            let mut parts = s.rsplit('/');
            let index: u64 = parts
                .next()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| InspectionError::Uncertain("invalid nested index".into()))?;
            let parent = parts
                .next()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| InspectionError::Uncertain("invalid nested parent".into()))?;
            if l.counts.get(&parent).is_some_and(|count| index >= *count) {
                return Err(InspectionError::Uncertain(
                    "replay omitted a recorded nested call".into(),
                ));
            }
        }
        Ok(())
    }
    async fn call_inner(&self, name: &str, arguments: Value) -> Outcome {
        let _active = match self.enter() {
            Ok(active) => active,
            Err(outcome) => return outcome,
        };
        let _serial = match self.serial.try_lock() {
            Ok(guard) => guard,
            Err(_) => return self.fail("concurrent nested calls have no stable order"),
        };
        if self.depth >= self.shared.max_depth {
            return self.fail("nested depth limit exceeded");
        }
        let index = match self.shared.lifecycle.lock() {
            Ok(mut l) => {
                let n = l.counts.entry(self.parent).or_default();
                let Some(next) = n.checked_add(1) else {
                    return self.fail("nested call index exhausted");
                };
                let index = *n;
                *n = next;
                index
            }
            Err(_) => return self.fail("nested lifecycle lock poisoned"),
        };
        let key = self.key(self.parent, index);
        let store = self.shared.owner.store();
        let entry = match store.get(&key) {
            Ok(v) => v,
            Err(e) => return self.fail(e.to_string()),
        };
        let reg = match self.shared.tools.get(name) {
            Some(v) => v,
            None => return self.fail(format!("nested tool unavailable: {name}")),
        };
        let (record, version) = if let Some(entry) = entry {
            let record: Record = match serde_json::from_slice(&entry.value) {
                Ok(v) => v,
                Err(e) => return self.fail(format!("invalid nested record: {e}")),
            };
            if record.name != name || record.arguments != arguments || record.policy != reg.replay {
                return self.fail("nested call identity, arguments, or replay policy changed");
            }
            if let Some(outcome) = record.outcome {
                return outcome;
            }
            if record.policy != ReplayPolicy::Safe || reg.replay != ReplayPolicy::Safe {
                return self.fail("interrupted unsafe nested call requires reconciliation");
            }
            (record, entry.version)
        } else {
            let invocation = Invocation {
                operation_id: Uuid::new_v4(),
                call: ToolCall {
                    id: format!("nested-{index}"),
                    name: name.into(),
                    arguments: arguments.to_string(),
                },
            };
            let prepared = match crate::tool::prepare(
                reg.tool.as_ref(),
                &invocation,
                &self.shared.cancellation,
                self.shared.timeout,
                |schema, args| jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(args)),
            )
            .await
            {
                Ok(p) => p,
                Err(outcome) => {
                    return self.fail(format!(
                        "nested preparation failed: {}",
                        outcome.tool_content()
                    ));
                }
            };
            let record = Record {
                name: name.into(),
                arguments,
                prepared: prepared.arguments().clone(),
                policy: reg.replay,
                operation_id: invocation.operation_id,
                outcome: None,
            };
            let value = match serde_json::to_vec(&record) {
                Ok(v) => v,
                Err(e) => return self.fail(e.to_string()),
            };
            if let Err(e) = store.commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value,
                }],
            ) {
                return self.fail(format!("nested intent commit: {e}"));
            }
            let entry = match store.get(&key) {
                Ok(Some(v)) => v,
                Ok(None) => return self.fail("nested intent missing after commit"),
                Err(e) => return self.fail(e.to_string()),
            };
            (record, entry.version)
        };
        let invocation = Invocation {
            operation_id: record.operation_id,
            call: ToolCall {
                id: format!("nested-{index}"),
                name: name.into(),
                arguments: record.arguments.to_string(),
            },
        };
        let prepared = match crate::tool::restore(
            reg.tool.as_ref(),
            &invocation,
            record.prepared.clone(),
            |schema, args| jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(args)),
        ) {
            Ok(p) => p,
            Err(_) => return self.fail("stored nested arguments no longer validate"),
        };
        let child = Arc::new(Self {
            shared: self.shared.clone(),
            parent: record.operation_id,
            depth: self.depth + 1,
            serial: tokio::sync::Mutex::new(()),
        });
        if let Ok(mut l) = self.shared.lifecycle.lock() {
            l.counts.insert(record.operation_id, 0);
        } else {
            return self.fail("nested lifecycle lock poisoned");
        }
        let reply = crate::tool::execute(
            reg.tool.as_ref(),
            &invocation,
            prepared,
            None,
            child,
            &self.shared.cancellation,
            self.shared.timeout,
        )
        .await;
        let outcome = if reply.state.is_some() {
            Outcome::Unknown("nested extension state requires reconciliation".into())
        } else {
            reply.outcome
        };
        // An uncertain descendant poisons the ancestor even if a tool ignored it.
        let outcome = if self.shared.failed.load(Ordering::Acquire) {
            Outcome::Unknown("nested descendant requires reconciliation".into())
        } else {
            outcome
        };
        let mut completed = record;
        completed.outcome = Some(outcome.clone());
        let value = match serde_json::to_vec(&completed) {
            Ok(v) => v,
            Err(e) => return self.fail(e.to_string()),
        };
        if let Err(e) = store.commit(
            &[Check {
                key: key.clone(),
                expected: Expected::Version(version),
            }],
            &[Mutation::Put { key, value }],
        ) {
            return self.fail(format!("nested result commit: {e}"));
        }
        outcome
    }
}
#[async_trait::async_trait]
impl ToolHost for NestedHost {
    fn tools(&self) -> Vec<ToolSpec> {
        let mut tools: Vec<_> = self.shared.tools.values().map(|t| t.tool.spec()).collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }
    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        self.call_inner(name, arguments).await
    }
    async fn call_batch(&self, calls: Vec<(String, Value)>) -> Vec<Outcome> {
        let mut outcomes = Vec::with_capacity(calls.len());
        for (name, args) in calls {
            outcomes.push(self.call_inner(&name, args).await);
        }
        outcomes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use pi_rs_core::{Tool, ToolContext};
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    struct Fake {
        calls: Arc<AtomicUsize>,
        pending: bool,
    }
    #[async_trait]
    impl Tool for Fake {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "fake".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, args: Value, _: ToolContext) -> Outcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.pending {
                std::future::pending::<()>().await;
            }
            Outcome::Succeeded(args.to_string())
        }
    }
    fn host(
        path: &std::path::Path,
        root: Uuid,
        calls: Arc<AtomicUsize>,
        policy: ReplayPolicy,
        pending: bool,
        depth: usize,
    ) -> Arc<NestedHost> {
        NestedHost::new(
            Arc::new(OwnedStore::open(path).unwrap()),
            root,
            vec![RegisteredTool {
                tool: Arc::new(Fake { calls, pending }),
                replay: policy,
            }],
            CancellationToken::new(),
            Duration::from_secs(10),
            depth,
        )
        .unwrap()
    }
    #[tokio::test]
    async fn restart_reuses_complete_and_rejects_changed_or_omitted_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let calls = Arc::new(AtomicUsize::new(0));
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        assert_eq!(
            h.call("fake", json!({"x":1})).await,
            Outcome::Succeeded("{\"x\":1}".into())
        );
        h.seal_and_inspect().unwrap();
        drop(h);
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        assert!(matches!(
            h.call("fake", json!({"x":1})).await,
            Outcome::Succeeded(_)
        ));
        h.seal_and_inspect().unwrap();
        drop(h);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        assert!(matches!(
            h.call("fake", json!({"x":2})).await,
            Outcome::Unknown(_)
        ));
        assert!(h.seal_and_inspect().is_err());
        drop(h);
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        assert!(h.seal_and_inspect().is_err());
    }
    #[tokio::test]
    async fn restart_never_reexecutes_interrupted_unsafe_but_replays_safe() {
        for policy in [ReplayPolicy::Unsafe, ReplayPolicy::Safe] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let root = Uuid::new_v4();
            let calls = Arc::new(AtomicUsize::new(0));
            let h = host(&path, root, calls.clone(), policy, true, 2);
            let mut fut = Box::pin(h.call("fake", json!({})));
            assert!(futures_util::poll!(fut.as_mut()).is_pending());
            drop(fut);
            assert!(h.seal_and_inspect().is_err());
            drop(h);
            let h = host(&path, root, calls.clone(), policy, false, 2);
            let result = h.call("fake", json!({})).await;
            if policy == ReplayPolicy::Safe {
                assert!(matches!(result, Outcome::Succeeded(_)));
                h.seal_and_inspect().unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            } else {
                assert!(matches!(result, Outcome::Unknown(_)));
                assert!(h.seal_and_inspect().is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }
    #[tokio::test]
    async fn depth_and_concurrent_calls_poison_inspection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let calls = Arc::new(AtomicUsize::new(0));
        let h = host(
            &path,
            Uuid::new_v4(),
            calls.clone(),
            ReplayPolicy::Unsafe,
            false,
            0,
        );
        assert!(matches!(
            h.call("fake", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert!(h.seal_and_inspect().is_err());
        drop(h);
        let h = host(&path, Uuid::new_v4(), calls, ReplayPolicy::Unsafe, true, 1);
        let mut first = Box::pin(h.call("fake", json!({})));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        assert!(matches!(
            h.call("fake", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert!(matches!(
            h.seal_and_inspect(),
            Err(InspectionError::ActiveOrSealed)
        ));
        drop(first);
        assert!(h.seal_and_inspect().is_err());
    }
    struct IgnoringParent;
    #[async_trait]
    impl Tool for IgnoringParent {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "parent".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
        async fn invoke_with_host(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> Outcome {
            let _ = host.call("fake", json!({})).await;
            Outcome::Succeeded("ignored child".into())
        }
    }
    #[tokio::test]
    async fn ignored_unknown_descendant_blocks_root_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let calls = Arc::new(AtomicUsize::new(0));
        let make = || {
            NestedHost::new(
                Arc::new(OwnedStore::open(&path).unwrap()),
                root,
                vec![
                    RegisteredTool {
                        tool: Arc::new(IgnoringParent),
                        replay: ReplayPolicy::Safe,
                    },
                    RegisteredTool {
                        tool: Arc::new(Fake {
                            calls: calls.clone(),
                            pending: true,
                        }),
                        replay: ReplayPolicy::Unsafe,
                    },
                ],
                CancellationToken::new(),
                Duration::from_millis(10),
                3,
            )
            .unwrap()
        };
        let h = make();
        // The intermediate parent can ignore the child's unknown result, but
        // inspection always scans every descendant record.
        let _ = h.call("parent", json!({})).await;
        assert!(h.seal_and_inspect().is_err());
        drop(h);
        let h = make();
        let _ = h.call("parent", json!({})).await;
        assert!(h.seal_and_inspect().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn intent_compare_and_swap_never_overwrites_an_existing_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let calls = Arc::new(AtomicUsize::new(0));
        let h = host(&path, root, calls, ReplayPolicy::Unsafe, false, 1);
        let key = h.key(root, 0);
        let store = h.shared.owner.store();
        store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value: b"original".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            store.commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Missing
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value: b"replacement".to_vec()
                }]
            ),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert_eq!(store.get(&key).unwrap().unwrap().value, b"original");
        assert!(h.seal_and_inspect().is_err());
    }
}
