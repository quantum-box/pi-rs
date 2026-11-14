//! Opt-in, display-only tool progress. A snapshot is a replacement partial result,
//! not a delta or a tool outcome. Reserve this namespace and use a fresh UUID for
//! each invocation attempt. Include the host's lease/task checks in every commit
//! and atomically fence the snapshot with the host's terminal transition.
//!
//! Only committed snapshots survive a crash. Storage errors can have unknown
//! outcomes; reopen before retrying. Never use a partial as a tool result or as
//! evidence that an effect is safe to replay. SQLite operations are synchronous.

use durable_store::{Check, Expected, Mutation, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const MAX_PROGRESS_BYTES: usize = 1024 * 1024;
// Leave room for the completed envelope around a legacy maximum-sized partial.
const MAX_COMPLETED_BYTES: usize = MAX_PROGRESS_BYTES + 32;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] durable_store::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("tool progress exceeds the encoded byte limit")]
    TooLarge,
    #[error("tool progress is already finished")]
    Finished,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "partial", rename_all = "snake_case")]
pub enum State {
    Running(Option<Value>),
    Aborted(Option<Value>),
    Completed,
}

pub struct Snapshot {
    id: Uuid,
    version: Option<u64>,
    state: State,
    retained_partial: Option<Value>,
}

pub struct Prepared {
    pub checks: Vec<Check>,
    pub mutations: Vec<Mutation>,
}

impl Snapshot {
    pub fn load(store: &Store, id: Uuid) -> Result<Self, Error> {
        let entry = store.get(&key(id))?;
        let (version, state, retained_partial) = match entry {
            Some(entry) => {
                if entry.value.len() > MAX_COMPLETED_BYTES {
                    return Err(Error::TooLarge);
                }
                let value: Value = serde_json::from_slice(&entry.value)?;
                let state: State = serde_json::from_value(value.clone())?;
                if entry.value.len() > MAX_PROGRESS_BYTES && state != State::Completed {
                    return Err(Error::TooLarge);
                }
                let retained = if state == State::Completed {
                    value
                        .get("retained_partial")
                        .filter(|v| !v.is_null())
                        .cloned()
                } else {
                    None
                };
                (Some(entry.version), state, retained)
            }
            None => (None, State::Running(None), None),
        };
        Ok(Self {
            id,
            version,
            state,
            retained_partial,
        })
    }
    pub fn is_persisted(&self) -> bool {
        self.version.is_some()
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    /// Last committed display-only partial retained after completion, if any.
    pub fn retained_partial(&self) -> Option<&Value> {
        self.retained_partial.as_ref()
    }
    /// Stage a complete replacement. Failure leaves the previous value intact.
    pub fn update(&mut self, partial: Value) -> Result<(), Error> {
        if !matches!(self.state, State::Running(_)) {
            return Err(Error::Finished);
        }
        let state = State::Running(Some(partial));
        encode(&state)?;
        // Both terminal envelopes must fit before accepting the update.
        if let State::Running(partial) = &state {
            encode(&State::Aborted(partial.clone()))?;
            encode_completed(partial)?;
        }
        self.state = state;
        Ok(())
    }
    /// Commit, then reload before the next checkpoint. Conflicts fence stale writers.
    pub fn prepare_checkpoint(&self) -> Result<Prepared, Error> {
        if !matches!(self.state, State::Running(_)) {
            return Err(Error::Finished);
        }
        self.prepare(&self.state)
    }
    pub fn prepare_abort(&self) -> Result<Prepared, Error> {
        let State::Running(partial) = &self.state else {
            return Err(Error::Finished);
        };
        self.prepare(&State::Aborted(partial.clone()))
    }
    pub fn prepare_complete(&self) -> Result<Prepared, Error> {
        let State::Running(partial) = &self.state else {
            return Err(Error::Finished);
        };
        self.prepare_bytes(encode_completed(partial)?)
    }
    fn prepare(&self, state: &State) -> Result<Prepared, Error> {
        self.prepare_bytes(encode(state)?)
    }
    fn prepare_bytes(&self, value: Vec<u8>) -> Result<Prepared, Error> {
        let key = key(self.id);
        Ok(Prepared {
            checks: vec![Check {
                key: key.clone(),
                expected: self.version.map_or(Expected::Missing, Expected::Version),
            }],
            mutations: vec![Mutation::Put {
                key,
                value,
            }],
        })
    }
}
fn key(id: Uuid) -> Vec<u8> {
    format!("durable/tool-progress/{id}").into_bytes()
}
fn encode(state: &State) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() > MAX_PROGRESS_BYTES {
        return Err(Error::TooLarge);
    }
    Ok(bytes)
}

fn encode_completed(partial: &Option<Value>) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "state": "completed",
        "retained_partial": partial,
    }))?;
    if bytes.len() > MAX_COMPLETED_BYTES {
        return Err(Error::TooLarge);
    }
    Ok(bytes)
}

use pi_rs_core::{ExtensionResult, Outcome, ToolHost, ToolSpec, UiRequest, UiResponse};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// The host decides checkpoint frequency, commits with its own lease checks, and
/// handles persistence errors. This callback receives replacement partials.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn update(&self, partial: Value) -> ExtensionResult<()>;
}

/// Invocation-scoped forwarding host. Dropping the scope stops new callbacks;
/// a callback already in flight must be fenced by the host's atomic store checks.
pub struct Scope {
    open: Arc<AtomicBool>,
}
impl Drop for Scope {
    fn drop(&mut self) {
        self.open.store(false, Ordering::Release);
    }
}
struct ProgressHost {
    inner: Arc<dyn ToolHost>,
    sink: Arc<dyn Sink>,
    open: Arc<AtomicBool>,
}
/// Wrap a host for one invocation and retain the scope until invocation ends.
/// The sink controls storage and observation; no queue, retry, timer or implicit
/// terminal commit is provided. Updates after scope drop are ignored.
pub fn scoped_host(inner: Arc<dyn ToolHost>, sink: Arc<dyn Sink>) -> (Arc<dyn ToolHost>, Scope) {
    let open = Arc::new(AtomicBool::new(true));
    (
        Arc::new(ProgressHost {
            inner,
            sink,
            open: open.clone(),
        }),
        Scope { open },
    )
}
#[async_trait::async_trait]
impl ToolHost for ProgressHost {
    fn tools(&self) -> Vec<ToolSpec> {
        self.inner.tools()
    }
    fn supports_progress(&self) -> bool {
        true
    }
    async fn report_progress(&self, partial: Value) -> ExtensionResult<()> {
        if self.open.load(Ordering::Acquire) {
            self.sink.update(partial).await
        } else {
            Ok(())
        }
    }
    fn has_ui(&self) -> bool {
        self.inner.has_ui()
    }
    async fn ui(&self, request: UiRequest) -> ExtensionResult<UiResponse> {
        self.inner.ui(request).await
    }
    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        self.inner.call(name, arguments).await
    }
    async fn call_batch(&self, calls: Vec<(String, Value)>) -> Vec<Outcome> {
        self.inner.call_batch(calls).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pi_rs_core::{CancellationToken, Tool, ToolContext};
    use serde_json::json;
    use std::time::Duration;

    struct Host;
    #[async_trait::async_trait]
    impl ToolHost for Host {
        fn tools(&self) -> Vec<ToolSpec> {
            vec![]
        }
        async fn call(&self, _: &str, _: Value) -> Outcome {
            Outcome::Skipped("unused".into())
        }
    }
    struct Checkpoint {
        store: Arc<Store>,
        id: Uuid,
        cancel: CancellationToken,
    }
    #[async_trait::async_trait]
    impl Sink for Checkpoint {
        async fn update(&self, partial: Value) -> ExtensionResult<()> {
            let mut snapshot = Snapshot::load(&self.store, self.id).map_err(|e| e.to_string())?;
            snapshot.update(partial).map_err(|e| e.to_string())?;
            let prepared = snapshot.prepare_checkpoint().map_err(|e| e.to_string())?;
            self.store
                .commit(&prepared.checks, &prepared.mutations)
                .map_err(|e| e.to_string())?;
            self.cancel.cancel();
            Ok(())
        }
    }
    struct ReportingTool;
    #[async_trait::async_trait]
    impl Tool for ReportingTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "report".into(),
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
            host.report_progress(json!({"content":[{"type":"text","text":"saved"}]}))
                .await
                .unwrap();
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn cancellation_reopens_committed_partial_and_fences_late_callbacks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("progress.db");
        let id = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let store = Arc::new(Store::open(&path).unwrap());
        let sink = Arc::new(Checkpoint {
            store: store.clone(),
            id,
            cancel: cancel.clone(),
        });
        let (host, scope) = scoped_host(Arc::new(Host), sink);
        let invocation = pi_rs_core::Invocation {
            operation_id: Uuid::new_v4(),
            call: pi_rs_core::ToolCall {
                id: "call".into(),
                name: "report".into(),
                arguments: "{}".into(),
            },
        };
        let prepared = crate::tool::prepare(
            &ReportingTool,
            &invocation,
            &cancel,
            Duration::from_secs(1),
            |_, _| true,
        )
        .await
        .unwrap();
        let result = crate::tool::execute(
            &ReportingTool,
            &invocation,
            prepared,
            None,
            host.clone(),
            &cancel,
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(result.outcome, Outcome::Unknown(_)));
        drop(scope);
        host.report_progress(json!("late")).await.unwrap();
        drop(host);
        drop(store);
        let reopened = Store::open(&path).unwrap();
        let snapshot = Snapshot::load(&reopened, id).unwrap();
        assert_eq!(
            snapshot.state(),
            &State::Running(Some(json!({"content":[{"type":"text","text":"saved"}]})))
        );
        let aborted = snapshot.prepare_abort().unwrap();
        reopened
            .commit(&aborted.checks, &aborted.mutations)
            .unwrap();
        assert!(matches!(
            Snapshot::load(&reopened, id).unwrap().state(),
            State::Aborted(Some(_))
        ));
    }
    #[test]
    fn failed_host_guard_leaves_snapshot_unchanged() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let mut snapshot = Snapshot::load(&store, id).unwrap();
        snapshot.update(json!("uncommitted")).unwrap();
        let mut prepared = snapshot.prepare_abort().unwrap();
        prepared.checks.push(Check {
            key: b"host-lease".to_vec(),
            expected: Expected::Version(1),
        });
        assert!(matches!(
            store.commit(&prepared.checks, &prepared.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(!Snapshot::load(&store, id).unwrap().is_persisted());
    }
    #[test]
    fn completion_retains_partial_with_bounded_terminal_envelope_and_reads_legacy() {
        assert_eq!(
            serde_json::from_str::<State>(r#"{"state":"completed"}"#).unwrap(),
            State::Completed
        );
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let mut snapshot = Snapshot::load(&store, id).unwrap();
        let overhead = serde_json::to_vec(&State::Aborted(Some(json!(""))))
            .unwrap()
            .len();
        let max = "x".repeat(MAX_PROGRESS_BYTES - overhead);
        snapshot.update(json!(max)).unwrap();
        assert!(matches!(
            snapshot.update(json!("x".repeat(MAX_PROGRESS_BYTES - overhead + 1))),
            Err(Error::TooLarge)
        ));
        let checkpoint = snapshot.prepare_checkpoint().unwrap();
        store
            .commit(&checkpoint.checks, &checkpoint.mutations)
            .unwrap();
        let snapshot = Snapshot::load(&store, id).unwrap();
        let complete = snapshot.prepare_complete().unwrap();
        store.commit(&complete.checks, &complete.mutations).unwrap();
        let completed = Snapshot::load(&store, id).unwrap();
        assert_eq!(completed.state(), &State::Completed);
        assert_eq!(completed.retained_partial(), Some(&json!(max)));
    }
    #[test]
    fn old_maximum_checkpoint_can_complete_after_upgrade() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let overhead = serde_json::to_vec(&State::Aborted(Some(json!(""))))
            .unwrap()
            .len();
        let partial = json!("x".repeat(1024 * 1024 - overhead));
        let old = serde_json::to_vec(&State::Running(Some(partial.clone()))).unwrap();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key(id),
                    value: old,
                }],
            )
            .unwrap();
        let complete = Snapshot::load(&store, id)
            .unwrap()
            .prepare_complete()
            .unwrap();
        store.commit(&complete.checks, &complete.mutations).unwrap();
        let completed = Snapshot::load(&store, id).unwrap();
        assert_eq!(completed.state(), &State::Completed);
        assert_eq!(completed.retained_partial(), Some(&partial));
    }
    #[test]
    fn stale_commit_and_oversize_are_rejected() {
        let store = Store::memory().unwrap();
        let id = Uuid::new_v4();
        let mut a = Snapshot::load(&store, id).unwrap();
        let mut b = Snapshot::load(&store, id).unwrap();
        a.update(json!("first")).unwrap();
        b.update(json!("stale")).unwrap();
        let p = a.prepare_checkpoint().unwrap();
        store.commit(&p.checks, &p.mutations).unwrap();
        let p = b.prepare_checkpoint().unwrap();
        assert!(matches!(
            store.commit(&p.checks, &p.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        let mut current = Snapshot::load(&store, id).unwrap();
        assert!(matches!(
            current.update(json!("x".repeat(MAX_PROGRESS_BYTES))),
            Err(Error::TooLarge)
        ));
        assert_eq!(current.state(), &State::Running(Some(json!("first"))));
        let stale = current.prepare_checkpoint().unwrap();
        let done = current.prepare_complete().unwrap();
        store.commit(&done.checks, &done.mutations).unwrap();
        assert!(matches!(
            store.commit(&stale.checks, &stale.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(matches!(
            Snapshot::load(&store, id).unwrap().prepare_abort(),
            Err(Error::Finished)
        ));
    }
}
