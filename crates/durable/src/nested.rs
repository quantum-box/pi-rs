//! Persistent nested tool calls. The runtime must retain the returned host,
//! wait for the parent invocation to finish, and call `seal_and_inspect` before
//! committing *any* parent result or state. An error means the parent must be
//! blocked for reconciliation, even if the parent returned success.
//!
//! Exactly one execution of a given root operation may be alive at a time,
//! including detached tasks. The scheduler must cancel and join all old work
//! before replay and keep `OwnedStore` ownership for the whole execution. Calls
//! made concurrently on one host are rejected. The opt-in keyed batch admits
//! calls in order and polls their persisted pending jobs concurrently.
//!
//! Tool argument preparation must be side-effect free. Nested tools that return
//! extension state are unsupported: their result is treated as uncertain. The
//! caller must not use this host for tools requiring transactional state updates.

use crate::{recovery::ReplayPolicy, runtime::RegisteredTool};
use durable_store::{Check, Expected, Mutation, owner::OwnedStore};
use futures_util::FutureExt;
use pi_rs_core::{CancellationToken, Invocation, Outcome, ToolCall, ToolHost, ToolSpec};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolution: Option<Resolution>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
struct Resolution {
    reference: String,
    outcome: Outcome,
    #[serde(default)]
    cancelled: bool,
}

/// A pending remote job and its stable identity. References are opaque.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingJob {
    pub operation_id: Uuid,
    pub reference: String,
}

pub(crate) fn pending_jobs(
    store: &durable_store::Store,
    root: Uuid,
) -> Result<Vec<PendingJob>, crate::runtime::Error> {
    let mut jobs = Vec::new();
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if let Some(Outcome::Pending { reference }) = record.outcome {
            jobs.push(PendingJob {
                operation_id: record.operation_id,
                reference,
            });
        }
    }
    Ok(jobs)
}

// Automatic replay is restricted to a root with only terminal records and at
// least one explicitly resolved job. Unresolved descendants remain barriers.
pub(crate) fn resolved_for_replay(
    store: &durable_store::Store,
    root: Uuid,
) -> Result<bool, crate::runtime::Error> {
    let mut resolved = false;
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if matches!(
            record.outcome,
            None | Some(Outcome::Pending { .. }) | Some(Outcome::Unknown(_))
        ) {
            return Ok(false);
        }
        resolved |= record.resolution.is_some();
    }
    Ok(resolved)
}

pub(crate) fn resolve_pending(
    store: &durable_store::Store,
    (conversation_key, conversation_version): (Vec<u8>, u64),
    root: Uuid,
    operation: Uuid,
    reference: &str,
    outcome: Outcome,
    cancelled: bool,
) -> Result<(), crate::runtime::Error> {
    use crate::runtime::Error;
    if !matches!(
        outcome,
        Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. } | Outcome::Failed(_)
    ) {
        return Err(Error::Invalid(
            "nested reconciliation requires a known terminal outcome".into(),
        ));
    }
    let mut found = None;
    for (key, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if record.operation_id == operation {
            if found.is_some() {
                return Err(Error::Invalid("duplicate nested operation ID".into()));
            }
            found = Some((key, entry, record));
        }
    }
    let (key, entry, mut record) =
        found.ok_or_else(|| Error::Invalid("nested operation not found".into()))?;
    let resolution = Resolution {
        reference: reference.into(),
        outcome: outcome.clone(),
        cancelled,
    };
    if let Some(existing) = &record.resolution {
        if *existing != resolution {
            return Err(Error::Invalid("conflicting nested resolution".into()));
        }
        // Check the conversation even on a retry.
        store.commit(
            &[
                Check {
                    key: conversation_key,
                    expected: Expected::Version(conversation_version),
                },
                Check {
                    key,
                    expected: Expected::Version(entry.version),
                },
            ],
            &[],
        )?;
        return Ok(());
    }
    if record.outcome
        != Some(Outcome::Pending {
            reference: reference.into(),
        })
    {
        return Err(Error::Invalid(
            "nested pending reference or outcome changed".into(),
        ));
    }
    record.outcome = Some(outcome);
    record.resolution = Some(resolution);
    store.commit(
        &[
            Check {
                key: conversation_key,
                expected: Expected::Version(conversation_version),
            },
            Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            },
        ],
        &[Mutation::Put {
            key,
            value: serde_json::to_vec(&record)?,
        }],
    )?;
    Ok(())
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
    non_pending_failure: AtomicBool,
    idle: tokio::sync::Notify,
}
#[derive(Default)]
struct Lifecycle {
    sealed: bool,
    active: usize,
    counts: HashMap<Uuid, u64>,
    keyed: HashSet<Vec<u8>>,
    closed: HashSet<Uuid>,
    active_by_parent: HashMap<Uuid, usize>,
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

struct Active(Arc<Shared>, Uuid);
impl Drop for Active {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.lifecycle.lock() {
            state.active -= 1;
            if let Some(count) = state.active_by_parent.get_mut(&self.1) {
                *count -= 1;
            }
            self.0.idle.notify_one();
        } else {
            self.0.non_pending_failure.store(true, Ordering::Release);
            self.0.failed.store(true, Ordering::Release);
        }
    }
}

// Revoke each invocation's host as soon as its dispatch finishes or is dropped.
pub(crate) struct InvocationScope(pub(crate) Arc<NestedHost>);
impl Drop for InvocationScope {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.shared.lifecycle.lock() {
            state.closed.insert(self.0.parent);
            if state
                .active_by_parent
                .get(&self.0.parent)
                .copied()
                .unwrap_or(0)
                != 0
            {
                self.0
                    .shared
                    .non_pending_failure
                    .store(true, Ordering::Release);
                self.0.shared.failed.store(true, Ordering::Release);
                self.0.shared.cancellation.cancel();
            }
        } else {
            self.0
                .shared
                .non_pending_failure
                .store(true, Ordering::Release);
            self.0.shared.failed.store(true, Ordering::Release);
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
                non_pending_failure: AtomicBool::new(false),
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
    fn explicit_key(&self, parent: Uuid, key: &str) -> Vec<u8> {
        let encoded: String = key.as_bytes().iter().map(|b| format!("{b:02x}")).collect();
        format!("nested/{}/{parent}/k{encoded}", self.shared.root).into_bytes()
    }
    /// Execute or reattach to a nested call identified by a caller-scoped key.
    /// Explicit keys follow upstream's path-segment rules. The same key may be
    /// reused only with the same tool and JSON arguments.
    pub async fn call_keyed(&self, key: &str, name: &str, arguments: Value) -> Outcome {
        if !valid_key(key) {
            return self.fail("invalid nested call key");
        }
        self.call_inner(Some(key), name, arguments, false, true)
            .await
    }
    /// Admit up to eight distinct keyed calls in order, then poll their persisted
    /// pending jobs concurrently. The parent remains active until all polls stop.
    /// A failed admission stops later admissions; no job is redispatched to poll.
    pub async fn call_keyed_batch(&self, calls: Vec<(String, String, Value)>) -> Vec<Outcome> {
        const MAX_BATCH: usize = 8;
        let mut keys = HashSet::new();
        if calls.len() > MAX_BATCH
            || calls
                .iter()
                .any(|(key, _, _)| !valid_key(key) || !keys.insert(key.clone()))
        {
            let error = self.fail("invalid or oversized keyed nested batch");
            return vec![error; calls.len().max(1)];
        }
        let _active = match self.enter() {
            Ok(active) => active,
            Err(outcome) => return vec![outcome; calls.len()],
        };
        let _serial = match self.serial.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                return vec![
                    self.fail("concurrent nested calls have no stable order");
                    calls.len()
                ];
            }
        };
        let mut outcomes = Vec::with_capacity(calls.len());
        for (key, name, arguments) in &calls {
            let result = self
                .call_inner(Some(key), name, arguments.clone(), true, false)
                .await;
            let stop = matches!(result, Outcome::Unknown(_));
            outcomes.push(result);
            if stop {
                self.shared
                    .non_pending_failure
                    .store(true, Ordering::Release);
                self.shared.failed.store(true, Ordering::Release);
                outcomes.resize(
                    calls.len(),
                    Outcome::Skipped("earlier nested admission failed".into()),
                );
                return outcomes;
            }
        }
        futures_util::future::join_all(calls.iter().zip(outcomes).map(
            |((key, name, _), outcome)| async move {
                if !matches!(outcome, Outcome::Pending { .. }) {
                    return outcome;
                }
                let Some(reg) = self.shared.tools.get(name) else {
                    return self.fail("nested tool unavailable");
                };
                if !reg.tool.can_poll() {
                    return outcome;
                }
                let storage_key = self.explicit_key(self.parent, key);
                let entry = match self.shared.owner.store().get(&storage_key) {
                    Ok(Some(entry)) => entry,
                    _ => return self.fail("nested pending record unavailable"),
                };
                let record: Record = match serde_json::from_slice(&entry.value) {
                    Ok(record) => record,
                    Err(_) => return self.fail("invalid nested pending record"),
                };
                if record.outcome != Some(outcome) {
                    return self.fail("nested pending record changed");
                }
                self.poll_pending(&storage_key, record, entry.version, reg)
                    .await
            },
        ))
        .await
    }
    fn fail(&self, reason: impl Into<String>) -> Outcome {
        self.shared
            .non_pending_failure
            .store(true, Ordering::Release);
        self.shared.failed.store(true, Ordering::Release);
        Outcome::Unknown(reason.into())
    }
    fn pending_failure(&self, reason: impl Into<String>) -> Outcome {
        self.shared.failed.store(true, Ordering::Release);
        Outcome::Unknown(reason.into())
    }
    pub(crate) fn only_pending_failure(&self) -> bool {
        self.shared.failed.load(Ordering::Acquire)
            && !self.shared.non_pending_failure.load(Ordering::Acquire)
    }
    fn enter(&self) -> Result<Active, Outcome> {
        let mut l = self
            .shared
            .lifecycle
            .lock()
            .map_err(|_| self.fail("nested lifecycle lock poisoned"))?;
        if l.sealed || l.closed.contains(&self.parent) || self.shared.failed.load(Ordering::Acquire)
        {
            return Err(self.fail("nested host sealed or failed"));
        }
        l.active += 1;
        *l.active_by_parent.entry(self.parent).or_default() += 1;
        Ok(Active(self.shared.clone(), self.parent))
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
            let member = parts
                .next()
                .ok_or_else(|| InspectionError::Uncertain("invalid nested key".into()))?;
            let parent = parts
                .next()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| InspectionError::Uncertain("invalid nested parent".into()))?;
            if let Some(count) = l.counts.get(&parent) {
                let omitted = if member.starts_with('k') {
                    !l.keyed.contains(&key)
                } else {
                    let index: u64 = member
                        .parse()
                        .map_err(|_| InspectionError::Uncertain("invalid nested index".into()))?;
                    index >= *count
                };
                if omitted {
                    return Err(InspectionError::Uncertain(
                        "replay omitted a recorded nested call".into(),
                    ));
                }
            }
        }
        Ok(())
    }
    async fn call_inner(
        &self,
        explicit: Option<&str>,
        name: &str,
        arguments: Value,
        serial_held: bool,
        poll: bool,
    ) -> Outcome {
        let _active = match self.enter() {
            Ok(active) => active,
            Err(outcome) => return outcome,
        };
        let _serial = if serial_held {
            None
        } else {
            match self.serial.try_lock() {
                Ok(guard) => Some(guard),
                Err(_) => return self.fail("concurrent nested calls have no stable order"),
            }
        };
        if self.depth >= self.shared.max_depth {
            return self.fail("nested depth limit exceeded");
        }
        let (key, call_id) = match self.shared.lifecycle.lock() {
            Ok(mut l) => {
                if let Some(member) = explicit {
                    let key = self.explicit_key(self.parent, member);
                    l.keyed.insert(key.clone());
                    (key, format!("nested-key-{member}"))
                } else {
                    let n = l.counts.entry(self.parent).or_default();
                    let Some(next) = n.checked_add(1) else {
                        return self.fail("nested call index exhausted");
                    };
                    let index = *n;
                    *n = next;
                    (self.key(self.parent, index), format!("nested-{index}"))
                }
            }
            Err(_) => return self.fail("nested lifecycle lock poisoned"),
        };
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
            if let Some(outcome) = &record.outcome {
                if poll && matches!(outcome, Outcome::Pending { .. }) && reg.tool.can_poll() {
                    return self.poll_pending(&key, record, entry.version, reg).await;
                }
                return outcome.clone();
            }
            if record.policy != ReplayPolicy::Safe || reg.replay != ReplayPolicy::Safe {
                return self.fail("interrupted unsafe nested call requires reconciliation");
            }
            (record, entry.version)
        } else {
            let invocation = Invocation {
                operation_id: Uuid::new_v4(),
                call: ToolCall {
                    id: call_id.clone(),
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
                resolution: None,
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
                id: call_id.clone(),
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
        let reply = crate::keyed_tool::scope(
            child.clone(),
            reg.tool.clone(),
            invocation.operation_id,
            child.clone(),
            crate::tool::execute(
                reg.tool.as_ref(),
                &invocation,
                prepared,
                None,
                child,
                &self.shared.cancellation,
                self.shared.timeout,
            ),
        )
        .await;
        let has_state = reply.state.is_some();
        let outcome = if has_state {
            Outcome::Unknown("nested extension state requires reconciliation".into())
        } else {
            reply.outcome
        };
        // An uncertain descendant poisons the ancestor even if a tool ignored it.
        if self.shared.failed.load(Ordering::Acquire) && !has_state {
            // Leave the intent unresolved: a replay-safe intermediate caller
            // must be able to revisit its descendants and reattach to a
            // retained pending reference. Unsafe callers remain non-replayable.
            return self.fail("nested descendant requires reconciliation");
        }
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
            &[Mutation::Put {
                key: key.clone(),
                value,
            }],
        ) {
            return self.fail(format!("nested result commit: {e}"));
        }
        if poll && matches!(outcome, Outcome::Pending { .. }) && reg.tool.can_poll() {
            let version = match store.get(&key) {
                Ok(Some(entry)) => entry.version,
                _ => return self.fail("nested pending record missing after commit"),
            };
            return self.poll_pending(&key, completed, version, reg).await;
        }
        outcome
    }

    async fn poll_pending(
        &self,
        key: &[u8],
        mut record: Record,
        mut version: u64,
        reg: &RegisteredTool,
    ) -> Outcome {
        loop {
            let Some(Outcome::Pending { reference }) = &record.outcome else {
                return self.fail("invalid nested pending record");
            };
            let result = std::panic::AssertUnwindSafe(crate::poll::poll_once(
                reg.tool.as_ref(),
                record.operation_id,
                reference,
                &self.shared.cancellation,
                self.shared.timeout,
            ))
            .catch_unwind()
            .await;
            let outcome = match result {
                Ok(crate::poll::PollResult::Outcome(outcome)) => outcome,
                Ok(crate::poll::PollResult::Cancelled) => {
                    return self.fail("nested polling cancelled");
                }
                Ok(crate::poll::PollResult::Unavailable | crate::poll::PollResult::TimedOut) => {
                    return self
                        .pending_failure("nested polling failed; pending reference retained");
                }
                Err(payload) => {
                    crate::extension::discard_panic_payload(payload);
                    return self
                        .pending_failure("nested polling failed; pending reference retained");
                }
            };
            // Unknown is not a terminal observation. Keep the last accepted
            // reference so a later safe replay can reattach, while poisoning
            // this invocation so an ignoring parent cannot commit success.
            if matches!(outcome, Outcome::Unknown(_)) {
                return self.pending_failure(
                    "nested polling returned an uncertain outcome; pending reference retained",
                );
            }
            record.outcome = Some(outcome.clone());
            let value = match serde_json::to_vec(&record) {
                Ok(value) => value,
                Err(e) => return self.fail(e.to_string()),
            };
            if let Err(e) = self.shared.owner.store().commit(
                &[Check {
                    key: key.to_vec(),
                    expected: Expected::Version(version),
                }],
                &[Mutation::Put {
                    key: key.to_vec(),
                    value,
                }],
            ) {
                return self.fail(format!("nested poll result commit: {e}"));
            }
            if !matches!(outcome, Outcome::Pending { .. }) {
                return outcome;
            }
            version = match self.shared.owner.store().get(key) {
                Ok(Some(entry)) => entry.version,
                _ => return self.fail("nested pending record missing after poll"),
            };
            tokio::select! {
                _ = self.shared.cancellation.cancelled() => return self.fail("nested polling cancelled"),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }
    }
}
fn valid_key(key: &str) -> bool {
    let positive_integer = !key.starts_with('0') && key.bytes().all(|b| b.is_ascii_digit());
    !key.is_empty() && !key.contains('/') && key != "__proto__" && !positive_integer
}

#[async_trait::async_trait]
impl ToolHost for NestedHost {
    fn tools(&self) -> Vec<ToolSpec> {
        let mut tools: Vec<_> = self.shared.tools.values().map(|t| t.tool.spec()).collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name));
        tools
    }
    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        self.call_inner(None, name, arguments, false, true).await
    }
    async fn call_batch(&self, calls: Vec<(String, Value)>) -> Vec<Outcome> {
        let mut outcomes = Vec::with_capacity(calls.len());
        for (name, args) in calls {
            outcomes.push(self.call_inner(None, &name, args, false, true).await);
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

    #[test]
    fn stale_conversation_version_cannot_settle_nested_job() {
        let store = durable_store::Store::memory().unwrap();
        let root = Uuid::new_v4();
        let operation = Uuid::new_v4();
        let conversation = b"conversation-fence".to_vec();
        let key = format!("nested/{root}/{root}/00000000000000000000").into_bytes();
        let record = Record {
            name: "job".into(),
            arguments: serde_json::json!({}),
            prepared: serde_json::json!({}),
            policy: ReplayPolicy::Unsafe,
            operation_id: operation,
            outcome: Some(Outcome::Pending {
                reference: "opaque".into(),
            }),
            resolution: None,
        };
        store
            .commit(
                &[],
                &[
                    Mutation::Put {
                        key: conversation.clone(),
                        value: vec![1],
                    },
                    Mutation::Put {
                        key: key.clone(),
                        value: serde_json::to_vec(&record).unwrap(),
                    },
                ],
            )
            .unwrap();
        let stale = store.get(&conversation).unwrap().unwrap().version;
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: conversation.clone(),
                    value: vec![2],
                }],
            )
            .unwrap();
        assert!(
            resolve_pending(
                &store,
                (conversation, stale),
                root,
                operation,
                "opaque",
                Outcome::Succeeded("done".into()),
                false
            )
            .is_err()
        );
        let retained: Record =
            serde_json::from_slice(&store.get(&key).unwrap().unwrap().value).unwrap();
        assert_eq!(retained.outcome, record.outcome);
    }

    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("panic payload drop");
        }
    }
    struct Polling {
        invoked: AtomicUsize,
        mode: AtomicUsize,
        ids: Mutex<Vec<Uuid>>,
    }
    #[async_trait]
    impl Tool for Polling {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "polling".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        fn can_poll(&self) -> bool {
            true
        }
        async fn invoke(&self, _: Value, context: ToolContext) -> Outcome {
            self.invoked.fetch_add(1, Ordering::SeqCst);
            self.ids.lock().unwrap().push(context.operation_id);
            Outcome::Pending {
                reference: "job".into(),
            }
        }
        async fn poll(&self, reference: &str, context: ToolContext) -> Option<Outcome> {
            assert_eq!(reference, "job");
            self.ids.lock().unwrap().push(context.operation_id);
            match self.mode.load(Ordering::SeqCst) {
                0 => std::future::pending().await,
                1 => None,
                3 => std::panic::panic_any(PanicOnDrop),
                4 => Some(Outcome::Unknown("poll status unavailable".into())),
                _ => Some(Outcome::Succeeded("done".into())),
            }
        }
    }
    fn polling_host(
        path: &std::path::Path,
        root: Uuid,
        tool: Arc<Polling>,
        cancel: CancellationToken,
    ) -> Arc<NestedHost> {
        NestedHost::new(
            Arc::new(OwnedStore::open(path).unwrap()),
            root,
            vec![RegisteredTool {
                tool,
                replay: ReplayPolicy::Unsafe,
            }],
            cancel,
            Duration::from_secs(10),
            2,
        )
        .unwrap()
    }
    #[tokio::test]
    async fn keyed_pending_survives_interruption_failure_and_abort_without_redispatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let tool = Arc::new(Polling {
            invoked: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            ids: Mutex::new(vec![]),
        });
        let cancel = CancellationToken::new();
        let h = polling_host(&path, root, tool.clone(), cancel);
        let mut future = Box::pin(h.call_keyed("job-key", "polling", json!({})));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        drop(future); // process loss after the pending reference was committed
        drop(h);

        tool.mode.store(1, Ordering::SeqCst);
        let h = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(matches!(
            h.call_keyed("job-key", "polling", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert!(h.seal_and_inspect().is_err());
        drop(h);

        tool.mode.store(4, Ordering::SeqCst);
        let h = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(matches!(
            h.call_keyed("job-key", "polling", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert!(h.seal_and_inspect().is_err());
        drop(h);

        tool.mode.store(0, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let h = polling_host(&path, root, tool.clone(), cancel.clone());
        let mut future = Box::pin(h.call_keyed("job-key", "polling", json!({})));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        cancel.cancel();
        assert!(matches!(future.await, Outcome::Unknown(_)));
        assert!(h.seal_and_inspect().is_err());
        drop(h);

        tool.mode.store(3, Ordering::SeqCst);
        let h = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(matches!(
            h.call_keyed("job-key", "polling", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert!(h.seal_and_inspect().is_err());
        drop(h);

        tool.mode.store(2, Ordering::SeqCst);
        let h = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert_eq!(
            h.call_keyed("job-key", "polling", json!({})).await,
            Outcome::Succeeded("done".into())
        );
        h.seal_and_inspect().unwrap();
        assert_eq!(tool.invoked.load(Ordering::SeqCst), 1);
        let ids = tool.ids.lock().unwrap();
        assert!(ids.len() >= 5 && ids.iter().all(|id| *id == ids[0]));
    }

    #[tokio::test]
    async fn timed_out_poll_seals_uncertain_and_reopens_without_redispatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let tool = Arc::new(Polling {
            invoked: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            ids: Mutex::new(vec![]),
        });
        let host = NestedHost::new(
            Arc::new(OwnedStore::open(&path).unwrap()),
            root,
            vec![RegisteredTool {
                tool: tool.clone(),
                replay: ReplayPolicy::Unsafe,
            }],
            CancellationToken::new(),
            Duration::from_millis(20),
            2,
        )
        .unwrap();
        assert!(matches!(
            host.call_keyed("job-key", "polling", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert!(host.seal_and_wait().await.is_err());
        drop(host);
        tool.mode.store(2, Ordering::SeqCst);
        let host = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert_eq!(
            host.call_keyed("job-key", "polling", json!({})).await,
            Outcome::Succeeded("done".into())
        );
        host.seal_and_inspect().unwrap();
        assert_eq!(tool.invoked.load(Ordering::SeqCst), 1);
        let ids = tool.ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|id| *id == ids[0]));
    }
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
    async fn keyed_replay_is_order_independent_and_rejects_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let calls = Arc::new(AtomicUsize::new(0));
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        for (key, value) in [("α", 1), ("b", 2)] {
            assert_eq!(
                h.call_keyed(key, "fake", json!({"x": value})).await,
                Outcome::Succeeded(json!({"x": value}).to_string())
            );
        }
        h.seal_and_inspect().unwrap();
        drop(h);
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        for (key, value) in [("b", 2), ("α", 1), ("b", 2)] {
            assert_eq!(
                h.call_keyed(key, "fake", json!({"x": value})).await,
                Outcome::Succeeded(json!({"x": value}).to_string())
            );
        }
        h.seal_and_inspect().unwrap();
        drop(h);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        assert!(matches!(
            h.call_keyed("b", "fake", json!({"x": 3})).await,
            Outcome::Unknown(_)
        ));
        assert!(h.seal_and_inspect().is_err());
        drop(h);
        let h = host(&path, root, calls.clone(), ReplayPolicy::Unsafe, false, 2);
        h.call_keyed("b", "fake", json!({"x": 2})).await;
        assert!(h.seal_and_inspect().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn keyed_interrupted_calls_follow_their_replay_policy() {
        for policy in [ReplayPolicy::Unsafe, ReplayPolicy::Safe] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let root = Uuid::new_v4();
            let calls = Arc::new(AtomicUsize::new(0));
            let h = host(&path, root, calls.clone(), policy, true, 2);
            let mut future = Box::pin(h.call_keyed("child", "fake", json!({})));
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            drop(future);
            assert!(h.seal_and_inspect().is_err());
            drop(h);
            let h = host(&path, root, calls.clone(), policy, false, 2);
            let result = h.call_keyed("child", "fake", json!({})).await;
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
    async fn explicit_keys_follow_upstream_validation() {
        for key in ["", "a/b", "__proto__", "1", "999999999999999999999999"] {
            let dir = tempfile::tempdir().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let h = host(
                &dir.path().join("db"),
                Uuid::new_v4(),
                calls.clone(),
                ReplayPolicy::Safe,
                false,
                2,
            );
            assert!(matches!(
                h.call_keyed(key, "fake", json!({})).await,
                Outcome::Unknown(_)
            ));
            assert!(h.seal_and_inspect().is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn keyed_batch_rejects_duplicate_invalid_and_oversized_before_dispatch() {
        let cases = vec![
            vec![
                ("same".to_string(), "fake".to_string(), json!({})),
                ("same".to_string(), "fake".to_string(), json!({})),
            ],
            vec![
                ("valid".to_string(), "fake".to_string(), json!({})),
                ("1".to_string(), "fake".to_string(), json!({})),
            ],
            (0..9)
                .map(|i| (format!("key-{i}"), "fake".to_string(), json!({})))
                .collect(),
        ];
        for calls in cases {
            let dir = tempfile::tempdir().unwrap();
            let invoked = Arc::new(AtomicUsize::new(0));
            let h = host(
                &dir.path().join("db"),
                Uuid::new_v4(),
                invoked.clone(),
                ReplayPolicy::Unsafe,
                false,
                2,
            );
            let result = h.call_keyed_batch(calls).await;
            assert!(
                result
                    .iter()
                    .all(|item| matches!(item, Outcome::Unknown(_)))
            );
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
            assert!(h.seal_and_inspect().is_err());
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
    struct PollingParent(Arc<AtomicUsize>);
    #[async_trait]
    impl Tool for PollingParent {
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
            self.0.fetch_add(1, Ordering::SeqCst);
            let _ = host.call("polling", json!({})).await;
            Outcome::Succeeded("ignored child".into())
        }
    }
    #[tokio::test]
    async fn uncertain_poll_through_intermediate_respects_replay_policy() {
        for policy in [ReplayPolicy::Safe, ReplayPolicy::Unsafe] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let root = Uuid::new_v4();
            let calls = Arc::new(AtomicUsize::new(0));
            let child = Arc::new(Polling {
                invoked: AtomicUsize::new(0),
                mode: AtomicUsize::new(4),
                ids: Mutex::new(vec![]),
            });
            let make = || {
                NestedHost::new(
                    Arc::new(OwnedStore::open(&path).unwrap()),
                    root,
                    vec![
                        RegisteredTool {
                            tool: Arc::new(PollingParent(calls.clone())),
                            replay: policy,
                        },
                        RegisteredTool {
                            tool: child.clone(),
                            replay: ReplayPolicy::Unsafe,
                        },
                    ],
                    CancellationToken::new(),
                    Duration::from_secs(1),
                    3,
                )
                .unwrap()
            };
            let h = make();
            assert!(matches!(
                h.call("parent", json!({})).await,
                Outcome::Unknown(_)
            ));
            assert!(h.seal_and_inspect().is_err());
            drop(h);
            child.mode.store(2, Ordering::SeqCst);
            let h = make();
            let result = h.call("parent", json!({})).await;
            if policy == ReplayPolicy::Safe {
                assert_eq!(result, Outcome::Succeeded("ignored child".into()));
                h.seal_and_inspect().unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 2);
                let ids = child.ids.lock().unwrap();
                assert_eq!(ids.len(), 3);
                assert!(ids.iter().all(|id| *id == ids[0]));
            } else {
                assert!(matches!(result, Outcome::Unknown(_)));
                assert!(h.seal_and_inspect().is_err());
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
            assert_eq!(child.invoked.load(Ordering::SeqCst), 1);
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
    async fn dropped_root_dispatch_revokes_escaped_host() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let fake: Arc<dyn Tool> = Arc::new(Fake {
            calls: calls.clone(),
            pending: false,
        });
        let operation_id = Uuid::new_v4();
        let host = NestedHost::new(
            Arc::new(OwnedStore::open(dir.path().join("db")).unwrap()),
            operation_id,
            vec![RegisteredTool {
                tool: fake.clone(),
                replay: ReplayPolicy::Unsafe,
            }],
            CancellationToken::new(),
            Duration::from_secs(10),
            3,
        )
        .unwrap();
        let ordinary: Arc<dyn ToolHost> = host.clone();
        let mut dispatch = Box::pin(crate::keyed_tool::scope(
            host.clone(),
            fake,
            operation_id,
            ordinary,
            std::future::pending::<()>(),
        ));
        assert!(futures_util::poll!(&mut dispatch).is_pending());
        drop(dispatch);
        assert!(matches!(
            host.call_keyed("late", "fake", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(host.seal_and_inspect().is_err());
    }
    #[tokio::test]
    async fn dropped_root_dispatch_cancels_active_nested_call() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let operation_id = Uuid::new_v4();
        let host = host(
            &dir.path().join("db"),
            operation_id,
            calls.clone(),
            ReplayPolicy::Unsafe,
            true,
            3,
        );
        let fake: Arc<dyn Tool> = Arc::new(Fake {
            calls: calls.clone(),
            pending: true,
        });
        let mut dispatch = Box::pin(crate::keyed_tool::scope(
            host.clone(),
            fake,
            operation_id,
            host.clone(),
            std::future::pending::<()>(),
        ));
        assert!(futures_util::poll!(dispatch.as_mut()).is_pending());
        let mut nested = Box::pin(host.call_keyed("active", "fake", json!({})));
        assert!(futures_util::poll!(nested.as_mut()).is_pending());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(dispatch);
        assert!(matches!(nested.await, Outcome::Unknown(_)));
        assert!(host.seal_and_inspect().is_err());
    }
    struct CapturingKeyed {
        saved: Arc<Mutex<Option<crate::keyed_tool::KeyedHost>>>,
    }
    #[async_trait]
    impl crate::keyed_tool::KeyedTool for CapturingKeyed {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "capture".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(
            &self,
            _: Value,
            _: ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            host: crate::keyed_tool::KeyedHost,
        ) -> pi_rs_core::ToolReply {
            *self.saved.lock().unwrap() = Some(host);
            Outcome::Succeeded("done".into()).into()
        }
    }
    #[tokio::test]
    async fn completed_child_cannot_use_escaped_keyed_host() {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let saved = Arc::new(Mutex::new(None));
        let h = NestedHost::new(
            Arc::new(OwnedStore::open(dir.path().join("db")).unwrap()),
            Uuid::new_v4(),
            vec![
                Arc::new(crate::keyed_tool::KeyedToolAdapter::new(CapturingKeyed {
                    saved: saved.clone(),
                }))
                .register(ReplayPolicy::Safe),
                RegisteredTool {
                    tool: Arc::new(Fake {
                        calls: calls.clone(),
                        pending: false,
                    }),
                    replay: ReplayPolicy::Unsafe,
                },
            ],
            CancellationToken::new(),
            Duration::from_secs(10),
            3,
        )
        .unwrap();
        assert!(matches!(
            h.call_keyed("first", "capture", json!({})).await,
            Outcome::Succeeded(_)
        ));
        let escaped = saved.lock().unwrap().take().unwrap();
        assert!(matches!(
            escaped.call_keyed("late", "fake", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(h.seal_and_inspect().is_err());
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
