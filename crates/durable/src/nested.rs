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
    #[serde(default)]
    before_pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    child_park: Option<ChildPark>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    before_gate: Option<BeforeGate>,
    policy: ReplayPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    durable_identity: Option<String>,
    operation_id: Uuid,
    outcome: Option<Outcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staged_outcome: Option<Outcome>,
    #[serde(default)]
    hooks_required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hook_snapshot: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_blocks: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    projected_structured: Option<Outcome>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    diagnostics: Vec<crate::runtime::ToolDiagnostic>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    derived_diagnostics: Vec<crate::runtime::ToolDiagnostic>,
    // Marks records whose generated diagnostics were explicitly tracked.
    #[serde(default)]
    diagnostic_provenance: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    diagnostic_keys: std::collections::BTreeMap<String, usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolution: Option<Resolution>,
}
#[derive(Clone, Serialize, Deserialize)]
struct ChildPark {
    suspension: Uuid,
    root_attempt: Uuid,
    leaf_key: Vec<u8>,
}
struct ChildParkRequest {
    checkpoint: Value,
    leaf_key: Vec<u8>,
    leaf_version: u64,
    operation_id: Uuid,
    reference: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct BeforeGate {
    signals: Vec<String>,
    until_millis: Option<i64>,
    wake: Option<Value>,
}

/// Checkpoint and IDs of pending nested operations that settled before replay.
/// These IDs identify nested journal operations, not scheduler task IDs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PendingWake {
    pub checkpoint: Value,
    pub ended: Vec<Uuid>,
}

/// A keyed nested journal operation and its current outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct UnpolledCall {
    pub operation_id: Uuid,
    pub outcome: Outcome,
}

/// A nested outcome with diagnostics committed for that keyed child.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyedResult {
    pub outcome: Outcome,
    pub diagnostics: Vec<crate::runtime::ToolDiagnostic>,
    /// Ordered output when a block-aware hook changed the ordinary text result.
    pub output_blocks: Option<Vec<Value>>,
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

/// A versioned observation used by the autonomous poller. The reference is
/// opaque, and both the conversation and record versions fence its result.
pub(crate) struct PendingPoll {
    key: Vec<u8>,
    version: u64,
    record: Record,
}

// A block-aware child exposes both channels, so the outcome's text must be
// derived from the final blocks. Non-text blocks have no plain-text projection.
fn outcome_from_blocks(outcome: Outcome, blocks: &[Value]) -> Outcome {
    let text: String = blocks
        .iter()
        .filter_map(|block| {
            (block.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| block.get("text").and_then(Value::as_str))
                .flatten()
        })
        .collect();
    match outcome {
        Outcome::Failed(_) => Outcome::Failed(text),
        Outcome::Skipped(_) => Outcome::Skipped(text),
        Outcome::SucceededWithDetails { details, .. } => Outcome::SucceededWithDetails {
            output: text,
            details,
        },
        _ => Outcome::Succeeded(text),
    }
}

fn durable_tool_identity(tool: &std::sync::Arc<dyn pi_rs_core::Tool>) -> Option<String> {
    crate::nested_identity::current(tool)
}

fn suspension_error(error: crate::nested_suspension::Error) -> crate::runtime::Error {
    match error {
        crate::nested_suspension::Error::Store(error) => error.into(),
        crate::nested_suspension::Error::Json(error) => error.into(),
        error => crate::runtime::Error::Invalid(error.to_string()),
    }
}

impl PendingPoll {
    pub(crate) fn expected_identity(&self) -> Option<&str> {
        self.record.durable_identity.as_deref()
    }
    pub(crate) fn name(&self) -> &str {
        &self.record.name
    }
    pub(crate) fn policy(&self) -> ReplayPolicy {
        self.record.policy
    }
    pub(crate) fn operation(&self) -> Uuid {
        self.record.operation_id
    }
    pub(crate) fn reference(&self) -> &str {
        match self.record.outcome.as_ref() {
            Some(Outcome::Pending { reference }) => reference,
            _ => unreachable!("pending snapshot"),
        }
    }

    pub(crate) fn commit(
        self,
        store: &durable_store::Store,
        conversation_key: Vec<u8>,
        conversation_version: u64,
        binding: Option<crate::nested_fence::Binding>,
        outcome: Outcome,
        schemas: &std::collections::BTreeMap<String, Value>,
    ) -> Result<(), crate::runtime::Error> {
        if matches!(outcome, Outcome::Unknown(_)) {
            return Err(crate::runtime::Error::Invalid(
                "uncertain nested poll".into(),
            ));
        }
        let reference = self.reference().to_owned();
        let next_reference = match &outcome {
            Outcome::Pending { reference } => Some(reference.clone()),
            _ => None,
        };
        let outcome = if self.record.hooks_required {
            outcome
        } else {
            validate_nested_output(schemas, &self.record.name, outcome)
        };
        let mut record = self.record;
        if !matches!(outcome, Outcome::Pending { .. }) && record.hooks_required {
            record.staged_outcome = Some(outcome.clone());
            record.outcome = None;
        } else {
            record.outcome = Some(outcome.clone());
        }
        if !matches!(outcome, Outcome::Pending { .. }) {
            record.resolution = Some(Resolution {
                reference: reference.clone(),
                outcome,
                cancelled: false,
            });
        }
        let record_check = Check {
            key: self.key.clone(),
            expected: Expected::Version(self.version),
        };
        let mut checks = vec![record_check];
        let mut mutations = vec![Mutation::Put {
            key: self.key.clone(),
            value: serde_json::to_vec(&record)?,
        }];
        if let Some(binding) = binding {
            // A child's pending reference can rotate. Keep the per-child
            // suspension in the same transaction as the observed leaf result.
            let mut found = false;
            for (key, entry) in
                store.list(format!("nested/{}/", binding.attempt.operation_id).as_bytes())?
            {
                let parent: Record = serde_json::from_slice(&entry.value)?;
                let Some(park) = &parent.child_park else {
                    continue;
                };
                if park.leaf_key != self.key {
                    continue;
                }
                if found {
                    return Err(crate::runtime::Error::Invalid(
                        "ambiguous nested child park".into(),
                    ));
                }
                found = true;
                let suspension = crate::nested_suspension::load(
                    store,
                    binding.conversation,
                    parent.operation_id,
                )
                .map_err(suspension_error)?
                .ok_or_else(|| {
                    crate::runtime::Error::Invalid("nested suspension missing".into())
                })?;
                if suspension.suspension_id() != park.suspension
                    || suspension.jobs().len() != 1
                    || suspension.jobs()[0].operation_id != record.operation_id
                    || suspension.jobs()[0].reference != reference
                {
                    return Err(crate::runtime::Error::Invalid(
                        "nested suspension evidence changed".into(),
                    ));
                }
                checks.push(Check {
                    key,
                    expected: Expected::Version(entry.version),
                });
                if let Some(next) = &next_reference
                    && next != &reference
                {
                    let batch = suspension
                        .advance_reference(record.operation_id, &reference, next.clone())
                        .map_err(suspension_error)?;
                    checks.extend(batch.checks);
                    mutations.extend(batch.mutations);
                } else {
                    checks.push(suspension.retired_check().map_err(suspension_error)?);
                }
            }
            match binding.commit(store, &checks, &mutations) {
                Ok(()) => (),
                Err(crate::nested_fence::Error::Stale) => {
                    return Err(durable_store::Error::Conflict {
                        key: conversation_key,
                    }
                    .into());
                }
                Err(crate::nested_fence::Error::Store(error)) => return Err(error.into()),
                Err(crate::nested_fence::Error::Json(error)) => return Err(error.into()),
            }
        } else {
            // Legacy records have no attempt identity; keep their exact snapshot fence.
            store.commit(
                &[
                    Check {
                        key: conversation_key,
                        expected: Expected::Version(conversation_version),
                    },
                    checks.remove(0),
                ],
                &mutations,
            )?;
        }
        Ok(())
    }
}

pub(crate) fn pending_polls(
    store: &durable_store::Store,
    root: Uuid,
) -> Result<Vec<PendingPoll>, crate::runtime::Error> {
    let mut jobs = Vec::new();
    for (key, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if record.child_park.is_none() && matches!(record.outcome, Some(Outcome::Pending { .. })) {
            jobs.push(PendingPoll {
                key,
                version: entry.version,
                record,
            });
        }
    }
    Ok(jobs)
}

pub(crate) fn pending_jobs(
    store: &durable_store::Store,
    root: Uuid,
) -> Result<Vec<PendingJob>, crate::runtime::Error> {
    let mut jobs = Vec::new();
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if record.child_park.is_none()
            && let Some(Outcome::Pending { reference }) = record.outcome
        {
            jobs.push(PendingJob {
                operation_id: record.operation_id,
                reference,
            });
        }
    }
    Ok(jobs)
}

/// Bind only a legacy unbound pending reference after an operator has
/// independently verified the remote job and its backend identity.
#[allow(clippy::too_many_arguments)] // The checked binding needs every part of the operator's assertion and its store fence.
pub(crate) fn bind_pending_identity(
    store: &durable_store::Store,
    conversation: (Vec<u8>, u64),
    root: Uuid,
    operation: Uuid,
    reference: &str,
    identity: &str,
    tools: &std::collections::HashMap<String, RegisteredTool>,
    allowed: impl Fn(&str) -> bool,
) -> Result<(), crate::runtime::Error> {
    let invalid =
        || crate::runtime::Error::Invalid("nested pending identity binding does not match".into());
    let mut found = None;
    for (key, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if record.operation_id == operation {
            if found.is_some() {
                return Err(invalid());
            }
            found = Some((key, entry, record));
        }
    }
    let (key, entry, mut record) = found.ok_or_else(invalid)?;
    if !matches!(&record.outcome, Some(Outcome::Pending { reference: r }) if r == reference)
        || record.staged_outcome.is_some()
        || !allowed(&record.name)
    {
        return Err(invalid());
    }
    let reg = tools.get(&record.name).ok_or_else(invalid)?;
    if reg.replay != record.policy || !reg.tool.can_poll() {
        return Err(invalid());
    }
    let _lease = crate::nested_identity::lease(&reg.tool, identity).ok_or_else(invalid)?;
    if record
        .durable_identity
        .as_deref()
        .is_some_and(|old| old != identity)
    {
        return Err(invalid());
    }
    let mutation = if record.durable_identity.is_none() {
        record.durable_identity = Some(identity.to_owned());
        vec![Mutation::Put {
            key: key.clone(),
            value: serde_json::to_vec(&record)?,
        }]
    } else {
        vec![]
    };
    store.commit(
        &[
            Check {
                key: conversation.0,
                expected: Expected::Version(conversation.1),
            },
            Check {
                key,
                expected: Expected::Version(entry.version),
            },
        ],
        &mutation,
    )?;
    Ok(())
}

/// Recheck bound child identities before replay. Legacy unbound children may
/// be used by safe parents under the legacy backend trust contract; unsafe
/// parks require every child to have a matching identity.
pub(crate) fn children_match(
    store: &durable_store::Store,
    root: Uuid,
    tools: &std::collections::HashMap<String, RegisteredTool>,
    require_identity: bool,
) -> Result<bool, crate::runtime::Error> {
    let mut any = false;
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        any = true;
        let Some(identity) = record.durable_identity.as_ref() else {
            if require_identity {
                return Ok(false);
            }
            continue;
        };
        let Some(reg) = tools.get(&record.name) else {
            return Ok(false);
        };
        if record.policy != reg.replay
            || durable_tool_identity(&reg.tool).as_ref() != Some(identity)
        {
            return Ok(false);
        }
    }
    Ok(any)
}

/// Lease all recorded child identities before an unsafe parent claims its
/// next attempt. Nonblocking acquisition avoids lock-order deadlocks.
type ChildLeaseResult = Result<
    Result<Vec<crate::nested_identity::IdentityLease>, crate::nested_identity::LeaseFailure>,
    crate::runtime::Error,
>;
pub(crate) fn lease_children(
    store: &durable_store::Store,
    root: Uuid,
    tools: &std::collections::HashMap<String, RegisteredTool>,
) -> ChildLeaseResult {
    lease_children_for_resume(store, root, tools, true)
}

pub(crate) fn lease_children_for_resume(
    store: &durable_store::Store,
    root: Uuid,
    tools: &std::collections::HashMap<String, RegisteredTool>,
    require_identity: bool,
) -> ChildLeaseResult {
    let mut leases = Vec::new();
    let mut any = false;
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        any = true;
        let record: Record = serde_json::from_slice(&entry.value)?;
        let Some(identity) = record.durable_identity.as_deref() else {
            if require_identity {
                return Ok(Err(crate::nested_identity::LeaseFailure::Mismatch));
            }
            continue;
        };
        let Some(reg) = tools.get(&record.name) else {
            return Ok(Err(crate::nested_identity::LeaseFailure::Mismatch));
        };
        if record.policy != reg.replay {
            return Ok(Err(crate::nested_identity::LeaseFailure::Mismatch));
        }
        let lease = match crate::nested_identity::checked_lease(&reg.tool, identity) {
            Ok(lease) => lease,
            Err(error) => return Ok(Err(error)),
        };
        leases.push(lease);
    }
    if !any {
        Ok(Err(crate::nested_identity::LeaseFailure::Mismatch))
    } else {
        Ok(Ok(leases))
    }
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
        if let Some(park) = &record.child_park {
            let Some(leaf) = store.get(&park.leaf_key)? else {
                return Ok(false);
            };
            let leaf: Record = serde_json::from_slice(&leaf.value)?;
            if !matches!(
                leaf.outcome,
                Some(
                    Outcome::Succeeded(_)
                        | Outcome::SucceededWithDetails { .. }
                        | Outcome::Failed(_)
                        | Outcome::Skipped(_)
                        | Outcome::Truncated { .. }
                        | Outcome::Structured { .. }
                )
            ) || leaf.resolution.is_none()
                || leaf.staged_outcome.is_some()
            {
                return Ok(false);
            }
            resolved = true;
            continue;
        }
        if record
            .before_gate
            .as_ref()
            .is_some_and(|gate| gate.wake.is_some())
        {
            resolved = true;
            continue;
        }
        if record.staged_outcome.is_none()
            && matches!(
                record.outcome,
                None | Some(Outcome::Pending { .. }) | Some(Outcome::Unknown(_))
            )
        {
            return Ok(false);
        }
        resolved |= record.resolution.is_some();
    }
    Ok(resolved)
}

// A committed pre-dispatch gate proves that child invocation has not started.
// Accepted pending references and staged known results are safe to reattach;
// records without a known outcome or durable gate remain reconciliation barriers.
pub(crate) fn recoverable_gate_intent(
    store: &durable_store::Store,
    root: Uuid,
) -> Result<bool, crate::runtime::Error> {
    let mut gates = 0;
    let mut pending = 0;
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if matches!(
            &record.staged_outcome,
            Some(Outcome::Pending { .. } | Outcome::Unknown(_))
        ) {
            return Ok(false);
        }
        if record.before_pending
            && record
                .before_gate
                .as_ref()
                .is_some_and(|gate| gate.wake.is_none())
            && record.outcome.is_none()
            && record.staged_outcome.is_none()
        {
            gates += 1;
            pending += 1;
        } else if record.before_pending || record.before_gate.is_some() {
            return Ok(false);
        } else if matches!(record.outcome, Some(Outcome::Pending { .. }))
            && record.staged_outcome.is_none()
        {
            pending += 1;
        } else if matches!(
            record.outcome,
            Some(Outcome::Unknown(_) | Outcome::Pending { .. })
        ) || (record.outcome.is_none() == record.staged_outcome.is_none())
        {
            return Ok(false);
        }
    }
    Ok(gates > 0 && pending <= 8)
}

pub(crate) fn has_staged_results(
    store: &durable_store::Store,
    root: Uuid,
) -> Result<bool, crate::runtime::Error> {
    for (_, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let record: Record = serde_json::from_slice(&entry.value)?;
        if record.staged_outcome.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn wake_before_gates(
    store: &durable_store::Store,
    root: Uuid,
    conversation: Uuid,
    conversation_key: Vec<u8>,
    conversation_version: u64,
    now: i64,
) -> Result<bool, crate::runtime::Error> {
    for (key, entry) in store.list(format!("nested/{root}/").as_bytes())? {
        let mut record: Record = serde_json::from_slice(&entry.value)?;
        let Some(gate) = record
            .before_gate
            .as_ref()
            .filter(|gate| gate.wake.is_none())
        else {
            continue;
        };
        let Some(wake) = crate::conversation_signal::Inbox::new(store).prepare_wake(
            conversation,
            &gate.signals,
            gate.until_millis,
            now,
        )?
        else {
            continue;
        };
        let reason = match &wake.signal {
            Some(signal) => {
                serde_json::json!({"reason":"signal", "name":signal.name, "id":signal.id, "payload":signal.payload})
            }
            None if gate.signals.is_empty() => serde_json::json!({"reason":"time"}),
            None => serde_json::json!({"reason":"expired"}),
        };
        record.before_gate.as_mut().unwrap().wake = Some(reason);
        let mut checks = wake.checks;
        checks.push(Check {
            key: conversation_key,
            expected: Expected::Version(conversation_version),
        });
        checks.push(Check {
            key: key.clone(),
            expected: Expected::Version(entry.version),
        });
        let mut mutations = wake.mutations;
        mutations.push(Mutation::Put {
            key,
            value: serde_json::to_vec(&record)?,
        });
        store.commit(&checks, &mutations)?;
        return Ok(true);
    }
    Ok(false)
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
    if record.child_park.is_some() {
        return Err(Error::Invalid(
            "nested child park is not a remote job; resolve its pending descendant instead".into(),
        ));
    }
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
    if record.hooks_required {
        record.staged_outcome = Some(outcome);
        record.outcome = None;
    } else {
        record.outcome = Some(outcome);
    }
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
    output_schemas: std::collections::BTreeMap<String, Value>,
    hooks: Option<NestedHooks>,
    fence: Option<crate::nested_fence::Binding>,
    cancellation: CancellationToken,
    timeout: Duration,
    max_depth: usize,
    // Protects sealing, active calls, and the visited call counts together.
    lifecycle: Mutex<Lifecycle>,
    failed: AtomicBool,
    non_pending_failure: AtomicBool,
    explicit_suspend: AtomicBool,
    parked_checkpoint: Mutex<Option<PendingWake>>,
    resumed_checkpoint: Mutex<Option<PendingWake>>,
    idle: tokio::sync::Notify,
}
impl Shared {
    fn commit(
        &self,
        checks: &[Check],
        mutations: &[Mutation],
    ) -> Result<(), crate::nested_fence::Error> {
        match &self.fence {
            Some(binding) => binding.commit(self.owner.store(), checks, mutations),
            None => self
                .owner
                .store()
                .commit(checks, mutations)
                .map_err(Into::into),
        }
    }
}
pub(crate) struct NestedHooks {
    pub selected: crate::extension::Resolved,
    pub snapshot: Value,
    pub conversation: Uuid,
    pub storage_failed: Arc<AtomicBool>,
    pub conversation_fence: Option<(Vec<u8>, u64)>,
}
struct NestedDiagnostic {
    shared: Arc<Shared>,
    key: Vec<u8>,
    operation: Uuid,
    claim: Option<Check>,
}
impl NestedDiagnostic {
    fn storage_error(&self, error: impl std::fmt::Display) -> String {
        self.shared
            .non_pending_failure
            .store(true, Ordering::Release);
        self.shared.failed.store(true, Ordering::Release);
        error.to_string()
    }
}
impl crate::keyed_tool::DiagnosticSink for NestedDiagnostic {
    fn record(
        &self,
        diagnostic_id: Option<&str>,
        diagnostic: crate::runtime::ToolDiagnostic,
    ) -> Result<(), String> {
        let store = self.shared.owner.store();
        loop {
            if self.shared.cancellation.is_cancelled() {
                return Err("nested attempt cancelled".into());
            }
            if self
                .shared
                .lifecycle
                .lock()
                .map_err(|_| "nested lifecycle lock poisoned")?
                .closed
                .contains(&self.operation)
            {
                return Err("nested attempt is no longer active".into());
            }
            let entry = store
                .get(&self.key)
                .map_err(|e| self.storage_error(e))?
                .ok_or_else(|| self.storage_error("nested intent missing"))?;
            let mut record: Record =
                serde_json::from_slice(&entry.value).map_err(|e| self.storage_error(e))?;
            if record.operation_id != self.operation
                || record.outcome.is_some()
                || record.staged_outcome.is_some()
            {
                return Err("nested attempt is no longer active".into());
            }
            crate::runtime::insert_diagnostic(
                &mut record.diagnostics,
                &mut record.diagnostic_keys,
                diagnostic_id,
                diagnostic.clone(),
            )?;
            let value = serde_json::to_vec(&record).map_err(|e| self.storage_error(e))?;
            let mut checks = vec![Check {
                key: self.key.clone(),
                expected: Expected::Version(entry.version),
            }];
            if let Some(claim) = &self.claim {
                checks.push(claim.clone());
            }
            match self.shared.commit(
                &checks,
                &[Mutation::Put {
                    key: self.key.clone(),
                    value,
                }],
            ) {
                Ok(()) => return Ok(()),
                Err(crate::nested_fence::Error::Store(durable_store::Error::Conflict { key }))
                    if key == self.key =>
                {
                    continue;
                }
                Err(error) => return Err(self.storage_error(error)),
            }
        }
    }
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
    child_park_request: Mutex<Option<ChildParkRequest>>,
    child_wake: Option<PendingWake>,
    child_replay_safe: bool,
    claim: Option<Check>,
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
        Self::new_with_schemas(
            owner,
            parent,
            tools,
            cancellation,
            timeout,
            max_depth,
            (Default::default(), None, None),
        )
    }
    pub(crate) fn new_with_schemas(
        owner: Arc<OwnedStore>,
        parent: Uuid,
        tools: Vec<RegisteredTool>,
        cancellation: CancellationToken,
        timeout: Duration,
        max_depth: usize,
        (output_schemas, hooks, fence): (
            std::collections::BTreeMap<String, Value>,
            Option<NestedHooks>,
            Option<crate::nested_fence::Binding>,
        ),
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
                output_schemas,
                hooks,
                fence,
                cancellation,
                timeout,
                max_depth,
                lifecycle: Mutex::new(lifecycle),
                failed: AtomicBool::new(false),
                non_pending_failure: AtomicBool::new(false),
                explicit_suspend: AtomicBool::new(false),
                parked_checkpoint: Mutex::new(None),
                resumed_checkpoint: Mutex::new(None),
                idle: tokio::sync::Notify::new(),
            }),
            parent,
            depth: 0,
            serial: tokio::sync::Mutex::new(()),
            child_park_request: Mutex::new(None),
            child_wake: None,
            child_replay_safe: false,
            claim: None,
        }))
    }
    fn commit(
        &self,
        checks: &[Check],
        mutations: &[Mutation],
    ) -> Result<(), crate::nested_fence::Error> {
        let mut checks = checks.to_vec();
        if let Some(claim) = &self.claim {
            checks.push(claim.clone());
        }
        self.shared.commit(&checks, mutations)
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
    /// Dispatch or reattach without polling a pending result. This is a
    /// building block for an explicitly suspended, replay-safe parent.
    pub(crate) async fn call_keyed_unpolled(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> Outcome {
        if !valid_key(key) {
            return self.fail("invalid nested call key");
        }
        self.call_inner(Some(key), name, arguments, false, false)
            .await
    }

    pub(crate) async fn call_keyed_unpolled_with_id(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> Result<UnpolledCall, String> {
        let outcome = self.call_keyed_unpolled(key, name, arguments.clone()).await;
        let lifecycle = self
            .shared
            .lifecycle
            .lock()
            .map_err(|_| "nested lifecycle lock poisoned")?;
        if lifecycle.sealed || lifecycle.closed.contains(&self.parent) {
            return Err("nested host sealed".into());
        }
        if !valid_key(key) {
            return Err("invalid nested call key".into());
        }
        let entry = self
            .shared
            .owner
            .store()
            .get(&self.explicit_key(self.parent, key))
            .map_err(|e| e.to_string())?
            .ok_or("nested call was not admitted")?;
        let record: Record = serde_json::from_slice(&entry.value).map_err(|e| e.to_string())?;
        if record.name != name
            || record.arguments != arguments
            || record.outcome.as_ref() != Some(&outcome)
        {
            return Err("nested call is uncertain or its result is not available".into());
        }
        Ok(UnpolledCall {
            operation_id: record.operation_id,
            outcome,
        })
    }

    /// Request a durable wait after all local nested invocations have returned.
    /// An unresolved or uncertain non-pending operation remains a barrier.
    pub(crate) fn suspend_pending(&self) -> Result<(), String> {
        self.suspend_pending_with_checkpoint(None)
    }

    pub(crate) fn suspend_pending_with_checkpoint(
        &self,
        checkpoint: Option<Value>,
    ) -> Result<(), String> {
        if checkpoint.as_ref().is_some_and(|value| {
            !serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 64 * 1024)
        }) {
            return Err("nested checkpoint exceeds 64 KiB".into());
        }
        if self.parent != self.shared.root {
            return self.request_child_park(checkpoint);
        }
        if self.shared.failed.load(Ordering::Acquire) {
            return Err("nested suspension requires a healthy root invocation".into());
        }
        let lifecycle = self
            .shared
            .lifecycle
            .lock()
            .map_err(|_| "nested lifecycle lock poisoned")?;
        if lifecycle.sealed || lifecycle.active != 0 {
            return Err("nested invocations remain active".into());
        }
        let mut pending = Vec::new();
        let mut child_parks = Vec::new();
        for (_, entry) in self
            .shared
            .owner
            .store()
            .list(&self.prefix())
            .map_err(|e| e.to_string())?
        {
            let record: Record = serde_json::from_slice(&entry.value).map_err(|e| e.to_string())?;
            if let Some(park) = record.child_park {
                child_parks.push(park);
                continue;
            }
            if record
                .before_gate
                .as_ref()
                .is_some_and(|gate| gate.wake.is_none())
            {
                if checkpoint.is_some() {
                    return Err(
                        "checkpointed suspension does not support nested before-tool gates".into(),
                    );
                }
                pending.push(record.operation_id);
                continue;
            }
            match record.outcome {
                Some(Outcome::Pending { .. }) => pending.push(record.operation_id),
                None | Some(Outcome::Unknown(_)) => {
                    return Err("nested operation is uncertain".into());
                }
                _ => (),
            }
        }
        if !child_parks.is_empty() {
            if child_parks.len() != 1 || pending.len() > 1 || checkpoint.is_none() {
                return Err("nested child park requires one checkpointed descendant".into());
            }
            let leaf = self
                .shared
                .owner
                .store()
                .get(&child_parks[0].leaf_key)
                .map_err(|e| e.to_string())?
                .ok_or("nested park leaf missing")?;
            let leaf: Record = serde_json::from_slice(&leaf.value).map_err(|e| e.to_string())?;
            if pending.is_empty() {
                let Some(resolution) = &leaf.resolution else {
                    return Err("nested park descendant is not resolved".into());
                };
                if leaf.outcome.as_ref() != Some(&resolution.outcome)
                    || matches!(
                        resolution.outcome,
                        Outcome::Pending { .. } | Outcome::Unknown(_)
                    )
                {
                    return Err("nested park descendant is not terminal".into());
                }
                // A confirmed wake can race the root's park. Persist the same
                // checkpoint and wake identity, then claim a fresh root attempt.
                pending.push(leaf.operation_id);
            } else if pending[0] != leaf.operation_id {
                return Err("nested park descendant changed".into());
            }
        }
        if pending.is_empty() || pending.len() > 8 {
            return Err("nested suspension requires one to eight pending jobs".into());
        }
        let mut parked = self
            .shared
            .parked_checkpoint
            .lock()
            .map_err(|_| "nested checkpoint lock poisoned")?;
        *parked = checkpoint.map(|checkpoint| PendingWake {
            checkpoint,
            ended: pending,
        });
        // Seal future admissions before asking the runtime to discard this
        // invocation's reply. The runtime waits for all calls before persisting.
        self.shared.explicit_suspend.store(true, Ordering::Release);
        self.pending_failure("explicit nested suspension");
        drop(lifecycle);
        Ok(())
    }
    // Deliberately bounded: only one keyed intermediate, one keyed pending
    // descendant, replay-safe root and child, with no extension hooks.
    fn request_child_park(&self, checkpoint: Option<Value>) -> Result<(), String> {
        let checkpoint = checkpoint.ok_or("nested child park requires a checkpoint")?;
        if self.depth != 1
            || self.child_wake.is_some()
            || !self.child_replay_safe
            || self.shared.hooks.is_some()
            || self.shared.failed.load(Ordering::Acquire)
        {
            return Err("unsupported nested child park".into());
        }
        let binding = self
            .shared
            .fence
            .as_ref()
            .ok_or("nested child park needs a runtime root")?;
        if binding.suspended {
            return Err("nested child park requires live root".into());
        }
        let store = self.shared.owner.store();
        let root = store
            .get(&crate::runtime::Harness::key(binding.conversation))
            .map_err(|e| e.to_string())?
            .ok_or("root missing")?;
        let state: crate::runtime::Conversation =
            serde_json::from_slice(&root.value).map_err(|e| e.to_string())?;
        if !state.active_tool_is_replay_safe()
            || state.active_tool_attempt() != Some(binding.attempt)
        {
            return Err("nested child park requires replay-safe root".into());
        }
        let mut lifecycle = self
            .shared
            .lifecycle
            .lock()
            .map_err(|_| "nested lifecycle lock poisoned")?;
        if lifecycle.sealed
            || lifecycle.active != 1
            || lifecycle.closed.contains(&self.parent)
            || lifecycle
                .active_by_parent
                .get(&self.parent)
                .copied()
                .unwrap_or(0)
                != 0
        {
            return Err("nested child executor is not quiescent".into());
        }
        let prefix = format!("nested/{}/{}/", self.shared.root, self.parent).into_bytes();
        let entries = store.list(&prefix).map_err(|e| e.to_string())?;
        if entries.len() != 1 || !entries[0].0[prefix.len()..].starts_with(b"k") {
            return Err("nested child park requires one keyed descendant".into());
        }
        let (leaf_key, leaf) = &entries[0];
        let record: Record = serde_json::from_slice(&leaf.value).map_err(|e| e.to_string())?;
        let Some(Outcome::Pending { reference }) = &record.outcome else {
            return Err("nested descendant is not pending".into());
        };
        if record.before_pending
            || record.before_gate.is_some()
            || record.child_park.is_some()
            || record.staged_outcome.is_some()
        {
            return Err("unsupported nested descendant state".into());
        }
        let mut request = self
            .child_park_request
            .lock()
            .map_err(|_| "nested park lock poisoned")?;
        if request.is_some() {
            return Err("nested child already requested a park".into());
        }
        // No other unfinished nested operation may be hidden by this subset.
        for (_, entry) in store.list(&self.prefix()).map_err(|e| e.to_string())? {
            let other: Record = serde_json::from_slice(&entry.value).map_err(|e| e.to_string())?;
            if other.operation_id != self.parent
                && other.operation_id != record.operation_id
                && (other.child_park.is_some()
                    || matches!(
                        other.outcome,
                        None | Some(Outcome::Pending { .. }) | Some(Outcome::Unknown(_))
                    ))
            {
                return Err("nested child park requires a single pending descendant".into());
            }
        }
        *request = Some(ChildParkRequest {
            checkpoint,
            leaf_key: leaf_key.clone(),
            leaf_version: leaf.version,
            operation_id: record.operation_id,
            reference: reference.clone(),
        });
        lifecycle.closed.insert(self.parent);
        Ok(())
    }

    pub async fn call_keyed_with_diagnostics(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
    ) -> KeyedResult {
        let mut result = self.call_keyed_result(key, name, arguments, true).await;
        if result.output_blocks.take().is_some() {
            result.outcome =
                self.fail("nested result contains ordered blocks; use a block-aware keyed result");
        }
        result
    }
    pub(crate) async fn call_keyed_result(
        &self,
        key: &str,
        name: &str,
        arguments: Value,
        poll: bool,
    ) -> KeyedResult {
        let mut outcome = if valid_key(key) {
            self.call_inner_raw(Some(key), name, arguments.clone(), false, poll)
                .await
        } else {
            self.fail("invalid nested call key")
        };
        // Keep the invocation open while reading its private checkpoint. A
        // detached caller must not read diagnostics after the host is sealed.
        let lifecycle = match self.shared.lifecycle.lock() {
            Ok(lifecycle) if !lifecycle.sealed && !lifecycle.closed.contains(&self.parent) => {
                lifecycle
            }
            _ => {
                return KeyedResult {
                    outcome: self.fail("nested host sealed or failed"),
                    diagnostics: Vec::new(),
                    output_blocks: None,
                };
            }
        };
        let mut output_blocks = None;
        let mut provenance_known = false;
        let mut diagnostics = if valid_key(key) {
            match self
                .shared
                .owner
                .store()
                .get(&self.explicit_key(self.parent, key))
            {
                Ok(Some(entry)) => match serde_json::from_slice::<Record>(&entry.value) {
                    Ok(record)
                        if record.name == name
                            && record.arguments == arguments
                            && record.outcome.as_ref() == Some(&outcome)
                            && !matches!(
                                outcome,
                                Outcome::Unknown(_) | Outcome::Pending { .. }
                            ) =>
                    {
                        if record.output_blocks.as_ref().is_some_and(|blocks| {
                            crate::content::UserContent::from_yield(
                                Value::Array(blocks.clone()),
                                false,
                            )
                            .is_none()
                                || !serde_json::to_vec(blocks)
                                    .is_ok_and(|bytes| bytes.len() <= 8 * 1024 * 1024)
                        }) {
                            return KeyedResult {
                                outcome: self.fail("invalid persisted nested output blocks"),
                                diagnostics: Vec::new(),
                                output_blocks: None,
                            };
                        }
                        output_blocks = record.output_blocks;
                        provenance_known = record.diagnostic_provenance;
                        record.diagnostics
                    }
                    Ok(_) => Vec::new(),
                    Err(error) => {
                        return KeyedResult {
                            outcome: self.fail(format!("nested diagnostic record: {error}")),
                            diagnostics: Vec::new(),
                            output_blocks: None,
                        };
                    }
                },
                Ok(None) => Vec::new(),
                Err(error) => {
                    return KeyedResult {
                        outcome: self.fail(format!("nested diagnostic read: {error}")),
                        diagnostics: Vec::new(),
                        output_blocks: None,
                    };
                }
            }
        } else {
            Vec::new()
        };
        // Older records may have retained their pre-hook outcome alongside
        // rewritten blocks. Project it before any fallback diagnostic is made.
        if let Some(blocks) = &output_blocks {
            if !provenance_known
                && outcome_from_blocks(outcome.clone(), blocks) != outcome
                && let Outcome::Failed(reason) | Outcome::Skipped(reason) = &outcome
            {
                diagnostics.retain(|diagnostic| {
                    !matches!(
                        diagnostic.code.as_deref(),
                        Some("tool_failed" | "tool_skipped" | "tool_error")
                    ) || diagnostic.message != *reason
                });
            }
            outcome = outcome_from_blocks(outcome, blocks);
        }
        if let Outcome::Failed(reason) = &outcome
            && !diagnostics
                .iter()
                .any(|diagnostic| diagnostic.severity == crate::runtime::DiagnosticSeverity::Error)
        {
            diagnostics.push(crate::runtime::ToolDiagnostic {
                severity: crate::runtime::DiagnosticSeverity::Error,
                code: Some(
                    if reason.starts_with("invalid_structured_output:") {
                        "invalid_structured_output"
                    } else {
                        "tool_error"
                    }
                    .into(),
                ),
                message: reason.clone(),
            });
        }
        drop(lifecycle);
        KeyedResult {
            outcome,
            diagnostics,
            output_blocks,
        }
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
                let outcome = self
                    .poll_pending(&storage_key, record, entry.version, reg)
                    .await;
                self.legacy_outcome(&storage_key, outcome)
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
    pub(crate) fn parked_checkpoint(&self) -> Result<Option<PendingWake>, String> {
        self.shared
            .parked_checkpoint
            .lock()
            .map(|value| value.clone())
            .map_err(|_| "nested checkpoint lock poisoned".into())
    }
    pub(crate) fn set_resumed_checkpoint(&self, wake: Option<PendingWake>) -> Result<(), String> {
        *self
            .shared
            .resumed_checkpoint
            .lock()
            .map_err(|_| "nested checkpoint lock poisoned")? = wake;
        Ok(())
    }
    pub(crate) fn resumed_checkpoint(&self) -> Option<PendingWake> {
        let lifecycle = self.shared.lifecycle.lock().ok()?;
        if lifecycle.sealed || lifecycle.closed.contains(&self.parent) {
            return None;
        }
        if self.parent != self.shared.root {
            return self.child_wake.clone();
        }
        self.shared.resumed_checkpoint.lock().ok()?.clone()
    }
    pub(crate) fn explicitly_suspended(&self) -> bool {
        self.shared.explicit_suspend.load(Ordering::Acquire)
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
    fn legacy_outcome(&self, key: &[u8], outcome: Outcome) -> Outcome {
        if matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
            return outcome;
        }
        match self.shared.owner.store().get(key) {
            Ok(Some(entry)) => match serde_json::from_slice::<Record>(&entry.value) {
                Ok(record) if record.output_blocks.is_some() => self
                    .fail("nested result contains ordered blocks; use a block-aware keyed result"),
                Ok(_) => outcome,
                Err(error) => self.fail(error.to_string()),
            },
            Ok(None) => outcome,
            Err(error) => self.fail(error.to_string()),
        }
    }

    async fn call_inner(
        &self,
        explicit: Option<&str>,
        name: &str,
        arguments: Value,
        serial_held: bool,
        poll: bool,
    ) -> Outcome {
        let key = if let Some(member) = explicit {
            self.explicit_key(self.parent, member)
        } else {
            let index = match self.shared.lifecycle.lock() {
                Ok(lifecycle) => *lifecycle.counts.get(&self.parent).unwrap_or(&0),
                Err(_) => return self.fail("nested lifecycle lock poisoned"),
            };
            self.key(self.parent, index)
        };
        let outcome = self
            .call_inner_raw(explicit, name, arguments, serial_held, poll)
            .await;
        self.legacy_outcome(&key, outcome)
    }

    async fn call_inner_raw(
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
        // Reattached terminal or pending children may otherwise bypass all
        // journal writes before returning or polling.
        if self.shared.fence.is_some()
            && let Err(error) = self.commit(&[], &[])
        {
            return self.fail(format!("nested root attempt: {error}"));
        }
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
        if self.child_wake.is_some() && entry.is_none() {
            return self.fail("resumed nested child cannot admit another descendant");
        }
        let reg = match self.shared.tools.get(name) {
            Some(v) => v,
            None => return self.fail(format!("nested tool unavailable: {name}")),
        };
        // Bind the recorded identity to the backend that actually executes,
        // including preparation and the durable outcome commit.
        let identity_lease = match crate::nested_identity::execution_lease(&reg.tool) {
            Ok(lease) => lease,
            Err(()) => return self.fail("nested tool identity is busy"),
        };
        let dispatch_identity = identity_lease.as_ref().and_then(|guard| (**guard).clone());
        let registered_unset = identity_lease.as_ref().is_some_and(|guard| guard.is_none());
        let mut resumed_wake = None;
        let mut resumed_claim = None;
        let (mut record, _version) = if let Some(entry) = entry {
            let record: Record = match serde_json::from_slice(&entry.value) {
                Ok(v) => v,
                Err(e) => return self.fail(format!("invalid nested record: {e}")),
            };
            if record.name != name
                || record.arguments != arguments
                || record.policy != reg.replay
                || record
                    .durable_identity
                    .as_ref()
                    .is_some_and(|identity| dispatch_identity.as_ref() != Some(identity))
            {
                return self.fail("nested call identity, arguments, or replay policy changed");
            }
            if let Some(park) = &record.child_park {
                if explicit.is_none()
                    || self.parent != self.shared.root
                    || self.shared.hooks.is_some()
                    || reg.replay != ReplayPolicy::Safe
                {
                    return self.fail("unsupported nested child park replay");
                }
                match self.claim_child_park(&key, entry.version, &record, park) {
                    Ok(Some((snapshot, executor, wake))) => {
                        resumed_wake = Some(wake);
                        resumed_claim = Some((snapshot, executor));
                    }
                    Ok(None) => {
                        return record
                            .outcome
                            .clone()
                            .unwrap_or_else(|| self.fail("nested park outcome missing"));
                    }
                    Err(error) => return self.fail(error),
                }
            } else if let Some(outcome) = &record.outcome {
                if poll && matches!(outcome, Outcome::Pending { .. }) && reg.tool.can_poll() {
                    return self.poll_pending(&key, record, entry.version, reg).await;
                }
                return match replay_finalized(&self.shared.output_schemas, name, &record) {
                    Ok(outcome) => outcome,
                    Err(error) => self.fail(error),
                };
            }
            if record.staged_outcome.is_some() {
                return self.finish_staged(&key, record).await;
            }
            if registered_unset {
                return self.fail("registered nested tool identity is unset");
            }
            if resumed_claim.is_none()
                && !record.before_pending
                && (record.policy != ReplayPolicy::Safe || reg.replay != ReplayPolicy::Safe)
            {
                return self.fail("interrupted unsafe nested call requires reconciliation");
            }
            (record, entry.version)
        } else {
            if registered_unset {
                return self.fail("registered nested tool identity is unset");
            }
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
                before_pending: self.shared.hooks.is_some(),
                child_park: None,
                before_gate: None,
                policy: reg.replay,
                durable_identity: dispatch_identity,
                operation_id: invocation.operation_id,
                outcome: None,
                staged_outcome: None,
                hooks_required: self.shared.hooks.is_some(),
                hook_snapshot: self
                    .shared
                    .hooks
                    .as_ref()
                    .map(|hooks| hooks.snapshot.clone()),
                output_blocks: None,
                projected_structured: None,
                diagnostics: Vec::new(),
                derived_diagnostics: Vec::new(),
                diagnostic_provenance: true,
                diagnostic_keys: Default::default(),
                resolution: None,
            };
            let value = match serde_json::to_vec(&record) {
                Ok(v) => v,
                Err(e) => return self.fail(e.to_string()),
            };
            if let Err(e) = self.commit(
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
        if record.before_pending {
            record = match self.before_nested(&key, record, reg, &call_id).await {
                Ok(record) => record,
                Err(outcome) => return outcome,
            };
            if record.staged_outcome.is_some() {
                return self.finish_staged(&key, record).await;
            }
        }
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
        let execution_claim = if let Some((snapshot, executor)) = &resumed_claim {
            match snapshot.claim_check(*executor) {
                Ok(check) => Some(check),
                Err(error) => return self.fail(error.to_string()),
            }
        } else {
            self.claim.clone()
        };
        let child = Arc::new(Self {
            shared: self.shared.clone(),
            parent: record.operation_id,
            depth: self.depth + 1,
            serial: tokio::sync::Mutex::new(()),
            child_park_request: Mutex::new(None),
            child_wake: resumed_wake,
            child_replay_safe: reg.replay == ReplayPolicy::Safe,
            claim: execution_claim,
        });
        if let Ok(mut l) = self.shared.lifecycle.lock() {
            l.counts.insert(record.operation_id, 0);
        } else {
            return self.fail("nested lifecycle lock poisoned");
        }
        let sink = Arc::new(NestedDiagnostic {
            shared: self.shared.clone(),
            key: key.clone(),
            operation: record.operation_id,
            claim: child.claim.clone(),
        });
        let child_control = Arc::new(Mutex::new(Default::default()));
        let reply = crate::keyed_tool::scope_with_control(
            child.clone(),
            reg.tool.clone(),
            invocation.operation_id,
            child.clone(),
            child_control.clone(),
            (Some(sink), None),
            crate::tool::execute(
                reg.tool.as_ref(),
                &invocation,
                prepared,
                None,
                child.clone(),
                &self.shared.cancellation,
                self.shared.timeout,
            ),
        )
        .await;
        let park_request = match child.child_park_request.lock() {
            Ok(mut requested) => requested.take(),
            Err(_) => return self.fail("nested child park lock poisoned"),
        };
        if let Some(request) = park_request {
            if explicit.is_none()
                || self.parent != self.shared.root
                || reply.state.is_some()
                || self.shared.failed.load(Ordering::Acquire)
                || !matches!(
                    reply.outcome,
                    Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. }
                )
                || child_control.lock().map_or(true, |control| {
                    *control != crate::reset::ToolControl::default()
                })
            {
                return self.fail("nested child park invocation did not finish cleanly");
            }
            return self.commit_child_park(&key, &record, request);
        }
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
        loop {
            let entry = match store.get(&key) {
                Ok(Some(entry)) => entry,
                _ => return self.fail("nested result intent missing"),
            };
            let current: Record = match serde_json::from_slice(&entry.value) {
                Ok(record) => record,
                Err(e) => return self.fail(e.to_string()),
            };
            if current.operation_id != completed.operation_id
                || (current.outcome.is_some() && resumed_claim.is_none())
                || current.name != completed.name
                || current.arguments != completed.arguments
            {
                return self.fail("nested result intent changed");
            }
            completed = current;
            if resumed_claim.is_some() {
                if completed.child_park.is_none()
                    || !matches!(completed.outcome, Some(Outcome::Pending { .. }))
                {
                    return self.fail("nested child park changed");
                }
                completed.child_park = None;
                completed.outcome = None;
            }
            if self.shared.hooks.is_some()
                && matches!(
                    outcome,
                    Outcome::Succeeded(_)
                        | Outcome::SucceededWithDetails { .. }
                        | Outcome::Failed(_)
                        | Outcome::Skipped(_)
                        | Outcome::Truncated { .. }
                        | Outcome::Structured { .. }
                )
            {
                completed.staged_outcome = Some(outcome.clone());
            } else {
                completed.outcome = Some(validate_nested_output(
                    &self.shared.output_schemas,
                    name,
                    outcome.clone(),
                ));
            }
            let value = match serde_json::to_vec(&completed) {
                Ok(v) => v,
                Err(e) => return self.fail(e.to_string()),
            };
            let mut checks = vec![Check {
                key: key.clone(),
                expected: Expected::Version(entry.version),
            }];
            let mut mutations = vec![Mutation::Put {
                key: key.clone(),
                value,
            }];
            if let Some((snapshot, executor)) = &resumed_claim {
                let retired = match snapshot.retire(*executor) {
                    Ok(batch) => batch,
                    Err(error) => return self.fail(error.to_string()),
                };
                checks.extend(retired.checks);
                mutations.extend(retired.mutations);
            }
            match self.commit(&checks, &mutations) {
                Ok(()) => break,
                Err(crate::nested_fence::Error::Store(durable_store::Error::Conflict {
                    key: conflict,
                })) if conflict == key => continue,
                Err(e) => return self.fail(format!("nested result commit: {e}")),
            }
        }
        if completed.staged_outcome.is_some() {
            return self.finish_staged(&key, completed).await;
        }
        if poll && matches!(outcome, Outcome::Pending { .. }) && reg.tool.can_poll() {
            let version = match store.get(&key) {
                Ok(Some(entry)) => entry.version,
                _ => return self.fail("nested pending record missing after commit"),
            };
            return self.poll_pending(&key, completed, version, reg).await;
        }
        validate_nested_output(&self.shared.output_schemas, name, outcome)
    }

    fn claim_child_park(
        &self,
        journal_key: &[u8],
        journal_version: u64,
        record: &Record,
        park: &ChildPark,
    ) -> Result<Option<(crate::nested_suspension::Snapshot, Uuid, PendingWake)>, String> {
        let binding = self
            .shared
            .fence
            .as_ref()
            .ok_or("nested park root missing")?;
        if binding.suspended || binding.attempt.attempt_id == park.root_attempt {
            return Ok(None);
        }
        let store = self.shared.owner.store();
        let prefix = format!("nested/{}/{}/k", self.shared.root, record.operation_id);
        if !park.leaf_key.starts_with(prefix.as_bytes())
            || record.outcome
                != Some(Outcome::Pending {
                    reference: format!("nested-child-park:{}", park.suspension),
                })
        {
            return Err("invalid nested park binding".into());
        }
        let leaf_entry = store
            .get(&park.leaf_key)
            .map_err(|e| e.to_string())?
            .ok_or("nested park leaf missing")?;
        let leaf: Record = serde_json::from_slice(&leaf_entry.value).map_err(|e| e.to_string())?;
        if matches!(leaf.outcome, Some(Outcome::Pending { .. })) {
            return Ok(None);
        }
        let Some(resolution) = &leaf.resolution else {
            return Err("nested park leaf has no terminal evidence".into());
        };
        if matches!(
            resolution.outcome,
            Outcome::Pending { .. } | Outcome::Unknown(_)
        ) || leaf.outcome.as_ref() != Some(&resolution.outcome)
            || leaf.staged_outcome.is_some()
        {
            return Err("nested park leaf is not terminal".into());
        }
        let mut suspension =
            crate::nested_suspension::load(store, binding.conversation, record.operation_id)
                .map_err(|e| e.to_string())?
                .ok_or("nested suspension missing")?;
        if suspension.suspension_id() != park.suspension
            || suspension.abort_requested()
            || suspension.jobs().len() != 1
            || suspension.jobs()[0].operation_id != leaf.operation_id
            || suspension.jobs()[0].reference != resolution.reference
        {
            return Err("nested suspension evidence changed".into());
        }
        let executor = binding.attempt.attempt_id;
        if suspension.executor() == executor {
            return Err("nested executor already claimed".into());
        }
        let extra = [
            Check {
                key: journal_key.to_vec(),
                expected: Expected::Version(journal_version),
            },
            Check {
                key: park.leaf_key.clone(),
                expected: Expected::Version(leaf_entry.version),
            },
        ];
        if !suspension.is_retired() {
            // A new root attempt can run only after the previous root host was
            // sealed and joined, or after its process died and ownership reopened.
            let mut batch = suspension
                .retire(suspension.executor())
                .map_err(|e| e.to_string())?;
            batch.checks.extend_from_slice(&extra);
            self.commit(&batch.checks, &batch.mutations)
                .map_err(|e| e.to_string())?;
            suspension =
                crate::nested_suspension::load(store, binding.conversation, record.operation_id)
                    .map_err(|e| e.to_string())?
                    .ok_or("nested suspension missing")?;
        }
        let wake = PendingWake {
            checkpoint: suspension.checkpoint().clone(),
            ended: vec![leaf.operation_id],
        };
        let mut batch = suspension.claim(executor).map_err(|e| e.to_string())?;
        batch.checks.extend_from_slice(&extra);
        self.commit(&batch.checks, &batch.mutations)
            .map_err(|e| e.to_string())?;
        let claimed =
            crate::nested_suspension::load(store, binding.conversation, record.operation_id)
                .map_err(|e| e.to_string())?
                .ok_or("nested claim missing")?;
        claimed.claim_check(executor).map_err(|e| e.to_string())?;
        Ok(Some((claimed, executor, wake)))
    }

    fn commit_child_park(
        &self,
        key: &[u8],
        original: &Record,
        request: ChildParkRequest,
    ) -> Outcome {
        let Some(binding) = &self.shared.fence else {
            return self.fail("nested park root missing");
        };
        let store = self.shared.owner.store();
        let Some(entry) = store.get(key).ok().flatten() else {
            return self.fail("nested park intent missing");
        };
        let Ok(mut record) = serde_json::from_slice::<Record>(&entry.value) else {
            return self.fail("invalid nested park intent");
        };
        if record.operation_id != original.operation_id
            || record.outcome.is_some()
            || record.child_park.is_some()
            || record.policy != ReplayPolicy::Safe
        {
            return self.fail("nested park intent changed");
        }
        let suspension = Uuid::new_v4();
        let batch = match crate::nested_suspension::prepare_retired(
            binding.conversation,
            record.operation_id,
            suspension,
            binding.attempt.attempt_id,
            request.checkpoint,
            vec![crate::nested_suspension::PendingJob {
                operation_id: request.operation_id,
                reference: request.reference,
            }],
        ) {
            Ok(batch) => batch,
            Err(error) => return self.fail(error.to_string()),
        };
        let reference = format!("nested-child-park:{suspension}");
        record.child_park = Some(ChildPark {
            suspension,
            root_attempt: binding.attempt.attempt_id,
            leaf_key: request.leaf_key.clone(),
        });
        record.outcome = Some(Outcome::Pending {
            reference: reference.clone(),
        });
        let value = match serde_json::to_vec(&record) {
            Ok(value) => value,
            Err(error) => return self.fail(error.to_string()),
        };
        let mut checks = batch.checks;
        checks.push(Check {
            key: key.to_vec(),
            expected: Expected::Version(entry.version),
        });
        checks.push(Check {
            key: request.leaf_key,
            expected: Expected::Version(request.leaf_version),
        });
        let mut mutations = batch.mutations;
        mutations.push(Mutation::Put {
            key: key.to_vec(),
            value,
        });
        if let Err(error) = self.commit(&checks, &mutations) {
            return self.fail(format!("nested child park commit: {error}"));
        }
        Outcome::Pending { reference }
    }

    /// Finalize staged children before waking their suspended parent.
    pub(crate) async fn finish_staged_results(&self) -> Result<(), String> {
        for (key, entry) in self
            .shared
            .owner
            .store()
            .list(&self.prefix())
            .map_err(|e| e.to_string())?
        {
            let record: Record = serde_json::from_slice(&entry.value).map_err(|e| e.to_string())?;
            if record.staged_outcome.is_some()
                && matches!(self.finish_staged(&key, record).await, Outcome::Unknown(_))
            {
                return Err("nested result hook did not complete".into());
            }
        }
        Ok(())
    }
    async fn before_nested(
        &self,
        key: &[u8],
        mut record: Record,
        reg: &RegisteredTool,
        call_id: &str,
    ) -> Result<Record, Outcome> {
        let Some(hooks) = &self.shared.hooks else {
            return Err(self.fail("nested before-tool hooks unavailable"));
        };
        if record
            .before_gate
            .as_ref()
            .is_some_and(|gate| gate.wake.is_none())
        {
            return Err(Outcome::Pending {
                reference: format!("nested-gate:{}", record.operation_id),
            });
        }
        let invocation = Invocation {
            operation_id: record.operation_id,
            call: ToolCall {
                id: call_id.into(),
                name: record.name.clone(),
                arguments: record.arguments.to_string(),
            },
        };
        let preparation_error = if record
            .before_gate
            .as_ref()
            .is_some_and(|gate| gate.wake.is_some())
        {
            match crate::tool::prepare(
                reg.tool.as_ref(),
                &invocation,
                &self.shared.cancellation,
                self.shared.timeout,
                |schema, args| jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(args)),
            )
            .await
            {
                Ok(prepared) => {
                    record.prepared = prepared.arguments().clone();
                    None
                }
                Err(outcome) => Some(outcome.tool_content()),
            }
        } else {
            None
        };
        if self.shared.cancellation.is_cancelled() {
            return Err(self.fail("nested before-tool preparation cancelled"));
        }
        let memo = crate::extension::DurableMemo::new(
            self.shared.owner.clone(),
            record.operation_id,
            hooks.storage_failed.clone(),
        )
        .with_nested_fence(self.shared.fence.clone());
        let mut snapshot = hooks.snapshot.clone();
        if let Some(wake) = record
            .before_gate
            .as_ref()
            .and_then(|gate| gate.wake.as_ref())
            && let Some(object) = snapshot.as_object_mut()
        {
            object.insert("nested_before_tool_wake".into(), wake.clone());
        }
        let context = crate::extension::Context {
            snapshot: &snapshot,
            memo: &memo,
        };
        let call = crate::extension::ToolCall {
            name: record.name.clone(),
            arguments: record.prepared.clone(),
        };
        let (rewritten, decision, reports) = if preparation_error.is_some() {
            (call, crate::extension::BeforeTool::Pass, Vec::new())
        } else {
            hooks.selected.before_tool(call, &context).await
        };
        if let Err(error) =
            self.record_hook_reports(hooks, record.operation_id, "before_tool", reports)
        {
            return Err(self.fail(error));
        }
        if self.shared.cancellation.is_cancelled() {
            return Err(self.fail("nested before-tool hooks cancelled"));
        }
        let mut invalid_gate = false;
        if let crate::extension::BeforeTool::Sleep(sleep) = &decision {
            invalid_gate = (sleep.until_millis.is_none() && sleep.signals.is_empty())
                || sleep.until_millis.is_some_and(|time| time < 0)
                || sleep.signals.iter().any(String::is_empty);
            if !invalid_gate {
                let store = self.shared.owner.store();
                loop {
                    match crate::conversation_signal::Inbox::new(store)
                        .prepare_wait(hooks.conversation, &sleep.signals)
                    {
                        Ok(wait) => {
                            let entry = match store.get(key) {
                                Ok(Some(entry)) => entry,
                                _ => return Err(self.fail("nested gate intent missing")),
                            };
                            let current: Record = match serde_json::from_slice(&entry.value) {
                                Ok(value) => value,
                                Err(error) => return Err(self.fail(error.to_string())),
                            };
                            if current.operation_id != record.operation_id
                                || !current.before_pending
                                || current.outcome.is_some()
                                || current.staged_outcome.is_some()
                            {
                                return Err(self.fail("nested gate intent changed"));
                            }
                            record.before_gate = Some(BeforeGate {
                                signals: sleep.signals.clone(),
                                until_millis: sleep.until_millis,
                                wake: None,
                            });
                            let value = match serde_json::to_vec(&record) {
                                Ok(value) => value,
                                Err(error) => return Err(self.fail(error.to_string())),
                            };
                            let mut checks = wait.checks;
                            checks.push(Check {
                                key: key.to_vec(),
                                expected: Expected::Version(entry.version),
                            });
                            let mut mutations = wait.mutations;
                            mutations.push(Mutation::Put {
                                key: key.to_vec(),
                                value,
                            });
                            match self.commit(&checks, &mutations) {
                                Ok(()) => (),
                                Err(crate::nested_fence::Error::Store(
                                    durable_store::Error::Conflict { key: conflict },
                                )) if self
                                    .claim
                                    .as_ref()
                                    .is_none_or(|claim| claim.key != conflict) =>
                                {
                                    continue;
                                }
                                Err(error) => {
                                    return Err(self.fail(format!("nested gate commit: {error}")));
                                }
                            }
                            return Err(Outcome::Pending {
                                reference: format!("nested-gate:{}", record.operation_id),
                            });
                        }
                        Err(crate::conversation_signal::Error::InvalidWait) => {
                            invalid_gate = true;
                            break;
                        }
                        Err(error) => return Err(self.fail(error.to_string())),
                    }
                }
            }
        }
        record.prepared = rewritten.arguments;
        let rejection =
            if let Some(error) = preparation_error {
                Some(("invalid_arguments", error))
            } else {
                match decision {
            crate::extension::BeforeTool::Sleep(_) if invalid_gate => Some((
                "invalid_park",
                "Tool call cannot wait at its gates: invalid or consumed signal or deadline".into(),
            )),
            crate::extension::BeforeTool::Block(reason) => {
                Some(("blocked", format!("Tool call blocked: {reason}")))
            }
            crate::extension::BeforeTool::Pass => match crate::tool::restore(
                reg.tool.as_ref(),
                &invocation,
                record.prepared.clone(),
                |schema, args| jsonschema::validator_for(schema).is_ok_and(|v| v.is_valid(args)),
            ) {
                Ok(_) => None,
                Err(outcome) => Some(("invalid_arguments", outcome.tool_content())),
            },
            _ => return Err(self.fail("invalid nested before-tool decision")),
        }
            };
        record.before_pending = false;
        record.before_gate = None;
        if let Some((code, message)) = rejection {
            let diagnostic = crate::runtime::ToolDiagnostic {
                severity: crate::runtime::DiagnosticSeverity::Error,
                code: Some(code.into()),
                message: message.clone(),
            };
            record.derived_diagnostics.push(diagnostic.clone());
            record.diagnostics.push(diagnostic);
            record.staged_outcome = Some(Outcome::Failed(message));
        }
        let entry = match self.shared.owner.store().get(key) {
            Ok(Some(entry)) => entry,
            _ => return Err(self.fail("nested before-tool intent missing")),
        };
        let current: Record = match serde_json::from_slice(&entry.value) {
            Ok(current) => current,
            Err(error) => return Err(self.fail(error.to_string())),
        };
        if current.operation_id != record.operation_id
            || !current.before_pending
            || current.outcome.is_some()
            || current.staged_outcome.is_some()
        {
            return Err(self.fail("nested before-tool intent changed"));
        }
        let value = match serde_json::to_vec(&record) {
            Ok(value) => value,
            Err(error) => return Err(self.fail(error.to_string())),
        };
        if let Err(error) = self.commit(
            &[Check {
                key: key.to_vec(),
                expected: Expected::Version(entry.version),
            }],
            &[Mutation::Put {
                key: key.to_vec(),
                value,
            }],
        ) {
            return Err(self.fail(format!("nested before-tool commit: {error}")));
        }
        Ok(record)
    }

    async fn finish_staged(&self, key: &[u8], mut record: Record) -> Outcome {
        let Some(hooks) = &self.shared.hooks else {
            return self.fail("nested result hooks unavailable for staged outcome");
        };
        let Some(original) = record.staged_outcome.clone() else {
            return self.fail("nested staged outcome missing");
        };
        let call = crate::extension::ToolCall {
            name: record.name.clone(),
            arguments: record.prepared.clone(),
        };
        let memo = crate::extension::DurableMemo::new(
            self.shared.owner.clone(),
            record.operation_id,
            hooks.storage_failed.clone(),
        )
        .with_nested_fence(self.shared.fence.clone());
        let context = crate::extension::Context {
            snapshot: record.hook_snapshot.as_ref().unwrap_or(&hooks.snapshot),
            memo: &memo,
        };
        let output = original.tool_content();
        let mut derived = record.derived_diagnostics.clone();
        // Legacy records predate explicit provenance. A matching error reason
        // is conservatively treated as derived during redaction.
        if !record.diagnostic_provenance
            && let Outcome::Failed(reason) | Outcome::Skipped(reason) = &original
        {
            for diagnostic in &record.diagnostics {
                if diagnostic.severity == crate::runtime::DiagnosticSeverity::Error
                    && diagnostic.message == *reason
                    && !derived.contains(diagnostic)
                {
                    derived.push(diagnostic.clone());
                }
            }
        }
        let (legacy, reports) = hooks
            .selected
            .after_tool(&call, Value::String(output.clone()), &context)
            .await;
        if let Err(error) =
            self.record_hook_reports(hooks, record.operation_id, "after_tool", reports)
        {
            return self.fail(error);
        }
        let rewritten = legacy.as_str().is_some_and(|value| value != output);
        let mut result = crate::extension::ToolResult {
            output: legacy.as_str().unwrap_or(&output).to_owned(),
            is_error: matches!(original, Outcome::Failed(_) | Outcome::Skipped(_)),
            details: match &original {
                Outcome::SucceededWithDetails { details, .. } => Some(details.clone()),
                _ => None,
            },
            structured_output: match &original {
                Outcome::Structured { data, .. } => Some(data.clone()),
                _ => None,
            },
            attachments: match &original {
                Outcome::Structured { attachments, .. } => attachments.clone(),
                _ => Vec::new(),
            },
            diagnostics: record.diagnostics.clone(),
        };
        if rewritten {
            result.details = None;
            result.structured_output = None;
            result.attachments.clear();
            result.diagnostics = vec![crate::runtime::ToolDiagnostic {
                severity: crate::runtime::DiagnosticSeverity::Info,
                code: Some("output_redacted".into()),
                message: "tool metadata withheld after output replacement".into(),
            }];
        }
        let before_rich_diagnostics = result.diagnostics.clone();
        let (result, rich_rewritten, rich_output_replaced, reports) = hooks
            .selected
            .after_tool_result_with_derived(&call, result, &context, &derived)
            .await;
        if let Err(error) =
            self.record_hook_reports(hooks, record.operation_id, "after_tool_result", reports)
        {
            return self.fail(error);
        }
        if self.shared.cancellation.is_cancelled() {
            return self.fail("nested result hooks cancelled");
        }
        if rich_output_replaced {
            derived.clear();
        } else {
            derived = crate::extension::remaining_derived_diagnostics(
                &before_rich_diagnostics,
                &result.diagnostics,
                &derived,
            );
        }
        let outcome = if !rewritten && !rich_rewritten {
            original
        } else if result.is_error {
            Outcome::Failed(result.output.clone())
        } else if let Some(data) = result.structured_output.clone() {
            Outcome::Structured {
                data,
                attachments: result.attachments.clone(),
            }
        } else if let Some(details) = result.details.clone() {
            Outcome::SucceededWithDetails {
                output: result.output.clone(),
                details,
            }
        } else {
            Outcome::Succeeded(result.output.clone())
        };
        let outcome = validate_nested_output(&self.shared.output_schemas, &record.name, outcome);
        let mut extra = std::collections::BTreeMap::new();
        extra.insert(
            "isError".into(),
            serde_json::json!(matches!(outcome, Outcome::Failed(_) | Outcome::Skipped(_))),
        );
        if let Outcome::SucceededWithDetails { details, .. } = &outcome {
            extra.insert("details".into(), details.clone());
        }
        if let Outcome::Structured { data, .. } = &outcome {
            extra.insert("structuredOutput".into(), data.clone());
        }
        let attachments = match &outcome {
            Outcome::Structured { attachments, .. } => attachments.clone(),
            _ => Vec::new(),
        };
        extra.insert(
            "attachments".into(),
            match serde_json::to_value(attachments) {
                Ok(value) => value,
                Err(error) => return self.fail(error.to_string()),
            },
        );
        let block_result = crate::extension::ToolBlockResult {
            output: vec![serde_json::json!({"type":"text", "text": result.output})],
            diagnostics: result.diagnostics.clone(),
            extra,
        };
        let original_blocks = block_result.clone();
        let (blocks, _, reports) = match hooks
            .selected
            .runtime_after_tool_blocks_with_derived(&call, block_result, true, &context, &derived)
            .await
        {
            Ok(value) => value,
            Err(error) => return self.fail(error),
        };
        if let Err(error) = self.record_hook_reports(
            hooks,
            record.operation_id,
            "after_tool_blocks",
            reports.clone(),
        ) {
            return self.fail(error);
        }
        if !reports.is_empty() || self.shared.cancellation.is_cancelled() {
            return self.fail("nested block result hook failed or was cancelled");
        }
        let bounded = blocks.diagnostics.len() <= 32
            && blocks.diagnostics.iter().all(|diagnostic| {
                diagnostic.message.len() <= 4096
                    && diagnostic
                        .code
                        .as_ref()
                        .is_none_or(|code| code.len() <= 256)
            })
            && serde_json::to_vec(&blocks.diagnostics).is_ok_and(|bytes| bytes.len() <= 65_536)
            && serde_json::to_vec(&blocks.output).is_ok_and(|bytes| bytes.len() <= 8 * 1024 * 1024)
            && serde_json::to_vec(&(
                &result.details,
                &result.structured_output,
                &result.attachments,
                &blocks.diagnostics,
            ))
            .is_ok_and(|bytes| bytes.len() <= 65_536);
        if blocks != original_blocks && !bounded {
            return self.fail("nested block result exceeds limits");
        }
        // The block-aware projection is authoritative. Never keep the earlier
        // text-bearing outcome alongside a rewritten block result: callers can
        // observe both, and the diagnostic fallback also reads that outcome.
        // A rich hook may replace text while keeping structured metadata. The
        // core outcome cannot carry both channels, so preserve the final text
        // blocks even when the later block hook leaves them unchanged.
        let blocks_changed = blocks.output != original_blocks.output
            || ((rewritten || rich_output_replaced)
                && matches!(outcome, Outcome::Structured { .. }));
        let projected_structured =
            if blocks_changed && matches!(outcome, Outcome::Structured { .. }) {
                Some(outcome.clone())
            } else {
                None
            };
        let outcome = if blocks_changed {
            outcome_from_blocks(outcome, &blocks.output)
        } else {
            outcome
        };
        let entry = match self.shared.owner.store().get(key) {
            Ok(Some(entry)) => entry,
            _ => return self.fail("nested staged result missing"),
        };
        let current: Record = match serde_json::from_slice(&entry.value) {
            Ok(value) => value,
            Err(error) => return self.fail(error.to_string()),
        };
        if current.operation_id != record.operation_id
            || current.outcome.is_some()
            || current.staged_outcome != record.staged_outcome
        {
            return self.fail("nested staged result changed");
        }
        record = current;
        record.staged_outcome = None;
        record.outcome = Some(outcome.clone());
        record.projected_structured = projected_structured;
        record.output_blocks = blocks_changed.then_some(blocks.output);
        record.diagnostics = blocks.diagnostics;
        record.derived_diagnostics.clear();
        record.diagnostic_provenance = true;
        record.diagnostic_keys.clear();
        let value = match serde_json::to_vec(&record) {
            Ok(value) => value,
            Err(error) => return self.fail(error.to_string()),
        };
        let mut checks = vec![Check {
            key: key.to_vec(),
            expected: Expected::Version(entry.version),
        }];
        if let Some((key, version)) = &hooks.conversation_fence {
            checks.push(Check {
                key: key.clone(),
                expected: Expected::Version(*version),
            });
        }
        if let Err(error) = self.commit(
            &checks,
            &[Mutation::Put {
                key: key.to_vec(),
                value,
            }],
        ) {
            return self.fail(format!("nested hook result commit: {error}"));
        }
        outcome
    }

    fn record_hook_reports(
        &self,
        hooks: &NestedHooks,
        operation: Uuid,
        phase: &str,
        reports: Vec<crate::extension::Report>,
    ) -> Result<(), String> {
        if reports.is_empty() {
            return Ok(());
        }
        let batch = crate::runtime::ExtensionReportBatch {
            conversation_id: hooks.conversation,
            operation_id: operation,
            phase: phase.into(),
            reports,
        };
        let key = format!(
            "runtime/extension-report/{}/{operation}/{phase}",
            hooks.conversation
        )
        .into_bytes();
        crate::report::index_with_fence(
            self.shared.owner.store(),
            key,
            &batch,
            false,
            self.shared.fence.as_ref(),
        )
        .map_err(|error| {
            if !matches!(error, crate::runtime::Error::Interrupted) {
                hooks.storage_failed.store(true, Ordering::Release);
            }
            error.to_string()
        })
    }

    async fn poll_pending(
        &self,
        key: &[u8],
        mut record: Record,
        mut version: u64,
        reg: &RegisteredTool,
    ) -> Outcome {
        loop {
            let identity_lease = if let Some(expected) = record.durable_identity.as_deref() {
                let Some(lease) = crate::nested_identity::lease(&reg.tool, expected) else {
                    return self.pending_failure(
                        "nested poll identity changed; pending reference retained",
                    );
                };
                Some(lease)
            } else {
                match crate::nested_identity::execution_lease(&reg.tool) {
                    Ok(Some(lease)) if lease.is_some() => Some(lease),
                    Ok(None) => None,
                    _ => {
                        return self.pending_failure(
                            "nested poll identity unavailable; pending reference retained",
                        );
                    }
                }
            };
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
            let terminal = !matches!(outcome, Outcome::Pending { .. });
            let staged = terminal && record.hooks_required;
            let outcome = if staged {
                outcome
            } else {
                validate_nested_output(&self.shared.output_schemas, &record.name, outcome)
            };
            if staged {
                record.staged_outcome = Some(outcome.clone());
                record.outcome = None;
            } else {
                record.outcome = Some(outcome.clone());
            }
            let value = match serde_json::to_vec(&record) {
                Ok(value) => value,
                Err(e) => return self.fail(e.to_string()),
            };
            if let Err(e) = self.commit(
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
            drop(identity_lease);
            if staged {
                return self.finish_staged(key, record).await;
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
fn replay_finalized(
    schemas: &std::collections::BTreeMap<String, Value>,
    name: &str,
    record: &Record,
) -> Result<Outcome, &'static str> {
    let outcome = record.outcome.as_ref().ok_or("nested outcome missing")?;
    if let Some(original) = &record.projected_structured {
        let blocks = record
            .output_blocks
            .as_ref()
            .ok_or("nested structured projection missing blocks")?;
        if !matches!(original, Outcome::Structured { .. })
            || outcome_from_blocks(original.clone(), blocks) != *outcome
        {
            return Err("invalid nested structured projection");
        }
        let checked = validate_nested_output(schemas, name, original.clone());
        return Ok(if checked == *original {
            outcome.clone()
        } else {
            checked
        });
    }
    Ok(validate_nested_output(schemas, name, outcome.clone()))
}

fn validate_nested_output(
    schemas: &std::collections::BTreeMap<String, Value>,
    name: &str,
    outcome: Outcome,
) -> Outcome {
    let Some(schema) = schemas.get(name) else {
        return outcome;
    };
    let invalid = match &outcome {
        Outcome::Structured { data, .. } => {
            !crate::runtime::local_schema_references(schema)
                || jsonschema::validator_for(schema)
                    .map_or(true, |validator| !validator.is_valid(data))
        }
        Outcome::Succeeded(_)
        | Outcome::SucceededWithDetails { .. }
        | Outcome::Truncated { .. } => true,
        _ => false,
    };
    if invalid {
        Outcome::Failed("invalid_structured_output: structured output missing or does not match the configured schema".into())
    } else {
        outcome
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
    fn gate_recovery_accepts_pending_and_rejects_uncertain_children() {
        let store = durable_store::Store::memory().unwrap();
        let root = Uuid::new_v4();
        let key = format!("nested/{root}/{root}/gate").into_bytes();
        let other = format!("nested/{root}/{root}/other").into_bytes();
        let mut record = Record {
            name: "child".into(),
            arguments: json!({}),
            prepared: json!({}),
            before_pending: true,
            child_park: None,
            before_gate: Some(BeforeGate {
                signals: vec!["gate".into()],
                until_millis: None,
                wake: None,
            }),
            policy: ReplayPolicy::Unsafe,
            durable_identity: None,
            operation_id: Uuid::new_v4(),
            outcome: None,
            staged_outcome: None,
            hooks_required: true,
            hook_snapshot: None,
            output_blocks: None,
            projected_structured: None,
            diagnostics: Vec::new(),
            derived_diagnostics: Vec::new(),
            diagnostic_provenance: true,
            diagnostic_keys: Default::default(),
            resolution: None,
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(recoverable_gate_intent(&store, root).unwrap());
        record.before_pending = false;
        record.before_gate = None;
        record.outcome = Some(Outcome::Pending {
            reference: "remote".into(),
        });
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: other.clone(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(recoverable_gate_intent(&store, root).unwrap());
        record.outcome = Some(Outcome::Unknown("uncertain".into()));
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: other.clone(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(!recoverable_gate_intent(&store, root).unwrap());
        record.outcome = None;
        record.staged_outcome = Some(Outcome::Succeeded("staged".into()));
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: other.clone(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(recoverable_gate_intent(&store, root).unwrap());
        record.staged_outcome = None;
        record.outcome = Some(Outcome::Succeeded("done".into()));
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: other,
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(recoverable_gate_intent(&store, root).unwrap());
        store.commit(&[], &[Mutation::Delete { key }]).unwrap();
        assert!(!recoverable_gate_intent(&store, root).unwrap());
    }

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
            before_pending: false,
            child_park: None,
            before_gate: None,
            policy: ReplayPolicy::Unsafe,
            durable_identity: None,
            operation_id: operation,
            outcome: Some(Outcome::Pending {
                reference: "opaque".into(),
            }),
            staged_outcome: None,
            hooks_required: false,
            hook_snapshot: None,
            output_blocks: None,
            projected_structured: None,
            diagnostics: Vec::new(),
            derived_diagnostics: Vec::new(),
            diagnostic_provenance: true,
            diagnostic_keys: Default::default(),
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
        let erased: Arc<dyn pi_rs_core::Tool> = tool.clone();
        let _ = crate::nested_identity::register(&erased, Some("test-backend:polling:v1".into()));
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
    async fn registered_unset_child_cannot_dispatch_until_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let tool = Arc::new(Polling {
            invoked: AtomicUsize::new(0),
            mode: AtomicUsize::new(2),
            ids: Mutex::new(vec![]),
        });
        let erased: Arc<dyn Tool> = tool.clone();
        let handle =
            crate::nested_identity::register(&erased, Some("test-backend:polling:v1".into()))
                .unwrap();
        let panicking_handle = handle.clone();
        assert!(
            tokio::spawn(async move {
                panicking_handle
                    .update(Some("changed".into()), || panic!("partial update"))
                    .await
            })
            .await
            .is_err()
        );
        let host = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(
            host.call_keyed_unpolled_with_id("key", "polling", json!({}))
                .await
                .is_err()
        );
        assert_eq!(tool.invoked.load(Ordering::SeqCst), 0);
        assert!(
            host.shared
                .owner
                .store()
                .list(format!("nested/{root}/").as_bytes())
                .unwrap()
                .is_empty()
        );
        drop(host);
        handle
            .update(Some("test-backend:polling:v1".into()), || {})
            .await
            .unwrap();
        let host = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(matches!(
            host.call_keyed_unpolled_with_id("key", "polling", json!({}))
                .await
                .unwrap()
                .outcome,
            Outcome::Pending { .. }
        ));
        assert_eq!(tool.invoked.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn invalidated_registered_legacy_job_cannot_poll_until_repaired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let tool = Arc::new(Polling {
            invoked: AtomicUsize::new(0),
            mode: AtomicUsize::new(2),
            ids: Mutex::new(vec![]),
        });
        let erased: Arc<dyn Tool> = tool.clone();
        let handle =
            crate::nested_identity::register(&erased, Some("test-backend:polling:v1".into()))
                .unwrap();
        let host = polling_host(&path, root, tool.clone(), CancellationToken::new());
        host.call_keyed_unpolled_with_id("key", "polling", json!({}))
            .await
            .unwrap();
        let store = host.shared.owner.store();
        let (key, entry) = store
            .list(format!("nested/{root}/").as_bytes())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let mut record: Value = serde_json::from_slice(&entry.value).unwrap();
        assert!(
            record
                .as_object_mut()
                .unwrap()
                .remove("durable_identity")
                .is_some()
        );
        store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        drop(host);
        let failing = handle.clone();
        assert!(
            tokio::spawn(async move {
                failing
                    .update(Some("changed".into()), || panic!("partial update"))
                    .await
            })
            .await
            .is_err()
        );
        let host = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(matches!(
            host.call_keyed("key", "polling", json!({})).await,
            Outcome::Unknown(_)
        ));
        assert_eq!(tool.ids.lock().unwrap().len(), 1);
        drop(host);
        handle
            .update(Some("test-backend:polling:v1".into()), || {})
            .await
            .unwrap();
        let host = polling_host(&path, root, tool.clone(), CancellationToken::new());
        assert!(matches!(
            host.call_keyed("key", "polling", json!({})).await,
            Outcome::Succeeded(_)
        ));
        assert_eq!(tool.ids.lock().unwrap().len(), 2);
    }
    #[tokio::test]
    async fn checkpoint_is_bounded_and_wake_is_root_scoped_and_revoked() {
        let dir = tempfile::tempdir().unwrap();
        let root = Uuid::new_v4();
        let tool = Arc::new(Polling {
            invoked: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            ids: Mutex::new(vec![]),
        });
        let host = polling_host(&dir.path().join("db"), root, tool, CancellationToken::new());
        let call = host
            .call_keyed_unpolled_with_id("key", "polling", json!({}))
            .await
            .unwrap();
        assert!(matches!(call.outcome, Outcome::Pending { .. }));
        assert!(
            host.suspend_pending_with_checkpoint(Some(json!("x".repeat(65_537))))
                .is_err()
        );
        assert!(!host.explicitly_suspended());
        host.suspend_pending_with_checkpoint(Some(json!({"saved":true})))
            .unwrap();
        let saved = host.parked_checkpoint().unwrap().unwrap();
        assert_eq!(saved.ended, vec![call.operation_id]);
        host.set_resumed_checkpoint(Some(saved.clone())).unwrap();
        assert_eq!(host.resumed_checkpoint(), Some(saved));
        let child = NestedHost {
            shared: host.shared.clone(),
            parent: Uuid::new_v4(),
            depth: 1,
            serial: tokio::sync::Mutex::new(()),
            child_park_request: Mutex::new(None),
            child_wake: None,
            child_replay_safe: false,
            claim: None,
        };
        assert!(child.resumed_checkpoint().is_none());
        drop(InvocationScope(host.clone()));
        assert!(host.resumed_checkpoint().is_none());
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
    #[test]
    fn structured_projection_revalidates_after_store_reopen_and_rejects_tampering() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let record = Record {
            name: "child".into(),
            arguments: json!({}),
            prepared: json!({}),
            before_pending: false,
            child_park: None,
            before_gate: None,
            policy: ReplayPolicy::Unsafe,
            durable_identity: None,
            operation_id: Uuid::new_v4(),
            outcome: Some(Outcome::Succeeded("safe".into())),
            staged_outcome: None,
            hooks_required: true,
            hook_snapshot: None,
            output_blocks: Some(vec![json!({"type":"text","text":"safe"})]),
            projected_structured: Some(Outcome::Structured {
                data: json!({"n":7}),
                attachments: vec![],
            }),
            diagnostics: vec![],
            derived_diagnostics: vec![],
            diagnostic_provenance: true,
            diagnostic_keys: Default::default(),
            resolution: None,
        };
        let schema =
            json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]});
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert("child".to_owned(), schema);
        let store = durable_store::Store::open(&path).unwrap();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: b"record".to_vec(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        drop(store);
        let store = durable_store::Store::open(&path).unwrap();
        let mut restored: Record =
            serde_json::from_slice(&store.get(b"record").unwrap().unwrap().value).unwrap();
        for _ in 0..2 {
            assert_eq!(
                replay_finalized(&schemas, "child", &restored).unwrap(),
                Outcome::Succeeded("safe".into())
            );
        }
        restored.output_blocks = Some(vec![json!({"type":"text","text":"tampered"})]);
        assert!(replay_finalized(&schemas, "child", &restored).is_err());
        restored.output_blocks = record.output_blocks;
        schemas.insert("child".into(), json!({"type":"string"}));
        assert!(
            matches!(replay_finalized(&schemas, "child", &restored).unwrap(), Outcome::Failed(reason) if reason.starts_with("invalid_structured_output:"))
        );
    }
    #[tokio::test]
    async fn rich_structured_projection_reattaches_to_parent_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let root = Uuid::new_v4();
        let calls = Arc::new(AtomicUsize::new(0));
        let make_host = || {
            let mut schemas = std::collections::BTreeMap::new();
            schemas.insert(
                "fake".to_owned(),
                json!({"type":"object","required":["secret"]}),
            );
            NestedHost::new_with_schemas(
                Arc::new(OwnedStore::open(&path).unwrap()),
                root,
                vec![RegisteredTool {
                    tool: Arc::new(Fake {
                        calls: calls.clone(),
                        pending: false,
                    }),
                    replay: ReplayPolicy::Unsafe,
                }],
                CancellationToken::new(),
                Duration::from_secs(10),
                2,
                (schemas, None, None),
            )
            .unwrap()
        };
        let h = make_host();
        let record = Record {
            name: "fake".into(),
            arguments: json!({}),
            prepared: json!({}),
            before_pending: false,
            child_park: None,
            before_gate: None,
            policy: ReplayPolicy::Unsafe,
            durable_identity: None,
            operation_id: Uuid::new_v4(),
            outcome: Some(Outcome::Succeeded("redacted".into())),
            staged_outcome: None,
            hooks_required: true,
            hook_snapshot: None,
            output_blocks: Some(vec![json!({"type":"text","text":"redacted"})]),
            projected_structured: Some(Outcome::Structured {
                data: json!({"secret":"private"}),
                attachments: vec![],
            }),
            diagnostics: vec![],
            derived_diagnostics: vec![],
            diagnostic_provenance: true,
            diagnostic_keys: Default::default(),
            resolution: None,
        };
        let key = h.explicit_key(root, "key");
        h.shared
            .owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key,
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        drop(h);
        let h = make_host();
        for _ in 0..2 {
            let result = h.call_keyed_result("key", "fake", json!({}), true).await;
            assert_eq!(result.outcome, Outcome::Succeeded("redacted".into()));
            assert_eq!(
                result.output_blocks,
                Some(vec![json!({"type":"text","text":"redacted"})])
            );
            assert!(!format!("{result:?}").contains("private"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
