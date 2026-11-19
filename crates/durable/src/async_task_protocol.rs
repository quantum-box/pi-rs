//! Durable records for task-backed tool dispatch. Records alone do not grant
//! an executor lease; the driver retains and joins the authenticated executor.
use crate::async_task_tool::ToolIdentity;
use crate::task::{Task, TaskOwner, TaskState};
use crate::task_store::{OwnedBatch, StoredTask, TaskStore};
use durable_store::{Check, Expected, Mutation, Store};
use pi_rs_core::Outcome;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("store: {0}")]
    Store(#[from] durable_store::Error),
    #[error("task: {0}")]
    Task(#[from] crate::task_store::Error),
    #[error("child: {0}")]
    Child(#[from] crate::keyed_child::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("parent settlement pending")]
    Deferred,
    #[error("stale or invalid task, identity, intent, or receipt")]
    Invalid,
}

impl Error {
    pub(crate) fn pre_transaction_busy(&self) -> bool {
        matches!(self, Self::Store(durable_store::Error::WriteLockBusy(_)))
    }
}

pub(crate) fn has_sealed_identity(store: &Store, task: Uuid) -> Result<bool, Error> {
    Ok(store.get(&identity_key(task))?.is_some())
}

fn identity_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/identity/{task}").into_bytes()
}
pub(crate) fn undispatched_intent_check(task: Uuid) -> Check {
    Check {
        key: intent_key(task),
        expected: Expected::Missing,
    }
}

fn intent_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/intent/{task}").into_bytes()
}
// An immutable dispatch record for each execution generation. The existing
// task-level record remains the dispatch fence for the leaf-only executor.
fn attempt_intent_key(task: Uuid, attempt: Uuid) -> Vec<u8> {
    format!("durable/async-task/attempt/{task}/{attempt}/intent").into_bytes()
}
fn prepared_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/prepared/{task}").into_bytes()
}
fn receipt_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/receipt/{task}").into_bytes()
}
fn raw_result_key(task: Uuid) -> Vec<u8> {
    format!("durable/async-task/raw-result/{task}").into_bytes()
}

/// Compose the child, keyed admission, and definition identity in one batch.
/// This neither validates the current tool registration nor transfers a lease.
pub(crate) fn prepare_child(
    store: &Store,
    parent: &StoredTask,
    key: &str,
    child: &Task,
    identity: &ToolIdentity,
) -> Result<(Uuid, OwnedBatch), Error> {
    if !identity.is_valid() {
        return Err(Error::Invalid);
    }
    let encoded = serde_json::to_string(identity)?;
    if encoded.len() > 65_536 {
        return Err(Error::Invalid);
    }
    let prepared = crate::keyed_child::prepare(store, parent, key, &encoded, child.input(), child)?;
    let id = prepared.child;
    let mut batch = OwnedBatch {
        checks: prepared.checks,
        mutations: prepared.mutations,
    };
    let identity_batch = prepare_child_identity(store, id, identity)?;
    batch.checks.extend(identity_batch.checks);
    batch.mutations.extend(identity_batch.mutations);
    Ok((id, batch))
}

/// Prepare an identity record for composition into the same checked child
/// admission and parent checkpoint transaction.
pub(crate) fn prepare_child_identity(
    store: &Store,
    id: Uuid,
    identity: &ToolIdentity,
) -> Result<OwnedBatch, Error> {
    if id.is_nil() || !identity.is_valid() {
        return Err(Error::Invalid);
    }
    let encoded = serde_json::to_vec(identity)?;
    if encoded.len() > 65_536 {
        return Err(Error::Invalid);
    }
    let key = identity_key(id);
    let existing = store.get(&key)?;
    if let Some(ref entry) = existing
        && serde_json::from_slice::<ToolIdentity>(&entry.value)? != *identity
    {
        return Err(Error::Invalid);
    }
    let checks = vec![Check {
        key: key.clone(),
        expected: existing
            .as_ref()
            .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
    }];
    let mutations = if existing.is_none() {
        vec![Mutation::Put {
            key,
            value: encoded,
        }]
    } else {
        vec![]
    };
    Ok(OwnedBatch { checks, mutations })
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Intent {
    task: Uuid,
    attempt: Uuid,
    identity: ToolIdentity,
    // Pre-generation records lack this flag and remain readable after upgrade.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    attempt_record: bool,
}

fn attempt_record_check(store: &Store, intent: &Intent) -> Result<Option<Check>, Error> {
    if !intent.attempt_record {
        return Ok(None);
    }
    let key = attempt_intent_key(intent.task, intent.attempt);
    let entry = store.get(&key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<Intent>(&entry.value)? != *intent {
        return Err(Error::Invalid);
    }
    Ok(Some(Check {
        key,
        expected: Expected::Version(entry.version),
    }))
}

/// Load a private intent for a joined driver attempt. The matching task attempt
/// is checked by the caller before the receipt can be written.
pub(crate) fn load_intent(store: &Store, task: Uuid) -> Result<Intent, Error> {
    let entry = store.get(&intent_key(task))?.ok_or(Error::Invalid)?;
    let intent: Intent = serde_json::from_slice(&entry.value)?;
    if intent.task != task || !intent.identity.is_valid() {
        return Err(Error::Invalid);
    }
    attempt_record_check(store, &intent)?;
    Ok(intent)
}

impl Intent {
    pub(crate) fn matches_attempt(&self, task: &Task) -> bool {
        self.task == task.id() && task.attempt_id() == Some(self.attempt)
    }
}

/// Stage an attempt-specific durable intent. It is not a lease or standalone
/// authorization. An owning worker may treat its commit as logical dispatch
/// only while it separately retains and joins the authenticated executor.
/// Restricted to unlinked standalone ownership.
pub(crate) fn prepare_intent(
    store: &Store,
    running: &StoredTask,
    identity: &ToolIdentity,
) -> Result<(Intent, OwnedBatch), Error> {
    prepare_authorized_phase(store, running, identity, false)
}

/// Like intent preparation, but authorizes only a completed durable raw
/// outcome. The raw result is tied to the original dispatch intent while the
/// task and ancestor checks bind the current (possibly recovered) attempt.
pub(crate) fn prepare_after_phase(
    store: &Store,
    running: &StoredTask,
    identity: &ToolIdentity,
) -> Result<(Intent, OwnedBatch), Error> {
    prepare_authorized_phase(store, running, identity, true)
}
fn prepare_authorized_phase(
    store: &Store,
    running: &StoredTask,
    identity: &ToolIdentity,
    after: bool,
) -> Result<(Intent, OwnedBatch), Error> {
    if !identity.is_valid() {
        return Err(Error::Invalid);
    }
    let tasks = TaskStore::new(store);
    if running.task.state() != &TaskState::Running || running.task.abort_requested() {
        return Err(Error::Invalid);
    }
    let attempt = running.task.attempt_id().ok_or(Error::Invalid)?;
    let mut checks = Vec::new();
    let mut current = running.clone();
    let mut seen = std::collections::HashSet::new();
    loop {
        if !seen.insert(current.task.id())
            || current.task.abort_requested()
            || current.task.is_terminal()
            || current.task.is_completing()
            || current.task.is_background()
        {
            return Err(Error::Invalid);
        }
        checks.push(Check {
            key: crate::task_store::key(current.task.id()),
            expected: Expected::Version(current.version),
        });
        checks.push(crate::ownership_edge::abort_fence_check(
            store,
            current.task.owner(),
        )?);
        match current.task.owner() {
            TaskOwner::Task(parent) => current = tasks.load(*parent)?.ok_or(Error::Invalid)?,
            TaskOwner::Conversation(id) => {
                let conversation = format!("runtime/conversation/{id}").into_bytes();
                let edge = crate::ownership_edge::edge_key(*id);
                if store.get(&conversation)?.is_some() || store.get(&edge)?.is_some() {
                    return Err(Error::Invalid);
                }
                checks.push(Check {
                    key: conversation,
                    expected: Expected::Missing,
                });
                checks.push(Check {
                    key: edge,
                    expected: Expected::Missing,
                });
                break;
            }
        }
    }
    let identity_key = identity_key(running.task.id());
    let stored_identity = store.get(&identity_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&stored_identity.value)? != *identity {
        return Err(Error::Invalid);
    }
    checks.push(Check {
        key: identity_key,
        expected: Expected::Version(stored_identity.version),
    });
    let key = intent_key(running.task.id());
    let expected = Intent {
        task: running.task.id(),
        attempt,
        identity: identity.clone(),
        attempt_record: true,
    };
    if after {
        if !identity.has_after_hook() {
            return Err(Error::Invalid);
        }
        let stored = store.get(&key)?.ok_or(Error::Invalid)?;
        let intent: Intent = serde_json::from_slice(&stored.value)?;
        if intent.task != running.task.id() || intent.identity != *identity {
            return Err(Error::Invalid);
        }
        checks.push(Check {
            key,
            expected: Expected::Version(stored.version),
        });
        checks.extend(attempt_record_check(store, &intent)?);
        let raw_key = raw_result_key(running.task.id());
        let raw = store.get(&raw_key)?.ok_or(Error::Invalid)?;
        let (raw_intent, _): (Intent, Outcome) = serde_json::from_slice(&raw.value)?;
        if raw_intent != intent {
            return Err(Error::Invalid);
        }
        checks.push(Check {
            key: raw_key,
            expected: Expected::Version(raw.version),
        });
        Ok((
            intent,
            OwnedBatch {
                checks,
                mutations: vec![],
            },
        ))
    } else {
        if store.get(&key)?.is_some() {
            return Err(Error::Invalid);
        }
        checks.push(Check {
            key: key.clone(),
            expected: Expected::Missing,
        });
        let attempt_key = attempt_intent_key(expected.task, expected.attempt);
        checks.push(Check {
            key: attempt_key.clone(),
            expected: Expected::Missing,
        });
        let value = serde_json::to_vec(&expected)?;
        let mutations = vec![
            Mutation::Put {
                key,
                value: value.clone(),
            },
            Mutation::Put {
                key: attempt_key,
                value,
            },
        ];
        Ok((expected, OwnedBatch { checks, mutations }))
    }
}

/// Persist the exact normalized arguments before invoking the tool. This does
/// not authorize dispatch; the worker must retain its identity and task leases.
pub(crate) fn prepare_arguments_record(
    store: &Store,
    intent: &Intent,
    arguments: &serde_json::Value,
) -> Result<OwnedBatch, Error> {
    let serialized = serde_json::to_vec(&(intent, arguments))?;
    if serialized.len() > 16 * 1024 * 1024 {
        return Err(Error::Invalid);
    }
    let intent_key = intent_key(intent.task);
    let stored_intent = store.get(&intent_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<Intent>(&stored_intent.value)? != *intent {
        return Err(Error::Invalid);
    }
    let definition_key = identity_key(intent.task);
    let definition = store.get(&definition_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != intent.identity {
        return Err(Error::Invalid);
    }
    let key = prepared_key(intent.task);
    let existing = store.get(&key)?;
    if existing
        .as_ref()
        .is_some_and(|entry| entry.value != serialized)
    {
        return Err(Error::Invalid);
    }
    let mut checks = vec![
        Check {
            key: intent_key,
            expected: Expected::Version(stored_intent.version),
        },
        Check {
            key: definition_key,
            expected: Expected::Version(definition.version),
        },
        Check {
            key: key.clone(),
            expected: existing
                .as_ref()
                .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
        },
    ];
    checks.extend(attempt_record_check(store, intent)?);
    let mutations = if existing.is_none() {
        vec![Mutation::Put {
            key,
            value: serialized,
        }]
    } else {
        vec![]
    };
    Ok(OwnedBatch { checks, mutations })
}

/// Store a tool outcome independently of task settlement and abort.
/// The caller must join and authenticate the executor; this checks persisted
/// identity only and is not a lease. A lost commit requires readback.
pub(crate) fn prepare_receipt(
    store: &Store,
    intent: &Intent,
    outcome: &Outcome,
) -> Result<OwnedBatch, Error> {
    let intent_key = intent_key(intent.task);
    let stored = store.get(&intent_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<Intent>(&stored.value)? != *intent {
        return Err(Error::Invalid);
    }
    let definition_key = identity_key(intent.task);
    let definition = store.get(&definition_key)?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != intent.identity {
        return Err(Error::Invalid);
    }
    let value = serde_json::to_vec(&(intent, outcome))?;
    if value.len() > 65_536 {
        return Err(Error::Invalid);
    }
    let key = receipt_key(intent.task);
    let existing = store.get(&key)?;
    if existing.as_ref().is_some_and(|entry| entry.value != value) {
        return Err(Error::Invalid);
    }
    let mut checks = vec![
        Check {
            key: definition_key,
            expected: Expected::Version(definition.version),
        },
        Check {
            key: intent_key,
            expected: Expected::Version(stored.version),
        },
        Check {
            key: key.clone(),
            expected: existing
                .as_ref()
                .map_or(Expected::Missing, |e| Expected::Version(e.version)),
        },
    ];
    checks.extend(attempt_record_check(store, intent)?);
    let mutations = if existing.is_none() {
        vec![Mutation::Put { key, value }]
    } else {
        vec![]
    };
    Ok(OwnedBatch { checks, mutations })
}

/// Persist the joined Tool result separately; it is not a final receipt and
/// does not by itself authorize the after hook.
pub(crate) fn prepare_raw_result(
    store: &Store,
    intent: &Intent,
    outcome: &Outcome,
) -> Result<OwnedBatch, Error> {
    if !intent.identity.has_after_hook() {
        return Err(Error::Invalid);
    }
    let mut batch = prepare_receipt(store, intent, outcome)?;
    let final_key = receipt_key(intent.task);
    if store.get(&final_key)?.is_some() {
        return Err(Error::Invalid);
    }
    let raw_key = raw_result_key(intent.task);
    // A final receipt must not already exist when publishing raw output.
    let raw = store.get(&raw_key)?;
    let encoded = serde_json::to_vec(&(intent, outcome))?;
    if raw.as_ref().is_some_and(|entry| entry.value != encoded) {
        return Err(Error::Invalid);
    }
    batch.checks.push(Check {
        key: raw_key.clone(),
        expected: raw
            .as_ref()
            .map_or(Expected::Missing, |e| Expected::Version(e.version)),
    });
    batch
        .mutations
        .retain(|m| !matches!(m, Mutation::Put { key, .. } if *key == final_key));
    if raw.is_none() {
        batch.mutations.push(Mutation::Put {
            key: raw_key,
            value: encoded,
        });
    }
    Ok(batch)
}
/// Read the persisted sealed identity to decide whether a missing definition
/// must remain blocked for an explicitly opted-in interrupted cleanup.
pub(crate) fn interrupted_identity(
    store: &Store,
    task: Uuid,
) -> Result<Option<ToolIdentity>, Error> {
    let Some(entry) = store.get(&identity_key(task))? else {
        return Ok(None);
    };
    let identity: ToolIdentity = serde_json::from_slice(&entry.value)?;
    if !identity.is_valid() {
        return Err(Error::Invalid);
    };
    Ok(identity.has_interrupted_after().then_some(identity))
}
/// Checks identify the original dispatch and require raw and final receipts to
/// remain absent. No record is changed or treated as an external outcome.
pub(crate) fn interrupted_evidence(
    store: &Store,
    task: &Task,
    identity: &ToolIdentity,
    require_running_attempt: bool,
) -> Result<Option<Vec<Check>>, Error> {
    if !identity.has_interrupted_after()
        || task.replay_safe()
        || task.checkpoint().phase != "sealed.execute.v1"
        || task.checkpoint().data.get("identity") != Some(&serde_json::to_value(identity)?)
    {
        return Err(Error::Invalid);
    }
    let definition = store.get(&identity_key(task.id()))?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != *identity {
        return Err(Error::Invalid);
    }
    let Some(intent_entry) = store.get(&intent_key(task.id()))? else {
        return Ok(None);
    };
    let intent: Intent = serde_json::from_slice(&intent_entry.value)?;
    if intent.task != task.id()
        || intent.identity != *identity
        || !intent.attempt_record
        || (require_running_attempt && !intent.matches_attempt(task))
        || (!require_running_attempt && task.interrupted_dispatch_attempt() != Some(intent.attempt))
    {
        return Err(Error::Invalid);
    }
    let Some(attempt_check) = attempt_record_check(store, &intent)? else {
        return Err(Error::Invalid);
    };
    let raw = raw_result_key(task.id());
    let final_key = receipt_key(task.id());
    if store.get(&raw)?.is_some() || store.get(&final_key)?.is_some() {
        return Ok(None);
    }
    Ok(Some(vec![
        Check {
            key: identity_key(task.id()),
            expected: Expected::Version(definition.version),
        },
        Check {
            key: intent_key(task.id()),
            expected: Expected::Version(intent_entry.version),
        },
        attempt_check,
        Check {
            key: raw,
            expected: Expected::Missing,
        },
        Check {
            key: final_key,
            expected: Expected::Missing,
        },
    ]))
}

/// Prove that this checkpoint never published a dispatch intent or outcome.
pub(crate) fn undispatched_interrupted_evidence(
    store: &Store,
    task: &Task,
    identity: &ToolIdentity,
) -> Result<Option<Vec<Check>>, Error> {
    if !identity.has_interrupted_after()
        || task.replay_safe()
        || task.checkpoint().phase != "sealed.execute.v1"
        || task.checkpoint().data.get("identity") != Some(&serde_json::to_value(identity)?)
    {
        return Err(Error::Invalid);
    }
    let definition = store.get(&identity_key(task.id()))?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != *identity {
        return Err(Error::Invalid);
    }
    let keys = [
        intent_key(task.id()),
        raw_result_key(task.id()),
        receipt_key(task.id()),
    ];
    for key in &keys {
        if store.get(key)?.is_some() {
            return Ok(None);
        }
    }
    let mut checks = vec![Check {
        key: identity_key(task.id()),
        expected: Expected::Version(definition.version),
    }];
    checks.extend(keys.into_iter().map(|key| Check {
        key,
        expected: Expected::Missing,
    }));
    Ok(Some(checks))
}

/// A real raw or final receipt, tied to the original interrupted dispatch.
/// The synthetic Unknown returned by `prepare_receipt_read` is not a receipt.
pub(crate) fn known_interrupted_evidence(
    store: &Store,
    task: &Task,
    identity: &ToolIdentity,
) -> Result<Option<Vec<Check>>, Error> {
    let entry = store.get(&intent_key(task.id()))?.ok_or(Error::Invalid)?;
    let intent: Intent = serde_json::from_slice(&entry.value)?;
    if !identity.has_interrupted_after()
        || task.replay_safe()
        || task.checkpoint().phase != "sealed.execute.v1"
        || task.checkpoint().data.get("identity") != Some(&serde_json::to_value(identity)?)
        || intent.task != task.id()
        || !intent.attempt_record
        || intent.identity != *identity
        || task.interrupted_dispatch_attempt() != Some(intent.attempt)
    {
        return Err(Error::Invalid);
    }
    let (_, checks) = prepare_receipt_read(store, task.id())?;
    if !checks.iter().any(|check| {
        check.key == intent_key(task.id()) && check.expected == Expected::Version(entry.version)
    }) {
        return Err(Error::Invalid);
    }
    if !checks.iter().any(|check| {
        (check.key == raw_result_key(task.id()) || check.key == receipt_key(task.id()))
            && matches!(check.expected, Expected::Version(_))
    }) {
        return Ok(None);
    }
    Ok(Some(checks))
}

pub(crate) fn prepare_after_recovery(
    store: &Store,
    running: &StoredTask,
) -> Result<OwnedBatch, Error> {
    if running.task.checkpoint().phase != "sealed.execute.v1" {
        return Err(Error::Invalid);
    }
    let (intent, _) = read_raw_result(store, running.task.id())?.ok_or(Error::Invalid)?;
    let (checked, batch) = prepare_after_phase(store, running, &intent.identity)?;
    if intent != checked {
        return Err(Error::Invalid);
    }
    Ok(batch)
}

pub(crate) fn read_raw_result(
    store: &Store,
    task: Uuid,
) -> Result<Option<(Intent, Outcome)>, Error> {
    let Some(raw) = store.get(&raw_result_key(task))? else {
        return Ok(None);
    };
    let (intent, outcome): (Intent, Outcome) = serde_json::from_slice(&raw.value)?;
    if intent.task != task
        || !intent.identity.has_after_hook()
        || load_intent(store, task)? != intent
    {
        return Err(Error::Invalid);
    }
    let definition = store.get(&identity_key(task))?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != intent.identity {
        return Err(Error::Invalid);
    }
    Ok(Some((intent, outcome)))
}

#[derive(Debug, PartialEq)]
pub(crate) enum ReceiptRead {
    Final(Outcome),
    AfterIncomplete(Outcome),
}

/// Read a receipt with checks suitable for a parent task transition. Every
/// record read here is fenced by the parent's eventual atomic commit.
pub(crate) fn prepare_receipt_read(
    store: &Store,
    task: Uuid,
) -> Result<(Option<ReceiptRead>, Vec<Check>), Error> {
    let definition_key = identity_key(task);
    let definition = store.get(&definition_key)?.ok_or(Error::Invalid)?;
    let identity: ToolIdentity = serde_json::from_slice(&definition.value)?;
    if !identity.is_valid() {
        return Err(Error::Invalid);
    }
    let mut checks = vec![Check {
        key: definition_key,
        expected: Expected::Version(definition.version),
    }];
    let intent_key = intent_key(task);
    let intent_entry = store.get(&intent_key)?;
    checks.push(Check {
        key: intent_key,
        expected: intent_entry
            .as_ref()
            .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
    });
    let receipt_key = receipt_key(task);
    let receipt_entry = store.get(&receipt_key)?;
    checks.push(Check {
        key: receipt_key,
        expected: receipt_entry
            .as_ref()
            .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
    });
    let raw_key = raw_result_key(task);
    let raw_entry = store.get(&raw_key)?;
    checks.push(Check {
        key: raw_key,
        expected: raw_entry
            .as_ref()
            .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
    });
    match (intent_entry, receipt_entry) {
        (None, None) if raw_entry.is_none() => Ok((None, checks)),
        (None, None) => Err(Error::Invalid),
        (None, Some(_)) => Err(Error::Invalid),
        (Some(intent), receipt) => {
            let intent: Intent = serde_json::from_slice(&intent.value)?;
            if intent.task != task || intent.identity != identity {
                return Err(Error::Invalid);
            }
            checks.extend(attempt_record_check(store, &intent)?);
            let Some(receipt) = receipt else {
                if let Some(raw) = raw_entry {
                    let (raw_intent, outcome): (Intent, Outcome) =
                        serde_json::from_slice(&raw.value)?;
                    if raw_intent != intent || !identity.has_after_hook() {
                        return Err(Error::Invalid);
                    }
                    return Ok((Some(ReceiptRead::AfterIncomplete(outcome)), checks));
                }
                return Ok((
                    Some(ReceiptRead::Final(Outcome::Unknown(
                        "dispatch intent has no durable receipt; reconcile external effects".into(),
                    ))),
                    checks,
                ));
            };
            let (stored, outcome): (Intent, Outcome) = serde_json::from_slice(&receipt.value)?;
            if stored != intent {
                return Err(Error::Invalid);
            }
            Ok((Some(ReceiptRead::Final(outcome)), checks))
        }
    }
}

pub(crate) fn read_receipt(store: &Store, intent: &Intent) -> Result<Option<Outcome>, Error> {
    let stored = store.get(&intent_key(intent.task))?.ok_or(Error::Invalid)?;
    if serde_json::from_slice::<Intent>(&stored.value)? != *intent {
        return Err(Error::Invalid);
    }
    attempt_record_check(store, intent)?;
    let definition = store
        .get(&identity_key(intent.task))?
        .ok_or(Error::Invalid)?;
    if serde_json::from_slice::<ToolIdentity>(&definition.value)? != intent.identity {
        return Err(Error::Invalid);
    }
    let Some(entry) = store.get(&receipt_key(intent.task))? else {
        return Ok(None);
    };
    let (stored, outcome): (Intent, Outcome) = serde_json::from_slice(&entry.value)?;
    if &stored != intent {
        return Err(Error::Invalid);
    }
    Ok(Some(outcome))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Checkpoint;
    use crate::task_store::Save;
    use pi_rs_core::{Tool, ToolContext, ToolSpec};
    use serde_json::json;

    struct Stub;
    #[async_trait::async_trait]
    impl Tool for Stub {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    fn fixture(store: &Store) -> (Uuid, Uuid, ToolIdentity) {
        let tasks = TaskStore::new(store);
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "parent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.start_attempt(Uuid::new_v4()))
            .unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("leaf".into(), false, 1, json!({}))
        .unwrap()
        .into_task_tool();
        let identity = ToolIdentity::new(&Stub, "implementation-tenant-v1", 1).unwrap();
        let (id, batch) = prepare_child(store, &snapshot, "leaf", &child, &identity).unwrap();
        assert_eq!(id, child.id());
        store.commit(&batch.checks, &batch.mutations).unwrap();
        let snapshot = tasks.load(id).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.start_attempt(Uuid::new_v4()))
            .unwrap();
        (parent.id(), id, identity)
    }
    fn commit(store: &Store, batch: OwnedBatch) {
        store.commit(&batch.checks, &batch.mutations).unwrap();
    }

    #[test]
    fn parent_abort_between_prepare_and_intent_commit_refuses_the_intent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (parent, child, identity) = fixture(&store);
        let tasks = TaskStore::new(&store);
        let snapshot = tasks.load(child).unwrap().unwrap();
        let other = ToolIdentity::new(&Stub, "different-backend", 1).unwrap();
        assert!(prepare_intent(&store, &snapshot, &other).is_err());
        let (_, pending) = prepare_intent(&store, &snapshot, &identity).unwrap();
        let parent_snapshot = tasks.load(parent).unwrap().unwrap();
        tasks.update(&parent_snapshot, Task::request_abort).unwrap();
        assert!(matches!(
            store.commit(&pending.checks, &pending.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(store.get(&intent_key(child)).unwrap().is_none());
        assert!(
            store
                .get(&attempt_intent_key(
                    child,
                    snapshot.task.attempt_id().unwrap()
                ))
                .unwrap()
                .is_none()
        );
        assert!(prepare_intent(&store, &snapshot, &identity).is_err());
    }

    #[test]
    fn dispatch_records_attempt_atomically_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let (_, child, identity) = fixture(&store);
        let (intent, batch) = prepare_intent(
            &store,
            &TaskStore::new(&store).load(child).unwrap().unwrap(),
            &identity,
        )
        .unwrap();
        let attempt_key = attempt_intent_key(child, intent.attempt);
        assert!(store.get(&attempt_key).unwrap().is_none());
        commit(&store, batch);
        drop(store);
        let store = Store::open(&path).unwrap();
        let attempt: Intent =
            serde_json::from_slice(&store.get(&attempt_key).unwrap().unwrap().value).unwrap();
        assert_eq!(attempt, load_intent(&store, child).unwrap());
        assert_eq!(attempt, intent);
        assert!(
            store
                .get(&attempt_intent_key(child, Uuid::new_v4()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn missing_attempt_record_fails_closed_for_dispatch_and_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (_, child, identity) = fixture(&store);
        let (intent, batch) = prepare_intent(
            &store,
            &TaskStore::new(&store).load(child).unwrap().unwrap(),
            &identity,
        )
        .unwrap();
        commit(&store, batch);
        let receipt = prepare_receipt(&store, &intent, &Outcome::Unknown("test".into())).unwrap();
        store
            .commit(
                &[],
                &[Mutation::Delete {
                    key: attempt_intent_key(child, intent.attempt),
                }],
            )
            .unwrap();
        assert!(load_intent(&store, child).is_err());
        assert!(prepare_arguments_record(&store, &intent, &json!({})).is_err());
        assert!(prepare_receipt(&store, &intent, &Outcome::Unknown("test".into())).is_err());
        assert!(prepare_receipt_read(&store, child).is_err());
        assert!(store.commit(&receipt.checks, &receipt.mutations).is_err());
    }

    #[test]
    fn after_phase_fences_attempt_record_during_authorization_and_memo_commit() {
        for replace in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path().join("db")).unwrap();
            let (_, child, identity) = fixture(&store);
            let identity = identity
                .with_before_hook("hook-v1".into())
                .unwrap()
                .with_after_hook();
            store
                .commit(
                    &[],
                    &[Mutation::Put {
                        key: identity_key(child),
                        value: serde_json::to_vec(&identity).unwrap(),
                    }],
                )
                .unwrap();
            let running = TaskStore::new(&store).load(child).unwrap().unwrap();
            let (intent, batch) = prepare_intent(&store, &running, &identity).unwrap();
            commit(&store, batch);
            commit(
                &store,
                prepare_raw_result(&store, &intent, &Outcome::Succeeded("raw".into())).unwrap(),
            );
            let (_, mut phase) = prepare_after_phase(&store, &running, &identity).unwrap();
            // Model a fenced after-hook memo write in the same transaction.
            let memo_key = b"test/after-memo".to_vec();
            phase.checks.push(Check {
                key: memo_key.clone(),
                expected: Expected::Missing,
            });
            phase.mutations.push(Mutation::Put {
                key: memo_key.clone(),
                value: b"value".to_vec(),
            });
            let key = attempt_intent_key(child, intent.attempt);
            let mutation = if replace {
                let mut other = intent.clone();
                other.attempt = Uuid::new_v4();
                Mutation::Put {
                    key,
                    value: serde_json::to_vec(&other).unwrap(),
                }
            } else {
                Mutation::Delete { key }
            };
            store.commit(&[], &[mutation]).unwrap();
            assert!(matches!(
                store.commit(&phase.checks, &phase.mutations),
                Err(durable_store::Error::Conflict { .. })
            ));
            assert!(store.get(&memo_key).unwrap().is_none());
            assert!(prepare_after_phase(&store, &running, &identity).is_err());
        }
    }

    #[test]
    fn legacy_after_phase_allows_intent_without_attempt_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (_, child, identity) = fixture(&store);
        let identity = identity
            .with_before_hook("hook-v1".into())
            .unwrap()
            .with_after_hook();
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: identity_key(child),
                    value: serde_json::to_vec(&identity).unwrap(),
                }],
            )
            .unwrap();
        let running = TaskStore::new(&store).load(child).unwrap().unwrap();
        let (mut intent, _) = prepare_intent(&store, &running, &identity).unwrap();
        intent.attempt_record = false;
        store
            .commit(
                &[],
                &[
                    Mutation::Put {
                        key: intent_key(child),
                        value: serde_json::to_vec(&intent).unwrap(),
                    },
                    Mutation::Put {
                        key: raw_result_key(child),
                        value: serde_json::to_vec(&(
                            intent.clone(),
                            Outcome::Succeeded("legacy".into()),
                        ))
                        .unwrap(),
                    },
                ],
            )
            .unwrap();
        let (loaded, batch) = prepare_after_phase(&store, &running, &identity).unwrap();
        assert_eq!(loaded, intent);
        commit(&store, batch);
    }

    #[test]
    fn legacy_task_intent_without_attempt_record_remains_readable() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (_, child, identity) = fixture(&store);
        let (mut intent, _) = prepare_intent(
            &store,
            &TaskStore::new(&store).load(child).unwrap().unwrap(),
            &identity,
        )
        .unwrap();
        intent.attempt_record = false;
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: intent_key(child),
                    value: serde_json::to_vec(&intent).unwrap(),
                }],
            )
            .unwrap();
        assert_eq!(load_intent(&store, child).unwrap(), intent);
        commit(
            &store,
            prepare_receipt(&store, &intent, &Outcome::Unknown("legacy".into())).unwrap(),
        );
        assert_eq!(
            read_receipt(&store, &intent).unwrap(),
            Some(Outcome::Unknown("legacy".into()))
        );
    }

    #[test]
    fn receipt_survives_abort_and_reopen_and_rejects_conflicting_rewrites() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let store = Store::open(&path).unwrap();
        let (parent, child, identity) = fixture(&store);
        let tasks = TaskStore::new(&store);
        let (intent, batch) =
            prepare_intent(&store, &tasks.load(child).unwrap().unwrap(), &identity).unwrap();
        commit(&store, batch);
        let snapshot = tasks.load(parent).unwrap().unwrap();
        tasks.update(&snapshot, Task::request_abort).unwrap();
        let child_snapshot = tasks.load(child).unwrap().unwrap();
        tasks.update(&child_snapshot, Task::request_abort).unwrap();
        let pending = Outcome::Pending {
            reference: "remote-42".into(),
        };
        commit(&store, prepare_receipt(&store, &intent, &pending).unwrap());
        assert!(prepare_receipt(&store, &intent, &Outcome::Unknown("other".into())).is_err());
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(
            read_receipt(&store, &intent).unwrap(),
            Some(pending.clone())
        );
        commit(&store, prepare_receipt(&store, &intent, &pending).unwrap());
        assert!(
            prepare_intent(
                &store,
                &TaskStore::new(&store).load(child).unwrap().unwrap(),
                &identity
            )
            .is_err()
        );
    }

    #[test]
    fn child_and_identity_are_one_checked_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let tasks = TaskStore::new(&store);
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "parent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        tasks.save(&[Save::Create(&parent)]).unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        tasks
            .update(&snapshot, |task| task.start_attempt(Uuid::new_v4()))
            .unwrap();
        let snapshot = tasks.load(parent.id()).unwrap().unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("leaf".into(), false, 1, json!({}))
        .unwrap();
        let identity = ToolIdentity::new(&Stub, "backend", 1).unwrap();
        let (_, batch) = prepare_child(&store, &snapshot, "leaf", &child, &identity).unwrap();
        // A competing writer occupying the identity key must prevent child creation.
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: identity_key(child.id()),
                    value: b"occupied".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            store.commit(&batch.checks, &batch.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(tasks.load(child.id()).unwrap().is_none());
    }

    #[test]
    fn killed_after_receipt_commit_retains_uncertain_reference() {
        const ENV: &str = "PI_RS_ASYNC_RECEIPT_KILL_TEST";
        if let Ok(directory) = std::env::var(ENV) {
            let dir = std::path::Path::new(&directory);
            let store = Store::open(dir.join("db")).unwrap();
            let (_, child, identity) = fixture(&store);
            let (intent, batch) = prepare_intent(
                &store,
                &TaskStore::new(&store).load(child).unwrap().unwrap(),
                &identity,
            )
            .unwrap();
            commit(&store, batch);
            if std::env::var_os("PI_RS_ASYNC_KILL_BEFORE_RECEIPT").is_none() {
                commit(
                    &store,
                    prepare_receipt(
                        &store,
                        &intent,
                        &Outcome::Pending {
                            reference: "remote-kill".into(),
                        },
                    )
                    .unwrap(),
                );
            }
            std::fs::write(dir.join("ready.tmp"), child.to_string()).unwrap();
            std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
            loop {
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_protocol::tests::killed_after_receipt_commit_retains_uncertain_reference")
            .env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("receipt worker did not become ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let intent: Intent =
            serde_json::from_slice(&store.get(&intent_key(id)).unwrap().unwrap().value).unwrap();
        assert_eq!(
            read_receipt(&store, &intent).unwrap(),
            Some(Outcome::Pending {
                reference: "remote-kill".into()
            })
        );
        assert_eq!(
            TaskStore::new(&store)
                .load(id)
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Running
        );
    }
    #[test]
    fn competing_receipt_commits_never_overwrite_a_reference() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let (_, child, identity) = fixture(&store);
        let (intent, batch) = prepare_intent(
            &store,
            &TaskStore::new(&store).load(child).unwrap().unwrap(),
            &identity,
        )
        .unwrap();
        commit(&store, batch);
        let first = Outcome::Pending {
            reference: "first".into(),
        };
        let second = Outcome::Pending {
            reference: "second".into(),
        };
        let one = prepare_receipt(&store, &intent, &first).unwrap();
        let two = prepare_receipt(&store, &intent, &second).unwrap();
        commit(&store, one);
        assert!(matches!(
            store.commit(&two.checks, &two.mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert_eq!(read_receipt(&store, &intent).unwrap(), Some(first));
    }
    #[test]
    fn killed_after_intent_before_receipt_leaves_unknown_without_retry() {
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_protocol::tests::killed_after_receipt_commit_retains_uncertain_reference")
            .env("PI_RS_ASYNC_RECEIPT_KILL_TEST", dir.path())
            .env("PI_RS_ASYNC_KILL_BEFORE_RECEIPT", "1").spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("intent worker did not become ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let id: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let intent: Intent =
            serde_json::from_slice(&store.get(&intent_key(id)).unwrap().unwrap().value).unwrap();
        assert_eq!(read_receipt(&store, &intent).unwrap(), None);
        assert!(matches!(
            prepare_receipt_read(&store, id).unwrap().0,
            Some(ReceiptRead::Final(Outcome::Unknown(_)))
        ));
        let task = TaskStore::new(&store).load(id).unwrap().unwrap();
        assert!(prepare_intent(&store, &task, &intent.identity).is_err());
    }
    struct RealTool {
        entered: std::sync::mpsc::Sender<()>,
        release: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    }
    #[async_trait::async_trait]
    impl Tool for RealTool {
        fn spec(&self) -> ToolSpec {
            Stub.spec()
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            self.entered.send(()).unwrap();
            let (lock, wake) = &*self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            Outcome::Pending {
                reference: "real-tool-reference".into(),
            }
        }
    }
    struct RealToolHandler(std::sync::Arc<dyn Tool>);
    impl RealToolHandler {
        fn invoke(
            &self,
            task: &Task,
            cancellation: pi_rs_core::CancellationToken,
        ) -> Result<crate::task_scheduler::Step, String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .map_err(|e| e.to_string())?;
            let result = runtime.block_on(self.0.invoke(
                json!({}),
                ToolContext {
                    operation_id: task.id(),
                    cancellation,
                },
            ));
            Ok(crate::task_scheduler::Step::Succeed(
                serde_json::to_value(result).map_err(|e| e.to_string())?,
            ))
        }
    }
    impl crate::task_scheduler::Handler for RealToolHandler {
        fn run(&self, task: &Task) -> Result<crate::task_scheduler::Step, String> {
            self.invoke(task, pi_rs_core::CancellationToken::new())
        }
        fn run_with_cancellation(
            &self,
            task: &Task,
            _: &mut crate::document::TaskDocuments<'_>,
            cancellation: &pi_rs_core::CancellationToken,
        ) -> Result<crate::task_scheduler::Step, String> {
            self.invoke(task, cancellation.clone())
        }
    }
    fn live_fixture(
        path: &std::path::Path,
        tool: std::sync::Arc<RealTool>,
    ) -> (
        crate::task_scheduler::Scheduler,
        Box<crate::task_scheduler::DetachedInvocation>,
        Box<crate::task_scheduler::DetachedInvocation>,
        Uuid,
        ToolIdentity,
    ) {
        use crate::task_scheduler::{Registry, Scheduler, Step, TickReservation};
        let mut registry = Registry::default();
        assert!(registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null)))));
        assert!(registry.register("leaf", false, RealToolHandler(tool.clone())));
        let mut scheduler = Scheduler::open(path, registry).unwrap();
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "parent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let TickReservation::Invocation(parent_job) = scheduler.reserve_tick(0).unwrap() else {
            panic!("parent reservation");
        };
        let owner = scheduler.owner();
        let tasks = TaskStore::new(owner.store());
        let parent_snapshot = tasks.load(parent.id()).unwrap().unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("leaf".into(), false, 1, json!({}))
        .unwrap()
        .into_task_tool();
        let identity = ToolIdentity::new(tool.as_ref(), "real-backend", 1).unwrap();
        let (_, batch) =
            prepare_child(owner.store(), &parent_snapshot, "child", &child, &identity).unwrap();
        commit(owner.store(), batch);
        let TickReservation::Invocation(child_job) = scheduler.reserve_tick(0).unwrap() else {
            panic!("child reservation");
        };
        (scheduler, parent_job, child_job, parent.id(), identity)
    }
    #[test]
    fn driver_abort_before_intent_refuses_reserved_child() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(RealTool {
            entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new())),
        });
        let (scheduler, parent_job, child_job, parent, identity) =
            live_fixture(&dir.path().join("db"), tool.clone());
        let driver = crate::task_driver::Driver::start(scheduler);
        assert!(
            child_job
                .commit_unhooked_intent(&identity, tool.as_ref(), "wrong-backend", 1)
                .is_err()
        );
        driver.request_abort(parent).unwrap();
        assert!(
            child_job
                .commit_unhooked_intent(&identity, tool.as_ref(), "real-backend", 1)
                .is_err()
        );
        assert!(received.try_recv().is_err());
        let _ = driver.shutdown_with_pending();
        drop(child_job);
        drop(parent_job);
    }
    #[test]
    fn driver_defers_child_while_parent_result_is_unsettled() {
        use std::sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        use std::time::Duration;

        let dir = tempfile::tempdir().unwrap();
        let (entered, calls) = std::sync::mpsc::channel();
        let tool = Arc::new(RealTool {
            entered,
            release: Arc::new((Mutex::new(false), Condvar::new())),
        });
        let (arrived, arrival) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let gate = release.clone();
        let count = Arc::new(AtomicUsize::new(0));
        let hook_count = count.clone();
        let hook = Arc::new(move || {
            let n = hook_count.fetch_add(1, Ordering::SeqCst);
            let _ = arrived.send(n);
            if n == 0 {
                let (lock, wake) = &*gate;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    ready = wake.wait(ready).unwrap();
                }
            }
        });
        let (driver, parent, owner, parent_id, child) =
            actual_driver_fixture_with_hook(&dir.path().join("db"), tool, Some(hook), None);
        assert_eq!(arrival.recv_timeout(Duration::from_secs(5)).unwrap(), 0);
        let completed = parent.execute().unwrap();
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if driver
                .get(child)
                .unwrap()
                .is_some_and(|t| t.state() == &TaskState::Ready)
            {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "child did not yield");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(driver.get(parent_id).is_ok());
        assert!(load_intent(owner.store(), child).is_err());
        assert!(calls.try_recv().is_err());
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(completed);
    }
    #[test]
    fn joined_real_tool_result_can_be_receipted_after_driver_abort() {
        actual_driver_worker_captures_pending_after_abort();
    }
    #[test]
    fn killed_after_joined_real_tool_receipt_preserves_reference() {
        killed_after_actual_driver_worker_receipt();
    }
    /// Abort after the driver commits intent but before the first Tool poll.
    #[test]
    fn provisional_intent_can_precede_abort_and_physical_tool_poll() {
        actual_driver_abort_after_intent_cancels_inflight_executor_and_receipts();
    }
    fn actual_driver_fixture(
        path: &std::path::Path,
        tool: std::sync::Arc<dyn Tool>,
    ) -> (
        crate::task_driver::Driver,
        Box<crate::task_scheduler::DetachedInvocation>,
        std::sync::Arc<durable_store::owner::OwnedStore>,
        Uuid,
        Uuid,
    ) {
        actual_driver_fixture_with_hook(path, tool, None, None)
    }
    fn actual_driver_fixture_with_hook(
        path: &std::path::Path,
        tool: std::sync::Arc<dyn Tool>,
        before_intent: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        after_intent: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    ) -> (
        crate::task_driver::Driver,
        Box<crate::task_scheduler::DetachedInvocation>,
        std::sync::Arc<durable_store::owner::OwnedStore>,
        Uuid,
        Uuid,
    ) {
        actual_driver_fixture_with_setup(path, tool, before_intent, after_intent, None, None)
    }
    fn actual_driver_fixture_with_setup(
        path: &std::path::Path,
        tool: std::sync::Arc<dyn Tool>,
        before_intent: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        after_intent: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        setup: Option<Box<dyn FnOnce(Uuid)>>,
        busy_hook: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    ) -> (
        crate::task_driver::Driver,
        Box<crate::task_scheduler::DetachedInvocation>,
        std::sync::Arc<durable_store::owner::OwnedStore>,
        Uuid,
        Uuid,
    ) {
        actual_driver_fixture_with_parent(
            path,
            tool,
            before_intent,
            after_intent,
            setup,
            busy_hook,
            std::sync::Arc::new(|_: &Task| Ok(crate::task_scheduler::Step::Succeed(json!(null)))),
        )
    }
    fn actual_driver_fixture_with_parent(
        path: &std::path::Path,
        tool: std::sync::Arc<dyn Tool>,
        before_intent: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        after_intent: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        setup: Option<Box<dyn FnOnce(Uuid)>>,
        busy_hook: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
        parent_handler: std::sync::Arc<dyn crate::task_scheduler::Handler>,
    ) -> (
        crate::task_driver::Driver,
        Box<crate::task_scheduler::DetachedInvocation>,
        std::sync::Arc<durable_store::owner::OwnedStore>,
        Uuid,
        Uuid,
    ) {
        use crate::task_scheduler::{Registry, TickReservation};
        let identity = ToolIdentity::new(tool.as_ref(), "real-backend", 1).unwrap();
        if crate::nested_identity::current(&tool).is_none() {
            crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        }
        let mut registry = Registry::default();
        assert!(registry.register_shared("parent", false, parent_handler));
        let mut host = crate::async_task_tool::StandaloneNoHooksHost::open(
            path,
            registry,
            crate::async_task_tool::UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        host.register_tool("leaf", tool, "real-backend", 1).unwrap();
        let mut scheduler = host.into_scheduler_for_test();
        assert!(scheduler.set_sealed_test_hooks("leaf", before_intent, after_intent));
        if let Some(hook) = busy_hook {
            assert!(scheduler.set_sealed_busy_test_hook("leaf", hook));
        }
        let owner = scheduler.owner();
        let parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "parent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false)
        .into_task_tool();
        scheduler.submit(&parent).unwrap();
        let TickReservation::Invocation(parent_job) = scheduler.reserve_tick(0).unwrap() else {
            panic!("parent reservation");
        };
        let parent_snapshot = TaskStore::new(owner.store())
            .load(parent.id())
            .unwrap()
            .unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(parent.id()),
            Checkpoint {
                phase: "initial".into(),
                data: json!({}),
            },
            3,
        )
        .unwrap()
        .registered_versioned("leaf".into(), false, 1, json!({}))
        .unwrap()
        .into_task_tool();
        let (_, batch) =
            prepare_child(owner.store(), &parent_snapshot, "child", &child, &identity).unwrap();
        commit(owner.store(), batch);
        if let Some(setup) = setup {
            setup(child.id());
        }
        (
            crate::task_driver::Driver::start(scheduler),
            parent_job,
            owner,
            parent.id(),
            child.id(),
        )
    }
    fn await_receipt(store: &Store, child: Uuid) -> (Intent, Outcome) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Ok(intent) = load_intent(store, child)
                && let Some(outcome) = read_receipt(store, &intent).unwrap()
            {
                return (intent, outcome);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker did not persist receipt"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    fn exercise_published_parent_deferral(abort: bool) {
        use std::sync::{Arc, Mutex, mpsc};
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(RealTool {
            entered,
            release: Arc::new((Mutex::new(true), std::sync::Condvar::new())),
        });
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let setup = Box::new(move |child| {
            crate::task_driver::before_execute_for_test(child, move |_| {
                reserved_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_setup(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            None,
        );
        reserved_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // The parent publishes its result while the child already owns a
        // reservation but has not committed its dispatch intent.
        let parent_result = parent_job.execute().unwrap();
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let task = driver
                .get(child)
                .expect("driver must remain healthy")
                .unwrap();
            if task.state() == &TaskState::Ready {
                break;
            }
            assert!(Instant::now() < deadline, "reserved child was not deferred");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        if abort {
            driver.request_abort(parent).unwrap();
        }
        assert!(driver.settle_for_test(parent_result).unwrap());
        while !driver.get(parent).unwrap().unwrap().is_terminal() {
            assert!(
                Instant::now() < deadline,
                "parent did not settle after child deferral"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        if abort {
            assert!(driver.get(parent).unwrap().unwrap().abort_requested());
        }
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn published_parent_defers_reserved_sealed_child_without_dispatch() {
        exercise_published_parent_deferral(false);
    }

    #[test]
    fn published_parent_abort_preempts_deferred_sealed_child() {
        exercise_published_parent_deferral(true);
    }

    fn exercise_deferred_sealed_child_busy(abort: bool) {
        use crate::task::WaitFor;
        use crate::task_scheduler::Step;
        use std::sync::{Arc, Mutex, OnceLock, mpsc};
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (entered, observed) = mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(RealTool {
            entered,
            release: Arc::new((Mutex::new(true), std::sync::Condvar::new())),
        });
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (writer_tx, writer_rx) = mpsc::channel();
        let (unlock_tx, unlock_rx) = mpsc::channel();
        let writer_path = path.clone();
        let child_id = Arc::new(OnceLock::new());
        let handler_child = child_id.clone();
        let parent_handler: Arc<dyn crate::task_scheduler::Handler> =
            Arc::new(move |task: &Task| {
                if task.checkpoint().phase == "parent" {
                    Ok(Step::Wait {
                        checkpoint: Checkpoint {
                            phase: "waiting".into(),
                            data: json!(null),
                        },
                        condition: WaitFor::Tasks(vec![*handler_child.get().unwrap()]),
                        children: vec![],
                    })
                } else {
                    Ok(Step::Succeed(json!("parent finished")))
                }
            });
        let setup = Box::new(move |child| {
            child_id.set(child).unwrap();
            crate::task_driver::before_execute_for_test(child, move |_| {
                reserved_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            crate::task_scheduler::before_undispatched_release_for_test(child, move || {
                let (locked_tx, locked_rx) = mpsc::channel();
                let writer = std::thread::spawn(move || {
                    let conn = rusqlite::Connection::open(writer_path).unwrap();
                    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                    locked_tx.send(()).unwrap();
                    unlock_rx.recv_timeout(Duration::from_secs(20)).unwrap();
                    conn.execute_batch("COMMIT").unwrap();
                });
                locked_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                writer_tx.send(writer).unwrap();
            });
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_parent(
            &path,
            tool,
            None,
            None,
            Some(setup),
            None,
            parent_handler,
        );
        reserved_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let parent_result = parent_job.execute().unwrap();
        release_tx.send(()).unwrap();
        let writer = writer_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !driver.has_pending_task(child) {
            assert!(driver.get(child).is_ok(), "busy release stopped driver");
            assert!(
                Instant::now() < deadline,
                "busy release did not retain its lease"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        unlock_tx.send(()).unwrap();
        writer.join().unwrap();
        if abort {
            driver.request_abort(parent).unwrap();
        }
        assert!(driver.settle_for_test(parent_result).unwrap());
        if !abort {
            observed
                .recv_timeout(Duration::from_secs(5))
                .expect("child did not retry after park");
        }
        while !driver.get(parent).unwrap().unwrap().is_terminal() {
            assert!(Instant::now() < deadline, "parent did not finish abort");
            std::thread::sleep(Duration::from_millis(5));
        }
        if abort {
            assert!(driver.get(parent).unwrap().unwrap().abort_requested());
            assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        } else {
            assert_eq!(
                driver.get(parent).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("parent finished"))
            );
            assert!(owner.store().get(&intent_key(child)).unwrap().is_some());
        }
        assert!(observed.try_recv().is_err());
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn deferred_sealed_child_retains_lease_when_release_writer_is_busy() {
        exercise_deferred_sealed_child_busy(false);
    }

    #[test]
    fn abort_preempts_deferred_sealed_child_after_release_writer_busy() {
        exercise_deferred_sealed_child_busy(true);
    }

    #[test]
    fn published_parent_defers_child_during_uncertain_abort_graph() {
        use std::sync::{Arc, Mutex, mpsc};
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(RealTool {
            entered,
            release: Arc::new((Mutex::new(true), std::sync::Condvar::new())),
        });
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let setup = Box::new(move |child| {
            crate::task_driver::before_execute_for_test(child, move |_| {
                reserved_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_setup(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            None,
        );
        reserved_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let parent_result = parent_job.execute().unwrap();
        let edge = crate::ownership_edge::edge_key(Uuid::new_v4());
        {
            let _gate = owner.task_gate().lock().unwrap();
            let tasks = TaskStore::new(owner.store());
            let root = Task::new(
                Uuid::new_v4(),
                TaskOwner::Conversation(Uuid::new_v4()),
                Checkpoint {
                    phase: "initial".into(),
                    data: json!(null),
                },
                3,
            )
            .unwrap()
            .registered("parent".into(), false);
            tasks.save(&[Save::Create(&root)]).unwrap();
            tasks
                .update(&tasks.load(root.id()).unwrap().unwrap(), |task| {
                    task.start()?;
                    task.hold_failure("pending abort".into())
                })
                .unwrap();
            owner
                .store()
                .commit(
                    &[Check {
                        key: edge.clone(),
                        expected: Expected::Missing,
                    }],
                    &[Mutation::Put {
                        key: edge.clone(),
                        value: b"invalid edge".to_vec(),
                    }],
                )
                .unwrap();
        }
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let task = driver
                .get(child)
                .expect("uncertain graph must not poison driver")
                .unwrap();
            if task.state() == &TaskState::Ready {
                break;
            }
            assert!(Instant::now() < deadline, "child was not deferred");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        let entry = owner.store().get(&edge).unwrap().unwrap();
        owner
            .store()
            .commit(
                &[Check {
                    key: edge.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Delete { key: edge }],
            )
            .unwrap();
        assert!(driver.settle_for_test(parent_result).unwrap());
        while !driver.get(parent).unwrap().unwrap().is_terminal() {
            assert!(
                Instant::now() < deadline,
                "parent did not settle after repair"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn published_parent_park_then_retries_the_same_sealed_child() {
        use crate::task::WaitFor;
        use crate::task_scheduler::Step;
        use std::sync::{Arc, Mutex, OnceLock, mpsc};
        use std::time::{Duration, Instant};
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = mpsc::channel();
        let tool: Arc<dyn Tool> = Arc::new(RealTool {
            entered,
            release: Arc::new((Mutex::new(true), std::sync::Condvar::new())),
        });
        let child_id = Arc::new(OnceLock::new());
        let handler_child = child_id.clone();
        let parent_handler: Arc<dyn crate::task_scheduler::Handler> =
            Arc::new(move |task: &Task| {
                if task.checkpoint().phase == "parent" {
                    Ok(Step::Wait {
                        checkpoint: Checkpoint {
                            phase: "waiting".into(),
                            data: json!(null),
                        },
                        condition: WaitFor::Tasks(vec![*handler_child.get().unwrap()]),
                        children: vec![],
                    })
                } else {
                    Ok(Step::Succeed(json!("parent finished")))
                }
            });
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let setup = Box::new(move |child| {
            child_id.set(child).unwrap();
            crate::task_driver::before_execute_for_test(child, move |_| {
                reserved_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_parent(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            None,
            parent_handler,
        );
        reserved_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let parent_result = parent_job.execute().unwrap();
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while driver.get(child).expect("healthy driver").unwrap().state() != &TaskState::Ready {
            assert!(Instant::now() < deadline, "child was not deferred");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        assert!(driver.settle_for_test(parent_result).unwrap());
        observed
            .recv_timeout(Duration::from_secs(5))
            .expect("child did not run after park");
        while !driver.get(parent).unwrap().unwrap().is_terminal() {
            assert!(Instant::now() < deadline, "parent did not resume");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            driver.get(parent).unwrap().unwrap().state(),
            &TaskState::Succeeded(json!("parent finished"))
        );
        assert!(observed.try_recv().is_err(), "child was dispatched twice");
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }

    #[test]
    fn actual_driver_worker_captures_pending_after_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let tool = std::sync::Arc::new(RealTool {
            entered,
            release: release.clone(),
        });
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(owner.store().get(&prepared_key(child)).unwrap().is_some());
        driver.request_abort(parent).unwrap();
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let (_, outcome) = await_receipt(owner.store(), child);
        assert_eq!(
            outcome,
            Outcome::Pending {
                reference: "real-tool-reference".into()
            }
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn actual_driver_shutdown_joins_worker_before_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let tool = std::sync::Arc::new(RealTool {
            entered,
            release: release.clone(),
        });
        let (driver, parent_job, owner, _, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (done, result) = std::sync::mpsc::channel();
        let shutdown =
            std::thread::spawn(move || done.send(driver.shutdown_with_pending()).unwrap());
        let early = result.recv_timeout(std::time::Duration::from_millis(100));
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        assert!(matches!(
            early,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        let report = result
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap()
            .unwrap();
        shutdown.join().unwrap();
        assert!(report.failure().is_none());
        assert_eq!(
            await_receipt(owner.store(), child).1,
            Outcome::Pending {
                reference: "real-tool-reference".into()
            }
        );
        drop(parent_job);
    }
    #[test]
    fn killed_after_actual_driver_worker_receipt() {
        const ENV: &str = "PI_RS_DRIVER_RECEIPT_KILL";
        if let Ok(directory) = std::env::var(ENV) {
            let dir = std::path::Path::new(&directory);
            let (entered, _received) = std::sync::mpsc::channel();
            let tool = std::sync::Arc::new(RealTool {
                entered,
                release: std::sync::Arc::new((
                    std::sync::Mutex::new(true),
                    std::sync::Condvar::new(),
                )),
            });
            let (driver, parent, owner, _, child) = actual_driver_fixture(&dir.join("db"), tool);
            let (_, outcome) = await_receipt(owner.store(), child);
            assert_eq!(
                outcome,
                Outcome::Pending {
                    reference: "real-tool-reference".into()
                }
            );
            std::fs::write(dir.join("ready.tmp"), child.to_string()).unwrap();
            std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
            loop {
                std::hint::black_box((&driver, &parent, &owner));
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("async_task_protocol::tests::killed_after_actual_driver_worker_receipt")
            .env(ENV, dir.path())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("driver worker did not persist receipt");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let child: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let intent = load_intent(&store, child).unwrap();
        assert_eq!(
            read_receipt(&store, &intent).unwrap(),
            Some(Outcome::Pending {
                reference: "real-tool-reference".into()
            })
        );
    }
    #[test]
    fn failed_driver_receipt_retains_joined_result_for_reconciliation() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let tool = std::sync::Arc::new(RealTool {
            entered,
            release: release.clone(),
        });
        let (driver, parent, owner, _, child) = actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // Simulate an invalid definition before receipt capture, outside the
        // normal cooperative writer contract, and retain the joined outcome.
        owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key: identity_key(child),
                    value: b"invalid".to_vec(),
                }],
            )
            .unwrap();
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while driver.get(child).is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "driver did not report receipt failure"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_some());
        let retained = report.pending().iter().find_map(|pending| {
            pending
                .completed()
                .and_then(|completed| completed.retained_tool_outcome())
        });
        assert_eq!(
            retained,
            Some(Outcome::Pending {
                reference: "real-tool-reference".into()
            })
        );
        drop(report);
        drop(parent);
    }
    #[test]
    fn actual_driver_refuses_tool_when_abort_precedes_dispatch_intent() {
        let dir = tempfile::tempdir().unwrap();
        let (tool_entered, tool_received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(RealTool {
            entered: tool_entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
        });
        let (before_entered, before_received) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let hook_release = release.clone();
        let before = std::sync::Arc::new(move || {
            before_entered.send(()).unwrap();
            let (lock, wake) = &*hook_release;
            let mut allowed = lock.lock().unwrap();
            while !*allowed {
                allowed = wake.wait(allowed).unwrap();
            }
        });
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture_with_hook(&dir.path().join("db"), tool, Some(before), None);
        before_received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent).unwrap();
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let unrelated = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "unrelated".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        TaskStore::new(owner.store())
            .save(&[crate::task_store::Save::Create(&unrelated)])
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let task = driver.get(unrelated.id()).unwrap().unwrap();
            if task.is_terminal() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "unrelated task did not settle"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(tool_received.try_recv().is_err());
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn shutdown_before_intent_releases_reservation_without_driver_failure() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(RealTool {
            entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
        });
        let (before_tx, before_rx) = std::sync::mpsc::channel();
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let waiting = gate.clone();
        let before = std::sync::Arc::new(move || {
            before_tx.send(()).unwrap();
            let (lock, wake) = &*waiting;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = wake.wait(ready).unwrap();
            }
        });
        let (driver, parent_job, owner, _, child) =
            actual_driver_fixture_with_hook(&dir.path().join("db"), tool, Some(before), None);
        before_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.stop_worker_for_test();
        {
            let (lock, wake) = &*gate;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        assert!(observed.try_recv().is_err());
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert_eq!(
            TaskStore::new(owner.store())
                .load(child)
                .unwrap()
                .unwrap()
                .task
                .state(),
            &TaskState::Ready
        );
        drop(parent_job);
    }

    fn identity_update_during_reserved_dispatch(mode: u8) {
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let tool: std::sync::Arc<dyn Tool> = std::sync::Arc::new(RealTool {
            entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
        });
        let handle = crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let waiting = gate.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (join_tx, join_rx) = std::sync::mpsc::channel();
        let setup = Box::new(move |_: Uuid| {
            let thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                runtime
                    .block_on(
                        handle.update(
                            Some(
                                if mode == 1 {
                                    "different"
                                } else {
                                    "real-backend"
                                }
                                .into(),
                            ),
                            move || {
                                started_tx.send(()).unwrap();
                                let (lock, wake) = &*waiting;
                                let mut ready = lock.lock().unwrap();
                                while !*ready {
                                    ready = wake.wait(ready).unwrap();
                                }
                            },
                        ),
                    )
                    .unwrap();
            });
            started_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            join_tx.send(thread).unwrap();
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_setup(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            None,
        );
        let unrelated = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "independent".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        TaskStore::new(owner.store())
            .save(&[Save::Create(&unrelated)])
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if driver.get(unrelated.id()).unwrap().unwrap().is_terminal() {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        if mode == 2 {
            driver.request_abort(parent).unwrap();
        }
        if mode == 3 {
            driver.stop_worker_for_test();
        }
        {
            let (lock, wake) = &*gate;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        join_rx.recv().unwrap().join().unwrap();
        if mode == 1 {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while driver.get(child).is_ok() {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            assert!(observed.try_recv().is_err());
            assert!(driver.shutdown_with_pending().unwrap().failure().is_some());
        } else if mode >= 2 {
            assert!(observed.try_recv().is_err());
            assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        } else {
            observed
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert!(matches!(
                await_receipt(owner.store(), child).1,
                Outcome::Pending { .. }
            ));
            assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        }
        drop(parent_job);
    }
    #[test]
    fn busy_identity_update_retries_matching_identity() {
        identity_update_during_reserved_dispatch(0);
    }
    #[test]
    fn busy_identity_update_rejects_changed_identity() {
        identity_update_during_reserved_dispatch(1);
    }
    #[test]
    fn busy_identity_abort_does_not_dispatch() {
        identity_update_during_reserved_dispatch(2);
    }
    #[test]
    fn busy_identity_shutdown_does_not_dispatch() {
        identity_update_during_reserved_dispatch(3);
    }
    #[test]
    fn abort_before_identity_mismatch_does_not_stop_other_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let tool: std::sync::Arc<dyn Tool> = std::sync::Arc::new(RealTool {
            entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
        });
        let identity =
            crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let waiting = gate.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let setup = Box::new(move |child| {
            crate::task_driver::before_execute_for_test(child, move |_| {
                ready_tx.send(()).unwrap();
                let (lock, wake) = &*waiting;
                let mut ready = lock.lock().unwrap();
                while !*ready {
                    ready = wake.wait(ready).unwrap();
                }
            });
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_setup(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            None,
        );
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime
            .block_on(identity.update(Some("different".into()), || {}))
            .unwrap();
        driver.request_abort(parent).unwrap();
        {
            let (lock, wake) = &*gate;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let other = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "other".into(),
                data: json!(null),
            },
            3,
        )
        .unwrap()
        .registered("parent".into(), false);
        TaskStore::new(owner.store())
            .save(&[Save::Create(&other)])
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !driver.get(other.id()).unwrap().unwrap().is_terminal() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        while !driver.get(child).unwrap().unwrap().is_terminal() {
            assert!(
                std::time::Instant::now() < deadline,
                "abort cleanup did not complete"
            );
            std::thread::yield_now();
        }
        assert!(observed.try_recv().is_err());
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn identity_turning_busy_after_reservation_releases_and_retries() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let tool: std::sync::Arc<dyn Tool> = std::sync::Arc::new(RealTool {
            entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
        });
        let binding = crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let setup = Box::new(move |child| {
            crate::task_driver::before_execute_for_test(child, move |_| {
                reserved_tx.send(()).unwrap();
                go_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            });
        });
        let (busy_tx, busy_rx) = std::sync::mpsc::sync_channel(1);
        let busy_hook = std::sync::Arc::new(move || {
            let _ = busy_tx.try_send(());
        });
        let (driver, parent_job, owner, _, child) = actual_driver_fixture_with_setup(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            Some(busy_hook),
        );
        reserved_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (updating_tx, updating_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(binding.update(Some("real-backend".into()), move || {
                    updating_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                }))
                .unwrap();
        });
        updating_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        go_tx.send(()).unwrap();
        busy_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while TaskStore::new(owner.store())
            .load(child)
            .unwrap()
            .unwrap()
            .task
            .state()
            != &TaskState::Ready
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(observed.try_recv().is_err());
        release_tx.send(()).unwrap();
        updater.join().unwrap();
        observed
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(matches!(
            await_receipt(owner.store(), child).1,
            Outcome::Pending { .. }
        ));
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        drop(parent_job);
    }
    struct CancellationObservedTool {
        entered: std::sync::mpsc::Sender<()>,
        cancelled: std::sync::mpsc::Sender<()>,
        resume: std::sync::Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl Tool for CancellationObservedTool {
        fn spec(&self) -> ToolSpec {
            Stub.spec()
        }
        async fn invoke(&self, _: serde_json::Value, context: ToolContext) -> Outcome {
            self.entered.send(()).unwrap();
            context.cancellation.cancelled().await;
            self.cancelled.send(()).unwrap();
            self.resume.notified().await;
            Outcome::Pending {
                reference: "observed-cancellation".into(),
            }
        }
    }
    fn released_attempt_boundary(mode: u8) {
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let tool_release =
            std::sync::Arc::new((std::sync::Mutex::new(mode == 0), std::sync::Condvar::new()));
        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel();
        let resume = std::sync::Arc::new(tokio::sync::Notify::new());
        let tool: std::sync::Arc<dyn Tool> = if matches!(mode, 1 | 2 | 5 | 6) {
            std::sync::Arc::new(CancellationObservedTool {
                entered,
                cancelled: cancel_tx,
                resume: resume.clone(),
            })
        } else {
            std::sync::Arc::new(RealTool {
                entered,
                release: tool_release.clone(),
            })
        };
        let identity =
            crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
        let (pause_tx, pause_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let setup = Box::new(move |child| {
            crate::task_driver::before_execute_for_test(child, move |_| {
                reserved_tx.send(()).unwrap();
                proceed_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
            });
            let pause = move || {
                pause_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
            };
            if mode == 0 || mode == 3 || mode == 4 {
                crate::task_scheduler::after_ready_for_test(child, pause);
            } else {
                crate::task_driver::after_execute_for_test(child, pause);
            }
        });
        let (driver, parent_job, owner, parent, child) = actual_driver_fixture_with_setup(
            &dir.path().join("db"),
            tool,
            None,
            None,
            Some(setup),
            None,
        );
        reserved_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (updating_tx, updating_rx) = std::sync::mpsc::channel();
        let (update_release_tx, update_release_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(identity.update(Some("real-backend".into()), move || {
                    updating_tx.send(()).unwrap();
                    update_release_rx
                        .recv_timeout(std::time::Duration::from_secs(10))
                        .unwrap();
                }))
                .unwrap();
        });
        updating_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        proceed_tx.send(()).unwrap();
        pause_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        update_release_tx.send(()).unwrap();
        updater.join().unwrap();
        if mode == 0 || mode == 3 || mode == 4 {
            assert!(owner.task_gate().try_lock().is_err());
            assert!(observed.try_recv().is_err());
            if mode == 3 {
                driver.stop_worker_for_test();
            }
            if mode == 4 {
                std::thread::scope(|scope| {
                    let (started_tx, started_rx) = std::sync::mpsc::channel();
                    crate::task_driver::abort_gate_contention_for_test(parent, move || {
                        started_tx.send(()).unwrap();
                    });
                    let driver_ref = &driver;
                    let abort = scope.spawn(move || driver_ref.request_abort(parent));
                    started_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    release_tx.send(()).unwrap();
                    {
                        let (lock, wake) = &*tool_release;
                        *lock.lock().unwrap() = true;
                        wake.notify_all();
                    }
                    abort.join().unwrap().unwrap();
                });
                assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
                drop(parent_job);
                return;
            }
            release_tx.send(()).unwrap();
            if mode == 3 {
                assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
                drop(parent_job);
                return;
            }
            observed
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert!(matches!(
                await_receipt(owner.store(), child).1,
                Outcome::Pending { .. }
            ));
        } else {
            observed
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert_eq!(driver.active_unhooked_count_for_test(), 2);
            if mode == 1 || mode == 2 {
                if mode == 1 {
                    driver.request_abort(parent).unwrap();
                } else {
                    driver.stop_worker_for_test();
                }
                cancel_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            release_tx.send(()).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while driver.active_unhooked_count_for_test() != 1 {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
            if mode == 5 || mode == 6 {
                if mode == 5 {
                    driver.request_abort(parent).unwrap();
                } else {
                    driver.stop_worker_for_test();
                }
                cancel_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            resume.notify_one();
            assert_eq!(
                await_receipt(owner.store(), child).1,
                Outcome::Pending {
                    reference: "observed-cancellation".into()
                }
            );
        }
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn ready_publication_keeps_old_lease_fenced_until_retirement() {
        released_attempt_boundary(0);
    }
    #[test]
    fn abort_during_ready_publication_retires_old_lease() {
        released_attempt_boundary(4);
    }
    #[test]
    fn shutdown_during_ready_publication_retires_old_lease() {
        released_attempt_boundary(3);
    }
    #[test]
    fn abort_cancels_new_attempt_after_old_registration_removed() {
        released_attempt_boundary(5);
    }
    #[test]
    fn shutdown_cancels_new_attempt_after_old_registration_removed() {
        released_attempt_boundary(6);
    }
    #[test]
    fn old_cancellation_registration_cannot_block_new_attempt_or_abort() {
        released_attempt_boundary(1);
    }
    #[test]
    fn old_cancellation_registration_cannot_block_new_attempt_or_shutdown() {
        released_attempt_boundary(2);
    }
    #[test]
    fn busy_identity_for_four_children_does_not_starve_update_dependency() {
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let (entered, observed) = std::sync::mpsc::channel();
        let tool: std::sync::Arc<dyn Tool> = std::sync::Arc::new(RealTool {
            entered,
            release: std::sync::Arc::new((std::sync::Mutex::new(true), std::sync::Condvar::new())),
        });
        let binding = crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let identity = ToolIdentity::new(tool.as_ref(), "real-backend", 1).unwrap();
        let (task_tx, task_rx) = std::sync::mpsc::channel();
        let mut registry = Registry::default();
        assert!(registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null)))));
        assert!(registry.register("other", false, move |_: &Task| {
            task_tx.send(()).unwrap();
            Ok(Step::Succeed(json!(null)))
        }));
        let mut host = crate::async_task_tool::StandaloneNoHooksHost::open(
            dir.path().join("db"),
            registry,
            crate::async_task_tool::UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        host.register_tool("leaf", tool, "real-backend", 1).unwrap();
        let scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let parent_id = Uuid::new_v4();
        let ids: Vec<_> = (0..4).map(|_| Uuid::new_v4()).collect();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: json!({}),
        };
        let mut parent = Task::new(
            parent_id,
            TaskOwner::Conversation(Uuid::new_v4()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("parent".into(), false)
        .into_task_tool();
        parent.start().unwrap();
        parent
            .wait(crate::task::WaitFor::Tasks(ids.clone()))
            .unwrap();
        TaskStore::new(owner.store())
            .save(&[Save::Create(&parent)])
            .unwrap();
        for id in &ids {
            let child = Task::new(*id, TaskOwner::Task(parent_id), checkpoint.clone(), 3)
                .unwrap()
                .registered_versioned("leaf".into(), false, 1, json!({}))
                .unwrap()
                .into_task_tool();
            TaskStore::new(owner.store())
                .save(&[Save::Create(&child)])
                .unwrap();
            commit(
                owner.store(),
                prepare_child_identity(owner.store(), *id, &identity).unwrap(),
            );
        }
        let other = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(Uuid::new_v4()),
            checkpoint,
            3,
        )
        .unwrap()
        .registered("other".into(), false);
        TaskStore::new(owner.store())
            .save(&[Save::Create(&other)])
            .unwrap();
        let (updating_tx, updating_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(binding.update(Some("real-backend".into()), move || {
                    updating_tx.send(()).unwrap();
                    task_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                }))
                .unwrap();
        });
        updating_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let driver = crate::task_driver::Driver::start(scheduler);
        updater.join().unwrap();
        for _ in 0..4 {
            observed
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        }
        for id in ids {
            assert!(matches!(
                await_receipt(owner.store(), id).1,
                Outcome::Pending { .. }
            ));
        }
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }
    struct CancellableTool(std::sync::mpsc::Sender<()>);
    #[async_trait::async_trait]
    impl Tool for CancellableTool {
        fn spec(&self) -> ToolSpec {
            Stub.spec()
        }
        async fn invoke(&self, _: serde_json::Value, context: ToolContext) -> Outcome {
            self.0.send(()).unwrap();
            context.cancellation.cancelled().await;
            Outcome::Unknown("cancelled after logical dispatch".into())
        }
    }
    #[test]
    fn actual_driver_abort_cancels_and_joins_cooperative_tool_with_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(CancellableTool(entered));
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent).unwrap();
        let (_, outcome) = await_receipt(owner.store(), child);
        assert_eq!(
            outcome,
            Outcome::Unknown("cancelled after logical dispatch".into())
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn actual_driver_observes_scheduler_internal_abort_mark() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(CancellableTool(entered));
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        {
            let _gate = owner.task_gate().lock().unwrap();
            crate::task_scheduler::request_abort_subtree(&TaskStore::new(owner.store()), parent)
                .unwrap();
        }
        let (_, outcome) = await_receipt(owner.store(), child);
        assert_eq!(
            outcome,
            Outcome::Unknown("cancelled after logical dispatch".into())
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent_job);
    }
    fn actual_driver_cancels_and_joins_sealed_tool(mode: u8) {
        use crate::task_scheduler::{Registry, Step};
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool: std::sync::Arc<dyn Tool> = std::sync::Arc::new(CancellableTool(entered));
        let _binding =
            crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let identity = ToolIdentity::new(tool.as_ref(), "real-backend", 1).unwrap();
        let mut registry = Registry::default();
        assert!(registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null)))));
        let mut host = crate::async_task_tool::StandaloneNoHooksHost::open(
            dir.path().join("db"),
            registry,
            crate::async_task_tool::UnhookedAuthorization::standalone_only(),
        )
        .unwrap();
        host.register_tool("leaf", tool, "real-backend", 1).unwrap();
        let scheduler = host.into_scheduler_for_test();
        let owner = scheduler.owner();
        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();
        let failed_id = Uuid::new_v4();
        let checkpoint = Checkpoint {
            phase: "start".into(),
            data: json!({}),
        };
        let mut parent = Task::new(
            parent_id,
            TaskOwner::Conversation(Uuid::new_v4()),
            checkpoint.clone(),
            3,
        )
        .unwrap()
        .registered("parent".into(), false)
        .into_task_tool();
        parent.start().unwrap();
        parent
            .wait(crate::task::WaitFor::TasksFailFast(vec![
                child_id, failed_id,
            ]))
            .unwrap();
        let child = Task::new(child_id, TaskOwner::Task(parent_id), checkpoint.clone(), 3)
            .unwrap()
            .registered_versioned("leaf".into(), false, 1, json!({}))
            .unwrap()
            .into_task_tool();
        let failed = Task::new(failed_id, TaskOwner::Task(parent_id), checkpoint, 3)
            .unwrap()
            .registered("missing".into(), false);
        TaskStore::new(owner.store())
            .save(&[
                crate::task_store::Save::Create(&parent),
                crate::task_store::Save::Create(&child),
                crate::task_store::Save::Create(&failed),
            ])
            .unwrap();
        commit(
            owner.store(),
            prepare_child_identity(owner.store(), child_id, &identity).unwrap(),
        );
        let driver = crate::task_driver::Driver::start(scheduler);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        if mode == 1 {
            {
                let _gate = owner.task_gate().lock().unwrap();
                let tasks = TaskStore::new(owner.store());
                let failed = tasks.load(failed_id).unwrap().unwrap();
                tasks
                    .update(&failed, |task| {
                        task.start()?;
                        task.fail("failure".into())
                    })
                    .unwrap();
            }
        } else if mode == 2 {
            let _gate = owner.task_gate().lock().unwrap();
            let tasks = TaskStore::new(owner.store());
            let parent = tasks.load(parent_id).unwrap().unwrap();
            tasks
                .update(&parent, |task| {
                    task.wake()?;
                    task.start()?;
                    task.hold_failure("parent failure".into())
                })
                .unwrap();
        } else {
            driver.request_abort(parent_id).unwrap();
        }
        let (_, outcome) = await_receipt(owner.store(), child_id);
        assert_eq!(
            outcome,
            Outcome::Unknown("cancelled after logical dispatch".into())
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if driver.get(child_id).unwrap().unwrap().is_terminal() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "abort cleanup did not finish"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(driver.shutdown_with_pending().unwrap().failure().is_none());
    }
    #[test]
    fn actual_driver_fail_fast_cancels_and_joins_sealed_tool() {
        actual_driver_cancels_and_joins_sealed_tool(1);
    }
    #[test]
    fn actual_driver_explicit_abort_cancels_joins_and_cleans_up_sealed_tool() {
        actual_driver_cancels_and_joins_sealed_tool(0);
    }
    #[test]
    fn actual_driver_parent_failure_cancels_joins_and_cleans_up_sealed_tool() {
        actual_driver_cancels_and_joins_sealed_tool(2);
    }
    #[test]
    fn actual_driver_abort_after_intent_cancels_inflight_executor_and_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let (tool_entered, tool_received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(CancellableTool(tool_entered));
        let (after_entered, after_received) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let hook_release = release.clone();
        let after = std::sync::Arc::new(move || {
            after_entered.send(()).unwrap();
            let (lock, wake) = &*hook_release;
            let mut allowed = lock.lock().unwrap();
            while !*allowed {
                allowed = wake.wait(allowed).unwrap();
            }
        });
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture_with_hook(&dir.path().join("db"), tool, None, Some(after));
        after_received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(load_intent(owner.store(), child).is_ok());
        driver.request_abort(parent).unwrap();
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        assert_eq!(
            await_receipt(owner.store(), child).1,
            Outcome::Unknown("cancelled after logical dispatch".into())
        );
        assert!(tool_received.try_recv().is_err());
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn actual_driver_shutdown_cancels_and_joins_cooperative_tool() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(CancellableTool(entered));
        let (driver, parent_job, owner, _, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        assert_eq!(
            await_receipt(owner.store(), child).1,
            Outcome::Unknown("cancelled after logical dispatch".into())
        );
        drop(parent_job);
    }
    #[test]
    fn killed_actual_driver_after_intent_keeps_uncertain_attempt() {
        const ENV: &str = "PI_RS_DRIVER_INTENT_KILL";
        if let Ok(directory) = std::env::var(ENV) {
            let dir = std::path::Path::new(&directory);
            let (entered, _received) = std::sync::mpsc::channel();
            let tool = std::sync::Arc::new(CancellableTool(entered));
            let signal_dir = dir.to_path_buf();
            let after = std::sync::Arc::new(move || {
                std::fs::write(signal_dir.join("ready.tmp"), b"ready").unwrap();
                std::fs::rename(signal_dir.join("ready.tmp"), signal_dir.join("ready")).unwrap();
                loop {
                    std::thread::park();
                }
            });
            let (driver, parent, owner, _, _) =
                actual_driver_fixture_with_hook(&dir.join("db"), tool, None, Some(after));
            loop {
                std::hint::black_box((&driver, &parent, &owner));
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_protocol::tests::killed_actual_driver_after_intent_keeps_uncertain_attempt")
            .env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("driver did not commit intent");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let records = store.list(b"durable/async-task/intent/").unwrap();
        assert_eq!(records.len(), 1);
        let intent: Intent = serde_json::from_slice(&records[0].1.value).unwrap();
        assert_eq!(read_receipt(&store, &intent).unwrap(), None);
        let task = TaskStore::new(&store).load(intent.task).unwrap().unwrap();
        assert_eq!(task.task.state(), &TaskState::Running);
        assert!(prepare_intent(&store, &task, &intent.identity).is_err());
        drop(store);
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = crate::task_scheduler::Registry::default();
        assert!(registry.register("parent", false, |_: &Task| Ok(
            crate::task_scheduler::Step::Succeed(json!(null))
        )));
        let calls_on_run = calls.clone();
        assert!(registry.register("leaf", false, move |_: &Task| {
            calls_on_run.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::task_scheduler::Step::Succeed(json!(null)))
        }));
        let mut scheduler =
            crate::task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        for _ in 0..20 {
            if !scheduler.tick(0).unwrap() {
                break;
            }
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
    struct KnownOnCancel(std::sync::mpsc::Sender<()>);
    #[async_trait::async_trait]
    impl Tool for KnownOnCancel {
        fn spec(&self) -> ToolSpec {
            Stub.spec()
        }
        async fn invoke(&self, _: serde_json::Value, context: ToolContext) -> Outcome {
            self.0.send(()).unwrap();
            context.cancellation.cancelled().await;
            Outcome::Structured {
                data: json!({"completed":true}),
                attachments: vec![],
            }
        }
    }
    #[test]
    fn actual_driver_keeps_known_structured_result_after_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(KnownOnCancel(entered));
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        driver.request_abort(parent).unwrap();
        assert_eq!(
            await_receipt(owner.store(), child).1,
            Outcome::Structured {
                data: json!({"completed":true}),
                attachments: vec![]
            }
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent_job);
    }
    #[test]
    fn killed_actual_driver_during_tool_leaves_no_retryable_outcome() {
        const ENV: &str = "PI_RS_DRIVER_RUNNING_TOOL_KILL";
        if let Ok(directory) = std::env::var(ENV) {
            let dir = std::path::Path::new(&directory);
            let (entered, received) = std::sync::mpsc::channel();
            let tool = std::sync::Arc::new(CancellableTool(entered));
            let (driver, parent, owner, _, child) = actual_driver_fixture(&dir.join("db"), tool);
            received
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            std::fs::write(dir.join("ready.tmp"), child.to_string()).unwrap();
            std::fs::rename(dir.join("ready.tmp"), dir.join("ready")).unwrap();
            loop {
                std::hint::black_box((&driver, &parent, &owner));
                std::thread::park();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let mut process = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("async_task_protocol::tests::killed_actual_driver_during_tool_leaves_no_retryable_outcome")
            .env(ENV, dir.path()).spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !dir.path().join("ready").exists() {
            if std::time::Instant::now() >= deadline {
                let _ = process.kill();
                let _ = process.wait();
                panic!("driver tool did not start");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let child: Uuid = std::fs::read_to_string(dir.path().join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let intent = load_intent(&store, child).unwrap();
        assert_eq!(read_receipt(&store, &intent).unwrap(), None);
        drop(store);
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = crate::task_scheduler::Registry::default();
        assert!(registry.register("parent", false, |_: &Task| Ok(
            crate::task_scheduler::Step::Succeed(json!(null))
        )));
        let calls_on_run = calls.clone();
        assert!(registry.register("leaf", false, move |_: &Task| {
            calls_on_run.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::task_scheduler::Step::Succeed(json!(null)))
        }));
        let mut scheduler =
            crate::task_scheduler::Scheduler::open(dir.path().join("db"), registry).unwrap();
        for _ in 0..20 {
            if !scheduler.tick(0).unwrap() {
                break;
            }
        }
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(scheduler);
        let store = Store::open(dir.path().join("db")).unwrap();
        assert!(matches!(
            crate::async_task_tool::read_sealed_outcome_evidence(&store, child).unwrap(),
            Some(crate::async_task_tool::SealedOutcomeEvidence::Uncertain(
                Outcome::Unknown(_)
            ))
        ));
        assert_eq!(load_intent(&store, child).unwrap(), intent);
        assert_eq!(read_receipt(&store, &intent).unwrap(), None);
    }
    #[test]
    fn worker_pins_host_tool_identity_until_joined_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let (entered, received) = std::sync::mpsc::channel();
        let tool: std::sync::Arc<dyn Tool> = std::sync::Arc::new(CancellableTool(entered));
        let identity =
            crate::nested_identity::register(&tool, Some("real-backend".into())).unwrap();
        let (driver, parent_job, owner, parent, child) =
            actual_driver_fixture(&dir.path().join("db"), tool);
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        assert!(
            runtime
                .block_on(identity.update(Some("changed".into()), || {}))
                .is_err()
        );
        driver.request_abort(parent).unwrap();
        assert_eq!(
            await_receipt(owner.store(), child).1,
            Outcome::Unknown("cancelled after logical dispatch".into())
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(report);
        assert!(
            runtime
                .block_on(identity.update(Some("changed".into()), || {}))
                .is_ok()
        );
        drop(parent_job);
    }
    struct MutableSpecTool {
        changed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        entered: std::sync::mpsc::Sender<()>,
    }
    #[async_trait::async_trait]
    impl Tool for MutableSpecTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "leaf".into(),
                description: String::new(),
                parameters: if self.changed.load(std::sync::atomic::Ordering::SeqCst) {
                    json!({"type":"object", "required":["changed"]})
                } else {
                    json!({"type":"object"})
                },
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            self.entered.send(()).unwrap();
            Outcome::Succeeded("unexpected".into())
        }
    }
    #[test]
    fn actual_driver_rejects_changed_spec_before_intent() {
        let dir = tempfile::tempdir().unwrap();
        let changed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (tool_entered, tool_received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(MutableSpecTool {
            changed: changed.clone(),
            entered: tool_entered,
        });
        let (before_entered, before_received) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let hook_release = release.clone();
        let before = std::sync::Arc::new(move || {
            before_entered.send(()).unwrap();
            let (lock, wake) = &*hook_release;
            let mut allowed = lock.lock().unwrap();
            while !*allowed {
                allowed = wake.wait(allowed).unwrap();
            }
        });
        let (driver, parent_job, owner, _, child) =
            actual_driver_fixture_with_hook(&dir.path().join("db"), tool, Some(before), None);
        before_received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        changed.store(true, std::sync::atomic::Ordering::SeqCst);
        {
            let (lock, wake) = &*release;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while driver.get(child).is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "driver did not reject changed spec"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(tool_received.try_recv().is_err());
        assert!(owner.store().get(&intent_key(child)).unwrap().is_none());
        assert!(driver.shutdown_with_pending().unwrap().failure().is_some());
        drop(parent_job);
    }
    struct NormalizingTool(std::sync::mpsc::Sender<serde_json::Value>);
    #[async_trait::async_trait]
    impl Tool for NormalizingTool {
        fn spec(&self) -> ToolSpec {
            Stub.spec()
        }
        async fn prepare_arguments(
            &self,
            mut value: serde_json::Value,
        ) -> pi_rs_core::ExtensionResult<serde_json::Value> {
            value["normalized"] = json!(true);
            Ok(value)
        }
        async fn invoke(&self, value: serde_json::Value, _: ToolContext) -> Outcome {
            self.0.send(value.clone()).unwrap();
            Outcome::Structured {
                data: value,
                attachments: vec![],
            }
        }
    }
    #[test]
    fn sealed_driver_persists_normalized_arguments_before_tool_poll() {
        let dir = tempfile::tempdir().unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        let tool = std::sync::Arc::new(NormalizingTool(sent));
        let (driver, parent, owner, _, child) = actual_driver_fixture(&dir.path().join("db"), tool);
        let arguments = received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let prepared = owner.store().get(&prepared_key(child)).unwrap().unwrap();
        let (intent, persisted): (Intent, serde_json::Value) =
            serde_json::from_slice(&prepared.value).unwrap();
        assert_eq!(persisted, arguments);
        assert_eq!(persisted, json!({"normalized":true}));
        assert_eq!(
            read_receipt(owner.store(), &intent)
                .unwrap()
                .or_else(|| Some(await_receipt(owner.store(), child).1)),
            Some(Outcome::Structured {
                data: arguments,
                attachments: vec![]
            })
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent);
    }
    struct UnsupportedNestedTool;
    #[async_trait::async_trait]
    impl Tool for UnsupportedNestedTool {
        fn spec(&self) -> ToolSpec {
            Stub.spec()
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
        async fn invoke_with_host(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: Option<&pi_rs_core::ExtensionState>,
            host: std::sync::Arc<dyn pi_rs_core::ToolHost>,
        ) -> Outcome {
            let _ = host.call("ignored", json!({})).await;
            Outcome::Succeeded("not-authorized".into())
        }
    }
    #[test]
    fn sealed_driver_rejects_ignored_nested_host_call() {
        let dir = tempfile::tempdir().unwrap();
        let (driver, parent, owner, _, child) = actual_driver_fixture(
            &dir.path().join("db"),
            std::sync::Arc::new(UnsupportedNestedTool),
        );
        let (_, outcome) = await_receipt(owner.store(), child);
        assert!(
            matches!(outcome, Outcome::Unknown(text) if text.contains("unsupported host operation"))
        );
        let report = driver.shutdown_with_pending().unwrap();
        assert!(report.failure().is_none());
        drop(parent);
    }
}
