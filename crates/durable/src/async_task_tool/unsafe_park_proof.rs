//! Checked proof that an unsafe invocation committed its park before a wake.
use super::{DurableToolStep, DurableWait};
use crate::task::{Checkpoint, Task, TaskState, WaitFor};
use durable_store::{Check, Expected, Mutation, Store};
use uuid::Uuid;

/// An executor-joined unsafe Tool outcome retained independently of settlement.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UnsafeDurableReceipt {
    pub task_id: Uuid,
    pub attempt_id: Uuid,
    pub outcome: pi_rs_core::Outcome,
}

/// A returned Park proposal. It is evidence only and does not prove a park
/// committed or authorize the Tool to resume.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UnsafeDurableParkEvidence {
    pub task_id: Uuid,
    pub attempt_id: Uuid,
    pub checkpoint: serde_json::Value,
    pub wait: DurableWait,
}

pub(crate) fn prepare_receipt(
    store: &Store,
    task: &Task,
    intent_key: &[u8],
    outcome: &pi_rs_core::Outcome,
) -> Result<(Vec<Check>, Vec<Mutation>), String> {
    let attempt = task.attempt_id().ok_or("unsafe receipt has no attempt")?;
    let receipt = UnsafeDurableReceipt {
        task_id: task.id(),
        attempt_id: attempt,
        outcome: outcome.clone(),
    };
    prepare_evidence(
        store,
        task,
        intent_key,
        b"/receipt",
        serde_json::to_vec(&receipt).map_err(|e| e.to_string())?,
    )
}

pub(crate) fn prepare_park_evidence(
    store: &Store,
    task: &Task,
    intent_key: &[u8],
    checkpoint: &serde_json::Value,
    wait: &DurableWait,
) -> Result<(Vec<Check>, Vec<Mutation>), String> {
    let attempt = task
        .attempt_id()
        .ok_or("unsafe park evidence has no attempt")?;
    let evidence = UnsafeDurableParkEvidence {
        task_id: task.id(),
        attempt_id: attempt,
        checkpoint: checkpoint.clone(),
        wait: wait.clone(),
    };
    prepare_evidence(
        store,
        task,
        intent_key,
        b"/park-evidence",
        serde_json::to_vec(&evidence).map_err(|e| e.to_string())?,
    )
}

fn prepare_evidence(
    store: &Store,
    task: &Task,
    intent_key: &[u8],
    suffix: &[u8],
    value: Vec<u8>,
) -> Result<(Vec<Check>, Vec<Mutation>), String> {
    let attempt = task.attempt_id().ok_or("unsafe evidence has no attempt")?;
    if task.replay_safe()
        || task.state() != &TaskState::Running
        || intent_key != self::intent_key(task.id(), attempt)
    {
        return Err("invalid unsafe receipt attempt".into());
    }
    let intent = store
        .get(intent_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing unsafe intent")?;
    let recorded: Task =
        serde_json::from_slice(&intent.value).map_err(|_| "invalid unsafe intent")?;
    if &recorded != task {
        return Err("unsafe receipt intent mismatch".into());
    }
    if value.len() > 65536 {
        return Err("unsafe receipt exceeds 64 KiB".into());
    }
    let mut receipt_key = intent_key.to_vec();
    receipt_key.extend_from_slice(suffix);
    let existing = store.get(&receipt_key).map_err(|e| e.to_string())?;
    if existing.as_ref().is_some_and(|entry| entry.value != value) {
        return Err("unsafe receipt already differs".into());
    }
    let checks = vec![
        Check {
            key: intent_key.to_vec(),
            expected: Expected::Version(intent.version),
        },
        Check {
            key: receipt_key.clone(),
            expected: existing
                .as_ref()
                .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
        },
    ];
    let mutations = if existing.is_none() {
        vec![Mutation::Put {
            key: receipt_key,
            value,
        }]
    } else {
        vec![]
    };
    Ok((checks, mutations))
}

/// Read Park proposals as reconciliation evidence. They cannot authorize wake.
pub fn read_park_evidence(
    store: &Store,
    task: Uuid,
) -> Result<Vec<UnsafeDurableParkEvidence>, String> {
    let prefix = format!("durable/unsafe-park-async/{task}/");
    let mut results = Vec::new();
    for (key, entry) in store.list(prefix.as_bytes()).map_err(|e| e.to_string())? {
        if !key.ends_with(b"/intent/park-evidence") {
            continue;
        }
        let evidence: UnsafeDurableParkEvidence =
            serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe park evidence")?;
        if evidence.task_id != task {
            return Err("unsafe park evidence task mismatch".into());
        }
        let intent_key = intent_key(task, evidence.attempt_id);
        let mut expected = intent_key.clone();
        expected.extend_from_slice(b"/park-evidence");
        if key != expected {
            return Err("unsafe park evidence key mismatch".into());
        }
        let intent = store
            .get(&intent_key)
            .map_err(|e| e.to_string())?
            .ok_or("unsafe park evidence lacks intent")?;
        let recorded: Task =
            serde_json::from_slice(&intent.value).map_err(|_| "invalid unsafe intent")?;
        if recorded.id() != task
            || recorded.attempt_id() != Some(evidence.attempt_id)
            || recorded.replay_safe()
            || recorded.state() != &TaskState::Running
        {
            return Err("unsafe park evidence intent mismatch".into());
        }
        results.push(evidence);
    }
    Ok(results)
}

/// Returns joined outcomes as reconciliation evidence, not authorization to replay.
pub fn read_receipts(store: &Store, task: Uuid) -> Result<Vec<UnsafeDurableReceipt>, String> {
    let prefix = format!("durable/unsafe-park-async/{task}/");
    let mut receipts = Vec::new();
    for (key, entry) in store.list(prefix.as_bytes()).map_err(|e| e.to_string())? {
        if !key.ends_with(b"/intent/receipt") {
            continue;
        }
        let receipt: UnsafeDurableReceipt =
            serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe receipt")?;
        if receipt.task_id != task {
            return Err("unsafe receipt task mismatch".into());
        }
        let intent_key = intent_key(task, receipt.attempt_id);
        let mut expected_key = intent_key.clone();
        expected_key.extend_from_slice(b"/receipt");
        if key != expected_key {
            return Err("unsafe receipt key mismatch".into());
        }
        let intent = store
            .get(&intent_key)
            .map_err(|e| e.to_string())?
            .ok_or("unsafe receipt lacks intent")?;
        let recorded: Task =
            serde_json::from_slice(&intent.value).map_err(|_| "invalid unsafe intent")?;
        if recorded.id() != task
            || recorded.attempt_id() != Some(receipt.attempt_id)
            || recorded.replay_safe()
            || recorded.state() != &TaskState::Running
        {
            return Err("unsafe receipt intent mismatch".into());
        }
        receipts.push(receipt);
    }
    Ok(receipts)
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ParkProof {
    version: u32,
    task: Uuid,
    attempt: Uuid,
    checkpoint: Checkpoint,
    wait: WaitFor,
}
fn key(task: Uuid) -> Vec<u8> {
    format!("durable/unsafe-park-async/{task}/park").into_bytes()
}
fn intent_key(task: Uuid, attempt: Uuid) -> Vec<u8> {
    format!("durable/unsafe-park-async/{task}/{attempt}/intent").into_bytes()
}

/// This mutation must be committed with the task wait, children, and result.
pub(crate) fn prepare_park(
    store: &Store,
    task: &Task,
    checkpoint: &Checkpoint,
    wait: &WaitFor,
) -> Result<(Check, Mutation), String> {
    if task.replay_safe()
        || task.state() != &TaskState::Running
        || task.abort_requested()
        || !matches!(wait, WaitFor::Tasks(_) | WaitFor::TasksAny(_))
    {
        return Err("invalid unsafe park".into());
    }
    let attempt = task.attempt_id().ok_or("unsafe park has no attempt")?;
    let proof = ParkProof {
        version: 1,
        task: task.id(),
        attempt,
        checkpoint: checkpoint.clone(),
        wait: wait.clone(),
    };
    let key = key(task.id());
    let old = store.get(&key).map_err(|e| e.to_string())?;
    let check = Check {
        key: key.clone(),
        expected: old
            .as_ref()
            .map_or(Expected::Missing, |entry| Expected::Version(entry.version)),
    };
    let mutation = Mutation::Put {
        key,
        value: serde_json::to_vec(&proof).map_err(|e| e.to_string())?,
    };
    Ok((check, mutation))
}

/// Bind the next dispatch to the exact committed park and prior attempt.
pub(crate) fn dispatch_checks(store: &Store, task: &Task) -> Result<Vec<Check>, String> {
    let park_key = key(task.id());
    let park_entry = store.get(&park_key).map_err(|e| e.to_string())?;
    if task.checkpoint().phase == "initial" {
        if park_entry.is_some() {
            return Err("unexpected unsafe park proof".into());
        }
        return Ok(vec![Check {
            key: park_key,
            expected: Expected::Missing,
        }]);
    }
    let entry = park_entry.ok_or("unsafe wake lacks committed park proof")?;
    let proof: ParkProof =
        serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe park proof")?;
    if proof.version != 1
        || proof.task != task.id()
        || proof.checkpoint != *task.checkpoint()
        || task.replay_safe()
    {
        return Err("unsafe park proof does not match task".into());
    }
    let ended = task
        .ended_children()
        .ok_or("unsafe wake lacks ended children")?;
    let wait_ids = match &proof.wait {
        WaitFor::Tasks(ids) | WaitFor::TasksAny(ids) if !ids.is_empty() => ids,
        _ => return Err("invalid unsafe wait".into()),
    };
    if ended.is_empty()
        || ended.iter().any(|id| !wait_ids.contains(id))
        || matches!(&proof.wait, WaitFor::Tasks(_)) && ended.len() != wait_ids.len()
    {
        return Err("unsafe wake does not match committed wait".into());
    }
    let previous_key = intent_key(task.id(), proof.attempt);
    let previous = store
        .get(&previous_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing parked attempt intent")?;
    let previous_task: Task =
        serde_json::from_slice(&previous.value).map_err(|_| "invalid parked attempt")?;
    if previous_task.id() != task.id()
        || previous_task.attempt_id() != Some(proof.attempt)
        || previous_task.owner() != task.owner()
        || previous_task.kind() != task.kind()
        || previous_task.definition_version() != task.definition_version()
        || previous_task.input() != task.input()
        || previous_task.replay_safe()
        || previous_task.abort_requested()
        || previous_task.state() != &TaskState::Running
    {
        return Err("parked attempt identity changed".into());
    }
    let mut result_key = previous_key.clone();
    result_key.extend_from_slice(b"/result");
    let result = store
        .get(&result_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing committed park result")?;
    let step: DurableToolStep =
        serde_json::from_slice(&result.value).map_err(|_| "invalid committed park result")?;
    let DurableToolStep::Park { checkpoint, wait } = step else {
        return Err("previous result did not park".into());
    };
    let same_wait = match (&proof.wait, &wait) {
        (WaitFor::Tasks(ids), DurableWait::All(calls))
        | (WaitFor::TasksAny(ids), DurableWait::Any(calls)) => {
            ids == &calls.iter().map(|c| c.task_id()).collect::<Vec<_>>()
                && calls.iter().all(|c| c.0.belongs_to(task.id()))
        }
        _ => false,
    };
    if !same_wait || proof.checkpoint.data.get("checkpoint") != Some(&checkpoint) {
        return Err("committed park result mismatch".into());
    }
    Ok(vec![
        Check {
            key: park_key,
            expected: Expected::Version(entry.version),
        },
        Check {
            key: previous_key,
            expected: Expected::Version(previous.version),
        },
        Check {
            key: result_key,
            expected: Expected::Version(result.version),
        },
    ])
}
