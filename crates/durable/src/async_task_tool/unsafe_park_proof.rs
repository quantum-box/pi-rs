//! Checked proof that an unsafe invocation committed its park before a wake.
use super::{DurableToolStep, DurableWait};
use crate::task::{Checkpoint, Task, TaskState, WaitFor};
use durable_store::{Check, Expected, Mutation, Store};
use sha2::{Digest, Sha256};
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

pub(crate) fn prepare_no_dispatch(
    store: &Store,
    task: &Task,
    intent: &[u8],
    outcome: &pi_rs_core::Outcome,
) -> Result<(Vec<Check>, Vec<Mutation>), String> {
    if !matches!(outcome, pi_rs_core::Outcome::Failed(_)) {
        return Err("invalid no-dispatch result".into());
    }
    prepare_evidence(
        store,
        task,
        intent,
        b"/no-dispatch",
        serde_json::to_vec(outcome).map_err(|e| e.to_string())?,
    )
}

/// Validate an atomically settled, first-attempt no-dispatch failure.
pub(crate) fn no_dispatch_result(
    store: &Store,
    task: &Task,
    outcome: &pi_rs_core::Outcome,
) -> Result<bool, String> {
    let Some(attempt) = task.retired_attempt_id() else {
        return Ok(false);
    };
    let intent = intent_key(task.id(), attempt);
    let mut marker = intent.clone();
    marker.extend_from_slice(b"/no-dispatch");
    let Some(entry) = store.get(&marker).map_err(|e| e.to_string())? else {
        return Ok(false);
    };
    let recorded_outcome: pi_rs_core::Outcome =
        serde_json::from_slice(&entry.value).map_err(|_| "invalid no-dispatch outcome")?;
    if !matches!(recorded_outcome, pi_rs_core::Outcome::Failed(_)) || &recorded_outcome != outcome {
        return Err("no-dispatch result mismatch".into());
    }
    let prefix = format!("durable/unsafe-park-async/{}/", task.id());
    let entries = store.list(prefix.as_bytes()).map_err(|e| e.to_string())?;
    let mut result_key = intent.clone();
    result_key.extend_from_slice(b"/result");
    if entries.len() != 3
        || entries
            .iter()
            .any(|(key, _)| key != &intent && key != &marker && key != &result_key)
    {
        return Err("ambiguous no-dispatch evidence".into());
    }
    let saved = store
        .get(&intent)
        .map_err(|e| e.to_string())?
        .ok_or("missing no-dispatch intent")?;
    let running: Task =
        serde_json::from_slice(&saved.value).map_err(|_| "invalid no-dispatch intent")?;
    if running.id() != task.id()
        || running.attempt_id() != Some(attempt)
        || running.owner() != task.owner()
        || running.kind() != task.kind()
        || running.definition_version() != task.definition_version()
        || running.input() != task.input()
        || running.replay_safe()
        || running.abort_requested()
        || running.restarts() != 0
        || running.checkpoint().phase != "initial"
        || running.state() != &TaskState::Running
    {
        return Err("no-dispatch intent mismatch".into());
    }
    let saved_result = store
        .get(&result_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing no-dispatch result")?;
    let step: DurableToolStep =
        serde_json::from_slice(&saved_result.value).map_err(|_| "invalid no-dispatch result")?;
    if step != DurableToolStep::Complete(outcome.clone()) {
        return Err("no-dispatch result evidence mismatch".into());
    }
    Ok(true)
}

pub(crate) fn prepare_park_evidence(
    store: &Store,
    task: &Task,
    intent_key: &[u8],
    checkpoint: &serde_json::Value,
    wait: &DurableWait,
    arguments: &serde_json::Value,
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
    let (mut checks, mut mutations) = prepare_evidence(
        store,
        task,
        intent_key,
        b"/park-evidence",
        serde_json::to_vec(&evidence).map_err(|e| e.to_string())?,
    )?;
    // The first unhooked attempt prepares arguments after intent. Bind them
    // in separate joined evidence without treating the proposal as a park.
    if task.checkpoint().phase == "initial"
        && !task
            .kind()
            .is_some_and(|kind| kind.starts_with("pi-rs.unsafe-park-async.v1.hook."))
    {
        let (extra_checks, extra_mutations) = prepare_evidence(
            store,
            task,
            intent_key,
            b"/prepared-arguments",
            argument_digest(arguments)?,
        )?;
        for check in extra_checks {
            if !checks.contains(&check) {
                checks.push(check);
            }
        }
        mutations.extend(extra_mutations);
    }
    Ok((checks, mutations))
}

fn argument_digest(arguments: &serde_json::Value) -> Result<Vec<u8>, String> {
    if !arguments.is_object() {
        return Err("invalid prepared arguments evidence".into());
    }
    let mut evidence = b"pi-rs.unsafe-park.arguments.sha256.v1\0".to_vec();
    evidence.extend_from_slice(&Sha256::digest(
        serde_json::to_vec(arguments).map_err(|e| e.to_string())?,
    ));
    Ok(evidence)
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
    park_checks(store, task, true, false)
}

/// Validate the identity and original park of a woken attempt without
/// asserting that its external outcome is absent.
pub(crate) fn woken_evidence_checks(
    store: &Store,
    task: &Task,
    attempt: Uuid,
) -> Result<Vec<Check>, String> {
    let key = intent_key(task.id(), attempt);
    let entry = store
        .get(&key)
        .map_err(|e| e.to_string())?
        .ok_or("missing woken intent")?;
    let intent: Task = serde_json::from_slice(&entry.value).map_err(|_| "invalid woken intent")?;
    if intent.id() != task.id()
        || intent.attempt_id() != Some(attempt)
        || intent.checkpoint() != task.checkpoint()
        || intent.owner() != task.owner()
        || intent.kind() != task.kind()
        || intent.input() != task.input()
        || intent.definition_version() != task.definition_version()
        || intent.replay_safe()
        || intent.state() != &TaskState::Running
        || intent.abort_requested()
    {
        return Err("woken intent identity mismatch".into());
    }
    let mut checks = park_checks(store, &intent, true, true)?;
    checks.push(Check {
        key,
        expected: Expected::Version(entry.version),
    });
    Ok(checks)
}

/// Authenticate a committed unsafe park without authorizing a Tool wake.
pub(crate) fn cancel_checks(
    store: &Store,
    task: &Task,
    boundary: u64,
) -> Result<Vec<Check>, String> {
    cancel_checks_with_attempt(store, task, boundary).map(|(checks, _)| checks)
}
pub(crate) fn cancel_checks_with_attempt(
    store: &Store,
    task: &Task,
    boundary: u64,
) -> Result<(Vec<Check>, Option<Uuid>), String> {
    let woken_enabled = task
        .kind()
        .is_some_and(|kind| kind.starts_with("pi-rs.unsafe-park-async.v1.hook.woken."));
    cancel_checks_with_policy(store, task, boundary, woken_enabled)
}

/// Capture evidence for one interrupted woken execution. This only proves the
/// original park, prepared arguments and absence of a joined outcome; callers
/// must separately authorize the registration, owner, abort and callback and
/// commit all returned checks atomically. It never authorizes dispatch itself.
pub(crate) fn woken_cancel_candidate_checks(
    store: &Store,
    task: &Task,
    boundary: u64,
) -> Result<(Uuid, Vec<Check>), String> {
    if task.state() != &TaskState::Running || task.abort_requested() || task.replay_safe() {
        return Err("invalid interrupted woken execution".into());
    }
    let expected = task.attempt_id().ok_or("missing woken attempt")?;
    let (checks, target) = cancel_checks_with_policy(store, task, boundary, true)?;
    if target != Some(expected) {
        return Err("woken attempt does not match the interrupted task".into());
    }
    Ok((expected, checks))
}

/// Capture the exact joined woken attempt after a live abort. This is only
/// execution evidence; callers must fence the task and conversation binding.
pub(crate) fn woken_live_abort_candidate_checks(
    store: &Store,
    task: &Task,
    boundary: u64,
) -> Result<Vec<Check>, String> {
    if task.state() != &TaskState::Running || !task.abort_requested() || task.replay_safe() {
        return Err("invalid live woken abort".into());
    }
    let attempt = task.attempt_id().ok_or("missing woken attempt")?;
    let (checks, target) = cancel_checks_with_policy(store, task, boundary, true)?;
    if target != Some(attempt) {
        return Err("woken attempt changed".into());
    }
    Ok(checks)
}

/// Validate the prior wake for an already-reserved abort cleanup. This proves
/// neither the current task version nor any conversation authorization.
#[allow(dead_code)]
pub(crate) fn woken_cleanup_checks(
    store: &Store,
    task: &Task,
    boundary: u64,
) -> Result<(Vec<Check>, Uuid), String> {
    if task.state() != &TaskState::Running || !task.abort_requested() || task.replay_safe() {
        return Err("invalid woken cleanup attempt".into());
    }
    let expected = task
        .woken_interrupted_attempt()
        .ok_or("missing interrupted wake")?;
    let (checks, target) = cancel_checks_with_policy(store, task, boundary, true)?;
    if target != Some(expected) {
        return Err("interrupted wake does not match evidence".into());
    }
    Ok((checks, expected))
}

fn cancel_checks_with_policy(
    store: &Store,
    task: &Task,
    boundary: u64,
    woken_enabled: bool,
) -> Result<(Vec<Check>, Option<Uuid>), String> {
    if task.checkpoint().phase != "replay-safe-async-wait" || task.replay_safe() {
        return Err("unsafe cancel requires a committed park".into());
    }
    let mut checks = park_checks(store, task, false, woken_enabled)?;
    let park_entry = store
        .get(&key(task.id()))
        .map_err(|e| e.to_string())?
        .ok_or("missing park proof")?;
    let proof: ParkProof =
        serde_json::from_slice(&park_entry.value).map_err(|_| "invalid park proof")?;
    if task.checkpoint().data.get("park_attempt")
        != Some(&serde_json::to_value(proof.attempt).map_err(|e| e.to_string())?)
    {
        return Err("park attempt does not match proof".into());
    }
    let mut joined_key = intent_key(task.id(), proof.attempt);
    joined_key.extend_from_slice(b"/park-evidence");
    let joined = store
        .get(&joined_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing joined park evidence")?;
    let evidence: UnsafeDurableParkEvidence =
        serde_json::from_slice(&joined.value).map_err(|_| "invalid joined park evidence")?;
    if evidence.task_id != task.id()
        || evidence.attempt_id != proof.attempt
        || proof.checkpoint.data.get("checkpoint") != Some(&evidence.checkpoint)
    {
        return Err("joined park evidence mismatch".into());
    }
    let same_wait = match (&proof.wait, &evidence.wait) {
        (WaitFor::Tasks(ids), DurableWait::All(calls))
        | (WaitFor::TasksAny(ids), DurableWait::Any(calls)) => {
            ids == &calls.iter().map(|c| c.task_id()).collect::<Vec<_>>()
                && calls.iter().all(|c| c.0.belongs_to(task.id()))
        }
        _ => false,
    };
    if !same_wait {
        return Err("joined park wait mismatch".into());
    }
    let prefix = format!("durable/unsafe-park-async/{}/", task.id()).into_bytes();
    let mut woken = None;
    for (key, entry) in store.list(&prefix).map_err(|e| e.to_string())? {
        if key.ends_with(b"/intent/receipt") || key.ends_with(b"/hook-final") {
            return Err("unsafe Tool receipt exists; reconciliation required".into());
        }
        if !key.ends_with(b"/intent") {
            continue;
        }
        let later: Task =
            serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe execution intent")?;
        if later.id() != task.id() {
            return Err("unsafe execution identity mismatch".into());
        }
        if woken_enabled
            && entry.version > park_entry.version
            && (later.checkpoint() != task.checkpoint() || later.ended_children().is_none())
        {
            return Err("later unsafe execution is not this park epoch".into());
        }
        if later.attempt_id() != Some(proof.attempt)
            && later.checkpoint() == task.checkpoint()
            && later.ended_children().is_some()
        {
            if !woken_enabled
                || entry.version <= park_entry.version
                || woken.is_some()
                || later.replay_safe()
                || later.owner() != task.owner()
                || later.kind() != task.kind()
                || later.definition_version() != task.definition_version()
                || later.input() != task.input()
                || later.state() != &TaskState::Running
                || later.abort_requested()
            {
                return Err("untrusted or ambiguous woken execution".into());
            }
            let attempt = later.attempt_id().ok_or("missing woken attempt")?;
            let actual = intent_key(task.id(), attempt);
            if key != actual {
                return Err("woken intent key mismatch".into());
            }
            checks.extend(park_checks(store, &later, true, true)?);
            for suffix in [
                b"/receipt".as_slice(),
                b"/result".as_slice(),
                b"/park-evidence".as_slice(),
            ] {
                let mut outcome = actual.clone();
                outcome.extend_from_slice(suffix);
                if store.get(&outcome).map_err(|e| e.to_string())?.is_some() {
                    return Err(
                        "woken execution has an external outcome; reconciliation required".into(),
                    );
                }
                checks.push(Check {
                    key: outcome,
                    expected: Expected::Missing,
                });
            }
            checks.push(Check {
                key: actual,
                expected: Expected::Version(entry.version),
            });
            woken = Some(attempt);
        }
    }
    if task.woken_interrupted_attempt().is_some() && task.woken_interrupted_attempt() != woken {
        return Err("interrupted woken attempt changed".into());
    }
    if woken.is_none() && (woken_enabled || task.ended_children().is_some()) {
        return Err("unbound wake; reconciliation required".into());
    }
    checks.push(Check {
        key: joined_key,
        expected: Expected::Version(joined.version),
    });
    checks.push(Check {
        key: prefix,
        expected: Expected::PrefixUnchangedSince(boundary),
    });
    Ok((checks, woken))
}

fn park_checks(
    store: &Store,
    task: &Task,
    require_wake: bool,
    allow_ended: bool,
) -> Result<Vec<Check>, String> {
    let park_key = key(task.id());
    let park_entry = store.get(&park_key).map_err(|e| e.to_string())?;
    if matches!(
        task.checkpoint().phase.as_str(),
        "initial" | "replay-safe-async-prepared"
    ) {
        if park_entry.is_some() {
            return Err("unexpected unsafe park proof".into());
        }
        // Initial/prepared may dispatch only once. A prior attempt's intent
        // means that an external effect may already have occurred, even if
        // the current checkpoint was restored or rewritten to an earlier phase.
        let boundary = store.put_version().map_err(|e| e.to_string())?;
        let prefix = format!("durable/unsafe-park-async/{}/", task.id()).into_bytes();
        if !store.list(&prefix).map_err(|e| e.to_string())?.is_empty() {
            return Err("prior unsafe execution evidence requires reconciliation".into());
        }
        return Ok(vec![
            Check {
                key: park_key,
                expected: Expected::Missing,
            },
            Check {
                key: prefix,
                expected: Expected::PrefixUnchangedSince(boundary),
            },
        ]);
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
    let wait_ids = match &proof.wait {
        WaitFor::Tasks(ids) | WaitFor::TasksAny(ids) if !ids.is_empty() => ids,
        _ => return Err("invalid unsafe wait".into()),
    };
    if require_wake {
        let ended = task
            .ended_children()
            .ok_or("unsafe wake lacks ended children")?;
        if ended.is_empty()
            || ended.iter().any(|id| !wait_ids.contains(id))
            || matches!(&proof.wait, WaitFor::Tasks(_)) && ended.len() != wait_ids.len()
        {
            return Err("unsafe wake does not match committed wait".into());
        }
    } else if !allow_ended && task.ended_children().is_some() {
        return Err("unsafe park epoch already woken".into());
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
    // Hooked attempts, and every resumed attempt, already durably stored the
    // prepared arguments before dispatch. The new park must preserve them;
    // comparing only the task and park proof would allow both to be replaced.
    match previous_task.checkpoint().phase.as_str() {
        "replay-safe-async-prepared" | "replay-safe-async-wait" => {
            let arguments = previous_task
                .checkpoint()
                .data
                .get("arguments")
                .ok_or("parked attempt lacks prepared arguments")?;
            if !arguments.is_object() || proof.checkpoint.data.get("arguments") != Some(arguments) {
                return Err(
                    "parked attempt prepared arguments changed; reconciliation required".into(),
                );
            }
        }
        "initial"
            if !previous_task
                .kind()
                .is_some_and(|kind| kind.starts_with("pi-rs.unsafe-park-async.v1.hook.")) => {}
        _ => return Err("invalid parked attempt checkpoint".into()),
    }
    let argument_check = if previous_task.checkpoint().phase == "initial" {
        let mut argument_key = previous_key.clone();
        argument_key.extend_from_slice(b"/prepared-arguments");
        let argument_entry = store.get(&argument_key).map_err(|e| e.to_string())?.ok_or(
            "unsafe first park lacks prepared arguments evidence; reconciliation required",
        )?;
        let arguments = proof
            .checkpoint
            .data
            .get("arguments")
            .ok_or("unsafe park lacks prepared arguments")?;
        if argument_entry.value != argument_digest(arguments)? {
            return Err(
                "parked attempt prepared arguments changed; reconciliation required".into(),
            );
        }
        Some(Check {
            key: argument_key,
            expected: Expected::Version(argument_entry.version),
        })
    } else {
        None
    };
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
    let mut checks = vec![
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
    ];
    checks.extend(argument_check);
    Ok(checks)
}

// An admission or before-phase settlement creates this only with an empty
// task evidence prefix and the task/checkpoint in the same checked commit.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NoDispatchProof {
    version: u32,
    task: Uuid,
    owner: crate::task::TaskOwner,
    kind: String,
    definition_version: u32,
    input_digest: Vec<u8>,
    checkpoint: Checkpoint,
    before_attempt: Option<Uuid>,
}
fn no_dispatch_key(task: Uuid) -> Vec<u8> {
    format!("durable/unsafe-park-async/{task}/no-dispatch").into_bytes()
}
fn task_evidence_prefix(task: Uuid) -> Vec<u8> {
    format!("durable/unsafe-park-async/{task}/").into_bytes()
}
fn host_error_outcome(checkpoint: &Checkpoint) -> Result<pi_rs_core::Outcome, String> {
    if checkpoint.phase != "replay-safe-async-after" {
        return Err("not a host error checkpoint".into());
    }
    match super::after_checkpoint_evidence(&checkpoint.data)? {
        super::ReplaySafeAfterEvidence::HostError { outcome, .. } => Ok(outcome),
        _ => Err("not a host error checkpoint".into()),
    }
}
pub(crate) fn prepare_no_dispatch_proof(
    store: &Store,
    task: &Task,
    checkpoint: &Checkpoint,
    admission: bool,
) -> Result<(Vec<Check>, Vec<Mutation>), String> {
    if task.replay_safe()
        || !task.is_task_tool()
        || !task
            .kind()
            .is_some_and(|k| k.starts_with("pi-rs.unsafe-park-async.v1.hook."))
        || if admission {
            task.state() != &TaskState::Ready
                || task.checkpoint() != checkpoint
                || task.attempt_id().is_some()
        } else {
            task.state() != &TaskState::Running
                || task.checkpoint().phase != "initial"
                || task.attempt_id().is_none()
        }
    {
        return Err("invalid no-dispatch proof origin".into());
    }
    host_error_outcome(checkpoint)?;
    let boundary = store.put_version().map_err(|e| e.to_string())?;
    let prefix = task_evidence_prefix(task.id());
    if !store.list(&prefix).map_err(|e| e.to_string())?.is_empty() {
        return Err("prior unsafe execution evidence requires reconciliation".into());
    }
    let proof = NoDispatchProof {
        version: 1,
        task: task.id(),
        owner: task.owner().clone(),
        kind: task.kind().ok_or("missing kind")?.into(),
        definition_version: task.definition_version(),
        input_digest: Sha256::digest(serde_json::to_vec(task.input()).map_err(|e| e.to_string())?)
            .to_vec(),
        checkpoint: checkpoint.clone(),
        before_attempt: if admission { None } else { task.attempt_id() },
    };
    let key = no_dispatch_key(task.id());
    Ok((
        vec![
            Check {
                key: prefix,
                expected: Expected::PrefixUnchangedSince(boundary),
            },
            Check {
                key: key.clone(),
                expected: Expected::Missing,
            },
        ],
        vec![Mutation::Put {
            key,
            value: serde_json::to_vec(&proof).map_err(|e| e.to_string())?,
        }],
    ))
}
fn no_dispatch_checks(store: &Store, task: &Task) -> Result<(NoDispatchProof, Vec<Check>), String> {
    let boundary = store.put_version().map_err(|e| e.to_string())?;
    let prefix = task_evidence_prefix(task.id());
    let records = store.list(&prefix).map_err(|e| e.to_string())?;
    let key = no_dispatch_key(task.id());
    let entry = records
        .iter()
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
        .ok_or("no-dispatch proof missing")?;
    let proof: NoDispatchProof =
        serde_json::from_slice(&entry.value).map_err(|_| "invalid no-dispatch proof")?;
    if proof.version != 1
        || proof.task != task.id()
        || &proof.owner != task.owner()
        || Some(proof.kind.as_str()) != task.kind()
        || proof.definition_version != task.definition_version()
        || proof.before_attempt.is_some_and(|attempt| attempt.is_nil())
        || proof.input_digest
            != Sha256::digest(serde_json::to_vec(task.input()).map_err(|e| e.to_string())?)
                .as_slice()
        || &proof.checkpoint != task.checkpoint()
        || task.replay_safe()
        || !task.is_task_tool()
        || records
            .iter()
            .any(|(k, _)| *k != key && *k != hook_final_key(task.id()))
    {
        return Err("no-dispatch evidence mismatch or prior intent".into());
    }
    host_error_outcome(task.checkpoint())?;
    if proof.before_attempt.is_none()
        && (!matches!(task.owner(), crate::task::TaskOwner::Conversation(_))
            || task.checkpoint() != &super::invalid_raw_checkpoint()?
            || task.input() != &serde_json::json!({}))
    {
        return Err("invalid no-dispatch admission origin".into());
    }
    if !task.is_terminal() && records.iter().any(|(k, _)| *k == hook_final_key(task.id())) {
        return Err("unsafe hook final exists before terminal settlement".into());
    }
    Ok((
        proof,
        vec![
            Check {
                key: prefix,
                expected: Expected::PrefixUnchangedSince(boundary),
            },
            Check {
                key,
                expected: Expected::Version(entry.version),
            },
        ],
    ))
}

/// Return a proved host failure only for the unsafe no-dispatch after phase.
pub(crate) fn no_dispatch_host_failure(
    store: &Store,
    task: &Task,
) -> Result<Option<pi_rs_core::Outcome>, String> {
    if task.checkpoint().phase != "replay-safe-async-after"
        || task
            .checkpoint()
            .data
            .get("host_error")
            .is_none_or(serde_json::Value::is_null)
    {
        return Ok(None);
    }
    no_dispatch_checks(store, task)?;
    host_error_outcome(task.checkpoint()).map(Some)
}

/// Bind a replayable after phase to the exact committed unsafe Complete and its joined receipt.
pub(crate) fn after_checks(store: &Store, task: &Task) -> Result<Vec<Check>, String> {
    if task.replay_safe() || task.checkpoint().phase != "replay-safe-async-after" {
        return Err("invalid unsafe after phase".into());
    }
    let checkpoint = &task.checkpoint().data;
    if checkpoint.get("host_error").is_some_and(|v| !v.is_null()) {
        return no_dispatch_checks(store, task).map(|(_, checks)| checks);
    }
    if checkpoint
        .get("version")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
        || !checkpoint
            .get("arguments")
            .is_some_and(serde_json::Value::is_object)
        || !checkpoint
            .get("host_error")
            .is_none_or(serde_json::Value::is_null)
    {
        return Err("invalid unsafe after checkpoint".into());
    }
    let outcome: pi_rs_core::Outcome = serde_json::from_value(
        checkpoint
            .get("outcome")
            .cloned()
            .ok_or("missing unsafe after outcome")?,
    )
    .map_err(|_| "invalid unsafe after outcome")?;
    if matches!(
        outcome,
        pi_rs_core::Outcome::Pending { .. } | pi_rs_core::Outcome::Unknown(_)
    ) {
        return Err("uncertain outcome cannot run after hook".into());
    }
    let boundary = store.put_version().map_err(|e| e.to_string())?;
    if !task.is_terminal()
        && store
            .get(&hook_final_key(task.id()))
            .map_err(|e| e.to_string())?
            .is_some()
    {
        return Err("unsafe hook final exists before terminal settlement".into());
    }
    let receipts = read_receipts(store, task.id())?;
    if receipts.len() != 1 || receipts[0].outcome != outcome {
        return Err("unsafe after receipt mismatch".into());
    }
    let intent_key = intent_key(task.id(), receipts[0].attempt_id);
    let intent = store
        .get(&intent_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing unsafe after intent")?;
    let original: Task =
        serde_json::from_slice(&intent.value).map_err(|_| "invalid unsafe after intent")?;
    if original.owner() != task.owner()
        || original.kind() != task.kind()
        || original.input() != task.input()
        || original.definition_version() != task.definition_version()
        || !matches!(
            original.checkpoint().phase.as_str(),
            "replay-safe-async-prepared" | "replay-safe-async-wait"
        )
        || original.checkpoint().data.get("arguments") != checkpoint.get("arguments")
    {
        return Err("unsafe after identity changed".into());
    }
    let mut receipt_key = intent_key.clone();
    receipt_key.extend_from_slice(b"/receipt");
    let receipt = store
        .get(&receipt_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing unsafe after receipt")?;
    let mut result_key = intent_key.clone();
    result_key.extend_from_slice(b"/result");
    let result = store
        .get(&result_key)
        .map_err(|e| e.to_string())?
        .ok_or("unsafe after result not committed")?;
    let step: DurableToolStep =
        serde_json::from_slice(&result.value).map_err(|_| "invalid unsafe after result")?;
    if !matches!(step, DurableToolStep::Complete(ref value) if *value == outcome) {
        return Err("unsafe after result mismatch".into());
    }
    Ok(vec![
        Check {
            key: format!("durable/unsafe-park-async/{}/", task.id()).into_bytes(),
            expected: Expected::PrefixUnchangedSince(boundary),
        },
        Check {
            key: intent_key,
            expected: Expected::Version(intent.version),
        },
        Check {
            key: receipt_key,
            expected: Expected::Version(receipt.version),
        },
        Check {
            key: result_key,
            expected: Expected::Version(result.version),
        },
    ])
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HookFinal {
    version: u32,
    task: Uuid,
    raw_attempt: Uuid,
    raw: pi_rs_core::Outcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_outcome: Option<pi_rs_core::Outcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_rich: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    final_attempt: Option<Uuid>,
}
fn hook_final_key(task: Uuid) -> Vec<u8> {
    format!("durable/unsafe-park-async/{task}/hook-final").into_bytes()
}

pub(crate) fn prepare_hook_phase(
    store: &Store,
    task: &Task,
    step: &crate::task_scheduler::Step,
) -> Result<(Vec<Check>, Vec<Mutation>), String> {
    let attempt = task.attempt_id().ok_or("missing hook attempt")?;
    let mut checks = vec![Check {
        key: intent_key(task.id(), attempt),
        expected: Expected::Missing,
    }];
    let mut mutations = Vec::new();
    match (task.checkpoint().phase.as_str(), step) {
        ("initial", crate::task_scheduler::Step::Continue(next))
            if next.phase == "replay-safe-async-prepared" => {}
        ("initial", crate::task_scheduler::Step::Continue(next))
            if next.phase == "replay-safe-async-after" =>
        {
            let (extra_checks, extra_mutations) =
                prepare_no_dispatch_proof(store, task, next, false)?;
            checks.extend(extra_checks);
            mutations.extend(extra_mutations);
        }
        ("replay-safe-async-after", crate::task_scheduler::Step::Succeed(value)) => {
            checks.extend(after_checks(store, task)?);
            let no_dispatch = task
                .checkpoint()
                .data
                .get("host_error")
                .is_some_and(|v| !v.is_null());
            let (raw_attempt, raw) = if no_dispatch {
                let (proof, _) = no_dispatch_checks(store, task)?;
                (
                    proof.before_attempt.unwrap_or(Uuid::nil()),
                    host_error_outcome(task.checkpoint())?,
                )
            } else {
                let receipt = read_receipts(store, task.id())?
                    .into_iter()
                    .next()
                    .ok_or("missing unsafe receipt")?;
                (receipt.attempt_id, receipt.outcome)
            };
            let rich = matches!(
                serde_json::from_value::<super::DurableTaskResult>(value.clone()),
                Ok(super::DurableTaskResult::Rich { .. })
            );
            if no_dispatch && !rich {
                return Err("no-dispatch outcome requires rich hooks".into());
            }
            let final_outcome = if rich {
                None
            } else {
                Some(
                    serde_json::from_value(value.clone())
                        .map_err(|_| "invalid hook final outcome")?,
                )
            };
            let record = HookFinal {
                version: if no_dispatch {
                    3
                } else if rich {
                    2
                } else {
                    1
                },
                task: task.id(),
                raw_attempt,
                raw,
                final_outcome,
                final_rich: rich.then(|| value.clone()),
                final_attempt: rich.then_some(attempt),
            };
            let key = hook_final_key(task.id());
            checks.push(Check {
                key: key.clone(),
                expected: Expected::Missing,
            });
            mutations.push(Mutation::Put {
                key,
                value: serde_json::to_vec(&record).map_err(|e| e.to_string())?,
            });
        }
        _ => return Err("unsupported unsafe hook settlement".into()),
    }
    Ok((checks, mutations))
}

/// Validate a terminal failed after phase before exposing its known raw
/// result as after-incomplete. A validated final marker instead indicates
/// that later owned-work failure made the overall result uncertain.
pub(crate) fn after_is_incomplete(
    store: &Store,
    task: &Task,
    raw: &pi_rs_core::Outcome,
) -> Result<bool, String> {
    if !matches!(task.state(), TaskState::Failed(_))
        || !task
            .kind()
            .is_some_and(|kind| kind.starts_with("pi-rs.unsafe-park-async.v1.hook."))
    {
        return Err("invalid unsafe after-incomplete state".into());
    }
    after_checks(store, task)?;
    let stored: pi_rs_core::Outcome = serde_json::from_value(
        task.checkpoint()
            .data
            .get("outcome")
            .cloned()
            .ok_or("missing after outcome")?,
    )
    .map_err(|_| "invalid after outcome")?;
    if &stored != raw {
        return Err("unsafe after-incomplete evidence mismatch".into());
    }
    if store
        .get(&hook_final_key(task.id()))
        .map_err(|e| e.to_string())?
        .is_some()
    {
        // The hook finalized before a later owned-child failure. The raw
        // outcome remains uncertain, but the hook is not incomplete.
        let entry = store
            .get(&hook_final_key(task.id()))
            .map_err(|e| e.to_string())?
            .ok_or("missing unsafe hook final")?;
        let record: HookFinal =
            serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe hook final")?;
        if matches!(record.version, 2 | 3) {
            let value = record
                .final_rich
                .as_ref()
                .ok_or("missing rich hook final")?;
            rich_hook_final_checks(store, task, value)?;
        } else {
            read_hook_final(store, task, raw)?;
        }
        Ok(false)
    } else {
        Ok(true)
    }
}

pub(crate) fn read_hook_final(
    store: &Store,
    task: &Task,
    raw: &pi_rs_core::Outcome,
) -> Result<pi_rs_core::Outcome, String> {
    let entry = store
        .get(&hook_final_key(task.id()))
        .map_err(|e| e.to_string())?
        .ok_or("missing unsafe hook final")?;
    let record: HookFinal =
        serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe hook final")?;
    let receipts = read_receipts(store, task.id())?;
    if record.version != 1
        || record.task != task.id()
        || &record.raw != raw
        || receipts.len() != 1
        || receipts[0].attempt_id != record.raw_attempt
        || receipts[0].outcome != record.raw
    {
        return Err("unsafe hook final mismatch".into());
    }
    after_checks(store, task)?;
    if record.final_rich.is_some() || record.final_attempt.is_some() {
        return Err("unexpected rich hook final".into());
    }
    record
        .final_outcome
        .ok_or_else(|| "missing hook final outcome".into())
}

/// Verify a rich final against its raw outcome and committed after phase.
pub(crate) fn rich_hook_final_checks(
    store: &Store,
    task: &Task,
    value: &serde_json::Value,
) -> Result<Vec<Check>, String> {
    let mut checks = after_checks(store, task)?;
    let entry = store
        .get(&hook_final_key(task.id()))
        .map_err(|e| e.to_string())?
        .ok_or("missing unsafe hook final")?;
    let record: HookFinal =
        serde_json::from_slice(&entry.value).map_err(|_| "invalid unsafe hook final")?;
    let receipts = read_receipts(store, task.id())?;
    let no_dispatch = record.version == 3;
    let valid_origin = if no_dispatch {
        let (proof, _) = no_dispatch_checks(store, task)?;
        let super::DurableTaskResult::Rich { task_content_v1 } =
            serde_json::from_value(value.clone()).map_err(|_| "invalid no-dispatch rich result")?
        else {
            return Err("missing no-dispatch rich result".into());
        };
        let pi_rs_core::Outcome::Failed(message) = host_error_outcome(task.checkpoint())? else {
            return Err("invalid no-dispatch host failure".into());
        };
        let code = task
            .checkpoint()
            .data
            .get("host_error")
            .and_then(serde_json::Value::as_str)
            .ok_or("missing host error code")?;
        let required = crate::runtime::ToolDiagnostic {
            severity: crate::runtime::DiagnosticSeverity::Error,
            code: Some(code.into()),
            message: super::replay_safe::bounded_diagnostic(&message),
        };
        super::replay_safe::validate_content_hook_result(&task_content_v1, Some(&required), false)?;
        receipts.is_empty()
            && record.raw_attempt == proof.before_attempt.unwrap_or(Uuid::nil())
            && record.raw == host_error_outcome(task.checkpoint())?
    } else {
        record.version == 2
            && receipts.len() == 1
            && receipts[0].attempt_id == record.raw_attempt
            && receipts[0].outcome == record.raw
    };
    if !valid_origin
        || record.task != task.id()
        || record.final_outcome.is_some()
        || record.final_rich.as_ref() != Some(value)
        || ((matches!(task.state(), TaskState::Succeeded(_))
            || (no_dispatch && matches!(task.state(), TaskState::Failed(_))))
            && record.final_attempt != task.retired_attempt_id())
        || record.final_attempt.is_none()
    {
        return Err("unsafe rich hook final mismatch".into());
    }
    let after_intent = intent_key(
        task.id(),
        record.final_attempt.ok_or("missing after attempt")?,
    );
    if store
        .get(&after_intent)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err("unsafe after attempt has tool intent".into());
    }
    checks.push(Check {
        key: after_intent,
        expected: Expected::Missing,
    });
    Ok(checks)
}

#[cfg(test)]
mod no_dispatch_tests {
    use super::*;

    #[test]
    fn initial_empty_prefix_check_fences_new_and_existing_intent() {
        let store = Store::memory().unwrap();
        let mut task = Task::new(
            Uuid::new_v4(),
            crate::task::TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::json!({}),
            },
            4,
        )
        .unwrap()
        .registered("pi-rs.unsafe-park-async.v1.hook.test".into(), false)
        .into_task_tool();
        task.start_attempt(Uuid::new_v4()).unwrap();
        let checks = dispatch_checks(&store, &task).unwrap();
        let prior = intent_key(task.id(), Uuid::new_v4());
        store
            .commit(
                &[Check {
                    key: prior.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: prior,
                    value: b"prior".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            store.commit(&checks, &[]),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(dispatch_checks(&store, &task).is_err());
    }

    #[test]
    fn prior_intent_racing_no_dispatch_commit_rejects_proof() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("db")).unwrap();
        let id = Uuid::new_v4();
        let mut task = Task::new(
            id,
            crate::task::TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::json!({}),
            },
            4,
        )
        .unwrap()
        .registered("pi-rs.unsafe-park-async.v1.hook.test".into(), false)
        .into_task_tool();
        task.start_attempt(Uuid::new_v4()).unwrap();
        let checkpoint = super::super::invalid_raw_checkpoint().unwrap();
        let (checks, mutations) =
            prepare_no_dispatch_proof(&store, &task, &checkpoint, false).unwrap();
        let prior = intent_key(id, Uuid::new_v4());
        store
            .commit(
                &[Check {
                    key: prior.clone(),
                    expected: Expected::Missing,
                }],
                &[Mutation::Put {
                    key: prior,
                    value: b"prior".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(
            store.commit(&checks, &mutations),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(store.get(&no_dispatch_key(id)).unwrap().is_none());
    }

    #[test]
    fn failed_task_validates_version_three_final_and_rejects_tampering() {
        let store = Store::memory().unwrap();
        let mut task = Task::new(
            Uuid::new_v4(),
            crate::task::TaskOwner::Conversation(Uuid::new_v4()),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::json!({}),
            },
            4,
        )
        .unwrap()
        .registered("pi-rs.unsafe-park-async.v1.hook.test".into(), false)
        .into_task_tool();
        task.start_attempt(Uuid::new_v4()).unwrap();
        let checkpoint = super::super::invalid_raw_checkpoint().unwrap();
        let raw = host_error_outcome(&checkpoint).unwrap();
        let (checks, mutations) =
            prepare_no_dispatch_proof(&store, &task, &checkpoint, false).unwrap();
        store.commit(&checks, &mutations).unwrap();
        task.advance(checkpoint).unwrap();
        task.yield_ready().unwrap();
        task.start_attempt(Uuid::new_v4()).unwrap();
        let pi_rs_core::Outcome::Failed(message) = &raw else {
            panic!("expected host failure")
        };
        let rich = serde_json::json!({"task_content_v1": {"output":[{"type":"text","text":"redacted"}], "isError":true, "diagnostics":[{"severity":"error","code":"invalid_arguments","message":message}]}});
        let (checks, mutations) = prepare_hook_phase(
            &store,
            &task,
            &crate::task_scheduler::Step::Succeed(rich.clone()),
        )
        .unwrap();
        store.commit(&checks, &mutations).unwrap();
        let running = task.clone();
        task.hold_success(rich).unwrap();
        task.record_completed_attempt(&running).unwrap();
        task.finalize_with_unsafe_reconciliation(Uuid::new_v4())
            .unwrap();
        assert!(!after_is_incomplete(&store, &task, &raw).unwrap());
        let key = hook_final_key(task.id());
        let entry = store.get(&key).unwrap().unwrap();
        let mut record: HookFinal = serde_json::from_slice(&entry.value).unwrap();
        let original_attempt = record.final_attempt;
        record.final_attempt = Some(Uuid::new_v4());
        store
            .commit(
                &[Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                }],
                &[Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&record).unwrap(),
                }],
            )
            .unwrap();
        assert!(after_is_incomplete(&store, &task, &raw).is_err());
        let entry = store.get(&key).unwrap().unwrap();
        record.final_attempt = original_attempt;
        record.final_rich = Some(serde_json::json!({"invalid":true}));
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
        assert!(after_is_incomplete(&store, &task, &raw).is_err());
    }
}
