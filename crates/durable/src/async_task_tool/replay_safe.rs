//! Standalone checkpointable tools with an explicitly registered replay policy.
use super::{
    AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableParkCancel, DurableTaskHost,
    DurableTaskResult, DurableToolResume, DurableToolStep, DurableWait, ToolIdentity,
};
use crate::task::{Checkpoint, Task, WaitFor};
use crate::task_scheduler::{AdmissionHandle, Handler, Scheduler, Step};
use crate::task_tool::{NestedTaskResult, TaskTool};
use pi_rs_core::{Outcome, Tool, ToolContext};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[cfg(test)]
type ParkHooks = Mutex<HashMap<uuid::Uuid, Box<dyn FnOnce() + Send>>>;
#[cfg(test)]
fn park_hooks() -> &'static ParkHooks {
    static HOOKS: std::sync::OnceLock<ParkHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
fn cancel_dispatch_hooks() -> &'static ParkHooks {
    static HOOKS: std::sync::OnceLock<ParkHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(super) fn before_woken_cancel_dispatch(id: uuid::Uuid, hook: impl FnOnce() + Send + 'static) {
    cancel_dispatch_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}
#[cfg(test)]
pub(super) fn after_unsafe_evidence(id: uuid::Uuid, hook: impl FnOnce() + Send + 'static) {
    park_hooks().lock().unwrap().insert(id, Box::new(hook));
}

#[cfg(test)]
fn restart_receipt_hooks() -> &'static ParkHooks {
    static HOOKS: std::sync::OnceLock<ParkHooks> = std::sync::OnceLock::new();
    HOOKS.get_or_init(Default::default)
}
#[cfg(test)]
pub(super) fn after_restart_receipt(id: uuid::Uuid, hook: impl FnOnce() + Send + 'static) {
    restart_receipt_hooks()
        .lock()
        .unwrap()
        .insert(id, Box::new(hook));
}

#[cfg(test)]
fn crash_test_boundary(tool: &str, task: uuid::Uuid, stage: &str) {
    if tool != "root" || std::env::var("PI_TEST_REPLAY_SAFE_CUT").ok().as_deref() != Some(stage) {
        return;
    }
    if let Some(dir) = std::env::var_os("PI_TEST_REPLAY_SAFE_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::write(dir.join("cut.tmp"), task.to_string()).unwrap();
        std::fs::rename(dir.join("cut.tmp"), dir.join("cut")).unwrap();
        loop {
            std::thread::park();
        }
    }
}

/// Attests that this tool has no non-idempotent effects, including during
/// argument preparation, and may be re-executed after an interrupted attempt.
/// The runtime cannot verify this assertion or join work spawned by the tool.
pub struct ReplaySafeAuthorization(());
impl ReplaySafeAuthorization {
    pub fn host_attests_replay_safe() -> Self {
        Self(())
    }
}

/// Explicitly accepts reconciliation after any interrupted running attempt.
/// A committed park is recoverable; external effects and parked cancellation
/// are the host's responsibility.
pub struct UnsafeDurableAuthorization(());
impl UnsafeDurableAuthorization {
    pub fn interruption_requires_reconciliation() -> Self {
        Self(())
    }
}

/// Host attestation that abort hooks finish cooperatively, have no detached work,
/// and tolerate replay across process loss. The runtime cannot verify this.
pub struct AbortAfterAuthorization(());
impl AbortAfterAuthorization {
    pub fn host_attests_cleanup_safe() -> Self {
        Self(())
    }
}

/// The host attests that the restart-limit hook is replay-safe, cooperative, and starts no detached work.
pub struct RestartAfterAuthorization(());
impl RestartAfterAuthorization {
    pub fn host_attests_cleanup_safe() -> Self {
        Self(())
    }
}

#[allow(clippy::too_many_arguments)] // Registration and both explicit hook policies are bound into identity.
pub(super) fn register(
    scheduler: &mut Scheduler,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<TaskTool>>,
    hook: Option<(String, Arc<dyn crate::extension::Hook>)>,
    block_results: bool,
    abort_after: bool,
    restart_after: bool,
    unavailable: bool,
    park_cancel: Option<(String, Arc<dyn DurableParkCancel>, bool)>,
) -> Result<Arc<TaskTool>, String> {
    register_with_policy(
        scheduler,
        tool,
        implementation,
        version,
        children,
        true,
        hook,
        block_results,
        abort_after,
        restart_after,
        unavailable,
        park_cancel.map(|(id, callback, retry)| (id, callback, retry, false)),
    )
}

pub(super) fn register_unsafe(
    scheduler: &mut Scheduler,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<TaskTool>>,
    hook: Option<(String, Arc<dyn crate::extension::Hook>)>,
) -> Result<Arc<TaskTool>, String> {
    register_with_policy(
        scheduler,
        tool,
        implementation,
        version,
        children,
        false,
        hook,
        false,
        false,
        false,
        false,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn register_unsafe_park_cancel(
    scheduler: &mut Scheduler,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<TaskTool>>,
    hook: (String, Arc<dyn crate::extension::Hook>),
    park_cancel: (String, Arc<dyn DurableParkCancel>, bool),
    woken: bool,
) -> Result<Arc<TaskTool>, String> {
    register_with_policy(
        scheduler,
        tool,
        implementation,
        version,
        children,
        false,
        Some(hook),
        true,
        true,
        false,
        false,
        Some((park_cancel.0, park_cancel.1, park_cancel.2, woken)),
    )
}

#[allow(clippy::too_many_arguments)] // Explicit policy and hook identity are part of registration.
fn register_with_policy(
    scheduler: &mut Scheduler,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<TaskTool>>,
    replay_safe: bool,
    hook: Option<(String, Arc<dyn crate::extension::Hook>)>,
    block_results: bool,
    abort_after: bool,
    restart_after: bool,
    unavailable: bool,
    park_cancel: Option<(String, Arc<dyn DurableParkCancel>, bool, bool)>,
) -> Result<Arc<TaskTool>, String> {
    let identity_tool: Arc<dyn Tool> = tool.clone();
    let definition_lease = if park_cancel.as_ref().is_some_and(|(_, _, _, woken)| *woken) {
        Some(
            crate::nested_identity::execution_lease(&identity_tool)
                .map_err(|_| "woken cancel identity busy")?
                .ok_or("woken cancel requires a registered identity lease")?,
        )
    } else {
        None
    };
    let woken_identity = match definition_lease.as_ref() {
        Some(guard) => Some(
            guard
                .as_deref()
                .ok_or("woken cancel identity missing")?
                .to_owned(),
        ),
        None => None,
    };
    if unavailable && (!block_results || abort_after || restart_after || !children.is_empty()) {
        return Err("invalid unavailable registration".into());
    }
    if (abort_after || restart_after) && !block_results {
        return Err("abort hooks require content hooks".into());
    }
    if block_results && hook.is_none() {
        return Err("content hooks require a registration".into());
    }
    let mut identity = ToolIdentity::new(tool.as_ref(), implementation, version)?;
    if let Some((id, _)) = &hook {
        identity = identity.with_before_hook(id.clone())?.with_after_hook();
    }
    jsonschema::validator_for(&tool.spec().parameters).map_err(|_| "invalid tool schema")?;
    let mut names = HashMap::new();
    let mut child_identities = Vec::new();
    for child in children {
        let id = child.identity().ok_or("invalid child identity")?;
        if child.is_direct()
            || (!id.replay_safe && !scheduler.registered_unsafe_checkpointable_child(&child))
        {
            return Err(
                "child must be replay-safe or a registered unsafe checkpointable tool".into(),
            );
        }
        let name = child.spec().name;
        if name.is_empty() || names.insert(name.clone(), child).is_some() {
            return Err("duplicate or empty child name".into());
        }
        child_identities.push((name, id));
    }
    child_identities.sort_by(|a, b| a.0.cmp(&b.0));
    let discriminator = if unavailable {
        7u32
    } else if restart_after {
        6u32
    } else if abort_after {
        5u32
    } else if block_results {
        4u32
    } else {
        1u32
    };
    let encoded = if let Some((cancel_id, _, retry, woken)) = &park_cancel {
        // 8 names the legacy at-most-once contract without park provenance;
        // 9 and 10 bind the retryable and provenance-required contracts.
        #[cfg(test)]
        let legacy_fixture = std::env::var_os("PI_TEST_LEGACY_PARK_CANCEL").is_some();
        #[cfg(not(test))]
        let legacy_fixture = false;
        serde_json::to_vec(&(
            if *woken && *retry {
                16u32
            } else if *woken {
                15u32
            } else if !replay_safe && *retry {
                12u32
            } else if !replay_safe {
                11u32
            } else if *retry {
                9u32
            } else if legacy_fixture {
                8u32
            } else {
                10u32
            },
            &identity,
            &child_identities,
            cancel_id,
            // Preserve older identities for all other registrations.
        ))
    } else {
        serde_json::to_vec(&(discriminator, &identity, &child_identities))
    }
    .map_err(|e| e.to_string())?;
    let encoded = if let Some(expected) = &woken_identity {
        serde_json::to_vec(&(encoded, expected)).map_err(|e| e.to_string())?
    } else {
        encoded
    };
    if encoded.len() > 65536 {
        return Err("definition identity too large".into());
    }
    let hash = Sha256::digest(encoded);
    let kind = if replay_safe {
        format!("pi-rs.replay-safe-async.v1.{hash:x}")
    } else if hook.is_some() {
        if park_cancel.as_ref().is_some_and(|(_, _, _, woken)| *woken) {
            format!("pi-rs.unsafe-park-async.v1.hook.woken.{hash:x}")
        } else {
            format!("pi-rs.unsafe-park-async.v1.hook.{hash:x}")
        }
    } else {
        format!("pi-rs.unsafe-park-async.v1.{hash:x}")
    };
    let spec = tool.spec();
    let has_hook = hook.is_some();
    let handler: Arc<dyn Handler> = Arc::new(Executor {
        tool,
        identity,
        implementation: implementation.into(),
        version,
        children: names,
        hook: hook.map(|(_, hook)| hook),
        replay_safe,
        block_results,
        unavailable,
        park_cancel: park_cancel.map(|(_, callback, retry, _)| (callback, retry)),
        _conversation_cancel: None,
        conversation_identity: None,
        woken_identity,
    });
    scheduler.install_checkpointable_async_handler(
        kind.clone(),
        version,
        replay_safe,
        handler.clone(),
        abort_after,
        restart_after,
        has_hook,
    )?;
    Ok(Arc::new(
        TaskTool::new(spec, kind, replay_safe, version, handler).with_deferred_arguments(),
    ))
}

/// Build a replay-safe definition before opening a conversation harness.
/// Selected children must share the same stable and live hook registration.
#[allow(clippy::too_many_arguments)]
pub(super) fn register_conversation_candidate(
    registry: &mut crate::task_scheduler::Registry,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<TaskTool>>,
    selected: Option<(
        crate::extension::StableExtensionSelection,
        crate::extension::RegistrationEvidence,
        Arc<dyn crate::extension::Hook>,
    )>,
    replay_safe: bool,
    conversation_cancel: Option<(String, Arc<dyn DurableParkCancel>, bool)>,
) -> Result<Arc<TaskTool>, String> {
    let identity_tool: Arc<dyn Tool> = tool.clone();
    let identity_guard = if conversation_cancel.is_some() {
        Some(
            crate::nested_identity::execution_lease(&identity_tool)
                .map_err(|_| "conversation cancel identity busy")?
                .ok_or("conversation cancel requires a registered identity lease")?,
        )
    } else {
        None
    };
    let conversation_identity = match identity_guard.as_ref() {
        Some(guard) => Some(
            guard
                .as_deref()
                .ok_or("conversation cancel identity missing")?
                .to_owned(),
        ),
        None => None,
    };
    let mut identity = ToolIdentity::new(tool.as_ref(), implementation, version)?;
    if let Some((selection, _, _)) = &selected {
        let hook_id = format!(
            "selection-v1:{}",
            serde_json::to_string(selection).map_err(|e| e.to_string())?
        );
        identity = identity.with_before_hook(hook_id)?.with_after_hook();
    }
    jsonschema::validator_for(&tool.spec().parameters).map_err(|_| "invalid tool schema")?;
    let mut names = HashMap::new();
    let mut child_identities = Vec::new();
    for child in children {
        let id = child.identity().ok_or("invalid child identity")?;
        if child.is_direct()
            || ((!id.replay_safe || !replay_safe)
                && !registry.matches_conversation_checkpointable(&id, &child.handler()))
            || !registry.matches_task_tool(&id, &child.handler())
        {
            return Err("child must be a registered replay-safe task tool".into());
        }
        let expected_selection = selected.as_ref().map(|(selection, _, _)| selection);
        let expected_registration = selected.as_ref().map(|(_, registration, _)| registration);
        if registry.conversation_hook_selection(&id) != expected_selection
            || child.conversation_registration() != expected_registration
        {
            return Err("child hook selection must match parent".into());
        }
        let name = child.spec().name;
        if name.is_empty() || names.insert(name.clone(), child).is_some() {
            return Err("duplicate or empty child name".into());
        }
        child_identities.push((name, id));
    }
    child_identities.sort_by(|a, b| a.0.cmp(&b.0));
    let encoded = serde_json::to_vec(&(
        if selected.is_some() { 4u32 } else { 1u32 },
        &identity,
        &child_identities,
    ))
    .map_err(|e| e.to_string())?;
    let encoded = if let Some((cancel_identity, _, enabled)) = &conversation_cancel {
        serde_json::to_vec(&(
            if *enabled {
                "conversation-cancel-dispatch-v1"
            } else {
                "conversation-cancel-v2"
            },
            encoded,
            cancel_identity,
            &conversation_identity,
        ))
        .map_err(|e| e.to_string())?
    } else {
        encoded
    };
    if encoded.len() > 65536 {
        return Err("definition identity too large".into());
    }
    let prefix = if replay_safe {
        "pi-rs.replay-safe-async.v1."
    } else if selected.is_some() {
        "pi-rs.unsafe-park-async.v1.hook."
    } else {
        "pi-rs.unsafe-park-async.v1."
    };
    let kind = format!("{prefix}{:x}", Sha256::digest(encoded));
    let spec = tool.spec();
    if spec != identity.spec {
        return Err("tool specification changed during registration".into());
    }
    let validate_tool = tool.clone();
    let validate_identity = identity.clone();
    let validate_implementation = implementation.to_owned();
    let validate = Arc::new(move || {
        validate_identity.matches(validate_tool.as_ref(), &validate_implementation, version)
    });
    let (selection, registration, hook) = match selected {
        Some((selection, registration, hook)) => (Some(selection), Some(registration), Some(hook)),
        None => (None, None, None),
    };
    let has_hook = hook.is_some();
    let has_conversation_cancel = conversation_cancel.is_some();
    let handler: Arc<dyn Handler> = Arc::new(Executor {
        tool,
        identity,
        implementation: implementation.into(),
        version,
        children: names,
        hook,
        replay_safe,
        block_results: has_hook,
        unavailable: false,
        park_cancel: None,
        _conversation_cancel: conversation_cancel,
        conversation_identity,
        woken_identity: None,
    });
    let mut adapter =
        TaskTool::new(spec, kind, replay_safe, version, handler).with_deferred_arguments();
    if has_hook {
        adapter = adapter.with_conversation_hooks(
            selection.clone().ok_or("missing selection")?,
            registration.ok_or("missing registration")?,
        );
    }
    let adapter = Arc::new(adapter);
    registry.register_conversation_checkpointable(&adapter, validate, selection)?;
    if has_conversation_cancel {
        registry.enable_conversation_claim_handler(&adapter)?;
    }
    Ok(adapter)
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ParkCheckpoint {
    version: u32,
    arguments: serde_json::Value,
    checkpoint: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    park_attempt: Option<uuid::Uuid>,
}

pub(crate) fn park_cancel_provenance_checks(
    store: &durable_store::Store,
    task: &Task,
    graph_version: u64,
) -> Result<Vec<durable_store::Check>, String> {
    if !task.replay_safe() {
        return super::unsafe_park_proof::cancel_checks(store, task, graph_version);
    }
    if task.ended_children().is_some() {
        return Err("park epoch was already woken".into());
    }
    let parked: ParkCheckpoint = serde_json::from_value(task.checkpoint().data.clone())
        .map_err(|_| "invalid park provenance")?;
    let attempt = parked
        .park_attempt
        .filter(|id| !id.is_nil())
        .ok_or("unproven legacy park")?;
    if parked.version != 2 {
        return Err("unsupported park provenance version".into());
    }
    let intent_key =
        format!("durable/replay-safe-async/{}/{attempt}/intent", task.id()).into_bytes();
    let mut result_key = intent_key.clone();
    result_key.extend_from_slice(b"/result");
    let intent_entry = store
        .get(&intent_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing park intent")?;
    let result_entry = store
        .get(&result_key)
        .map_err(|e| e.to_string())?
        .ok_or("missing park result")?;
    let intent: Task = serde_json::from_slice(&intent_entry.value).map_err(|e| e.to_string())?;
    let result: DurableToolStep =
        serde_json::from_slice(&result_entry.value).map_err(|e| e.to_string())?;
    if intent.id() != task.id()
        || intent.kind() != task.kind()
        || intent.definition_version() != task.definition_version()
        || intent.owner() != task.owner()
        || intent.input() != task.input()
        || intent.attempt_id() != Some(attempt)
        || intent.state() != &crate::task::TaskState::Running
        || intent.abort_requested()
        || !matches!(
            intent.checkpoint().phase.as_str(),
            "replay-safe-async-prepared" | "replay-safe-async-wait"
        )
        || intent.checkpoint().data.get("arguments") != Some(&parked.arguments)
        || !matches!(result, DurableToolStep::Park { checkpoint, .. } if checkpoint == parked.checkpoint)
    {
        return Err("park intent or result mismatch".into());
    }
    let prefix = format!("durable/replay-safe-async/{}/", task.id()).into_bytes();
    for (key, entry) in store.list(&prefix).map_err(|e| e.to_string())? {
        if !key.ends_with(b"/intent") {
            continue;
        }
        let later: Task =
            serde_json::from_slice(&entry.value).map_err(|_| "invalid execution intent")?;
        if later.id() != task.id() {
            return Err("execution intent identity mismatch".into());
        }
        if later.attempt_id() != Some(attempt)
            && later.checkpoint() == task.checkpoint()
            && later.ended_children().is_some()
        {
            return Err("park epoch has a woken execution intent".into());
        }
    }
    Ok(vec![
        durable_store::Check {
            key: intent_key,
            expected: durable_store::Expected::Version(intent_entry.version),
        },
        durable_store::Check {
            key: result_key,
            expected: durable_store::Expected::Version(result_entry.version),
        },
        durable_store::Check {
            key: prefix,
            expected: durable_store::Expected::PrefixUnchangedSince(graph_version),
        },
    ])
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HookCheckpoint {
    version: u32,
    arguments: serde_json::Value,
    outcome: Option<Outcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    host_error: Option<String>,
}
pub(crate) fn invalid_raw_checkpoint() -> Result<Checkpoint, String> {
    let data = serde_json::to_value(HookCheckpoint {
        version: 1,
        arguments: serde_json::json!({}),
        outcome: Some(Outcome::Failed(
            "Invalid tool arguments: invalid checkpointable arguments".into(),
        )),
        host_error: Some("invalid_arguments".into()),
    })
    .map_err(|error| error.to_string())?;
    Ok(Checkpoint {
        phase: "replay-safe-async-after".into(),
        data,
    })
}
pub(crate) fn after_checkpoint_evidence(
    data: &serde_json::Value,
) -> Result<super::ReplaySafeAfterEvidence, String> {
    if serde_json::to_vec(data).map_err(|e| e.to_string())?.len() > 65536 {
        return Err("after checkpoint exceeds limit".into());
    }
    let checkpoint: HookCheckpoint =
        serde_json::from_value(data.clone()).map_err(|_| "invalid after checkpoint")?;
    if checkpoint.version != 1
        || !checkpoint.arguments.is_object()
        || serde_json::to_vec(&checkpoint.arguments)
            .map_err(|e| e.to_string())?
            .len()
            > 60 * 1024
    {
        return Err("invalid after checkpoint".into());
    }
    let outcome = checkpoint.outcome.ok_or("missing after outcome")?;
    if matches!(outcome, Outcome::Unknown(_) | Outcome::Pending { .. }) {
        return Err("uncertain after outcome".into());
    }
    match checkpoint.host_error {
        Some(code)
            if matches!(
                code.as_str(),
                "blocked" | "invalid_arguments" | "invalid_park" | "tool_unavailable"
            ) && matches!(outcome, Outcome::Failed(_)) =>
        {
            Ok(super::ReplaySafeAfterEvidence::HostError { code, outcome })
        }
        None => Ok(super::ReplaySafeAfterEvidence::ToolOutcome(outcome)),
        _ => Err("invalid host error checkpoint".into()),
    }
}

use crate::task_scheduler::HookFenceError;

struct HookMemo<'a, 'b> {
    admission: &'a AdmissionHandle<'b>,
    cancellation: &'a pi_rs_core::CancellationToken,
    failed: Mutex<Option<HookFenceError>>,
    signal_check: Mutex<Option<durable_store::Check>>,
}
impl HookMemo<'_, '_> {
    fn access(
        &self,
        key: &str,
        value: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, HookFenceError> {
        // Serialize access with failure publication: a concurrent operation
        // cannot commit after another operation has failed.
        let mut failed = self.failed.lock().map_err(|_| "hook memo unavailable")?;
        if failed.is_some() {
            return Err("hook memo unavailable after failure".into());
        }
        let inserting = value.is_some();
        let signal_check = self
            .signal_check
            .lock()
            .map(|check| check.clone())
            .map_err(|_| HookFenceError::from("hook signal fence unavailable"));
        let result = signal_check
            .and_then(|check| {
                self.admission
                    .replay_safe_hook_memo(key, value, self.cancellation, check.as_ref())
            })
            .and_then(|value| {
                if inserting && value.is_none() {
                    Err("memo insertion failed".into())
                } else {
                    Ok(value)
                }
            });
        if let Err(error) = &result {
            *failed = Some(error.clone());
        }
        result
    }
    fn failure(&self) -> Option<HookFenceError> {
        self.failed.lock().map_or_else(
            |_| Some("hook memo unavailable".into()),
            |failed| failed.clone(),
        )
    }
    fn stage_signal_check(
        &self,
        docs: &mut crate::document::TaskDocuments<'_>,
    ) -> Result<(), String> {
        if let Some(check) = self
            .signal_check
            .lock()
            .map_err(|_| "hook signal fence unavailable")?
            .clone()
        {
            docs.stage_hook_signal_check(check)?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl crate::extension::Memo for HookMemo<'_, '_> {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, String> {
        self.access(key, None).map_err(|error| error.to_string())
    }
    async fn get_or_insert(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.access(key, Some(value))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "memo insertion failed".into())
    }
    async fn read_signal(
        &self,
        key: &str,
    ) -> Result<Option<crate::task_store::SignalState>, String> {
        let mut failed = self.failed.lock().map_err(|_| "hook memo unavailable")?;
        if failed.is_some() {
            return Err("hook memo unavailable after failure".into());
        }
        let result: Result<_, HookFenceError> = self
            .admission
            .replay_safe_hook_signal_state(key, self.cancellation)
            .and_then(|(state, check)| {
                let mut saved = self
                    .signal_check
                    .lock()
                    .map_err(|_| "hook signal fence unavailable")?;
                if saved.as_ref().is_some_and(|old| old != &check) {
                    return Err(crate::task_scheduler::UNSAFE_ANCESTOR_BUSY.into());
                }
                *saved = Some(check);
                Ok(state)
            });
        if let Err(error) = &result {
            *failed = Some(error.clone());
        }
        result.map_err(|error| error.to_string())
    }
}

fn validate_content_result_with_limit(
    result: &crate::extension::ToolBlockResult,
    limit: usize,
) -> Result<(), String> {
    if crate::content::UserContent::from_yield(
        serde_json::Value::Array(result.output.clone()),
        false,
    )
    .is_none()
    {
        return Err("invalid content blocks".into());
    }
    for block in &result.output {
        if block.get("type").and_then(serde_json::Value::as_str) == Some("image") {
            let image = pi_rs_core::ImageContent {
                data: block
                    .get("data")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("invalid image data")?
                    .to_owned(),
                mime_type: block
                    .get("mimeType")
                    .and_then(serde_json::Value::as_str)
                    .ok_or("invalid image type")?
                    .to_owned(),
            };
            image.validate().map_err(|e| e.to_string())?;
        }
    }
    if result.extra.keys().any(|key| {
        !matches!(
            key.as_str(),
            "isError" | "details" | "structuredOutput" | "attachments"
        )
    }) || !result
        .extra
        .get("isError")
        .is_some_and(serde_json::Value::is_boolean)
        || result.extra.get("attachments").is_some_and(|v| {
            serde_json::from_value::<Vec<pi_rs_core::AttachmentRef>>(v.clone()).is_err()
        })
        || result.diagnostics.len() > 32
        || result
            .diagnostics
            .iter()
            .any(|d| d.message.len() > 4096 || d.code.as_ref().is_some_and(|v| v.len() > 256))
    {
        return Err("invalid rich result metadata".into());
    }
    if serde_json::to_vec(result).map_err(|e| e.to_string())?.len() > limit {
        return Err("rich result exceeds size limit".into());
    }
    Ok(())
}

fn validate_content_result(result: &crate::extension::ToolBlockResult) -> Result<(), String> {
    validate_content_result_with_limit(result, 60 * 1024)
}

pub(super) fn bounded_diagnostic(message: &str) -> String {
    let mut end = message.len().min(4096);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

pub(crate) fn validate_content_hook_result(
    result: &crate::extension::ToolBlockResult,
    required: Option<&crate::runtime::ToolDiagnostic>,
    initial: bool,
) -> Result<(), String> {
    // The trusted initial projection may duplicate and JSON-escape a bounded raw
    // checkpoint. Allow it to reach the first hook; replacements remain subject
    // to the final result bound before any later hook can observe them.
    validate_content_result_with_limit(result, if initial { 4 * 65536 } else { 60 * 1024 })?;
    if !initial
        && serde_json::to_vec(&DurableTaskResult::Rich {
            task_content_v1: result.clone(),
        })
        .map_err(|e| e.to_string())?
        .len()
            > 60 * 1024
    {
        return Err("rich result exceeds size limit".into());
    }
    if let Some(gate) = required
        && (result.extra.get("isError") != Some(&serde_json::Value::Bool(true))
            || !result.diagnostics.contains(gate)
            || (matches!(
                gate.code.as_deref(),
                Some("aborted" | "restart_limit" | "tool_unavailable" | "interrupted")
            ) && (result.extra.contains_key("structuredOutput")
                || result.extra.contains_key("attachments"))))
    {
        return Err("host error metadata cannot be suppressed".into());
    }
    Ok(())
}

struct AbortMemo(std::sync::atomic::AtomicBool);
#[async_trait::async_trait]
impl crate::extension::Memo for AbortMemo {
    async fn get(&self, _: &str) -> Result<Option<serde_json::Value>, String> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("memo unavailable during abort cleanup".into())
    }
    async fn get_or_insert(
        &self,
        _: &str,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.0.store(true, std::sync::atomic::Ordering::Release);
        Err("memo unavailable during abort cleanup".into())
    }
}

struct Executor {
    tool: Arc<dyn DurableAsyncTool>,
    identity: ToolIdentity,
    implementation: String,
    version: u32,
    children: HashMap<String, Arc<TaskTool>>,
    hook: Option<Arc<dyn crate::extension::Hook>>,
    replay_safe: bool,
    block_results: bool,
    unavailable: bool,
    park_cancel: Option<(Arc<dyn DurableParkCancel>, bool)>,
    // Registered and identity-bound, but deliberately not dispatchable yet.
    _conversation_cancel: Option<(String, Arc<dyn DurableParkCancel>, bool)>,
    conversation_identity: Option<String>,
    woken_identity: Option<String>,
}

struct Host<'a, 'b> {
    task: Task,
    docs: Mutex<&'a mut crate::document::TaskDocuments<'b>>,
    children: &'a HashMap<String, Arc<TaskTool>>,
    started: Mutex<HashSet<uuid::Uuid>>,
}

#[async_trait::async_trait]
impl DurableTaskHost for Host<'_, '_> {
    async fn start_child(
        &self,
        key: &str,
        tool: &str,
        input: serde_json::Value,
    ) -> Result<AsyncTaskCall, String> {
        let child = self.children.get(tool).ok_or("child is not registered")?;
        let mut docs = self.docs.lock().map_err(|e| e.to_string())?;
        let call = child.start_child(&self.task, &mut docs, key, input)?;
        self.started
            .lock()
            .map_err(|e| e.to_string())?
            .insert(call.task_id());
        Ok(AsyncTaskCall(call))
    }
    async fn child_result(&self, child: &AsyncTaskCall) -> Result<Option<AsyncTaskResult>, String> {
        let mut docs = self.docs.lock().map_err(|e| e.to_string())?;
        if !child.0.belongs_to(self.task.id()) {
            return Err("child belongs to another parent".into());
        }
        let task = docs.get_owned_child(child.task_id())?;
        if !task.replay_safe()
            && task
                .kind()
                .is_some_and(|kind| kind.starts_with("pi-rs.unsafe-park-async.v1."))
        {
            use crate::task::TaskState;
            if !task.is_terminal() {
                return Ok(None);
            }
            let (receipts, has_intent) = docs.read_unsafe_child_evidence(child.task_id())?;
            let outcome = receipts.into_iter().next().map(|receipt| receipt.outcome);
            return Ok(Some(match task.state() {
                TaskState::Succeeded(value) if value.get("task_content_v1").is_some() => {
                    let result: DurableTaskResult = serde_json::from_value(value.clone()).map_err(|_| "invalid unsafe rich child result")?;
                    let DurableTaskResult::Rich { task_content_v1 } = result else { return Err("missing unsafe rich child result".into()); };
                    validate_content_result(&task_content_v1)?;
                    docs.verify_unsafe_rich_hook_final(&task, value)?;
                    AsyncTaskResult::Rich(task_content_v1)
                }
                TaskState::Succeeded(value) => {
                    let result: Outcome = serde_json::from_value(value.clone()).map_err(|_| "invalid unsafe child result")?;
                    if outcome.is_none() && docs.unsafe_child_no_dispatch(&task, &result)? {
                        return Ok(Some(AsyncTaskResult::Outcome(result)));
                    }
                    let raw = outcome.as_ref().ok_or("unsafe child result lacks receipt")?;
                    let expected = if task.checkpoint().phase == "replay-safe-async-after" { docs.read_unsafe_hook_final(&task, raw)? } else { raw.clone() };
                    if expected != result { return Err("unsafe child result lacks matching final evidence".into()); }
                    AsyncTaskResult::Outcome(result)
                }
                TaskState::Failed(_) if task.checkpoint().phase == "replay-safe-async-after" => {
                    let raw = match outcome {
                        Some(raw) => Some(raw),
                        None => docs.unsafe_no_dispatch_host_failure(&task)?,
                    };
                    if let Some(raw) = raw {
                        if docs.unsafe_after_is_incomplete(&task, &raw)? { AsyncTaskResult::AfterIncomplete(raw) } else { AsyncTaskResult::Uncertain(raw) }
                    } else { AsyncTaskResult::Uncertain(Outcome::Unknown("unsafe child requires reconciliation: after outcome unavailable".into())) }
                }
                TaskState::Failed(reason) => AsyncTaskResult::Uncertain(outcome.unwrap_or_else(|| Outcome::Unknown(format!("unsafe child requires reconciliation: {reason}")))),
                TaskState::Cancelled if has_intent => AsyncTaskResult::Uncertain(outcome.unwrap_or_else(|| Outcome::Unknown("unsafe child was cancelled after dispatch; reconciliation may be required".into()))),
                TaskState::Cancelled => AsyncTaskResult::Cancelled,
                _ => return Err("invalid unsafe child terminal state".into()),
            }));
        }
        Ok(match child.0.result(&self.task, &mut docs)? {
            None => None,
            Some(NestedTaskResult::Succeeded(value)) => {
                let result: DurableTaskResult = serde_json::from_value(value)
                    .map_err(|_| "child result is not a Tool outcome")?;
                Some(match result {
                    DurableTaskResult::Rich { task_content_v1 } => {
                        validate_content_result(&task_content_v1)?;
                        AsyncTaskResult::Rich(task_content_v1)
                    }
                    DurableTaskResult::Outcome(
                        outcome @ (Outcome::Pending { .. } | Outcome::Unknown(_)),
                    ) => AsyncTaskResult::Uncertain(outcome),
                    DurableTaskResult::Outcome(outcome) => AsyncTaskResult::Outcome(outcome),
                })
            }
            Some(NestedTaskResult::Failed(reason)) => Some(AsyncTaskResult::Failed(reason)),
            Some(NestedTaskResult::Cancelled) => Some(AsyncTaskResult::Cancelled),
        })
    }
}
impl Host<'_, '_> {
    fn validate_wait(&self, calls: &[AsyncTaskCall]) -> Result<Vec<uuid::Uuid>, String> {
        if calls.is_empty() || calls.len() > 256 {
            return Err("invalid wait size".into());
        }
        let started = self.started.lock().map_err(|e| e.to_string())?;
        let mut docs = self.docs.lock().map_err(|e| e.to_string())?;
        let mut seen = HashSet::new();
        let mut ids = Vec::with_capacity(calls.len());
        for call in calls {
            let id = call.task_id();
            if !call.0.belongs_to(self.task.id()) || !seen.insert(id) {
                return Err("wait must name distinct direct children".into());
            }
            if !started.contains(&id) {
                docs.get_owned_child(id)?;
            }
            ids.push(id);
        }
        Ok(ids)
    }
}

impl Executor {
    fn validate_hook_arguments(&self, args: &serde_json::Value) -> Result<(), String> {
        if !args.is_object()
            || serde_json::to_vec(args).map_err(|e| e.to_string())?.len() > 60 * 1024
            || !jsonschema::validator_for(&self.identity.spec.parameters)
                .is_ok_and(|v| v.is_valid(args))
        {
            return Err("hook arguments exceed limit or do not match schema".into());
        }
        Ok(())
    }
    fn bounded_hook_arguments(&self, args: &serde_json::Value) -> Result<(), String> {
        if !args.is_object()
            || serde_json::to_vec(args).map_err(|e| e.to_string())?.len() > 60 * 1024
        {
            return Err("host result arguments exceed supported bounds".into());
        }
        Ok(())
    }
    fn host_result_checkpoint(
        &self,
        args: serde_json::Value,
        code: &str,
        reason: &str,
    ) -> Result<Step, String> {
        self.bounded_hook_arguments(&args)?;
        if !matches!(
            code,
            "blocked" | "invalid_arguments" | "invalid_park" | "tool_unavailable"
        ) {
            return Err("unsupported host result".into());
        }
        // Bound bytes, not characters: diagnostic validation has a 4 KiB
        // byte limit, and UTF-8 characters can occupy up to four bytes.
        let mut end = reason.len().min(2048);
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        let reason = &reason[..end];
        let message = match code {
            "blocked" => format!("Tool call blocked: {reason}"),
            "invalid_park" => format!("Tool call cannot wait at its gates: {reason}"),
            "tool_unavailable" => format!("Tool {reason} is not available"),
            _ => format!("Invalid tool arguments: {reason}"),
        };
        let data = serde_json::to_value(HookCheckpoint {
            version: 1,
            arguments: args,
            outcome: Some(Outcome::Failed(message)),
            host_error: Some(code.into()),
        })
        .map_err(|e| e.to_string())?;
        if serde_json::to_vec(&data).map_err(|e| e.to_string())?.len() > 65536 {
            return Err("host result checkpoint too large".into());
        }
        Ok(Step::Continue(Checkpoint {
            phase: "replay-safe-async-after".into(),
            data,
        }))
    }
    fn hook_registry(&self) -> Result<crate::extension::Resolved, String> {
        let mut registry = crate::extension::Registry::default();
        registry
            .install(crate::extension::Extension {
                name: "durable-hook".into(),
                hooks: vec![self.hook.as_ref().ok_or("missing hook")?.clone()],
                ..Default::default()
            })
            .map_err(|e| e.to_string())?;
        Ok(registry.snapshot().resolve(None))
    }
    fn before(
        &self,
        task: &Task,
        docs: &mut crate::document::TaskDocuments<'_>,
        admission: &AdmissionHandle<'_>,
        cancel: &pi_rs_core::CancellationToken,
    ) -> Result<Step, HookFenceError> {
        admission.replay_safe_durable_check(cancel)?;
        let generation = admission.checkpointable_extension_generation();
        if task.checkpoint().phase == "replay-safe-async-hook-gate"
            && task.checkpoint().data.as_u64().is_none()
        {
            return Err("invalid hook gate checkpoint".into());
        }
        if self.unavailable {
            return Ok(self.host_result_checkpoint(
                task.input().clone(),
                "tool_unavailable",
                &self.identity.spec.name,
            )?);
        }
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(None),
            signal_check: Mutex::new(None),
        };
        let snapshot = admission.checkpointable_hook_snapshot()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        enum BeforeResult {
            Decision(crate::extension::ToolCall, crate::extension::BeforeTool),
            Invalid(serde_json::Value, String),
        }
        let before = runtime.block_on(async {
            let operation = async {
                let args = match self.tool.prepare_arguments(task.input().clone()).await {
                    Ok(args) => args,
                    Err(error) => {
                        self.bounded_hook_arguments(task.input())?;
                        return Ok(BeforeResult::Invalid(task.input().clone(), error.to_string()));
                    }
                };
                self.bounded_hook_arguments(&args)?;
                if let Err(error) = self.validate_hook_arguments(&args) { return Ok(BeforeResult::Invalid(args, error)); }
                let ctx = crate::extension::Context { snapshot: &snapshot, memo: &memo };
                let (call, decision, _) = self.hook_registry()?.before_tool(crate::extension::ToolCall { name: self.identity.spec.name.clone(), arguments: args }, &ctx).await;
                Ok::<_, String>(BeforeResult::Decision(call, decision))
            };
            tokio::select! { biased; _ = cancel.cancelled() => Err("before hook cancelled".into()), result = operation => result }
        });
        if cancel.is_cancelled() {
            return Err("before hook cancelled".into());
        }
        if let Some(error) = memo.failure() {
            return Err(
                if matches!(error, HookFenceError::WriteLockBusy)
                    || matches!(&error, HookFenceError::Other(reason) if reason == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY || reason == "prior unsafe execution evidence requires reconciliation")
                {
                    error
                } else {
                    "task-fenced hook memo failed".into()
                },
            );
        }
        memo.stage_signal_check(docs)?;
        let before = before?;
        admission.replay_safe_durable_check(cancel)?;
        let (call, decision) = match before {
            BeforeResult::Invalid(args, reason) => {
                return Ok(self.host_result_checkpoint(args, "invalid_arguments", &reason)?);
            }
            BeforeResult::Decision(call, decision) => (call, decision),
        };
        match decision {
            crate::extension::BeforeTool::Pass => (),
            crate::extension::BeforeTool::Block(reason) => {
                return Ok(self.host_result_checkpoint(call.arguments, "blocked", &reason)?);
            }
            crate::extension::BeforeTool::Sleep(sleep)
                if admission.is_conversation_checkpointable() && self.replay_safe =>
            {
                let invalid = (sleep.until_millis.is_none() && sleep.signals.is_empty())
                    || sleep.until_millis.is_some_and(|deadline| deadline < 0)
                    || sleep
                        .signals
                        .iter()
                        .any(|key| key.is_empty() || key.len() > 4096)
                    || sleep
                        .signals
                        .iter()
                        .enumerate()
                        .any(|(i, key)| sleep.signals[..i].contains(key));
                if invalid {
                    return Ok(self.host_result_checkpoint(
                        call.arguments,
                        "invalid_park",
                        "invalid extension sleep",
                    )?);
                }
                let condition = if sleep.signals.is_empty() {
                    WaitFor::Sleep {
                        until_millis: sleep.until_millis,
                        signals: vec![],
                    }
                } else {
                    let token = crate::extension::checkpointable_hook_signal_token();
                    let keys = sleep
                        .signals
                        .iter()
                        .map(|key| token.key(key.clone()).map_err(|e| e.to_string()))
                        .collect::<Result<Vec<_>, _>>()?;
                    let (available, check) =
                        admission.replay_safe_hook_wait_keys_available(&keys, cancel)?;
                    docs.stage_hook_signal_check(check)?;
                    if !available {
                        return Ok(self.host_result_checkpoint(
                            call.arguments,
                            "invalid_park",
                            "signal key already consumed or expired",
                        )?);
                    }
                    WaitFor::TypedSleep {
                        until_millis: sleep.until_millis,
                        signals: keys,
                    }
                };
                return Ok(Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "replay-safe-async-hook-gate".into(),
                        data: serde_json::json!(
                            generation.ok_or("missing hook registry generation")?
                        ),
                    },
                    condition,
                    children: vec![],
                });
            }
            _ => return Err("hook sleep is unsupported".into()),
        }
        self.bounded_hook_arguments(&call.arguments)?;
        if let Err(reason) = self.validate_hook_arguments(&call.arguments) {
            return Ok(self.host_result_checkpoint(
                call.arguments,
                "invalid_arguments",
                &reason,
            )?);
        }
        let data = serde_json::to_value(HookCheckpoint {
            version: 1,
            arguments: call.arguments,
            outcome: None,
            host_error: None,
        })
        .map_err(|e| e.to_string())?;
        if serde_json::to_vec(&data).map_err(|e| e.to_string())?.len() > 65536 {
            return Err("hook checkpoint too large".into());
        }
        Ok(Step::Continue(Checkpoint {
            phase: "replay-safe-async-prepared".into(),
            data,
        }))
    }
    #[allow(clippy::too_many_arguments)] // Fenced hook context and the stored result channels are independent.
    fn after_blocks(
        &self,
        task: &Task,
        docs: &mut crate::document::TaskDocuments<'_>,
        admission: &AdmissionHandle<'_>,
        cancel: &pi_rs_core::CancellationToken,
        args: serde_json::Value,
        outcome: Outcome,
        host_error: Option<String>,
    ) -> Result<Step, HookFenceError> {
        let mut extra = std::collections::BTreeMap::new();
        extra.insert(
            "isError".into(),
            serde_json::json!(matches!(outcome, Outcome::Failed(_) | Outcome::Skipped(_))),
        );
        match &outcome {
            Outcome::SucceededWithDetails { details, .. } => {
                extra.insert("details".into(), details.clone());
            }
            Outcome::Structured { data, attachments } => {
                extra.insert("structuredOutput".into(), data.clone());
                extra.insert(
                    "attachments".into(),
                    serde_json::to_value(attachments).map_err(|e| e.to_string())?,
                );
            }
            _ => (),
        }
        let required = if let Some(code) = &host_error {
            let Outcome::Failed(message) = &outcome else {
                return Err("invalid host error outcome".into());
            };
            Some(crate::runtime::ToolDiagnostic {
                severity: crate::runtime::DiagnosticSeverity::Error,
                code: Some(code.clone()),
                message: bounded_diagnostic(message),
            })
        } else {
            None
        };
        let mut diagnostics: Vec<crate::runtime::ToolDiagnostic> =
            required.clone().into_iter().collect();
        if required.is_none() {
            match &outcome {
                Outcome::Failed(reason) | Outcome::Skipped(reason) => {
                    diagnostics.push(crate::runtime::ToolDiagnostic {
                        severity: crate::runtime::DiagnosticSeverity::Error,
                        message: bounded_diagnostic(reason),
                        code: Some(
                            if matches!(outcome, Outcome::Failed(_)) {
                                "tool_failed"
                            } else {
                                "tool_skipped"
                            }
                            .into(),
                        ),
                    })
                }
                Outcome::Truncated { original_bytes } => {
                    diagnostics.push(crate::runtime::ToolDiagnostic {
                        severity: crate::runtime::DiagnosticSeverity::Warn,
                        message: format!("tool output omitted ({original_bytes} bytes)"),
                        code: Some("output_truncated".into()),
                    })
                }
                _ => (),
            }
        }
        let snapshot = admission.checkpointable_hook_snapshot()?;
        // A conversation schema governs structured output, not an implicit text
        // copy of it. Avoid creating that copy when it is already invalid, so a
        // hook cannot accidentally retain fragments of the rejected projection.
        let schema = snapshot
            .get("tool_filters")
            .and_then(|v| v.get("output_schemas"))
            .and_then(|v| v.get(&self.identity.spec.name));
        let suppress_projection = match (&outcome, schema) {
            (Outcome::Structured { data, .. }, Some(schema))
                if matches!(task.owner(), crate::task::TaskOwner::Conversation(_)) =>
            {
                !crate::runtime::local_schema_references(schema)
                    || jsonschema::validator_for(schema)
                        .map_or(true, |validator| !validator.is_valid(data))
            }
            _ => false,
        };
        let initial = crate::extension::ToolBlockResult {
            output: vec![
                serde_json::json!({"type":"text", "text":if suppress_projection { String::new() } else { outcome.tool_content() }}),
            ],
            diagnostics,
            extra,
        };
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(None),
            signal_check: Mutex::new(None),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let result = runtime.block_on(async {
            let call = crate::extension::ToolCall {
                name: self.identity.spec.name.clone(),
                arguments: args,
            };
            let ctx = crate::extension::Context {
                snapshot: &snapshot,
                memo: &memo,
            };
            let selected = self.hook_registry()?;
            let nested = matches!(task.owner(), crate::task::TaskOwner::Task(_));
            tokio::select! { biased;
                _ = cancel.cancelled() => Err("after hook cancelled".to_string()),
                result = selected.after_tool_blocks(&call, initial, nested, &ctx) => {
                    let (value, reports) = result?;
                    if !reports.is_empty() { return Err("block hook failed".into()); }
                    Ok(value)
                }
            }
        });
        if cancel.is_cancelled() {
            return Err("after hook cancelled".into());
        }
        if let Some(error) = memo.failure() {
            return Err(
                if matches!(error, HookFenceError::WriteLockBusy)
                    || matches!(&error, HookFenceError::Other(reason) if reason == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY)
                {
                    error
                } else {
                    "task-fenced hook memo failed".into()
                },
            );
        }
        memo.stage_signal_check(docs)?;
        let result = result?;
        admission.replay_safe_durable_check(cancel)?;
        validate_content_hook_result(&result, required.as_ref(), false)?;
        let value = serde_json::to_value(DurableTaskResult::Rich {
            task_content_v1: result,
        })
        .map_err(|e| e.to_string())?;
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > 60 * 1024 {
            return Err("rich result exceeds size limit".into());
        }
        Ok(Step::Succeed(value))
    }

    fn after(
        &self,
        task: &Task,
        docs: &mut crate::document::TaskDocuments<'_>,
        admission: &AdmissionHandle<'_>,
        cancel: &pi_rs_core::CancellationToken,
    ) -> Result<Step, HookFenceError> {
        admission.replay_safe_durable_check(cancel)?;
        let stored: HookCheckpoint = serde_json::from_value(task.checkpoint().data.clone())
            .map_err(|_| "invalid after checkpoint")?;
        if stored.version != 1 {
            return Err("invalid after checkpoint version".into());
        }
        self.bounded_hook_arguments(&stored.arguments)?;
        if self.unavailable != (stored.host_error.as_deref() == Some("tool_unavailable")) {
            return Err("unavailable checkpoint identity mismatch".into());
        }
        if stored.host_error.is_none() {
            self.validate_hook_arguments(&stored.arguments)?;
        }
        if stored.host_error.as_deref().is_some_and(|code| {
            !matches!(
                code,
                "blocked" | "invalid_arguments" | "invalid_park" | "tool_unavailable"
            )
        }) {
            return Err("invalid host result code".into());
        }
        let outcome = stored.outcome.ok_or("missing completed outcome")?;
        if stored.host_error.is_some() && !matches!(outcome, Outcome::Failed(_)) {
            return Err("invalid host result outcome".into());
        }
        if self.block_results {
            return self.after_blocks(
                task,
                docs,
                admission,
                cancel,
                stored.arguments,
                outcome,
                stored.host_error,
            );
        }
        let diagnostic_message = match &outcome {
            Outcome::Failed(reason) => reason.clone(),
            _ => String::new(),
        };
        let text = outcome.tool_content();
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(None),
            signal_check: Mutex::new(None),
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let value = runtime.block_on(async {
            let call = crate::extension::ToolCall { name: self.identity.spec.name.clone(), arguments: stored.arguments };
            let snapshot = serde_json::Value::Null; let ctx = crate::extension::Context { snapshot: &snapshot, memo: &memo };
            let selected = self.hook_registry()?;
            let operation = async {
                let (value, _) = selected.after_tool(&call, serde_json::Value::String(text.clone()), &ctx).await;
                let Some(code) = stored.host_error.as_deref() else { return Ok(value); };
                let output = value.as_str().unwrap_or(&text).to_owned();
                let diagnostic = crate::runtime::ToolDiagnostic { severity: crate::runtime::DiagnosticSeverity::Error, message: diagnostic_message.clone(), code: Some(code.into()) };
                let original = crate::extension::ToolResult { output, is_error: true, details: None, structured_output: None, attachments: vec![], diagnostics: vec![diagnostic.clone()] };
                let (result, _, _) = selected.after_tool_result(&call, original.clone(), &ctx).await;
                if !result.is_error || result.details.is_some() || result.structured_output.is_some() || !result.attachments.is_empty() || result.diagnostics != original.diagnostics {
                    return Err("unsupported host-result metadata replacement".to_string());
                }
                Ok(serde_json::Value::String(result.output))
            };
            tokio::select! { biased; _ = cancel.cancelled() => Err("after hook cancelled".to_string()), result = operation => result }
        });
        if cancel.is_cancelled() {
            return Err("after hook cancelled".into());
        }
        if let Some(error) = memo.failure() {
            return Err(
                if matches!(error, HookFenceError::WriteLockBusy)
                    || matches!(&error, HookFenceError::Other(reason) if reason == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY)
                {
                    error
                } else {
                    "task-fenced hook memo failed".into()
                },
            );
        }
        memo.stage_signal_check(docs)?;
        let value = value?;
        admission.replay_safe_durable_check(cancel)?;
        let result = match value.as_str() {
            Some(output) if output != text => match outcome {
                Outcome::Succeeded(_) => Outcome::Succeeded(output.to_owned()),
                Outcome::SucceededWithDetails { details, .. } => Outcome::SucceededWithDetails {
                    output: output.to_owned(),
                    details,
                },
                Outcome::Failed(_) if stored.host_error.is_some() => Outcome::Failed(
                    output
                        .strip_prefix("Tool failed: ")
                        .unwrap_or(output)
                        .to_owned(),
                ),
                _ => return Err("non-text after replacement unsupported".into()),
            },
            _ => outcome,
        };
        let value = serde_json::to_value(result).map_err(|e| e.to_string())?;
        if serde_json::to_vec(&value).map_err(|e| e.to_string())?.len() > 65536 {
            return Err("hook result too large".into());
        }
        Ok(Step::Succeed(value))
    }
    fn run_with_restart_cleanup(
        &self,
        task: &Task,
        admission: &AdmissionHandle<'_>,
    ) -> Result<Step, String> {
        if !self.block_results
            || !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err("restart hook identity changed".into());
        }
        if let Some(value) = admission.restart_hook_receipt(self, None)? {
            super::validate_restart_after_result(&value)?;
            return Ok(Step::Succeed(serde_json::Value::Null));
        }
        let args = match task.checkpoint().phase.as_str() {
            "initial" | "replay-safe-async-hook-gate" => task.input().clone(),
            "replay-safe-async-prepared" => {
                serde_json::from_value::<HookCheckpoint>(task.checkpoint().data.clone())
                    .map_err(|_| "invalid hook checkpoint")?
                    .arguments
            }
            "replay-safe-async-wait" => {
                serde_json::from_value::<ParkCheckpoint>(task.checkpoint().data.clone())
                    .map_err(|_| "invalid park checkpoint")?
                    .arguments
            }
            _ => return Err("unsupported restart checkpoint".into()),
        };
        self.bounded_hook_arguments(&args)?;
        let gate = crate::runtime::ToolDiagnostic {
            severity: crate::runtime::DiagnosticSeverity::Error,
            code: Some("restart_limit".into()),
            message: "Tool exceeded its restart limit; external effects may be unknown".into(),
        };
        let initial = crate::extension::ToolBlockResult {
            output: vec![serde_json::json!({"type":"text","text":gate.message})],
            diagnostics: vec![gate.clone()],
            extra: [("isError".into(), serde_json::json!(true))]
                .into_iter()
                .collect(),
        };
        let memo = AbortMemo(std::sync::atomic::AtomicBool::new(false));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let result = rt.block_on(async {
            let call = crate::extension::ToolCall {
                name: self.identity.spec.name.clone(),
                arguments: args,
            };
            let snapshot = serde_json::Value::Null;
            let ctx = crate::extension::Context {
                snapshot: &snapshot,
                memo: &memo,
            };
            let (value, reports) = self
                .hook_registry()?
                .after_tool_blocks(
                    &call,
                    initial,
                    matches!(task.owner(), crate::task::TaskOwner::Task(_)),
                    &ctx,
                )
                .await?;
            if !reports.is_empty() {
                return Err("restart hook failed".to_string());
            }
            Ok(value)
        })?;
        if memo.0.load(std::sync::atomic::Ordering::Acquire) {
            return Err("restart hook used unavailable memo".into());
        }
        validate_content_hook_result(&result, Some(&gate), false)?;
        let value = serde_json::to_value(DurableTaskResult::Rich {
            task_content_v1: result,
        })
        .map_err(|e| e.to_string())?;
        super::validate_restart_after_result(&value)?;
        #[cfg(test)]
        if let Some(dir) = std::env::var_os("PI_TEST_RESTART_STALE_LEASE") {
            if !admission.test_restart_stale_lease(self, &value) {
                return Err("stale restart lease accepted".into());
            }
            std::fs::write(
                std::path::PathBuf::from(dir).join("restart-stale-rejected"),
                b"yes",
            )
            .map_err(|e| e.to_string())?;
        }
        if !self
            .identity
            .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err("restart hook identity changed during execution".into());
        }
        admission.restart_hook_receipt(self, Some(&value))?;
        #[cfg(test)]
        {
            let hook = restart_receipt_hooks().lock().unwrap().remove(&task.id());
            if let Some(hook) = hook {
                hook();
            }
        }
        #[cfg(test)]
        if let Some(dir) = std::env::var_os("PI_TEST_RESTART_AFTER_RECEIPT") {
            std::fs::write(
                std::path::PathBuf::from(dir).join("restart-receipt"),
                b"ready",
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(Step::Succeed(serde_json::Value::Null))
    }
}

impl Handler for Executor {
    fn conversation_cancel_dispatch_enabled(&self) -> bool {
        self._conversation_cancel
            .as_ref()
            .is_some_and(|(_, _, enabled)| *enabled)
    }
    fn conversation_cleanup_identity_lease(
        &self,
    ) -> Result<crate::nested_identity::IdentityLease, crate::nested_identity::LeaseFailure> {
        let expected = self
            .conversation_identity
            .as_deref()
            .ok_or(crate::nested_identity::LeaseFailure::Mismatch)?;
        if self._conversation_cancel.is_none() {
            return Err(crate::nested_identity::LeaseFailure::Mismatch);
        }
        let tool: Arc<dyn Tool> = self.tool.clone();
        let lease = crate::nested_identity::checked_lease(&tool, expected)?;
        if !self
            .identity
            .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err(crate::nested_identity::LeaseFailure::Mismatch);
        }
        Ok(lease)
    }
    fn requires_park_cancel_claim(&self) -> bool {
        self.park_cancel.is_some()
    }
    fn woken_cleanup_identity_lease(
        &self,
    ) -> Result<
        tokio::sync::OwnedRwLockReadGuard<Option<String>>,
        crate::nested_identity::LeaseFailure,
    > {
        let expected = self
            .woken_identity
            .as_deref()
            .ok_or(crate::nested_identity::LeaseFailure::Mismatch)?;
        let tool: Arc<dyn Tool> = self.tool.clone();
        let lease = crate::nested_identity::checked_lease(&tool, expected)?;
        if !self
            .identity
            .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err(crate::nested_identity::LeaseFailure::Mismatch);
        }
        Ok(lease)
    }
    fn run(&self, _: &Task) -> Result<Step, String> {
        Err("driver reservation required".into())
    }
    fn run_abort_with_admission(
        &self,
        task: &Task,
        admission: &AdmissionHandle<'_>,
        _cancellation: Option<&pi_rs_core::CancellationToken>,
    ) -> Result<Step, String> {
        if task.holds_restart_cleanup() {
            return self.run_with_restart_cleanup(task, admission);
        }
        if admission.is_conversation_checkpointable() && self._conversation_cancel.is_some() {
            let identity_tool: Arc<dyn Tool> = self.tool.clone();
            let expected = self
                .conversation_identity
                .as_deref()
                .ok_or("missing conversation cancel identity")?;
            let _identity_lease = crate::nested_identity::checked_lease(&identity_tool, expected)
                .map_err(
                |_| "conversation cancel identity unavailable; reconciliation required",
            )?;
            if !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
            {
                return Err("conversation cancel tool identity changed".into());
            }
            let (_, cancel, enabled) = self
                ._conversation_cancel
                .as_ref()
                .ok_or("missing cancel registration")?;
            let _claim = admission
                .record_conversation_park_claim(self, cancel.conversation_cancel_policy())?;
            #[cfg(test)]
            if crate::task_scheduler::take_test_conversation_start(task.id()) {
                let result = admission.record_conversation_start_markers(
                    self,
                    _claim.claim_id,
                    &_identity_lease,
                    expected,
                    cancel.conversation_cancel_policy(),
                );
                if crate::task_scheduler::take_test_conversation_finish(task.id()) {
                    let marker = result?;
                    let receipt = crate::task_scheduler::ConversationCancelReceipt {
                        version: 1,
                        claim: _claim.clone(),
                        result: "test evidence only".into(),
                    };
                    // A substituted marker must not write a receipt or remove either marker.
                    if admission
                        .store_conversation_cancel_receipt_and_retire(
                            self,
                            &receipt,
                            &_identity_lease,
                            expected,
                            b"substituted",
                        )
                        .is_ok()
                    {
                        return Err("substituted marker accepted".into());
                    }
                    let finish = admission.store_conversation_cancel_receipt_and_retire(
                        self,
                        &receipt,
                        &_identity_lease,
                        expected,
                        &marker,
                    );
                    crate::task_scheduler::report_conversation_start_result(task.id());
                    finish?;
                    return Err("test receipt evidence only; callback remains disabled".into());
                }
                crate::task_scheduler::report_conversation_start_result(task.id());
                result?;
                if admission
                    .record_conversation_start_markers(
                        self,
                        _claim.claim_id,
                        &_identity_lease,
                        expected,
                        cancel.conversation_cancel_policy(),
                    )
                    .is_ok()
                {
                    return Err("duplicate start markers accepted".into());
                }
                return Err("test start evidence only; callback remains disabled".into());
            }
            #[cfg(test)]
            if crate::task_scheduler::take_test_conversation_receipt(task.id()) {
                // A successful claim is single-use even when the same live
                // attempt tries to acquire it again before any callback runs.
                if admission
                    .record_conversation_park_claim(self, cancel.conversation_cancel_policy())
                    .is_ok()
                {
                    return Err("duplicate conversation cancel claim accepted".into());
                }
                let receipt = crate::task_scheduler::ConversationCancelReceipt {
                    version: 1,
                    claim: _claim.clone(),
                    result: "test evidence only".into(),
                };
                if admission
                    .read_live_conversation_cancel_receipt(
                        self,
                        _claim.claim_id,
                        &_identity_lease,
                        expected,
                    )?
                    .is_some()
                {
                    return Err("unexpected pre-existing receipt".into());
                }
                admission.store_conversation_cancel_receipt(
                    self,
                    &receipt,
                    &_identity_lease,
                    expected,
                )?;
                admission.store_conversation_cancel_receipt(
                    self,
                    &receipt,
                    &_identity_lease,
                    expected,
                )?;
                if admission.read_live_conversation_cancel_receipt(
                    self,
                    _claim.claim_id,
                    &_identity_lease,
                    expected,
                )? != Some(receipt.clone())
                {
                    return Err("receipt read mismatch".into());
                }
                if admission
                    .read_live_conversation_cancel_receipt(
                        self,
                        uuid::Uuid::new_v4(),
                        &_identity_lease,
                        expected,
                    )
                    .is_ok()
                {
                    return Err("foreign claim accepted".into());
                }
                let mut foreign = receipt.clone();
                foreign.claim.attempt = uuid::Uuid::new_v4();
                if admission
                    .store_conversation_cancel_receipt(self, &foreign, &_identity_lease, expected)
                    .is_ok()
                {
                    return Err("foreign attempt accepted".into());
                }
            }
            if !enabled {
                return Err(
                    "conversation cancel callback is not enabled; reconciliation required".into(),
                );
            }
            let stored: ParkCheckpoint = serde_json::from_value(task.checkpoint().data.clone())
                .map_err(|_| "invalid parked cancel checkpoint")?;
            if stored.version != 2
                || stored.park_attempt.is_none()
                || !stored.arguments.is_object()
                || !jsonschema::validator_for(&self.identity.spec.parameters)
                    .is_ok_and(|schema| schema.is_valid(&stored.arguments))
            {
                return Err("unsupported parked cancel checkpoint".into());
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let start = admission.conversation_callback_start_lease()?;
            let marker_result = admission.record_conversation_start_markers(
                self,
                _claim.claim_id,
                &_identity_lease,
                expected,
                cancel.conversation_cancel_policy(),
            );
            #[cfg(test)]
            crate::task_scheduler::report_conversation_start_result(task.id());
            let marker = marker_result?;
            let cancellation = pi_rs_core::CancellationToken::new();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.block_on(async {
                    let future = cancel.cancel(
                        stored.arguments,
                        stored.checkpoint,
                        ToolContext {
                            operation_id: task.id(),
                            cancellation: cancellation.clone(),
                        },
                    );
                    let mut future = std::pin::pin!(future);
                    let mut start = Some(start);
                    tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        std::future::poll_fn(|cx| match start.take() {
                            Some(lease) => lease.poll_first(future.as_mut(), cx),
                            None => future.as_mut().poll(cx),
                        }),
                    )
                    .await
                })
            }));
            cancellation.cancel();
            if !matches!(result, Ok(Ok(Ok(())))) {
                admission.retire_conversation_cancel_unknown(
                    &_claim,
                    &_identity_lease,
                    expected,
                    &marker,
                )?;
                return Err("conversation cancel outcome unknown; reconciliation required".into());
            }
            let receipt = crate::task_scheduler::ConversationCancelReceipt {
                version: 1,
                claim: _claim,
                result: "callback returned".into(),
            };
            if let Err(error) = admission.store_conversation_cancel_receipt_and_retire(
                self,
                &receipt,
                &_identity_lease,
                expected,
                &marker,
            ) {
                admission.retire_conversation_cancel_unknown(
                    &receipt.claim,
                    &_identity_lease,
                    expected,
                    &marker,
                )?;
                return Err(format!(
                    "conversation cancel result requires reconciliation: {error}"
                ));
            }
            #[cfg(test)]
            crate::task_scheduler::report_conversation_receipt_saved(task.id());
            return Ok(Step::Succeed(serde_json::Value::Null));
        }
        if !self.replay_safe
            && self.park_cancel.is_some()
            && task.checkpoint().phase != "replay-safe-async-wait"
        {
            return Err("unsafe execution is not a proven park; reconciliation required".into());
        }
        if !self.block_results
            || !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err("abort hook identity changed".into());
        }
        if let Some(value) = admission.abort_hook_receipt(self, None)? {
            super::validate_aborted_after_result(&value)?;
            return Ok(Step::Succeed(serde_json::Value::Null));
        }
        if let Some((cancel, retry)) = &self.park_cancel
            && task.checkpoint().phase == "replay-safe-async-wait"
        {
            // The host serializes every Tool and callback configuration change
            // through this nonblocking identity lease. It is held through the
            // bounded callback, not across any scheduler or store lock.
            let identity_tool: Arc<dyn Tool> = self.tool.clone();
            let _identity_lease = match &self.woken_identity {
                Some(expected) => Some(
                    crate::nested_identity::checked_lease(&identity_tool, expected).map_err(
                        |_| "woken cancel identity unavailable; reconciliation required",
                    )?,
                ),
                None => None,
            };
            if self.woken_identity.is_some()
                && !self
                    .identity
                    .matches(self.tool.as_ref(), &self.implementation, self.version)
            {
                return Err("woken cancel tool identity changed".into());
            }
            let stored: ParkCheckpoint = serde_json::from_value(task.checkpoint().data.clone())
                .map_err(|_| "invalid parked cancel checkpoint")?;
            if stored.version != 2
                || stored.park_attempt.is_none()
                || !stored.arguments.is_object()
                || !jsonschema::validator_for(&self.identity.spec.parameters)
                    .is_ok_and(|schema| schema.is_valid(&stored.arguments))
            {
                return Err("unsupported parked cancel checkpoint".into());
            }
            let decision = admission.park_cancel_claim(self, false, *retry)?;
            if !self.replay_safe && decision == crate::task_scheduler::ParkCancelDecision::Uncertain
            {
                return Err("unsafe cancel outcome uncertain; reconciliation required".into());
            }
            if decision == crate::task_scheduler::ParkCancelDecision::Invoke {
                #[cfg(test)]
                if let Some(dir) = std::env::var_os("PI_TEST_PARK_CANCEL_SPEC_BARRIER") {
                    let dir = std::path::PathBuf::from(dir);
                    std::fs::write(dir.join("spec-barrier"), b"yes").map_err(|e| e.to_string())?;
                    while !dir.join("spec-release").exists() {
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                }
                #[cfg(test)]
                if let Some(dir) = std::env::var_os("PI_TEST_PARK_CANCEL_AFTER_CLAIM") {
                    std::fs::write(std::path::PathBuf::from(dir).join("cancel-claimed"), b"yes")
                        .map_err(|e| e.to_string())?;
                    loop {
                        std::thread::park();
                    }
                }
                if !self
                    .identity
                    .matches(self.tool.as_ref(), &self.implementation, self.version)
                {
                    return Err("cancel tool identity changed before dispatch".into());
                }
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                let cancellation = pi_rs_core::CancellationToken::new();
                let result = runtime.block_on(async {
                    admission.verify_park_cancel_dispatch(self, *retry)?;
                    if !self.identity.matches(
                        self.tool.as_ref(),
                        &self.implementation,
                        self.version,
                    ) {
                        return Err("cancel tool identity changed before dispatch".to_string());
                    }
                    #[cfg(test)]
                    {
                        let hook = cancel_dispatch_hooks().lock().unwrap().remove(&task.id());
                        if let Some(hook) = hook {
                            hook();
                        }
                    }
                    Ok::<_, String>(
                        tokio::time::timeout(
                            std::time::Duration::from_secs(30),
                            cancel.cancel(
                                stored.arguments,
                                stored.checkpoint,
                                ToolContext {
                                    operation_id: task.id(),
                                    cancellation: cancellation.clone(),
                                },
                            ),
                        )
                        .await,
                    )
                })?;
                cancellation.cancel();
                if matches!(result, Ok(Ok(()))) {
                    admission.park_cancel_claim(self, true, *retry)?;
                    #[cfg(test)]
                    if let Some(dir) = std::env::var_os("PI_TEST_PARK_CANCEL_AFTER_RETURN") {
                        std::fs::write(
                            std::path::PathBuf::from(dir).join("cancel-returned"),
                            b"yes",
                        )
                        .map_err(|e| e.to_string())?;
                        loop {
                            std::thread::park();
                        }
                    }
                } else if !self.replay_safe {
                    return Err("unsafe cancel outcome uncertain; reconciliation required".into());
                }
            }
        }
        let args = match task.checkpoint().phase.as_str() {
            "initial" | "replay-safe-async-hook-gate" => task.input().clone(),
            "replay-safe-async-prepared" | "replay-safe-async-after" => {
                serde_json::from_value::<HookCheckpoint>(task.checkpoint().data.clone())
                    .map_err(|_| "invalid hook checkpoint")?
                    .arguments
            }
            "replay-safe-async-wait" => {
                serde_json::from_value::<ParkCheckpoint>(task.checkpoint().data.clone())
                    .map_err(|_| "invalid park checkpoint")?
                    .arguments
            }
            _ => return Err("unsupported abort checkpoint".into()),
        };
        self.bounded_hook_arguments(&args)?;
        let gate = crate::runtime::ToolDiagnostic {
            severity: crate::runtime::DiagnosticSeverity::Error,
            code: Some("aborted".into()),
            message: "Tool was aborted".into(),
        };
        let initial = crate::extension::ToolBlockResult {
            output: vec![serde_json::json!({"type":"text","text":"Tool was aborted"})],
            diagnostics: vec![gate.clone()],
            extra: [("isError".into(), serde_json::json!(true))]
                .into_iter()
                .collect(),
        };
        let memo = AbortMemo(std::sync::atomic::AtomicBool::new(false));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let result = rt.block_on(async {
            let call = crate::extension::ToolCall {
                name: self.identity.spec.name.clone(),
                arguments: args,
            };
            let snapshot = serde_json::Value::Null;
            let ctx = crate::extension::Context {
                snapshot: &snapshot,
                memo: &memo,
            };
            let (value, reports) = self
                .hook_registry()?
                .after_tool_blocks(
                    &call,
                    initial,
                    matches!(task.owner(), crate::task::TaskOwner::Task(_)),
                    &ctx,
                )
                .await?;
            if !reports.is_empty() {
                return Err("abort hook failed".to_string());
            }
            Ok(value)
        })?;
        if memo.0.load(std::sync::atomic::Ordering::Acquire) {
            return Err("abort hook used unavailable memo".into());
        }
        validate_content_hook_result(&result, Some(&gate), false)?;
        let value = serde_json::to_value(DurableTaskResult::Rich {
            task_content_v1: result,
        })
        .map_err(|e| e.to_string())?;
        super::validate_aborted_after_result(&value)?;
        #[cfg(test)]
        if let Some(dir) = std::env::var_os("PI_TEST_ABORT_STALE_LEASE") {
            if !admission.test_abort_stale_lease(self, &value) {
                return Err("stale lease accepted".into());
            }
            std::fs::write(std::path::PathBuf::from(dir).join("stale-rejected"), b"yes")
                .map_err(|e| e.to_string())?;
        }
        if !self
            .identity
            .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err("abort hook identity changed during execution".into());
        }
        admission.abort_hook_receipt(self, Some(&value))?;
        #[cfg(test)]
        if let Some(dir) = std::env::var_os("PI_TEST_ABORT_AFTER_RECEIPT") {
            std::fs::write(
                std::path::PathBuf::from(dir).join("abort-receipt"),
                b"ready",
            )
            .unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok(Step::Succeed(serde_json::Value::Null))
    }
    fn run_with_admission_and_cancellation(
        &self,
        task: &Task,
        docs: &mut crate::document::TaskDocuments<'_>,
        admission: &AdmissionHandle<'_>,
        cancellation: &pi_rs_core::CancellationToken,
    ) -> Result<Step, String> {
        if !self
            .identity
            .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err("tool identity changed".into());
        }
        if self.hook.is_some()
            && matches!(
                task.checkpoint().phase.as_str(),
                "initial" | "replay-safe-async-hook-gate" | "replay-safe-async-after"
            )
        {
            let result = if task.checkpoint().phase != "replay-safe-async-after" {
                self.before(task, docs, admission, cancellation)
            } else {
                self.after(task, docs, admission, cancellation)
            };
            if !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
            {
                return Err("tool identity changed during hook".into());
            }
            return match result {
                Err(HookFenceError::WriteLockBusy) => Ok(Step::DeferUndispatched),
                Err(HookFenceError::Other(error))
                    if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY =>
                {
                    Ok(Step::DeferUndispatched)
                }
                Ok(step) if !self.replay_safe => {
                    docs.stage_unsafe_hook_phase(&step)?;
                    Ok(step)
                }
                Err(error) => Err(error.to_string()),
                Ok(step) => Ok(step),
            };
        }
        let intent = match admission.checkpointable_durable_intent(
            false,
            self.replay_safe,
            Some(cancellation),
        ) {
            Ok(intent) => intent,
            Err(HookFenceError::WriteLockBusy) => return Ok(Step::DeferUndispatched),
            Err(HookFenceError::Other(error))
                if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY =>
            {
                return Ok(Step::DeferUndispatched);
            }
            Err(error) => return Err(error.to_string()),
        };
        #[cfg(test)]
        crash_test_boundary(&self.tool.spec().name, task.id(), "intent");
        let (resume, saved_args) = match task.checkpoint().phase.as_str() {
            "initial" => (
                DurableToolResume {
                    checkpoint: None,
                    ended: vec![],
                },
                None,
            ),
            "replay-safe-async-prepared" if self.hook.is_some() => {
                let stored: HookCheckpoint = serde_json::from_value(task.checkpoint().data.clone())
                    .map_err(|_| "invalid hook checkpoint")?;
                if stored.version != 1 || stored.outcome.is_some() || stored.host_error.is_some() {
                    return Err("invalid prepared hook checkpoint".into());
                }
                (
                    DurableToolResume {
                        checkpoint: None,
                        ended: vec![],
                    },
                    Some(stored.arguments),
                )
            }
            "replay-safe-async-wait" if task.ended_children().is_some() => {
                let stored: ParkCheckpoint = serde_json::from_value(task.checkpoint().data.clone())
                    .map_err(|_| "invalid durable checkpoint")?;
                if !matches!(stored.version, 1 | 2)
                    || (stored.version == 2 && stored.park_attempt.is_none())
                {
                    return Err("unsupported durable checkpoint version".into());
                }
                (
                    DurableToolResume {
                        checkpoint: Some(stored.checkpoint),
                        ended: task.ended_children().unwrap_or_default().to_vec(),
                    },
                    Some(stored.arguments),
                )
            }
            _ => return Err("invalid durable checkpoint or wake".into()),
        };
        let fresh_unhooked = if admission.is_conversation_checkpointable()
            && saved_args.is_none()
            && task.restarts() == 0
            && self.replay_safe
            && self.hook.is_none()
        {
            docs.prove_first_checkpointable_intent(&intent)?
        } else {
            false
        };
        let unsafe_first = admission.is_conversation_checkpointable()
            && saved_args.is_none()
            && task.restarts() == 0
            && !self.replay_safe
            && self.hook.is_none();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let host = Host {
            task: task.clone(),
            docs: Mutex::new(docs),
            children: &self.children,
            started: Mutex::new(HashSet::new()),
        };
        let (result, args, no_dispatch) = runtime.block_on(async {
            let args = match saved_args {
                Some(args) => args,
                None => tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err("argument preparation cancelled".into()),
                    prepared = self.tool.prepare_arguments(task.input().clone()) => match prepared {
                        Ok(args) => args,
                        Err(_) if !cancellation.is_cancelled() && (fresh_unhooked || (unsafe_first && host.docs.lock().map_err(|e| e.to_string())?.prove_first_checkpointable_intent(&intent)?)) => return Ok((DurableToolStep::Complete(Outcome::Failed("argument preparation failed".into())), serde_json::Value::Null, unsafe_first)),
                        Err(error) => return Err(error.to_string()),
                    },
                },
            };
            if cancellation.is_cancelled() { return Err("tool execution cancelled".into()); }
            if !args.is_object()
                || serde_json::to_vec(&args).map_err(|e| e.to_string())?.len() > 16 * 1024 * 1024
                || !jsonschema::validator_for(&self.tool.spec().parameters)
                    .is_ok_and(|v| v.is_valid(&args))
            {
                if fresh_unhooked || (unsafe_first && host.docs.lock().map_err(|e| e.to_string())?.prove_first_checkpointable_intent(&intent)?) {
                    return Ok((DurableToolStep::Complete(Outcome::Failed("normalized arguments do not match schema".into())), serde_json::Value::Null, unsafe_first));
                }
                return Err("normalized arguments do not match schema".into());
            }
            let result = self
                .tool
                .execute_durable(
                    args.clone(),
                    ToolContext {
                        operation_id: task.id(),
                        cancellation: cancellation.clone(),
                    },
                    resume,
                    &host,
                )
                .await?;
            Ok::<_, String>((result, args, false))
        })?;
        if !self.replay_safe && !no_dispatch {
            admission.capture_unsafe_durable_outcome(&intent, &result, &args)?;
            #[cfg(test)]
            {
                let hook = park_hooks().lock().unwrap().remove(&task.id());
                if let Some(hook) = hook {
                    hook();
                }
            }
        }
        if !self
            .identity
            .matches(self.tool.as_ref(), &self.implementation, self.version)
        {
            return Err("tool identity changed during execution".into());
        }
        if serde_json::to_vec(&result)
            .map_err(|e| e.to_string())?
            .len()
            > 65536
        {
            return Err("durable result too large".into());
        }
        let step = match &result {
            DurableToolStep::Complete(Outcome::Pending { .. } | Outcome::Unknown(_)) => {
                return Err("uncertain tool outcome requires reconciliation".into());
            }
            DurableToolStep::Complete(outcome) if self.hook.is_some() => {
                let data = serde_json::to_value(HookCheckpoint {
                    version: 1,
                    arguments: args.clone(),
                    outcome: Some(outcome.clone()),
                    host_error: None,
                })
                .map_err(|e| e.to_string())?;
                if serde_json::to_vec(&data).map_err(|e| e.to_string())?.len() > 65536 {
                    return Err("after checkpoint too large".into());
                }
                Step::Continue(Checkpoint {
                    phase: "replay-safe-async-after".into(),
                    data,
                })
            }
            DurableToolStep::Complete(outcome) => {
                Step::Succeed(serde_json::to_value(outcome).map_err(|e| e.to_string())?)
            }
            DurableToolStep::Park { checkpoint, wait } => {
                let condition = match wait {
                    DurableWait::All(calls) => WaitFor::Tasks(host.validate_wait(calls)?),
                    DurableWait::Any(calls) => WaitFor::TasksAny(host.validate_wait(calls)?),
                    #[cfg(test)]
                    DurableWait::Signal(_) => return Err("signal waits are unsupported".into()),
                };
                #[cfg(test)]
                let legacy_fixture = std::env::var_os("PI_TEST_LEGACY_PARK_CANCEL").is_some();
                #[cfg(not(test))]
                let legacy_fixture = false;
                let proven_park = (self.park_cancel.is_some()
                    || self._conversation_cancel.is_some())
                    && !legacy_fixture;
                let data = serde_json::to_value(ParkCheckpoint {
                    version: if proven_park { 2 } else { 1 },
                    arguments: args.clone(),
                    checkpoint: checkpoint.clone(),
                    park_attempt: if proven_park {
                        Some(task.attempt_id().ok_or("missing park attempt")?)
                    } else {
                        None
                    },
                })
                .map_err(|e| e.to_string())?;
                if serde_json::to_vec(&data).map_err(|e| e.to_string())?.len() > 65536 {
                    return Err("durable checkpoint too large".into());
                }
                Step::Wait {
                    checkpoint: Checkpoint {
                        phase: "replay-safe-async-wait".into(),
                        data,
                    },
                    condition,
                    children: vec![],
                }
            }
        };
        let docs = host.docs.into_inner().map_err(|e| e.to_string())?;
        if no_dispatch {
            let DurableToolStep::Complete(outcome) = &result else {
                return Err("invalid no-dispatch result".into());
            };
            docs.stage_unsafe_no_dispatch(&intent, outcome)?;
        }
        docs.stage_checkpointable_durable_result(intent, &result)?;
        if !self.replay_safe
            && let Step::Wait {
                checkpoint,
                condition,
                ..
            } = &step
        {
            docs.stage_unsafe_park_proof(checkpoint, condition)?;
        }
        #[cfg(test)]
        crash_test_boundary(&self.tool.spec().name, task.id(), "result");
        Ok(step)
    }
}

#[cfg(test)]
mod evidence_tests {
    use super::*;
    #[test]
    fn rejects_ambiguous_or_malformed_after_evidence() {
        let checkpoint = |outcome: Outcome, host_error: Option<&str>| {
            serde_json::to_value(HookCheckpoint {
                version: 1,
                arguments: serde_json::json!({}),
                outcome: Some(outcome),
                host_error: host_error.map(str::to_owned),
            })
            .unwrap()
        };
        assert!(
            after_checkpoint_evidence(&checkpoint(Outcome::Unknown("uncertain".into()), None))
                .is_err()
        );
        assert!(
            after_checkpoint_evidence(&checkpoint(
                Outcome::Succeeded("done".into()),
                Some("blocked")
            ))
            .is_err()
        );
        assert!(
            after_checkpoint_evidence(&checkpoint(Outcome::Failed("bad".into()), Some("unknown")))
                .is_err()
        );
        assert!(
            after_checkpoint_evidence(
                &serde_json::json!({"version":1,"arguments":{},"outcome":null})
            )
            .is_err()
        );
    }
}

#[cfg(all(test, unix))]
mod conversation_receipt_kill_tests {
    use super::*;
    use futures_util::{StreamExt, stream};
    use pi_rs_core::{
        Error, Message, Model, ModelEvent, ModelRequest, ModelStream, ToolCall, ToolSpec,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct ModelStub;
    #[async_trait::async_trait]
    impl Model for ModelStub {
        async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
            let done = request
                .messages
                .iter()
                .any(|message| matches!(message, Message::Tool { .. }));
            Ok(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: if done {
                    vec![]
                } else {
                    vec![ToolCall {
                        id: "call".into(),
                        name: "root".into(),
                        arguments: "{}".into(),
                    }]
                },
                usage: None,
            })])
            .boxed())
        }
    }
    struct Root(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl Tool for Root {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "root".into(),
                description: "".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
            unreachable!()
        }
    }
    #[async_trait::async_trait]
    impl DurableAsyncTool for Root {
        async fn execute_durable(
            &self,
            _: serde_json::Value,
            _: ToolContext,
            _: DurableToolResume,
            _: &dyn DurableTaskHost,
        ) -> Result<DurableToolStep, String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DurableToolStep::Complete(Outcome::Succeeded(
                "external-result".into(),
            )))
        }
    }
    fn setup(
        calls: Arc<AtomicUsize>,
    ) -> (crate::runtime::Options, crate::task_scheduler::Registry) {
        let mut registry = crate::task_scheduler::Registry::default();
        let adapter = crate::async_task_tool::register_conversation_unsafe_durable_tool(&mut registry, Arc::new(Root(calls)), "receipt-boundary", 1, vec![], crate::async_task_tool::UnsafeDurableAuthorization::interruption_requires_reconciliation()).unwrap();
        (
            crate::runtime::Options {
                model: Arc::new(ModelStub),
                tools: vec![adapter.register()],
                restart_limit: 4,
                max_turns: 4,
                uncertain_policy: crate::recovery::UncertainPolicy::RequireReconciliation,
            },
            registry,
        )
    }
    #[tokio::test]
    async fn worker() {
        let Some(dir) = std::env::var_os("PI_TEST_UNSAFE_CONV_RECEIPT_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let (options, registry) = setup(Arc::new(AtomicUsize::new(0)));
        let mut scheduler = crate::runtime::Scheduler::open(dir.join("db"), options).unwrap();
        let _driver = scheduler.start_tasks(registry).unwrap();
        let id = scheduler
            .create_conversation(crate::runtime::Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        std::fs::write(dir.join("conversation"), id.to_string()).unwrap();
        scheduler.submit(id, None, "work".into()).unwrap();
        std::future::pending::<()>().await;
    }
    #[tokio::test]
    async fn kill_after_unsafe_receipt_preserves_evidence_without_replay() {
        struct Kill(std::process::Child);
        impl Drop for Kill {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut child = Kill(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "async_task_tool::replay_safe::conversation_receipt_kill_tests::worker",
                ])
                .env("PI_TEST_UNSAFE_CONV_RECEIPT_DIR", dir.path())
                .env("PI_TEST_REPLAY_SAFE_DIR", dir.path())
                .env("PI_TEST_REPLAY_SAFE_CUT", "result")
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(15), async {
            while !dir.path().join("cut").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let task_id: uuid::Uuid = std::fs::read_to_string(dir.path().join("cut"))
            .unwrap()
            .parse()
            .unwrap();
        let id: uuid::Uuid = std::fs::read_to_string(dir.path().join("conversation"))
            .unwrap()
            .parse()
            .unwrap();
        let store = durable_store::Store::open(&path).unwrap();
        let receipts =
            crate::async_task_tool::read_unsafe_durable_receipts(&store, task_id).unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].outcome,
            Outcome::Succeeded("external-result".into())
        );
        let task = crate::task_store::TaskStore::new(&store)
            .load(task_id)
            .unwrap()
            .unwrap()
            .task;
        assert!(matches!(task.state(), crate::task::TaskState::Running));
        drop(store);
        let calls = Arc::new(AtomicUsize::new(0));
        let (options, registry) = setup(calls.clone());
        let mut scheduler = crate::runtime::Scheduler::open(&path, options).unwrap();
        let driver = scheduler.start_tasks(registry).unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let store = durable_store::Store::open(&path).unwrap();
                let task = crate::task_store::TaskStore::new(&store)
                    .load(task_id)
                    .unwrap()
                    .unwrap()
                    .task;
                if task.is_terminal() {
                    assert!(matches!(task.state(), crate::task::TaskState::Failed(_)));
                    assert_eq!(
                        crate::async_task_tool::read_unsafe_durable_receipts(&store, task_id)
                            .unwrap(),
                        receipts
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(15), scheduler.wait(id))
            .await
            .unwrap()
            .unwrap();
        assert!(
            scheduler
                .blocked(id)
                .unwrap()
                .is_some_and(|(_, reason)| reason.contains("reconciliation"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            !scheduler
                .conversation(id)
                .unwrap()
                .messages
                .iter()
                .any(|message| matches!(message, Message::Tool { .. }))
        );
        let _ = driver.shutdown_with_pending();
        let _ = scheduler.shutdown().await;
    }
}
