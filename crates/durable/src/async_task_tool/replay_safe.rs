//! Standalone checkpointable tools with an explicitly registered replay policy.
use super::{
    AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableTaskResult,
    DurableToolResume, DurableToolStep, DurableWait, ToolIdentity,
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
) -> Result<Arc<TaskTool>, String> {
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
    let encoded = serde_json::to_vec(&(
        if unavailable {
            7u32
        } else if restart_after {
            6u32
        } else if abort_after {
            5u32
        } else if block_results {
            4u32
        } else {
            1u32
        },
        &identity,
        &child_identities,
    ))
    .map_err(|e| e.to_string())?;
    if encoded.len() > 65536 {
        return Err("definition identity too large".into());
    }
    let hash = Sha256::digest(encoded);
    let kind = if replay_safe {
        format!("pi-rs.replay-safe-async.v1.{hash:x}")
    } else if hook.is_some() {
        format!("pi-rs.unsafe-park-async.v1.hook.{hash:x}")
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ParkCheckpoint {
    version: u32,
    arguments: serde_json::Value,
    checkpoint: serde_json::Value,
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
pub(super) fn after_checkpoint_evidence(
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
                "blocked" | "invalid_arguments" | "tool_unavailable"
            ) && matches!(outcome, Outcome::Failed(_)) =>
        {
            Ok(super::ReplaySafeAfterEvidence::HostError { code, outcome })
        }
        None => Ok(super::ReplaySafeAfterEvidence::ToolOutcome(outcome)),
        _ => Err("invalid host error checkpoint".into()),
    }
}

struct HookMemo<'a, 'b> {
    admission: &'a AdmissionHandle<'b>,
    cancellation: &'a pi_rs_core::CancellationToken,
    failed: Mutex<Option<String>>,
}
impl HookMemo<'_, '_> {
    fn access(
        &self,
        key: &str,
        value: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, String> {
        // Serialize access with failure publication: a concurrent operation
        // cannot commit after another operation has failed.
        let mut failed = self.failed.lock().map_err(|_| "hook memo unavailable")?;
        if failed.is_some() {
            return Err("hook memo unavailable after failure".into());
        }
        let inserting = value.is_some();
        let result = self
            .admission
            .replay_safe_hook_memo(key, value, self.cancellation)
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
    fn failure(&self) -> Option<String> {
        self.failed.lock().map_or_else(
            |_| Some("hook memo unavailable".into()),
            |failed| failed.clone(),
        )
    }
}
#[async_trait::async_trait]
impl crate::extension::Memo for HookMemo<'_, '_> {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, String> {
        self.access(key, None)
    }
    async fn get_or_insert(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.access(key, Some(value))?
            .ok_or_else(|| "memo insertion failed".into())
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

fn bounded_diagnostic(message: &str) -> String {
    let mut end = message.len().min(4096);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

pub(super) fn validate_content_hook_result(
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
                Some("aborted" | "restart_limit" | "tool_unavailable")
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
                TaskState::Succeeded(value) => {
                    let result: Outcome = serde_json::from_value(value.clone()).map_err(|_| "invalid unsafe child result")?;
                    let raw = outcome.as_ref().ok_or("unsafe child result lacks receipt")?;
                    let expected = if task.checkpoint().phase == "replay-safe-async-after" { docs.read_unsafe_hook_final(&task, raw)? } else { raw.clone() };
                    if expected != result { return Err("unsafe child result lacks matching final evidence".into()); }
                    AsyncTaskResult::Outcome(result)
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
        if !matches!(code, "blocked" | "invalid_arguments" | "tool_unavailable") {
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
        admission: &AdmissionHandle<'_>,
        cancel: &pi_rs_core::CancellationToken,
    ) -> Result<Step, String> {
        admission.replay_safe_durable_check(cancel)?;
        if self.unavailable {
            return self.host_result_checkpoint(
                task.input().clone(),
                "tool_unavailable",
                &self.identity.spec.name,
            );
        }
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(None),
        };
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
                let snapshot = serde_json::Value::Null;
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
            return Err(if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY {
                error
            } else {
                "task-fenced hook memo failed".into()
            });
        }
        let before = before?;
        admission.replay_safe_durable_check(cancel)?;
        let (call, decision) = match before {
            BeforeResult::Invalid(args, reason) => {
                return self.host_result_checkpoint(args, "invalid_arguments", &reason);
            }
            BeforeResult::Decision(call, decision) => (call, decision),
        };
        match decision {
            crate::extension::BeforeTool::Pass => (),
            crate::extension::BeforeTool::Block(reason) => {
                return self.host_result_checkpoint(call.arguments, "blocked", &reason);
            }
            _ => return Err("hook sleep is unsupported".into()),
        }
        self.bounded_hook_arguments(&call.arguments)?;
        if let Err(reason) = self.validate_hook_arguments(&call.arguments) {
            return self.host_result_checkpoint(call.arguments, "invalid_arguments", &reason);
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
    fn after_blocks(
        &self,
        task: &Task,
        admission: &AdmissionHandle<'_>,
        cancel: &pi_rs_core::CancellationToken,
        args: serde_json::Value,
        outcome: Outcome,
        host_error: Option<String>,
    ) -> Result<Step, String> {
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
        let initial = crate::extension::ToolBlockResult {
            output: vec![serde_json::json!({"type":"text", "text":outcome.tool_content()})],
            diagnostics,
            extra,
        };
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(None),
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
            let snapshot = serde_json::Value::Null;
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
            return Err(if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY {
                error
            } else {
                "task-fenced hook memo failed".into()
            });
        }
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
        admission: &AdmissionHandle<'_>,
        cancel: &pi_rs_core::CancellationToken,
    ) -> Result<Step, String> {
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
            !matches!(code, "blocked" | "invalid_arguments" | "tool_unavailable")
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
            return Err(if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY {
                error
            } else {
                "task-fenced hook memo failed".into()
            });
        }
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
            "initial" => task.input().clone(),
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
    fn run(&self, _: &Task) -> Result<Step, String> {
        Err("driver reservation required".into())
    }
    fn run_abort_with_admission(
        &self,
        task: &Task,
        admission: &AdmissionHandle<'_>,
    ) -> Result<Step, String> {
        if task.holds_restart_cleanup() {
            return self.run_with_restart_cleanup(task, admission);
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
        let args = match task.checkpoint().phase.as_str() {
            "initial" => task.input().clone(),
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
                "initial" | "replay-safe-async-after"
            )
        {
            let result = if task.checkpoint().phase == "initial" {
                self.before(task, admission, cancellation)
            } else {
                self.after(task, admission, cancellation)
            };
            if !self
                .identity
                .matches(self.tool.as_ref(), &self.implementation, self.version)
            {
                return Err("tool identity changed during hook".into());
            }
            return match result {
                Err(error) if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY => {
                    Ok(Step::DeferUndispatched)
                }
                Ok(step) if !self.replay_safe => {
                    docs.stage_unsafe_hook_phase(&step)?;
                    Ok(step)
                }
                result => result,
            };
        }
        let intent = match admission.checkpointable_durable_intent(
            false,
            self.replay_safe,
            Some(cancellation),
        ) {
            Ok(intent) => intent,
            Err(error) if error == crate::task_scheduler::UNSAFE_ANCESTOR_BUSY => {
                return Ok(Step::DeferUndispatched);
            }
            Err(error) => return Err(error),
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
                if stored.version != 1 {
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
        let (result, args) = runtime.block_on(async {
            let args = match saved_args {
                Some(args) => args,
                None => tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err("argument preparation cancelled".into()),
                    prepared = self.tool.prepare_arguments(task.input().clone()) => prepared.map_err(|e| e.to_string())?,
                },
            };
            if cancellation.is_cancelled() { return Err("tool execution cancelled".into()); }
            if !args.is_object()
                || serde_json::to_vec(&args).map_err(|e| e.to_string())?.len() > 16 * 1024 * 1024
                || !jsonschema::validator_for(&self.tool.spec().parameters)
                    .is_ok_and(|v| v.is_valid(&args))
            {
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
            Ok::<_, String>((result, args))
        })?;
        if !self.replay_safe {
            admission.capture_unsafe_durable_outcome(&intent, &result)?;
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
                let data = serde_json::to_value(ParkCheckpoint {
                    version: 1,
                    arguments: args.clone(),
                    checkpoint: checkpoint.clone(),
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
