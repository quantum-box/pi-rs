//! Standalone checkpointable tools whose host explicitly guarantees replay safety.
use super::{
    AsyncTaskCall, AsyncTaskResult, DurableAsyncTool, DurableTaskHost, DurableToolResume,
    DurableToolStep, DurableWait, ToolIdentity,
};
use crate::task::{Checkpoint, Task, WaitFor};
use crate::task_scheduler::{AdmissionHandle, Handler, Scheduler, Step};
use crate::task_tool::{NestedTaskResult, TaskTool};
use pi_rs_core::{Outcome, Tool, ToolContext};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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

pub(super) fn register(
    scheduler: &mut Scheduler,
    tool: Arc<dyn DurableAsyncTool>,
    implementation: &str,
    version: u32,
    children: Vec<Arc<TaskTool>>,
    hook: Option<(String, Arc<dyn crate::extension::Hook>)>,
) -> Result<Arc<TaskTool>, String> {
    let mut identity = ToolIdentity::new(tool.as_ref(), implementation, version)?;
    if let Some((id, _)) = &hook {
        identity = identity.with_before_hook(id.clone())?.with_after_hook();
    }
    jsonschema::validator_for(&tool.spec().parameters).map_err(|_| "invalid tool schema")?;
    let mut names = HashMap::new();
    let mut child_identities = Vec::new();
    for child in children {
        let id = child.identity().ok_or("invalid child identity")?;
        if !id.replay_safe || child.is_direct() {
            return Err("only replay-safe task-backed children are supported".into());
        }
        let name = child.spec().name;
        if name.is_empty() || names.insert(name.clone(), child).is_some() {
            return Err("duplicate or empty child name".into());
        }
        child_identities.push((name, id));
    }
    child_identities.sort_by(|a, b| a.0.cmp(&b.0));
    let encoded =
        serde_json::to_vec(&(1u32, &identity, &child_identities)).map_err(|e| e.to_string())?;
    if encoded.len() > 65536 {
        return Err("definition identity too large".into());
    }
    let hash = Sha256::digest(encoded);
    let kind = format!("pi-rs.replay-safe-async.v1.{hash:x}");
    let spec = tool.spec();
    let handler: Arc<dyn Handler> = Arc::new(Executor {
        tool,
        identity,
        implementation: implementation.into(),
        version,
        children: names,
        hook: hook.map(|(_, hook)| hook),
    });
    scheduler.install_replay_safe_async_handler(kind.clone(), version, handler.clone())?;
    Ok(Arc::new(
        TaskTool::new(spec, kind, true, version, handler).with_deferred_arguments(),
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
struct HookMemo<'a, 'b> {
    admission: &'a AdmissionHandle<'b>,
    cancellation: &'a pi_rs_core::CancellationToken,
    failed: Mutex<bool>,
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
        if *failed {
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
        if result.is_err() {
            *failed = true;
        }
        result
    }
    fn failed(&self) -> bool {
        self.failed.lock().map_or(true, |failed| *failed)
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

struct Executor {
    tool: Arc<dyn DurableAsyncTool>,
    identity: ToolIdentity,
    implementation: String,
    version: u32,
    children: HashMap<String, Arc<TaskTool>>,
    hook: Option<Arc<dyn crate::extension::Hook>>,
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
        Ok(match child.0.result(&self.task, &mut docs)? {
            None => None,
            Some(NestedTaskResult::Succeeded(value)) => {
                let outcome: Outcome = serde_json::from_value(value)
                    .map_err(|_| "child result is not a Tool outcome")?;
                Some(
                    if matches!(outcome, Outcome::Pending { .. } | Outcome::Unknown(_)) {
                        AsyncTaskResult::Uncertain(outcome)
                    } else {
                        AsyncTaskResult::Outcome(outcome)
                    },
                )
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
        if !matches!(code, "blocked" | "invalid_arguments") {
            return Err("unsupported host result".into());
        }
        let reason: String = reason.chars().take(2048).collect();
        let message = if code == "blocked" {
            format!("Tool call blocked: {reason}")
        } else {
            format!("Invalid tool arguments: {reason}")
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
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(false),
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
        })?;
        if memo.failed() {
            return Err("task-fenced hook memo failed".into());
        }
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
        if stored.host_error.is_none() {
            self.validate_hook_arguments(&stored.arguments)?;
        }
        if stored
            .host_error
            .as_deref()
            .is_some_and(|code| !matches!(code, "blocked" | "invalid_arguments"))
        {
            return Err("invalid host result code".into());
        }
        let outcome = stored.outcome.ok_or("missing completed outcome")?;
        if stored.host_error.is_some() && !matches!(outcome, Outcome::Failed(_)) {
            return Err("invalid host result outcome".into());
        }
        let diagnostic_message = match &outcome {
            Outcome::Failed(reason) => reason.clone(),
            _ => String::new(),
        };
        let text = outcome.tool_content();
        let memo = HookMemo {
            admission,
            cancellation: cancel,
            failed: Mutex::new(false),
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
        })?;
        if memo.failed() {
            return Err("task-fenced hook memo failed".into());
        }
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
}

impl Handler for Executor {
    fn run(&self, _: &Task) -> Result<Step, String> {
        Err("driver reservation required".into())
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
        if self.hook.is_some() && task.checkpoint().phase == "initial" {
            return self.before(task, admission, cancellation);
        }
        if self.hook.is_some() && task.checkpoint().phase == "replay-safe-async-after" {
            return self.after(task, admission, cancellation);
        }
        let intent = admission.replay_safe_durable_intent(false, Some(cancellation))?;
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
                return Err("uncertain outcomes are unsupported for replay-safe tools".into());
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
        docs.stage_replay_safe_durable_result(intent, &result)?;
        #[cfg(test)]
        crash_test_boundary(&self.tool.spec().name, task.id(), "result");
        Ok(step)
    }
}
