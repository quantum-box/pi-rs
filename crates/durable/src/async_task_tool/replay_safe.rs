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
) -> Result<Arc<TaskTool>, String> {
    let identity = ToolIdentity::new(tool.as_ref(), implementation, version)?;
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

struct Executor {
    tool: Arc<dyn DurableAsyncTool>,
    identity: ToolIdentity,
    implementation: String,
    version: u32,
    children: HashMap<String, Arc<TaskTool>>,
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
                None => self
                    .tool
                    .prepare_arguments(task.input().clone())
                    .await
                    .map_err(|e| e.to_string())?,
            };
            if !args.is_object()
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
