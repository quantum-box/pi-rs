use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::SystemTime;

use pi_rs_core::{
    CancellationToken, Error, Extension, OperationState, Outcome, Result, Session, Tool,
    ToolContext, ToolExecution, ToolSpec,
};
use serde_json::Value;
use uuid::Uuid;

use crate::RunScope;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Destination {
    CodingJob,
    WorkspaceRuntime,
    Mcp { server: String, tool: String },
    Business { operation: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Delegation {
    pub spec: ToolSpec,
    pub destination: Destination,
    pub read_only: bool,
    /// Authorization and execution bind to this server-owned resource/version.
    pub resource: String,
    pub version: String,
}

pub struct Grant {
    pub scope: RunScope,
    pub operation_id: Uuid,
    pub delegation: Delegation,
    pub expires_at: SystemTime,
    /// Service-issued authorization. Keep this out of journals and events.
    pub capability: String,
}

#[async_trait::async_trait]
pub trait DelegatedPort: Send + Sync {
    async fn discover(&self, scope: &RunScope, names: &BTreeSet<String>)
    -> Result<Vec<Delegation>>;
    /// Re-read actor rights, approval, and target version at dispatch.
    async fn authorize(
        &self,
        scope: &RunScope,
        delegation: &Delegation,
        args: &Value,
        operation_id: Uuid,
    ) -> Result<Option<Grant>>;
    /// The destination independently enforces the grant and idempotency key.
    async fn execute(&self, grant: Grant, args: Value, cancel: CancellationToken) -> Outcome;
    /// A cancellation request alone does not imply a terminal state.
    async fn cancel(
        &self,
        scope: &RunScope,
        operation_id: Uuid,
        reference: Option<&str>,
    ) -> Result<()>;
    async fn inspect(
        &self,
        scope: &RunScope,
        operation_id: Uuid,
        reference: Option<&str>,
    ) -> Result<Option<Outcome>>;
}

pub struct DelegatedTools {
    tools: Vec<Arc<dyn Tool>>,
}

impl DelegatedTools {
    /// Load only the explicitly selected operations. This profile registers no
    /// local command or filesystem execution.
    pub async fn discover(
        scope: &RunScope,
        port: Arc<dyn DelegatedPort>,
        names: BTreeSet<String>,
    ) -> Result<Self> {
        let mut tools = Vec::new();
        for delegation in port.discover(scope, &names).await? {
            if !names.contains(&delegation.spec.name)
                || delegation.resource.is_empty()
                || delegation.version.is_empty()
                || delegation.spec.name == "execute_command"
            {
                return Err(Error::Config(
                    "discovery returned an unavailable or unbound tool".into(),
                ));
            }
            tools.push(Arc::new(RemoteTool {
                scope: scope.clone(),
                port: port.clone(),
                delegation,
            }) as Arc<dyn Tool>);
        }
        Ok(Self { tools })
    }
}

impl Extension for DelegatedTools {
    fn id(&self) -> &str {
        "aip-delegated-tools"
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }
}

struct RemoteTool {
    scope: RunScope,
    port: Arc<dyn DelegatedPort>,
    delegation: Delegation,
}

#[async_trait::async_trait]
impl Tool for RemoteTool {
    fn spec(&self) -> ToolSpec {
        self.delegation.spec.clone()
    }

    fn execution(&self) -> ToolExecution {
        if self.delegation.read_only {
            ToolExecution::ParallelRead
        } else {
            ToolExecution::ExternalJob
        }
    }

    async fn invoke(&self, args: Value, context: ToolContext) -> Outcome {
        let decision = self
            .port
            .authorize(&self.scope, &self.delegation, &args, context.operation_id)
            .await;
        let Ok(Some(grant)) = decision else {
            return Outcome::Skipped("delegated authorization did not allow dispatch".into());
        };
        if grant.scope != self.scope
            || grant.operation_id != context.operation_id
            || grant.delegation != self.delegation
            || grant.capability.is_empty()
            || grant.expires_at <= SystemTime::now()
        {
            return Outcome::Skipped(
                "delegation scope, approval expiry, or target version changed".into(),
            );
        }
        if context.cancellation.is_cancelled() {
            return Outcome::Skipped("cancelled before delegated dispatch".into());
        }
        self.port.execute(grant, args, context.cancellation).await
    }
}

pub async fn reconcile(
    session: &mut Session,
    scope: &RunScope,
    port: &dyn DelegatedPort,
    operation_id: Uuid,
    cancel_remote: bool,
) -> Result<bool> {
    scope.bind(session)?;
    if !session.branch().iter().any(|record| {
        record.run_id == Some(scope.journal_run_id)
            && matches!(&record.event, pi_rs_core::Event::Assistant { invocations, .. }
                if invocations.iter().any(|invocation| invocation.operation_id == operation_id))
    }) {
        return Err(Error::Session(
            "operation belongs to a different AIP run".into(),
        ));
    }
    let state = session
        .operations()
        .into_iter()
        .find_map(|(invocation, state)| (invocation.operation_id == operation_id).then_some(state))
        .ok_or_else(|| Error::Session("operation is outside the active session branch".into()))?;
    let reference = match &state {
        OperationState::Started | OperationState::Finished(Outcome::Unknown(_)) => None,
        OperationState::Finished(Outcome::Pending { reference }) => Some(reference.as_str()),
        OperationState::Finished(_) => return Ok(true),
        OperationState::Planned => return Ok(false),
    };
    if cancel_remote {
        port.cancel(scope, operation_id, reference).await?;
    }
    let Some(outcome @ (Outcome::Succeeded(_) | Outcome::Failed(_))) =
        port.inspect(scope, operation_id, reference).await?
    else {
        return Ok(false);
    };
    session.reconcile(operation_id, outcome)?;
    Ok(true)
}
