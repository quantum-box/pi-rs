//! Embed a single Rust turn in an AIP-owned execution. The host owns persistence,
//! permission checks, charging, and continuation. This crate holds no credentials.
mod memory;
mod model;
mod tools;

pub use memory::*;
pub use model::*;
pub use tools::*;

use pi_rs_core::{
    Agent, CancellationToken, Error, Limits, Model, Observer, QueuedInput, Registry, Result,
    Session,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunScope {
    pub tenant: String,
    pub actor: String,
    pub workspace: String,
    /// Stable journal run ID assigned by AIP. It is reused on explicit resume.
    pub journal_run_id: Uuid,
    pub aip_run: String,
    pub parent_run: Option<String>,
    pub audit_id: String,
}

impl RunScope {
    pub fn bind(&self, session: &mut Session) -> Result<()> {
        if [
            &self.tenant,
            &self.actor,
            &self.workspace,
            &self.aip_run,
            &self.audit_id,
        ]
        .into_iter()
        .any(|value| value.trim().is_empty())
            || self.journal_run_id.is_nil()
        {
            return Err(Error::Config("AIP execution scope is incomplete".into()));
        }
        let binding = serde_json::to_vec(&(&self.tenant, &self.actor, &self.workspace))?;
        session.bind_host(&format!("{:x}", Sha256::digest(binding)))?;
        let identity = serde_json::to_vec(&(&self.aip_run, &self.parent_run, &self.audit_id))?;
        session.bind_host_run(
            self.journal_run_id,
            &format!("{:x}", Sha256::digest(identity)),
        )
    }
}

pub struct HostRegistry {
    scope: RunScope,
    registry: Registry,
}

impl HostRegistry {
    pub async fn discover(
        scope: &RunScope,
        port: std::sync::Arc<dyn DelegatedPort>,
        names: std::collections::BTreeSet<String>,
        mut extensions: Vec<std::sync::Arc<dyn pi_rs_core::Extension>>,
    ) -> Result<Self> {
        if extensions
            .iter()
            .any(|extension| !extension.tools().is_empty())
        {
            return Err(Error::Config(
                "AIP tools must come from the delegated profile".into(),
            ));
        }
        extensions.push(std::sync::Arc::new(
            DelegatedTools::discover(scope, port, names).await?,
        ));
        let registry = Registry::compose(extensions, &Default::default())?;
        if registry.has_tool("execute_command") {
            return Err(Error::Config(
                "AIP must delegate commands to Coding Job".into(),
            ));
        }
        Ok(Self {
            scope: scope.clone(),
            registry,
        })
    }
}

pub struct HostTurn<'a> {
    pub scope: &'a RunScope,
    pub budget: &'a dyn BudgetPort,
    pub model: &'a dyn Model,
    pub model_name: &'a str,
    pub registry: &'a HostRegistry,
    pub system_prompt: &'a str,
    pub limits: Limits,
}

impl HostTurn<'_> {
    /// Apply a host command to the same authenticated journal as the turn.
    pub fn extension_command(
        &self,
        session: &mut Session,
        extension_id: &str,
        command: &str,
        arguments: &str,
        observer: &mut Observer<'_>,
    ) -> Result<()> {
        self.check_profile(session)?;
        Agent {
            model: self.model,
            model_name: self.model_name,
            registry: &self.registry.registry,
            system_prompt: self.system_prompt,
            limits: self.limits.clone(),
        }
        .extension_command(session, extension_id, command, arguments, observer)
    }

    fn check_profile(&self, session: &mut Session) -> Result<()> {
        if self.registry.scope != *self.scope {
            return Err(Error::Config(
                "tool profile belongs to a different AIP run".into(),
            ));
        }
        self.scope.bind(session)
    }

    pub async fn run(
        &self,
        session: &mut Session,
        input: Option<String>,
        followups: &mut mpsc::Receiver<QueuedInput>,
        cancel: &CancellationToken,
        observer: &mut Observer<'_>,
    ) -> Result<String> {
        self.check_profile(session)?;
        let model = BudgetedModel {
            scope: self.scope,
            inner: self.model,
            budget: self.budget,
        };
        model.settle_session(session).await?;
        let agent = Agent {
            model: &model,
            model_name: self.model_name,
            registry: &self.registry.registry,
            system_prompt: self.system_prompt,
            limits: self.limits.clone(),
        };
        let result = agent
            .run_correlated(
                session,
                self.scope.journal_run_id,
                input,
                followups,
                cancel,
                observer,
            )
            .await;
        model.settle_session(session).await?;
        result
    }
}
