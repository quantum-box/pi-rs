use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::{
    CancellationToken, Error, ExtensionState, ModelRequest, Outcome, Result, ToolReply, ToolSpec,
};

pub type ExtensionResult<T> = std::result::Result<T, String>;
pub type ExtensionStates = BTreeMap<String, ExtensionState>;

pub struct ToolContext {
    /// Persisted before execution. Pass this to external systems as an idempotency or lookup key.
    pub operation_id: Uuid,
    pub cancellation: CancellationToken,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecution {
    #[default]
    Sequential,
    /// The host attests this tool has no effects and is safe to overlap.
    ParallelRead,
    /// An external job may return a pending operation reference.
    ExternalJob,
}

/// Mediates nested tool calls through the same journal and policies as model calls.
#[async_trait::async_trait]
pub trait ToolHost: Send + Sync {
    fn tools(&self) -> Vec<ToolSpec>;
    async fn call(&self, name: &str, arguments: Value) -> Outcome;
    /// Submit an ordered group together so compatible reads can overlap.
    async fn call_batch(&self, calls: Vec<(String, Value)>) -> Vec<Outcome> {
        futures_util::future::join_all(
            calls
                .into_iter()
                .map(|(name, arguments)| async move { self.call(&name, arguments).await }),
        )
        .await
    }
}

#[async_trait::async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn execution(&self) -> ToolExecution {
        ToolExecution::Sequential
    }
    /// Maximum serialized result bytes, bounded by the agent's 8 MiB ceiling.
    fn output_limit(&self) -> usize {
        1024 * 1024
    }
    async fn invoke(&self, arguments: Value, context: ToolContext) -> Outcome;
    /// Invoke with this tool's owning extension state from the active branch.
    async fn invoke_with_state(
        &self,
        arguments: Value,
        context: ToolContext,
        _state: Option<&ExtensionState>,
    ) -> Outcome {
        self.invoke(arguments, context).await
    }
    /// Override for tools that orchestrate other tools, such as codemode.
    async fn invoke_with_host(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> Outcome {
        self.invoke_with_state(arguments, context, state).await
    }
    /// Return output and a private owning-state update for one atomic journal commit.
    async fn invoke_transaction(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        self.invoke_with_host(arguments, context, state, host)
            .await
            .into()
    }
    /// Programmatic callers may request a structured form of a tool's output.
    async fn invoke_programmatic(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        self.invoke_transaction(arguments, context, state, host)
            .await
    }
    /// Explicitly opt into automatic pending-job polling.
    fn can_poll(&self) -> bool {
        false
    }
    /// Read an accepted job's state without dispatching the original operation again.
    /// None leaves reconciliation to the host. Cancellation must preserve the job reference.
    async fn poll(&self, _reference: &str, _context: ToolContext) -> Option<Outcome> {
        None
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny(String),
}

pub struct Compaction {
    pub summary: String,
    /// Retain this many recent user turns, keeping complete assistant/tool groups.
    pub keep_turns: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestPurpose {
    Turn,
    Summary,
}

/// Hooks run in registration order. The first denial stops pre-tool hooks.
/// An error stops the run. A post-tool error commits the known result without state changes.
/// Post-hooks prepare state updates and must not perform external operations.
#[async_trait::async_trait]
pub trait Extension: Send + Sync {
    fn id(&self) -> &str;

    fn dependencies(&self) -> &[&str] {
        &[]
    }

    /// Dependency names owned by a runtime-loaded extension.
    fn dependency_ids(&self) -> Vec<&str> {
        self.dependencies().to_vec()
    }

    fn commands(&self) -> &[&str] {
        &[]
    }

    /// Command names owned by a runtime-loaded extension.
    fn command_names(&self) -> Vec<&str> {
        self.commands().to_vec()
    }

    /// Host-only command. Returns a state change to commit on the active branch.
    fn command(
        &self,
        _name: &str,
        _arguments: &str,
        _previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        Err("extension command unavailable".into())
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        Vec::new()
    }

    /// Release long-lived clients and subprocesses owned by this extension.
    async fn shutdown(&self) -> ExtensionResult<()> {
        Ok(())
    }

    /// Return the new value only when a state migration is needed.
    fn migrate(
        &self,
        _previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        Ok(None)
    }

    fn context(
        &self,
        _request: &mut ModelRequest,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        Ok(())
    }

    async fn before_request(
        &self,
        request: &mut ModelRequest,
        _purpose: RequestPurpose,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        self.context(request, state)
    }

    async fn before_tool(
        &self,
        _name: &str,
        _args: &Value,
        _context: &ToolContext,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        Ok(Decision::Allow)
    }

    fn after_tool(
        &self,
        _name: &str,
        _outcome: &Outcome,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        Ok(None)
    }

    fn before_compaction(
        &self,
        _compaction: &mut Compaction,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        Ok(Decision::Allow)
    }

    fn after_compaction(
        &self,
        _compaction: &Compaction,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        Ok(None)
    }
}

pub(crate) struct RegisteredTool {
    pub owner: String,
    pub tool: Arc<dyn Tool>,
    pub spec: ToolSpec,
    pub validator: jsonschema::Validator,
}

#[derive(Default)]
pub struct Registry {
    pub(crate) extensions: Vec<Arc<dyn Extension>>,
    pub(crate) tools: BTreeMap<String, Arc<RegisteredTool>>,
    active: Option<BTreeSet<String>>,
}

impl Registry {
    /// Compose enabled extensions in dependency order, preserving input order
    /// among extensions whose dependencies are already satisfied.
    pub fn compose(
        extensions: Vec<Arc<dyn Extension>>,
        disabled: &BTreeSet<String>,
    ) -> Result<Self> {
        let ids: BTreeSet<_> = extensions.iter().map(|extension| extension.id()).collect();
        if ids.len() != extensions.len() || ids.contains("") {
            return Err(Error::Config("invalid or duplicate extension ID".into()));
        }
        if disabled.iter().any(|id| !ids.contains(id.as_str())) {
            return Err(Error::Config("unknown disabled extension ID".into()));
        }
        let mut pending: Vec<_> = extensions
            .into_iter()
            .filter(|extension| !disabled.contains(extension.id()))
            .collect();
        let mut registry = Self::default();
        while !pending.is_empty() {
            let index = pending
                .iter()
                .position(|extension| {
                    extension.dependency_ids().iter().all(|dependency| {
                        registry
                            .extensions
                            .iter()
                            .any(|item| item.id() == *dependency)
                    })
                })
                .ok_or_else(|| {
                    Error::Config("extension dependencies are missing, disabled, or cyclic".into())
                })?;
            registry.register(pending.remove(index))?;
        }
        Ok(registry)
    }

    /// Restrict the advertised and executable tools while retaining extension hooks.
    pub fn retain_tools(&mut self, mut keep: impl FnMut(&ToolSpec) -> bool) {
        self.tools.retain(|_, tool| keep(&tool.spec));
    }

    /// Select model-facing tools while retaining callable tools for orchestration.
    pub fn set_active_tools(&mut self, names: impl IntoIterator<Item = String>) -> Result<()> {
        let names: BTreeSet<_> = names.into_iter().collect();
        if names.iter().any(|name| !self.has_tool(name)) {
            return Err(Error::Config("active tool is not registered".into()));
        }
        self.active = Some(names);
        Ok(())
    }
    pub fn active_tool_specs(&self) -> Vec<ToolSpec> {
        self.tools
            .iter()
            .filter(|(name, _)| {
                self.active
                    .as_ref()
                    .is_none_or(|active| active.contains(*name))
            })
            .map(|(_, tool)| tool.spec.clone())
            .collect()
    }
    pub fn tool_specs(&self) -> Vec<ToolSpec> {
        self.tools.values().map(|tool| tool.spec.clone()).collect()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut failure = None;
        for extension in self.extensions.iter().rev() {
            if let Err(error) = hook(extension.as_ref(), extension.shutdown().await) {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub fn has_tool(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    pub fn is_read_only(&self) -> bool {
        self.tools
            .values()
            .all(|tool| tool.tool.execution() == ToolExecution::ParallelRead)
    }

    /// Build a child tool profile with the same policy hooks and only
    /// explicitly allowed tools attested to be read-only by the host.
    pub fn read_only_subset(&self, names: &BTreeSet<String>) -> Result<Self> {
        let mut tools = BTreeMap::new();
        for name in names {
            let tool = self
                .tools
                .get(name)
                .filter(|tool| tool.tool.execution() == ToolExecution::ParallelRead)
                .ok_or_else(|| {
                    Error::Config("review requested a tool outside the read-only profile".into())
                })?;
            tools.insert(name.clone(), tool.clone());
        }
        Ok(Self {
            extensions: self.extensions.clone(),
            tools,
            active: None,
        })
    }

    pub fn register(&mut self, extension: Arc<dyn Extension>) -> Result<()> {
        let id = extension.id();
        if id.is_empty() || self.extensions.iter().any(|item| item.id() == id) {
            return Err(Error::Config(format!(
                "invalid or duplicate extension ID {id:?}"
            )));
        }
        if extension
            .dependency_ids()
            .iter()
            .any(|dependency| !self.extensions.iter().any(|item| item.id() == *dependency))
        {
            return Err(Error::Config(format!(
                "extension {id:?} has an unavailable dependency"
            )));
        }
        let command_names = extension.command_names();
        let commands: BTreeSet<_> = command_names.iter().copied().collect();
        if commands.contains("") || commands.len() != command_names.len() {
            return Err(Error::Config(format!(
                "extension {id:?} has invalid or duplicate commands"
            )));
        }
        let mut tools = BTreeMap::new();
        for tool in extension.tools() {
            let spec = tool.spec();
            let name = &spec.name;
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || self.tools.contains_key(name)
                || tools.contains_key(name)
            {
                return Err(Error::Config(format!(
                    "invalid or duplicate tool name {name:?}"
                )));
            }
            if spec.parameters.get("type").and_then(Value::as_str) != Some("object") {
                return Err(Error::Config(format!(
                    "tool {name} requires an object schema"
                )));
            }
            let validator = jsonschema::validator_for(&spec.parameters)
                .map_err(|_| Error::Config(format!("invalid JSON Schema for {name}")))?;
            tools.insert(
                name.clone(),
                Arc::new(RegisteredTool {
                    owner: extension.id().to_owned(),
                    tool,
                    spec,
                    validator,
                }),
            );
        }
        self.tools.extend(tools);
        self.extensions.push(extension);
        Ok(())
    }
}

pub(crate) fn hook<T>(extension: &dyn Extension, result: ExtensionResult<T>) -> Result<T> {
    result.map_err(|message| Error::Extension {
        id: extension.id().into(),
        message,
    })
}
