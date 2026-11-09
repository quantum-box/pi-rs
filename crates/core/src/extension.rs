use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::{CancellationToken, Error, ExtensionState, ModelRequest, Outcome, Result, ToolSpec};

pub type ExtensionResult<T> = std::result::Result<T, String>;
pub type ExtensionStates = BTreeMap<String, ExtensionState>;

pub struct ToolContext {
    /// Persisted before execution. Pass this to external systems as an idempotency or lookup key.
    pub operation_id: Uuid,
    pub cancellation: CancellationToken,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ToolExecution {
    #[default]
    Sequential,
    /// The host attests this tool has no effects and is safe to overlap.
    ParallelRead,
    /// An external job may return a pending operation reference.
    ExternalJob,
}

#[async_trait::async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn execution(&self) -> ToolExecution {
        ToolExecution::Sequential
    }
    async fn invoke(&self, arguments: Value, context: ToolContext) -> Outcome;
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

    fn commands(&self) -> &[&str] {
        &[]
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
    pub tool: Arc<dyn Tool>,
    pub spec: ToolSpec,
    pub validator: jsonschema::Validator,
}

#[derive(Default)]
pub struct Registry {
    pub(crate) extensions: Vec<Arc<dyn Extension>>,
    pub(crate) tools: BTreeMap<String, Arc<RegisteredTool>>,
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
                    extension.dependencies().iter().all(|dependency| {
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

    pub fn has_tool(&self, name: &str) -> bool {
        self.tools.contains_key(name)
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
            .dependencies()
            .iter()
            .any(|dependency| !self.extensions.iter().any(|item| item.id() == *dependency))
        {
            return Err(Error::Config(format!(
                "extension {id:?} has an unavailable dependency"
            )));
        }
        let commands: BTreeSet<_> = extension.commands().iter().copied().collect();
        if commands.contains("") || commands.len() != extension.commands().len() {
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
