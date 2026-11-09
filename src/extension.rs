use std::collections::BTreeMap;
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

#[async_trait::async_trait]
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
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

/// Hooks run in registration order. The first denial stops pre-tool hooks.
/// An error stops the run. A post-tool error commits the known result without state changes.
/// Post-hooks prepare state updates and must not perform external operations.
#[async_trait::async_trait]
pub trait Extension: Send + Sync {
    fn id(&self) -> &str;

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
    pub(crate) tools: BTreeMap<String, RegisteredTool>,
}

impl Registry {
    pub fn register(&mut self, extension: Arc<dyn Extension>) -> Result<()> {
        let id = extension.id();
        if id.is_empty() || self.extensions.iter().any(|item| item.id() == id) {
            return Err(Error::Config(format!(
                "invalid or duplicate extension ID {id:?}"
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
                RegisteredTool {
                    tool,
                    spec,
                    validator,
                },
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
