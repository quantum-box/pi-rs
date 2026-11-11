use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use uuid::Uuid;

use crate::{
    CancellationToken, Error, ExtensionState, ModelRequest, Outcome, Result, ToolReply, ToolSpec,
};

pub type ExtensionResult<T> = std::result::Result<T, String>;
pub type ExtensionStates = BTreeMap<String, ExtensionState>;

/// Why the host is asking an extension for temporary resource paths.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceDiscoveryReason {
    Startup,
    Reload,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDiscoveryEvent {
    pub cwd: std::path::PathBuf,
    pub reason: ResourceDiscoveryReason,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredResources {
    #[serde(default)]
    pub skill_paths: Vec<std::path::PathBuf>,
    #[serde(default)]
    pub prompt_paths: Vec<std::path::PathBuf>,
    #[serde(default)]
    pub theme_paths: Vec<std::path::PathBuf>,
}

/// A handler's resources and its source, used to resolve relative paths.
#[derive(Clone, Debug)]
pub struct ExtensionResources {
    pub extension_id: String,
    pub source: Option<std::path::PathBuf>,
    pub resources: DiscoveredResources,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheWarmingAction {
    Warm,
    Stop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheWarmingPhase {
    Streaming,
    Idle,
}

/// The default cache refresh decision and the inputs visible to native extensions.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheWarmingDecision {
    pub session_id: Uuid,
    pub model: crate::ResolvedModelIdentity,
    pub phase: CacheWarmingPhase,
    pub prompt_tokens: u64,
    pub warm_cost: f64,
    pub miss_cost: f64,
    pub continuation_probability: f64,
    pub action: CacheWarmingAction,
}

/// A native extension's command-line option and its default value.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExtensionFlag {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub default: ExtensionFlagValue,
}

/// Command-line flag types supported by native extensions.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ExtensionFlagValue {
    Boolean(bool),
    String(String),
}

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
    fn has_ui(&self) -> bool {
        false
    }
    async fn ui(&self, _request: crate::UiRequest) -> ExtensionResult<crate::UiResponse> {
        Err("interactive extension UI is unavailable".into())
    }
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
    /// Normalize model arguments before schema validation and policy checks.
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        Ok(arguments)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputSource {
    Interactive,
    Rpc,
    Extension,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputEvent {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<crate::ImageContent>,
    pub source: InputSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streaming_behavior: Option<crate::InputKind>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum InputAction {
    Continue,
    Transform {
        text: String,
        /// Omitting images retains the attachments from the previous hook.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        images: Option<Vec<crate::ImageContent>>,
    },
    Handled,
}

/// A shell command entered by the user, before the host executes it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserBashEvent {
    pub command: String,
    pub exclude_from_context: bool,
    pub cwd: std::path::PathBuf,
}

/// A result supplied by an extension instead of executing the command locally.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserBashResult {
    #[serde(default)]
    pub output: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub full_output_path: Option<String>,
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

    /// Flags exposed to the host by this extension.
    fn cli_flags(&self) -> Vec<ExtensionFlag> {
        Vec::new()
    }

    /// Command names owned by a runtime-loaded extension.
    fn command_names(&self) -> Vec<&str> {
        self.commands().to_vec()
    }

    /// Optional metadata for command discovery.
    fn command_description(&self, _name: &str) -> Option<&str> {
        None
    }
    fn command_source(&self) -> Option<&std::path::Path> {
        None
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

    /// Opt in to receiving input before template expansion and model processing.
    fn handles_input(&self) -> bool {
        false
    }

    async fn input(
        &self,
        _event: &InputEvent,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<InputAction> {
        Ok(InputAction::Continue)
    }

    fn handles_user_bash(&self) -> bool {
        false
    }

    /// Return a result to replace execution, or None to pass to the next handler.
    async fn user_bash(
        &self,
        _event: &UserBashEvent,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<UserBashResult>> {
        Ok(None)
    }

    fn handles_lifecycle(&self) -> bool {
        false
    }

    /// Opt in to model and thinking-level selection events.
    fn handles_model_selection(&self) -> bool {
        false
    }

    /// Opt in to supplying temporary skills, prompt templates and themes.
    fn handles_resource_discovery(&self) -> bool {
        false
    }

    async fn discover_resources(
        &self,
        _event: &ResourceDiscoveryEvent,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<DiscoveredResources> {
        Ok(DiscoveredResources::default())
    }

    /// Opt in to session start and shutdown events in addition to run and turn events.
    fn handles_session_lifecycle(&self) -> bool {
        false
    }

    /// Observe a run or turn boundary after the preceding journal records are committed.
    async fn lifecycle(
        &self,
        _event: &crate::LifecycleEvent,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        Ok(())
    }

    fn handles_cache_warming(&self) -> bool {
        false
    }

    fn handles_tree_navigation(&self) -> bool {
        false
    }

    fn handles_session_transitions(&self) -> bool {
        false
    }

    /// Return true to cancel a session switch or fork before it writes anything.
    async fn before_session_transition(
        &self,
        _event: &crate::BeforeSessionTransition,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<bool> {
        Ok(false)
    }

    async fn before_tree(
        &self,
        _preparation: &crate::TreePreparation,
        _state: Option<&ExtensionState>,
        _cancel: &crate::CancellationToken,
    ) -> ExtensionResult<Option<crate::TreeDecision>> {
        Ok(None)
    }

    async fn cache_warming_decision(
        &self,
        _event: &CacheWarmingDecision,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<CacheWarmingAction>> {
        Ok(None)
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

#[derive(Clone, Debug)]
pub struct RegisteredCommand {
    pub extension_id: String,
    pub name: String,
    pub invocation_name: String,
    pub description: Option<String>,
    pub source_path: Option<std::path::PathBuf>,
}

pub(crate) struct RegisteredTool {
    pub owner: String,
    pub tool: Arc<dyn Tool>,
    pub spec: ToolSpec,
    pub validator: jsonschema::Validator,
}

pub struct RegistryUpdate {
    pub registry: Registry,
    pub system_prompt: Option<String>,
}

/// Refresh runtime tool definitions at model-request boundaries.
#[async_trait::async_trait]
pub trait RegistryRefresher: Send + Sync {
    async fn refresh(&self, current: &Registry) -> Result<Option<RegistryUpdate>>;
}

#[derive(Clone, Default)]
pub struct Registry {
    pub(crate) extensions: Vec<Arc<dyn Extension>>,
    pub(crate) tools: BTreeMap<String, Arc<RegisteredTool>>,
    active: Option<BTreeSet<String>>,
    pub(crate) ui: Option<Arc<dyn crate::ExtensionUi>>,
    refresher: Option<Arc<dyn RegistryRefresher>>,
}

impl Registry {
    pub fn has_resource_discovery_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|item| item.handles_resource_discovery())
    }

    /// Discover resources in extension order. A failed handler does not stop
    /// later extensions; the host decides how to report errors.
    pub async fn discover_resources(
        &self,
        event: &ResourceDiscoveryEvent,
        states: &ExtensionStates,
        timeout: std::time::Duration,
        mut on_error: impl FnMut(&str, &str),
    ) -> Vec<ExtensionResources> {
        let mut result = Vec::new();
        for extension in self
            .extensions
            .iter()
            .filter(|item| item.handles_resource_discovery())
        {
            match tokio::time::timeout(
                timeout,
                extension.discover_resources(event, states.get(extension.id())),
            )
            .await
            {
                Ok(Ok(resources)) => result.push(ExtensionResources {
                    extension_id: extension.id().into(),
                    source: extension.command_source().map(std::path::Path::to_path_buf),
                    resources,
                }),
                Ok(Err(error)) => on_error(extension.id(), &error),
                Err(_) => on_error(extension.id(), "resource discovery timed out"),
            }
        }
        result
    }
    pub fn has_session_transition_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|extension| extension.handles_session_transitions())
    }

    pub async fn session_transition_cancelled(
        &self,
        event: &crate::BeforeSessionTransition,
        states: &ExtensionStates,
        timeout: std::time::Duration,
        observer: &mut crate::Observer<'_>,
    ) -> bool {
        let deadline =
            tokio::time::Instant::now() + timeout.min(std::time::Duration::from_secs(10));
        for extension in self
            .extensions
            .iter()
            .filter(|extension| extension.handles_session_transitions())
        {
            let error = match tokio::time::timeout_at(
                deadline,
                extension.before_session_transition(event, states.get(extension.id())),
            )
            .await
            {
                Ok(Ok(true)) => return true,
                Ok(Ok(false)) => continue,
                Ok(Err(error)) => error,
                Err(_) => "session transition hook timed out".into(),
            };
            observer(crate::Notification::ExtensionLifecycleError {
                extension: extension.id(),
                event: event.kind(),
                message: &error,
            });
        }
        false
    }
    pub fn has_session_lifecycle_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|extension| extension.handles_lifecycle() && extension.handles_session_lifecycle())
    }

    pub fn has_model_selection_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|extension| extension.handles_lifecycle() && extension.handles_model_selection())
    }

    pub fn has_cache_warming_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|extension| extension.handles_cache_warming())
    }

    /// Each handler sees the original decision. The last override takes effect.
    pub async fn cache_warming_decision(
        &self,
        event: &CacheWarmingDecision,
        states: &ExtensionStates,
        mut on_error: impl FnMut(&str, &str),
    ) -> CacheWarmingAction {
        let mut action = event.action;
        for extension in self
            .extensions
            .iter()
            .filter(|extension| extension.handles_cache_warming())
        {
            match extension
                .cache_warming_decision(event, states.get(extension.id()))
                .await
            {
                Ok(Some(override_action)) => action = override_action,
                Ok(None) => {}
                Err(message) => on_error(extension.id(), &message),
            }
        }
        action
    }

    pub fn has_input_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|extension| extension.handles_input())
    }

    pub fn has_user_bash_hooks(&self) -> bool {
        self.extensions
            .iter()
            .any(|extension| extension.handles_user_bash())
    }

    /// The first handler returning a result replaces the shell execution.
    pub async fn user_bash(
        &self,
        event: &UserBashEvent,
        states: &ExtensionStates,
        cancel: &CancellationToken,
    ) -> Result<Option<UserBashResult>> {
        for extension in self
            .extensions
            .iter()
            .filter(|extension| extension.handles_user_bash())
        {
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                result = extension.user_bash(event, states.get(extension.id())) => result,
            };
            if let Some(result) = hook(extension.as_ref(), result)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    /// Run opted-in extensions in registration order. A handled input is not submitted.
    pub async fn prepare_input(
        &self,
        event: InputEvent,
        states: &ExtensionStates,
    ) -> Result<Option<InputEvent>> {
        prepare_input(&self.extensions, event, states).await
    }

    pub fn set_refresher(&mut self, refresher: Arc<dyn RegistryRefresher>) {
        self.refresher = Some(refresher);
    }
    pub async fn refreshed(&self) -> Result<Option<RegistryUpdate>> {
        match &self.refresher {
            Some(refresher) => refresher.refresh(self).await,
            None => Ok(None),
        }
    }

    /// Resolve command collisions in extension registration order, as Pi does.
    pub fn registered_commands(&self) -> Vec<RegisteredCommand> {
        let mut commands = Vec::new();
        let mut counts = BTreeMap::new();
        for extension in &self.extensions {
            for name in extension.command_names() {
                *counts.entry(name.to_owned()).or_insert(0_usize) += 1;
                commands.push(RegisteredCommand {
                    extension_id: extension.id().into(),
                    name: name.into(),
                    invocation_name: String::new(),
                    description: extension.command_description(name).map(str::to_owned),
                    source_path: extension.command_source().map(std::path::Path::to_path_buf),
                });
            }
        }
        let mut seen = BTreeMap::new();
        let mut taken = BTreeSet::new();
        for command in &mut commands {
            let occurrence = seen.entry(command.name.clone()).or_insert(0_usize);
            *occurrence += 1;
            command.invocation_name = if counts[&command.name] > 1 {
                format!("{}:{}", command.name, occurrence)
            } else {
                command.name.clone()
            };
            let mut suffix = *occurrence;
            while taken.contains(&command.invocation_name) {
                suffix += 1;
                command.invocation_name = format!("{}:{suffix}", command.name);
            }
            taken.insert(command.invocation_name.clone());
        }
        commands
    }

    /// Attach the embedding application's optional interaction surface.
    pub fn set_ui(&mut self, ui: Arc<dyn crate::ExtensionUi>) {
        self.ui = Some(ui);
    }
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
            ui: self.ui.clone(),
            refresher: None,
        })
    }

    /// Replace one registered extension atomically, retaining its hook order and UI.
    /// Returns false when the extension is not enabled in this registry.
    pub fn replace(&mut self, extension: Arc<dyn Extension>) -> Result<bool> {
        let Some(index) = self
            .extensions
            .iter()
            .position(|item| item.id() == extension.id())
        else {
            return Ok(false);
        };
        let mut candidate = self.clone();
        candidate.extensions.remove(index);
        candidate
            .tools
            .retain(|_, tool| tool.owner != extension.id());
        candidate.register(extension)?;
        let replacement = candidate.extensions.pop().expect("registered extension");
        candidate.extensions.insert(index, replacement);
        if let Some(active) = &mut candidate.active {
            active.retain(|name| candidate.tools.contains_key(name));
        }
        *self = candidate;
        Ok(true)
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

pub(crate) async fn prepare_input(
    extensions: &[Arc<dyn Extension>],
    mut event: InputEvent,
    states: &ExtensionStates,
) -> Result<Option<InputEvent>> {
    for extension in extensions
        .iter()
        .filter(|extension| extension.handles_input())
    {
        match hook(
            extension.as_ref(),
            extension.input(&event, states.get(extension.id())).await,
        )? {
            InputAction::Continue => {}
            InputAction::Handled => return Ok(None),
            InputAction::Transform { text, images } => {
                event.text = text;
                if let Some(images) = images {
                    for image in &images {
                        image.validate()?;
                    }
                    event.images = images;
                }
            }
        }
    }
    Ok(Some(event))
}

pub(crate) fn hook<T>(extension: &dyn Extension, result: ExtensionResult<T>) -> Result<T> {
    result.map_err(|message| Error::Extension {
        id: extension.id().into(),
        message,
    })
}

#[cfg(test)]
mod replacement_tests {
    use super::*;
    use serde_json::json;

    struct BashHook(&'static str, Option<&'static str>);
    #[async_trait::async_trait]
    impl Extension for BashHook {
        fn id(&self) -> &str {
            self.0
        }
        fn handles_user_bash(&self) -> bool {
            true
        }
        async fn user_bash(
            &self,
            event: &UserBashEvent,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<UserBashResult>> {
            assert_eq!(event.command, "echo hello");
            assert!(event.exclude_from_context);
            if self.0 == "winner" {
                assert_eq!(state.unwrap().data["value"], 42);
            }
            Ok(self.1.map(|output| UserBashResult {
                output: output.into(),
                exit_code: Some(7),
                ..Default::default()
            }))
        }
    }

    #[tokio::test]
    async fn user_bash_hooks_use_first_result_and_receive_state_and_cancellation() {
        let mut registry = Registry::default();
        registry.register(Arc::new(BashHook("pass", None))).unwrap();
        registry
            .register(Arc::new(BashHook("winner", Some("replaced"))))
            .unwrap();
        registry
            .register(Arc::new(BashHook("later", Some("wrong"))))
            .unwrap();
        let event = UserBashEvent {
            command: "echo hello".into(),
            exclude_from_context: true,
            cwd: ".".into(),
        };
        let states = ExtensionStates::from([(
            "winner".into(),
            ExtensionState {
                version: 1,
                data: json!({"value":42}),
            },
        )]);
        let cancel = CancellationToken::new();
        let result = registry
            .user_bash(&event, &states, &cancel)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.output, "replaced");
        assert_eq!(result.exit_code, Some(7));
        cancel.cancel();
        assert!(matches!(
            registry.user_bash(&event, &states, &cancel).await,
            Err(Error::Cancelled)
        ));
    }

    struct ResourceHook(&'static str, bool);
    #[async_trait::async_trait]
    impl Extension for ResourceHook {
        fn id(&self) -> &str {
            self.0
        }
        fn handles_resource_discovery(&self) -> bool {
            true
        }
        async fn discover_resources(
            &self,
            event: &ResourceDiscoveryEvent,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<DiscoveredResources> {
            assert_eq!(event.reason, ResourceDiscoveryReason::Reload);
            if self.1 {
                return Err("fixture failure".into());
            }
            assert_eq!(state.unwrap().data["enabled"], true);
            Ok(DiscoveredResources {
                prompt_paths: vec!["relative.md".into()],
                ..Default::default()
            })
        }
    }

    #[tokio::test]
    async fn resource_discovery_passes_state_and_continues_after_handler_failure() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(ResourceHook("broken", true)))
            .unwrap();
        registry
            .register(Arc::new(ResourceHook("working", false)))
            .unwrap();
        let states = [(
            "working".into(),
            ExtensionState {
                version: 1,
                data: json!({"enabled": true}),
            },
        )]
        .into();
        let mut failures = Vec::new();
        let result = registry
            .discover_resources(
                &ResourceDiscoveryEvent {
                    cwd: "/workspace".into(),
                    reason: ResourceDiscoveryReason::Reload,
                },
                &states,
                std::time::Duration::from_secs(1),
                |id, error| failures.push((id.to_owned(), error.to_owned())),
            )
            .await;
        assert_eq!(failures, [("broken".into(), "fixture failure".into())]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].extension_id, "working");
        assert_eq!(
            result[0].resources.prompt_paths,
            [std::path::PathBuf::from("relative.md")]
        );
    }

    struct TestExtension(&'static str, &'static str);
    impl Extension for TestExtension {
        fn id(&self) -> &str {
            self.0
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(TestTool(self.1))]
        }
    }
    struct TestTool(&'static str);
    #[async_trait::async_trait]
    impl Tool for TestTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: self.0.into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            Outcome::Succeeded(String::new())
        }
    }

    #[test]
    fn replacing_tools_is_atomic_and_keeps_extension_order() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(TestExtension("first", "old")))
            .unwrap();
        registry
            .register(Arc::new(TestExtension("second", "other")))
            .unwrap();
        registry
            .set_active_tools(["old".into(), "other".into()])
            .unwrap();
        assert!(
            registry
                .replace(Arc::new(TestExtension("first", "new")))
                .unwrap()
        );
        assert!(!registry.has_tool("old"));
        assert!(registry.has_tool("new"));
        assert_eq!(
            registry
                .active_tool_specs()
                .iter()
                .map(|spec| spec.name.as_str())
                .collect::<Vec<_>>(),
            ["other"]
        );
        assert_eq!(
            registry
                .extensions
                .iter()
                .map(|extension| extension.id())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        let before = registry.tool_specs();
        assert!(
            registry
                .replace(Arc::new(TestExtension("first", "other")))
                .is_err()
        );
        assert_eq!(registry.tool_specs(), before);
        assert!(
            !registry
                .replace(Arc::new(TestExtension("disabled", "extra")))
                .unwrap()
        );
        assert!(!registry.has_tool("extra"));
    }
}
