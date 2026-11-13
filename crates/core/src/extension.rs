use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    CancellationToken, Error, ExtensionState, ModelRequest, Outcome, Result, ToolReply, ToolSpec,
};

pub type ExtensionResult<T> = std::result::Result<T, String>;
pub type ExtensionStates = BTreeMap<String, ExtensionState>;
type BranchModels = BTreeMap<
    (Option<Uuid>, Option<String>, String),
    (Arc<dyn crate::Model>, Option<crate::Limits>),
>;

/// Partial update to a text result. Omitted fields retain their values.
#[derive(Clone, Debug, Default)]
pub struct ToolResultPatch {
    pub output: Option<String>,
    pub details: Option<Value>,
}

impl ToolResultPatch {
    pub fn apply(self, outcome: &Outcome) -> ExtensionResult<Outcome> {
        ToolResultPatchV2 {
            output: self.output,
            details: self.details,
            is_error: None,
        }
        .apply(outcome)
    }
}

/// Partial text result update with an independently adjustable error status.
#[derive(Clone, Debug, Default)]
pub struct ToolResultPatchV2 {
    pub output: Option<String>,
    pub details: Option<Value>,
    pub is_error: Option<bool>,
}

impl ToolResultPatchV2 {
    pub fn apply(self, outcome: &Outcome) -> ExtensionResult<Outcome> {
        let (output, details, is_error) = match outcome {
            Outcome::Succeeded(output) => (output.clone(), None, false),
            Outcome::SucceededWithDetails { output, details } => {
                (output.clone(), Some(details.clone()), false)
            }
            Outcome::Failed(_) => (outcome.tool_content(), None, true),
            Outcome::TextResult {
                output,
                details,
                is_error,
            } => (output.clone(), details.clone(), *is_error),
            _ => return Err("text result patch requires a text outcome".into()),
        };
        if self.output.is_none() && self.details.is_none() && self.is_error.is_none() {
            return Ok(outcome.clone());
        }
        let output = self.output.unwrap_or(output);
        let details = self.details.or(details);
        let is_error = self.is_error.unwrap_or(is_error);
        if matches!(
            outcome,
            Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. }
        ) && !is_error
        {
            return Ok(match details {
                Some(details) => Outcome::SucceededWithDetails { output, details },
                None => Outcome::Succeeded(output),
            });
        }
        Ok(Outcome::TextResult {
            output,
            details,
            is_error,
        })
    }
}

/// Branch-local extension data that is not sent to the model.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomEntry {
    pub custom_type: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_json"
    )]
    pub data: Option<Value>,
}

fn deserialize_present_json<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    serde::Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ExtensionCommandOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ExtensionState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<CustomEntry>,
}

/// Extension content included in model context, with independent display metadata.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomMessage {
    pub custom_type: String,
    pub content: crate::ContextContent,
    pub display: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_json"
    )]
    pub details: Option<Value>,
}

/// Delivery boundary for an extension message sent during an active run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CustomMessageDelivery {
    #[default]
    Steer,
    FollowUp,
    NextTurn,
    /// Start a new turn when idle; while active, deliver as steering.
    TriggerTurn,
    /// Append after the current assistant turn without starting another turn.
    AfterTurn,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct QueuedCustomMessage {
    pub id: uuid::Uuid,
    pub delivery: CustomMessageDelivery,
    pub message: CustomMessage,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExtensionCommandAction {
    Entry {
        entry: CustomEntry,
    },
    Message {
        message: CustomMessage,
    },
    /// Add a message and ask the host to start a model turn.
    TriggerMessage {
        message: CustomMessage,
    },
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ExtensionCommandEffects {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ExtensionState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<ExtensionCommandAction>,
}

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
    fn supports_progress(&self) -> bool {
        false
    }
    /// Publish an ephemeral partial result while this tool is executing.
    async fn report_progress(&self, _partial_result: Value) -> ExtensionResult<()> {
        Ok(())
    }
    fn has_ui(&self) -> bool {
        false
    }
    async fn ui(&self, _request: crate::UiRequest) -> ExtensionResult<crate::UiResponse> {
        Err("interactive extension UI is unavailable".into())
    }
    /// Whether this host can journal custom-message delivery requests.
    fn supports_custom_messages(&self) -> bool {
        false
    }
    /// Queue custom context through the active session owner.
    async fn send_message(
        &self,
        _message: CustomMessage,
        _delivery: CustomMessageDelivery,
    ) -> ExtensionResult<()> {
        Err("custom message delivery is unavailable".into())
    }
    /// Whether stable, extension-supplied message keys are supported here.
    fn supports_keyed_messages(&self) -> bool {
        false
    }
    /// Send with a stable key unique to this extension and callback phase within a run.
    async fn send_message_with_key(
        &self,
        _message: CustomMessage,
        _delivery: CustomMessageDelivery,
        _key: &str,
    ) -> ExtensionResult<()> {
        Err("keyed custom messages are unavailable".into())
    }
    /// Scope a callback host to the extension being invoked.
    fn for_extension(&self, _extension_id: &str) -> Option<Arc<dyn ToolHost>> {
        None
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
    /// Whether the host default execution timeout applies when none was explicitly configured.
    fn use_default_timeout(&self) -> bool {
        true
    }
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
    /// Pass an explicit host deadline to tools that need synchronous interruption.
    async fn invoke_transaction_with_timeout(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
        _timeout: Option<std::time::Duration>,
    ) -> ToolReply {
        self.invoke_transaction(arguments, context, state, host)
            .await
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

/// A hook's reported change status. The host tracks mutable access separately;
/// this status never proves that assistant messages were left unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextChange {
    Unchanged,
    Changed,
}

/// An index-relative edit that can only insert or remove user messages.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextUserEdit {
    Insert {
        index: usize,
        message: crate::Message,
    },
    Remove {
        index: usize,
    },
}

/// A host-owned record of mutable access, retained independently of the wrapper.
#[derive(Clone, Default)]
pub struct ContextAccess {
    assistant_accessed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    touched: std::sync::Arc<std::sync::atomic::AtomicBool>,
    user_edits: std::sync::Arc<std::sync::Mutex<Vec<ContextUserEdit>>>,
}
impl ContextAccess {
    pub fn assistant_accessed(&self) -> bool {
        self.assistant_accessed
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn touched(&self) -> bool {
        self.touched.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn user_edits(&self) -> Vec<ContextUserEdit> {
        self.user_edits.lock().expect("context edits").clone()
    }
    fn record_user_edit(&self, edit: ContextUserEdit) -> ExtensionResult<()> {
        let mut edits = self
            .user_edits
            .lock()
            .map_err(|_| "context edit tracker failed".to_owned())?;
        if edits.len() >= 4096 {
            return Err("too many context user edits".into());
        }
        edits.push(edit);
        self.mark_safe();
        Ok(())
    }
    fn mark_safe(&self) {
        self.touched
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
    fn mark_assistant(&self) {
        self.mark_safe();
        self.assistant_accessed
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

// A tool-call assistant and its contiguous results are one provider turn.
// Do not insert while any of its calls are still awaiting results, or between
// contiguous results (including malformed/extra results already in history).
fn splits_tool_group(messages: &[crate::Message], index: usize) -> bool {
    if index > 0
        && matches!(messages.get(index), Some(crate::Message::Tool { .. }))
        && (matches!(messages.get(index - 1), Some(crate::Message::Tool { .. }))
            || matches!(messages.get(index - 1), Some(crate::Message::Assistant { calls, .. }) if !calls.is_empty()))
    {
        return true;
    }
    let mut start = index;
    while start > 0 && matches!(messages[start - 1], crate::Message::Tool { .. }) {
        start -= 1;
    }
    let Some(crate::Message::Assistant { calls, .. }) =
        start.checked_sub(1).and_then(|i| messages.get(i))
    else {
        return false;
    };
    let mut outstanding = BTreeMap::<&str, usize>::new();
    for call in calls {
        *outstanding.entry(&call.id).or_default() += 1;
    }
    for message in &messages[start..index] {
        if let crate::Message::Tool { call_id, .. } = message
            && let Some(count) = outstanding.get_mut(call_id.as_str())
        {
            *count = count.saturating_sub(1);
        }
    }
    outstanding.values().any(|count| *count != 0)
}

fn insert_context_user(
    messages: &mut Vec<crate::Message>,
    access: &ContextAccess,
    index: usize,
    message: crate::Message,
) -> ExtensionResult<()> {
    if index > messages.len()
        || !matches!(
            message,
            crate::Message::User { .. } | crate::Message::UserWithImages { .. }
        )
    {
        return Err("context insertion must be a user message at a valid index".into());
    }
    if splits_tool_group(messages, index) {
        return Err("context insertion cannot split an assistant tool-call/result group".into());
    }
    access.record_user_edit(ContextUserEdit::Insert {
        index,
        message: message.clone(),
    })?;
    messages.insert(index, message);
    Ok(())
}
fn remove_context_user(
    messages: &mut Vec<crate::Message>,
    access: &ContextAccess,
    index: usize,
) -> ExtensionResult<crate::Message> {
    if !matches!(
        messages.get(index),
        Some(crate::Message::User { .. } | crate::Message::UserWithImages { .. })
    ) {
        return Err("context removal must target a user message".into());
    }
    access.record_user_edit(ContextUserEdit::Remove { index })?;
    Ok(messages.remove(index))
}

/// A portable request with host-tracked mutable access. General mutable access
/// invalidates private assistant replay, even if the result compares equal.
pub struct ContextRequest<'a> {
    request: &'a mut ModelRequest,
    access: ContextAccess,
}
impl<'a> ContextRequest<'a> {
    pub fn new(request: &'a mut ModelRequest) -> Self {
        Self {
            request,
            access: ContextAccess::default(),
        }
    }
    pub fn access(&self) -> ContextAccess {
        self.access.clone()
    }
    pub fn insert_user(&mut self, index: usize, message: crate::Message) -> ExtensionResult<()> {
        insert_context_user(&mut self.request.messages, &self.access, index, message)
    }
    pub fn remove_user(&mut self, index: usize) -> ExtensionResult<crate::Message> {
        remove_context_user(&mut self.request.messages, &self.access, index)
    }
    pub fn tools_mut(&mut self) -> &mut Vec<crate::ToolSpec> {
        self.access.mark_safe();
        &mut self.request.tools
    }
    pub fn system_content_mut(&mut self) -> Option<&mut String> {
        self.access.mark_safe();
        match self.request.messages.first_mut() {
            Some(crate::Message::System { content }) => Some(content),
            _ => None,
        }
    }
}
impl std::ops::Deref for ContextRequest<'_> {
    type Target = ModelRequest;
    fn deref(&self) -> &Self::Target {
        self.request
    }
}
impl std::ops::DerefMut for ContextRequest<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.access.mark_assistant();
        self.request
    }
}

/// A portable message list; general mutable access invalidates assistant replay.
pub struct ContextMessages<'a> {
    messages: &'a mut Vec<crate::Message>,
    access: ContextAccess,
}
impl<'a> ContextMessages<'a> {
    pub fn new(messages: &'a mut Vec<crate::Message>) -> Self {
        Self {
            messages,
            access: ContextAccess::default(),
        }
    }
    pub fn access(&self) -> ContextAccess {
        self.access.clone()
    }
    pub fn insert_user(&mut self, index: usize, message: crate::Message) -> ExtensionResult<()> {
        insert_context_user(self.messages, &self.access, index, message)
    }
    pub fn remove_user(&mut self, index: usize) -> ExtensionResult<crate::Message> {
        remove_context_user(self.messages, &self.access, index)
    }
    pub fn system_content_mut(&mut self) -> Option<&mut String> {
        self.access.mark_safe();
        match self.messages.first_mut() {
            Some(crate::Message::System { content }) => Some(content),
            _ => None,
        }
    }
}
impl std::ops::Deref for ContextMessages<'_> {
    type Target = Vec<crate::Message>;
    fn deref(&self) -> &Self::Target {
        self.messages
    }
}
impl std::ops::DerefMut for ContextMessages<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.access.mark_assistant();
        self.messages
    }
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

/// The first extension that handles a user command can replace its result or execution.
pub enum UserBashReplacement {
    Result(UserBashResult),
    Operations {
        extension: Arc<dyn Extension>,
        state: Option<ExtensionState>,
    },
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

    /// Opt in to returning branch-local custom entries from commands.
    fn supports_command_entries(&self) -> bool {
        false
    }

    /// The default preserves the existing state-only command contract.
    fn command_output(
        &self,
        name: &str,
        arguments: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionCommandOutput> {
        self.command(name, arguments, previous)
            .map(|state| ExtensionCommandOutput {
                state: Some(state),
                entries: Vec::new(),
            })
    }

    /// Opt in to returning ordered entries and messages from commands.
    fn supports_command_effects(&self) -> bool {
        false
    }

    /// The default maps state-only and custom-entry commands to ordered effects.
    fn command_effects(
        &self,
        name: &str,
        arguments: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionCommandEffects> {
        self.command_output(name, arguments, previous)
            .map(|output| ExtensionCommandEffects {
                state: output.state,
                actions: output
                    .entries
                    .into_iter()
                    .map(|entry| ExtensionCommandAction::Entry { entry })
                    .collect(),
            })
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

    async fn input_with_host(
        &self,
        event: &InputEvent,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<InputAction> {
        self.input(event, state).await
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

    async fn user_bash_with_host(
        &self,
        event: &UserBashEvent,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<UserBashResult>> {
        self.user_bash(event, state).await
    }

    /// Opt in to selecting a custom streaming implementation of user shell execution.
    fn handles_user_bash_operations(&self) -> bool {
        false
    }

    /// Return true to execute this command using `execute_user_bash`.
    async fn user_bash_operations(
        &self,
        _event: &UserBashEvent,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<bool> {
        Ok(false)
    }

    async fn user_bash_operations_with_host(
        &self,
        event: &UserBashEvent,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<bool> {
        self.user_bash_operations(event, state).await
    }

    /// Execute the command, sending combined output bytes as they become available.
    async fn execute_user_bash(
        &self,
        _event: &UserBashEvent,
        _state: Option<&ExtensionState>,
        _timeout: Option<std::time::Duration>,
        _cancel: CancellationToken,
        _output: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> ExtensionResult<Option<i32>> {
        Err("user shell operations unavailable".into())
    }

    async fn execute_user_bash_with_host(
        &self,
        event: &UserBashEvent,
        state: Option<&ExtensionState>,
        timeout: Option<std::time::Duration>,
        cancel: CancellationToken,
        output: tokio::sync::mpsc::Sender<Vec<u8>>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<i32>> {
        self.execute_user_bash(event, state, timeout, cancel, output)
            .await
    }

    fn handles_lifecycle(&self) -> bool {
        false
    }

    /// Opt in to observing tool execution start and end events.
    fn handles_tool_execution_lifecycle(&self) -> bool {
        false
    }

    /// Opt in separately to tool execution update events.
    fn handles_tool_execution_updates(&self) -> bool {
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

    async fn discover_resources_with_host(
        &self,
        event: &ResourceDiscoveryEvent,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<DiscoveredResources> {
        self.discover_resources(event, state).await
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

    /// Observe an active-run lifecycle event with access to its message host.
    /// Existing extensions continue to use `lifecycle` by default.
    async fn lifecycle_with_host(
        &self,
        event: &crate::LifecycleEvent,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<()> {
        self.lifecycle(event, state).await
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

    async fn before_session_transition_with_host(
        &self,
        event: &crate::BeforeSessionTransition,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<bool> {
        self.before_session_transition(event, state).await
    }

    async fn before_tree(
        &self,
        _preparation: &crate::TreePreparation,
        _state: Option<&ExtensionState>,
        _cancel: &crate::CancellationToken,
    ) -> ExtensionResult<Option<crate::TreeDecision>> {
        Ok(None)
    }

    async fn before_tree_with_host(
        &self,
        preparation: &crate::TreePreparation,
        state: Option<&ExtensionState>,
        cancel: &crate::CancellationToken,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<crate::TreeDecision>> {
        self.before_tree(preparation, state, cancel).await
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
        _request: &mut ContextRequest<'_>,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<ContextChange> {
        Ok(ContextChange::Unchanged)
    }

    async fn before_request(
        &self,
        request: &mut ContextRequest<'_>,
        _purpose: RequestPurpose,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<ContextChange> {
        self.context(request, state)
    }

    async fn before_request_with_host(
        &self,
        request: &mut ContextRequest<'_>,
        purpose: RequestPurpose,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<ContextChange> {
        self.before_request(request, purpose, state).await
    }

    /// Opt in to transforming the complete message list after context hooks.
    fn handles_context_with_system(&self) -> bool {
        false
    }

    /// Transform the provider-visible messages, including system messages.
    async fn context_with_system(
        &self,
        _messages: &mut ContextMessages<'_>,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<ContextChange> {
        Ok(ContextChange::Unchanged)
    }

    async fn context_with_system_with_host(
        &self,
        messages: &mut ContextMessages<'_>,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<ContextChange> {
        self.context_with_system(messages, state).await
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

    /// Opt in to host-aware legacy policy dispatch instead of `tool_call`.
    fn handles_before_tool_host(&self) -> bool {
        false
    }

    async fn before_tool_with_host(
        &self,
        name: &str,
        args: &Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Decision> {
        self.before_tool(name, args, context, state).await
    }

    /// Opt in to modifying validated tool arguments before execution.
    fn handles_tool_call(&self) -> bool {
        false
    }

    /// Later hooks observe changes to `args`. The default preserves the policy hook.
    async fn tool_call(
        &self,
        name: &str,
        args: &mut Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        self.before_tool(name, args, context, state).await
    }

    async fn tool_call_with_host(
        &self,
        name: &str,
        args: &mut Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Decision> {
        if self.handles_before_tool_host() {
            self.before_tool_with_host(name, args, context, state, host)
                .await
        } else {
            self.tool_call(name, args, context, state).await
        }
    }

    /// Opt in to transforming completed tool results before they enter the journal.
    fn handles_tool_result(&self) -> bool {
        false
    }

    /// Return a replacement outcome. Later extensions receive the latest result.
    async fn tool_result(
        &self,
        _call: &crate::ToolCall,
        _outcome: &Outcome,
        _context: &ToolContext,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<Outcome>> {
        Ok(None)
    }

    /// Patch text results while preserving fields omitted by the handler.
    async fn tool_result_patch(
        &self,
        _call: &crate::ToolCall,
        _outcome: &Outcome,
        _context: &ToolContext,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ToolResultPatch>> {
        Ok(None)
    }

    /// Patch text, metadata and error status independently. The default delegates
    /// to the original partial-patch hook for source compatibility.
    async fn tool_result_patch_v2(
        &self,
        call: &crate::ToolCall,
        outcome: &Outcome,
        context: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ToolResultPatchV2>> {
        Ok(self
            .tool_result_patch(call, outcome, context, state)
            .await?
            .map(|patch| ToolResultPatchV2 {
                output: patch.output,
                details: patch.details,
                is_error: None,
            }))
    }

    /// Dispatch legacy replacement and partial result hooks in order.
    async fn tool_result_update(
        &self,
        call: &crate::ToolCall,
        outcome: &Outcome,
        context: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<Outcome>> {
        let replacement = self.tool_result(call, outcome, context, state).await?;
        let current = replacement.as_ref().unwrap_or(outcome);
        match self
            .tool_result_patch_v2(call, current, context, state)
            .await?
        {
            Some(patch) => patch.apply(current).map(Some),
            None => Ok(replacement),
        }
    }

    async fn tool_result_update_with_host(
        &self,
        call: &crate::ToolCall,
        outcome: &Outcome,
        context: &ToolContext,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<Outcome>> {
        self.tool_result_update(call, outcome, context, state).await
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

    async fn before_compaction_with_host(
        &self,
        compaction: &mut Compaction,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Decision> {
        self.before_compaction(compaction, state)
    }

    fn after_compaction(
        &self,
        _compaction: &Compaction,
        _state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        Ok(None)
    }

    async fn after_compaction_with_host(
        &self,
        compaction: &Compaction,
        state: Option<&ExtensionState>,
        _host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        self.after_compaction(compaction, state)
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
    pub(crate) omit_default_tool_timeout: bool,
    branch_models: Arc<RwLock<BranchModels>>,
}

impl Registry {
    /// Register a model instance for recovery on a branch that selected it.
    pub fn register_branch_model(
        &self,
        name: impl Into<String>,
        model: Arc<dyn crate::Model>,
    ) -> Result<()> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        self.branch_models
            .write()
            .expect("branch models")
            .insert((None, None, name), (model, None));
        Ok(())
    }

    pub(crate) fn register_session_branch_model(
        &self,
        session_id: Uuid,
        name: String,
        model: Arc<dyn crate::Model>,
    ) -> Result<()> {
        if name.trim().is_empty() {
            return Err(Error::Config("model name is empty".into()));
        }
        self.branch_models
            .write()
            .expect("branch models")
            .insert((Some(session_id), None, name), (model, None));
        Ok(())
    }
    /// Register an exact provider/model identity for a single journal.
    pub fn register_session_provider_model(
        &self,
        session_id: Uuid,
        provider: &str,
        name: &str,
        model: Arc<dyn crate::Model>,
    ) -> Result<()> {
        self.register_session_provider_model_with_limits(session_id, provider, name, model, None)
    }
    /// Register a session-specific provider client and its effective run limits.
    pub fn register_session_provider_model_with_limits(
        &self,
        session_id: Uuid,
        provider: &str,
        name: &str,
        model: Arc<dyn crate::Model>,
        limits: Option<crate::Limits>,
    ) -> Result<()> {
        if provider.trim().is_empty() || name.trim().is_empty() {
            return Err(Error::Config("provider or model name is empty".into()));
        }
        self.branch_models.write().expect("branch models").insert(
            (Some(session_id), Some(provider.to_owned()), name.to_owned()),
            (model, limits),
        );
        Ok(())
    }
    pub(crate) fn provider_branch_config(
        &self,
        session_id: Uuid,
        provider: &str,
        name: &str,
    ) -> Option<(Arc<dyn crate::Model>, Option<crate::Limits>)> {
        self.branch_models
            .read()
            .expect("branch models")
            .get(&(Some(session_id), Some(provider.to_owned()), name.to_owned()))
            .cloned()
    }
    pub(crate) fn branch_model(
        &self,
        session_id: Uuid,
        name: &str,
    ) -> Option<Arc<dyn crate::Model>> {
        let models = self.branch_models.read().expect("branch models");
        models
            .get(&(Some(session_id), None, name.to_owned()))
            .or_else(|| models.get(&(None, None, name.to_owned())))
            .map(|(model, _)| model.clone())
    }
    pub(crate) fn copy_session_branch_models(&self, source: Uuid, destination: Uuid) {
        let mut models = self.branch_models.write().expect("branch models");
        let copies: Vec<_> = models
            .iter()
            .filter(|((owner, _, _), _)| *owner == Some(source))
            .map(|((_, provider, name), model)| (provider.clone(), name.clone(), model.clone()))
            .collect();
        for (provider, name, model) in copies {
            models.insert((Some(destination), provider, name), model);
        }
    }
    /// Remove recovery clients for a session before rebuilding its host configuration.
    pub fn remove_session_branch_models(&self, session_id: Uuid) {
        self.branch_models
            .write()
            .expect("branch models")
            .retain(|(owner, _, _), _| *owner != Some(session_id));
    }
    /// Allow tools that opt out to run without the host's default timeout.
    /// Explicitly configured host timeouts should leave this disabled.
    pub fn omit_default_tool_timeout(&mut self, omit: bool) {
        self.omit_default_tool_timeout = omit;
    }
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
    /// Discover resources while journaling custom messages sent by callbacks.
    pub async fn discover_resources_in_session(
        &self,
        event: &ResourceDiscoveryEvent,
        session: &mut crate::Session,
        timeout: std::time::Duration,
        observer: &mut crate::Observer<'_>,
        mut on_error: impl FnMut(&str, &str),
    ) -> Result<Vec<ExtensionResources>> {
        let states = session.extension_states();
        let mut result = Vec::new();
        for extension in self
            .extensions
            .iter()
            .filter(|item| item.handles_resource_discovery())
        {
            let (sender, mut incoming) = tokio::sync::mpsc::channel(32);
            let host = Arc::new(crate::agent::LifecycleMessageHost { sender, idle: true });
            let outcome = {
                let future = tokio::time::timeout(
                    timeout,
                    extension.discover_resources_with_host(event, states.get(extension.id()), host),
                );
                tokio::pin!(future);
                loop {
                    tokio::select! {
                        biased;
                        Some((input, reply)) = incoming.recv() => {
                            let persisted = if matches!(input.delivery, CustomMessageDelivery::NextTurn | CustomMessageDelivery::TriggerTurn) {
                                crate::agent::queue_custom_message(session, input, observer)
                            } else {
                                session.append_custom_message(input.message).map(|()| observer(crate::Notification::Committed(session.records().last().expect("appended custom message"))))
                            };
                            match persisted { Ok(()) => { let _ = reply.send(Ok(())); }, Err(error) => { let _ = reply.send(Err(error.to_string())); return Err(error); } }
                        }
                        result = &mut future => break result,
                    }
                }
            };
            match outcome {
                Ok(Ok(resources)) => result.push(ExtensionResources {
                    extension_id: extension.id().into(),
                    source: extension.command_source().map(std::path::Path::to_path_buf),
                    resources,
                }),
                Ok(Err(error)) => on_error(extension.id(), &error),
                Err(_) => on_error(extension.id(), "resource discovery timed out"),
            }
        }
        Ok(result)
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
        self.session_transition_cancelled_inner(event, states, timeout, observer, None)
            .await
    }

    pub(crate) async fn session_transition_cancelled_with_host(
        &self,
        event: &crate::BeforeSessionTransition,
        states: &ExtensionStates,
        timeout: std::time::Duration,
        observer: &mut crate::Observer<'_>,
        host: Arc<dyn ToolHost>,
    ) -> bool {
        self.session_transition_cancelled_inner(event, states, timeout, observer, Some(host))
            .await
    }

    async fn session_transition_cancelled_inner(
        &self,
        event: &crate::BeforeSessionTransition,
        states: &ExtensionStates,
        timeout: std::time::Duration,
        observer: &mut crate::Observer<'_>,
        host: Option<Arc<dyn ToolHost>>,
    ) -> bool {
        let deadline =
            tokio::time::Instant::now() + timeout.min(std::time::Duration::from_secs(10));
        for extension in self
            .extensions
            .iter()
            .filter(|extension| extension.handles_session_transitions())
        {
            let error = match tokio::time::timeout_at(deadline, async {
                match &host {
                    Some(host) => {
                        extension
                            .before_session_transition_with_host(
                                event,
                                states.get(extension.id()),
                                host.clone(),
                            )
                            .await
                    }
                    None => {
                        extension
                            .before_session_transition(event, states.get(extension.id()))
                            .await
                    }
                }
            })
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
    /// Check a transition while the session owner journals callback messages.
    pub async fn session_transition_cancelled_in_session(
        &self,
        event: &crate::BeforeSessionTransition,
        session: &mut crate::Session,
        timeout: std::time::Duration,
        observer: &mut crate::Observer<'_>,
    ) -> Result<bool> {
        let states = session.extension_states();
        let (sender, mut incoming) = tokio::sync::mpsc::channel(32);
        let host = Arc::new(crate::agent::LifecycleMessageHost { sender, idle: true });
        let mut errors = Vec::new();
        let mut collect = |event: crate::Notification<'_>| {
            if let crate::Notification::ExtensionLifecycleError {
                extension,
                event,
                message,
            } = event
            {
                errors.push((extension.to_owned(), event, message.to_owned()));
            }
        };
        let cancelled = {
            let future = self.session_transition_cancelled_with_host(
                event,
                &states,
                timeout,
                &mut collect,
                host,
            );
            tokio::pin!(future);
            loop {
                tokio::select! {
                    biased;
                    Some((input, reply)) = incoming.recv() => {
                        let persisted = if matches!(input.delivery, CustomMessageDelivery::NextTurn | CustomMessageDelivery::TriggerTurn) {
                            crate::agent::queue_custom_message(session, input, observer)
                        } else {
                            session.append_custom_message(input.message).map(|()| observer(crate::Notification::Committed(session.records().last().expect("appended custom message"))))
                        };
                        match persisted { Ok(()) => { let _ = reply.send(Ok(())); }, Err(error) => { let _ = reply.send(Err(error.to_string())); return Err(error); } }
                    }
                    result = &mut future => break result,
                }
            }
        };
        for (extension, event, message) in &errors {
            observer(crate::Notification::ExtensionLifecycleError {
                extension,
                event,
                message,
            });
        }
        Ok(cancelled)
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
        self.extensions.iter().any(|extension| {
            extension.handles_user_bash() || extension.handles_user_bash_operations()
        })
    }

    /// Select the first extension that supplies a result or custom operations.
    pub async fn prepare_user_bash(
        &self,
        event: &UserBashEvent,
        states: &ExtensionStates,
        cancel: &CancellationToken,
    ) -> Result<Option<UserBashReplacement>> {
        self.prepare_user_bash_inner(event, states, cancel, None)
            .await
    }

    pub async fn prepare_user_bash_with_host(
        &self,
        event: &UserBashEvent,
        states: &ExtensionStates,
        cancel: &CancellationToken,
        host: Arc<dyn ToolHost>,
    ) -> Result<Option<UserBashReplacement>> {
        self.prepare_user_bash_inner(event, states, cancel, Some(host))
            .await
    }

    async fn prepare_user_bash_inner(
        &self,
        event: &UserBashEvent,
        states: &ExtensionStates,
        cancel: &CancellationToken,
        host: Option<Arc<dyn ToolHost>>,
    ) -> Result<Option<UserBashReplacement>> {
        for extension in &self.extensions {
            let state = states.get(extension.id());
            if extension.handles_user_bash() {
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    result = async { match &host { Some(host) => extension.user_bash_with_host(event, state, host.clone()).await, None => extension.user_bash(event, state).await } } => result,
                };
                if let Some(result) = hook(extension.as_ref(), result)? {
                    return Ok(Some(UserBashReplacement::Result(result)));
                }
            }
            if extension.handles_user_bash_operations() {
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    result = async { match &host { Some(host) => extension.user_bash_operations_with_host(event, state, host.clone()).await, None => extension.user_bash_operations(event, state).await } } => result,
                };
                if hook(extension.as_ref(), result)? {
                    return Ok(Some(UserBashReplacement::Operations {
                        extension: extension.clone(),
                        state: state.cloned(),
                    }));
                }
            }
        }
        Ok(None)
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
    /// Handler errors are reported and do not prevent later handlers from running.
    pub async fn prepare_input(
        &self,
        event: InputEvent,
        states: &ExtensionStates,
    ) -> Result<Option<InputEvent>> {
        prepare_input(&self.extensions, event, states).await
    }

    pub(crate) async fn prepare_input_with_host(
        &self,
        event: InputEvent,
        states: &ExtensionStates,
        host: Arc<dyn ToolHost>,
    ) -> Result<Option<InputEvent>> {
        prepare_input_inner(&self.extensions, event, states, Some(host)).await
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
            omit_default_tool_timeout: self.omit_default_tool_timeout,
            branch_models: self.branch_models.clone(),
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
    event: InputEvent,
    states: &ExtensionStates,
) -> Result<Option<InputEvent>> {
    prepare_input_inner(extensions, event, states, None).await
}

pub(crate) async fn prepare_input_with_host(
    extensions: &[Arc<dyn Extension>],
    event: InputEvent,
    states: &ExtensionStates,
    host: Arc<dyn ToolHost>,
) -> Result<Option<InputEvent>> {
    prepare_input_inner(extensions, event, states, Some(host)).await
}

async fn prepare_input_inner(
    extensions: &[Arc<dyn Extension>],
    mut event: InputEvent,
    states: &ExtensionStates,
    host: Option<Arc<dyn ToolHost>>,
) -> Result<Option<InputEvent>> {
    for extension in extensions
        .iter()
        .filter(|extension| extension.handles_input())
    {
        let response = crate::sdk::scope_extension_callback(async {
            match &host {
                Some(host) => {
                    extension
                        .input_with_host(&event, states.get(extension.id()), host.clone())
                        .await
                }
                None => extension.input(&event, states.get(extension.id())).await,
            }
        })
        .await;
        let action = match response {
            Ok(action) => action,
            Err(error) => {
                eprintln!("Extension {} (input): {error}", extension.id());
                continue;
            }
        };
        match action {
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

    struct InputHook(&'static str);

    #[async_trait::async_trait]
    impl Extension for InputHook {
        fn id(&self) -> &str {
            self.0
        }
        fn handles_input(&self) -> bool {
            true
        }
        async fn input(
            &self,
            event: &InputEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<InputAction> {
            match self.0 {
                "first" => Ok(InputAction::Transform {
                    text: format!("{} first", event.text),
                    images: None,
                }),
                "failure" => Err("input hook failed".into()),
                "last" => Ok(InputAction::Transform {
                    text: format!("{} last", event.text),
                    images: None,
                }),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn input_hook_failure_preserves_prior_transform_and_runs_later_hooks() {
        let mut registry = Registry::default();
        for id in ["first", "failure", "last"] {
            registry.register(Arc::new(InputHook(id))).unwrap();
        }
        let event = InputEvent {
            text: "hello".into(),
            images: vec![],
            source: InputSource::Rpc,
            streaming_behavior: None,
        };
        let result = registry
            .prepare_input(event, &ExtensionStates::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.text, "hello first last");
        assert_eq!(result.source, InputSource::Rpc);
    }

    #[test]
    fn custom_entry_distinguishes_missing_data_from_json_null() {
        let missing: CustomEntry = serde_json::from_value(json!({"customType":"x"})).unwrap();
        let null: CustomEntry =
            serde_json::from_value(json!({"customType":"x", "data":null})).unwrap();
        assert!(missing.data.is_none());
        assert_eq!(null.data, Some(Value::Null));
        assert!(serde_json::to_value(missing).unwrap().get("data").is_none());
        assert_eq!(serde_json::to_value(&null).unwrap()["data"], Value::Null);
        let event = crate::Event::CustomEntry { entry: null };
        let decoded: crate::Event =
            serde_json::from_slice(&serde_json::to_vec(&event).unwrap()).unwrap();
        assert!(
            matches!(decoded, crate::Event::CustomEntry { entry } if entry.data == Some(Value::Null))
        );
    }

    struct BashHook(&'static str, Option<&'static str>);

    struct BashOperationsHook;
    #[async_trait::async_trait]
    impl Extension for BashOperationsHook {
        fn id(&self) -> &str {
            "operations"
        }
        fn handles_user_bash_operations(&self) -> bool {
            true
        }
        async fn user_bash_operations(
            &self,
            event: &UserBashEvent,
            _: Option<&ExtensionState>,
        ) -> ExtensionResult<bool> {
            Ok(event.command == "echo hello")
        }
    }

    #[tokio::test]
    async fn user_bash_operations_and_results_follow_registration_order() {
        let mut registry = Registry::default();
        registry.register(Arc::new(BashHook("pass", None))).unwrap();
        registry.register(Arc::new(BashOperationsHook)).unwrap();
        registry
            .register(Arc::new(BashHook("later", Some("wrong"))))
            .unwrap();
        let event = UserBashEvent {
            command: "echo hello".into(),
            exclude_from_context: true,
            cwd: ".".into(),
        };
        let choice = registry
            .prepare_user_bash(&event, &ExtensionStates::new(), &CancellationToken::new())
            .await
            .unwrap();
        assert!(
            matches!(choice, Some(UserBashReplacement::Operations { extension, .. }) if extension.id() == "operations")
        );
    }
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

    struct ResourceMessageHook;
    #[async_trait::async_trait]
    impl Extension for ResourceMessageHook {
        fn id(&self) -> &str {
            "resource-message"
        }
        fn handles_resource_discovery(&self) -> bool {
            true
        }
        async fn discover_resources_with_host(
            &self,
            _: &ResourceDiscoveryEvent,
            _: Option<&ExtensionState>,
            host: Arc<dyn ToolHost>,
        ) -> ExtensionResult<DiscoveredResources> {
            host.send_message(
                CustomMessage {
                    custom_type: "resource".into(),
                    content: crate::ContextContent::Text("discovery".into()),
                    display: false,
                    details: Some(json!({"id":1})),
                },
                CustomMessageDelivery::TriggerTurn,
            )
            .await?;
            Ok(DiscoveredResources {
                prompt_paths: vec!["fixture.md".into()],
                ..Default::default()
            })
        }
    }
    #[tokio::test]
    async fn resource_discovery_journals_trigger_before_acknowledging_callback() {
        let mut registry = Registry::default();
        registry.register(Arc::new(ResourceMessageHook)).unwrap();
        let mut session = crate::Session::with_store(crate::MemoryStore::default()).unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            registry.discover_resources_in_session(
                &ResourceDiscoveryEvent {
                    cwd: "/workspace".into(),
                    reason: ResourceDiscoveryReason::Startup,
                },
                &mut session,
                std::time::Duration::from_secs(1),
                &mut |_| {},
                |_, _| {},
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            result[0].resources.prompt_paths,
            [std::path::PathBuf::from("fixture.md")]
        );
        let restored = crate::Session::from_records(session.records().to_vec()).unwrap();
        assert_eq!(
            restored.queued_custom_messages()[0].1.message.details,
            Some(json!({"id":1}))
        );
        assert!(restored.pending_custom_trigger().is_some());
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
