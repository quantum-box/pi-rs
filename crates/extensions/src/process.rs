//! Versioned process boundary for user-authored Rust extensions.
use std::collections::BTreeMap;
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use pi_rs_core::{
    CacheWarmingAction, CacheWarmingDecision, CancellationToken, Compaction, Decision, Error,
    Extension, ExtensionFlag, ExtensionFlagValue, ExtensionResult, ExtensionState, InputAction,
    InputEvent, LifecycleEvent, Message, ModelRequest, Outcome, RequestPurpose, Result, Tool,
    ToolContext, ToolExecution, ToolHost, ToolReply, ToolSpec, TreeDecision, TreePreparation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_MESSAGE: u64 = 4 * 1024 * 1024;
// A lifecycle event can include an 8 MiB journaled tool result, whose JSON
// representation may expand to six times its original size when escaped.
const MAX_REQUEST: u64 = 64 * 1024 * 1024;
const PROTOCOL: u16 = 1;
const FLAGS_ENV: &str = "PI_RS_NATIVE_EXTENSION_FLAGS";

/// Read a configured flag in a native executable extension callback.
pub fn extension_flag(name: &str) -> Option<ExtensionFlagValue> {
    static FLAGS: LazyLock<BTreeMap<String, ExtensionFlagValue>> = LazyLock::new(|| {
        std::env::var(FLAGS_ENV)
            .ok()
            .and_then(|values| serde_json::from_str(&values).ok())
            .unwrap_or_default()
    });
    FLAGS.get(name).cloned()
}

#[derive(Clone)]
struct Endpoint {
    path: PathBuf,
    flags: Option<Arc<str>>,
    pi_lifecycle_fields: bool,
    text_result_v2: bool,
}

impl Endpoint {
    fn command(&self, protocol: &str) -> Command {
        let mut command = Command::new(&self.path);
        if let Some(flags) = &self.flags {
            command.env(FLAGS_ENV, flags.as_ref());
        } else {
            command.env_remove(FLAGS_ENV);
        }
        command
            .arg(protocol)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command
    }
}

fn extension_context(request: &ModelRequest) -> ModelRequest {
    ModelRequest {
        model: request.model.clone(),
        messages: request
            .messages
            .iter()
            .map(|message| match message {
                Message::Assistant { content, calls, .. } => Message::Assistant {
                    content: content.clone(),
                    calls: calls.clone(),
                    replay: None,
                },
                _ => message.clone(),
            })
            .collect(),
        tools: request.tools.clone(),
        max_output_tokens: request.max_output_tokens,
    }
}

fn restore_provider_replay(original: &ModelRequest, updated: &mut ModelRequest) {
    let same_model = original.model == updated.model;
    for (index, message) in updated.messages.iter_mut().enumerate() {
        let Message::Assistant {
            content,
            calls,
            replay,
        } = message
        else {
            continue;
        };
        if !same_model {
            *replay = None;
            continue;
        }
        let matching = |previous: &Message| match previous {
            Message::Assistant {
                content: original_content,
                calls: original_calls,
                replay,
            } if original_content == content && original_calls == calls => replay.clone(),
            _ => None,
        };
        // Tool IDs identify a continuation even if a hook inserted other messages.
        *replay = original.messages.get(index).and_then(matching).or_else(|| {
            (!calls.is_empty())
                .then(|| original.messages.iter().find_map(matching))
                .flatten()
        });
    }
}

// Older executable hooks discard unknown tool-message fields. Retain provenance
// only for unchanged content; rewrites have unknown legacy provenance.
fn restore_legacy_tool_metadata(
    original: &ModelRequest,
    updated: &mut ModelRequest,
    restore_status: bool,
    restore_structure: bool,
) {
    for message in &mut updated.messages {
        if let Message::Tool {
            call_id,
            content,
            is_error,
            is_structured,
            ..
        } = message
            && let Some(Message::Tool {
                content: previous_content,
                is_error: previous_error,
                is_structured: previous_structure,
                ..
            }) = original
                .messages
                .iter()
                .find(|item| matches!(item, Message::Tool { call_id: id, .. } if id == call_id))
        {
            if restore_status {
                *is_error = *previous_error;
            }
            if restore_structure {
                *is_structured = if content == previous_content {
                    *previous_structure
                } else {
                    None
                };
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ProtocolRequest {
    Describe,
    ToolCall {
        name: String,
        arguments: Value,
        operation_id: String,
        state: Option<ExtensionState>,
    },
    Migrate {
        state: Option<ExtensionState>,
    },
    PrepareArguments {
        name: String,
        arguments: Value,
    },
    Context {
        request: ModelRequest,
        state: Option<ExtensionState>,
    },
    Input {
        event: InputEvent,
        state: Option<ExtensionState>,
    },
    UserBash {
        event: pi_rs_core::UserBashEvent,
        state: Option<ExtensionState>,
    },
    UserBashOperations {
        event: pi_rs_core::UserBashEvent,
        state: Option<ExtensionState>,
    },
    ExecuteUserBash {
        event: pi_rs_core::UserBashEvent,
        state: Option<ExtensionState>,
        timeout_secs: Option<f64>,
    },
    Lifecycle {
        event: LifecycleEvent,
        state: Option<ExtensionState>,
    },
    DiscoverResources {
        event: pi_rs_core::ResourceDiscoveryEvent,
        state: Option<ExtensionState>,
    },
    CacheWarmingDecision {
        event: CacheWarmingDecision,
        state: Option<ExtensionState>,
    },
    BeforeTree {
        preparation: TreePreparation,
        state: Option<ExtensionState>,
    },
    BeforeSessionTransition {
        event: pi_rs_core::BeforeSessionTransition,
        state: Option<ExtensionState>,
    },
    BeforeRequest {
        request: ModelRequest,
        summary: bool,
        state: Option<ExtensionState>,
    },
    ContextWithSystem {
        messages: Vec<Message>,
        state: Option<ExtensionState>,
    },
    Command {
        name: String,
        arguments: String,
        state: Option<ExtensionState>,
    },
    CommandOutput {
        name: String,
        arguments: String,
        state: Option<ExtensionState>,
    },
    CommandEffects {
        name: String,
        arguments: String,
        state: Option<ExtensionState>,
    },
    BeforeTool {
        name: String,
        arguments: Value,
        operation_id: String,
        state: Option<ExtensionState>,
    },
    Invoke {
        name: String,
        arguments: Value,
        operation_id: String,
        #[serde(default)]
        state: Option<ExtensionState>,
        #[serde(default)]
        transaction: bool,
        #[serde(default)]
        programmatic: bool,
    },
    Poll {
        name: String,
        reference: String,
        operation_id: String,
    },
    AfterTool {
        name: String,
        outcome: Outcome,
        state: Option<ExtensionState>,
    },
    ToolResult {
        call: pi_rs_core::ToolCall,
        outcome: Outcome,
        operation_id: String,
        state: Option<ExtensionState>,
        #[serde(default)]
        text_result_v2: bool,
    },
    BeforeCompaction {
        summary: String,
        keep_turns: usize,
        state: Option<ExtensionState>,
    },
    AfterCompaction {
        summary: String,
        keep_turns: usize,
        state: Option<ExtensionState>,
    },
}
#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u16,
    response: std::result::Result<Value, String>,
}
#[derive(Serialize, Deserialize)]
struct Description {
    id: String,
    dependencies: Vec<String>,
    commands: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    flags: Vec<ExtensionFlag>,
    tools: Vec<DescriptionTool>,
    #[serde(default)]
    host_calls: bool,
    #[serde(default)]
    command_entries: bool,
    #[serde(default)]
    command_effects: bool,
    #[serde(default)]
    input_hook: bool,
    #[serde(default)]
    tool_result_hook: bool,
    #[serde(default)]
    text_result_v2: bool,
    #[serde(default)]
    tool_message_error: bool,
    #[serde(default)]
    tool_message_structured: bool,
    /// This executable distinguishes explicit false from missing provenance.
    #[serde(default)]
    tool_message_provenance_v2: bool,
    #[serde(default)]
    tool_call_hook: bool,
    #[serde(default)]
    user_bash_hook: bool,
    #[serde(default)]
    user_bash_operations_hook: bool,
    #[serde(default)]
    lifecycle_hook: bool,
    #[serde(default)]
    lifecycle_host_calls: bool,
    #[serde(default)]
    tool_hook_host_calls: bool,
    #[serde(default)]
    before_tool_host_calls: bool,
    #[serde(default)]
    request_hook_host_calls: bool,
    #[serde(default)]
    input_hook_host_calls: bool,
    #[serde(default)]
    context_hook_host_calls: bool,
    #[serde(default)]
    transition_hook_host_calls: bool,
    #[serde(default)]
    tree_hook_host_calls: bool,
    #[serde(default)]
    resources_hook_host_calls: bool,
    #[serde(default)]
    user_bash_hook_host_calls: bool,
    #[serde(default)]
    user_bash_stream_host_calls: bool,
    #[serde(default)]
    tool_execution_lifecycle_hook: bool,
    #[serde(default)]
    tool_execution_update_hook: bool,
    #[serde(default)]
    model_selection_hook: bool,
    #[serde(default)]
    session_lifecycle_hook: bool,
    #[serde(default)]
    session_transition_reasons: bool,
    /// Opt in to Pi-compatible camelCase lifecycle and tree fields.
    #[serde(default)]
    pi_lifecycle_fields: bool,
    #[serde(default)]
    session_info_hook: bool,
    #[serde(default)]
    cache_warming_hook: bool,
    #[serde(default)]
    tree_navigation_hook: bool,
    #[serde(default)]
    session_transition_hook: bool,
    #[serde(default)]
    resource_discovery_hook: bool,
    #[serde(default)]
    context_with_system_hook: bool,
}
#[derive(Serialize, Deserialize)]
struct DescriptionTool {
    spec: ToolSpec,
    execution: ToolExecution,
    #[serde(default)]
    can_poll: bool,
    #[serde(default)]
    transactional: bool,
    #[serde(default)]
    prepares_arguments: bool,
}
#[derive(Serialize, Deserialize)]
struct DecisionValue {
    allow: bool,
    reason: Option<String>,
}
impl From<Decision> for DecisionValue {
    fn from(value: Decision) -> Self {
        match value {
            Decision::Allow => Self {
                allow: true,
                reason: None,
            },
            Decision::Deny(reason) => Self {
                allow: false,
                reason: Some(reason),
            },
        }
    }
}
impl From<DecisionValue> for Decision {
    fn from(value: DecisionValue) -> Self {
        if value.allow {
            Self::Allow
        } else {
            Self::Deny(
                value
                    .reason
                    .unwrap_or_else(|| "extension denied the operation".into()),
            )
        }
    }
}

/// Explicitly selected native executable. State is supplied by the journal on
/// every request, so a process restart does not reset its logical state.
pub struct ExecutableExtension {
    endpoint: Endpoint,
    description: Description,
}
impl ExecutableExtension {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let executable = path.as_ref().canonicalize()?;
        if !executable.is_file() {
            return Err(Error::Config("extension executable must be a file".into()));
        }
        let endpoint = Endpoint {
            path: executable,
            flags: None,
            pi_lifecycle_fields: false,
            text_result_v2: false,
        };
        let description =
            call(&endpoint, ProtocolRequest::Describe, None).map_err(Error::Config)?;
        let mut extension = Self {
            endpoint,
            description,
        };
        extension.endpoint.pi_lifecycle_fields = extension.description.pi_lifecycle_fields;
        extension.endpoint.text_result_v2 = extension.description.text_result_v2;
        extension.apply_flags(&mut BTreeMap::new())?;
        Ok(extension)
    }

    /// Apply the values owned by this executable, leaving other flags for the host.
    pub fn apply_flags(
        &mut self,
        overrides: &mut BTreeMap<String, ExtensionFlagValue>,
    ) -> Result<()> {
        let mut values = BTreeMap::new();
        for flag in &self.description.flags {
            if flag.name.is_empty()
                || flag.name.len() > 128
                || !flag.name.as_bytes()[0].is_ascii_alphabetic()
                || !flag
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
                || values.contains_key(&flag.name)
            {
                return Err(Error::Config(format!(
                    "invalid or duplicate extension flag {:?}",
                    flag.name
                )));
            }
            let value = match (overrides.get(&flag.name).cloned(), &flag.default) {
                (None, default) => default.clone(),
                (Some(ExtensionFlagValue::Boolean(value)), ExtensionFlagValue::Boolean(_)) => {
                    ExtensionFlagValue::Boolean(value)
                }
                (Some(ExtensionFlagValue::String(value)), ExtensionFlagValue::Boolean(_)) => {
                    ExtensionFlagValue::Boolean(value.parse::<bool>().map_err(|_| {
                        Error::Config(format!("--{} requires true or false", flag.name))
                    })?)
                }
                (Some(ExtensionFlagValue::String(value)), ExtensionFlagValue::String(_)) => {
                    ExtensionFlagValue::String(value)
                }
                (Some(ExtensionFlagValue::Boolean(_)), ExtensionFlagValue::String(_)) => {
                    return Err(Error::Config(format!("--{} requires a value", flag.name)));
                }
            };
            values.insert(flag.name.clone(), value);
        }
        if !values.is_empty() {
            let encoded = serde_json::to_string(&values)?;
            if encoded.len() > 16 * 1024 {
                return Err(Error::Config("native extension flags exceed 16 KiB".into()));
            }
            self.endpoint.flags = Some(Arc::from(encoded));
            for name in values.keys() {
                overrides.remove(name);
            }
        }
        Ok(())
    }
    fn request<T: serde::de::DeserializeOwned>(
        &self,
        request: ProtocolRequest,
    ) -> ExtensionResult<T> {
        call(&self.endpoint, request, None)
    }

    async fn request_async<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        request: ProtocolRequest,
    ) -> ExtensionResult<T> {
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
            .await
            .map_err(|error| error.to_string())?
    }
}

#[async_trait::async_trait]
impl Extension for ExecutableExtension {
    fn id(&self) -> &str {
        &self.description.id
    }
    fn dependency_ids(&self) -> Vec<&str> {
        self.description
            .dependencies
            .iter()
            .map(String::as_str)
            .collect()
    }
    fn command_source(&self) -> Option<&Path> {
        Some(&self.endpoint.path)
    }
    fn cli_flags(&self) -> Vec<ExtensionFlag> {
        self.description.flags.clone()
    }
    fn command_names(&self) -> Vec<&str> {
        self.description
            .commands
            .iter()
            .map(String::as_str)
            .collect()
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.description
            .tools
            .iter()
            .map(|tool| {
                Arc::new(ExecutableTool {
                    endpoint: self.endpoint.clone(),
                    spec: tool.spec.clone(),
                    execution: tool.execution,
                    can_poll: tool.can_poll,
                    transactional: tool.transactional,
                    prepares_arguments: tool.prepares_arguments,
                    host_calls: self.description.host_calls,
                }) as Arc<dyn Tool>
            })
            .collect()
    }
    fn handles_input(&self) -> bool {
        self.description.input_hook
    }

    async fn input(
        &self,
        event: &InputEvent,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<InputAction> {
        self.request_async(ProtocolRequest::Input {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
    }
    async fn input_with_host(
        &self,
        event: &InputEvent,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<InputAction> {
        if !self.description.input_hook_host_calls {
            return self.input(event, state).await;
        }
        let request = ProtocolRequest::Input {
            event: event.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    fn handles_user_bash(&self) -> bool {
        self.description.user_bash_hook
    }
    async fn user_bash(
        &self,
        event: &pi_rs_core::UserBashEvent,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<pi_rs_core::UserBashResult>> {
        self.request_async(ProtocolRequest::UserBash {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
    }
    async fn user_bash_with_host(
        &self,
        event: &pi_rs_core::UserBashEvent,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<pi_rs_core::UserBashResult>> {
        if !self.description.user_bash_hook_host_calls {
            return self.user_bash(event, state).await;
        }
        let request = ProtocolRequest::UserBash {
            event: event.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    fn handles_user_bash_operations(&self) -> bool {
        self.description.user_bash_operations_hook
    }
    async fn user_bash_operations(
        &self,
        event: &pi_rs_core::UserBashEvent,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<bool> {
        self.request_async(ProtocolRequest::UserBashOperations {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
    }
    async fn user_bash_operations_with_host(
        &self,
        event: &pi_rs_core::UserBashEvent,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<bool> {
        if !self.description.user_bash_hook_host_calls {
            return self.user_bash_operations(event, state).await;
        }
        let request = ProtocolRequest::UserBashOperations {
            event: event.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    async fn execute_user_bash(
        &self,
        event: &pi_rs_core::UserBashEvent,
        state: Option<&ExtensionState>,
        timeout: Option<Duration>,
        cancel: CancellationToken,
        output: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> ExtensionResult<Option<i32>> {
        let endpoint = self.endpoint.clone();
        let request = BashStreamRequest {
            event: event.clone(),
            state: state.cloned(),
            timeout_secs: timeout.map(|duration| duration.as_secs_f64()),
        };
        let guard = CancelOnDrop(cancel.child_token());
        let cancelled = guard.0.clone();
        tokio::task::spawn_blocking(move || {
            call_bash_stream(&endpoint, request, &cancelled, output)
        })
        .await
        .map_err(|error| error.to_string())?
    }
    async fn execute_user_bash_with_host(
        &self,
        event: &pi_rs_core::UserBashEvent,
        state: Option<&ExtensionState>,
        timeout: Option<Duration>,
        cancel: CancellationToken,
        output: tokio::sync::mpsc::Sender<Vec<u8>>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<i32>> {
        if !self.description.user_bash_stream_host_calls {
            return self
                .execute_user_bash(event, state, timeout, cancel, output)
                .await;
        }
        let request = ProtocolRequest::ExecuteUserBash {
            event: event.clone(),
            state: state.cloned(),
            timeout_secs: timeout.map(|duration| duration.as_secs_f64()),
        };
        let host = Arc::new(ShellProgressHost {
            inner: host,
            output,
        });
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(cancel.child_token());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    fn handles_lifecycle(&self) -> bool {
        self.description.lifecycle_hook
    }
    fn handles_tool_execution_lifecycle(&self) -> bool {
        self.description.tool_execution_lifecycle_hook
    }
    fn handles_tool_execution_updates(&self) -> bool {
        self.description.tool_execution_update_hook
    }
    fn handles_model_selection(&self) -> bool {
        self.description.model_selection_hook
    }
    fn handles_resource_discovery(&self) -> bool {
        self.description.resource_discovery_hook
    }
    async fn discover_resources(
        &self,
        event: &pi_rs_core::ResourceDiscoveryEvent,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::DiscoveredResources> {
        self.request_async(ProtocolRequest::DiscoverResources {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
    }
    async fn discover_resources_with_host(
        &self,
        event: &pi_rs_core::ResourceDiscoveryEvent,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<pi_rs_core::DiscoveredResources> {
        if !self.description.resources_hook_host_calls {
            return self.discover_resources(event, state).await;
        }
        let request = ProtocolRequest::DiscoverResources {
            event: event.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    fn handles_session_lifecycle(&self) -> bool {
        self.description.session_lifecycle_hook
    }
    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        use pi_rs_core::{SessionShutdownReason as Shutdown, SessionStartReason as Start};
        if matches!(event, LifecycleEvent::SessionInfoChanged { .. })
            && !self.description.session_info_hook
        {
            return Ok(());
        }
        let mut event = event.clone();
        if !self.description.session_transition_reasons {
            match &mut event {
                LifecycleEvent::SessionStart { reason, .. } => {
                    *reason = match *reason {
                        Start::New | Start::Fork => Start::Resume,
                        Start::Reload => Start::Startup,
                        reason => reason,
                    };
                }
                LifecycleEvent::SessionShutdown { reason, .. } => {
                    *reason = match *reason {
                        Shutdown::New | Shutdown::Fork => Shutdown::Resume,
                        Shutdown::Reload => Shutdown::Quit,
                        reason => reason,
                    };
                }
                _ => {}
            }
        }
        self.request_async(ProtocolRequest::Lifecycle {
            event,
            state: state.cloned(),
        })
        .await
    }
    async fn lifecycle_with_host(
        &self,
        event: &LifecycleEvent,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<()> {
        if !self.description.lifecycle_host_calls {
            return self.lifecycle(event, state).await;
        }
        let request = ProtocolRequest::Lifecycle {
            event: event.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancellation = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            call_duplex(&path, request, host, &cancellation, &runtime)
        })
        .await
        .map_err(|error| error.to_string())?
    }
    fn handles_cache_warming(&self) -> bool {
        self.description.cache_warming_hook
    }
    async fn cache_warming_decision(
        &self,
        event: &CacheWarmingDecision,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<CacheWarmingAction>> {
        self.request_async(ProtocolRequest::CacheWarmingDecision {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
    }
    fn handles_tree_navigation(&self) -> bool {
        self.description.tree_navigation_hook
    }
    fn handles_session_transitions(&self) -> bool {
        self.description.session_transition_hook
    }
    async fn before_session_transition(
        &self,
        event: &pi_rs_core::BeforeSessionTransition,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<bool> {
        self.request_async(ProtocolRequest::BeforeSessionTransition {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
    }
    async fn before_session_transition_with_host(
        &self,
        event: &pi_rs_core::BeforeSessionTransition,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<bool> {
        if !self.description.transition_hook_host_calls {
            return self.before_session_transition(event, state).await;
        }
        let request = ProtocolRequest::BeforeSessionTransition {
            event: event.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    async fn before_tree(
        &self,
        preparation: &TreePreparation,
        state: Option<&ExtensionState>,
        _cancel: &CancellationToken,
    ) -> ExtensionResult<Option<TreeDecision>> {
        self.request_async(ProtocolRequest::BeforeTree {
            preparation: preparation.clone(),
            state: state.cloned(),
        })
        .await
    }
    async fn before_tree_with_host(
        &self,
        preparation: &TreePreparation,
        state: Option<&ExtensionState>,
        cancel: &CancellationToken,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<TreeDecision>> {
        if !self.description.tree_hook_host_calls {
            return self.before_tree(preparation, state, cancel).await;
        }
        let request = ProtocolRequest::BeforeTree {
            preparation: preparation.clone(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }
    fn migrate(
        &self,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        self.request(ProtocolRequest::Migrate {
            state: previous.cloned(),
        })
    }
    fn context(
        &self,
        request: &mut ModelRequest,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let mut updated = self.request(ProtocolRequest::Context {
            request: extension_context(request),
            state: state.cloned(),
        })?;
        restore_provider_replay(request, &mut updated);
        restore_legacy_tool_metadata(
            request,
            &mut updated,
            !self.description.tool_message_error,
            !self.description.tool_message_provenance_v2,
        );
        *request = updated;
        Ok(())
    }
    async fn before_request(
        &self,
        request: &mut ModelRequest,
        purpose: RequestPurpose,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let message = ProtocolRequest::BeforeRequest {
            request: extension_context(request),
            summary: purpose == RequestPurpose::Summary,
            state: state.cloned(),
        };
        let mut updated = self.request_async(message).await?;
        restore_provider_replay(request, &mut updated);
        restore_legacy_tool_metadata(
            request,
            &mut updated,
            !self.description.tool_message_error,
            !self.description.tool_message_provenance_v2,
        );
        *request = updated;
        Ok(())
    }
    async fn before_request_with_host(
        &self,
        request: &mut ModelRequest,
        purpose: RequestPurpose,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<()> {
        if !self.description.request_hook_host_calls {
            return self.before_request(request, purpose, state).await;
        }
        let payload = ProtocolRequest::BeforeRequest {
            request: extension_context(request),
            summary: matches!(purpose, RequestPurpose::Summary),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        let mut updated = tokio::task::spawn_blocking(move || {
            call_duplex(&path, payload, host, &cancelled, &runtime)
        })
        .await
        .map_err(|error| error.to_string())??;
        restore_provider_replay(request, &mut updated);
        restore_legacy_tool_metadata(
            request,
            &mut updated,
            !self.description.tool_message_error,
            !self.description.tool_message_provenance_v2,
        );
        *request = updated;
        Ok(())
    }
    fn handles_context_with_system(&self) -> bool {
        self.description.context_with_system_hook
    }
    async fn context_with_system(
        &self,
        messages: &mut Vec<Message>,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let original = ModelRequest {
            model: String::new(),
            messages: messages.clone(),
            tools: Vec::new(),
            max_output_tokens: 0,
        };
        let updated = self
            .request_async(ProtocolRequest::ContextWithSystem {
                messages: extension_context(&original).messages,
                state: state.cloned(),
            })
            .await?;
        let mut updated = ModelRequest {
            messages: updated,
            ..original.clone()
        };
        restore_provider_replay(&original, &mut updated);
        restore_legacy_tool_metadata(
            &original,
            &mut updated,
            !self.description.tool_message_error,
            !self.description.tool_message_provenance_v2,
        );
        *messages = updated.messages;
        Ok(())
    }
    async fn context_with_system_with_host(
        &self,
        messages: &mut Vec<Message>,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<()> {
        if !self.description.context_hook_host_calls {
            return self.context_with_system(messages, state).await;
        }
        let original = ModelRequest {
            model: String::new(),
            messages: messages.clone(),
            tools: Vec::new(),
            max_output_tokens: 0,
        };
        let payload = ProtocolRequest::ContextWithSystem {
            messages: extension_context(&original).messages,
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        let updated = tokio::task::spawn_blocking(move || {
            call_duplex(&path, payload, host, &cancelled, &runtime)
        })
        .await
        .map_err(|error| error.to_string())??;
        let mut updated = ModelRequest {
            messages: updated,
            ..original.clone()
        };
        restore_provider_replay(&original, &mut updated);
        restore_legacy_tool_metadata(
            &original,
            &mut updated,
            !self.description.tool_message_error,
            !self.description.tool_message_provenance_v2,
        );
        *messages = updated.messages;
        Ok(())
    }

    fn command(
        &self,
        name: &str,
        arguments: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        self.request(ProtocolRequest::Command {
            name: name.into(),
            arguments: arguments.into(),
            state: previous.cloned(),
        })
    }
    fn supports_command_entries(&self) -> bool {
        self.description.command_entries
    }
    fn command_output(
        &self,
        name: &str,
        arguments: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ExtensionCommandOutput> {
        if self.description.command_entries {
            self.request(ProtocolRequest::CommandOutput {
                name: name.into(),
                arguments: arguments.into(),
                state: previous.cloned(),
            })
        } else {
            self.command(name, arguments, previous).map(|state| {
                pi_rs_core::ExtensionCommandOutput {
                    state: Some(state),
                    entries: Vec::new(),
                }
            })
        }
    }
    fn supports_command_effects(&self) -> bool {
        self.description.command_effects
    }
    fn command_effects(
        &self,
        name: &str,
        arguments: &str,
        previous: Option<&ExtensionState>,
    ) -> ExtensionResult<pi_rs_core::ExtensionCommandEffects> {
        if self.description.command_effects {
            self.request(ProtocolRequest::CommandEffects {
                name: name.into(),
                arguments: arguments.into(),
                state: previous.cloned(),
            })
        } else {
            self.command_output(name, arguments, previous)
                .map(|output| pi_rs_core::ExtensionCommandEffects {
                    state: output.state,
                    actions: output
                        .entries
                        .into_iter()
                        .map(|entry| pi_rs_core::ExtensionCommandAction::Entry { entry })
                        .collect(),
                })
        }
    }
    async fn before_tool(
        &self,
        name: &str,
        arguments: &Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        let request = ProtocolRequest::BeforeTool {
            name: name.into(),
            arguments: arguments.clone(),
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let result: DecisionValue =
            tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
                .await
                .map_err(|error| error.to_string())??;
        Ok(result.into())
    }
    async fn before_tool_with_host(
        &self,
        name: &str,
        arguments: &Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Decision> {
        if !self.description.before_tool_host_calls {
            return self.before_tool(name, arguments, context, state).await;
        }
        let request = ProtocolRequest::BeforeTool {
            name: name.into(),
            arguments: arguments.clone(),
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        let result: DecisionValue = tokio::task::spawn_blocking(move || {
            call_duplex(&path, request, host, &cancelled, &runtime)
        })
        .await
        .map_err(|error| error.to_string())??;
        Ok(result.into())
    }
    fn handles_tool_call(&self) -> bool {
        self.description.tool_call_hook
    }
    async fn tool_call(
        &self,
        name: &str,
        arguments: &mut Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        if !self.description.tool_call_hook {
            return self.before_tool(name, arguments, context, state).await;
        }
        let request = ProtocolRequest::ToolCall {
            name: name.into(),
            arguments: arguments.clone(),
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let (updated, decision): (Value, DecisionValue) =
            tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
                .await
                .map_err(|error| error.to_string())??;
        *arguments = updated;
        Ok(decision.into())
    }
    async fn tool_call_with_host(
        &self,
        name: &str,
        arguments: &mut Value,
        context: &ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Decision> {
        if !self.description.tool_call_hook {
            return self
                .before_tool_with_host(name, arguments, context, state, host)
                .await;
        }
        if !self.description.tool_hook_host_calls {
            return self.tool_call(name, arguments, context, state).await;
        }
        let request = ProtocolRequest::ToolCall {
            name: name.into(),
            arguments: arguments.clone(),
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        let (updated, decision): (Value, DecisionValue) = tokio::task::spawn_blocking(move || {
            call_duplex(&path, request, host, &cancelled, &runtime)
        })
        .await
        .map_err(|error| error.to_string())??;
        *arguments = updated;
        Ok(decision.into())
    }
    fn handles_tool_result(&self) -> bool {
        self.description.tool_result_hook
    }
    async fn tool_result(
        &self,
        call_data: &pi_rs_core::ToolCall,
        outcome: &Outcome,
        context: &ToolContext,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<Outcome>> {
        let legacy = !self.endpoint.text_result_v2;
        let sent = if legacy {
            outcome.legacy_text_result()
        } else {
            outcome.clone()
        };
        let request = ProtocolRequest::ToolResult {
            call: call_data.clone(),
            outcome: sent.clone(),
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
            text_result_v2: !legacy,
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let updated: Option<Outcome> =
            tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
                .await
                .map_err(|error| error.to_string())??;
        // An older executable cannot represent patched failure metadata. Preserve
        // it when the old hook makes no change, or changes only the text.
        if legacy
            && let Outcome::TextResult {
                output, details, ..
            } = outcome
        {
            return Ok(updated.map(|result| {
                if result == sent {
                    return outcome.clone();
                }
                match result {
                    Outcome::Failed(text) => Outcome::TextResult {
                        output: if text == *output {
                            output.clone()
                        } else {
                            text
                        },
                        details: details.clone(),
                        is_error: true,
                    },
                    other => other,
                }
            }));
        }
        Ok(updated)
    }

    async fn tool_result_update_with_host(
        &self,
        call_data: &pi_rs_core::ToolCall,
        outcome: &Outcome,
        context: &ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ExtensionResult<Option<Outcome>> {
        if !self.description.tool_hook_host_calls || !self.endpoint.text_result_v2 {
            return self
                .tool_result_update(call_data, outcome, context, state)
                .await;
        }
        let request = ProtocolRequest::ToolResult {
            call: call_data.clone(),
            outcome: outcome.clone(),
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
            text_result_v2: true,
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let runtime = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || call_duplex(&path, request, host, &cancelled, &runtime))
            .await
            .map_err(|error| error.to_string())?
    }

    fn after_tool(
        &self,
        name: &str,
        outcome: &Outcome,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        self.request(ProtocolRequest::AfterTool {
            name: name.into(),
            outcome: if self.endpoint.text_result_v2 {
                outcome.clone()
            } else {
                outcome.legacy_text_result()
            },
            state: state.cloned(),
        })
    }
    fn before_compaction(
        &self,
        compaction: &mut Compaction,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Decision> {
        let (summary, keep_turns, decision): (String, usize, DecisionValue) =
            self.request(ProtocolRequest::BeforeCompaction {
                summary: compaction.summary.clone(),
                keep_turns: compaction.keep_turns,
                state: state.cloned(),
            })?;
        compaction.summary = summary;
        compaction.keep_turns = keep_turns;
        Ok(decision.into())
    }
    fn after_compaction(
        &self,
        compaction: &Compaction,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        self.request(ProtocolRequest::AfterCompaction {
            summary: compaction.summary.clone(),
            keep_turns: compaction.keep_turns,
            state: state.cloned(),
        })
    }
}

struct ExecutableTool {
    endpoint: Endpoint,
    spec: ToolSpec,
    execution: ToolExecution,
    can_poll: bool,
    transactional: bool,
    prepares_arguments: bool,
    host_calls: bool,
}
impl ExecutableTool {
    async fn request<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        request: ProtocolRequest,
        cancellation: CancellationToken,
    ) -> ExtensionResult<T> {
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(cancellation.child_token());
        let cancelled = guard.0.clone();
        tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
            .await
            .map_err(|error| error.to_string())?
    }
    async fn invoke_remote(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        programmatic: bool,
        host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        if !self.transactional {
            return self
                .invoke_with_state(arguments, context, state)
                .await
                .into();
        }
        let request = ProtocolRequest::Invoke {
            name: self.spec.name.clone(),
            arguments,
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
            transaction: true,
            programmatic,
        };
        if self.host_calls {
            let path = self.endpoint.clone();
            let guard = CancelOnDrop(context.cancellation.child_token());
            let cancellation = guard.0.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                call_duplex(&path, request, host, &cancellation, &runtime)
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result)
            .unwrap_or_else(|error| Outcome::Unknown(error).into())
        } else {
            self.request(request, context.cancellation)
                .await
                .unwrap_or_else(|error| Outcome::Unknown(error).into())
        }
    }
}
#[async_trait::async_trait]
impl Tool for ExecutableTool {
    async fn prepare_arguments(&self, arguments: Value) -> ExtensionResult<Value> {
        if !self.prepares_arguments {
            return Ok(arguments);
        }
        self.request(
            ProtocolRequest::PrepareArguments {
                name: self.spec.name.clone(),
                arguments,
            },
            CancellationToken::new(),
        )
        .await
    }
    fn can_poll(&self) -> bool {
        self.can_poll
    }
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }
    fn execution(&self) -> ToolExecution {
        self.execution
    }
    async fn invoke(&self, arguments: Value, context: ToolContext) -> Outcome {
        self.invoke_with_state(arguments, context, None).await
    }
    async fn invoke_with_state(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
    ) -> Outcome {
        let request = ProtocolRequest::Invoke {
            name: self.spec.name.clone(),
            arguments,
            operation_id: context.operation_id.to_string(),
            state: state.cloned(),
            transaction: false,
            programmatic: false,
        };
        self.request(request, context.cancellation)
            .await
            .unwrap_or_else(Outcome::Unknown)
    }
    async fn invoke_with_host(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> Outcome {
        self.invoke_remote(arguments, context, state, false, host)
            .await
            .outcome
    }
    async fn invoke_transaction(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        self.invoke_remote(arguments, context, state, false, host)
            .await
    }
    async fn invoke_programmatic(
        &self,
        arguments: Value,
        context: ToolContext,
        state: Option<&ExtensionState>,
        host: Arc<dyn ToolHost>,
    ) -> ToolReply {
        self.invoke_remote(arguments, context, state, true, host)
            .await
    }
    async fn poll(&self, reference: &str, context: ToolContext) -> Option<Outcome> {
        let request = ProtocolRequest::Poll {
            name: self.spec.name.clone(),
            reference: reference.into(),
            operation_id: context.operation_id.to_string(),
        };
        let path = self.endpoint.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
            .await
            .ok()
            .and_then(std::result::Result::ok)
            .flatten()
    }
}
struct ShellProgressHost {
    inner: Arc<dyn ToolHost>,
    output: tokio::sync::mpsc::Sender<Vec<u8>>,
}
#[async_trait::async_trait]
impl ToolHost for ShellProgressHost {
    fn tools(&self) -> Vec<ToolSpec> {
        self.inner.tools()
    }
    fn supports_progress(&self) -> bool {
        true
    }
    fn supports_custom_messages(&self) -> bool {
        self.inner.supports_custom_messages()
    }
    fn supports_keyed_messages(&self) -> bool {
        self.inner.supports_keyed_messages()
    }
    async fn send_message_with_key(
        &self,
        message: pi_rs_core::CustomMessage,
        delivery: pi_rs_core::CustomMessageDelivery,
        key: &str,
    ) -> ExtensionResult<()> {
        self.inner
            .send_message_with_key(message, delivery, key)
            .await
    }
    async fn send_message(
        &self,
        message: pi_rs_core::CustomMessage,
        delivery: pi_rs_core::CustomMessageDelivery,
    ) -> ExtensionResult<()> {
        self.inner.send_message(message, delivery).await
    }
    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        self.inner.call(name, arguments).await
    }
    async fn report_progress(&self, value: Value) -> ExtensionResult<()> {
        let data: Vec<u8> = serde_json::from_value(
            value
                .get("shellOutput")
                .cloned()
                .ok_or_else(|| "invalid shell progress".to_owned())?,
        )
        .map_err(|error| error.to_string())?;
        self.output
            .send(data)
            .await
            .map_err(|_| "shell output consumer closed".to_owned())
    }
}

struct DetachedHost;
#[async_trait::async_trait]
impl ToolHost for DetachedHost {
    fn tools(&self) -> Vec<ToolSpec> {
        vec![]
    }
    async fn call(&self, _: &str, _: Value) -> Outcome {
        Outcome::Skipped("host tool calls are unavailable through this executable protocol".into())
    }
}

#[derive(Serialize, Deserialize)]
struct DuplexRequest {
    request: ProtocolRequest,
    tools: Vec<ToolSpec>,
    #[serde(default)]
    has_ui: bool,
    #[serde(default)]
    has_progress: bool,
    #[serde(default)]
    has_custom_messages: bool,
    #[serde(default)]
    has_keyed_messages: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DuplexFrame {
    SendMessage {
        message: pi_rs_core::CustomMessage,
        delivery: pi_rs_core::CustomMessageDelivery,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
    },
    Progress {
        partial_result: Value,
    },
    Ui {
        request: pi_rs_core::UiRequest,
    },
    Call {
        calls: Vec<(String, Value)>,
    },
    Complete {
        response: std::result::Result<Value, String>,
    },
}

fn write_line(writer: &mut impl Write, value: &impl Serialize) -> ExtensionResult<()> {
    let mut bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    if bytes.len() as u64 >= MAX_MESSAGE {
        return Err("native extension frame exceeds 4 MiB".into());
    }
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .and_then(|_| writer.flush())
        .map_err(|error| error.to_string())
}
fn read_line<T: serde::de::DeserializeOwned>(reader: &mut impl BufRead) -> ExtensionResult<T> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_MESSAGE + 1)
        .read_until(b'\n', &mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_MESSAGE {
        return Err("native extension frame exceeds 4 MiB".into());
    }
    if bytes.is_empty() {
        return Err("native extension connection closed".into());
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid native extension frame: {error}"))
}

#[derive(Serialize, Deserialize)]
struct BashStreamRequest {
    event: pi_rs_core::UserBashEvent,
    state: Option<ExtensionState>,
    timeout_secs: Option<f64>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BashStreamFrame {
    Output {
        data: Vec<u8>,
    },
    Complete {
        response: ExtensionResult<Option<i32>>,
    },
}

fn call_bash_stream(
    endpoint: &Endpoint,
    request: BashStreamRequest,
    cancel: &CancellationToken,
    output: tokio::sync::mpsc::Sender<Vec<u8>>,
) -> ExtensionResult<Option<i32>> {
    let mut child = ChildGuard::spawn(endpoint.command("--pi-rs-extension-bash-v1"))
        .map_err(|error| error.to_string())?;
    let mut input = child.stdin().take().expect("piped stdin");
    let (written, writes) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = written.send(write_line(&mut input, &request));
    });
    let stdout = child.stdout().take().expect("piped stdout");
    let (sender, frames) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        loop {
            let frame = read_line::<BashStreamFrame>(&mut reader);
            let terminal = !matches!(frame, Ok(BashStreamFrame::Output { .. }));
            if sender.send(frame).is_err() || terminal {
                break;
            }
        }
    });
    loop {
        if cancel.is_cancelled() {
            return Err("native shell operation cancelled".into());
        }
        if let Ok(Err(error)) = writes.try_recv() {
            return Err(error);
        }
        match frames.recv_timeout(Duration::from_millis(5)) {
            Ok(Ok(BashStreamFrame::Output { data })) => {
                output
                    .blocking_send(data)
                    .map_err(|_| "shell output consumer closed")?;
            }
            Ok(Ok(BashStreamFrame::Complete { response })) => return response,
            Ok(Err(error)) => return Err(error),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err("native shell operation output closed".into());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

fn serve_bash_stream(extension: Arc<dyn Extension>) -> Result<()> {
    let mut input = std::io::BufReader::new(std::io::stdin());
    let request: BashStreamRequest = read_line(&mut input).map_err(Error::Config)?;
    let timeout = request
        .timeout_secs
        .map(Duration::try_from_secs_f64)
        .transpose()
        .map_err(|error| Error::Config(format!("invalid shell timeout: {error}")))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
        let result = extension.execute_user_bash(&request.event, request.state.as_ref(), timeout, CancellationToken::new(), sender);
        tokio::pin!(result);
        let mut stdout = std::io::stdout().lock();
        let mut open = true;
        loop {
            tokio::select! {
                biased;
                data = receiver.recv(), if open => match data {
                    Some(data) => write_line(&mut stdout, &BashStreamFrame::Output { data }).map_err(Error::Config)?,
                    None => open = false,
                },
                response = &mut result => {
                    while let Ok(data) = receiver.try_recv() {
                        write_line(&mut stdout, &BashStreamFrame::Output { data }).map_err(Error::Config)?;
                    }
                    write_line(&mut stdout, &BashStreamFrame::Complete { response }).map_err(Error::Config)?;
                    return Ok(());
                }
            }
        }
    })
}

fn call_duplex<T: serde::de::DeserializeOwned>(
    endpoint: &Endpoint,
    request: ProtocolRequest,
    host: Arc<dyn ToolHost>,
    cancellation: &CancellationToken,
    runtime: &tokio::runtime::Handle,
) -> ExtensionResult<T> {
    let command = endpoint.command("--pi-rs-extension-v2");
    let mut child = ChildGuard::spawn(command).map_err(|error| error.to_string())?;
    let mut input = child.stdin().take().expect("piped stdin");
    let output = child.stdout().take().expect("piped stdout");
    let (writer, writes) = std::sync::mpsc::sync_channel::<Value>(1);
    let (errors, failed) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        for value in writes {
            if let Err(error) = write_line(&mut input, &value) {
                let _ = errors.send(error);
                break;
            }
        }
    });
    let (sender, frames) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut output = std::io::BufReader::new(output);
        loop {
            let frame = read_line::<DuplexFrame>(&mut output);
            let terminal = !matches!(
                frame,
                Ok(DuplexFrame::Call { .. }
                    | DuplexFrame::SendMessage { .. }
                    | DuplexFrame::Ui { .. }
                    | DuplexFrame::Progress { .. })
            );
            if sender.send(frame).is_err() || terminal {
                break;
            }
        }
    });
    writer
        .send(
            serde_json::to_value(DuplexRequest {
                request,
                tools: host.tools(),
                has_ui: host.has_ui(),
                has_progress: host.supports_progress(),
                has_custom_messages: host.supports_custom_messages(),
                has_keyed_messages: host.supports_keyed_messages(),
            })
            .map_err(|error| error.to_string())?,
        )
        .map_err(|_| "native extension input closed")?;
    loop {
        if cancellation.is_cancelled() {
            return Err(
                "native extension operation cancelled; outcome requires reconciliation".into(),
            );
        }
        if let Ok(error) = failed.try_recv() {
            return Err(error);
        }
        match frames.recv_timeout(Duration::from_millis(5)) {
            Ok(Ok(DuplexFrame::SendMessage {
                message,
                delivery,
                key,
            })) => {
                let response = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Err("native extension cancelled during message delivery".to_owned()),
                        result = async { match key { Some(key) => host.send_message_with_key(message, delivery, &key).await, None => host.send_message(message, delivery).await } } => result,
                    }
                });
                writer
                    .send(serde_json::to_value(response).map_err(|error| error.to_string())?)
                    .map_err(|_| "native extension input closed")?;
            }
            Ok(Ok(DuplexFrame::Progress { partial_result })) => {
                let response = runtime.block_on(host.report_progress(partial_result));
                writer
                    .send(serde_json::to_value(response).map_err(|error| error.to_string())?)
                    .map_err(|_| "native extension input closed")?;
            }
            Ok(Ok(DuplexFrame::Call { calls })) => {
                if calls.len() > 4096 {
                    return Err("native extension batch exceeds execution limit".into());
                }
                let results = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Err("native extension cancelled during host call".to_owned()),
                        results = host.call_batch(calls) => Ok(results),
                    }
                })?;
                let results: Vec<_> = if endpoint.text_result_v2 {
                    results
                } else {
                    results
                        .into_iter()
                        .map(|result| result.legacy_text_result())
                        .collect()
                };
                writer
                    .send(serde_json::to_value(results).map_err(|error| error.to_string())?)
                    .map_err(|_| "native extension input closed")?;
            }
            Ok(Ok(DuplexFrame::Ui { request })) => {
                let response = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Err("native extension cancelled during UI request".to_owned()),
                        result = host.ui(request) => result,
                    }
                });
                writer
                    .send(serde_json::to_value(response).map_err(|error| error.to_string())?)
                    .map_err(|_| "native extension input closed")?;
            }
            Ok(Ok(DuplexFrame::Complete { response })) => {
                let result = serde_json::from_value(response?)
                    .map_err(|error| format!("invalid native extension result: {error}"));
                // A complete frame ends the operation. Drop also reaps descendants.
                return result;
            }
            Ok(Err(error)) => return Err(error),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err("native extension output closed".into());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
}

struct RemoteHost {
    tools: Vec<ToolSpec>,
    has_ui: bool,
    has_progress: bool,
    has_custom_messages: bool,
    has_keyed_messages: bool,
    io: Arc<std::sync::Mutex<(std::io::BufReader<std::io::Stdin>, std::io::Stdout)>>,
}
#[async_trait::async_trait]
impl ToolHost for RemoteHost {
    fn supports_progress(&self) -> bool {
        self.has_progress
    }
    async fn report_progress(&self, partial_result: Value) -> ExtensionResult<()> {
        if !self.has_progress {
            return Ok(());
        }
        let io = self.io.clone();
        tokio::task::spawn_blocking(move || {
            let mut io = io
                .lock()
                .map_err(|_| "native extension connection lock failed")?;
            write_line(&mut io.1, &DuplexFrame::Progress { partial_result })?;
            read_line::<ExtensionResult<()>>(&mut io.0)?
        })
        .await
        .map_err(|error| error.to_string())?
    }
    fn has_ui(&self) -> bool {
        self.has_ui
    }
    async fn ui(&self, request: pi_rs_core::UiRequest) -> ExtensionResult<pi_rs_core::UiResponse> {
        if !self.has_ui {
            return Err("interactive extension UI is unavailable".into());
        }
        request.validate()?;
        let io = self.io.clone();
        tokio::task::spawn_blocking(move || {
            let mut io = io
                .lock()
                .map_err(|_| "native extension connection lock failed")?;
            write_line(&mut io.1, &DuplexFrame::Ui { request })?;
            read_line::<ExtensionResult<pi_rs_core::UiResponse>>(&mut io.0)?
        })
        .await
        .map_err(|error| error.to_string())?
    }
    fn tools(&self) -> Vec<ToolSpec> {
        self.tools.clone()
    }
    fn supports_custom_messages(&self) -> bool {
        self.has_custom_messages
    }
    async fn send_message(
        &self,
        message: pi_rs_core::CustomMessage,
        delivery: pi_rs_core::CustomMessageDelivery,
    ) -> ExtensionResult<()> {
        if !self.has_custom_messages {
            return Err("custom message delivery is unavailable".into());
        }
        let io = self.io.clone();
        tokio::task::spawn_blocking(move || {
            let mut io = io
                .lock()
                .map_err(|_| "native extension connection lock failed")?;
            write_line(
                &mut io.1,
                &DuplexFrame::SendMessage {
                    message,
                    delivery,
                    key: None,
                },
            )?;
            read_line::<ExtensionResult<()>>(&mut io.0)?
        })
        .await
        .map_err(|error| error.to_string())?
    }
    fn supports_keyed_messages(&self) -> bool {
        self.has_keyed_messages
    }
    async fn send_message_with_key(
        &self,
        message: pi_rs_core::CustomMessage,
        delivery: pi_rs_core::CustomMessageDelivery,
        key: &str,
    ) -> ExtensionResult<()> {
        if !self.has_keyed_messages {
            return Err("keyed custom messages are unavailable".into());
        }
        let io = self.io.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut io = io
                .lock()
                .map_err(|_| "native extension connection lock failed")?;
            write_line(
                &mut io.1,
                &DuplexFrame::SendMessage {
                    message,
                    delivery,
                    key: Some(key),
                },
            )?;
            read_line::<ExtensionResult<()>>(&mut io.0)?
        })
        .await
        .map_err(|error| error.to_string())?
    }
    async fn call(&self, name: &str, arguments: Value) -> Outcome {
        self.call_batch(vec![(name.into(), arguments)])
            .await
            .remove(0)
    }
    async fn call_batch(&self, calls: Vec<(String, Value)>) -> Vec<Outcome> {
        let count = calls.len();
        let io = self.io.clone();
        let result = tokio::task::spawn_blocking(move || -> ExtensionResult<Vec<Outcome>> {
            let mut io = io
                .lock()
                .map_err(|_| "native extension connection lock failed")?;
            write_line(&mut io.1, &DuplexFrame::Call { calls })?;
            let outcomes: Vec<Outcome> = read_line(&mut io.0)?;
            if outcomes.len() != count {
                return Err("native extension received an invalid batch size".into());
            }
            Ok(outcomes)
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);
        result.unwrap_or_else(|error| vec![Outcome::Unknown(error); count])
    }
}
fn serve_duplex(extension: Arc<dyn Extension>) -> Result<()> {
    let mut input = std::io::BufReader::new(std::io::stdin());
    let request: DuplexRequest = read_line(&mut input).map_err(Error::Config)?;
    let host = Arc::new(RemoteHost {
        tools: request.tools,
        has_ui: request.has_ui,
        has_progress: request.has_progress,
        has_custom_messages: request.has_custom_messages,
        has_keyed_messages: request.has_keyed_messages,
        io: Arc::new(std::sync::Mutex::new((input, std::io::stdout()))),
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let response = runtime.block_on(dispatch_with_host(extension, request.request, host.clone()));
    let mut io = host
        .io
        .lock()
        .map_err(|_| Error::Config("native extension connection lock failed".into()))?;
    write_line(&mut io.1, &DuplexFrame::Complete { response }).map_err(Error::Config)
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Select the field naming advertised by the executable. Older v1 executables
/// never advertised this capability and must keep receiving the v1 names.
fn wire_request(request: &ProtocolRequest, pi_fields: bool) -> ExtensionResult<Value> {
    let mut value = serde_json::to_value(request).map_err(|error| error.to_string())?;
    if pi_fields {
        if value["type"] == "before_tree" {
            value["lifecycleFieldNames"] = Value::String("camelCase".into());
        }
        return Ok(value);
    }
    fn rename(value: &mut Value, pairs: &[(&str, &str)]) {
        if let Some(object) = value.as_object_mut() {
            for (new, old) in pairs {
                if let Some(field) = object.remove(*new) {
                    object.insert((*old).to_owned(), field);
                }
            }
        }
    }
    match value["type"].as_str() {
        Some("before_tree") => rename(
            &mut value["preparation"],
            &[
                ("targetId", "target_id"),
                ("oldLeafId", "old_leaf_id"),
                ("commonAncestorId", "common_ancestor_id"),
                ("entriesToSummarize", "entries_to_summarize"),
                ("userWantsSummary", "user_wants_summary"),
                ("customInstructions", "custom_instructions"),
                ("replaceInstructions", "replace_instructions"),
            ],
        ),
        Some("before_session_transition") => rename(
            &mut value["event"],
            &[
                ("targetSessionFile", "target_session_file"),
                ("entryId", "entry_id"),
            ],
        ),
        Some("lifecycle") => {
            let event = &mut value["event"];
            match event["type"].as_str() {
                Some("session_start") => {
                    rename(event, &[("previousSessionFile", "previous_session_file")])
                }
                Some("session_shutdown") => {
                    rename(event, &[("targetSessionFile", "target_session_file")])
                }
                Some("agent_start" | "agent_end") => {
                    rename(event, &[("sessionId", "session_id"), ("runId", "run_id")])
                }
                Some("turn_start" | "turn_end") => {
                    rename(event, &[("runId", "run_id"), ("turnIndex", "turn_index")])
                }
                Some("session_tree") => rename(
                    event,
                    &[
                        ("newLeafId", "new_leaf_id"),
                        ("oldLeafId", "old_leaf_id"),
                        ("summaryEntry", "summary_entry"),
                        ("fromExtension", "from_extension"),
                    ],
                ),
                _ => {}
            }
            if let Some(messages) = event.get_mut("messages").and_then(Value::as_array_mut) {
                for message in messages {
                    rename(message, &[("recordId", "record_id")]);
                }
            }
        }
        _ => {}
    }
    Ok(value)
}

fn call<T: serde::de::DeserializeOwned>(
    endpoint: &Endpoint,
    request: ProtocolRequest,
    cancel: Option<&CancellationToken>,
) -> ExtensionResult<T> {
    let mut wire = wire_request(&request, endpoint.pi_lifecycle_fields)?;
    if !endpoint.text_result_v2
        && wire["type"] == "lifecycle"
        && wire["event"]["type"] == "tool_execution_end"
    {
        let result = &mut wire["event"]["result"];
        if let Ok(outcome) = serde_json::from_value::<Outcome>(result.clone()) {
            *result = serde_json::to_value(outcome.legacy_text_result())
                .map_err(|error| error.to_string())?;
        }
    }
    let bytes = serde_json::to_vec(&wire).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_REQUEST {
        return Err("extension request exceeds 64 MiB".into());
    }
    let command = endpoint.command("--pi-rs-extension-v1");
    let mut child = ChildGuard::spawn(command).map_err(|error| error.to_string())?;
    let mut input = child.stdin().take().expect("piped stdin");
    let output = child.stdout().take().expect("piped stdout");
    let (writer, written) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = writer.send(input.write_all(&bytes));
    });
    let (reader, read) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = output
            .take(MAX_MESSAGE + 1)
            .read_to_end(&mut bytes)
            .map(|_| bytes);
        let _ = reader.send(result);
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(
                "native extension operation cancelled; outcome requires reconciliation".into(),
            );
        }
        if Instant::now() >= deadline {
            return Err(
                "native extension exceeded 30 seconds; outcome requires reconciliation".into(),
            );
        }
        if let Some(status) = child
            .process
            .try_wait()
            .map_err(|error| error.to_string())?
        {
            break status;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    written
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| "extension input writer timed out")?
        .map_err(|error| error.to_string())?;
    let bytes = read
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .map_err(|_| "extension output reader timed out")?
        .map_err(|error| error.to_string())?;
    child.finished = true;
    if !status.success() {
        return Err(format!("native extension exited with {status}"));
    }
    if bytes.len() as u64 > MAX_MESSAGE {
        return Err("native extension response exceeds 4 MiB".into());
    }
    let envelope: Envelope = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid native extension response: {error}"))?;
    if envelope.version != PROTOCOL {
        return Err("unsupported native extension protocol version".into());
    }
    serde_json::from_value(envelope.response?)
        .map_err(|error| format!("invalid native extension result: {error}"))
}
#[cfg(windows)]
type NativeChild = Box<dyn process_wrap::std::ChildWrapper>;
#[cfg(not(windows))]
type NativeChild = std::process::Child;
struct ChildGuard {
    process: NativeChild,
    finished: bool,
}
impl ChildGuard {
    fn spawn(command: Command) -> std::io::Result<Self> {
        #[cfg(windows)]
        let process = {
            let mut wrapped = process_wrap::std::CommandWrap::from(command);
            wrapped.wrap(process_wrap::std::JobObject);
            wrapped.spawn()?
        };
        #[cfg(not(windows))]
        let process = {
            let mut command = command;
            command.spawn()?
        };
        Ok(Self {
            process,
            finished: false,
        })
    }
    fn stdin(&mut self) -> &mut Option<std::process::ChildStdin> {
        #[cfg(windows)]
        {
            self.process.stdin()
        }
        #[cfg(not(windows))]
        {
            &mut self.process.stdin
        }
    }
    fn stdout(&mut self) -> &mut Option<std::process::ChildStdout> {
        #[cfg(windows)]
        {
            self.process.stdout()
        }
        #[cfg(not(windows))]
        {
            &mut self.process.stdout
        }
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        #[cfg(unix)]
        {
            // Reap descendants that kept a pipe open after their parent exited.
            unsafe {
                libc::kill(-(self.process.id() as i32), libc::SIGKILL);
            }
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Entry point for a native extension binary. Its stdout is reserved for the
/// protocol response. Diagnostic output belongs on stderr.
pub fn serve_extension(extension: Arc<dyn Extension>) -> Result<()> {
    if std::env::args().any(|argument| argument == "--pi-rs-extension-bash-v1") {
        return serve_bash_stream(extension);
    }
    if std::env::args().any(|argument| argument == "--pi-rs-extension-v2") {
        return serve_duplex(extension);
    }
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_REQUEST + 1)
        .read_to_end(&mut input)?;
    let response = if input.len() as u64 > MAX_REQUEST {
        Err("extension request exceeds 64 MiB".into())
    } else {
        match serde_json::from_slice::<Value>(&input) {
            Ok(raw) => {
                let legacy_tree_response =
                    raw["type"] == "before_tree" && raw["lifecycleFieldNames"] != "camelCase";
                match serde_json::from_value(raw) {
                    Ok(request) => {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;
                        runtime
                            .block_on(dispatch(extension, request))
                            .map(|mut response| {
                                if legacy_tree_response {
                                    legacy_tree_decision(&mut response);
                                }
                                response
                            })
                    }
                    Err(error) => Err(format!("invalid extension request: {error}")),
                }
            }
            Err(error) => Err(format!("invalid extension request: {error}")),
        }
    };
    let envelope = Envelope {
        version: PROTOCOL,
        response,
    };
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &envelope)?;
    writeln!(stdout)?;
    Ok(())
}

fn legacy_tree_decision(response: &mut Value) {
    if let Some(object) = response.as_object_mut() {
        for (modern, legacy) in [
            ("customInstructions", "custom_instructions"),
            ("replaceInstructions", "replace_instructions"),
        ] {
            if let Some(field) = object.remove(modern) {
                object.insert(legacy.into(), field);
            }
        }
    }
}

async fn dispatch(
    extension: Arc<dyn Extension>,
    request: ProtocolRequest,
) -> ExtensionResult<Value> {
    dispatch_with_host(extension, request, Arc::new(DetachedHost)).await
}

async fn dispatch_with_host(
    extension: Arc<dyn Extension>,
    request: ProtocolRequest,
    host: Arc<dyn ToolHost>,
) -> ExtensionResult<Value> {
    match request {
        ProtocolRequest::Describe => encode(Ok(Description {
            id: extension.id().into(),
            dependencies: extension
                .dependency_ids()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            commands: extension
                .command_names()
                .into_iter()
                .map(str::to_owned)
                .collect(),
            flags: extension.cli_flags(),
            host_calls: true,
            command_entries: extension.supports_command_entries(),
            command_effects: extension.supports_command_effects(),
            input_hook: extension.handles_input(),
            tool_result_hook: extension.handles_tool_result(),
            text_result_v2: true,
            tool_message_error: true,
            tool_message_structured: true,
            tool_message_provenance_v2: true,
            tool_call_hook: extension.handles_tool_call(),
            user_bash_hook: extension.handles_user_bash(),
            user_bash_operations_hook: extension.handles_user_bash_operations(),
            lifecycle_hook: extension.handles_lifecycle(),
            lifecycle_host_calls: true,
            tool_hook_host_calls: true,
            before_tool_host_calls: true,
            request_hook_host_calls: true,
            input_hook_host_calls: true,
            context_hook_host_calls: true,
            transition_hook_host_calls: true,
            tree_hook_host_calls: true,
            resources_hook_host_calls: true,
            user_bash_hook_host_calls: true,
            user_bash_stream_host_calls: true,
            tool_execution_lifecycle_hook: extension.handles_tool_execution_lifecycle(),
            tool_execution_update_hook: extension.handles_tool_execution_updates(),
            model_selection_hook: extension.handles_model_selection(),
            session_lifecycle_hook: extension.handles_session_lifecycle(),
            session_transition_reasons: true,
            pi_lifecycle_fields: true,
            session_info_hook: true,
            cache_warming_hook: extension.handles_cache_warming(),
            tree_navigation_hook: extension.handles_tree_navigation(),
            session_transition_hook: extension.handles_session_transitions(),
            resource_discovery_hook: extension.handles_resource_discovery(),
            context_with_system_hook: extension.handles_context_with_system(),
            tools: extension
                .tools()
                .into_iter()
                .map(|tool| DescriptionTool {
                    spec: tool.spec(),
                    execution: tool.execution(),
                    can_poll: tool.can_poll(),
                    transactional: true,
                    prepares_arguments: true,
                })
                .collect(),
        })),
        ProtocolRequest::Migrate { state } => encode(extension.migrate(state.as_ref())),
        ProtocolRequest::PrepareArguments { name, arguments } => {
            let tool = extension
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == name)
                .ok_or_else(|| "unknown extension tool".to_owned())?;
            encode(tool.prepare_arguments(arguments).await)
        }
        ProtocolRequest::Context { mut request, state } => {
            extension.context(&mut request, state.as_ref())?;
            encode(Ok(request))
        }
        ProtocolRequest::Input { event, state } => {
            if host.supports_custom_messages() {
                encode(
                    extension
                        .input_with_host(&event, state.as_ref(), host)
                        .await,
                )
            } else {
                encode(extension.input(&event, state.as_ref()).await)
            }
        }
        ProtocolRequest::UserBash { event, state } => {
            if host.supports_custom_messages() {
                encode(
                    extension
                        .user_bash_with_host(&event, state.as_ref(), host)
                        .await,
                )
            } else {
                encode(extension.user_bash(&event, state.as_ref()).await)
            }
        }
        ProtocolRequest::UserBashOperations { event, state } => {
            if host.supports_custom_messages() {
                encode(
                    extension
                        .user_bash_operations_with_host(&event, state.as_ref(), host)
                        .await,
                )
            } else {
                encode(extension.user_bash_operations(&event, state.as_ref()).await)
            }
        }
        ProtocolRequest::ExecuteUserBash {
            event,
            state,
            timeout_secs,
        } => {
            let timeout = timeout_secs
                .map(Duration::try_from_secs_f64)
                .transpose()
                .map_err(|error| error.to_string())?;
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
            let result = extension.execute_user_bash_with_host(
                &event,
                state.as_ref(),
                timeout,
                CancellationToken::new(),
                sender,
                host.clone(),
            );
            tokio::pin!(result);
            let mut open = true;
            loop {
                tokio::select! {
                    biased;
                    data = receiver.recv(), if open => match data { Some(data) => host.report_progress(serde_json::json!({"shellOutput":data})).await?, None => open = false },
                    response = &mut result => {
                        while let Ok(data) = receiver.try_recv() { host.report_progress(serde_json::json!({"shellOutput":data})).await?; }
                        break encode(response);
                    }
                }
            }
        }
        ProtocolRequest::Lifecycle { event, state } => {
            if host.supports_custom_messages() {
                encode(
                    extension
                        .lifecycle_with_host(&event, state.as_ref(), host)
                        .await,
                )
            } else {
                encode(extension.lifecycle(&event, state.as_ref()).await)
            }
        }
        ProtocolRequest::DiscoverResources { event, state } => {
            if host.supports_custom_messages() {
                encode(
                    extension
                        .discover_resources_with_host(&event, state.as_ref(), host)
                        .await,
                )
            } else {
                encode(extension.discover_resources(&event, state.as_ref()).await)
            }
        }
        ProtocolRequest::CacheWarmingDecision { event, state } => encode(
            extension
                .cache_warming_decision(&event, state.as_ref())
                .await,
        ),
        ProtocolRequest::BeforeTree { preparation, state } => {
            let cancel = CancellationToken::new();
            if host.supports_custom_messages() {
                encode(
                    extension
                        .before_tree_with_host(&preparation, state.as_ref(), &cancel, host)
                        .await,
                )
            } else {
                encode(
                    extension
                        .before_tree(&preparation, state.as_ref(), &cancel)
                        .await,
                )
            }
        }
        ProtocolRequest::BeforeSessionTransition { event, state } => {
            if host.supports_custom_messages() {
                encode(
                    extension
                        .before_session_transition_with_host(&event, state.as_ref(), host)
                        .await,
                )
            } else {
                encode(
                    extension
                        .before_session_transition(&event, state.as_ref())
                        .await,
                )
            }
        }
        ProtocolRequest::BeforeRequest {
            mut request,
            summary,
            state,
        } => {
            let purpose = if summary {
                RequestPurpose::Summary
            } else {
                RequestPurpose::Turn
            };
            if host.supports_custom_messages() {
                extension
                    .before_request_with_host(&mut request, purpose, state.as_ref(), host)
                    .await?;
            } else {
                extension
                    .before_request(&mut request, purpose, state.as_ref())
                    .await?;
            }
            encode(Ok(request))
        }
        ProtocolRequest::ContextWithSystem {
            mut messages,
            state,
        } => {
            if host.supports_custom_messages() {
                extension
                    .context_with_system_with_host(&mut messages, state.as_ref(), host)
                    .await?;
            } else {
                extension
                    .context_with_system(&mut messages, state.as_ref())
                    .await?;
            }
            encode(Ok(messages))
        }
        ProtocolRequest::Command {
            name,
            arguments,
            state,
        } => {
            if !extension.command_names().contains(&name.as_str()) {
                return Err("unknown extension command".into());
            }
            encode(extension.command(&name, &arguments, state.as_ref()))
        }
        ProtocolRequest::CommandOutput {
            name,
            arguments,
            state,
        } => {
            if !extension.command_names().contains(&name.as_str()) {
                return Err("unknown extension command".into());
            }
            encode(extension.command_output(&name, &arguments, state.as_ref()))
        }
        ProtocolRequest::CommandEffects {
            name,
            arguments,
            state,
        } => {
            if !extension.command_names().contains(&name.as_str()) {
                return Err("unknown extension command".into());
            }
            encode(extension.command_effects(&name, &arguments, state.as_ref()))
        }
        ProtocolRequest::ToolCall {
            name,
            mut arguments,
            operation_id,
            state,
        } => {
            let context = context(&operation_id)?;
            let decision = if host.supports_custom_messages() {
                extension
                    .tool_call_with_host(&name, &mut arguments, &context, state.as_ref(), host)
                    .await?
            } else {
                extension
                    .tool_call(&name, &mut arguments, &context, state.as_ref())
                    .await?
            };
            encode(Ok((arguments, DecisionValue::from(decision))))
        }
        ProtocolRequest::BeforeTool {
            name,
            arguments,
            operation_id,
            state,
        } => {
            let context = context(&operation_id)?;
            let decision = if host.supports_custom_messages() {
                extension
                    .before_tool_with_host(&name, &arguments, &context, state.as_ref(), host)
                    .await
            } else {
                extension
                    .before_tool(&name, &arguments, &context, state.as_ref())
                    .await
            };
            encode(decision.map(DecisionValue::from))
        }
        ProtocolRequest::Invoke {
            name,
            arguments,
            operation_id,
            state,
            transaction,
            programmatic,
        } => {
            let tool = extension
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == name)
                .ok_or_else(|| "unknown extension tool".to_owned())?;
            if transaction {
                let context = context(&operation_id)?;
                let reply = if programmatic {
                    tool.invoke_programmatic(arguments, context, state.as_ref(), host)
                        .await
                } else {
                    tool.invoke_transaction(arguments, context, state.as_ref(), host)
                        .await
                };
                encode(Ok(reply))
            } else {
                encode(Ok(tool
                    .invoke_with_state(arguments, context(&operation_id)?, state.as_ref())
                    .await))
            }
        }
        ProtocolRequest::Poll {
            name,
            reference,
            operation_id,
        } => {
            let tool = extension
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == name && tool.can_poll())
                .ok_or_else(|| "unknown polling tool".to_owned())?;
            encode(Ok(tool.poll(&reference, context(&operation_id)?).await))
        }
        ProtocolRequest::AfterTool {
            name,
            outcome,
            state,
        } => encode(extension.after_tool(&name, &outcome, state.as_ref())),
        ProtocolRequest::ToolResult {
            call,
            outcome,
            operation_id,
            state,
            text_result_v2,
        } => {
            let context = context(&operation_id)?;
            let result = if host.supports_custom_messages() {
                extension
                    .tool_result_update_with_host(&call, &outcome, &context, state.as_ref(), host)
                    .await
            } else {
                extension
                    .tool_result_update(&call, &outcome, &context, state.as_ref())
                    .await
            };
            encode(result.map(|result| {
                if text_result_v2 {
                    result
                } else {
                    result.map(|outcome| outcome.legacy_text_result())
                }
            }))
        }
        ProtocolRequest::BeforeCompaction {
            summary,
            keep_turns,
            state,
        } => {
            let mut compaction = Compaction {
                summary,
                keep_turns,
            };
            let decision = extension.before_compaction(&mut compaction, state.as_ref())?;
            encode(Ok((
                compaction.summary,
                compaction.keep_turns,
                DecisionValue::from(decision),
            )))
        }
        ProtocolRequest::AfterCompaction {
            summary,
            keep_turns,
            state,
        } => encode(extension.after_compaction(
            &Compaction {
                summary,
                keep_turns,
            },
            state.as_ref(),
        )),
    }
}
fn context(operation_id: &str) -> ExtensionResult<ToolContext> {
    Ok(ToolContext {
        operation_id: operation_id.parse().map_err(|_| "invalid operation ID")?,
        cancellation: CancellationToken::new(),
    })
}
fn encode<T: Serialize>(value: ExtensionResult<T>) -> ExtensionResult<Value> {
    serde_json::to_value(value?).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_tool_messages_decode_without_status_and_omit_false_on_wire() {
        let legacy = serde_json::json!({"role":"tool","call_id":"call","content":"text"});
        let message: Message = serde_json::from_value(legacy.clone()).unwrap();
        assert!(matches!(
            &message,
            Message::Tool {
                is_error: false,
                ..
            }
        ));
        assert!(matches!(
            &message,
            Message::Tool {
                is_structured: None,
                ..
            }
        ));
        assert_eq!(serde_json::to_value(message).unwrap(), legacy);
        let explicit = Message::Tool {
            call_id: "call".into(),
            content: "text".into(),
            is_error: false,
            is_structured: Some(false),
        };
        let encoded = serde_json::to_value(&explicit).unwrap();
        assert_eq!(encoded["is_structured"], false);
        assert_eq!(
            serde_json::from_value::<Message>(encoded).unwrap(),
            explicit
        );
    }

    #[test]
    fn lifecycle_wire_naming_is_negotiated_without_duplicate_fields() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyFork {
            r#type: String,
            entry_id: u64,
            position: String,
        }
        let request = ProtocolRequest::BeforeSessionTransition {
            event: pi_rs_core::BeforeSessionTransition::Fork {
                entry_id: 42,
                position: pi_rs_core::ForkPosition::Before,
            },
            state: None,
        };
        let legacy = wire_request(&request, false).unwrap();
        let decoded: LegacyFork = serde_json::from_value(legacy["event"].clone()).unwrap();
        assert_eq!(
            (
                decoded.r#type.as_str(),
                decoded.entry_id,
                decoded.position.as_str()
            ),
            ("session_before_fork", 42, "before")
        );
        assert!(legacy["event"].get("entryId").is_none());
        let modern = wire_request(&request, true).unwrap();
        assert_eq!(modern["event"]["entryId"], 42);
        assert!(modern["event"].get("entry_id").is_none());
        // The current executable decoder must accept each profile separately.
        assert!(serde_json::from_value::<ProtocolRequest>(legacy).is_ok());
        assert!(serde_json::from_value::<ProtocolRequest>(modern).is_ok());
    }

    #[test]
    fn old_descriptor_defaults_to_legacy_lifecycle_fields() {
        let description: Description = serde_json::from_value(serde_json::json!({
            "id":"legacy", "dependencies":[], "commands":[], "tools":[]
        }))
        .unwrap();
        assert!(!description.pi_lifecycle_fields);
    }

    #[test]
    fn tree_request_and_response_preserve_both_protocol_generations() {
        let request = ProtocolRequest::BeforeTree {
            preparation: TreePreparation {
                target_id: 3,
                old_leaf_id: 5,
                common_ancestor_id: Some(2),
                entries_to_summarize: Vec::new(),
                user_wants_summary: true,
                custom_instructions: None,
                replace_instructions: false,
                label: None,
            },
            state: None,
        };
        let old = wire_request(&request, false).unwrap();
        let new = wire_request(&request, true).unwrap();
        assert_eq!(old["preparation"]["target_id"], 3);
        assert!(old["preparation"].get("targetId").is_none());
        assert!(old.get("lifecycleFieldNames").is_none());
        assert_eq!(new["preparation"]["targetId"], 3);
        assert_eq!(new["lifecycleFieldNames"], "camelCase");
        assert!(serde_json::from_value::<ProtocolRequest>(old).is_ok());
        assert!(serde_json::from_value::<ProtocolRequest>(new).is_ok());

        let mut legacy_response =
            serde_json::json!({"customInstructions":"custom","replaceInstructions":true});
        legacy_tree_decision(&mut legacy_response);
        assert_eq!(legacy_response["custom_instructions"], "custom");
        assert_eq!(legacy_response["replace_instructions"], true);
        assert!(legacy_response.get("customInstructions").is_none());
        let decoded: TreeDecision = serde_json::from_value(legacy_response).unwrap();
        assert_eq!(decoded.custom_instructions.as_deref(), Some("custom"));
        assert_eq!(decoded.replace_instructions, Some(true));
    }

    #[tokio::test]
    async fn progress_protocol_is_optional_for_older_hosts() {
        let request: DuplexRequest = serde_json::from_value(serde_json::json!({
            "request":{"type":"describe"}, "tools":[], "has_ui":false
        }))
        .unwrap();
        assert!(!request.has_progress);
        assert!(!request.has_keyed_messages);
        let legacy: DuplexFrame = serde_json::from_value(serde_json::json!({"type":"send_message","message":{"customType":"test","content":"hello","display":false},"delivery":"steer"})).unwrap();
        assert!(matches!(legacy, DuplexFrame::SendMessage { key: None, .. }));
        let keyed = DuplexFrame::SendMessage {
            message: pi_rs_core::CustomMessage {
                custom_type: "test".into(),
                content: pi_rs_core::ContextContent::Text("hello".into()),
                display: false,
                details: None,
            },
            delivery: pi_rs_core::CustomMessageDelivery::Steer,
            key: Some("stable".into()),
        };
        assert_eq!(serde_json::to_value(keyed).unwrap()["key"], "stable");
        let frame = DuplexFrame::Progress {
            partial_result: serde_json::json!({"text":"halfway"}),
        };
        let encoded = serde_json::to_value(frame).unwrap();
        assert_eq!(encoded["type"], "progress");
        assert_eq!(encoded["partial_result"]["text"], "halfway");
        assert!(matches!(
            serde_json::from_value::<DuplexFrame>(encoded).unwrap(),
            DuplexFrame::Progress { .. }
        ));
    }

    #[tokio::test]
    async fn tool_execution_lifecycle_capability_and_events_round_trip() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "tool-lifecycle"
            }
            fn handles_lifecycle(&self) -> bool {
                true
            }
            fn handles_tool_execution_lifecycle(&self) -> bool {
                true
            }
            fn handles_tool_execution_updates(&self) -> bool {
                true
            }
            async fn lifecycle(
                &self,
                event: &LifecycleEvent,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                assert!(
                    matches!(event, LifecycleEvent::ToolExecutionStart { tool_call_id, args, .. } if tool_call_id == "call" && args["path"] == "test")
                );
                Ok(())
            }
        }
        let extension: Arc<dyn Extension> = Arc::new(Hook);
        let description = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(description["tool_execution_lifecycle_hook"], true);
        assert_eq!(description["tool_execution_update_hook"], true);
        let request = ProtocolRequest::Lifecycle {
            event: LifecycleEvent::ToolExecutionStart {
                tool_call_id: "call".into(),
                tool_name: "read".into(),
                args: serde_json::json!({"path":"test"}),
                parent_tool_call_id: None,
            },
            state: None,
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        let decoded = serde_json::from_slice(&encoded).unwrap();
        dispatch(extension, decoded).await.unwrap();
    }

    #[tokio::test]
    async fn tool_call_hook_is_advertised_and_transports_mutations() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "arguments"
            }
            fn handles_tool_call(&self) -> bool {
                true
            }
            async fn tool_call(
                &self,
                _: &str,
                args: &mut Value,
                _: &ToolContext,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Decision> {
                args["path"] = "changed".into();
                Ok(Decision::Allow)
            }
        }
        let extension: Arc<dyn Extension> = Arc::new(Hook);
        let description = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(description["tool_call_hook"], true);
        let request = ProtocolRequest::ToolCall {
            name: "fixture".into(),
            arguments: serde_json::json!({"path":"original"}),
            operation_id: "00000000-0000-4000-8000-000000000001".into(),
            state: None,
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        let result = dispatch(extension, serde_json::from_slice(&encoded).unwrap())
            .await
            .unwrap();
        assert_eq!(result[0]["path"], "changed");
        assert_eq!(result[1]["allow"], true);
    }

    #[tokio::test]
    async fn tool_result_hook_is_advertised_and_dispatched() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "result"
            }
            fn handles_tool_result(&self) -> bool {
                true
            }
            async fn tool_result(
                &self,
                call: &pi_rs_core::ToolCall,
                outcome: &Outcome,
                _: &ToolContext,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<Outcome>> {
                assert_eq!(call.name, "fixture");
                assert_eq!(outcome, &Outcome::Succeeded("original".into()));
                Ok(Some(Outcome::Succeeded("changed".into())))
            }
        }
        let extension: Arc<dyn Extension> = Arc::new(Hook);
        let description = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(description["tool_result_hook"], true);
        let result = dispatch(
            extension,
            ProtocolRequest::ToolResult {
                call: pi_rs_core::ToolCall {
                    id: "call".into(),
                    name: "fixture".into(),
                    arguments: "{}".into(),
                },
                outcome: Outcome::Succeeded("original".into()),
                operation_id: "00000000-0000-4000-8000-000000000001".into(),
                state: None,
                text_result_v2: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_value::<Option<Outcome>>(result).unwrap(),
            Some(Outcome::Succeeded("changed".into()))
        );
    }

    #[tokio::test]
    async fn result_patch_dispatch_preserves_details() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "patch"
            }
            fn handles_tool_result(&self) -> bool {
                true
            }
            async fn tool_result_patch(
                &self,
                _: &pi_rs_core::ToolCall,
                _: &Outcome,
                _: &ToolContext,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<pi_rs_core::ToolResultPatch>> {
                Ok(Some(pi_rs_core::ToolResultPatch {
                    output: Some("changed".into()),
                    details: None,
                }))
            }
        }
        let result = dispatch(
            Arc::new(Hook),
            ProtocolRequest::ToolResult {
                call: pi_rs_core::ToolCall {
                    id: "call".into(),
                    name: "fixture".into(),
                    arguments: "{}".into(),
                },
                outcome: Outcome::SucceededWithDetails {
                    output: "original".into(),
                    details: serde_json::json!({"retained": true}),
                },
                operation_id: "00000000-0000-4000-8000-000000000001".into(),
                state: None,
                text_result_v2: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_value::<Option<Outcome>>(result).unwrap(),
            Some(Outcome::SucceededWithDetails {
                output: "changed".into(),
                details: serde_json::json!({"retained": true})
            })
        );
    }

    #[tokio::test]
    async fn failed_result_patch_is_dispatched() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "patch"
            }
            fn handles_tool_result(&self) -> bool {
                true
            }
            async fn tool_result_patch(
                &self,
                _: &pi_rs_core::ToolCall,
                _: &Outcome,
                _: &ToolContext,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<pi_rs_core::ToolResultPatch>> {
                Ok(Some(pi_rs_core::ToolResultPatch {
                    output: Some("changed".into()),
                    details: None,
                }))
            }
        }
        let result = dispatch(
            Arc::new(Hook),
            ProtocolRequest::ToolResult {
                call: pi_rs_core::ToolCall {
                    id: "call".into(),
                    name: "fixture".into(),
                    arguments: "{}".into(),
                },
                outcome: Outcome::Failed("original".into()),
                operation_id: "00000000-0000-4000-8000-000000000001".into(),
                state: None,
                text_result_v2: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::from_value::<Option<Outcome>>(result).unwrap(),
            Some(Outcome::TextResult {
                output: "changed".into(),
                details: None,
                is_error: true
            })
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_context_hooks_distinguish_unchanged_and_rewritten_provenance() {
        use std::os::unix::fs::PermissionsExt;
        let script = r##"#!/usr/bin/env python3
import json,sys,os
r=json.load(sys.stdin)
name=os.path.basename(sys.argv[0])
if r['type']=='describe':
    value={'id':name,'dependencies':[],'commands':[],'tools':[],'context_with_system_hook':True,'tool_message_error':name!='legacy','tool_message_structured':name in ('prior','modern'),'tool_message_provenance_v2':name=='modern'}
else:
    value=r.get('messages') if r['type']=='context_with_system' else r['request']
    messages=value if isinstance(value,list) else value['messages']
    for m in messages:
        if m['role']=='tool':
            if name=='legacy': m.pop('is_error',None)
            if name!='modern': m.pop('is_structured',None)
            if m['call_id']=='to_text':
                m['content']='replacement text'
                if name=='modern': m['is_structured']=False
            if m['call_id']=='to_structured':
                m['content']='{"data":{"content":[{"type":"image","mimeType":"image/png","data":"YWJj"}]}}'
                if name=='modern': m['is_structured']=True
print(json.dumps({'version':1,'response':{'Ok':value}}))
"##;
        let dir = tempfile::tempdir().unwrap();
        for name in ["legacy", "intermediate", "prior", "modern"] {
            let path = dir.path().join(name);
            std::fs::write(&path, script).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            let extension = ExecutableExtension::load(path).unwrap();
            let item = |id: &str, content: &str, is_structured| Message::Tool {
                call_id: id.into(),
                content: content.into(),
                is_error: true,
                is_structured: Some(is_structured),
            };
            let original = ModelRequest {
                model: "fixture".into(),
                messages: vec![
                    item("unchanged_structured", "structured", true),
                    item("unchanged_plain", "plain", false),
                    item("to_text", "old structured", true),
                    item("to_structured", "old plain", false),
                ],
                tools: vec![],
                max_output_tokens: 10,
            };
            let check = |messages: &[Message]| {
                assert!(matches!(
                    &messages[0],
                    Message::Tool {
                        is_error: true,
                        is_structured: Some(true),
                        ..
                    }
                ));
                assert!(matches!(
                    &messages[1],
                    Message::Tool {
                        is_error: true,
                        is_structured: Some(false),
                        ..
                    }
                ));
                for (index, expected) in [(2, false), (3, true)] {
                    let Message::Tool {
                        content,
                        is_error,
                        is_structured,
                        ..
                    } = &messages[index]
                    else {
                        panic!("tool message missing")
                    };
                    assert!(*is_error);
                    assert_eq!(
                        *is_structured,
                        if name == "modern" {
                            Some(expected)
                        } else {
                            None
                        }
                    );
                    assert_eq!(
                        pi_rs_core::tool_content_is_structured(content, *is_structured),
                        expected
                    );
                }
            };
            let mut request = original.clone();
            extension.context(&mut request, None).unwrap();
            check(&request.messages);
            let mut request = original.clone();
            extension
                .before_request(&mut request, RequestPurpose::Turn, None)
                .await
                .unwrap();
            check(&request.messages);
            let mut messages = original.messages;
            extension
                .context_with_system(&mut messages, None)
                .await
                .unwrap();
            check(&messages);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nested_host_results_negotiate_outcome_variants_across_processes() {
        use std::os::unix::fs::PermissionsExt;
        struct Host;
        #[async_trait::async_trait]
        impl ToolHost for Host {
            fn tools(&self) -> Vec<ToolSpec> {
                vec![]
            }
            async fn call(&self, name: &str, _: Value) -> Outcome {
                Outcome::TextResult {
                    output: format!("{name} output"),
                    details: Some(serde_json::json!({"id": name})),
                    is_error: name == "failure",
                }
            }
        }
        let script = r##"#!/usr/bin/env python3
import json, sys, os
name=os.path.basename(sys.argv[0])
if '--pi-rs-extension-v1' in sys.argv:
    r=json.load(sys.stdin)
    assert r['type']=='describe'
    d={'id':name, 'dependencies':[], 'commands':[], 'host_calls':True, 'text_result_v2':name=='modern', 'tools':[{'spec':{'name':'outer','description':'test','parameters':{'type':'object'}},'execution':'sequential','transactional':True}]}
    print(json.dumps({'version':1,'response':{'Ok':d}}))
else:
    request=json.loads(sys.stdin.readline())
    assert request['request']['type']=='invoke'
    print(json.dumps({'type':'call','calls':[['success',{}],['failure',{}]]}),flush=True)
    results=json.loads(sys.stdin.readline())
    if name=='modern':
        assert results[0]['status']=='text_result' and results[0]['detail']['is_error'] is False
        assert results[1]['status']=='text_result' and results[1]['detail']['is_error'] is True
        assert results[1]['detail']['details']=={'id':'failure'}
    else:
        assert results[0]=={'status':'succeeded_with_details','detail':{'output':'success output','details':{'id':'success'}}}
        assert results[1]=={'status':'failed','detail':'failure output'}
    print(json.dumps({'type':'complete','response':{'Ok':{'outcome':{'status':'succeeded','detail':'nested calls complete'}}}}),flush=True)
"##;
        let dir = tempfile::tempdir().unwrap();
        for name in ["legacy", "modern"] {
            let path = dir.path().join(name);
            std::fs::write(&path, script).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            let extension = ExecutableExtension::load(path).unwrap();
            assert_eq!(extension.endpoint.text_result_v2, name == "modern");
            let tool = extension.tools().pop().unwrap();
            let context = ToolContext {
                operation_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                cancellation: CancellationToken::new(),
            };
            let result = tool
                .invoke_with_host(serde_json::json!({}), context, None, Arc::new(Host))
                .await;
            assert_eq!(
                result,
                Outcome::Succeeded("nested calls complete".into()),
                "{name}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_custom_message_host_call_preserves_metadata() {
        use std::os::unix::fs::PermissionsExt;
        struct Host;
        #[async_trait::async_trait]
        impl ToolHost for Host {
            fn tools(&self) -> Vec<ToolSpec> {
                vec![]
            }
            fn supports_custom_messages(&self) -> bool {
                true
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                Outcome::Skipped("unused".into())
            }
            async fn send_message(
                &self,
                message: pi_rs_core::CustomMessage,
                delivery: pi_rs_core::CustomMessageDelivery,
            ) -> ExtensionResult<()> {
                assert_eq!(delivery, pi_rs_core::CustomMessageDelivery::FollowUp);
                assert_eq!(message.custom_type, "notice");
                assert!(!message.display);
                assert_eq!(message.details, Some(serde_json::json!({"key": 3})));
                Ok(())
            }
        }
        let script = r##"#!/usr/bin/env python3
import json, sys
if '--pi-rs-extension-v1' in sys.argv:
    r=json.load(sys.stdin)
    d={'id':'message-test','dependencies':[],'commands':[],'host_calls':True,'tools':[{'spec':{'name':'outer','description':'test','parameters':{'type':'object'}},'execution':'sequential','transactional':True}]}
    print(json.dumps({'version':1,'response':{'Ok':d}}))
else:
    r=json.loads(sys.stdin.readline())
    assert r['has_custom_messages'] is True
    print(json.dumps({'type':'send_message','delivery':'followUp','message':{'customType':'notice','content':'hello','display':False,'details':{'key':3}}}),flush=True)
    assert json.loads(sys.stdin.readline()) == {'Ok':None}
    print(json.dumps({'type':'complete','response':{'Ok':{'outcome':{'status':'succeeded','detail':'done'}}}}),flush=True)
"##;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("message-test");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let extension = ExecutableExtension::load(path).unwrap();
        let tool = extension.tools().pop().unwrap();
        let context = ToolContext {
            operation_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            cancellation: CancellationToken::new(),
        };
        assert_eq!(
            tool.invoke_with_host(serde_json::json!({}), context, None, Arc::new(Host))
                .await,
            Outcome::Succeeded("done".into())
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_lifecycle_callback_uses_negotiated_message_host() {
        use std::os::unix::fs::PermissionsExt;
        struct Host;
        #[async_trait::async_trait]
        impl ToolHost for Host {
            fn tools(&self) -> Vec<ToolSpec> {
                vec![]
            }
            fn supports_custom_messages(&self) -> bool {
                true
            }
            fn supports_keyed_messages(&self) -> bool {
                true
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                Outcome::Skipped("unused".into())
            }
            async fn send_message_with_key(
                &self,
                message: pi_rs_core::CustomMessage,
                delivery: pi_rs_core::CustomMessageDelivery,
                key: &str,
            ) -> ExtensionResult<()> {
                assert_eq!(key, "stable");
                assert_eq!(message.custom_type, "from-hook");
                assert_eq!(delivery, pi_rs_core::CustomMessageDelivery::TriggerTurn);
                Ok(())
            }
        }
        let script = r##"#!/usr/bin/env python3
import json, sys
if '--pi-rs-extension-v1' in sys.argv:
    r=json.load(sys.stdin)
    d={'id':'hook-test','dependencies':[],'commands':[],'tools':[],'lifecycle_hook':True,'lifecycle_host_calls':True,'pi_lifecycle_fields':True}
    print(json.dumps({'version':1,'response':{'Ok':d}}))
else:
    r=json.loads(sys.stdin.readline())
    assert r['request']['type']=='lifecycle' and r['has_custom_messages'] is True and r['has_keyed_messages'] is True
    print(json.dumps({'type':'send_message','delivery':'triggerTurn','key':'stable','message':{'customType':'from-hook','content':'hello','display':False}}),flush=True)
    assert json.loads(sys.stdin.readline()) == {'Ok':None}
    print(json.dumps({'type':'complete','response':{'Ok':None}}),flush=True)
"##;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hook-test");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let extension = ExecutableExtension::load(path).unwrap();
        extension
            .lifecycle_with_host(
                &LifecycleEvent::AgentSettled { aborted: false },
                None,
                Arc::new(Host),
            )
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_input_request_and_tool_hooks_use_message_channel() {
        use std::os::unix::fs::PermissionsExt;
        struct Host(std::sync::atomic::AtomicUsize);
        #[async_trait::async_trait]
        impl ToolHost for Host {
            fn tools(&self) -> Vec<ToolSpec> {
                vec![]
            }
            fn supports_custom_messages(&self) -> bool {
                true
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                Outcome::Skipped("unused".into())
            }
            async fn send_message(
                &self,
                _: pi_rs_core::CustomMessage,
                _: pi_rs_core::CustomMessageDelivery,
            ) -> ExtensionResult<()> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }
        let script = r##"#!/usr/bin/env python3
import json, sys
if '--pi-rs-extension-v1' in sys.argv:
    json.load(sys.stdin)
    d={'id':'hooks','dependencies':[],'commands':[],'tools':[],'input_hook':True,'input_hook_host_calls':True,'request_hook_host_calls':True,'context_hook_host_calls':True,'context_with_system_hook':True,'transition_hook_host_calls':True,'tree_hook_host_calls':True,'tree_navigation_hook':True,'resources_hook_host_calls':True,'resource_discovery_hook':True,'user_bash_hook_host_calls':True,'user_bash_stream_host_calls':True,'user_bash_hook':True,'user_bash_operations_hook':True,'tool_hook_host_calls':True,'before_tool_host_calls':True,'tool_call_hook':True,'tool_result_hook':True,'text_result_v2':True}
    print(json.dumps({'version':1,'response':{'Ok':d}}))
else:
    frame=json.loads(sys.stdin.readline()); r=frame['request']; kind=r['type']
    assert frame['has_custom_messages']
    print(json.dumps({'type':'send_message','delivery':'steer','message':{'customType':kind,'content':'hello','display':False}}),flush=True)
    assert json.loads(sys.stdin.readline())=={'Ok':None}
    if kind=='input': result={'action':'continue'}
    elif kind=='before_request': result=r['request']
    elif kind=='context_with_system': result=r['messages']
    elif kind=='user_bash': result=None
    elif kind=='user_bash_operations': result=False
    elif kind=='execute_user_bash':
        print(json.dumps({'type':'progress','partial_result':{'shellOutput':[111,107]}}),flush=True)
        assert json.loads(sys.stdin.readline())=={'Ok':None}
        result=0
    elif kind=='discover_resources': result={'skillPaths':[],'promptPaths':['fixture.md'],'themePaths':[]}
    elif kind=='before_session_transition': result=False
    elif kind=='before_tree': result=None
    elif kind=='before_tool': result={'allow':True,'reason':None}
    elif kind=='tool_call': result=[r['arguments'],{'allow':True,'reason':None}]
    elif kind=='tool_result': result=None
    else: raise AssertionError(kind)
    print(json.dumps({'type':'complete','response':{'Ok':result}}),flush=True)
"##;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hooks");
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let extension = ExecutableExtension::load(path).unwrap();
        let host = Arc::new(Host(std::sync::atomic::AtomicUsize::new(0)));
        let event = InputEvent {
            text: "input".into(),
            images: vec![],
            source: pi_rs_core::InputSource::Interactive,
            streaming_behavior: None,
        };
        assert!(matches!(
            extension
                .input_with_host(&event, None, host.clone())
                .await
                .unwrap(),
            InputAction::Continue
        ));
        let mut request = ModelRequest {
            model: "model".into(),
            messages: vec![Message::User {
                content: "hello".into(),
            }],
            tools: vec![],
            max_output_tokens: 64,
        };
        extension
            .before_request_with_host(&mut request, RequestPurpose::Turn, None, host.clone())
            .await
            .unwrap();
        extension
            .context_with_system_with_host(&mut request.messages, None, host.clone())
            .await
            .unwrap();
        let bash = pi_rs_core::UserBashEvent {
            command: "echo hi".into(),
            exclude_from_context: false,
            cwd: "/workspace".into(),
        };
        assert!(
            extension
                .user_bash_with_host(&bash, None, host.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !extension
                .user_bash_operations_with_host(&bash, None, host.clone())
                .await
                .unwrap()
        );
        assert!(
            extension
                .before_tree_with_host(
                    &TreePreparation {
                        target_id: 3,
                        old_leaf_id: 5,
                        common_ancestor_id: Some(2),
                        entries_to_summarize: Vec::new(),
                        user_wants_summary: false,
                        custom_instructions: None,
                        replace_instructions: false,
                        label: None,
                    },
                    None,
                    &CancellationToken::new(),
                    host.clone()
                )
                .await
                .unwrap()
                .is_none()
        );
        let (output, mut received) = tokio::sync::mpsc::channel(4);
        assert_eq!(
            extension
                .execute_user_bash_with_host(
                    &bash,
                    None,
                    None,
                    CancellationToken::new(),
                    output,
                    host.clone()
                )
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(received.recv().await.unwrap(), b"ok");
        let discovered = extension
            .discover_resources_with_host(
                &pi_rs_core::ResourceDiscoveryEvent {
                    cwd: "/workspace".into(),
                    reason: pi_rs_core::ResourceDiscoveryReason::Startup,
                },
                None,
                host.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            discovered.prompt_paths,
            [std::path::PathBuf::from("fixture.md")]
        );
        assert!(
            !extension
                .before_session_transition_with_host(
                    &pi_rs_core::BeforeSessionTransition::Fork {
                        entry_id: 1,
                        position: pi_rs_core::ForkPosition::At
                    },
                    None,
                    host.clone()
                )
                .await
                .unwrap()
        );
        let context = ToolContext {
            operation_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            cancellation: CancellationToken::new(),
        };
        let mut args = serde_json::json!({});
        assert_eq!(
            extension
                .before_tool_with_host("tool", &args, &context, None, host.clone())
                .await
                .unwrap(),
            Decision::Allow
        );
        assert_eq!(
            extension
                .tool_call_with_host("tool", &mut args, &context, None, host.clone())
                .await
                .unwrap(),
            Decision::Allow
        );
        let call = pi_rs_core::ToolCall {
            id: "id".into(),
            name: "tool".into(),
            arguments: "{}".into(),
        };
        assert!(
            extension
                .tool_result_update_with_host(
                    &call,
                    &Outcome::Succeeded("ok".into()),
                    &context,
                    None,
                    host.clone()
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(host.0.load(std::sync::atomic::Ordering::SeqCst), 12);
    }

    #[tokio::test]
    async fn streamed_shell_dispatch_forwards_output_and_message_host_calls() {
        struct Host(std::sync::Mutex<Vec<u8>>, std::sync::atomic::AtomicUsize);
        #[async_trait::async_trait]
        impl ToolHost for Host {
            fn tools(&self) -> Vec<ToolSpec> {
                vec![]
            }
            fn supports_progress(&self) -> bool {
                true
            }
            fn supports_custom_messages(&self) -> bool {
                true
            }
            async fn call(&self, _: &str, _: Value) -> Outcome {
                Outcome::Skipped("unused".into())
            }
            async fn report_progress(&self, value: Value) -> ExtensionResult<()> {
                self.0.lock().unwrap().extend(
                    serde_json::from_value::<Vec<u8>>(value["shellOutput"].clone()).unwrap(),
                );
                Ok(())
            }
            async fn send_message(
                &self,
                _: pi_rs_core::CustomMessage,
                _: pi_rs_core::CustomMessageDelivery,
            ) -> ExtensionResult<()> {
                self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }
        struct Shell;
        #[async_trait::async_trait]
        impl Extension for Shell {
            fn id(&self) -> &str {
                "shell"
            }
            async fn execute_user_bash_with_host(
                &self,
                _: &pi_rs_core::UserBashEvent,
                _: Option<&ExtensionState>,
                _: Option<Duration>,
                _: CancellationToken,
                output: tokio::sync::mpsc::Sender<Vec<u8>>,
                host: Arc<dyn ToolHost>,
            ) -> ExtensionResult<Option<i32>> {
                output.send(b"before".to_vec()).await.unwrap();
                host.send_message(
                    pi_rs_core::CustomMessage {
                        custom_type: "shell".into(),
                        content: pi_rs_core::ContextContent::Text("message".into()),
                        display: false,
                        details: None,
                    },
                    pi_rs_core::CustomMessageDelivery::NextTurn,
                )
                .await?;
                output.send(b"after".to_vec()).await.unwrap();
                Ok(Some(0))
            }
        }
        let host = Arc::new(Host(
            std::sync::Mutex::new(Vec::new()),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let result = dispatch_with_host(
            Arc::new(Shell),
            ProtocolRequest::ExecuteUserBash {
                event: pi_rs_core::UserBashEvent {
                    command: "echo".into(),
                    exclude_from_context: false,
                    cwd: "/".into(),
                },
                state: None,
                timeout_secs: None,
            },
            host.clone(),
        )
        .await
        .unwrap();
        assert_eq!(result, serde_json::json!(0));
        assert_eq!(&*host.0.lock().unwrap(), b"beforeafter");
        assert_eq!(host.1.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn older_host_receives_legacy_result_from_new_extension() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "hook"
            }
            async fn tool_result_patch_v2(
                &self,
                _: &pi_rs_core::ToolCall,
                _: &Outcome,
                _: &ToolContext,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<pi_rs_core::ToolResultPatchV2>> {
                Ok(Some(pi_rs_core::ToolResultPatchV2 {
                    output: Some("fixed".into()),
                    details: Some(serde_json::json!({"code": 42})),
                    is_error: Some(true),
                }))
            }
        }
        let raw = serde_json::json!({"type":"tool_result", "call":{"id":"id", "name":"tool", "arguments":"{}"}, "outcome":{"status":"failed","detail":"bad"}, "operation_id":"00000000-0000-4000-8000-000000000001", "state":null});
        let request = serde_json::from_value(raw).unwrap();
        let response = dispatch(Arc::new(Hook), request).await.unwrap();
        assert_eq!(
            serde_json::from_value::<Option<Outcome>>(response).unwrap(),
            Some(Outcome::Failed("fixed".into()))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_result_patch_chain_and_legacy_negotiation() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = r##"#!/usr/bin/env python3
import json, sys, os
r=json.load(sys.stdin)
name=os.path.basename(sys.argv[0])
if r['type']=='describe':
    value={'id':name,'dependencies':[],'commands':[],'tools':[], 'tool_result_hook':True, 'text_result_v2':name!='legacy'}
elif r['type']=='tool_result':
    o=r['outcome']
    if name=='first':
        assert o['status']=='failed' and r['text_result_v2']
        value={'status':'text_result','detail':{'output':'exact','details':{'code':42},'is_error':True}}
    elif name=='second':
        assert o['detail']=={'output':'legacy changed','details':{'code':42},'is_error':True}
        value={'status':'text_result','detail':{'output':'legacy changed','details':{'code':42},'is_error':False}}
    else:
        assert o['status']=='failed' and o['detail']=='exact'
        value={'status':'failed','detail':'legacy changed'}
else:
    raise RuntimeError(r['type'])
print(json.dumps({'version':1,'response':{'Ok':value}}))
"##;
        let mut extensions = Vec::new();
        for name in ["first", "legacy", "second"] {
            let path = root.path().join(name);
            std::fs::write(&path, script).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            extensions.push(ExecutableExtension::load(path).unwrap());
        }
        let call = pi_rs_core::ToolCall {
            id: "id".into(),
            name: "tool".into(),
            arguments: "{}".into(),
        };
        let context = ToolContext {
            operation_id: "00000000-0000-4000-8000-000000000001".parse().unwrap(),
            cancellation: CancellationToken::new(),
        };
        let mut outcome = Outcome::Failed("original".into());
        for ext in extensions {
            outcome = ext
                .tool_result_update(&call, &outcome, &context, None)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(outcome.tool_content(), "legacy changed");
        assert!(!outcome.is_error());
        assert!(
            matches!(outcome, Outcome::TextResult { details: Some(details), .. } if details["code"] == 42)
        );
    }

    #[tokio::test]
    async fn command_output_protocol_transports_custom_entries() {
        struct Entries;
        impl Extension for Entries {
            fn id(&self) -> &str {
                "entries"
            }
            fn commands(&self) -> &[&str] {
                &["bookmark"]
            }
            fn supports_command_entries(&self) -> bool {
                true
            }
            fn command_output(
                &self,
                _: &str,
                arguments: &str,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<pi_rs_core::ExtensionCommandOutput> {
                Ok(pi_rs_core::ExtensionCommandOutput {
                    state: None,
                    entries: vec![pi_rs_core::CustomEntry {
                        custom_type: "bookmark".into(),
                        data: Some(serde_json::json!({"note":arguments})),
                    }],
                })
            }
        }
        let extension: Arc<dyn Extension> = Arc::new(Entries);
        let description = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(description["command_entries"], true);
        let result = dispatch(
            extension,
            ProtocolRequest::CommandOutput {
                name: "bookmark".into(),
                arguments: "remember".into(),
                state: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(result["entries"][0]["customType"], "bookmark");
        assert_eq!(result["entries"][0]["data"]["note"], "remember");
    }

    #[tokio::test]
    async fn command_effects_protocol_preserves_custom_message_metadata() {
        struct Messages;
        impl Extension for Messages {
            fn id(&self) -> &str {
                "messages"
            }
            fn commands(&self) -> &[&str] {
                &["context"]
            }
            fn supports_command_effects(&self) -> bool {
                true
            }
            fn command_effects(
                &self,
                _: &str,
                _: &str,
                _: Option<&ExtensionState>,
            ) -> ExtensionResult<pi_rs_core::ExtensionCommandEffects> {
                Ok(pi_rs_core::ExtensionCommandEffects {
                    state: None,
                    actions: vec![pi_rs_core::ExtensionCommandAction::Message {
                        message: pi_rs_core::CustomMessage {
                            custom_type: "notice".into(),
                            content: pi_rs_core::ContextContent::Text("hello".into()),
                            display: false,
                            details: Some(serde_json::Value::Null),
                        },
                    }],
                })
            }
        }
        let extension: Arc<dyn Extension> = Arc::new(Messages);
        let description = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(description["command_effects"], true);
        let result = dispatch(
            extension,
            ProtocolRequest::CommandEffects {
                name: "context".into(),
                arguments: String::new(),
                state: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(result["actions"][0]["message"]["customType"], "notice");
        assert_eq!(result["actions"][0]["message"]["content"], "hello");
        assert_eq!(result["actions"][0]["message"]["display"], false);
        let decoded: pi_rs_core::ExtensionCommandEffects = serde_json::from_value(result).unwrap();
        assert!(
            matches!(&decoded.actions[0], pi_rs_core::ExtensionCommandAction::Message { message } if message.details == Some(serde_json::Value::Null))
        );
    }

    #[test]
    fn duplex_frames_are_bounded_and_leave_later_frames_unread() {
        let mut bytes = Vec::new();
        write_line(&mut bytes, &serde_json::json!({"first":1})).unwrap();
        write_line(&mut bytes, &serde_json::json!({"second":2})).unwrap();
        let mut reader = std::io::Cursor::new(bytes);
        assert_eq!(read_line::<Value>(&mut reader).unwrap()["first"], 1);
        assert_eq!(read_line::<Value>(&mut reader).unwrap()["second"], 2);
        assert!(read_line::<Value>(&mut reader).is_err());
        assert!(write_line(&mut Vec::new(), &"x".repeat(MAX_MESSAGE as usize)).is_err());
        let oversized = vec![b'x'; MAX_MESSAGE as usize + 1];
        assert!(
            read_line::<Value>(&mut std::io::Cursor::new(oversized))
                .unwrap_err()
                .contains("exceeds")
        );
    }

    #[test]
    fn legacy_descriptors_disable_new_argument_preparation_by_default() {
        let descriptor: DescriptionTool = serde_json::from_value(serde_json::json!({
            "spec":{"name":"old","description":"legacy","parameters":{"type":"object"}},
            "execution":"sequential"
        }))
        .unwrap();
        assert!(!descriptor.prepares_arguments);
        let descriptor: Description = serde_json::from_value(serde_json::json!({
            "id":"legacy", "dependencies":[], "commands":[], "tools":[]
        }))
        .unwrap();
        assert!(!descriptor.command_entries);
        assert!(!descriptor.command_effects);
        assert!(!descriptor.cache_warming_hook);
        assert!(!descriptor.session_lifecycle_hook);
        assert!(!descriptor.session_transition_reasons);
        assert!(!descriptor.session_info_hook);
    }

    struct CacheDecisionExtension;

    struct ResourceExtension;
    #[async_trait::async_trait]
    impl Extension for ResourceExtension {
        fn id(&self) -> &str {
            "resource-test"
        }
        fn handles_resource_discovery(&self) -> bool {
            true
        }
        async fn discover_resources(
            &self,
            event: &pi_rs_core::ResourceDiscoveryEvent,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<pi_rs_core::DiscoveredResources> {
            assert_eq!(event.reason, pi_rs_core::ResourceDiscoveryReason::Reload);
            assert_eq!(state.unwrap().data["enabled"], true);
            Ok(pi_rs_core::DiscoveredResources {
                skill_paths: vec!["skills".into()],
                prompt_paths: vec!["prompts/example.md".into()],
                theme_paths: vec!["themes".into()],
            })
        }
    }

    #[tokio::test]
    async fn resource_discovery_protocol_preserves_paths_reason_and_state() {
        let extension = Arc::new(ResourceExtension);
        let descriptor = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(descriptor["resource_discovery_hook"], true);
        let request = serde_json::from_value(serde_json::json!({
            "type":"discover_resources", "event":{"cwd":"/workspace","reason":"reload"},
            "state":{"version":1,"data":{"enabled":true}}
        }))
        .unwrap();
        let result = dispatch(extension, request).await.unwrap();
        assert_eq!(result["skillPaths"], serde_json::json!(["skills"]));
        assert_eq!(
            result["promptPaths"],
            serde_json::json!(["prompts/example.md"])
        );
        assert_eq!(result["themePaths"], serde_json::json!(["themes"]));
    }

    #[async_trait::async_trait]
    impl Extension for CacheDecisionExtension {
        fn id(&self) -> &str {
            "cache-decision-test"
        }
        fn handles_cache_warming(&self) -> bool {
            true
        }
        async fn cache_warming_decision(
            &self,
            event: &CacheWarmingDecision,
            state: Option<&ExtensionState>,
        ) -> ExtensionResult<Option<CacheWarmingAction>> {
            assert_eq!(event.action, CacheWarmingAction::Warm);
            assert_eq!(event.prompt_tokens, 50_000);
            assert_eq!(state.unwrap().data["pause"], true);
            Ok(Some(CacheWarmingAction::Stop))
        }
    }

    #[tokio::test]
    async fn cache_warming_protocol_passes_decision_and_private_state() {
        let extension = Arc::new(CacheDecisionExtension);
        let descriptor = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(descriptor["cache_warming_hook"], true);
        let request = serde_json::from_value(serde_json::json!({
            "type":"cache_warming_decision",
            "event": {
                "sessionId":"00000000-0000-0000-0000-000000000001",
                "model":{"provider":"relay","model":"test"},
                "phase":"streaming","promptTokens":50_000,"warmCost":0.05,
                "missCost":0.5,"continuationProbability":1.0,"action":"warm"
            },
            "state":{"version":1,"data":{"pause":true}}
        }))
        .unwrap();
        assert_eq!(dispatch(extension, request).await.unwrap(), "stop");
    }
    struct PollTool;
    #[async_trait::async_trait]
    impl Tool for PollTool {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "polling".into(),
                description: "test".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }
        fn can_poll(&self) -> bool {
            true
        }
        async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
            panic!("poll must not redispatch")
        }
        async fn invoke_with_state(
            &self,
            _: Value,
            _: ToolContext,
            state: Option<&ExtensionState>,
        ) -> Outcome {
            Outcome::Succeeded(state.unwrap().data["text"].as_str().unwrap().into())
        }
        async fn invoke_transaction(
            &self,
            arguments: Value,
            context: ToolContext,
            state: Option<&ExtensionState>,
            _: Arc<dyn ToolHost>,
        ) -> ToolReply {
            ToolReply {
                outcome: self.invoke_with_state(arguments, context, state).await,
                state: Some(ExtensionState {
                    version: 1,
                    data: serde_json::json!({"saved":true}),
                }),
            }
        }
        async fn poll(&self, reference: &str, _: ToolContext) -> Option<Outcome> {
            Some(Outcome::Succeeded(reference.into()))
        }
    }
    struct PollExtension;
    impl Extension for PollExtension {
        fn id(&self) -> &str {
            "poll-extension"
        }
        fn tools(&self) -> Vec<Arc<dyn Tool>> {
            vec![Arc::new(PollTool)]
        }
    }
    #[test]
    fn protocol_forwards_owning_extension_state_to_tool() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let request = ProtocolRequest::Invoke {
            name: "polling".into(),
            arguments: serde_json::json!({}),
            operation_id: "00000000-0000-0000-0000-000000000001".into(),
            state: Some(ExtensionState {
                version: 1,
                data: serde_json::json!({"text":"own state"}),
            }),
            transaction: false,
            programmatic: false,
        };
        let result = runtime
            .block_on(dispatch(Arc::new(PollExtension), request))
            .unwrap();
        assert_eq!(
            serde_json::from_value::<Outcome>(result).unwrap(),
            Outcome::Succeeded("own state".into())
        );
    }
    #[test]
    fn protocol_preserves_private_transactional_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let request = ProtocolRequest::Invoke {
            name: "polling".into(),
            arguments: serde_json::json!({}),
            operation_id: "00000000-0000-0000-0000-000000000001".into(),
            state: Some(ExtensionState {
                version: 1,
                data: serde_json::json!({"text":"result"}),
            }),
            transaction: true,
            programmatic: false,
        };
        let result = runtime
            .block_on(dispatch(Arc::new(PollExtension), request))
            .unwrap();
        let result: ToolReply = serde_json::from_value(result).unwrap();
        assert_eq!(result.outcome, Outcome::Succeeded("result".into()));
        assert_eq!(result.state.unwrap().data["saved"], true);
    }
    #[test]
    fn protocol_advertises_and_forwards_polling_without_invocation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let extension: Arc<dyn Extension> = Arc::new(PollExtension);
        let description = runtime
            .block_on(dispatch(extension.clone(), ProtocolRequest::Describe))
            .unwrap();
        assert_eq!(description["tools"][0]["can_poll"], true);
        assert_eq!(description["tools"][0]["prepares_arguments"], true);
        let request = ProtocolRequest::Poll {
            name: "polling".into(),
            reference: "job-1".into(),
            operation_id: "00000000-0000-0000-0000-000000000001".into(),
        };
        let result = runtime.block_on(dispatch(extension, request)).unwrap();
        assert_eq!(
            serde_json::from_value::<Option<Outcome>>(result).unwrap(),
            Some(Outcome::Succeeded("job-1".into()))
        );
    }
    #[test]
    fn protocol_preserves_commands_and_state_across_process_requests() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let extension: Arc<dyn Extension> = Arc::new(crate::Notes);
        let descriptor = runtime
            .block_on(dispatch(extension.clone(), ProtocolRequest::Describe))
            .unwrap();
        let descriptor: Description = serde_json::from_value(descriptor).unwrap();
        assert_eq!(descriptor.commands, ["set", "append", "clear"]);
        let state = runtime
            .block_on(dispatch(
                extension.clone(),
                ProtocolRequest::Command {
                    name: "set".into(),
                    arguments: "first".into(),
                    state: None,
                },
            ))
            .unwrap();
        let state = runtime
            .block_on(dispatch(
                extension.clone(),
                ProtocolRequest::Command {
                    name: "append".into(),
                    arguments: "second".into(),
                    state: Some(serde_json::from_value(state).unwrap()),
                },
            ))
            .unwrap();
        assert_eq!(state["data"]["text"], "first\nsecond");
        assert!(
            runtime
                .block_on(dispatch(
                    extension,
                    ProtocolRequest::Command {
                        name: "unknown".into(),
                        arguments: String::new(),
                        state: None,
                    }
                ))
                .is_err()
        );
    }

    #[test]
    fn protocol_context_with_system_passes_messages_and_state() {
        struct Hook;
        #[async_trait::async_trait]
        impl Extension for Hook {
            fn id(&self) -> &str {
                "hook"
            }
            fn handles_context_with_system(&self) -> bool {
                true
            }
            async fn context_with_system(
                &self,
                messages: &mut Vec<Message>,
                state: Option<&ExtensionState>,
            ) -> ExtensionResult<()> {
                assert_eq!(state.unwrap().data["value"], "saved");
                assert!(matches!(messages.first(), Some(Message::System { .. })));
                messages.push(Message::User {
                    content: "added".into(),
                });
                Ok(())
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let extension: Arc<dyn Extension> = Arc::new(Hook);
        let descriptor: Description = serde_json::from_value(
            runtime
                .block_on(dispatch(extension.clone(), ProtocolRequest::Describe))
                .unwrap(),
        )
        .unwrap();
        assert!(descriptor.context_with_system_hook);
        let result = runtime
            .block_on(dispatch(
                extension,
                ProtocolRequest::ContextWithSystem {
                    messages: vec![Message::System {
                        content: "prompt".into(),
                    }],
                    state: Some(ExtensionState {
                        version: 1,
                        data: serde_json::json!({"value":"saved"}),
                    }),
                },
            ))
            .unwrap();
        let messages: Vec<Message> = serde_json::from_value(result).unwrap();
        assert!(matches!(&messages[1], Message::User { content } if content == "added"));
    }

    #[test]
    fn protocol_forwards_the_async_request_hook_with_previous_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let extension: Arc<dyn Extension> = Arc::new(crate::Notes);
        let result = runtime
            .block_on(dispatch(
                extension,
                ProtocolRequest::BeforeRequest {
                    request: ModelRequest {
                        model: "fixture".into(),
                        messages: Vec::new(),
                        tools: Vec::new(),
                        max_output_tokens: 32,
                    },
                    summary: false,
                    state: Some(ExtensionState {
                        version: 1,
                        data: serde_json::json!({"text":"remember"}),
                    }),
                },
            ))
            .unwrap();
        let request: ModelRequest = serde_json::from_value(result).unwrap();
        assert!(
            matches!(&request.messages[0], pi_rs_core::Message::User { content } if content.contains("remember"))
        );
    }

    #[tokio::test]
    async fn protocol_forwards_user_bash_and_describes_opt_in() {
        struct ShellHook;
        #[async_trait::async_trait]
        impl Extension for ShellHook {
            fn id(&self) -> &str {
                "shell-hook"
            }
            fn handles_user_bash(&self) -> bool {
                true
            }
            async fn user_bash(
                &self,
                event: &pi_rs_core::UserBashEvent,
                state: Option<&ExtensionState>,
            ) -> ExtensionResult<Option<pi_rs_core::UserBashResult>> {
                assert_eq!(event.command, "echo test");
                assert!(event.exclude_from_context);
                assert_eq!(state.unwrap().data["value"], 42);
                Ok(Some(pi_rs_core::UserBashResult {
                    output: "intercepted".into(),
                    exit_code: Some(0),
                    ..Default::default()
                }))
            }
        }
        let extension: Arc<dyn Extension> = Arc::new(ShellHook);
        let description = dispatch(extension.clone(), ProtocolRequest::Describe)
            .await
            .unwrap();
        assert_eq!(description["user_bash_hook"], true);
        let result = dispatch(
            extension,
            ProtocolRequest::UserBash {
                event: pi_rs_core::UserBashEvent {
                    command: "echo test".into(),
                    exclude_from_context: true,
                    cwd: ".".into(),
                },
                state: Some(ExtensionState {
                    version: 1,
                    data: serde_json::json!({"value":42}),
                }),
            },
        )
        .await
        .unwrap();
        assert_eq!(result["output"], "intercepted");
        assert_eq!(result["exitCode"], 0);
    }
}
