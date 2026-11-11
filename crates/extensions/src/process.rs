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

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ProtocolRequest {
    Describe,
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
    Lifecycle {
        event: LifecycleEvent,
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
    BeforeRequest {
        request: ModelRequest,
        summary: bool,
        state: Option<ExtensionState>,
    },
    Command {
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
    input_hook: bool,
    #[serde(default)]
    lifecycle_hook: bool,
    #[serde(default)]
    cache_warming_hook: bool,
    #[serde(default)]
    tree_navigation_hook: bool,
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
        };
        let description =
            call(&endpoint, ProtocolRequest::Describe, None).map_err(Error::Config)?;
        let mut extension = Self {
            endpoint,
            description,
        };
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
    fn handles_lifecycle(&self) -> bool {
        self.description.lifecycle_hook
    }
    async fn lifecycle(
        &self,
        event: &LifecycleEvent,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        self.request_async(ProtocolRequest::Lifecycle {
            event: event.clone(),
            state: state.cloned(),
        })
        .await
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
        *request = updated;
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
    fn after_tool(
        &self,
        name: &str,
        outcome: &Outcome,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<Option<ExtensionState>> {
        self.request(ProtocolRequest::AfterTool {
            name: name.into(),
            outcome: outcome.clone(),
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
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum DuplexFrame {
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

fn call_duplex(
    endpoint: &Endpoint,
    request: ProtocolRequest,
    host: Arc<dyn ToolHost>,
    cancellation: &CancellationToken,
    runtime: &tokio::runtime::Handle,
) -> ExtensionResult<ToolReply> {
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
            let terminal = !matches!(frame, Ok(DuplexFrame::Call { .. } | DuplexFrame::Ui { .. }));
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
    io: Arc<std::sync::Mutex<(std::io::BufReader<std::io::Stdin>, std::io::Stdout)>>,
}
#[async_trait::async_trait]
impl ToolHost for RemoteHost {
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

fn call<T: serde::de::DeserializeOwned>(
    endpoint: &Endpoint,
    request: ProtocolRequest,
    cancel: Option<&CancellationToken>,
) -> ExtensionResult<T> {
    let bytes = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_MESSAGE {
        return Err("extension request exceeds 4 MiB".into());
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
    if std::env::args().any(|argument| argument == "--pi-rs-extension-v2") {
        return serve_duplex(extension);
    }
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_MESSAGE + 1)
        .read_to_end(&mut input)?;
    let response = if input.len() as u64 > MAX_MESSAGE {
        Err("extension request exceeds 4 MiB".into())
    } else {
        match serde_json::from_slice(&input) {
            Ok(request) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(dispatch(extension, request))
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
            input_hook: extension.handles_input(),
            lifecycle_hook: extension.handles_lifecycle(),
            cache_warming_hook: extension.handles_cache_warming(),
            tree_navigation_hook: extension.handles_tree_navigation(),
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
            encode(extension.input(&event, state.as_ref()).await)
        }
        ProtocolRequest::Lifecycle { event, state } => {
            encode(extension.lifecycle(&event, state.as_ref()).await)
        }
        ProtocolRequest::CacheWarmingDecision { event, state } => encode(
            extension
                .cache_warming_decision(&event, state.as_ref())
                .await,
        ),
        ProtocolRequest::BeforeTree { preparation, state } => encode(
            extension
                .before_tree(&preparation, state.as_ref(), &CancellationToken::new())
                .await,
        ),
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
            extension
                .before_request(&mut request, purpose, state.as_ref())
                .await?;
            encode(Ok(request))
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
        ProtocolRequest::BeforeTool {
            name,
            arguments,
            operation_id,
            state,
        } => {
            let context = context(&operation_id)?;
            encode(
                extension
                    .before_tool(&name, &arguments, &context, state.as_ref())
                    .await
                    .map(DecisionValue::from),
            )
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
        assert!(!descriptor.cache_warming_hook);
    }

    struct CacheDecisionExtension;

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
}
