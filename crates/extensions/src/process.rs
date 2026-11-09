//! Versioned process boundary for user-authored Rust extensions.
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pi_rs_core::{
    CancellationToken, Compaction, Decision, Error, Extension, ExtensionResult, ExtensionState,
    ModelRequest, Outcome, RequestPurpose, Result, Tool, ToolContext, ToolExecution, ToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_MESSAGE: u64 = 4 * 1024 * 1024;
const PROTOCOL: u16 = 1;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ProtocolRequest {
    Describe,
    Migrate {
        state: Option<ExtensionState>,
    },
    Context {
        request: ModelRequest,
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
    tools: Vec<DescriptionTool>,
}
#[derive(Serialize, Deserialize)]
struct DescriptionTool {
    spec: ToolSpec,
    execution: ToolExecution,
    #[serde(default)]
    can_poll: bool,
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
    executable: PathBuf,
    description: Description,
}
impl ExecutableExtension {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let executable = path.as_ref().canonicalize()?;
        if !executable.is_file() {
            return Err(Error::Config("extension executable must be a file".into()));
        }
        let description =
            call(&executable, ProtocolRequest::Describe, None).map_err(Error::Config)?;
        Ok(Self {
            executable,
            description,
        })
    }
    fn request<T: serde::de::DeserializeOwned>(
        &self,
        request: ProtocolRequest,
    ) -> ExtensionResult<T> {
        call(&self.executable, request, None)
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
                    executable: self.executable.clone(),
                    spec: tool.spec.clone(),
                    execution: tool.execution,
                    can_poll: tool.can_poll,
                }) as Arc<dyn Tool>
            })
            .collect()
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
        *request = self.request(ProtocolRequest::Context {
            request: request.clone(),
            state: state.cloned(),
        })?;
        Ok(())
    }
    async fn before_request(
        &self,
        request: &mut ModelRequest,
        purpose: RequestPurpose,
        state: Option<&ExtensionState>,
    ) -> ExtensionResult<()> {
        let message = ProtocolRequest::BeforeRequest {
            request: request.clone(),
            summary: purpose == RequestPurpose::Summary,
            state: state.cloned(),
        };
        let path = self.executable.clone();
        let guard = CancelOnDrop(CancellationToken::new());
        let cancelled = guard.0.clone();
        *request = tokio::task::spawn_blocking(move || call(&path, message, Some(&cancelled)))
            .await
            .map_err(|error| error.to_string())??;
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
        let path = self.executable.clone();
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
    executable: PathBuf,
    spec: ToolSpec,
    execution: ToolExecution,
    can_poll: bool,
}
#[async_trait::async_trait]
impl Tool for ExecutableTool {
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
        };
        let path = self.executable.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        let result =
            tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled))).await;
        match result {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => Outcome::Unknown(error),
            Err(error) => Outcome::Unknown(error.to_string()),
        }
    }
    async fn poll(&self, reference: &str, context: ToolContext) -> Option<Outcome> {
        let request = ProtocolRequest::Poll {
            name: self.spec.name.clone(),
            reference: reference.into(),
            operation_id: context.operation_id.to_string(),
        };
        let path = self.executable.clone();
        let guard = CancelOnDrop(context.cancellation.child_token());
        let cancelled = guard.0.clone();
        tokio::task::spawn_blocking(move || call(&path, request, Some(&cancelled)))
            .await
            .ok()
            .and_then(std::result::Result::ok)
            .flatten()
    }
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

fn call<T: serde::de::DeserializeOwned>(
    path: &Path,
    request: ProtocolRequest,
    cancel: Option<&CancellationToken>,
) -> ExtensionResult<T> {
    let bytes = serde_json::to_vec(&request).map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_MESSAGE {
        return Err("extension request exceeds 4 MiB".into());
    }
    let mut command = Command::new(path);
    command
        .arg("--pi-rs-extension-v1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command.spawn().map_err(|error| error.to_string())?;
    let mut child = ChildGuard {
        process: child,
        finished: false,
    };
    let mut input = child.process.stdin.take().expect("piped stdin");
    let output = child.process.stdout.take().expect("piped stdout");
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
struct ChildGuard {
    process: std::process::Child,
    finished: bool,
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
            tools: extension
                .tools()
                .into_iter()
                .map(|tool| DescriptionTool {
                    spec: tool.spec(),
                    execution: tool.execution(),
                    can_poll: tool.can_poll(),
                })
                .collect(),
        })),
        ProtocolRequest::Migrate { state } => encode(extension.migrate(state.as_ref())),
        ProtocolRequest::Context { mut request, state } => {
            extension.context(&mut request, state.as_ref())?;
            encode(Ok(request))
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
        } => {
            let tool = extension
                .tools()
                .into_iter()
                .find(|tool| tool.spec().name == name)
                .ok_or_else(|| "unknown extension tool".to_owned())?;
            encode(Ok(tool
                .invoke_with_state(arguments, context(&operation_id)?, state.as_ref())
                .await))
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
