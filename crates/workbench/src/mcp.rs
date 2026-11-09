//! Native MCP client connections exposed through the shared tool contract.
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pi_rs::{Error, Extension, Outcome, Result, Tool, ToolContext, ToolSpec};
use rmcp::{
    RoleClient, ServiceExt,
    model::CallToolResponse,
    service::RunningService,
    transport::{
        StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::StreamableHttpClientTransportConfig,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(rename = "type")]
    pub transport: Option<String>,
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub url: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub timeout: Option<u64>,
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub exposure: Option<String>,
    #[serde(default)]
    pub tool_exposure: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct NativeClient;
impl rmcp::ClientHandler for NativeClient {
    fn get_info(&self) -> rmcp::model::ClientConfig {
        let mut info = rmcp::model::ClientConfig::default();
        info.capabilities.extensions = Some(
            [(
                rmcp::model::TASKS_EXTENSION_ID.to_owned(),
                Default::default(),
            )]
            .into(),
        );
        info
    }
}
#[derive(Serialize, Deserialize)]
struct TaskReference {
    connection: String,
    task_id: String,
}
enum CallResult {
    Data(Value),
    Task(String),
}
struct Connection {
    identity: String,
    service: tokio::sync::Mutex<RunningService<RoleClient, NativeClient>>,
    timeout: Duration,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.service.get_mut().cancellation_token().cancel();
    }
}

pub struct McpExtension {
    id: String,
    tools: Vec<Arc<dyn Tool>>,
    pub server_name: String,
    pub description: String,
    connection: Arc<Connection>,
}
impl McpExtension {
    pub async fn connect(name: &str, config: &ServerConfig, workspace: &Path) -> Result<Self> {
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        {
            return Err(Error::Config(
                "MCP server names use letters, digits, underscore or hyphen".into(),
            ));
        }
        let timeout = Duration::from_secs(config.timeout.unwrap_or(60).clamp(1, 3600));
        let mut identity = connection_identity(name, config, workspace)?;
        let service = if let Some(command) = &config.command {
            if config.url.is_some()
                || config
                    .transport
                    .as_deref()
                    .is_some_and(|kind| kind != "stdio")
            {
                return Err(Error::Config(
                    "MCP command requires the stdio transport".into(),
                ));
            }
            let mut process = tokio::process::Command::new(expand_home(command));
            for argument in &config.args {
                process.arg(expand_home(argument));
            }
            for (key, value) in std::env::vars_os().collect::<BTreeMap<_, _>>() {
                identity.update(key.as_encoded_bytes());
                identity.update([0]);
                identity.update(value.as_encoded_bytes());
                identity.update([0]);
            }
            for (key, value) in &config.env {
                let value = resolve_value(value).await?;
                identity.update(json!([key, value]).to_string());
                process.env(key, value);
            }
            process.current_dir(
                config
                    .cwd
                    .as_ref()
                    .map(|path| workspace.join(expand_home(&path.to_string_lossy())))
                    .unwrap_or_else(|| workspace.into()),
            );
            process.kill_on_drop(true);
            let wrapped = wrap_process(process);
            let transport = TokioChildProcess::new(wrapped)?;
            tokio::time::timeout(
                timeout.min(Duration::from_secs(10)),
                NativeClient.serve(transport),
            )
            .await
            .map_err(|_| Error::Config(format!("MCP {name} initialization timed out")))?
            .map_err(|error| Error::Config(format!("MCP {name} initialization failed: {error}")))?
        } else if let Some(url) = &config.url {
            if config
                .transport
                .as_deref()
                .is_some_and(|kind| !matches!(kind, "http" | "streamable-http"))
            {
                return Err(Error::Config("MCP URL requires streamable HTTP".into()));
            }
            let parsed = reqwest_mcp::Url::parse(url)
                .map_err(|_| Error::Config("invalid MCP URL".into()))?;
            let loopback = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
            if !(parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback))
                || !parsed.username().is_empty()
                || parsed.password().is_some()
            {
                return Err(Error::Config(
                    "MCP URL requires HTTPS or loopback HTTP and no embedded credentials".into(),
                ));
            }
            let mut headers = HashMap::new();
            for (name, value) in &config.headers {
                let name: reqwest_mcp::header::HeaderName = name
                    .parse()
                    .map_err(|_| Error::Config("invalid MCP header name".into()))?;
                let resolved = resolve_value(value).await?;
                identity.update(json!([name.as_str(), resolved]).to_string());
                let value = resolved
                    .parse()
                    .map_err(|_| Error::Config("invalid MCP header value".into()))?;
                headers.insert(name, value);
            }
            let client = reqwest_mcp::Client::builder()
                .redirect(reqwest_mcp::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .build()
                .map_err(|error| Error::Config(error.to_string()))?;
            let transport = StreamableHttpClientTransport::with_client(
                client,
                StreamableHttpClientTransportConfig::with_uri(url.clone()).custom_headers(headers),
            );
            tokio::time::timeout(
                timeout.min(Duration::from_secs(10)),
                NativeClient.serve(transport),
            )
            .await
            .map_err(|_| Error::Config(format!("MCP {name} initialization timed out")))?
            .map_err(|error| Error::Config(format!("MCP {name} initialization failed: {error}")))?
        } else {
            return Err(Error::Config(format!("MCP {name} needs command or url")));
        };
        let info = service.peer_info();
        let connection = Arc::new(Connection {
            identity: format!("{:x}", identity.finalize()),
            service: tokio::sync::Mutex::new(service),
            timeout,
        });
        let definitions = if info
            .as_ref()
            .is_some_and(|info| info.capabilities.tools.is_some())
        {
            tokio::time::timeout(timeout, connection.service.lock().await.list_all_tools())
                .await
                .map_err(|_| Error::Config(format!("MCP {name} tool discovery timed out")))?
                .map_err(|error| {
                    Error::Config(format!("MCP {name} tool discovery failed: {error}"))
                })?
        } else {
            Vec::new()
        };
        let prefix = format!("mcp__{}__", normalize(name));
        let mut counts = BTreeMap::<String, usize>::new();
        for definition in &definitions {
            *counts.entry(normalize(&definition.name)).or_default() += 1;
        }
        let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
        for definition in definitions {
            if !tool_enabled(config, &definition.name) {
                continue;
            }
            let normalized = normalize(&definition.name);
            let suffix = if counts[&normalized] > 1 {
                format!("_{:x}", Sha256::digest(definition.name.as_bytes()))[..9].to_owned()
            } else {
                String::new()
            };
            let spec = ToolSpec {
                name: format!("{prefix}{normalized}{suffix}"),
                description: definition
                    .description
                    .map(|description| description.into_owned())
                    .unwrap_or_else(|| definition.name.to_string()),
                parameters: Value::Object((*definition.input_schema).clone()),
            };
            tools.push(Arc::new(McpTool {
                connection: connection.clone(),
                remote_name: definition.name.into_owned(),
                spec,
                resource: false,
            }));
        }
        if info
            .clone()
            .is_some_and(|info| info.capabilities.resources.is_some())
        {
            for (name, description, parameters) in [
                (
                    "list_resources",
                    "List this MCP server's available resources",
                    json!({"type":"object","properties":{},"additionalProperties":false}),
                ),
                (
                    "list_resource_templates",
                    "List parameterized resource URI templates from this MCP server",
                    json!({"type":"object","properties":{},"additionalProperties":false}),
                ),
                (
                    "read_resource",
                    "Read a resource URI from this MCP server",
                    json!({"type":"object","properties":{"uri":{"type":"string"}},"required":["uri"],"additionalProperties":false}),
                ),
            ] {
                if !tool_enabled(config, name) {
                    continue;
                }
                let mut exposed = format!("{prefix}{name}");
                while tools.iter().any(|tool| tool.spec().name == exposed) {
                    exposed.push_str("_resource");
                }
                tools.push(Arc::new(McpTool {
                    connection: connection.clone(),
                    remote_name: name.into(),
                    spec: ToolSpec {
                        name: exposed,
                        description: description.into(),
                        parameters,
                    },
                    resource: true,
                }));
            }
        }
        if info
            .as_ref()
            .is_some_and(|info| info.capabilities.prompts.is_some())
        {
            for (name, description, parameters) in [
                (
                    "list_prompts",
                    "List prompts provided by this MCP server",
                    json!({"type":"object","properties":{},"additionalProperties":false}),
                ),
                (
                    "get_prompt",
                    "Expand a named MCP prompt with string arguments",
                    json!({"type":"object","properties":{"name":{"type":"string"},"arguments":{"type":"object","additionalProperties":{"type":"string"}}},"required":["name"],"additionalProperties":false}),
                ),
            ] {
                if !tool_enabled(config, name) {
                    continue;
                }
                let mut exposed = format!("{prefix}{name}");
                while tools.iter().any(|tool| tool.spec().name == exposed) {
                    exposed.push_str("_prompt");
                }
                tools.push(Arc::new(McpTool {
                    connection: connection.clone(),
                    remote_name: name.into(),
                    spec: ToolSpec {
                        name: exposed,
                        description: description.into(),
                        parameters,
                    },
                    resource: true,
                }));
            }
        }
        let description = config
            .description
            .clone()
            .or_else(|| info.clone().and_then(|info| info.instructions.clone()))
            .unwrap_or_default();
        Ok(Self {
            id: format!("mcp-{}", normalize(name)),
            tools,
            server_name: name.into(),
            description,
            connection,
        })
    }
    pub async fn close(&self) {
        let _ = self
            .connection
            .service
            .lock()
            .await
            .close_with_timeout(Duration::from_secs(5))
            .await;
    }
    pub fn tool_count(&self) -> usize {
        self.tools.len()
    }
}
#[async_trait::async_trait]
impl Extension for McpExtension {
    fn id(&self) -> &str {
        &self.id
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }
    async fn shutdown(&self) -> pi_rs::ExtensionResult<()> {
        self.close().await;
        Ok(())
    }
    fn context(
        &self,
        request: &mut pi_rs::ModelRequest,
        _: Option<&pi_rs::ExtensionState>,
    ) -> pi_rs::ExtensionResult<()> {
        if !self.description.is_empty()
            && let Some(pi_rs::Message::System { content }) = request.messages.first_mut()
        {
            content.push_str(&format!(
                "\n\nMCP server {}: {}",
                self.server_name, self.description
            ));
        }
        Ok(())
    }
}
struct McpTool {
    connection: Arc<Connection>,
    remote_name: String,
    spec: ToolSpec,
    resource: bool,
}
impl McpTool {
    async fn invoke_resource(&self, arguments: Value) -> Result<Value> {
        let service = self.connection.service.lock().await;
        match self.remote_name.as_str() {
            "list_resources" => {
                let resources = service
                    .list_all_resources()
                    .await
                    .map_err(|error| Error::Config(error.to_string()))?;
                Ok(json!({"resources":resources}))
            }
            "list_resource_templates" => {
                let templates = service
                    .list_all_resource_templates()
                    .await
                    .map_err(|error| Error::Config(error.to_string()))?;
                Ok(json!({"resourceTemplates":templates}))
            }
            "list_prompts" => {
                let prompts = service
                    .list_all_prompts()
                    .await
                    .map_err(|error| Error::Config(error.to_string()))?;
                Ok(json!({"prompts":prompts}))
            }
            "get_prompt" => {
                let prompt = service
                    .get_prompt(serde_json::from_value(arguments)?)
                    .await
                    .map_err(|error| Error::Config(error.to_string()))?;
                Ok(serde_json::to_value(prompt)?)
            }
            "read_resource" => {
                let result = service
                    .read_resource_once(serde_json::from_value(json!({"uri":arguments["uri"]}))?)
                    .await
                    .map_err(|error| Error::Config(error.to_string()))?;
                match result {
                    rmcp::model::ReadResourceResponse::Complete(result) => {
                        Ok(serde_json::to_value(result)?)
                    }
                    _ => Err(Error::Config(
                        "MCP resource requires additional client input".into(),
                    )),
                }
            }
            _ => Err(Error::Config("unknown MCP resource operation".into())),
        }
    }
}
fn connection_identity(name: &str, config: &ServerConfig, workspace: &Path) -> Result<Sha256> {
    let workspace = workspace.canonicalize()?;
    let mut identity = Sha256::new();
    identity.update(json!({"name":name,"command":config.command,"args":config.args,"url":config.url,"cwd":config.cwd,"workspace":workspace}).to_string());
    Ok(identity)
}

fn tool_enabled(config: &ServerConfig, name: &str) -> bool {
    !config
        .tool_exposure
        .get(name)
        .or(config.exposure.as_ref())
        .is_some_and(|value| value == "hidden" || value == "off")
}

#[async_trait::async_trait]
impl Tool for McpTool {
    fn can_poll(&self) -> bool {
        !self.resource
    }
    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }
    async fn invoke(&self, arguments: Value, context: ToolContext) -> Outcome {
        let call = async {
            if self.resource {
                self.invoke_resource(arguments).await.map(CallResult::Data)
            } else {
                let params = serde_json::from_value(
                    json!({"name":self.remote_name,"arguments":arguments,"_meta":{"operationId":context.operation_id.to_string()}}),
                )?;
                let result = self
                    .connection
                    .service
                    .lock()
                    .await
                    .call_tool_once(params)
                    .await
                    .map_err(|error| Error::Config(error.to_string()))?;
                match result {
                    CallToolResponse::Complete(result) => {
                        Ok(CallResult::Data(serde_json::to_value(result)?))
                    }
                    CallToolResponse::Task(task) => {
                        Ok(CallResult::Task(serde_json::to_string(&TaskReference {
                            connection: self.connection.identity.clone(),
                            task_id: task.task.task_id,
                        })?))
                    }
                    _ => Err(Error::Config(
                        "MCP tool requires additional client input".into(),
                    )),
                }
            }
        };
        tokio::select! {
            _ = context.cancellation.cancelled() => Outcome::Unknown("MCP request cancelled after dispatch".into()),
            result = tokio::time::timeout(self.connection.timeout, call) => match result {
                Ok(Ok(CallResult::Data(data))) if data["isError"] == true => Outcome::Failed(data.to_string()),
                Ok(Ok(CallResult::Data(data))) => Outcome::Structured { data, attachments: vec![] },
                Ok(Ok(CallResult::Task(reference))) => Outcome::Pending { reference },
                Ok(Err(error)) => Outcome::Unknown(error.to_string()),
                Err(_) => Outcome::Unknown("MCP request timed out after dispatch".into()),
            }
        }
    }
    async fn poll(&self, reference: &str, context: ToolContext) -> Option<Outcome> {
        let task: TaskReference = serde_json::from_str(reference).ok()?;
        if task.connection != self.connection.identity {
            return None;
        }
        loop {
            let params = serde_json::from_value(json!({"taskId":task.task_id})).ok()?;
            let response = self.connection.service.lock().await.get_task(params).await;
            let Ok(response) = response else {
                return Some(Outcome::Pending {
                    reference: reference.into(),
                });
            };
            match response.task.payload {
                rmcp::model::TaskPayload::Completed { result } => {
                    let data = Value::Object(result);
                    return Some(if data["isError"] == true {
                        Outcome::Failed(data.to_string())
                    } else {
                        Outcome::Structured {
                            data,
                            attachments: vec![],
                        }
                    });
                }
                rmcp::model::TaskPayload::Failed { error } => {
                    return Some(Outcome::Failed(Value::Object(error).to_string()));
                }
                rmcp::model::TaskPayload::Cancelled => {
                    return Some(Outcome::Failed("Remote MCP task was cancelled".into()));
                }
                rmcp::model::TaskPayload::Working => {}
                _ => {
                    return Some(Outcome::Pending {
                        reference: reference.into(),
                    });
                }
            }
            let interval = Duration::from_millis(
                response
                    .task
                    .task
                    .poll_interval_ms
                    .unwrap_or(1000)
                    .clamp(100, 10_000),
            );
            tokio::select! {
                _ = context.cancellation.cancelled() => return Some(Outcome::Pending { reference: reference.into() }),
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }
}
fn wrap_process(process: tokio::process::Command) -> process_wrap::tokio::CommandWrap {
    let mut wrapped = process_wrap::tokio::CommandWrap::from(process);
    #[cfg(unix)]
    wrapped.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    wrapped.wrap(process_wrap::tokio::JobObject);
    wrapped.wrap(process_wrap::tokio::KillOnDrop);
    wrapped
}

fn normalize(name: &str) -> String {
    name.chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}
fn expand_home(value: &str) -> String {
    value
        .strip_prefix("~/")
        .and_then(|path| {
            std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(|home| {
                    PathBuf::from(home)
                        .join(path)
                        .to_string_lossy()
                        .into_owned()
                })
        })
        .unwrap_or_else(|| value.into())
}
pub async fn resolve_value(value: &str) -> Result<String> {
    if let Some(command) = value.strip_prefix('!') {
        let mut process = if cfg!(windows) {
            let mut command = tokio::process::Command::new("cmd");
            command.arg("/C");
            command
        } else {
            let mut command = tokio::process::Command::new("sh");
            command.arg("-c");
            command
        };
        use tokio::io::AsyncReadExt;
        process
            .arg(command)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = wrap_process(process).spawn()?;
        let operation = async {
            let mut output = Vec::new();
            child
                .stdout()
                .take()
                .expect("piped credential output")
                .take(64 * 1024 + 1)
                .read_to_end(&mut output)
                .await?;
            if output.len() > 64 * 1024 {
                Box::into_pin(child.kill()).await?;
                return Err(Error::Config(
                    "credential command output exceeds limit".into(),
                ));
            }
            if !child.wait().await?.success() {
                return Err(Error::Config("credential command failed".into()));
            }
            String::from_utf8(output)
                .map(|value| value.trim().into())
                .map_err(|_| Error::Config("credential command returned non-UTF-8".into()))
        };
        return tokio::time::timeout(Duration::from_secs(10), operation)
            .await
            .map_err(|_| Error::Config("credential command timed out".into()))?;
    }

    let expression =
        regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("environment interpolation");
    let mut failure = None;
    let result = expression
        .replace_all(
            value,
            |captures: &regex::Captures<'_>| match std::env::var(&captures[1]) {
                Ok(value) => value,
                Err(_) => {
                    failure = Some(Error::Config(format!(
                        "missing environment variable {}",
                        &captures[1]
                    )));
                    String::new()
                }
            },
        )
        .into_owned();
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn task_identity_uses_canonical_workspace_across_resume() {
        let config: ServerConfig =
            serde_json::from_value(json!({"url":"http://127.0.0.1:8000/mcp"})).unwrap();
        let relative = connection_identity("jobs", &config, Path::new("."))
            .unwrap()
            .finalize();
        let absolute = connection_identity("jobs", &config, &std::env::current_dir().unwrap())
            .unwrap()
            .finalize();
        assert_eq!(relative, absolute);
    }
    #[test]
    fn resource_and_prompt_exposure_respects_server_and_tool_overrides() {
        let config: ServerConfig = serde_json::from_value(
            json!({"command":"server","exposure":"hidden","toolExposure":{"get_prompt":"direct"}}),
        )
        .unwrap();
        assert!(!tool_enabled(&config, "list_resources"));
        assert!(!tool_enabled(&config, "list_prompts"));
        assert!(tool_enabled(&config, "get_prompt"));
    }
    #[test]
    fn normalizes_mcp_names_and_preserves_configuration_defaults() {
        assert_eq!(normalize("docs-search/read.file"), "docs_search_read_file");
        let config: ServerConfig =
            serde_json::from_value(json!({"command":"server","args":["--stdio"]})).unwrap();
        assert_eq!(config.command.as_deref(), Some("server"));
        assert_eq!(config.enabled, None);
        assert!(config.tool_exposure.is_empty());
    }
    #[tokio::test]
    async fn resolves_environment_and_bounded_credential_commands() {
        assert!(!resolve_value("${PATH}").await.unwrap().is_empty());
        assert!(
            resolve_value("${PI_RS_MISSING_FIXTURE_ENVIRONMENT}")
                .await
                .is_err()
        );
        #[cfg(unix)]
        assert_eq!(resolve_value("!printf fixture").await.unwrap(), "fixture");
    }
}
