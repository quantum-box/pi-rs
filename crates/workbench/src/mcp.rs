//! Native MCP client connections exposed through the shared tool contract.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use pi_rs::{Error, Extension, Outcome, Result, Tool, ToolContext, ToolSpec};
use rmcp::{
    RoleClient,
    model::{CallToolResponse, ProtocolVersion, SubscriptionFilter},
    service::{
        ClientLifecycleMode, ClientServiceExt, NotificationContext, RunningService, ServiceError,
    },
    transport::{
        StreamableHttpClientTransport, TokioChildProcess,
        streamable_http_client::{InsufficientScopeError, StreamableHttpClientTransportConfig},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
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
    pub protocol_version: Option<String>,
    pub description: Option<String>,
    pub enabled: Option<bool>,
    pub exposure: Option<String>,
    pub oauth: Option<bool>,
    #[serde(default)]
    pub tool_exposure: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct NativeClient {
    interaction: Arc<std::sync::Mutex<Option<Interaction>>>,
    catalog: Arc<CatalogSignal>,
}

#[derive(Default)]
struct CatalogSignal {
    revision: AtomicU64,
    listening: AtomicBool,
}

impl CatalogSignal {
    fn changed(&self) {
        self.revision.fetch_add(1, Ordering::SeqCst);
    }
}

struct CatalogSnapshot {
    signal: Arc<CatalogSignal>,
    revision: u64,
    checked_at: Instant,
}

const CATALOG_FALLBACK: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct Interaction {
    host: Arc<dyn pi_rs::ToolHost>,
    cancellation: pi_rs::CancellationToken,
}
struct InteractionGuard(Arc<std::sync::Mutex<Option<Interaction>>>);
impl Drop for InteractionGuard {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = None;
    }
}
impl rmcp::ClientHandler for NativeClient {
    fn get_info(&self) -> rmcp::model::ClientConfig {
        let mut info = rmcp::model::ClientConfig::default();
        info.capabilities.elicitation = Some(
            rmcp::model::ElicitationCapability::new()
                .with_form(rmcp::model::FormElicitationCapability::default())
                .with_url(rmcp::model::UrlElicitationCapability::default()),
        );
        info.capabilities.extensions = Some(
            [(
                rmcp::model::TASKS_EXTENSION_ID.to_owned(),
                Default::default(),
            )]
            .into(),
        );
        info
    }

    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.catalog.changed();
    }

    async fn create_elicitation(
        &self,
        request: rmcp::model::ElicitRequestParams,
        context: rmcp::service::RequestContext<RoleClient>,
    ) -> std::result::Result<rmcp::model::ElicitResult, rmcp::model::ErrorData> {
        use rmcp::model::{ElicitRequestParams, ElicitResult, ElicitationAction};
        let interaction = self
            .interaction
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(interaction) = interaction.filter(|interaction| interaction.host.has_ui()) else {
            return Ok(ElicitResult::new(ElicitationAction::Decline));
        };
        let (message, requested_schema) = match request {
            ElicitRequestParams::FormElicitationParams {
                message,
                requested_schema,
                ..
            } => (message, requested_schema),
            ElicitRequestParams::UrlElicitationParams { message, url, .. } => {
                return tokio::select! {
                    biased;
                    _ = interaction.cancellation.cancelled() => Ok(ElicitResult::new(ElicitationAction::Cancel)),
                    _ = context.ct.cancelled() => Ok(ElicitResult::new(ElicitationAction::Cancel)),
                    result = elicit_url(&interaction, message, url) => result,
                };
            }
            _ => return Ok(ElicitResult::new(ElicitationAction::Decline)),
        };
        let schema = serde_json::to_value(requested_schema).map_err(|_| {
            rmcp::model::ErrorData::invalid_params("invalid elicitation schema", None)
        })?;
        let validator = jsonschema::validator_for(&schema).map_err(|_| {
            rmcp::model::ErrorData::invalid_params("invalid elicitation schema", None)
        })?;
        let defaults: serde_json::Map<String, Value> = schema["properties"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(name, property)| {
                property
                    .get("default")
                    .map(|value| (name.clone(), value.clone()))
            })
            .collect();
        let mut text = serde_json::to_string_pretty(&defaults).unwrap_or_default();
        let mut title = format!("{message}\nEnter values matching this schema:\n{schema}");
        loop {
            let response = tokio::select! {
                biased;
                _ = interaction.cancellation.cancelled() => return Ok(ElicitResult::new(ElicitationAction::Cancel)),
                _ = context.ct.cancelled() => return Ok(ElicitResult::new(ElicitationAction::Cancel)),
                response = interaction.host.ui(pi_rs::UiRequest::Editor {
                    title: title.clone(), initial_value: Some(text.clone()), timeout: None,
                }) => response,
            };
            let Ok(pi_rs::UiResponse::Text(value)) = response else {
                return Ok(ElicitResult::new(ElicitationAction::Cancel));
            };
            text = value;
            match serde_json::from_str::<Value>(&text) {
                Ok(value) => match validator.validate(&value) {
                    Ok(()) => {
                        return Ok(ElicitResult::new(ElicitationAction::Accept).with_content(value));
                    }
                    Err(error) => {
                        title = format!("{message}\nInvalid values: {error}\nSchema: {schema}")
                    }
                },
                Err(error) => title = format!("{message}\nInvalid JSON: {error}\nSchema: {schema}"),
            }
        }
    }
}
async fn elicit_url(
    interaction: &Interaction,
    message: String,
    url: String,
) -> std::result::Result<rmcp::model::ElicitResult, rmcp::model::ErrorData> {
    use rmcp::model::{ElicitResult, ElicitationAction, ErrorData};
    let parsed = reqwest_mcp::Url::parse(&url)
        .map_err(|_| ErrorData::invalid_params("invalid elicitation URL", None))?;
    let loopback = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if url.len() > 8192
        || !(parsed.scheme() == "https" || (parsed.scheme() == "http" && loopback))
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(ErrorData::invalid_params(
            "elicitation requires HTTPS or loopback HTTP without embedded credentials",
            None,
        ));
    }
    let url = parsed.to_string();
    let answer = interaction
        .host
        .ui(pi_rs::UiRequest::Confirm {
            title: message.clone(),
            message: format!("Open this page in your browser?\n{url}"),
            timeout: None,
        })
        .await;
    match answer {
        Ok(pi_rs::UiResponse::Confirmed(true)) => {}
        Ok(pi_rs::UiResponse::Confirmed(false)) => {
            return Ok(ElicitResult::new(ElicitationAction::Decline));
        }
        _ => return Ok(ElicitResult::new(ElicitationAction::Cancel)),
    }
    open_browser(&url)
        .await
        .map_err(|_| ErrorData::internal_error("could not launch browser", None))?;
    let answer = interaction
        .host
        .ui(pi_rs::UiRequest::Confirm {
            title: message,
            message: "Finish the browser step, then confirm completion here.".into(),
            timeout: None,
        })
        .await;
    Ok(ElicitResult::new(match answer {
        Ok(pi_rs::UiResponse::Confirmed(true)) => ElicitationAction::Accept,
        _ => ElicitationAction::Cancel,
    }))
}

pub(crate) async fn open_browser(url: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = tokio::process::Command::new("open");
        command.arg("--").arg(url);
        command
    };
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = tokio::process::Command::new("rundll32");
        command.arg("url.dll,FileProtocolHandler").arg(url);
        command
    };
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    let mut command = {
        let mut command = tokio::process::Command::new("xdg-open");
        command.arg(url);
        command
    };
    command
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let status = tokio::time::timeout(Duration::from_secs(10), command.status())
        .await
        .map_err(|_| Error::Config("browser launch timed out".into()))?
        .map_err(|_| Error::Config("could not launch browser".into()))?;
    if !status.success() {
        return Err(Error::Config("could not launch browser".into()));
    }
    Ok(())
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
#[derive(Clone, PartialEq, Eq)]
struct ConnectionIdentity {
    digest: String,
    oauth_generation: Option<Uuid>,
}

struct Connection {
    identity: RwLock<ConnectionIdentity>,
    service: tokio::sync::Mutex<RunningService<RoleClient, NativeClient>>,
    timeout: Duration,
    name: String,
    config: ServerConfig,
    workspace: PathBuf,
    closed: AtomicBool,
}
impl Connection {
    fn identity(&self) -> ConnectionIdentity {
        self.identity
            .read()
            .expect("MCP connection identity")
            .clone()
    }

    async fn active_service(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, RunningService<RoleClient, NativeClient>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Config("MCP connection is closed".into()));
        }
        let mut service = self.service.lock().await;
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::Config("MCP connection is closed".into()));
        }
        if service.is_closed() || service.peer().is_transport_closed() {
            let replacement =
                McpExtension::connect(&self.name, &self.config, &self.workspace).await?;
            if replacement.connection.identity() != self.identity()
                || self.closed.load(Ordering::SeqCst)
            {
                replacement.close().await;
                return Err(Error::Config(
                    "MCP credentials or connection identity changed; reload the configuration"
                        .into(),
                ));
            }
            let mut incoming = replacement.connection.service.lock().await;
            std::mem::swap(&mut *service, &mut *incoming);
            drop(incoming);
        }
        Ok(service)
    }

    async fn rebind_after_upgrade(&self, previous: Uuid, generation: Uuid) -> Result<()> {
        let mut service = self.service.lock().await;
        if self.closed.load(Ordering::SeqCst) || self.identity().oauth_generation != Some(previous)
        {
            return Err(Error::Config(
                "MCP connection changed during reauthorization".into(),
            ));
        }
        let replacement = McpExtension::connect(&self.name, &self.config, &self.workspace).await?;
        let identity = replacement.connection.identity();
        if self.closed.load(Ordering::SeqCst) || identity.oauth_generation != Some(generation) {
            replacement.close().await;
            return Err(Error::Config(
                "MCP credentials changed during reauthorization".into(),
            ));
        }
        let mut incoming = replacement.connection.service.lock().await;
        std::mem::swap(&mut *service, &mut *incoming);
        *self.identity.write().expect("MCP connection identity") = identity;
        drop(incoming);
        drop(service);
        replacement.close().await;
        Ok(())
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.service.get_mut().cancellation_token().cancel();
    }
}

async fn listen_for_tool_changes(service: &RunningService<RoleClient, NativeClient>, stdio: bool) {
    let Some(info) = service.peer_info() else {
        return;
    };
    if info
        .capabilities
        .tools
        .as_ref()
        .is_none_or(|tools| tools.list_changed != Some(true))
    {
        return;
    }
    let signal = service.service().catalog.clone();
    if info.protocol_version < ProtocolVersion::V_2026_07_28 {
        // A legacy HTTP server can advertise notifications but reject its GET stream.
        signal.listening.store(stdio, Ordering::SeqCst);
        return;
    }
    let filter = SubscriptionFilter::builder().tools_list_changed().build();
    let Ok(Ok(mut subscription)) =
        tokio::time::timeout(Duration::from_secs(2), service.listen(filter)).await
    else {
        return;
    };
    if subscription.acknowledged().tools_list_changed != Some(true) {
        return;
    }
    signal.listening.store(true, Ordering::SeqCst);
    tokio::spawn(async move {
        while let Ok(Some(_)) = subscription.next().await {
            signal.changed();
        }
        signal.listening.store(false, Ordering::SeqCst);
        signal.changed();
    });
}

pub struct McpExtension {
    id: String,
    tools: Vec<Arc<dyn Tool>>,
    deferred: BTreeSet<String>,
    has_codemode: bool,
    has_deferred: bool,
    pub server_name: String,
    pub description: String,
    connection: Arc<Connection>,
    catalog: CatalogSnapshot,
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
        if config
            .exposure
            .iter()
            .chain(config.tool_exposure.values())
            .any(|value| {
                !matches!(
                    value.as_str(),
                    "direct" | "deferred" | "codemode" | "codemode-deferred" | "hidden" | "off"
                )
            })
        {
            return Err(Error::Config(
                "MCP exposure must be direct, deferred, codemode, or hidden".into(),
            ));
        }
        if config.oauth == Some(true) && config.command.is_some() {
            return Err(Error::Config("MCP OAuth requires an HTTP server".into()));
        }
        let timeout = Duration::from_secs(config.timeout.unwrap_or(60).clamp(1, 3600));
        let lifecycle = match config.protocol_version.as_deref() {
            None => ClientLifecycleMode::Initialize,
            Some("2026-07-28") => ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
            Some(version) => {
                return Err(Error::Config(format!(
                    "MCP {name} has unsupported protocolVersion {version}"
                )));
            }
        };
        let mut identity = connection_identity(name, config, workspace)?;
        let mut oauth_generation = None;
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
                NativeClient::default().serve_with_lifecycle(transport, lifecycle),
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
            let initialize = async {
                let transport_config = StreamableHttpClientTransportConfig::with_uri(url.clone())
                    .custom_headers(headers);
                if config.oauth == Some(true) {
                    if config
                        .headers
                        .keys()
                        .any(|name| name.eq_ignore_ascii_case("authorization"))
                    {
                        return Err(Error::Config(
                            "MCP OAuth cannot be combined with an Authorization header".into(),
                        ));
                    }
                    let (client, generation) = crate::mcp_auth::client(name, url, client).await?;
                    identity.update(generation.as_bytes());
                    oauth_generation =
                        Some(Uuid::parse_str(&generation).map_err(|_| {
                            Error::Config("invalid MCP credential generation".into())
                        })?);
                    NativeClient::default()
                        .serve_with_lifecycle(
                            StreamableHttpClientTransport::with_client(client, transport_config),
                            lifecycle,
                        )
                        .await
                } else {
                    NativeClient::default()
                        .serve_with_lifecycle(
                            StreamableHttpClientTransport::with_client(client, transport_config),
                            lifecycle,
                        )
                        .await
                }
                .map_err(|error| {
                    if config.oauth == Some(true) {
                        Error::Config(format!("MCP {name} OAuth connection failed"))
                    } else {
                        Error::Config(format!("MCP {name} initialization failed: {error}"))
                    }
                })
            };
            tokio::time::timeout(timeout.min(Duration::from_secs(10)), initialize)
                .await
                .map_err(|_| Error::Config(format!("MCP {name} initialization timed out")))??
        } else {
            return Err(Error::Config(format!("MCP {name} needs command or url")));
        };
        listen_for_tool_changes(&service, config.command.is_some()).await;
        let connection = Arc::new(Connection {
            identity: RwLock::new(ConnectionIdentity {
                digest: format!("{:x}", identity.finalize()),
                oauth_generation,
            }),
            service: tokio::sync::Mutex::new(service),
            timeout,
            name: name.into(),
            config: config.clone(),
            workspace: workspace.to_owned(),
            closed: AtomicBool::new(false),
        });
        Self::discover(connection).await
    }

    /// Reload definitions while keeping the established transport and remote state.
    pub async fn refresh(&self) -> Result<Self> {
        drop(self.connection.active_service().await?);
        Self::discover(self.connection.clone()).await
    }

    /// Keep a notified catalog between turns and poll when notifications are unavailable.
    pub async fn refresh_if_changed(&self) -> Result<Option<Self>> {
        let service = self.connection.active_service().await?;
        let signal = &service.service().catalog;
        if Arc::ptr_eq(signal, &self.catalog.signal) {
            let no_tools = service
                .peer_info()
                .is_some_and(|info| info.capabilities.tools.is_none());
            let unchanged = signal.listening.load(Ordering::SeqCst)
                && signal.revision.load(Ordering::SeqCst) == self.catalog.revision
                && self.catalog.checked_at.elapsed() < CATALOG_FALLBACK;
            if no_tools || unchanged {
                return Ok(None);
            }
        }
        drop(service);
        Self::discover(self.connection.clone()).await.map(Some)
    }

    async fn discover(connection: Arc<Connection>) -> Result<Self> {
        let name = connection.name.clone();
        let config = connection.config.clone();
        let timeout = connection.timeout;
        let service = connection.service.lock().await;
        let info = service.peer_info();
        let signal = service.service().catalog.clone();
        // A notification received during discovery leaves this snapshot dirty.
        let revision = signal.revision.load(Ordering::SeqCst);
        let definitions = if info
            .as_ref()
            .is_some_and(|info| info.capabilities.tools.is_some())
        {
            tokio::time::timeout(timeout, service.list_all_tools())
                .await
                .map_err(|_| Error::Config(format!("MCP {name} tool discovery timed out")))?
                .map_err(|error| {
                    Error::Config(format!("MCP {name} tool discovery failed: {error}"))
                })?
        } else {
            Vec::new()
        };
        drop(service);
        let catalog = CatalogSnapshot {
            signal,
            revision,
            checked_at: Instant::now(),
        };
        let prefix = format!("mcp__{}__", normalize(&name));
        let mut counts = BTreeMap::<String, usize>::new();
        for definition in &definitions {
            *counts.entry(normalize(&definition.name)).or_default() += 1;
        }
        let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
        let mut deferred = BTreeSet::new();
        let mut has_codemode = false;
        let mut has_deferred = false;
        for definition in definitions {
            if !tool_enabled(&config, &definition.name) {
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
            if is_deferred(&config, &definition.name) {
                has_deferred |= exposure(&config, &definition.name) == "deferred";
                has_codemode |= matches!(
                    exposure(&config, &definition.name),
                    "codemode" | "codemode-deferred"
                );
                deferred.insert(spec.name.clone());
            }
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
                if !tool_enabled(&config, name) {
                    continue;
                }
                let mut exposed = format!("{prefix}{name}");
                while tools.iter().any(|tool| tool.spec().name == exposed) {
                    exposed.push_str("_resource");
                }
                if is_deferred(&config, name) {
                    has_deferred |= exposure(&config, name) == "deferred";
                    has_codemode |=
                        matches!(exposure(&config, name), "codemode" | "codemode-deferred");
                    deferred.insert(exposed.clone());
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
                if !tool_enabled(&config, name) {
                    continue;
                }
                let mut exposed = format!("{prefix}{name}");
                while tools.iter().any(|tool| tool.spec().name == exposed) {
                    exposed.push_str("_prompt");
                }
                if is_deferred(&config, name) {
                    has_deferred |= exposure(&config, name) == "deferred";
                    has_codemode |=
                        matches!(exposure(&config, name), "codemode" | "codemode-deferred");
                    deferred.insert(exposed.clone());
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
            .or_else(|| {
                info.clone()
                    .and_then(|info| info.instructions.clone())
                    .and_then(|instructions| {
                        instructions
                            .lines()
                            .find(|line| !line.trim().is_empty())
                            .map(str::to_owned)
                    })
            })
            .unwrap_or_default();
        Ok(Self {
            id: format!("mcp-{}", normalize(&name)),
            tools,
            deferred,
            has_codemode,
            has_deferred,
            server_name: name,
            description,
            connection,
            catalog,
        })
    }
    pub fn has_deferred(&self) -> bool {
        self.has_deferred
    }
    pub fn has_codemode(&self) -> bool {
        self.has_codemode
    }
    pub fn deferred_tools(&self) -> Vec<ToolSpec> {
        self.tools
            .iter()
            .map(|tool| tool.spec())
            .filter(|tool| self.deferred.contains(&tool.name))
            .collect()
    }
    pub async fn close(&self) {
        self.connection.closed.store(true, Ordering::SeqCst);
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
enum CallError {
    Other(String),
    Scope(Option<String>),
}

impl From<Error> for CallError {
    fn from(error: Error) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<serde_json::Error> for CallError {
    fn from(error: serde_json::Error) -> Self {
        Error::from(error).into()
    }
}

fn call_error(error: ServiceError) -> CallError {
    if let ServiceError::TransportSend(transport) = &error {
        let mut cause = Some(transport.error.as_ref() as &dyn std::error::Error);
        while let Some(error) = cause {
            if let Some(scope) = error.downcast_ref::<InsufficientScopeError>() {
                return CallError::Scope(scope.get_required_scope().map(str::to_owned));
            }
            cause = error.source();
        }
    }
    Error::Config(error.to_string()).into()
}

fn scope_tokens(required: &str) -> Option<Vec<String>> {
    if required.len() > 2048 {
        return None;
    }
    let scopes: Vec<_> = required
        .split(' ')
        .filter(|scope| !scope.is_empty())
        .collect();
    if scopes.is_empty()
        || scopes.len() > 32
        || scopes.iter().any(|scope| {
            !scope
                .bytes()
                .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        })
    {
        return None;
    }
    Some(scopes.into_iter().map(str::to_owned).collect())
}

impl McpTool {
    async fn invoke_call(
        &self,
        arguments: Value,
        context: ToolContext,
        host: Option<Arc<dyn pi_rs::ToolHost>>,
    ) -> Outcome {
        let attempt = || async {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    Err(CallError::Other("MCP request cancelled after dispatch".into()))
                }
                result = tokio::time::timeout(
                    self.connection.timeout, self.call_once(&arguments, &context, host.as_ref()),
                ) => result.unwrap_or_else(|_| {
                    Err(CallError::Other("MCP request timed out after dispatch".into()))
                }),
            }
        };
        let mut result = attempt().await;
        if let Err(CallError::Scope(required)) = &result {
            let Some(scopes) = required.as_deref().and_then(scope_tokens) else {
                return Outcome::Failed("MCP server requires additional OAuth scopes but did not supply a valid scope list".into());
            };
            let authorized = tokio::select! {
                _ = context.cancellation.cancelled() => Err(Error::Cancelled),
                result = self.authorize_scopes(&scopes, host.as_deref()) => result,
            };
            if let Err(error) = authorized {
                return Outcome::Failed(error.to_string());
            }
            // The typed HTTP 403 rejected the request before tool dispatch.
            // Retry once with the same operation identifier and new authorization.
            result = attempt().await;
        }
        match result {
            Ok(CallResult::Data(data)) if data["isError"] == true => {
                Outcome::Failed(data.to_string())
            }
            Ok(CallResult::Data(data)) => Outcome::Structured {
                data,
                attachments: vec![],
            },
            Ok(CallResult::Task(reference)) => Outcome::Pending { reference },
            Err(CallError::Scope(_)) => Outcome::Failed(
                "MCP server still requires additional OAuth scopes after reauthorization".into(),
            ),
            Err(CallError::Other(error)) => Outcome::Unknown(error),
        }
    }

    async fn authorize_scopes(
        &self,
        scopes: &[String],
        host: Option<&dyn pi_rs::ToolHost>,
    ) -> Result<()> {
        let connection = &self.connection;
        let Some(previous) = connection.identity().oauth_generation else {
            return Err(Error::Config(
                "MCP server requires additional scopes for its configured credentials".into(),
            ));
        };
        let url = connection
            .config
            .url
            .as_deref()
            .expect("OAuth requires HTTP");
        let scope_text = scopes.join(" ");
        let host = host.filter(|host| host.has_ui()).ok_or_else(|| {
            Error::Config(format!(
                "MCP {} requires OAuth scopes {scope_text}; sign in with these scopes",
                connection.name
            ))
        })?;
        let confirmation = host.ui(pi_rs::UiRequest::Confirm {
            title: format!("MCP permissions for {}", connection.name),
            message: format!("Allow additional OAuth scopes: {scope_text}? The browser will open to authorize them."),
            timeout: Some(120_000),
        }).await.map_err(|_| Error::Config("MCP scope confirmation failed".into()))?;
        if confirmation != pi_rs::UiResponse::Confirmed(true) {
            return Err(Error::Config("MCP scope upgrade declined".into()));
        }
        let generation =
            crate::mcp_auth::upgrade(&connection.name, url, previous, scopes, |_| {}).await?;
        connection.rebind_after_upgrade(previous, generation).await
    }

    async fn call_once(
        &self,
        arguments: &Value,
        context: &ToolContext,
        host: Option<&Arc<dyn pi_rs::ToolHost>>,
    ) -> std::result::Result<CallResult, CallError> {
        if self.resource {
            return self.invoke_resource(arguments).await.map(CallResult::Data);
        }
        let params = serde_json::from_value(
            json!({"name":self.remote_name,"arguments":arguments,"_meta":{"operationId":context.operation_id.to_string()}}),
        )?;
        let service = self.connection.active_service().await?;
        let interaction = service.service().interaction.clone();
        *interaction
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = host.map(|host| Interaction {
            host: host.clone(),
            cancellation: context.cancellation.clone(),
        });
        let _interaction = InteractionGuard(interaction);
        let result = service.call_tool_once(params).await.map_err(call_error)?;
        match result {
            CallToolResponse::Complete(result) => {
                Ok(CallResult::Data(serde_json::to_value(result)?))
            }
            CallToolResponse::Task(task) => {
                Ok(CallResult::Task(serde_json::to_string(&TaskReference {
                    connection: self.connection.identity().digest,
                    task_id: task.task.task_id,
                })?))
            }
            _ => Err(Error::Config("MCP tool requires additional client input".into()).into()),
        }
    }
    async fn invoke_resource(&self, arguments: &Value) -> std::result::Result<Value, CallError> {
        let service = self.connection.active_service().await?;
        match self.remote_name.as_str() {
            "list_resources" => {
                let resources = service.list_all_resources().await.map_err(call_error)?;
                Ok(json!({"resources":resources}))
            }
            "list_resource_templates" => {
                let templates = service
                    .list_all_resource_templates()
                    .await
                    .map_err(call_error)?;
                Ok(json!({"resourceTemplates":templates}))
            }
            "list_prompts" => {
                let prompts = service.list_all_prompts().await.map_err(call_error)?;
                Ok(json!({"prompts":prompts}))
            }
            "get_prompt" => {
                let prompt = service
                    .get_prompt(serde_json::from_value(arguments.clone())?)
                    .await
                    .map_err(call_error)?;
                Ok(serde_json::to_value(prompt)?)
            }
            "read_resource" => {
                let result = service
                    .read_resource_once(serde_json::from_value(json!({"uri":arguments["uri"]}))?)
                    .await
                    .map_err(call_error)?;
                match result {
                    rmcp::model::ReadResourceResponse::Complete(result) => {
                        Ok(serde_json::to_value(result)?)
                    }
                    _ => Err(
                        Error::Config("MCP resource requires additional client input".into())
                            .into(),
                    ),
                }
            }
            _ => Err(Error::Config("unknown MCP resource operation".into()).into()),
        }
    }
}
fn connection_identity(name: &str, config: &ServerConfig, workspace: &Path) -> Result<Sha256> {
    let workspace = workspace.canonicalize()?;
    let mut identity = Sha256::new();
    identity.update(json!({"name":name,"command":config.command,"args":config.args,"url":config.url,"cwd":config.cwd,"workspace":workspace}).to_string());
    if let Some(version) = &config.protocol_version {
        identity.update(version.as_bytes());
    }
    Ok(identity)
}

fn exposure<'a>(config: &'a ServerConfig, name: &str) -> &'a str {
    config
        .tool_exposure
        .get(name)
        .or_else(|| {
            config
                .tool_exposure
                .iter()
                .find(|(pattern, _)| {
                    pattern.contains('*')
                        && regex::Regex::new(&format!(
                            "^{}$",
                            regex::escape(pattern).replace("\\*", ".*")
                        ))
                        .is_ok_and(|matcher| matcher.is_match(name))
                })
                .map(|(_, value)| value)
        })
        .or(config.exposure.as_ref())
        .map(String::as_str)
        .unwrap_or("codemode")
}
fn tool_enabled(config: &ServerConfig, name: &str) -> bool {
    !matches!(exposure(config, name), "hidden" | "off")
}
fn is_deferred(config: &ServerConfig, name: &str) -> bool {
    matches!(
        exposure(config, name),
        "deferred" | "codemode" | "codemode-deferred"
    )
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
        self.invoke_call(arguments, context, None).await
    }
    async fn invoke_with_host(
        &self,
        arguments: Value,
        context: ToolContext,
        _state: Option<&pi_rs::ExtensionState>,
        host: Arc<dyn pi_rs::ToolHost>,
    ) -> Outcome {
        self.invoke_call(arguments, context, Some(host)).await
    }
    async fn poll(&self, reference: &str, context: ToolContext) -> Option<Outcome> {
        let task: TaskReference = serde_json::from_str(reference).ok()?;
        loop {
            let params = serde_json::from_value(json!({"taskId":task.task_id})).ok()?;
            let response = match self.connection.active_service().await {
                Ok(service) => {
                    if task.connection != self.connection.identity().digest {
                        return None;
                    }
                    service.get_task(params).await
                }
                Err(_) => {
                    return Some(Outcome::Pending {
                        reference: reference.into(),
                    });
                }
            };
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
    fn exact_exposure_overrides_wildcard_selection() {
        let config: ServerConfig = serde_json::from_value(json!({"url":"http://127.0.0.1/mcp","exposure":"hidden","toolExposure":{"read*":"deferred","read_one":"direct"}})).unwrap();
        assert_eq!(exposure(&config, "read_one"), "direct");
        assert!(is_deferred(&config, "read_many"));
        assert!(!tool_enabled(&config, "write"));
    }
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
