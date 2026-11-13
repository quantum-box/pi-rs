//! Native MCP client connections exposed through the shared tool contract.
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io::{Read, Seek, SeekFrom, Write};
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

use crate::mcp_provider_auth::ProviderClient;
pub use crate::mcp_provider_auth::TokenResolver;

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
    pub oauth: Option<crate::mcp_auth::OAuthSetting>,
    pub auth: Option<ProviderAuth>,
    #[serde(default)]
    pub tool_exposure: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ProviderAuth {
    pub provider: String,
}

impl ServerConfig {
    pub fn oauth_client(&self) -> Option<&crate::mcp_auth::OAuthClientSettings> {
        match &self.oauth {
            Some(crate::mcp_auth::OAuthSetting::Client(client)) => Some(client),
            _ => None,
        }
    }

    fn oauth_required(&self) -> bool {
        use crate::mcp_auth::OAuthSetting;
        matches!(
            self.oauth,
            Some(OAuthSetting::Enabled(true) | OAuthSetting::Client(_))
        )
    }

    fn oauth_allowed(&self) -> bool {
        !matches!(
            self.oauth,
            Some(crate::mcp_auth::OAuthSetting::Enabled(false))
        )
    }
}

#[derive(Clone, Default)]
struct NativeClient {
    interaction: Arc<std::sync::Mutex<Option<Interaction>>>,
    catalog: Arc<CatalogSignal>,
    dispatch_closed: Arc<std::sync::Mutex<crate::mcp_oauth_client::DispatchState>>,
}

struct AbandonedCall {
    closed: Arc<std::sync::Mutex<crate::mcp_oauth_client::DispatchState>>,
    service: Option<rmcp::service::RunningServiceCancellationToken>,
}
impl Drop for AbandonedCall {
    fn drop(&mut self) {
        if let Some(service) = self.service.take() {
            // Close the gate before stopping rmcp: its best-effort cancellation
            // notification can be lost when the peer queue is full. A POST
            // already started remains ambiguous and is reported as such.
            self.closed
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .closed = true;
            service.cancel();
        }
    }
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
    crate::oauth_callback::open_browser(&url)
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
    provider_token: Option<TokenResolver>,
    http_options: Option<HttpOptionsWithProxyHosts>,
    closed: AtomicBool,
    page_outputs: PageOutputs,
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
            let replacement = McpExtension::connect_with_proxy_hosts(
                &self.name,
                &self.config,
                &self.workspace,
                self.provider_token.clone(),
                self.http_options.clone(),
            )
            .await?;
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
        let replacement = McpExtension::connect_with_proxy_hosts(
            &self.name,
            &self.config,
            &self.workspace,
            None,
            self.http_options.clone(),
        )
        .await?;
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

/// HTTP transport settings supplied by an embedding host.
#[derive(Clone)]
pub struct HttpOptions {
    pub proxies: Vec<reqwest_mcp::Proxy>,
    pub idle_timeout: Option<Duration>,
}

/// HTTP settings with proxy host names for OAuth DNS validation.
#[derive(Clone)]
pub struct HttpOptionsWithProxyHosts {
    pub http: HttpOptions,
    pub proxy_hosts: Vec<String>,
}

impl std::ops::Deref for HttpOptionsWithProxyHosts {
    type Target = HttpOptions;
    fn deref(&self) -> &HttpOptions {
        &self.http
    }
}

impl McpExtension {
    pub async fn connect(name: &str, config: &ServerConfig, workspace: &Path) -> Result<Self> {
        Self::connect_with_provider_token(name, config, workspace, None).await
    }

    pub async fn connect_with_provider_token(
        name: &str,
        config: &ServerConfig,
        workspace: &Path,
        provider_token: Option<TokenResolver>,
    ) -> Result<Self> {
        Self::connect_with_http_options(name, config, workspace, provider_token, None).await
    }

    pub async fn connect_with_http_options(
        name: &str,
        config: &ServerConfig,
        workspace: &Path,
        provider_token: Option<TokenResolver>,
        http_options: Option<HttpOptions>,
    ) -> Result<Self> {
        Self::connect_with_proxy_hosts(
            name,
            config,
            workspace,
            provider_token,
            http_options.map(|http| HttpOptionsWithProxyHosts {
                http,
                proxy_hosts: Vec::new(),
            }),
        )
        .await
    }

    pub async fn connect_with_proxy_hosts(
        name: &str,
        config: &ServerConfig,
        workspace: &Path,
        provider_token: Option<TokenResolver>,
        http_options: Option<HttpOptionsWithProxyHosts>,
    ) -> Result<Self> {
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
        if config.oauth_required() && config.command.is_some() {
            return Err(Error::Config("MCP OAuth requires an HTTP server".into()));
        }
        let provider_token = if let Some(auth) = &config.auth {
            if auth.provider.trim().is_empty() || config.command.is_some() || config.url.is_none() {
                return Err(Error::Config(
                    "MCP auth.provider requires a provider name and HTTP server".into(),
                ));
            }
            if config.oauth_required()
                || config
                    .headers
                    .keys()
                    .any(|name| name.eq_ignore_ascii_case("authorization"))
            {
                return Err(Error::Config(
                    "MCP auth.provider cannot be combined with OAuth or an Authorization header"
                        .into(),
                ));
            }
            Some(provider_token.ok_or_else(|| {
                Error::Config("MCP provider authentication is unavailable".into())
            })?)
        } else {
            None
        };
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
            let manual_auth = headers.contains_key(&reqwest_mcp::header::AUTHORIZATION);
            let may_authorize = config.oauth_allowed() && !manual_auth && provider_token.is_none();
            let use_oauth = config.oauth_required()
                || (may_authorize && crate::mcp_auth::has_credentials(name, url)?);
            let client = reqwest_mcp::Client::builder()
                .redirect(reqwest_mcp::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10));
            let client = if let Some(options) = &http_options {
                let mut client = client.no_proxy();
                for proxy in &options.proxies {
                    client = client.proxy(proxy.clone());
                }
                if let Some(timeout) = options.idle_timeout {
                    client = client.read_timeout(timeout);
                }
                client
            } else if provider_token.is_some() {
                client.no_proxy()
            } else {
                client
            };
            let client = client
                .build()
                .map_err(|error| Error::Config(error.to_string()))?;
            let initialize = async {
                let transport_config = StreamableHttpClientTransportConfig::with_uri(url.clone())
                    .custom_headers(headers);
                if use_oauth {
                    if manual_auth {
                        return Err(Error::Config(
                            "MCP OAuth cannot be combined with an Authorization header".into(),
                        ));
                    }
                    let (client, generation) = crate::mcp_auth::client_with_http_options(
                        name,
                        url,
                        client,
                        http_options.as_ref(),
                    )
                    .await?;
                    identity.update(generation.as_bytes());
                    oauth_generation =
                        Some(Uuid::parse_str(&generation).map_err(|_| {
                            Error::Config("invalid MCP credential generation".into())
                        })?);
                    NativeClient {
                        dispatch_closed: client.1.clone(),
                        ..Default::default()
                    }
                    .serve_with_lifecycle(
                        StreamableHttpClientTransport::with_client(client, transport_config),
                        lifecycle,
                    )
                    .await
                } else if let Some(resolver) = provider_token.clone() {
                    let client = ProviderClient {
                        client,
                        endpoint: Arc::from(url.as_str()),
                        resolver,
                    };
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
                    let challenge = error.auth_challenge().filter(|challenge| {
                        challenge
                            .split_ascii_whitespace()
                            .next()
                            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("Bearer"))
                    });
                    if may_authorize
                        && (challenge.is_some() || (use_oauth && error.is_authorization_required()))
                    {
                        Error::AuthorizationRequired {
                            message: format!(
                                "MCP {name} needs OAuth sign-in; run pi mcp login {name}"
                            ),
                            challenge: challenge.map(str::to_owned),
                        }
                    } else if use_oauth {
                        Error::Config(format!("MCP {name} OAuth connection failed"))
                    } else if provider_token.is_some() {
                        Error::Config(format!("MCP {name} provider authentication failed"))
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
            provider_token,
            http_options,
            closed: AtomicBool::new(false),
            page_outputs: std::sync::Mutex::new(VecDeque::new()),
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
            for (name, policy, description, parameters) in [
                (
                    "list_resources",
                    "list_resources",
                    "List this MCP server's available resources",
                    json!({"type":"object","properties":{},"additionalProperties":false}),
                ),
                (
                    "list_resource_templates",
                    "list_resource_templates",
                    "List parameterized resource URI templates from this MCP server",
                    json!({"type":"object","properties":{},"additionalProperties":false}),
                ),
                (
                    "list_resources_page",
                    "list_resources",
                    "List one page of this MCP server's resources; pass nextCursor as cursor to continue. For saved oversized pages, pass outputId and offset to read chunks",
                    json!({"type":"object","properties":{"cursor":{"type":"string"},"outputId":{"type":"string"},"offset":{"type":"integer","minimum":0}},"additionalProperties":false}),
                ),
                (
                    "list_resource_templates_page",
                    "list_resource_templates",
                    "List one page of this MCP server's resource templates; pass nextCursor as cursor to continue. For saved oversized pages, pass outputId and offset to read chunks",
                    json!({"type":"object","properties":{"cursor":{"type":"string"},"outputId":{"type":"string"},"offset":{"type":"integer","minimum":0}},"additionalProperties":false}),
                ),
                (
                    "read_resource",
                    "read_resource",
                    "Read a resource URI from this MCP server",
                    json!({"type":"object","properties":{"uri":{"type":"string"}},"required":["uri"],"additionalProperties":false}),
                ),
            ] {
                if !tool_enabled(&config, policy) {
                    continue;
                }
                let mut exposed = format!("{prefix}{name}");
                while tools.iter().any(|tool| tool.spec().name == exposed) {
                    exposed.push_str("_resource");
                }
                if is_deferred(&config, policy) {
                    has_deferred |= exposure(&config, policy) == "deferred";
                    has_codemode |=
                        matches!(exposure(&config, policy), "codemode" | "codemode-deferred");
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
        self.connection
            .page_outputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
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
        request: &mut pi_rs::ContextRequest<'_>,
        _: Option<&pi_rs::ExtensionState>,
    ) -> pi_rs::ExtensionResult<pi_rs::ContextChange> {
        if !self.description.is_empty()
            && let Some(content) = request.system_content_mut()
        {
            content.push_str(&format!(
                "\n\nMCP server {}: {}",
                self.server_name, self.description
            ));
            return Ok(pi_rs::ContextChange::Changed);
        }
        Ok(pi_rs::ContextChange::Unchanged)
    }
}
struct McpTool {
    connection: Arc<Connection>,
    remote_name: String,
    spec: ToolSpec,
    resource: bool,
}
enum PreparedPage {
    Inline(Value),
    Saved {
        file: tempfile::NamedTempFile,
        bytes: usize,
        cursor: Option<String>,
    },
}

type PageOutputs = std::sync::Mutex<VecDeque<(Uuid, String, tempfile::NamedTempFile)>>;

async fn run_page_preparation<F>(
    cancellation: &pi_rs::CancellationToken,
    work: F,
) -> Result<PreparedPage>
where
    F: FnOnce(pi_rs::CancellationToken) -> Result<PreparedPage> + Send + 'static,
{
    let worker_cancel = cancellation.child_token();
    let _cancel_on_drop = worker_cancel.clone().drop_guard();
    let task = tokio::task::spawn_blocking(move || work(worker_cancel));
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = task => result.map_err(|e| Error::Config(format!("MCP page preparation failed: {e}")))?,
    }
}

// Serde may pass an entire large string to Write at once. Split those writes
// so a dropped or cancelled invocation stops serialization cooperatively.
struct CancellableWriter<W> {
    inner: W,
    cancellation: pi_rs::CancellationToken,
}
impl<W: Write> Write for CancellableWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other("MCP page preparation cancelled"));
        }
        self.inner.write(&bytes[..bytes.len().min(64 * 1024)])
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn publish_page_output(
    outputs: &PageOutputs,
    closed: &AtomicBool,
    cancellation: &pi_rs::CancellationToken,
    id: Uuid,
    kind: String,
    file: tempfile::NamedTempFile,
) -> Result<()> {
    let mut outputs = outputs.lock().unwrap_or_else(|e| e.into_inner());
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    if closed.load(Ordering::SeqCst) {
        return Err(Error::Config("MCP connection is closed".into()));
    }
    if outputs.len() >= 8 {
        outputs.pop_front();
    }
    outputs.push_back((id, kind, file));
    // Cancellation can arrive from another thread while this lock is held.
    if cancellation.is_cancelled() {
        outputs.pop_back();
        return Err(Error::Cancelled);
    }
    Ok(())
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
        let page_tool = self.resource
            && matches!(
                self.remote_name.as_str(),
                "list_resources_page" | "list_resource_templates_page"
            );
        if page_tool && arguments.get("outputId").is_some() {
            if context.cancellation.is_cancelled() || self.connection.closed.load(Ordering::SeqCst)
            {
                return Outcome::Failed("MCP page retrieval cancelled or connection closed".into());
            }
            return match self.read_page_output(&arguments) {
                Ok(data) => Outcome::Structured {
                    data,
                    attachments: vec![],
                },
                Err(error) => Outcome::Failed(error.to_string()),
            };
        }
        if page_tool && let Err(error) = resource_page_params(&arguments) {
            return Outcome::Failed(error.to_string());
        }
        let attempt = || async {
            let dispatch_cancel = context.cancellation.child_token();
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    Err(CallError::Other("MCP request cancelled after dispatch".into()))
                }
                _ = async {
                    tokio::time::sleep(self.connection.timeout).await;
                    dispatch_cancel.cancel();
                } => {
                    Err(CallError::Other("MCP request timed out after dispatch".into()))
                }
                result = self.call_once(&arguments, &context, host.as_ref(), &dispatch_cancel) => result,
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
            Ok(CallResult::Data(data)) => {
                let data = if page_tool {
                    match self.save_large_page(data, &context.cancellation).await {
                        Ok(data) => data,
                        Err(error) => return Outcome::Failed(error.to_string()),
                    }
                } else {
                    data
                };
                Outcome::Structured {
                    data,
                    attachments: vec![],
                }
            }
            Ok(CallResult::Task(reference)) => Outcome::Pending { reference },
            Err(CallError::Scope(_)) => Outcome::Failed(
                "MCP server still requires additional OAuth scopes after reauthorization".into(),
            ),
            Err(CallError::Other(error)) => Outcome::Unknown(error),
        }
    }

    // Preparation can serialize and write arbitrarily large pages. Run it off
    // the async runtime and publish only after cancellation and close checks.
    async fn save_large_page(
        &self,
        data: Value,
        cancellation: &pi_rs::CancellationToken,
    ) -> Result<Value> {
        let prepared = run_page_preparation(cancellation, move |worker_cancel| {
            let mut writer = CancellableWriter {
                inner: Vec::new(),
                cancellation: worker_cancel.clone(),
            };
            if let Err(error) = serde_json::to_writer(&mut writer, &data) {
                return if worker_cancel.is_cancelled() {
                    Err(Error::Cancelled)
                } else {
                    Err(error.into())
                };
            }
            let text = writer.inner;
            if worker_cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            if text.len() <= 512 * 1024 {
                return Ok(PreparedPage::Inline(data));
            }
            let mut file = tempfile::NamedTempFile::new()
                .map_err(|e| Error::Config(format!("cannot save MCP page: {e}")))?;
            for chunk in text.chunks(64 * 1024) {
                if worker_cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                file.write_all(chunk)
                    .map_err(|e| Error::Config(format!("cannot save MCP page: {e}")))?;
            }
            if worker_cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let cursor = data
                .get("nextCursor")
                .and_then(Value::as_str)
                .filter(|c| c.len() <= 16 * 1024)
                .map(str::to_owned);
            Ok(PreparedPage::Saved {
                file,
                bytes: text.len(),
                cursor,
            })
        })
        .await?;
        match prepared {
            PreparedPage::Inline(data) => {
                if cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if self.connection.closed.load(Ordering::SeqCst) {
                    return Err(Error::Config("MCP connection is closed".into()));
                }
                Ok(data)
            }
            PreparedPage::Saved {
                file,
                bytes,
                cursor,
            } => {
                let id = Uuid::new_v4();
                publish_page_output(
                    &self.connection.page_outputs,
                    &self.connection.closed,
                    cancellation,
                    id,
                    self.remote_name.clone(),
                    file,
                )?;
                let mut result = json!({"server": self.connection.name, "outputId": id.to_string(),
                    "outputBytes": bytes, "readTool": self.spec.name,
                    "message": "Full page saved; call this tool with outputId and byte offset to retrieve JSON chunks."});
                if let Some(cursor) = cursor {
                    result["nextCursor"] = json!(cursor);
                }
                Ok(result)
            }
        }
    }

    fn read_page_output(&self, arguments: &Value) -> Result<Value> {
        if arguments.get("cursor").is_some() {
            return Err(Error::Config(
                "cursor cannot be combined with outputId".into(),
            ));
        }
        let id = arguments
            .get("outputId")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| Error::Config("invalid outputId".into()))?;
        let offset = match arguments.get("offset") {
            None => 0,
            Some(value) => value
                .as_u64()
                .ok_or_else(|| Error::Config("offset must be a non-negative integer".into()))?,
        };
        let mut outputs = self
            .connection
            .page_outputs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (_, _, file) = outputs
            .iter_mut()
            .find(|(key, kind, _)| *key == id && *kind == self.remote_name)
            .ok_or_else(|| Error::Config("MCP page output expired or unknown".into()))?;
        let size = file
            .as_file()
            .metadata()
            .map_err(|e| Error::Config(e.to_string()))?
            .len();
        if offset > size {
            return Err(Error::Config("offset exceeds page output".into()));
        }
        let file = file.as_file_mut();
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| Error::Config(e.to_string()))?;
        let mut bytes = vec![0; (size - offset).min(16 * 1024) as usize];
        file.read_exact(&mut bytes)
            .map_err(|e| Error::Config(e.to_string()))?;
        let valid = match std::str::from_utf8(&bytes) {
            Ok(_) => bytes.len(),
            Err(e) if e.error_len().is_none() => e.valid_up_to(),
            Err(_) => return Err(Error::Config("offset is not a UTF-8 boundary".into())),
        };
        let chunk = std::str::from_utf8(&bytes[..valid]).expect("valid UTF-8");
        let next = offset + valid as u64;
        let mut result = json!({"outputId": id.to_string(), "offset": offset, "text": chunk});
        if next < size {
            result["nextOffset"] = json!(next);
        }
        Ok(result)
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
        let generation = crate::mcp_auth::upgrade(
            &connection.name,
            url,
            previous,
            scopes,
            connection.http_options.as_ref(),
            |_| {},
        )
        .await?;
        connection.rebind_after_upgrade(previous, generation).await
    }

    async fn call_once(
        &self,
        arguments: &Value,
        context: &ToolContext,
        host: Option<&Arc<dyn pi_rs::ToolHost>>,
        dispatch_cancel: &pi_rs::CancellationToken,
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
        service
            .service()
            .dispatch_closed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancellation = Some(dispatch_cancel.clone());
        let mut abandoned = AbandonedCall {
            closed: service.service().dispatch_closed.clone(),
            service: Some(service.cancellation_token()),
        };
        let result = service.call_tool_once(params).await;
        service
            .service()
            .dispatch_closed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancellation = None;
        abandoned.service = None;
        let result = result.map_err(call_error)?;
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
                Ok(json!({"resources": visible_resource_listing(serde_json::to_value(resources)?)}))
            }
            "list_resource_templates" => {
                let templates = service
                    .list_all_resource_templates()
                    .await
                    .map_err(call_error)?;
                Ok(
                    json!({"resourceTemplates": visible_resource_listing(serde_json::to_value(templates)?)}),
                )
            }
            "list_resources_page" => {
                let result = service
                    .list_resources(Some(resource_page_params(arguments)?))
                    .await
                    .map_err(call_error)?;
                Ok(resource_page(
                    &self.connection.name,
                    "resources",
                    serde_json::to_value(result.resources)?,
                    result.next_cursor,
                ))
            }
            "list_resource_templates_page" => {
                let result = service
                    .list_resource_templates(Some(resource_page_params(arguments)?))
                    .await
                    .map_err(call_error)?;
                Ok(resource_page(
                    &self.connection.name,
                    "resourceTemplates",
                    serde_json::to_value(result.resource_templates)?,
                    result.next_cursor,
                ))
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

fn resource_page_params(arguments: &Value) -> Result<rmcp::model::PaginatedRequestParams> {
    let cursor = match arguments.get("cursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => return Err(Error::Config("cursor must be a string".into())),
    };
    let mut params = rmcp::model::PaginatedRequestParams::default();
    params.cursor = cursor;
    Ok(params)
}

fn resource_page(server: &str, key: &str, items: Value, cursor: Option<String>) -> Value {
    let items: Vec<_> = visible_resource_listing(items)
        .into_iter()
        .map(|mut item| {
            item["server"] = json!(server);
            item
        })
        .collect();
    let mut result = json!({"server": server});
    result[key] = json!(items);
    if let Some(cursor) = cursor {
        result["nextCursor"] = json!(cursor);
    }
    result
}

/// Match Pi's MCP App listing filter, including its MIME-profile prefix matching.
fn visible_resource_listing(value: Value) -> Vec<Value> {
    static APP_PROFILE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"(?i);\s*profile\s*=\s*"?mcp-app"?"#).expect("MCP App MIME profile")
    });
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let mut entry = entry.as_object()?.clone();
            let uri = entry
                .get("uri")
                .or_else(|| entry.get("uriTemplate"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let mime = entry.get("mimeType").and_then(Value::as_str).unwrap_or("");
            if uri.starts_with("ui://") || APP_PROFILE.is_match(mime) {
                return None;
            }
            entry.remove("_meta");
            entry.remove("icons");
            Some(Value::Object(entry))
        })
        .collect()
}
fn connection_identity(name: &str, config: &ServerConfig, workspace: &Path) -> Result<Sha256> {
    let workspace = workspace.canonicalize()?;
    let mut identity = Sha256::new();
    identity.update(json!({"name":name,"command":config.command,"args":config.args,"url":config.url,"cwd":config.cwd,"workspace":workspace}).to_string());
    if let Some(version) = &config.protocol_version {
        identity.update(version.as_bytes());
    }
    if let Some(auth) = &config.auth {
        identity.update(json!(["provider", auth.provider]).to_string());
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

    #[tokio::test]
    async fn cancelled_page_preparation_does_not_block_or_leak_file() {
        let cancellation = pi_rs::CancellationToken::new();
        let worker_token = cancellation.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            run_page_preparation(&worker_token, move |_worker_cancel| {
                let file = tempfile::NamedTempFile::new().unwrap();
                started_tx.send(file.path().to_owned()).unwrap();
                release_rx.recv().unwrap();
                let _ = finished_tx.send(());
                Ok(PreparedPage::Saved {
                    file,
                    bytes: 1,
                    cursor: None,
                })
            })
            .await
        });
        let path = tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(path.exists());
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(Error::Cancelled)));
        release_tx.send(()).unwrap();
        finished_rx.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn dropping_preparation_cancels_worker_without_cancelling_parent() {
        let parent = pi_rs::CancellationToken::new();
        let caller_parent = parent.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            run_page_preparation(&caller_parent, move |worker_cancel| {
                let file = tempfile::NamedTempFile::new().unwrap();
                started_tx.send(file.path().to_owned()).unwrap();
                while !worker_cancel.is_cancelled() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                drop(file);
                let _ = stopped_tx.send(());
                Err(Error::Cancelled)
            })
            .await
        });
        let path = tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(5), stopped_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(!parent.is_cancelled());
        assert!(!path.exists());
    }

    #[test]
    fn serialization_checks_cancellation_during_large_string() {
        struct CancellingSink {
            token: pi_rs::CancellationToken,
            bytes: usize,
        }
        impl Write for CancellingSink {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.bytes += bytes.len();
                if self.bytes >= 64 * 1024 {
                    self.token.cancel();
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let token = pi_rs::CancellationToken::new();
        let sink = CancellingSink {
            token: token.clone(),
            bytes: 0,
        };
        let mut writer = CancellableWriter {
            inner: sink,
            cancellation: token,
        };
        assert!(
            serde_json::to_writer(&mut writer, &json!({"large": "x".repeat(1024 * 1024)})).is_err()
        );
        assert!(writer.inner.bytes < 128 * 1024);
    }

    #[test]
    fn closing_while_page_waits_to_publish_discards_file() {
        let outputs = Arc::new(PageOutputs::default());
        let closed = Arc::new(AtomicBool::new(false));
        let guard = outputs.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_owned();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let other_outputs = outputs.clone();
        let other_closed = closed.clone();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            publish_page_output(
                &other_outputs,
                &other_closed,
                &pi_rs::CancellationToken::new(),
                Uuid::new_v4(),
                "list_resources_page".into(),
                file,
            )
        });
        started_rx.recv().unwrap();
        // close sets the flag before taking the same cache lock and clearing it.
        closed.store(true, Ordering::SeqCst);
        drop(guard);
        assert!(worker.join().unwrap().is_err());
        assert!(outputs.lock().unwrap().is_empty());
        assert!(!path.exists());
    }

    #[test]
    fn cancelled_page_is_not_published() {
        let outputs = PageOutputs::default();
        let token = pi_rs::CancellationToken::new();
        token.cancel();
        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_owned();
        assert!(matches!(
            publish_page_output(
                &outputs,
                &AtomicBool::new(false),
                &token,
                Uuid::new_v4(),
                "list_resources_page".into(),
                file
            ),
            Err(Error::Cancelled)
        ));
        assert!(outputs.lock().unwrap().is_empty());
        assert!(!path.exists());
    }

    struct ReleaseTestLatch(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for ReleaseTestLatch {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
            wake.notify_all();
        }
    }

    #[test]
    fn frozen_transport_is_released_during_panic_unwind() {
        let latch = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn({
            let latch = latch.clone();
            move || {
                let (lock, wake) = &*latch;
                let held = lock.lock().unwrap();
                waiting_tx.send(()).unwrap();
                let (_guard, result) = wake
                    .wait_timeout_while(held, Duration::from_secs(5), |released| !*released)
                    .unwrap();
                finished_tx.send(!result.timed_out()).unwrap();
            }
        });
        waiting_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let result = std::panic::catch_unwind(|| {
            let _release = ReleaseTestLatch(latch);
            panic!("fixture early failure");
        });
        assert!(result.is_err());
        assert!(finished_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelled_tool_refresh_never_dispatches_with_full_notification_queue() {
        use rmcp::{
            model::{ClientJsonRpcMessage, ClientNotification, ServerJsonRpcMessage},
            transport::Transport,
        };
        use std::sync::{Condvar, Mutex, atomic::AtomicUsize};
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

        struct Freeze<T> {
            inner: T,
            armed: Arc<AtomicBool>,
            blocked: std::sync::mpsc::Sender<()>,
            latch: Arc<(Mutex<bool>, Condvar)>,
            latch_expired: Arc<AtomicBool>,
        }
        impl<T: Transport<RoleClient>> Transport<RoleClient> for Freeze<T> {
            type Error = T::Error;
            fn send(
                &mut self,
                message: ClientJsonRpcMessage,
            ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send + 'static
            {
                if matches!(message, ClientJsonRpcMessage::Notification(_))
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    self.blocked.send(()).unwrap();
                    let (lock, wake) = &*self.latch;
                    let release = lock.lock().unwrap();
                    let (_guard, timeout) = wake
                        .wait_timeout_while(release, Duration::from_secs(30), |release| !*release)
                        .unwrap();
                    self.latch_expired
                        .store(timeout.timed_out(), Ordering::SeqCst);
                }
                self.inner.send(message)
            }
            fn receive(
                &mut self,
            ) -> impl std::future::Future<Output = Option<ServerJsonRpcMessage>> + Send
            {
                self.inner.receive()
            }
            fn close(
                &mut self,
            ) -> impl std::future::Future<Output = std::result::Result<(), Self::Error>> + Send
            {
                self.inner.close()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let resource = format!("{base}/mcp");
        let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::channel(1);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = calls.clone();
        let server = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let refresh_tx = refresh_tx.clone();
                let release = release.clone();
                let count = call_count.clone();
                tokio::spawn(async move {
                    let mut socket = BufReader::new(socket);
                    let mut first = String::new();
                    if socket.read_line(&mut first).await.is_err() {
                        return;
                    }
                    let mut length = 0;
                    loop {
                        let mut line = String::new();
                        if socket.read_line(&mut line).await.is_err()
                            || line == "\r\n"
                            || line.is_empty()
                        {
                            break;
                        }
                        if let Some(value) =
                            line.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            length = value.trim().parse().unwrap();
                        }
                    }
                    let mut bytes = vec![0; length];
                    if socket.read_exact(&mut bytes).await.is_err() {
                        return;
                    }
                    let body = if first.starts_with("POST /token ") {
                        let _ = refresh_tx.send(()).await;
                        if let Some(receiver) = release.lock().await.take() {
                            let _ = receiver.await;
                        }
                        r#"{"access_token":"new","token_type":"Bearer","refresh_token":"rotated"}"#
                            .to_owned()
                    } else {
                        let request: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                        match request["method"].as_str() {
                            Some("initialize") => json!({"jsonrpc":"2.0","id":request["id"],"result":{"protocolVersion":request["params"]["protocolVersion"],"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}).to_string(),
                            Some("tools/call") => { count.fetch_add(1, Ordering::SeqCst); json!({"jsonrpc":"2.0","id":request["id"],"result":{"content":[]}}).to_string() },
                            _ => String::new(),
                        }
                    };
                    let status = if body.is_empty() {
                        "202 Accepted"
                    } else {
                        "200 OK"
                    };
                    let _ = socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await;
                });
            }
        });
        let client =
            crate::mcp_auth::test_oauth_client(path.clone(), &resource, &format!("{base}/token"))
                .await;
        let native = NativeClient {
            dispatch_closed: client.1.clone(),
            ..Default::default()
        };
        let transport = StreamableHttpClientTransport::with_client(
            client,
            StreamableHttpClientTransportConfig::with_uri(resource.clone()),
        );
        let armed = Arc::new(AtomicBool::new(false));
        let latch = Arc::new((Mutex::new(false), Condvar::new()));
        let _release_on_drop = ReleaseTestLatch(latch.clone());
        let latch_expired = Arc::new(AtomicBool::new(false));
        let (blocked_tx, blocked_rx) = std::sync::mpsc::channel();
        let service = native
            .serve_with_lifecycle(
                Freeze {
                    inner: transport,
                    armed: armed.clone(),
                    blocked: blocked_tx,
                    latch: latch.clone(),
                    latch_expired: latch_expired.clone(),
                },
                ClientLifecycleMode::Initialize,
            )
            .await
            .unwrap();
        let mut envelope: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        envelope["credentials"]["token_response"]["expires_in"] = json!(1);
        envelope["credentials"]["token_received_at"] = json!(0);
        std::fs::write(&path, envelope.to_string()).unwrap();
        let config: ServerConfig = serde_json::from_value(json!({"url":resource})).unwrap();
        let connection = Arc::new(Connection {
            identity: RwLock::new(ConnectionIdentity {
                digest: "fixture".into(),
                oauth_generation: None,
            }),
            service: tokio::sync::Mutex::new(service),
            timeout: Duration::from_secs(30),
            name: "fixture".into(),
            config,
            workspace: dir.path().to_owned(),
            provider_token: None,
            http_options: None,
            closed: AtomicBool::new(false),
            page_outputs: std::sync::Mutex::new(VecDeque::new()),
        });
        let peer = connection.service.lock().await.peer().clone();
        let tool = McpTool {
            connection,
            remote_name: "mutate".into(),
            spec: ToolSpec {
                name: "mutate".into(),
                description: "fixture".into(),
                parameters: json!({"type":"object"}),
            },
            resource: false,
        };
        let cancellation = pi_rs::CancellationToken::new();
        let context = ToolContext {
            operation_id: Uuid::new_v4(),
            cancellation: cancellation.clone(),
        };
        let call = tokio::spawn(async move { tool.invoke_call(json!({}), context, None).await });
        tokio::time::timeout(Duration::from_secs(5), refresh_rx.recv())
            .await
            .unwrap()
            .unwrap();
        armed.store(true, Ordering::SeqCst);
        let notification: ClientNotification = serde_json::from_value(json!({"method":"notifications/progress","params":{"progressToken":"fixture","progress":1}})).unwrap();
        let first = {
            let peer = peer.clone();
            let notification = notification.clone();
            tokio::spawn(async move { peer.send_notification(notification).await })
        };
        tokio::task::spawn_blocking(move || {
            blocked_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        })
        .await
        .unwrap();
        let submitted = Arc::new(AtomicUsize::new(0));
        let mut flood = Vec::new();
        // The service loop is held in send(), so these exceed rmcp's
        // 1,024-entry peer queue before the tool's drop guard runs.
        for _ in 0..1200 {
            let peer = peer.clone();
            let notification = notification.clone();
            let submitted = submitted.clone();
            flood.push(tokio::spawn(async move {
                submitted.fetch_add(1, Ordering::SeqCst);
                peer.send_notification(notification).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while submitted.load(Ordering::SeqCst) < 1200 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), call)
                .await
                .unwrap()
                .unwrap(),
            Outcome::Unknown(_)
        ));
        {
            let (lock, wake) = &*latch;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
        let _ = release_tx.send(());
        tokio::time::timeout(Duration::from_secs(5), async {
            while !String::from_utf8_lossy(&std::fs::read(&path).unwrap()).contains("rotated") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            !latch_expired.load(Ordering::SeqCst),
            "transport latch expired before release"
        );
        first.abort();
        for task in flood {
            task.abort();
        }
        server.abort();
    }
    #[tokio::test]
    async fn http_proxy_is_preserved_when_transport_reconnects() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy =
            reqwest_mcp::Proxy::all(format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let initialized = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = initialized.clone();
        let server = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let count = count.clone();
                tokio::spawn(async move {
                    let mut socket = BufReader::new(socket);
                    let mut first = String::new();
                    if socket.read_line(&mut first).await.is_err() {
                        return;
                    }
                    let mut length = 0;
                    loop {
                        let mut line = String::new();
                        if socket.read_line(&mut line).await.is_err()
                            || line == "\r\n"
                            || line.is_empty()
                        {
                            break;
                        }
                        if let Some(value) =
                            line.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            length = value.trim().parse().unwrap();
                        }
                    }
                    let mut body = vec![0; length];
                    if socket.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let request: Value = serde_json::from_slice(&body).unwrap_or_default();
                    let response = if request["method"] == "initialize" {
                        assert!(first.starts_with("POST http://127.0.0.1:1/mcp "));
                        count.fetch_add(1, Ordering::SeqCst);
                        Some(json!({"jsonrpc":"2.0","id":request["id"],"result":{"protocolVersion":request["params"]["protocolVersion"],"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}).to_string())
                    } else {
                        None
                    };
                    let response = response.unwrap_or_default();
                    let status = if response.is_empty() {
                        "202 Accepted"
                    } else {
                        "200 OK"
                    };
                    let _ = socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await;
                });
            }
        });
        let config: ServerConfig =
            serde_json::from_value(json!({"url":"http://127.0.0.1:1/mcp","oauth":false})).unwrap();
        let root = tempfile::tempdir().unwrap();
        let extension = McpExtension::connect_with_http_options(
            "fixture",
            &config,
            root.path(),
            None,
            Some(HttpOptions {
                proxies: vec![proxy],
                idle_timeout: None,
            }),
        )
        .await
        .unwrap();
        extension
            .connection
            .service
            .lock()
            .await
            .close()
            .await
            .unwrap();
        drop(extension.connection.active_service().await.unwrap());
        assert_eq!(initialized.load(Ordering::SeqCst), 2);
        extension.close().await;
        server.abort();
    }
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
