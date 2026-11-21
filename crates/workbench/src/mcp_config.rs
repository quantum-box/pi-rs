//! MCP configuration overlay and command-line management.
use super::{Cli, config, output};
use clap::Subcommand;
use pi_rs::{
    CancellationToken, Error, ExtensionUi, Result, UiComponentAction, UiComponentSpan, UiRequest,
    UiResponse,
};
use pi_rs_workbench::mcp::{McpExtension, ServerConfig};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Subcommand)]
pub enum McpCommand {
    List {
        /// Print server reports as a single JSON object.
        #[arg(long)]
        json: bool,
    },
    /// Sign in to an HTTP MCP server using OAuth and a browser.
    Login {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
        /// Maximum time in seconds for OAuth sign-in.
        #[arg(long, default_value = "300")]
        timeout: f64,
        /// Pre-registered client ID. Omit for dynamic registration.
        #[arg(long)]
        client_id: Option<String>,
        /// Environment variable containing the confidential client secret.
        #[arg(long, requires = "client_id")]
        client_secret_env: Option<String>,
        /// HTTPS client metadata document used when the server supports URL-based client IDs.
        #[arg(long, conflicts_with_all = ["client_id", "client_secret_env"])]
        client_metadata_url: Option<String>,
        /// Requested OAuth scope. May be specified more than once.
        #[arg(long = "scope")]
        scopes: Vec<String>,
    },
    Logout {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
    },
    Add {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long = "header")]
        headers: Vec<String>,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        bearer_token_env_var: Option<String>,
        /// Authenticate with credentials saved by mcp login.
        #[arg(long)]
        oauth: bool,
        #[command(flatten)]
        oauth_client: Box<McpOAuthAddOptions>,
        #[arg(long, value_parser = ["direct", "deferred", "codemode", "hidden"])]
        exposure: Option<String>,
        #[arg(long = "env")]
        environment: Vec<String>,
        #[arg(last = true)]
        command: Vec<String>,
    },
    Remove {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
    },
}

#[derive(Clone, clap::Args)]
pub struct McpOAuthAddOptions {
    /// Pre-registered OAuth client ID.
    #[arg(long)]
    oauth_client_id: Option<String>,
    /// OAuth client secret or reference such as '${CLIENT_SECRET}' or '!command'.
    #[arg(long)]
    oauth_client_secret: Option<String>,
    /// Fixed loopback OAuth callback port.
    #[arg(long)]
    oauth_callback_port: Option<u16>,
    /// Client name sent during dynamic registration.
    #[arg(long)]
    oauth_client_name: Option<String>,
    /// OAuth client registration mode.
    #[arg(long, value_parser = ["auto", "cimd", "dcr"])]
    oauth_client_registration: Option<String>,
}

pub fn configurations(args: &Cli) -> Result<BTreeMap<String, Value>> {
    Ok(configurations_with_project_scope(args, ConfigurationRead::Runtime)?.0)
}

type ScopedConfigurations = (
    BTreeMap<String, Value>,
    BTreeSet<String>,
    BTreeSet<String>,
    Vec<String>,
    Vec<String>,
);

fn valid_server_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConfigurationRead {
    Runtime,
    Share,
    Report,
}

fn configurations_with_project_scope(
    args: &Cli,
    mode: ConfigurationRead,
) -> Result<ScopedConfigurations> {
    let validate_shape = mode != ConfigurationRead::Runtime;
    if args.no_mcp || !super::resource_paths::builtin_enabled(args, "builtin:mcp")? {
        return Ok((
            BTreeMap::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            Vec::new(),
            Vec::new(),
        ));
    }
    let mut configuration_errors = Vec::new();
    let user_path = config::agent_dir().join("mcp.json");
    let user = match config::read_object(&user_path) {
        Ok(user) => user,
        Err(_) if validate_shape => {
            configuration_errors.push(format!(
                "{}: could not read or parse MCP configuration",
                user_path.display()
            ));
            Default::default()
        }
        Err(error) => return Err(error),
    };
    if validate_shape
        && user
            .get("mcpServers")
            .is_some_and(|value| !value.is_object())
    {
        configuration_errors.push(format!(
            "{}: mcpServers must be an object",
            user_path.display()
        ));
    }
    let mut servers = user
        .get("mcpServers")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if mode == ConfigurationRead::Report {
        servers.retain(|name, value| {
            if !valid_server_name(name) || report_server_config(value).is_none() {
                configuration_errors.push(format!(
                    "{}: invalid MCP server configuration for {name}",
                    user_path.display()
                ));
                false
            } else {
                true
            }
        });
    }
    let mut order: Vec<_> = servers.keys().cloned().collect();
    let mut project_definitions = BTreeSet::new();
    let mut project_overrides = BTreeSet::new();
    if args.trusted {
        let project_path = args.workspace.join(".pi/mcp.json");
        let project = match config::read_object(&project_path) {
            Ok(project) => project,
            Err(_) if validate_shape => {
                configuration_errors.push(format!(
                    "{}: could not read or parse MCP configuration",
                    project_path.display()
                ));
                Default::default()
            }
            Err(error) => return Err(error),
        };
        if validate_shape
            && project
                .get("mcpServers")
                .is_some_and(|value| !value.is_object())
        {
            configuration_errors.push(format!(
                "{}: mcpServers must be an object",
                project_path.display()
            ));
        }
        if let Some(entries) = project.get("mcpServers").and_then(Value::as_object) {
            let mut entries = entries.clone();
            if mode == ConfigurationRead::Report {
                entries.retain(|name, entry| {
                    let full = ["command", "url", "type"]
                        .iter()
                        .any(|key| entry.get(*key).is_some());
                    if !valid_server_name(name)
                        || !entry.is_object()
                        || (full && report_server_config(entry).is_none())
                    {
                        configuration_errors.push(format!(
                            "{}: invalid MCP server configuration for {name}",
                            project_path.display()
                        ));
                        false
                    } else {
                        true
                    }
                });
            }
            order.extend(
                entries
                    .keys()
                    .filter(|name| !servers.contains_key(*name))
                    .cloned(),
            );
            project_definitions.extend(entries.iter().filter_map(|(name, entry)| {
                entry
                    .as_object()
                    .filter(|entry| {
                        ["command", "url", "type"]
                            .iter()
                            .any(|key| entry.contains_key(*key))
                    })
                    .map(|_| name.clone())
            }));
            project_overrides.extend(
                entries
                    .keys()
                    .filter(|name| !project_definitions.contains(*name))
                    .cloned(),
            );
            merge_project_servers(&mut servers, &entries)?;
        }
    }
    Ok((
        servers.into_iter().collect(),
        project_definitions,
        project_overrides,
        order,
        configuration_errors,
    ))
}

fn merge_project_servers(
    servers: &mut serde_json::Map<String, Value>,
    entries: &serde_json::Map<String, Value>,
) -> Result<()> {
    const OVERRIDE_KEYS: &[&str] = &["enabled", "exposure", "toolExposure", "oauth"];
    for (name, entry) in entries {
        if entry.get("auth").is_some() {
            return Err(Error::Config(format!(
                "project MCP server {name} cannot use auth.provider"
            )));
        }
        if !["command", "url", "type"]
            .iter()
            .any(|key| entry.get(*key).is_some())
            && let (Some(base), Some(overlay)) = (
                servers.get_mut(name).and_then(Value::as_object_mut),
                entry.as_object(),
            )
        {
            if let Some(key) = overlay
                .keys()
                .find(|key| !OVERRIDE_KEYS.contains(&key.as_str()))
            {
                return Err(Error::Config(format!(
                    "project MCP server {name} override has unsupported field {key}"
                )));
            }
            for (key, value) in overlay {
                if !valid_override_value(key, value) {
                    return Err(Error::Config(format!(
                        "project MCP server {name} override has invalid field {key}"
                    )));
                }
            }
            let mut candidate = base.clone();
            candidate.extend(overlay.clone());
            for key in OVERRIDE_KEYS {
                if !overlay.contains_key(*key) {
                    // Preserve serde's existing global Option semantics while
                    // validating non-null inherited values before enabling.
                    if *key == "oauth"
                        || (matches!(*key, "enabled" | "exposure")
                            && candidate.get(*key).is_some_and(Value::is_null))
                    {
                        continue;
                    }
                }
                if candidate
                    .get(*key)
                    .is_some_and(|value| !valid_override_value(key, value))
                {
                    return Err(Error::Config(format!(
                        "project MCP server {name} merged configuration has invalid field {key}"
                    )));
                }
            }
            let config: ServerConfig =
                serde_json::from_value(Value::Object(candidate)).map_err(|_| {
                    Error::Config(format!("project MCP server {name} override is invalid"))
                })?;
            if let Some(pi_rs_workbench::mcp_auth::OAuthSetting::Client(client)) = &config.oauth {
                pi_rs_workbench::mcp_auth::validate_oauth_client_settings(client).map_err(
                    |_| {
                        Error::Config(format!(
                            "project MCP server {name} override has invalid OAuth settings"
                        ))
                    },
                )?;
            }
            if matches!(
                config.oauth,
                Some(pi_rs_workbench::mcp_auth::OAuthSetting::Client(_))
                    | Some(pi_rs_workbench::mcp_auth::OAuthSetting::Enabled(true))
            ) && config.command.is_some()
            {
                return Err(Error::Config(format!(
                    "project MCP server {name} OAuth requires an HTTP server"
                )));
            }
            for (key, value) in overlay {
                base.insert(key.clone(), value.clone());
            }
            continue;
        }
        servers.insert(name.clone(), entry.clone());
    }
    Ok(())
}

#[derive(Clone)]
struct LoginSnapshot {
    server: ServerConfig,
    project_definition: bool,
    project_oauth_override: bool,
}

fn login_snapshot(
    user: &Value,
    project: Option<&Value>,
    name: &str,
    local: bool,
) -> Result<LoginSnapshot> {
    let mut servers = user
        .get("mcpServers")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut project_definition = false;
    let mut project_oauth_override = false;
    if let Some(entries) = project
        .and_then(|value| value.get("mcpServers"))
        .and_then(Value::as_object)
    {
        if let Some(entry) = entries.get(name) {
            project_definition = ["command", "url", "type"]
                .iter()
                .any(|key| entry.get(*key).is_some());
            project_oauth_override = !project_definition && entry.get("oauth").is_some();
        }
        merge_project_servers(&mut servers, entries)?;
    }
    if local && !project_definition {
        return Err(Error::Config(format!(
            "MCP server {name} is not configured in the trusted project"
        )));
    }
    let value = servers
        .get(name)
        .ok_or_else(|| Error::Config("MCP server is not configured".into()))?;
    Ok(LoginSnapshot {
        server: serde_json::from_value(value.clone())?,
        project_definition,
        project_oauth_override,
    })
}

fn same_login_auth(before: &LoginSnapshot, after: &LoginSnapshot) -> bool {
    let a = &before.server;
    let b = &after.server;
    let auth_header = |server: &ServerConfig| {
        server
            .headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    before.project_definition == after.project_definition
        && before.project_oauth_override == after.project_oauth_override
        && a.url == b.url
        && a.command == b.command
        && a.transport == b.transport
        && a.enabled == b.enabled
        && a.oauth == b.oauth
        && a.auth == b.auth
        && auth_header(a) == auth_header(b)
}

fn valid_override_value(key: &str, value: &Value) -> bool {
    match key {
        "enabled" => value.is_boolean(),
        "exposure" => valid_exposure(value),
        "toolExposure" => value
            .as_object()
            .is_some_and(|tools| tools.values().all(valid_exposure)),
        "oauth" => valid_oauth(value),
        _ => false,
    }
}

fn valid_exposure(value: &Value) -> bool {
    value.as_str().is_some_and(|value| {
        matches!(
            value,
            "direct" | "deferred" | "codemode" | "codemode-deferred" | "hidden" | "off"
        )
    })
}

fn valid_oauth(value: &Value) -> bool {
    // Serde treats null in an optional member as absent, but Pi validates
    // explicitly supplied OAuth fields and rejects null values.
    if value
        .as_object()
        .is_some_and(|fields| fields.values().any(Value::is_null))
    {
        return false;
    }
    valid_inherited_oauth(value)
}

fn valid_inherited_oauth(value: &Value) -> bool {
    use pi_rs_workbench::mcp_auth::{OAuthSetting, validate_oauth_client_settings};
    match serde_json::from_value::<OAuthSetting>(value.clone()) {
        Ok(OAuthSetting::Enabled(_)) => true,
        Ok(OAuthSetting::Client(client)) => validate_oauth_client_settings(&client).is_ok(),
        Err(_) => false,
    }
}

pub fn auto_enable_codemode(args: &Cli) -> Result<bool> {
    let mut value = true;
    let mut paths = vec![config::agent_dir().join("mcp.json")];
    if args.trusted {
        paths.push(args.workspace.join(".pi/mcp.json"));
    }
    for path in paths {
        if let Some(setting) = config::read_object(&path)?.get("autoEnableCodemode") {
            value = setting
                .as_bool()
                .ok_or_else(|| Error::Config("autoEnableCodemode must be a boolean".into()))?;
        }
    }
    Ok(value)
}

pub async fn connect_all(args: &Cli, report: bool) -> Result<Vec<Arc<McpExtension>>> {
    Ok(discover(args, report, false, None).await?.connected)
}

#[derive(Default)]
pub(super) struct Discovery {
    pub connected: Vec<Arc<McpExtension>>,
    pub sign_in: Vec<PendingSignIn>,
    pub skipped_disabled: BTreeSet<String>,
    pub inspection: Vec<Inspection>,
}

#[derive(Clone)]
pub(super) struct PendingSignIn {
    pub name: String,
    pub config: ServerConfig,
    pub challenge: Option<String>,
}

pub(super) async fn discover(
    args: &Cli,
    report: bool,
    interactive: bool,
    share_services: Option<&super::runtime::ShareServices<'_>>,
) -> Result<Discovery> {
    let require_complete = share_services.is_some();
    let mode = if require_complete {
        ConfigurationRead::Share
    } else {
        ConfigurationRead::Runtime
    };
    let (servers, project, overrides, _, configuration_errors) =
        configurations_with_project_scope(args, mode)?;
    if !configuration_errors.is_empty() {
        return Err(Error::Config(format!(
            "Invalid MCP configuration for Radius share: {}",
            configuration_errors.join("\n"),
        )));
    }
    let mut discovery = Discovery {
        inspection: inspection_entries(&servers, &project, &overrides),
        ..Discovery::default()
    };
    let mut failed = false;
    let mut normalized = BTreeSet::new();
    let mut pending = Vec::new();
    for (name, value) in servers {
        let normalized_name = name.replace('-', "_");
        if require_complete {
            let id = format!("mcp-{normalized_name}");
            if args.disable_extension.contains(&id) {
                discovery.skipped_disabled.insert(id);
                continue;
            }
            if value["enabled"] == false {
                continue;
            }
            if !valid_server_name(&name) || report_server_config(&value).is_none() {
                failed = true;
                eprintln!("MCP {name}: invalid configuration");
                continue;
            }
        }
        if !normalized.insert(normalized_name) {
            failed = true;
            if report {
                output(
                    json!({"type":"mcp_server","name":name,"status":"invalid","error":"server name collides after normalization"}),
                );
            } else {
                eprintln!("MCP {name}: server name collides after normalization");
            }
            continue;
        }
        let config = match serde_json::from_value::<ServerConfig>(value) {
            Ok(config) => config,
            Err(error) => {
                failed = true;
                if report {
                    output(
                        json!({"type":"mcp_server","name":name,"status":"invalid","error":error.to_string()}),
                    );
                } else {
                    eprintln!("MCP {name}: invalid configuration");
                }
                continue;
            }
        };
        if config.enabled == Some(false) {
            if report {
                output(json!({"type":"mcp_server","name":name,"status":"disabled"}));
            }
            continue;
        }
        let args = args.clone();
        pending.push(async move {
            let result = connect_for_discovery(&args, &name, &config, share_services).await;
            (name, config, result)
        });
    }
    for (name, config, result) in futures_util::future::join_all(pending).await {
        match result {
            Ok(extension) => {
                if report {
                    output(
                        json!({"type":"mcp_server","name":name,"status":"connected","tools":extension.tool_count(),"description":extension.description}),
                    );
                }
                discovery.connected.push(Arc::new(extension));
            }
            Err(Error::AuthorizationRequired { challenge, .. }) if interactive => {
                discovery.sign_in.push(PendingSignIn {
                    name,
                    config,
                    challenge,
                });
            }
            Err(error) => {
                failed = true;
                if report {
                    output(
                        json!({"type":"mcp_server","name":name,"status":"error","error":error.to_string()}),
                    );
                } else {
                    eprintln!("MCP {name}: {error}");
                }
            }
        }
    }
    if require_complete && failed {
        futures_util::future::join_all(discovery.connected.iter().map(|server| server.close()))
            .await;
        return Err(Error::Config(
            "MCP discovery failed while preparing Radius share".into(),
        ));
    }
    Ok(discovery)
}

async fn connect(args: &Cli, name: &str, server: &ServerConfig) -> Result<McpExtension> {
    connect_for_discovery(args, name, server, None).await
}

async fn connect_for_discovery(
    args: &Cli,
    name: &str,
    server: &ServerConfig,
    share_services: Option<&super::runtime::ShareServices<'_>>,
) -> Result<McpExtension> {
    use pi_rs_workbench::mcp::TokenResolver;
    let resolver: Option<TokenResolver> = server.auth.as_ref().map(|auth| {
        let args = args.clone();
        let provider = auth.provider.clone();
        let share_refresh = share_services.map(|services| services.refresh.clone());
        let cleanup = share_services.map(|services| services.processes.clone());
        Arc::new(move || {
            let mut args = args.clone();
            let provider = provider.clone();
            let share_refresh = share_refresh.clone();
            let cleanup = cleanup.clone();
            Box::pin(async move {
                args.model_profiles = super::model_profiles::Profiles::load()?;
                args.model_keys = super::model_auth::Keys::load_async().await?;
                let provider = args.model_profiles.provider(&provider).ok_or_else(|| {
                    Error::Config("MCP auth.provider does not name an installed provider".into())
                })?;
                let endpoint = if args.endpoint_explicit {
                    args.endpoint_for(&provider)
                } else {
                    args.model_profiles.endpoint(&provider)
                };
                let token = match args.model_credential_for(&provider, endpoint) {
                    Some(source) => Some(source.resolve_with_cleanup(cleanup.as_ref()).await?),
                    None => None,
                };
                if token.is_some() || provider != super::Provider::Radius {
                    return Ok(token);
                }
                let gateway =
                    super::radius_auth::Gateway::configured_with_settings(&args.settings)?;
                if gateway.model_endpoint().as_str() != endpoint {
                    return Ok(None);
                }
                let min_validity = std::time::Duration::from_secs(60);
                match share_refresh {
                    Some(refresh) => refresh.access(gateway, min_validity).await,
                    None => gateway.access(min_validity).await,
                }
            }) as futures_util::future::BoxFuture<'static, Result<Option<String>>>
        }) as TokenResolver
    });
    McpExtension::connect_with_owned_stdio(
        name,
        server,
        &args.workspace,
        resolver,
        Some(super::http_proxy::configured_mcp(&args.settings)?),
        share_services.map(|services| &services.processes),
    )
    .await
}

fn report_exposure(value: &str) -> &str {
    match value {
        "codemode-deferred" => "codemode",
        "off" => "hidden",
        other => other,
    }
}

fn report_server_config(value: &Value) -> Option<ServerConfig> {
    let config: ServerConfig = serde_json::from_value(value.clone()).ok()?;
    if config.command.is_some() == config.url.is_some()
        || config
            .exposure
            .iter()
            .chain(config.tool_exposure.values())
            .any(|value| !valid_exposure(&json!(value)))
        || config.oauth_client().is_some_and(|client| {
            pi_rs_workbench::mcp_auth::validate_oauth_client_settings(client).is_err()
        })
        || config.command.is_some()
            && matches!(
                &config.oauth,
                Some(
                    pi_rs_workbench::mcp_auth::OAuthSetting::Enabled(true)
                        | pi_rs_workbench::mcp_auth::OAuthSetting::Client(_)
                )
            )
        || config.command.is_some()
            && config
                .transport
                .as_deref()
                .is_some_and(|kind| kind != "stdio")
        || config.command.is_none()
            && config.url.is_some()
            && config
                .transport
                .as_deref()
                .is_some_and(|kind| !matches!(kind, "http" | "streamable-http"))
        || config.url.as_ref().is_some_and(|url| {
            reqwest_mcp::Url::parse(url)
                .ok()
                .is_none_or(|url| !matches!(url.scheme(), "http" | "https"))
        })
    {
        return None;
    }
    Some(config)
}

async fn list_json(args: &Cli) -> Result<()> {
    use std::io::Write;

    let mut errors = Vec::new();
    let (servers, project_definitions, project_overrides, order, config_errors) =
        match configurations_with_project_scope(args, ConfigurationRead::Report) {
            Ok(configurations) => configurations,
            Err(error) => {
                errors.push(error.to_string());
                (
                    BTreeMap::new(),
                    BTreeSet::new(),
                    BTreeSet::new(),
                    Vec::new(),
                    Vec::new(),
                )
            }
        };
    errors.extend(config_errors);
    let global_path = config::agent_dir().join("mcp.json");
    let project_path = std::fs::canonicalize(&args.workspace)
        .unwrap_or_else(|_| args.workspace.clone())
        .join(".pi/mcp.json");
    let mut normalized = BTreeSet::new();
    let mut pending = Vec::new();
    let mut servers = servers;
    for name in order {
        let Some(value) = servers.remove(&name) else {
            continue;
        };
        let project = project_definitions.contains(&name);
        let source = if project { &project_path } else { &global_path };
        if !valid_server_name(&name) {
            errors.push(format!(
                "{}: invalid MCP server name {name}",
                source.display()
            ));
            continue;
        }
        let config = match report_server_config(&value) {
            Some(config) => config,
            None => {
                errors.push(format!(
                    "{}: invalid MCP server configuration for {name}",
                    source.display()
                ));
                continue;
            }
        };
        let transport = if let Some(command) = &config.command {
            if config.args.is_empty() {
                command.clone()
            } else {
                format!("{command} [arguments omitted]")
            }
        } else if let Some(url) = &config.url {
            let Ok(mut parsed) = reqwest_mcp::Url::parse(url) else {
                errors.push(format!("invalid MCP URL for {name}"));
                continue;
            };
            if !matches!(parsed.scheme(), "http" | "https") {
                errors.push(format!("invalid MCP URL for {name}"));
                continue;
            }
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            format!("{}/[path omitted]", parsed.origin().ascii_serialization())
        } else {
            errors.push(format!("MCP server {name} requires a command or URL"));
            continue;
        };
        if !normalized.insert(name.replace('-', "_")) {
            errors.push(format!(
                "{}: MCP server {name} collides after normalization",
                source.display()
            ));
            continue;
        }
        let enabled = config.enabled != Some(false);
        let base_exposure =
            report_exposure(config.exposure.as_deref().unwrap_or("codemode")).to_owned();
        let mut report = json!({"name":name,"scope":if project {"project"} else {"global"},"source":source.to_string_lossy(),"enabled":enabled,"exposure":base_exposure,"transport":transport,"state":"disabled","tools":[]});
        if project_overrides.contains(&name) {
            report["override"] = json!(project_path.to_string_lossy());
        }
        let args = args.clone();
        pending.push(async move {
            if !enabled {
                return (report, false);
            }
            match connect(&args, &name, &config).await {
                Ok(connection) => {
                    let tools = connection.remote_tool_names();
                    report["state"] = json!("connected");
                    report["tools"] = json!(tools);
                    let changes: serde_json::Map<String, Value> = tools
                        .iter()
                        .filter(|tool| {
                            report_exposure(pi_rs_workbench::mcp::tool_exposure(&config, tool))
                                != base_exposure
                        })
                        .map(|tool| {
                            let exposure =
                                report_exposure(pi_rs_workbench::mcp::tool_exposure(&config, tool));
                            (tool.clone(), json!(exposure))
                        })
                        .collect();
                    if !changes.is_empty() {
                        report["toolExposure"] = Value::Object(changes);
                    }
                    if connection.offers_resources() {
                        report["resources"] = json!(true);
                    }
                    connection.close().await;
                    (report, false)
                }
                Err(error) => {
                    report["state"] =
                        json!(if matches!(&error, Error::AuthorizationRequired { .. }) {
                            "needs-auth"
                        } else {
                            "failed"
                        });
                    // Transport errors can include URLs or server-supplied text.
                    // Avoid exposing configured credentials in machine-readable output.
                    report["error"] =
                        json!(if matches!(&error, Error::AuthorizationRequired { .. }) {
                            "MCP server requires sign-in"
                        } else {
                            "MCP server connection failed"
                        });
                    (report, true)
                }
            }
        });
    }
    let reports = futures_util::future::join_all(pending).await;
    let failed = !errors.is_empty() || reports.iter().any(|(_, failed)| *failed);
    let mut result = json!({"servers":reports.into_iter().map(|(report, _)| report).collect::<Vec<_>>(),"errors":errors});
    if !args.trusted && project_path.exists() {
        result["note"] = json!(format!(
            "{} is ignored because the project is not trusted.",
            project_path.display()
        ));
    }
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, &result)?;
    writeln!(stdout)?;
    if failed {
        return Err(Error::Config(
            "one or more MCP servers failed to connect".into(),
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub(super) struct Inspection {
    name: String,
    status: &'static str,
    exposure: String,
    scope: &'static str,
    description: String,
    tools: Vec<String>,
}

fn inspection_entries(
    servers: &BTreeMap<String, Value>,
    project: &BTreeSet<String>,
    overrides: &BTreeSet<String>,
) -> Vec<Inspection> {
    servers
        .iter()
        .map(|(name, config)| {
            let status = if report_server_config(config).is_none() {
                "invalid"
            } else if config["enabled"] == false {
                "disabled"
            } else {
                "disconnected"
            };
            let scope = if project.contains(name) {
                "project"
            } else if overrides.contains(name) {
                "global · project override"
            } else {
                "global"
            };
            Inspection {
                name: name.clone(),
                status,
                exposure: report_exposure(config["exposure"].as_str().unwrap_or("codemode")).into(),
                scope,
                description: inspection_description(
                    config["description"].as_str().unwrap_or_default(),
                ),
                tools: Vec::new(),
            }
        })
        .collect()
}

fn inspection_description(description: &str) -> String {
    description
        .lines()
        .next()
        .unwrap_or_default()
        .chars()
        .take(512)
        .collect()
}

pub(super) async fn inspect_servers(
    configured: &[Inspection],
    connected: &[Arc<McpExtension>],
    ui: &dyn ExtensionUi,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut entries = configured.to_vec();
    for entry in &mut entries {
        if let Some(server) = connected
            .iter()
            .find(|server| server.server_name == entry.name)
        {
            entry.status = "connected";
            entry.description = inspection_description(&server.description);
            entry.tools = server
                .remote_tool_names()
                .iter()
                .map(|tool| {
                    format!(
                        "{tool} · {}",
                        report_exposure(server.remote_tool_exposure(tool))
                    )
                })
                .collect();
        }
    }
    // Put disconnected and invalid entries first, as the Pi manager does.
    entries.sort_by_key(|entry| !matches!(entry.status, "disconnected" | "invalid"));
    let options = entries
        .iter()
        .map(|entry| {
            let count = entry.tools.len();
            let tool_label = if count == 1 { "tool" } else { "tools" };
            format!(
                "{} · {} · {count} {tool_label} · {} · {}",
                entry.name, entry.status, entry.exposure, entry.scope,
            )
        })
        .collect::<Vec<_>>();
    if options.is_empty() {
        let _ = show_inspection(
            "MCP servers".into(),
            vec!["No MCP servers configured.".into()],
            false,
            ui,
            cancel,
        )
        .await?;
        return Ok(());
    }
    let mut page = 0;
    while let Some(index) =
        select_inspection("MCP servers", &options, &mut page, ui, cancel).await?
    {
        let entry = &entries[index];
        let mut lines = vec![
            format!("Status · {}", entry.status),
            format!("Scope · {}", entry.scope),
            format!("Exposure · {}", entry.exposure),
            format!("Tools · {}", entry.tools.len()),
        ];
        if !entry.description.is_empty() {
            lines.push(entry.description.clone());
        }
        while show_inspection(
            format!("MCP server · {}", entry.name),
            lines.clone(),
            !entry.tools.is_empty(),
            ui,
            cancel,
        )
        .await?
        {
            let title = format!("MCP tools · {}", entry.name);
            let mut tool_page = 0;
            let _ = select_inspection(&title, &entry.tools, &mut tool_page, ui, cancel).await?;
        }
    }
    Ok(())
}

async fn show_inspection(
    title: String,
    lines: Vec<String>,
    tools: bool,
    ui: &dyn ExtensionUi,
    cancel: &CancellationToken,
) -> Result<bool> {
    let mut actions = vec![UiComponentAction {
        key: "enter".into(),
        value: "back".into(),
        label: Some("Back".into()),
    }];
    if tools {
        actions.push(UiComponentAction {
            key: "t".into(),
            value: "tools".into(),
            label: Some("Tools".into()),
        });
    }
    let request = UiRequest::Custom {
        title,
        lines: lines
            .into_iter()
            .map(|text| {
                vec![UiComponentSpan {
                    text,
                    ..Default::default()
                }]
            })
            .collect(),
        actions,
        timeout: None,
    };
    let response = ui
        .request(request, cancel.clone())
        .await
        .map_err(|error| Error::Config(format!("MCP inspector UI failed: {error}")))?;
    Ok(matches!(response, UiResponse::Text(action) if action == "tools"))
}

async fn select_inspection(
    title: &str,
    items: &[String],
    page: &mut usize,
    ui: &dyn ExtensionUi,
    cancel: &CancellationToken,
) -> Result<Option<usize>> {
    // Leave room for both pagination actions in the native dialog.
    const PAGE_SIZE: usize = 126;
    const PREVIOUS: &str = "← Previous page";
    const NEXT: &str = "Next page →";
    loop {
        let start = *page * PAGE_SIZE;
        let end = (start + PAGE_SIZE).min(items.len());
        let mut options = items[start..end].to_vec();
        if *page > 0 {
            options.push(PREVIOUS.into());
        }
        if end < items.len() {
            options.push(NEXT.into());
        }
        let response = ui
            .request(
                UiRequest::Select {
                    title: title.into(),
                    options,
                    timeout: None,
                },
                cancel.clone(),
            )
            .await
            .map_err(|error| Error::Config(format!("MCP inspector UI failed: {error}")))?;
        match response {
            UiResponse::Text(value) if value == PREVIOUS && *page > 0 => *page -= 1,
            UiResponse::Text(value) if value == NEXT && end < items.len() => *page += 1,
            UiResponse::Text(value) => {
                return Ok(items[start..end]
                    .iter()
                    .position(|item| item == &value)
                    .map(|index| start + index));
            }
            _ => return Ok(None),
        }
    }
}

pub async fn command(args: &Cli, command: McpCommand) -> Result<()> {
    match command {
        McpCommand::List { json: true } => {
            return list_json(args).await;
        }
        McpCommand::List { json: false } => {
            let expected = configurations(args)?
                .values()
                .filter(|value| value["enabled"] != false)
                .count();
            let connected = connect_all(args, true).await?;
            let count = connected.len();
            for connection in connected {
                connection.close().await;
            }
            if count != expected {
                return Err(Error::Config(
                    "one or more MCP servers failed to connect".into(),
                ));
            }
        }
        McpCommand::Add {
            name,
            local,
            url,
            cwd,
            headers,
            description,
            bearer_token_env_var,
            oauth,
            oauth_client,
            exposure,
            environment,
            command,
        } => {
            let McpOAuthAddOptions {
                oauth_client_id,
                oauth_client_secret,
                oauth_callback_port,
                oauth_client_name,
                oauth_client_registration,
            } = *oauth_client;
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
            {
                return Err(Error::Config("invalid MCP server name".into()));
            }
            if url.is_some() != command.is_empty() {
                return Err(Error::Config(
                    "choose either --url or a command after --".into(),
                ));
            }
            let mut value = if let Some(url) = url {
                json!({"url":url})
            } else {
                json!({"command":command[0],"args":command[1..]})
            };
            if let Some(cwd) = cwd {
                if value.get("command").is_none() {
                    return Err(Error::Config("--cwd requires a stdio server".into()));
                }
                value["cwd"] = json!(cwd);
            }
            if let Some(description) = description {
                value["description"] = json!(description);
            }
            if !headers.is_empty() {
                if value.get("url").is_none() {
                    return Err(Error::Config("--header requires an HTTP server".into()));
                }
                let mut map = serde_json::Map::new();
                for item in headers {
                    let (name, header_value) = item
                        .split_once('=')
                        .filter(|(name, _)| !name.is_empty())
                        .ok_or_else(|| Error::Config("--header expects NAME=VALUE".into()))?;
                    let name = http::header::HeaderName::from_bytes(name.as_bytes())
                        .map_err(|_| Error::Config("invalid MCP header name".into()))?;
                    http::header::HeaderValue::from_str(header_value)
                        .map_err(|_| Error::Config("invalid MCP header value".into()))?;
                    map.insert(name.as_str().into(), json!(header_value));
                }
                value["headers"] = Value::Object(map);
            }
            let mut oauth_options = serde_json::Map::new();
            for (key, option) in [
                ("clientId", oauth_client_id),
                ("clientSecret", oauth_client_secret),
                ("clientName", oauth_client_name),
                ("clientRegistration", oauth_client_registration),
            ] {
                if let Some(option) = option {
                    oauth_options.insert(key.into(), json!(option));
                }
            }
            if let Some(port) = oauth_callback_port {
                if port == 0 {
                    return Err(Error::Config(
                        "OAuth callback port must be between 1 and 65535".into(),
                    ));
                }
                oauth_options.insert("callbackPort".into(), json!(port));
            }
            if oauth || !oauth_options.is_empty() {
                if value.get("url").is_none()
                    || bearer_token_env_var.is_some()
                    || value["headers"].as_object().is_some_and(|headers| {
                        headers
                            .keys()
                            .any(|name| name.eq_ignore_ascii_case("authorization"))
                    })
                {
                    return Err(Error::Config(
                        "--oauth requires --url and cannot use --bearer-token-env-var".into(),
                    ));
                }
                if oauth_options.is_empty() {
                    value["oauth"] = json!(true);
                } else {
                    let options = Value::Object(oauth_options);
                    if !valid_oauth(&options) {
                        return Err(Error::Config("invalid MCP OAuth client settings".into()));
                    }
                    value["oauth"] = options;
                }
            }
            if let Some(exposure) = exposure {
                value["exposure"] = json!(exposure);
            }
            if let Some(variable) = bearer_token_env_var {
                if value.get("url").is_none() {
                    return Err(Error::Config(
                        "--bearer-token-env-var requires an HTTP server".into(),
                    ));
                }
                if !variable
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    || variable.is_empty()
                {
                    return Err(Error::Config("invalid token environment variable".into()));
                }
                let headers = value
                    .as_object_mut()
                    .unwrap()
                    .entry("headers")
                    .or_insert_with(|| json!({}));
                let headers = headers.as_object_mut().unwrap();
                if headers
                    .keys()
                    .any(|name| name.eq_ignore_ascii_case("authorization"))
                {
                    return Err(Error::Config(
                        "--bearer-token-env-var cannot be combined with an Authorization header"
                            .into(),
                    ));
                }
                headers.insert(
                    "Authorization".into(),
                    json!(format!("Bearer ${{{variable}}}")),
                );
            }
            if !environment.is_empty() {
                if value.get("command").is_none() {
                    return Err(Error::Config("--env requires a stdio server".into()));
                }
                let mut env = serde_json::Map::new();
                for item in environment {
                    let (name, value) =
                        item.split_once('=')
                            .filter(|(name, _)| !name.is_empty())
                            .ok_or_else(|| Error::Config("--env expects NAME=VALUE".into()))?;
                    env.insert(name.into(), json!(value));
                }
                value["env"] = Value::Object(env);
            }
            let path = path(args, local);
            config::update_object(&path, |document| {
                let entries = document
                    .entry("mcpServers")
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .ok_or_else(|| Error::Config("mcpServers must be an object".into()))?;
                entries.insert(name.clone(), value);
                Ok(())
            })?;
            output(json!({"type":"mcp_saved","name":name,"path":path}));
        }
        McpCommand::Login {
            name,
            local,
            timeout,
            client_id,
            client_secret_env,
            client_metadata_url,
            scopes,
        } => {
            let timeout = std::time::Duration::try_from_secs_f64(timeout)
                .ok()
                .filter(|duration| !duration.is_zero())
                .ok_or_else(|| {
                    Error::Config("--timeout must be a positive number of seconds".into())
                })?;
            let deadline = tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| Error::Config("--timeout is too large".into()))?;
            let user_path = path(args, false);
            let project_path = args.trusted.then(|| path(args, true));
            let read_snapshot = move |user_path: &std::path::Path,
                                      project_path: Option<&std::path::Path>,
                                      name: &str| {
                let user = config::read_object(user_path)?;
                let project = project_path.map(config::read_object).transpose()?;
                login_snapshot(&user, project.as_ref(), name, local)
            };
            let snapshot = read_snapshot(&user_path, project_path.as_deref(), &name)?;
            let server = &snapshot.server;
            if let Some(auth) = &server.auth {
                return Err(Error::Config(format!(
                    "MCP {name} uses credentials from provider {}; sign in to that provider",
                    auth.provider
                )));
            }
            let url = server
                .url
                .as_deref()
                .filter(|_| server.command.is_none())
                .ok_or_else(|| Error::Config("MCP OAuth requires an HTTP server".into()))?;
            if server
                .headers
                .keys()
                .any(|name| name.eq_ignore_ascii_case("authorization"))
            {
                return Err(Error::Config(
                    "remove the MCP Authorization header before using OAuth".into(),
                ));
            }
            if matches!(
                server.oauth,
                Some(pi_rs_workbench::mcp_auth::OAuthSetting::Enabled(false))
            ) {
                return Err(Error::Config(
                    "MCP OAuth is disabled in the effective configuration; enable it before signing in"
                        .into(),
                ));
            }
            if server
                .transport
                .as_deref()
                .is_some_and(|kind| !matches!(kind, "http" | "streamable-http"))
            {
                return Err(Error::Config("MCP OAuth requires streamable HTTP".into()));
            }
            let client_secret = client_secret_env
                .map(|name| {
                    std::env::var(name).map_err(|_| {
                        Error::Config("OAuth client secret environment variable is not set".into())
                    })
                })
                .transpose()?;
            let announce =
                |url: &str| output(json!({"type":"mcp_authorization","name":name,"url":url}));
            let http_options = super::http_proxy::configured_mcp(&args.settings)?;
            use pi_rs_workbench::mcp_auth::{CliLoginClient, login_cli_with_precommit};
            let client = if let Some(metadata_url) = client_metadata_url.as_deref() {
                CliLoginClient::Metadata(metadata_url)
            } else if client_id.is_none() && server.oauth_client().is_some() {
                CliLoginClient::Configured(server.oauth_client())
            } else {
                CliLoginClient::Secret {
                    client_id: client_id.as_deref(),
                    client_secret: client_secret.as_deref(),
                }
            };
            let config_paths = std::iter::once(user_path.clone())
                .chain(project_path.iter().cloned())
                .collect();
            let finish_name = name.clone();
            let finish_snapshot = snapshot.clone();
            login_cli_with_precommit(
                &name,
                url,
                client,
                &scopes,
                Some(&http_options),
                deadline,
                &args.command_cancel,
                announce,
                config_paths,
                move || {
                    let changed = || {
                        Error::Config(
                            "MCP authentication configuration changed during login".into(),
                        )
                    };
                    let current = read_snapshot(&user_path, project_path.as_deref(), &finish_name)
                        .map_err(|_| changed())?;
                    if !same_login_auth(&finish_snapshot, &current) {
                        return Err(changed());
                    }
                    Ok(())
                },
            )
            .await?;
            output(json!({"type":"mcp_logged_in","name":name}));
        }
        McpCommand::Logout { name, local } => {
            let (servers, project_definitions, _, _, _) =
                configurations_with_project_scope(args, ConfigurationRead::Runtime)?;
            if local && !project_definitions.contains(&name) {
                return Err(Error::Config(format!(
                    "MCP server {name} is not configured in the trusted project"
                )));
            }
            let value = servers
                .get(&name)
                .ok_or_else(|| Error::Config("MCP server is not configured".into()))?;
            for key in ["enabled", "exposure", "toolExposure", "oauth"] {
                if !project_definitions.contains(&name)
                    && matches!(key, "enabled" | "exposure" | "oauth")
                    && value.get(key).is_some_and(Value::is_null)
                {
                    continue;
                }
                if key == "oauth"
                    && !project_definitions.contains(&name)
                    && value.get(key).is_some_and(valid_inherited_oauth)
                {
                    continue;
                }
                if value
                    .get(key)
                    .is_some_and(|field| !valid_override_value(key, field))
                {
                    return Err(Error::Config(format!("invalid MCP server field {key}")));
                }
            }
            let inherited_global = !project_definitions.contains(&name);
            if value
                .get("description")
                .is_some_and(|field| !(field.is_string() || inherited_global && field.is_null()))
                || value.get("timeout").is_some_and(|field| {
                    !(inherited_global && field.is_null()) && field.as_u64().is_none()
                })
            {
                return Err(Error::Config("invalid MCP server configuration".into()));
            }
            let server: ServerConfig = serde_json::from_value(value.clone())?;
            if let Some(auth) = &server.auth {
                return Err(Error::Config(format!(
                    "MCP {name} uses credentials from provider {}; sign out of that provider",
                    auth.provider
                )));
            }
            if server
                .headers
                .keys()
                .any(|header| header.eq_ignore_ascii_case("authorization"))
            {
                return Err(Error::Config(
                    "MCP OAuth cannot be combined with an Authorization header".into(),
                ));
            }
            if server.command.is_some()
                || server
                    .transport
                    .as_deref()
                    .is_some_and(|kind| !matches!(kind, "http" | "streamable-http"))
            {
                return Err(Error::Config("MCP OAuth requires an HTTP server".into()));
            }
            let url = server
                .url
                .as_deref()
                .ok_or_else(|| Error::Config("MCP OAuth requires an HTTP server".into()))?;
            let parsed = reqwest_mcp::Url::parse(url)
                .map_err(|_| Error::Config("invalid MCP URL".into()))?;
            if !matches!(parsed.scheme(), "http" | "https") {
                return Err(Error::Config("MCP OAuth requires an HTTP server".into()));
            }
            pi_rs_workbench::mcp_auth::logout(&name, url).await?;
            output(json!({"type":"mcp_logged_out","name":name}));
        }
        McpCommand::Remove { name, local } => {
            let path = path(args, local);
            config::update_object(&path, |document| {
                document
                    .get_mut("mcpServers")
                    .and_then(Value::as_object_mut)
                    .and_then(|servers| servers.remove(&name))
                    .ok_or_else(|| {
                        Error::Config("MCP server not found in this configuration".into())
                    })?;
                Ok(())
            })?;
            output(json!({"type":"mcp_removed","name":name,"path":path}));
        }
    }
    Ok(())
}
fn path(args: &Cli, local: bool) -> PathBuf {
    if local {
        args.workspace.join(".pi/mcp.json")
    } else {
        config::agent_dir().join("mcp.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_override_rejects_unknown_fields_before_changing_global_server() {
        for extra in ["headers", "args", "url_typo", "description"] {
            let original = json!({"service":{"url":"https://example.invalid/mcp","enabled":false,"headers":{"Authorization":"Bearer ${TOKEN}"}}});
            let mut servers = original.as_object().unwrap().clone();
            let mut overlay = json!({"service":{"enabled":true}});
            overlay["service"][extra] = json!("unexpected");
            let error =
                merge_project_servers(&mut servers, overlay.as_object().unwrap()).unwrap_err();
            assert!(error.to_string().contains(extra));
            assert_eq!(Value::Object(servers), original);
        }
    }

    #[test]
    fn project_override_retains_native_oauth_and_global_credentials() {
        let mut servers = json!({"service":{"url":"https://example.invalid/mcp","headers":{"Authorization":"Bearer ${TOKEN}"},"enabled":false}}).as_object().unwrap().clone();
        let project = json!({"service":{"enabled":true,"exposure":"deferred","toolExposure":{"read":"hidden"},"oauth":false}});
        merge_project_servers(&mut servers, project.as_object().unwrap()).unwrap();
        assert_eq!(servers["service"]["enabled"], true);
        assert_eq!(servers["service"]["oauth"], false);
        assert_eq!(
            servers["service"]["headers"]["Authorization"],
            "Bearer ${TOKEN}"
        );
        let before = servers.clone();
        let forbidden = json!({"service":{"enabled":true,"auth":{"provider":"openai"}}});
        assert!(merge_project_servers(&mut servers, forbidden.as_object().unwrap()).is_err());
        assert_eq!(servers, before);
    }

    #[test]
    fn project_override_rejects_invalid_values_before_enabling_server() {
        for invalid in [
            json!({"enabled": null}),
            json!({"enabled": "true"}),
            json!({"enabled": true, "exposure": null}),
            json!({"enabled": true, "exposure": "unknown"}),
            json!({"enabled": true, "toolExposure": {"read": "unknown"}}),
            json!({"enabled": true, "oauth": null}),
            json!({"enabled": true, "oauth": {"callbackPort": null}}),
            json!({"enabled": true, "oauth": {"clientId": null}}),
            json!({"enabled": true, "oauth": {"clientRegistration": "unknown"}}),
            json!({"enabled": true, "oauth": {"clientSecret": "secret"}}),
            json!({"enabled": true, "oauth": {"clientId": " "}}),
            json!({"enabled": true, "oauth": {"clientName": ""}}),
            json!({"enabled": true, "oauth": {"clientRegistration": "cimd", "clientId": "id"}}),
            json!({"enabled": true, "oauth": {"callbackPort": 0}}),
            json!({"enabled": true, "oauth": {"callbackUrl": "http://example.com/callback"}}),
            json!({"enabled": true, "oauth": {"callbackUrl": "http://127.0.0.1:9000/callback", "callbackPort": 9001}}),
            json!({"enabled": true, "oauth": {"clientMetadataUrl": "http://example.com/client.json"}}),
        ] {
            let original = json!({"service":{"url":"https://example.invalid/mcp","enabled":false}});
            let mut servers = original.as_object().unwrap().clone();
            let overlay = json!({"service": invalid});
            assert!(merge_project_servers(&mut servers, overlay.as_object().unwrap()).is_err());
            assert_eq!(Value::Object(servers), original);
        }
        let mut servers = json!({"service":{"url":"https://example.invalid/mcp","enabled":false}})
            .as_object()
            .unwrap()
            .clone();
        let valid = json!({"service":{"enabled":true,"oauth":{"callbackUrl":"http://127.0.0.1:9000/callback","callbackPort":9000,"clientMetadataUrl":"https://example.com/client.json"}}});
        merge_project_servers(&mut servers, valid.as_object().unwrap()).unwrap();
        assert_eq!(servers["service"]["enabled"], true);
        let original = json!({"service":{"command":"fixture","enabled":false}});
        let mut servers = original.as_object().unwrap().clone();
        let invalid = json!({"service":{"enabled":true,"oauth":true}});
        assert!(merge_project_servers(&mut servers, invalid.as_object().unwrap()).is_err());
        assert_eq!(Value::Object(servers), original);
        let enabled = json!({"service":{"enabled":true}});
        for inherited in [
            json!(null),
            json!({"callbackUrl": null, "callbackPort": null}),
        ] {
            let original = json!({"service":{"url":"https://example.invalid/mcp","enabled":false,"oauth":inherited}});
            let mut servers = original.as_object().unwrap().clone();
            merge_project_servers(&mut servers, enabled.as_object().unwrap()).unwrap();
            assert_eq!(servers["service"]["enabled"], true);
            assert_eq!(servers["service"]["oauth"], original["service"]["oauth"]);
        }
        let original = json!({"service":{"url":"https://example.invalid/mcp","enabled":false,"oauth":{"callbackPort":0}}});
        let mut servers = original.as_object().unwrap().clone();
        assert!(merge_project_servers(&mut servers, enabled.as_object().unwrap()).is_err());
        assert_eq!(Value::Object(servers), original);
        for (key, value) in [
            ("exposure", json!("bad")),
            ("toolExposure", json!(null)),
            ("toolExposure", json!({"read": null})),
        ] {
            let mut original =
                json!({"service":{"url":"https://example.invalid/mcp","enabled":false}});
            original["service"][key] = value;
            let mut servers = original.as_object().unwrap().clone();
            let error =
                merge_project_servers(&mut servers, enabled.as_object().unwrap()).unwrap_err();
            assert!(error.to_string().contains(key));
            assert_eq!(Value::Object(servers), original);
        }
        let original = json!({"service":{"url":"https://example.invalid/mcp","enabled":false,"oauth":{"callbackUrl":"http://127.0.0.1:80/callback","callbackPort":9001,"clientId":"sensitive-id"}}});
        let mut servers = original.as_object().unwrap().clone();
        let error = merge_project_servers(&mut servers, enabled.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(!error.contains("sensitive-id"));
        assert_eq!(Value::Object(servers), original);
    }

    #[test]
    fn inherited_optional_nulls_preserve_native_defaults() {
        for (global, project) in [
            (json!({"enabled":null}), json!({"exposure":"hidden"})),
            (
                json!({"enabled":false,"exposure":null}),
                json!({"enabled":true}),
            ),
            (
                json!({"enabled":false,"exposure":null,"oauth":null}),
                json!({"enabled":true}),
            ),
        ] {
            let mut base = json!({"url":"https://example.invalid/mcp"});
            base.as_object_mut()
                .unwrap()
                .extend(global.as_object().unwrap().clone());
            let mut servers = json!({"service":base}).as_object().unwrap().clone();
            let overlay = json!({"service":project});
            merge_project_servers(&mut servers, overlay.as_object().unwrap()).unwrap();
            let config: ServerConfig = serde_json::from_value(servers["service"].clone()).unwrap();
            if global["enabled"].is_null() {
                assert_eq!(config.enabled, None);
            }
            if global.get("exposure").is_some_and(Value::is_null) {
                assert_eq!(config.exposure, None);
                assert_eq!(config.exposure.as_deref().unwrap_or("codemode"), "codemode");
            }
        }
    }
}
