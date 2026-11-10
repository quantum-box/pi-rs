//! MCP configuration overlay and command-line management.
use super::{Cli, config, output};
use clap::Subcommand;
use pi_rs::{Error, Result};
use pi_rs_workbench::mcp::{McpExtension, ServerConfig};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Subcommand)]
pub enum McpCommand {
    List,
    /// Sign in to an HTTP MCP server using OAuth and a browser.
    Login {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
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
    },
    Add {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        bearer_token_env_var: Option<String>,
        /// Authenticate with credentials saved by mcp login.
        #[arg(long)]
        oauth: bool,
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

pub fn configurations(args: &Cli) -> Result<BTreeMap<String, Value>> {
    if args.no_mcp || !super::resource_paths::builtin_enabled(args, "builtin:mcp")? {
        return Ok(BTreeMap::new());
    }
    let user = config::read_object(&config::agent_dir().join("mcp.json"))?;
    let mut servers = user
        .get("mcpServers")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if args.trusted {
        let project = config::read_object(&args.workspace.join(".pi/mcp.json"))?;
        if let Some(entries) = project.get("mcpServers").and_then(Value::as_object) {
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
                    for key in ["enabled", "exposure", "toolExposure", "oauth"] {
                        if let Some(value) = overlay.get(key) {
                            base.insert(key.into(), value.clone());
                        }
                    }
                    continue;
                }
                servers.insert(name.clone(), entry.clone());
            }
        }
    }
    Ok(servers.into_iter().collect())
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
    Ok(discover(args, report, false).await?.connected)
}

#[derive(Default)]
pub(super) struct Discovery {
    pub connected: Vec<Arc<McpExtension>>,
    pub sign_in: Vec<PendingSignIn>,
}

#[derive(Clone)]
pub(super) struct PendingSignIn {
    pub name: String,
    pub config: ServerConfig,
    pub challenge: Option<String>,
}

pub(super) async fn discover(args: &Cli, report: bool, interactive: bool) -> Result<Discovery> {
    let mut normalized = BTreeSet::new();
    let mut pending = Vec::new();
    for (name, value) in configurations(args)? {
        if !normalized.insert(name.replace('-', "_")) {
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
            let result = connect(&args, &name, &config).await;
            (name, config, result)
        });
    }
    let mut discovery = Discovery::default();
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
    Ok(discovery)
}

async fn connect(args: &Cli, name: &str, server: &ServerConfig) -> Result<McpExtension> {
    use pi_rs_workbench::mcp::TokenResolver;
    let resolver: Option<TokenResolver> = server.auth.as_ref().map(|auth| {
        let args = args.clone();
        let provider = auth.provider.clone();
        Arc::new(move || {
            let mut args = args.clone();
            let provider = provider.clone();
            Box::pin(async move {
                args.model_profiles = super::model_profiles::Profiles::load()?;
                args.model_keys = super::model_auth::Keys::load()?;
                let provider = args.model_profiles.provider(&provider).ok_or_else(|| {
                    Error::Config("MCP auth.provider does not name an installed provider".into())
                })?;
                let endpoint = if args.endpoint_explicit {
                    args.endpoint_for(&provider)
                } else {
                    args.model_profiles.endpoint(&provider)
                };
                let token = args.model_token_for(&provider, endpoint);
                if token.is_some() || provider != super::Provider::Radius {
                    return Ok(token);
                }
                let gateway = super::radius_auth::Gateway::configured()?;
                if gateway.url("/messages").as_str() != endpoint {
                    return Ok(None);
                }
                gateway.access(std::time::Duration::from_secs(60)).await
            }) as futures_util::future::BoxFuture<'static, Result<Option<String>>>
        }) as TokenResolver
    });
    McpExtension::connect_with_provider_token(name, server, &args.workspace, resolver).await
}

pub async fn command(args: &Cli, command: McpCommand) -> Result<()> {
    match command {
        McpCommand::List => {
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
            bearer_token_env_var,
            oauth,
            exposure,
            environment,
            command,
        } => {
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
            if oauth {
                if value.get("url").is_none() || bearer_token_env_var.is_some() {
                    return Err(Error::Config(
                        "--oauth requires --url and cannot use --bearer-token-env-var".into(),
                    ));
                }
                value["oauth"] = json!(true);
            }
            if let Some(exposure) = exposure {
                value["exposure"] = json!(exposure);
            }
            if let Some(variable) = bearer_token_env_var {
                if !variable
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    || variable.is_empty()
                {
                    return Err(Error::Config("invalid token environment variable".into()));
                }
                value["headers"] = json!({"Authorization":format!("Bearer ${{{variable}}}")});
            }
            if !environment.is_empty() {
                let mut env = serde_json::Map::new();
                for item in environment {
                    let (name, value) = item
                        .split_once('=')
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
            client_id,
            client_secret_env,
            client_metadata_url,
            scopes,
        } => {
            let path = path(args, local);
            let document = config::read_object(&path)?;
            let value = document.get("mcpServers").and_then(|servers| servers.get(&name))
                .ok_or_else(|| Error::Config("MCP server is not configured in this scope; use --local for a project server".into()))?;
            let server: ServerConfig = serde_json::from_value(value.clone())?;
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
            let client_secret = client_secret_env
                .map(|name| {
                    std::env::var(name).map_err(|_| {
                        Error::Config("OAuth client secret environment variable is not set".into())
                    })
                })
                .transpose()?;
            let announce =
                |url: &str| output(json!({"type":"mcp_authorization","name":name,"url":url}));
            let server_oauth = server.oauth.clone();
            if let Some(metadata_url) = client_metadata_url {
                pi_rs_workbench::mcp_auth::login_with_metadata(
                    &name,
                    url,
                    &metadata_url,
                    &scopes,
                    announce,
                )
                .await?;
            } else if client_id.is_none() && server.oauth_client().is_some() {
                pi_rs_workbench::mcp_auth::login_configured(
                    &name,
                    url,
                    server.oauth_client(),
                    None,
                    &scopes,
                    announce,
                )
                .await?;
            } else {
                pi_rs_workbench::mcp_auth::login_with_secret(
                    &name,
                    url,
                    client_id.as_deref(),
                    client_secret.as_deref(),
                    &scopes,
                    announce,
                )
                .await?;
            }
            config::update_object(&path, |document| {
                let server = document
                    .get_mut("mcpServers")
                    .and_then(|servers| servers.get_mut(&name))
                    .ok_or_else(|| {
                        Error::Config("MCP configuration changed during login".into())
                    })?;
                let current: ServerConfig = serde_json::from_value(server.clone())?;
                if current.url.as_deref() != Some(url)
                    || current.oauth != server_oauth
                    || current.auth.is_some()
                    || current.command.is_some()
                    || current
                        .headers
                        .keys()
                        .any(|name| name.eq_ignore_ascii_case("authorization"))
                {
                    return Err(Error::Config(
                        "MCP authentication configuration changed during login".into(),
                    ));
                }
                if current.oauth_client().is_none() {
                    server["oauth"] = json!(true);
                }
                Ok(())
            })?;
            output(json!({"type":"mcp_logged_in","name":name}));
        }
        McpCommand::Logout { name } => {
            let servers = configurations(args)?;
            let server: ServerConfig = serde_json::from_value(
                servers
                    .get(&name)
                    .ok_or_else(|| Error::Config("MCP server is not configured".into()))?
                    .clone(),
            )?;
            if let Some(auth) = &server.auth {
                return Err(Error::Config(format!(
                    "MCP {name} uses credentials from provider {}; sign out of that provider",
                    auth.provider
                )));
            }
            let url = server
                .url
                .as_deref()
                .ok_or_else(|| Error::Config("MCP OAuth requires an HTTP server".into()))?;
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
