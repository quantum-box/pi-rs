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
            merge_project_servers(&mut servers, entries)?;
        }
    }
    Ok(servers.into_iter().collect())
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
    use pi_rs_workbench::mcp_auth::{OAuthSetting, validate_oauth_client_settings};

    // Serde treats null in an optional member as absent, but Pi validates
    // explicitly supplied OAuth fields and rejects null values.
    if value
        .as_object()
        .is_some_and(|fields| fields.values().any(Value::is_null))
    {
        return false;
    }
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
                    Some(source) => Some(source.resolve().await?),
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
                gateway.access(std::time::Duration::from_secs(60)).await
            }) as futures_util::future::BoxFuture<'static, Result<Option<String>>>
        }) as TokenResolver
    });
    McpExtension::connect_with_proxy_hosts(
        name,
        server,
        &args.workspace,
        resolver,
        Some(super::http_proxy::configured_mcp(&args.settings)?),
    )
    .await
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
            let http_options = super::http_proxy::configured_mcp(&args.settings)?;
            if let Some(metadata_url) = client_metadata_url {
                pi_rs_workbench::mcp_auth::login_with_metadata_with_http_options(
                    &name,
                    url,
                    &metadata_url,
                    &scopes,
                    Some(&http_options),
                    announce,
                )
                .await?;
            } else if client_id.is_none() && server.oauth_client().is_some() {
                pi_rs_workbench::mcp_auth::login_configured_with_http_options(
                    &name,
                    url,
                    server.oauth_client(),
                    None,
                    &scopes,
                    Some(&http_options),
                    announce,
                )
                .await?;
            } else {
                pi_rs_workbench::mcp_auth::login_with_secret_with_http_options(
                    &name,
                    url,
                    client_id.as_deref(),
                    client_secret.as_deref(),
                    &scopes,
                    Some(&http_options),
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
