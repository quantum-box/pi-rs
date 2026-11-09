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
    Add {
        name: String,
        #[arg(short = 'l', long)]
        local: bool,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        bearer_token_env_var: Option<String>,
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
                if !["command", "url", "type"]
                    .iter()
                    .any(|key| entry.get(*key).is_some())
                    && let (Some(base), Some(overlay)) = (
                        servers.get_mut(name).and_then(Value::as_object_mut),
                        entry.as_object(),
                    )
                {
                    for key in ["enabled", "exposure", "toolExposure"] {
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
        let workspace = args.workspace.clone();
        pending.push(async move {
            let result = McpExtension::connect(&name, &config, &workspace).await;
            (name, result)
        });
    }
    let mut connected = Vec::new();
    for (name, result) in futures_util::future::join_all(pending).await {
        match result {
            Ok(extension) => {
                if report {
                    output(
                        json!({"type":"mcp_server","name":name,"status":"connected","tools":extension.tool_count(),"description":extension.description}),
                    );
                }
                connected.push(Arc::new(extension));
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
    Ok(connected)
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
            if url.is_some() == !command.is_empty() {
                return Err(Error::Config(
                    "choose either --url or a command after --".into(),
                ));
            }
            let mut value = if let Some(url) = url {
                json!({"url":url})
            } else {
                json!({"command":command[0],"args":command[1..]})
            };
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
