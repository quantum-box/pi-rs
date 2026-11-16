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

pub async fn configurations(args: &Cli) -> Result<BTreeMap<String, Value>> {
    Ok(configurations_with_project_scope(args, false).await?.0)
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

async fn configurations_with_project_scope(
    args: &Cli,
    strict: bool,
) -> Result<ScopedConfigurations> {
    if args.no_mcp || !super::resource_paths::builtin_enabled(args, "builtin:mcp")? {
        return Ok((
            BTreeMap::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            Vec::new(),
            Vec::new(),
        ));
    }
    // Do not observe a configuration between OAuth enablement and credential
    // persistence. Release the gate before connecting or acquiring credentials.
    with_config_gate(mcp_read_guard(args), || read_configurations(args, strict)).await
}

async fn with_config_gate<T>(
    gate: impl std::future::Future<Output = Result<McpWriteGuard>>,
    read: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let _guard = gate.await?;
    read()
}

fn read_configurations(args: &Cli, strict: bool) -> Result<ScopedConfigurations> {
    let mut configuration_errors = Vec::new();
    let user_path = config::agent_dir().join("mcp.json");
    let user = match config::read_object(&user_path) {
        Ok(user) => user,
        Err(_) if strict => {
            configuration_errors.push(format!(
                "{}: could not read or parse MCP configuration",
                user_path.display()
            ));
            Default::default()
        }
        Err(error) => return Err(error),
    };
    if strict
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
    if strict {
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
            Err(_) if strict => {
                configuration_errors.push(format!(
                    "{}: could not read or parse MCP configuration",
                    project_path.display()
                ));
                Default::default()
            }
            Err(error) => return Err(error),
        };
        if strict
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
            if strict {
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
        && a.oauth == b.oauth
        && a.auth == b.auth
        && auth_header(a) == auth_header(b)
}

fn login_needs_enable(server: &ServerConfig) -> bool {
    matches!(
        server.oauth,
        None | Some(pi_rs_workbench::mcp_auth::OAuthSetting::Enabled(false))
    )
}

struct McpWriteGuard {
    _account: Option<super::mcp_shared_gate::AccountGuard>,
    _gate: Option<std::fs::File>,
    _targets: Vec<std::fs::File>,
}

// All participating clients acquire credential (if any), account, then
// sorted legacy mcp.lock files. Old readers/login transactions are not atomic.
async fn account_guard_for(
    paths: &[PathBuf],
    credential: Option<&std::fs::File>,
    readonly: bool,
) -> Result<McpWriteGuard> {
    let account = super::mcp_shared_gate::acquire(credential).await?;
    account_guard_with(account, paths, credential, readonly).await
}

async fn account_guard_with(
    account: super::mcp_shared_gate::AccountGuard,
    paths: &[PathBuf],
    credential: Option<&std::fs::File>,
    readonly: bool,
) -> Result<McpWriteGuard> {
    let account_id = account
        .file()
        .map(|file| same_file::Handle::from_file(file.try_clone()?))
        .transpose()?;
    let credential_id = credential
        .map(|file| same_file::Handle::from_file(file.try_clone()?))
        .transpose()?;
    let mut lock_paths: Vec<_> = paths.iter().map(|p| p.with_extension("lock")).collect();
    lock_paths.sort();
    let mut opened = Vec::new();
    for path in lock_paths {
        let result = if readonly {
            let writable = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path);
            match writable {
                Err(ref error) if weak_read_lock_error(&path, error) => {
                    std::fs::File::open(&path).map_err(Error::from)
                }
                result => result.map_err(Error::from),
            }
        } else {
            open_mcp_lock(&path)
        };
        let lock = match result {
            Ok(lock) => lock,
            Err(Error::Io(error)) if readonly && weak_read_lock_error(&path, &error) => continue,
            Err(error) => return Err(error),
        };
        let id = same_file::Handle::from_file(lock.try_clone()?)?;
        if account_id.as_ref() == Some(&id) || credential_id.as_ref() == Some(&id) {
            return Err(Error::Config("MCP locks refer to the same file".into()));
        }
        if opened.iter().any(|(other, _, _)| other == &id) {
            continue;
        }
        let order = lock_identity(&lock)?;
        opened.push((id, order, lock));
    }
    // Open and validate every handle before waiting. The account gate serializes
    // new participants; stable physical ordering also avoids alias path inversions.
    opened.sort_by_key(|(_, order, _)| *order);
    let mut targets = Vec::with_capacity(opened.len());
    for (_, _, lock) in opened {
        lock_mcp_file(&lock).await?;
        targets.push(lock);
    }
    Ok(McpWriteGuard {
        _account: Some(account),
        _gate: None,
        _targets: targets,
    })
}

fn weak_read_lock_error(path: &std::path::Path, error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        return true;
    }
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::EROFS) {
        return true;
    }
    if error.kind() == std::io::ErrorKind::NotFound {
        return path.parent().is_some_and(|parent| matches!(std::fs::metadata(parent), Err(e) if e.kind() == std::io::ErrorKind::NotFound));
    }
    false
}

#[cfg(unix)]
fn lock_identity(file: &std::fs::File) -> Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}
#[cfg(windows)]
fn lock_identity(file: &std::fs::File) -> Result<(u64, u64)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let mut info = std::mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    if unsafe { GetFileInformationByHandle(file.as_raw_handle().cast(), info.as_mut_ptr()) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let info = unsafe { info.assume_init() };
    Ok((
        u64::from(info.dwVolumeSerialNumber),
        (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    ))
}
#[cfg(not(any(unix, windows)))]
fn lock_identity(_file: &std::fs::File) -> Result<(u64, u64)> {
    Err(Error::Config("MCP lock identity is unavailable".into()))
}

fn read_paths(args: &Cli) -> Vec<PathBuf> {
    let mut paths = vec![path(args, false)];
    if args.trusted {
        paths.push(path(args, true));
    }
    paths
}
async fn mcp_read_guard(args: &Cli) -> Result<McpWriteGuard> {
    account_guard_for(&read_paths(args), None, true).await
}

fn open_mcp_lock(path: &std::path::Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(path.parent().unwrap_or(std::path::Path::new(".")))?;
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?)
}

async fn lock_mcp_file(file: &std::fs::File) -> Result<()> {
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

async fn mcp_write_guard(
    target: Option<&std::path::Path>,
    credential_lock: Option<&std::fs::File>,
) -> Result<McpWriteGuard> {
    let paths = target
        .into_iter()
        .map(std::path::Path::to_path_buf)
        .collect::<Vec<_>>();
    account_guard_for(&paths, credential_lock, false).await
}

#[cfg(test)]
async fn mcp_write_guard_at(
    gate_path: &std::path::Path,
    target: Option<&std::path::Path>,
    credential_lock: Option<&std::fs::File>,
) -> Result<McpWriteGuard> {
    let gate = open_mcp_lock(gate_path)?;
    let gate_id = same_file::Handle::from_file(gate.try_clone()?)?;
    let credential_id = credential_lock
        .map(|file| same_file::Handle::from_file(file.try_clone()?))
        .transpose()?;
    if credential_id.as_ref() == Some(&gate_id) {
        return Err(Error::Config(
            "MCP configuration and credential locks refer to the same file".into(),
        ));
    }
    lock_mcp_file(&gate).await?;
    let target = if let Some(target) = target {
        let lock = open_mcp_lock(&target.with_extension("lock"))?;
        let target_id = same_file::Handle::from_file(lock.try_clone()?)?;
        if gate_id == target_id || credential_id.as_ref() == Some(&target_id) {
            return Err(Error::Config(
                "MCP configuration locks refer to the same file".into(),
            ));
        }
        lock_mcp_file(&lock).await?;
        Some(lock)
    } else {
        None
    };
    Ok(McpWriteGuard {
        _account: None,
        _gate: Some(gate),
        _targets: target.into_iter().collect(),
    })
}

async fn update_mcp_object(
    path: &std::path::Path,
    update: impl FnOnce(&mut serde_json::Map<String, Value>) -> Result<()>,
) -> Result<()> {
    let _guard = mcp_write_guard(Some(path), None).await?;
    let mut document = config::read_object(path)?;
    update(document.as_object_mut().expect("config is an object"))?;
    config::write_json(path, &document)
}

fn finish_login(
    user_path: &std::path::Path,
    project_path: Option<&std::path::Path>,
    name: &str,
    local: bool,
    before: &LoginSnapshot,
) -> Result<Option<pi_rs_workbench::mcp_auth::LoginConfigRollback>> {
    let mut user = config::read_object(user_path)?;
    let mut project = project_path.map(config::read_object).transpose()?;
    let after = login_snapshot(&user, project.as_ref(), name, local).map_err(|_| {
        Error::Config("MCP authentication configuration changed during login".into())
    })?;
    if !same_login_auth(before, &after) {
        return Err(Error::Config(
            "MCP authentication configuration changed during login".into(),
        ));
    }
    let target_project = after.project_definition || after.project_oauth_override;
    if login_needs_enable(&after.server) {
        let target = if target_project {
            project.as_mut().expect("trusted project")
        } else {
            &mut user
        };
        let entry = target
            .get_mut("mcpServers")
            .and_then(|servers| servers.get_mut(name))
            .ok_or_else(|| Error::Config("MCP configuration changed during login".into()))?;
        let previous_oauth = entry.get("oauth").cloned();
        entry["oauth"] = json!(true);
        let expected = login_snapshot(&user, project.as_ref(), name, local)?;
        let rollback_user_path = user_path.to_owned();
        let rollback_project_path = project_path.map(std::path::Path::to_path_buf);
        let rollback_name = name.to_owned();
        let before = before.clone();
        let rollback: pi_rs_workbench::mcp_auth::LoginConfigRollback = Box::new(move || {
            let user_path = rollback_user_path;
            let project_path = rollback_project_path;
            let name = rollback_name;
            let mut user = config::read_object(&user_path)?;
            let mut project = project_path
                .as_deref()
                .map(config::read_object)
                .transpose()?;
            let current = login_snapshot(&user, project.as_ref(), &name, local)?;
            if same_login_auth(&before, &current) {
                return Ok(());
            }
            if !same_login_auth(&expected, &current) {
                return Err(Error::Config(
                    "MCP configuration changed before rollback".into(),
                ));
            }
            let target = if target_project {
                project.as_mut().expect("trusted project")
            } else {
                &mut user
            };
            let entry = target
                .get_mut("mcpServers")
                .and_then(|entries| entries.get_mut(&name))
                .and_then(Value::as_object_mut)
                .ok_or_else(|| Error::Config("MCP configuration changed before rollback".into()))?;
            if let Some(previous_oauth) = previous_oauth {
                entry.insert("oauth".into(), previous_oauth);
            } else {
                entry.remove("oauth");
            }
            config::write_json(
                if target_project {
                    project_path.as_deref().expect("trusted project")
                } else {
                    &user_path
                },
                target,
            )
        });
        let target = if target_project {
            project.as_ref().expect("trusted project")
        } else {
            &user
        };
        let result = config::write_json(
            if target_project {
                project_path.expect("trusted project")
            } else {
                user_path
            },
            target,
        );
        if let Err(error) = result {
            if rollback().is_err() {
                return Err(Error::Config("MCP configuration update failed and rollback failed; inspect the MCP configuration".into()));
            }
            return Err(error);
        }
        return Ok(Some(rollback));
    }
    Ok(None)
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

pub async fn auto_enable_codemode(args: &Cli) -> Result<bool> {
    let _guard = mcp_read_guard(args).await?;
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
    for (name, value) in configurations(args).await? {
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
        match configurations_with_project_scope(args, true).await {
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

pub async fn command(args: &Cli, command: McpCommand) -> Result<()> {
    match command {
        McpCommand::List { json: true } => {
            return list_json(args).await;
        }
        McpCommand::List { json: false } => {
            let expected = configurations(args)
                .await?
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
            update_mcp_object(&path, |document| {
                let entries = document
                    .entry("mcpServers")
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .ok_or_else(|| Error::Config("mcpServers must be an object".into()))?;
                entries.insert(name.clone(), value);
                Ok(())
            })
            .await?;
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
            let snapshot = tokio::select! {
                biased;
                _ = args.mcp_login_cancel.cancelled() => return Err(Error::Cancelled),
                result = tokio::time::timeout_at(deadline, async {
                    let _guard = mcp_read_guard(args).await?;
                    let user = config::read_object(&path(args, false))?;
                    let project = if args.trusted { Some(config::read_object(&path(args, true))?) } else { None };
                    login_snapshot(&user, project.as_ref(), &name, local)
                }) => result.map_err(|_| Error::Config("MCP login timed out".into()))??,
            };
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
            let user_path = path(args, false);
            let project_path = path(args, true);
            let guard_paths = read_paths(args);
            let finish_user_path = user_path.clone();
            let finish_project_path = args.trusted.then_some(project_path.clone());
            let finish_name = name.clone();
            let finish_snapshot = snapshot.clone();
            login_cli_with_precommit(
                &name,
                url,
                client,
                &scopes,
                Some(&http_options),
                deadline,
                &args.mcp_login_cancel,
                announce,
                move |credential_lock| async move {
                    account_guard_for(&guard_paths, Some(&credential_lock), false).await
                },
                move |_| {
                    finish_login(
                        &finish_user_path,
                        finish_project_path.as_deref(),
                        &finish_name,
                        local,
                        &finish_snapshot,
                    )
                },
            )
            .await?;
            output(json!({"type":"mcp_logged_in","name":name}));
        }
        McpCommand::Logout { name, local } => {
            let (servers, project_definitions, _, _, _) =
                configurations_with_project_scope(args, false).await?;
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
            update_mcp_object(&path, |document| {
                document
                    .get_mut("mcpServers")
                    .and_then(Value::as_object_mut)
                    .and_then(|servers| servers.remove(&name))
                    .ok_or_else(|| {
                        Error::Config("MCP server not found in this configuration".into())
                    })?;
                Ok(())
            })
            .await?;
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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_account_gate_serializes_distinct_homes_and_cancels_waiter() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let first = super::super::mcp_shared_gate::acquire_for_test(&profile, None)
            .await
            .unwrap();
        let project = root.path().join("project/mcp.json");
        let guard = account_guard_with(
            first,
            &[root.path().join("home-a/mcp.json"), project.clone()],
            None,
            false,
        )
        .await
        .unwrap();
        let waiting = tokio::time::timeout(
            std::time::Duration::from_millis(150),
            super::super::mcp_shared_gate::acquire_for_test(&profile, None),
        )
        .await;
        assert!(waiting.is_err());
        drop(guard);
        let second = super::super::mcp_shared_gate::acquire_for_test(&profile, None)
            .await
            .unwrap();
        let _guard = account_guard_with(
            second,
            &[root.path().join("home-b/mcp.json"), project],
            None,
            false,
        )
        .await
        .unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_gate_acquires_duplicate_legacy_locks_once() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let first = root.path().join("first/mcp.json");
        let second = root.path().join("second/mcp.json");
        let _held = open_mcp_lock(&first.with_extension("lock")).unwrap();
        std::fs::create_dir_all(second.parent().unwrap()).unwrap();
        std::fs::hard_link(first.with_extension("lock"), second.with_extension("lock")).unwrap();
        let account = super::super::mcp_shared_gate::acquire_for_test(&profile, None)
            .await
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            account_guard_with(account, &[first, second], None, false),
        )
        .await;
        assert!(result.expect("duplicate must not deadlock").is_ok());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_gate_rejects_credential_lock_alias_before_waiting() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let credential_path = root.path().join("credentials.lock");
        let credential = open_mcp_lock(&credential_path).unwrap();
        credential.lock().unwrap();
        let config = root.path().join("mcp.json");
        std::fs::hard_link(&credential_path, config.with_extension("lock")).unwrap();
        let account = super::super::mcp_shared_gate::acquire_for_test(&profile, Some(&credential))
            .await
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            account_guard_with(account, &[config], Some(&credential), false),
        )
        .await;
        assert!(result.expect("credential alias must not deadlock").is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn injected_read_guard_locks_existing_readonly_file() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profile");
        std::fs::create_dir(&profile).unwrap();
        std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = root.path().join("mcp.json");
        let lock_path = config.with_extension("lock");
        std::fs::File::create(&lock_path).unwrap();
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o444)).unwrap();
        let account = super::super::mcp_shared_gate::acquire_for_test(&profile, None)
            .await
            .unwrap();
        let guard = account_guard_with(account, &[config], None, true)
            .await
            .unwrap();
        let contender = std::fs::File::open(&lock_path).unwrap();
        assert!(matches!(
            contender.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        drop(guard);
        contender.try_lock().unwrap();
    }

    #[test]
    fn readonly_fallback_distinguishes_missing_parent_and_contention() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing/mcp.lock");
        assert!(weak_read_lock_error(
            &missing,
            &std::io::Error::from(std::io::ErrorKind::NotFound)
        ));
        assert!(!weak_read_lock_error(
            &missing,
            &std::io::Error::from(std::io::ErrorKind::WouldBlock)
        ));
        assert!(!weak_read_lock_error(
            &root.path().join("mcp.lock"),
            &std::io::Error::from(std::io::ErrorKind::NotFound)
        ));
        #[cfg(unix)]
        assert!(weak_read_lock_error(
            &missing,
            &std::io::Error::from_raw_os_error(libc::EROFS)
        ));
    }

    #[test]
    fn login_resolution_uses_trusted_definition_and_oauth_override() {
        let user = json!({"mcpServers":{"service":{"url":"https://global.example/mcp"}}});
        let project = json!({"mcpServers":{"service":{"url":"https://project.example/mcp"}}});
        let snapshot = login_snapshot(&user, Some(&project), "service", false).unwrap();
        assert!(snapshot.project_definition);
        assert_eq!(
            snapshot.server.url.as_deref(),
            Some("https://project.example/mcp")
        );
        assert!(login_snapshot(&user, Some(&project), "service", true).is_ok());
        assert!(login_snapshot(&user, None, "service", true).is_err());
        let partial = json!({"mcpServers":{"service":{"oauth":false}}});
        let snapshot = login_snapshot(&user, Some(&partial), "service", false).unwrap();
        assert!(!snapshot.project_definition);
        assert!(snapshot.project_oauth_override);
        assert!(login_snapshot(&user, Some(&partial), "service", true).is_err());
        assert!(matches!(
            snapshot.server.oauth,
            Some(pi_rs_workbench::mcp_auth::OAuthSetting::Enabled(false))
        ));
    }

    #[test]
    fn login_finish_updates_override_without_losing_unrelated_edits() {
        let root = tempfile::tempdir().unwrap();
        let user_path = root.path().join("user/mcp.json");
        let project_path = root.path().join("project/mcp.json");
        let mut user = json!({"mcpServers":{"service":{"url":"https://global.example/mcp"}}});
        let mut project = json!({"mcpServers":{"service":{"oauth":false}}});
        let before = login_snapshot(&user, Some(&project), "service", false).unwrap();
        user["other"] = json!("keep user edit");
        project["other"] = json!("keep project edit");
        config::write_json(&user_path, &user).unwrap();
        config::write_json(&project_path, &project).unwrap();
        finish_login(&user_path, Some(&project_path), "service", false, &before).unwrap();
        let actual_user = config::read_object(&user_path).unwrap();
        let actual_project = config::read_object(&project_path).unwrap();
        assert_eq!(actual_user, user);
        assert_eq!(actual_project["other"], "keep project edit");
        assert_eq!(actual_project["mcpServers"]["service"]["oauth"], true);
    }

    #[test]
    fn login_finish_preserves_project_oauth_client_object() {
        let root = tempfile::tempdir().unwrap();
        let user_path = root.path().join("user/mcp.json");
        let project_path = root.path().join("project/mcp.json");
        let user = json!({"mcpServers":{"service":{"url":"https://global.example/mcp"}}});
        let project = json!({"mcpServers":{"service":{"oauth":{"clientId":"fixture"}}}});
        let before = login_snapshot(&user, Some(&project), "service", false).unwrap();
        config::write_json(&user_path, &user).unwrap();
        config::write_json(&project_path, &project).unwrap();
        finish_login(&user_path, Some(&project_path), "service", false, &before).unwrap();
        assert_eq!(config::read_object(&user_path).unwrap(), user);
        assert_eq!(config::read_object(&project_path).unwrap(), project);
    }

    #[test]
    fn failed_login_rollback_restores_oauth_without_losing_unrelated_edits() {
        for original in [None, Some(json!(false))] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("mcp.json");
            let mut user = json!({"mcpServers":{"service":{"url":"https://example.test/mcp"}}});
            if let Some(value) = &original {
                user["mcpServers"]["service"]["oauth"] = value.clone();
            }
            let before = login_snapshot(&user, None, "service", false).unwrap();
            config::write_json(&path, &user).unwrap();
            let undo = finish_login(&path, None, "service", false, &before)
                .unwrap()
                .unwrap();
            let mut changed = config::read_object(&path).unwrap();
            changed["unrelated"] = json!("retained");
            config::write_json(&path, &changed).unwrap();
            undo().unwrap();
            let restored = config::read_object(&path).unwrap();
            assert_eq!(restored["unrelated"], "retained");
            assert_eq!(
                restored["mcpServers"]["service"].get("oauth"),
                original.as_ref()
            );
        }
    }

    #[test]
    fn failed_login_rollback_refuses_changed_authentication() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("mcp.json");
        let user = json!({"mcpServers":{"service":{"url":"https://example.test/mcp"}}});
        let before = login_snapshot(&user, None, "service", false).unwrap();
        config::write_json(&path, &user).unwrap();
        let undo = finish_login(&path, None, "service", false, &before)
            .unwrap()
            .unwrap();
        let mut changed = config::read_object(&path).unwrap();
        changed["mcpServers"]["service"]["url"] = json!("https://changed.test/mcp");
        config::write_json(&path, &changed).unwrap();
        assert!(undo().is_err());
        assert_eq!(config::read_object(&path).unwrap(), changed);
    }

    #[tokio::test]
    async fn login_finish_detects_changes_while_waiting_for_gate() {
        let root = tempfile::tempdir().unwrap();
        let user_path = root.path().join("a/mcp.json");
        let project_path = root.path().join("z/mcp.json");
        let user = json!({"mcpServers":{"service":{"url":"https://global.example/mcp"}}});
        let mut project = json!({"mcpServers":{}});
        let before = login_snapshot(&user, Some(&project), "service", false).unwrap();
        config::write_json(&user_path, &user).unwrap();
        config::write_json(&project_path, &project).unwrap();
        let gate = root.path().join("gate.lock");
        let held = mcp_write_guard_at(&gate, None, None).await.unwrap();
        project["mcpServers"]["service"] = json!({"oauth":false});
        config::write_json(&project_path, &project).unwrap();
        drop(held);
        let _guard = mcp_write_guard_at(&gate, Some(&user_path), None)
            .await
            .unwrap();
        assert!(finish_login(&user_path, Some(&project_path), "service", false, &before).is_err());
        assert_eq!(config::read_object(&user_path).unwrap(), user);
        assert_eq!(config::read_object(&project_path).unwrap(), project);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mcp_gate_rejects_hardlinked_target_lock() {
        let root = tempfile::tempdir().unwrap();
        let gate = root.path().join("gate.lock");
        let target = root.path().join("mcp.json");
        std::fs::write(&gate, "").unwrap();
        std::fs::hard_link(&gate, target.with_extension("lock")).unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            mcp_write_guard_at(&gate, Some(&target), None),
        )
        .await;
        assert!(result.expect("lock acquisition hung").is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mcp_gate_rejects_aliases_of_credential_lock() {
        for alias_gate in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let credential = root.path().join("credential.lock");
            let gate = root.path().join("gate.lock");
            let target = root.path().join("mcp.json");
            std::fs::write(&credential, "").unwrap();
            std::fs::hard_link(
                &credential,
                if alias_gate {
                    gate.clone()
                } else {
                    target.with_extension("lock")
                },
            )
            .unwrap();
            let held = open_mcp_lock(&credential).unwrap();
            held.lock().unwrap();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                mcp_write_guard_at(&gate, Some(&target), Some(&held)),
            )
            .await;
            assert!(result.expect("lock acquisition hung").is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn gate_checks_the_held_credential_file_after_path_replacement() {
        let root = tempfile::tempdir().unwrap();
        let credential = root.path().join("credential.lock");
        let gate = root.path().join("gate.lock");
        std::fs::write(&credential, "").unwrap();
        let held = open_mcp_lock(&credential).unwrap();
        held.lock().unwrap();
        std::fs::hard_link(&credential, &gate).unwrap();
        std::fs::remove_file(&credential).unwrap();
        std::fs::write(&credential, "replacement").unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            mcp_write_guard_at(&gate, None, Some(&held)),
        )
        .await;
        assert!(result.expect("lock acquisition hung").is_err());
    }

    #[tokio::test]
    async fn mcp_gate_wait_can_be_cancelled_without_project_write() {
        let root = tempfile::tempdir().unwrap();
        let gate = root.path().join("gate.lock");
        let project = root.path().join("project/mcp.json");
        let held = mcp_write_guard_at(&gate, None, None).await.unwrap();
        assert!(!project.parent().unwrap().exists());
        let waiting = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            mcp_write_guard_at(&gate, Some(&project), None),
        )
        .await;
        assert!(waiting.is_err());
        assert!(!project.parent().unwrap().exists());
        drop(held);
        assert!(
            mcp_write_guard_at(&gate, Some(&project), None)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn guarded_snapshot_waits_for_final_configuration() {
        let root = tempfile::tempdir().unwrap();
        let gate = root.path().join("gate.lock");
        let config = root.path().join("mcp.json");
        std::fs::write(&config, "intermediate").unwrap();
        let held = mcp_write_guard_at(&gate, None, None).await.unwrap();
        let gate_for_reader = gate.clone();
        let config_for_reader = config.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut reader = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            with_config_gate(mcp_write_guard_at(&gate_for_reader, None, None), || {
                Ok(std::fs::read_to_string(config_for_reader)?)
            })
            .await
        });
        started_rx.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut reader)
                .await
                .is_err()
        );
        std::fs::write(&config, "final").unwrap();
        drop(held);
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), reader)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(read, "final");
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            mcp_write_guard_at(&gate, None, None),
        )
        .await
        .expect("snapshot retained its gate")
        .unwrap();
    }

    #[tokio::test]
    async fn failed_snapshot_releases_the_configuration_gate() {
        let root = tempfile::tempdir().unwrap();
        let gate = root.path().join("gate.lock");
        let result = with_config_gate(mcp_write_guard_at(&gate, None, None), || {
            Err::<(), _>(Error::Config("invalid fixture configuration".into()))
        })
        .await;
        assert!(result.is_err());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            mcp_write_guard_at(&gate, None, None),
        )
        .await
        .expect("failed snapshot retained its gate")
        .unwrap();
    }

    #[tokio::test]
    async fn cancelled_snapshot_does_not_run_after_gate_is_released() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // The current gate waits directly in this future. If it ever starts a
        // detached worker, the test must also wait for that worker to exit.
        struct NotifyDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for NotifyDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let root = tempfile::tempdir().unwrap();
        let gate = root.path().join("gate.lock");
        let held = mcp_write_guard_at(&gate, None, None).await.unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let task_gate = gate.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _drop_notification = NotifyDrop(Some(dropped_tx));
            started_tx.send(()).unwrap();
            with_config_gate(mcp_write_guard_at(&task_gate, None, None), || {
                observed.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        dropped_rx
            .await
            .expect("cancelled gate future was not dropped");
        drop(held);
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            mcp_write_guard_at(&gate, None, None),
        )
        .await
        .expect("cancelled waiter retained the gate")
        .unwrap();
        drop(next);
        tokio::task::yield_now().await;
        assert!(!ran.load(Ordering::SeqCst));
    }

    #[test]
    fn login_finish_refuses_authentication_or_scope_changes() {
        for change in ["url", "oauth", "authorization", "definition"] {
            let root = tempfile::tempdir().unwrap();
            let user_path = root.path().join("user/mcp.json");
            let project_path = root.path().join("project/mcp.json");
            let mut user = json!({"mcpServers":{"service":{"url":"https://global.example/mcp"}}});
            let mut project = json!({"mcpServers":{}});
            let before = login_snapshot(&user, Some(&project), "service", false).unwrap();
            match change {
                "url" => {
                    user["mcpServers"]["service"]["url"] = json!("https://changed.example/mcp")
                }
                "oauth" => project["mcpServers"]["service"] = json!({"oauth":false}),
                "authorization" => {
                    user["mcpServers"]["service"]["headers"] =
                        json!({"Authorization":"Bearer changed"})
                }
                "definition" => {
                    project["mcpServers"]["service"] = json!({"url":"https://project.example/mcp"})
                }
                _ => unreachable!(),
            }
            config::write_json(&user_path, &user).unwrap();
            config::write_json(&project_path, &project).unwrap();
            assert!(
                finish_login(&user_path, Some(&project_path), "service", false, &before).is_err(),
                "{change}"
            );
            assert_eq!(config::read_object(&user_path).unwrap(), user);
            assert_eq!(config::read_object(&project_path).unwrap(), project);
        }
    }

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
