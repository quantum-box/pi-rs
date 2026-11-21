//! Shared local runtime construction for print, RPC and terminal sessions.
use super::{Cli, DEFAULT_SYSTEM_PROMPT, mcp_config, output, resources};
use pi_rs::{
    Agent, CancellationToken, Extension, ExtensionUi, Limits, ProjectResources, Registry, Result,
    Session, UiRequest, UiResponse,
};
use pi_rs_workbench::coding::{CodingTools, ShellConfig, ShellContext};
use pi_rs_workbench::{CommandPolicy, ProjectInstructions, Tools, WorkbenchContext};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub struct Runtime {
    pub auto_compaction: pi_rs::AutoCompactionControl,
    pub retry: pi_rs::RetryControl,
    provider: super::image_policy::WorkbenchModel,
    pub registry: Registry,
    pub extension_sources: std::collections::BTreeMap<String, std::path::PathBuf>,
    shell: ShellConfig,
    system_prompt: String,
    pub resources: resources::Resources,
    resource_origins: ResourceOrigins,
    catalog: Arc<LiveMcpCatalog>,
    mcp_sign_in: std::collections::VecDeque<mcp_config::PendingSignIn>,
    mcp_inspection: Vec<mcp_config::Inspection>,
}

#[derive(Clone, Debug)]
struct ResourceOrigin {
    root: std::path::PathBuf,
    source: String,
    base_dir: Option<std::path::PathBuf>,
}

#[derive(Default)]
struct ResourceOrigins {
    skills: Vec<ResourceOrigin>,
    prompts: Vec<ResourceOrigin>,
    themes: Vec<ResourceOrigin>,
}

impl ResourceOrigin {
    fn source_info(path: &std::path::Path, origins: &[Self]) -> Option<serde_json::Value> {
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_owned());
        let origin = origins
            .iter()
            .find(|origin| canonical.starts_with(&origin.root))?;
        let mut result =
            json!({"path":path,"source":origin.source,"scope":"temporary","origin":"top-level"});
        if let Some(base) = &origin.base_dir {
            result["baseDir"] = json!(base);
        }
        Some(result)
    }
}

fn synthetic_command_source(command: &pi_rs::RegisteredCommand) -> serde_json::Value {
    match command.source_kind {
        pi_rs::ExtensionCommandSourceKind::Builtin => json!({
            "path": format!("builtin:{}", command.extension_id),
            "source": "builtin",
            "scope": "user",
            "origin": "top-level",
        }),
        _ => json!({
            "path": format!("<inline:{}>", command.extension_id),
            "source": "inline",
            "scope": "temporary",
            "origin": "top-level",
        }),
    }
}

fn discovered_resource_source(item: &pi_rs::ExtensionResources, registry: &Registry) -> String {
    if let Some(name) = item.source.as_ref().and_then(|path| path.file_name()) {
        let name = name.to_string_lossy();
        let name = name
            .strip_suffix(".ts")
            .or_else(|| name.strip_suffix(".js"))
            .unwrap_or(&name);
        format!("extension:{name}")
    } else if registry.extension_source_is_builtin(&item.extension_id) {
        format!("extension:builtin:{}", item.extension_id)
    } else {
        format!("extension:inline:{}", item.extension_id)
    }
}

impl Runtime {
    pub fn share_base(&self) -> super::sessions::ShareContext {
        super::sessions::ShareContext {
            system_prompt: self.system_prompt.clone(),
            tools: self.registry.active_tool_specs(),
        }
    }

    pub async fn discover_resources(
        &mut self,
        args: &Cli,
        session: &mut Session,
        reason: pi_rs::ResourceDiscoveryReason,
    ) -> Result<pi_rs::DiscoveredResources> {
        self.discover_resources_checked(args, session, reason, false)
            .await
    }

    async fn discover_resources_checked(
        &mut self,
        args: &Cli,
        session: &mut Session,
        reason: pi_rs::ResourceDiscoveryReason,
        require_complete: bool,
    ) -> Result<pi_rs::DiscoveredResources> {
        if !self.registry.has_resource_discovery_hooks() {
            return Ok(pi_rs::DiscoveredResources::default());
        }
        let event = pi_rs::ResourceDiscoveryEvent {
            cwd: args
                .workspace
                .canonicalize()
                .unwrap_or_else(|_| args.workspace.clone()),
            reason,
        };
        let mut failed = false;
        let mut on_error = |extension: &str, message: &str| {
            failed = true;
            super::observe(
                pi_rs::Notification::ExtensionLifecycleError {
                    extension,
                    event: "resources_discover",
                    message,
                },
                args.trace,
            );
        };
        let results = if session.host_binding().is_some() {
            self.registry
                .discover_resources(
                    &event,
                    &session.extension_states(),
                    args.tool_timeout(),
                    &mut on_error,
                )
                .await
        } else {
            self.registry
                .discover_resources_in_session(
                    &event,
                    session,
                    args.tool_timeout(),
                    &mut |event| super::observe(event, args.trace),
                    &mut on_error,
                )
                .await?
        };
        let mut discovered = pi_rs::DiscoveredResources::default();
        let mut origins = ResourceOrigins::default();
        for item in results {
            let source = discovered_resource_source(&item, &self.registry);
            let base_dir = item
                .source
                .as_deref()
                .and_then(std::path::Path::parent)
                .map(std::path::Path::to_path_buf);
            let mut resolve = |path: std::path::PathBuf| {
                let path = match resolve_discovered_path(&path, &event.cwd) {
                    Ok(path) => path,
                    Err(error) => {
                        failed = true;
                        eprintln!("Extension {}: {error}", item.extension_id);
                        return None;
                    }
                };
                if path.exists() {
                    Some(path)
                } else {
                    failed = true;
                    eprintln!(
                        "Extension {}: resource {} was not found",
                        item.extension_id,
                        path.display()
                    );
                    None
                }
            };
            let mut add = |paths: Vec<std::path::PathBuf>,
                           target: &mut Vec<std::path::PathBuf>,
                           records: &mut Vec<ResourceOrigin>| {
                for path in paths.into_iter().filter_map(&mut resolve) {
                    let root = path.canonicalize().unwrap_or_else(|_| path.clone());
                    let origin = ResourceOrigin {
                        root: root.clone(),
                        source: source.clone(),
                        base_dir: base_dir.clone(),
                    };
                    if let Some(existing) = records.iter_mut().find(|entry| entry.root == root) {
                        *existing = origin;
                    } else {
                        records.push(origin);
                    }
                    target.push(path);
                }
            };
            add(
                item.resources.skill_paths,
                &mut discovered.skill_paths,
                &mut origins.skills,
            );
            add(
                item.resources.prompt_paths,
                &mut discovered.prompt_paths,
                &mut origins.prompts,
            );
            add(
                item.resources.theme_paths,
                &mut discovered.theme_paths,
                &mut origins.themes,
            );
        }
        if require_complete && failed {
            return Err(pi_rs::Error::Config(
                "Extension resource discovery failed while preparing Radius share".into(),
            ));
        }
        let resources = resources::Resources::load_with_discovered(args, &discovered)?;
        for warning in &resources.warnings {
            if !self.resources.warnings.contains(warning) {
                eprintln!("{warning}");
            }
        }
        let servers = self.catalog.servers.lock().expect("MCP catalogs").clone();
        let (selection, _, hidden) = tool_discovery(args, &servers)?;
        self.system_prompt = select_tools(
            &mut self.registry,
            &selection,
            &hidden,
            &resources,
            args,
            &servers,
        )?;
        *self.catalog.resources.lock().expect("resource catalog") = resources.clone();
        self.resources = resources;
        self.resource_origins = origins;
        Ok(discovered)
    }

    pub fn command_catalog(&self, args: &Cli) -> serde_json::Value {
        let user_root = super::config::agent_dir();
        let user_root = user_root.canonicalize().unwrap_or(user_root);
        let workspace = args
            .workspace
            .canonicalize()
            .unwrap_or_else(|_| args.workspace.clone());
        let source_info = |path: &std::path::Path, packages: &[std::path::PathBuf]| {
            let scope = if path.starts_with(&user_root) {
                "user"
            } else if path.starts_with(&workspace) {
                "project"
            } else {
                "temporary"
            };
            let origin = if packages.iter().any(|root| path.starts_with(root)) {
                "package"
            } else {
                "top-level"
            };
            json!({"path":path,"source":"local","scope":scope,"origin":origin})
        };
        let mut commands = Vec::new();
        for command in self.registry.registered_commands() {
            let source = match command.source_path.as_deref() {
                Some(path) => source_info(path, &self.resources.packages.extensions),
                None => synthetic_command_source(&command),
            };
            let mut entry =
                json!({"name":command.invocation_name,"source":"extension","sourceInfo":source});
            if let Some(description) = command.description {
                entry["description"] = json!(description);
            }
            commands.push(entry);
        }
        let mut prompt_surrogates = Vec::new();
        for (name, prompt) in &self.resources.prompts {
            if let Some(unit) = prompt.fallback_surrogate {
                prompt_surrogates.push(json!([commands.len(), unit]));
            }
            commands.push(json!({
                "name": name,
                "description": prompt.description,
                "source": "prompt",
                "sourceInfo": ResourceOrigin::source_info(&prompt.path, &self.resource_origins.prompts).unwrap_or_else(|| source_info(&prompt.path, &self.resources.packages.prompts)),
            }));
        }
        for (name, skill) in &self.resources.skills {
            commands.push(json!({
                "name": format!("skill:{name}"),
                "description": skill.description,
                "source": "skill",
                "sourceInfo": ResourceOrigin::source_info(&skill.path, &self.resource_origins.skills).unwrap_or_else(|| source_info(&skill.path, &self.resources.packages.skills)),
            }));
        }
        let mut catalog = json!({"commands":commands});
        if !prompt_surrogates.is_empty() {
            catalog["_pi_rs_prompt_surrogates"] = json!(prompt_surrogates);
        }
        catalog
    }

    pub fn agent<'a>(&'a self, args: &'a Cli, session: &Session) -> Agent<'a> {
        let session_id = session.session_id();
        self.registry.remove_session_branch_models(session_id);
        // Resolve every recorded identity through its own configured provider.
        // The journal id scopes the cache so credentials cannot cross sessions.
        for record in session.records() {
            let (provider, model) = match &record.event {
                pi_rs::Event::ModelSelected {
                    provider: Some(provider),
                    model,
                    ..
                }
                | pi_rs::Event::BranchModelSelected {
                    provider, model, ..
                } => (provider, model),
                pi_rs::Event::BranchSummary {
                    fallback_model: Some(selection),
                    ..
                } => (&selection.provider, &selection.model),
                _ => continue,
            };
            let Some(selected) = args.model_profiles.provider(provider) else {
                continue;
            };
            let mut branch_args = args.clone();
            if branch_args
                .activate_provider(selected, Some(model))
                .is_err()
            {
                continue;
            }
            branch_args.model = model.clone();
            if let Ok(client) = model_client(
                &branch_args,
                super::thinking::reported(&branch_args).is_some(),
                true,
            ) {
                let _ = self.registry.register_session_provider_model_with_limits(
                    session_id,
                    provider,
                    model,
                    Arc::new(client),
                    Some(Limits {
                        compaction_tokens: Some(args.settings.compaction_tokens(provider, model)),
                        ..effective_limits(&branch_args)
                    }),
                );
            }
        }
        self.shell.set_context_with_external_id(
            ShellContext {
                id: session_id,
                path: (!args.no_session && !args.ephemeral_worker_session)
                    .then(|| args.session.clone()),
                provider: args.provider.id().into(),
                model: args.model.clone(),
                reasoning_level: Some(args.thinking.clone().unwrap_or_else(|| "off".into())),
            },
            session.external_session_id(),
        );
        self.auto_compaction.set_token_settings(
            args.settings
                .compaction_tokens(args.provider.id(), &args.model),
        );
        Agent {
            model: &self.provider,
            model_name: &args.model,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: effective_limits(args),
        }
    }
    pub fn mcp_status(&self, args: &Cli) -> Result<()> {
        let servers = self.catalog.servers.lock().expect("MCP catalogs");
        for (name, value) in
            mcp_config::registered_configurations(args, self.registry.registered_mcp_servers())?
        {
            if let Some(server) = servers.iter().find(|server| server.server_name == name) {
                output(
                    json!({"type":"mcp_server","name":name,"status":"connected","tools":server.tool_count(),"description":server.description}),
                );
            } else {
                output(
                    json!({"type":"mcp_server","name":name,"status":if value["enabled"] == false { "disabled" } else { "disconnected" }}),
                );
            }
        }
        Ok(())
    }
    pub async fn inspect_mcp(
        &self,
        ui: &dyn ExtensionUi,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let servers = self.catalog.servers.lock().expect("MCP catalogs").clone();
        mcp_config::inspect_servers(&self.mcp_inspection, &servers, ui, cancel).await
    }

    pub async fn refresh_mcp(&mut self, _args: &Cli) -> Result<()> {
        if let Some(update) =
            pi_rs::RegistryRefresher::refresh(self.catalog.as_ref(), &self.registry).await?
        {
            self.registry = update.registry;
            if let Some(prompt) = update.system_prompt {
                self.system_prompt = prompt;
            }
        }
        Ok(())
    }

    pub async fn authorize_pending_mcp(
        &mut self,
        args: &Cli,
        ui: &dyn ExtensionUi,
        cancel: &CancellationToken,
    ) -> Result<()> {
        while let Some(mcp_config::PendingSignIn {
            name,
            config,
            challenge,
        }) = self.mcp_sign_in.front().cloned()
        {
            let current = || {
                mcp_config::registered_configurations(args, self.registry.registered_mcp_servers())?
                    .remove(&name)
                    .map(serde_json::from_value::<pi_rs_workbench::mcp::ServerConfig>)
                    .transpose()
                    .map_err(pi_rs::Error::from)
            };
            if args
                .disable_extension
                .contains(&format!("mcp-{}", name.replace('-', "_")))
                || current()?.as_ref() != Some(&config)
            {
                self.mcp_sign_in.pop_front();
                continue;
            }
            let confirmation = ui
                .request(
                    UiRequest::Confirm {
                        title: format!("Sign in to MCP {name}"),
                        message: "Open the browser to connect this MCP server?".into(),
                        timeout: Some(120_000),
                    },
                    cancel.clone(),
                )
                .await;
            if cancel.is_cancelled() {
                return Err(pi_rs::Error::Cancelled);
            }
            if confirmation != Ok(UiResponse::Confirmed(true)) {
                self.mcp_sign_in.pop_front();
                continue;
            }
            let url = config.url.as_deref().expect("OAuth requires HTTP");
            let result = async {
                if current()?.as_ref() != Some(&config) {
                    return Err(pi_rs::Error::Config(
                        "MCP configuration changed before sign-in".into(),
                    ));
                }
                pi_rs_workbench::mcp_auth::login_configured_with_http_options(
                    &name,
                    url,
                    config.oauth_client(),
                    challenge.as_deref(),
                    &[],
                    Some(&super::http_proxy::configured_mcp(&args.settings)?),
                    |_| {},
                )
                .await?;
                if current()?.as_ref() != Some(&config) {
                    return Err(pi_rs::Error::Config(
                        "MCP configuration changed during sign-in".into(),
                    ));
                }
                pi_rs_workbench::mcp::McpExtension::connect_with_proxy_hosts(
                    &name,
                    &config,
                    &args.workspace,
                    None,
                    Some(super::http_proxy::configured_mcp(&args.settings)?),
                )
                .await
            }
            .await;
            self.mcp_sign_in.pop_front();
            let extension = match result {
                Ok(extension) => Arc::new(extension),
                Err(error) => {
                    eprintln!("MCP {name}: {error}");
                    continue;
                }
            };
            if let Err(error) = self.registry.register(extension.clone()) {
                extension.close().await;
                eprintln!("MCP {name}: {error}");
                continue;
            }
            self.catalog
                .servers
                .lock()
                .expect("MCP catalogs")
                .push(extension);
        }
        Ok(())
    }

    pub fn configure_model_options(&mut self, args: &Cli, reported: bool) -> Result<()> {
        self.provider = model_client(args, reported, true)?;
        Ok(())
    }
    pub async fn session_lifecycle(
        &self,
        args: &Cli,
        session: &Session,
        event: pi_rs::LifecycleEvent,
    ) {
        self.session_lifecycle_failed(args, session, event, |_| true)
            .await;
    }

    async fn session_lifecycle_failed(
        &self,
        args: &Cli,
        session: &Session,
        event: pi_rs::LifecycleEvent,
        include: impl FnMut(&str) -> bool + Send,
    ) -> bool {
        let mut failed = false;
        if self.registry.has_session_lifecycle_hooks() && session.host_binding().is_none() {
            pi_rs::notify_extension_lifecycle_filtered(
                &self.registry,
                event,
                &session.extension_states(),
                args.tool_timeout(),
                &mut |notification| {
                    failed |= matches!(
                        notification,
                        pi_rs::Notification::ExtensionLifecycleError { .. }
                    );
                    super::observe(notification, args.trace);
                },
                include,
            )
            .await;
        }
        failed
    }

    pub async fn session_lifecycle_owned(
        &self,
        args: &Cli,
        session: &mut Session,
        event: pi_rs::LifecycleEvent,
    ) -> Result<()> {
        if self.registry.has_session_lifecycle_hooks() && session.host_binding().is_none() {
            pi_rs::notify_extension_lifecycle_in_session(
                &self.registry,
                session,
                event,
                args.tool_timeout(),
                &mut |notification| super::observe(notification, args.trace),
            )
            .await?;
        }
        Ok(())
    }
    pub async fn close(&self) -> Result<()> {
        self.registry.shutdown().await
    }
}

pub(super) struct ShareServices<'a> {
    pub refresh: &'a super::share_refresh::Broker,
    pub processes: pi_rs_workbench::process_cleanup::Children,
}

/// Prepare an idle context on an in-memory copy. A cancellation or deadline
/// stops preparation; a configured runtime still receives its shutdown hooks.
pub(super) async fn standalone_share_snapshot(
    args: &Cli,
    source: &Session,
    refresh: &super::share_refresh::Broker,
) -> Result<super::sessions::ShareSnapshot> {
    standalone_share_snapshot_after(args, source, refresh, std::future::ready(())).await
}

async fn standalone_share_snapshot_after(
    args: &Cli,
    source: &Session,
    refresh: &super::share_refresh::Broker,
    before_configure: impl std::future::Future<Output = ()>,
) -> Result<super::sessions::ShareSnapshot> {
    if args.trust_pending {
        return Err(pi_rs::Error::Config("project trust is pending".into()));
    }
    let mut args = args.clone();
    args.restore_session_model(source)?;
    super::thinking::adjust(&mut args);
    let session = standalone_share_session_copy(source)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let services = ShareServices {
        refresh,
        processes: pi_rs_workbench::process_cleanup::Children::default(),
    };
    let result = async {
        let runtime = share_preparation_step(
            &args,
            deadline,
            refresh,
            configure_after(&args, false, before_configure, Some(&services)),
        )
        .await?;
        standalone_share_snapshot_with_runtime(&args, source, session, runtime, deadline, refresh)
            .await
    }
    .await;
    if let Err(cleanup) = services.processes.finish().await {
        if let Err(error) = result {
            eprintln!("{error}");
        }
        return Err(cleanup);
    }
    let snapshot = result?;
    if args.command_cancel.is_cancelled() {
        return Err(pi_rs::Error::Cancelled);
    }
    if tokio::time::Instant::now() >= deadline {
        return Err(pi_rs::Error::Config(
            "Radius share preparation timed out".into(),
        ));
    }
    Ok(snapshot)
}

async fn share_preparation_step<T>(
    args: &Cli,
    deadline: tokio::time::Instant,
    refresh: &super::share_refresh::Broker,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let error = tokio::select! {
        biased;
        _ = args.command_cancel.cancelled() => pi_rs::Error::Cancelled,
        _ = tokio::time::sleep_until(deadline) =>
            pi_rs::Error::Config("Radius share preparation timed out".into()),
        result = work => return result,
    };
    refresh.seal();
    Err(error)
}

fn standalone_share_session_copy(source: &Session) -> Result<Session> {
    let metadata = source
        .records()
        .iter()
        .filter_map(|record| {
            source
                .record_metadata(record.id)
                .map(|value| (record.id, value.clone()))
        })
        .collect();
    Session::from_records_with_metadata(source.records().to_vec(), metadata)
}

async fn standalone_share_snapshot_with_runtime(
    args: &Cli,
    source: &Session,
    mut session: Session,
    mut runtime: Runtime,
    deadline: tokio::time::Instant,
    refresh: &super::share_refresh::Broker,
) -> Result<super::sessions::ShareSnapshot> {
    let mut started = std::collections::BTreeSet::new();
    let result = share_preparation_step(args, deadline, refresh, async {
        if runtime
            .session_lifecycle_failed(
                args,
                &session,
                pi_rs::LifecycleEvent::SessionStart {
                    reason: pi_rs::SessionStartReason::Startup,
                    previous_session_file: None,
                },
                |extension| started.insert(extension.to_owned()),
            )
            .await
        {
            return Err(pi_rs::Error::Config(
                "Extension session start failed while preparing Radius share".into(),
            ));
        }
        runtime
            .discover_resources_checked(
                args,
                &mut session,
                pi_rs::ResourceDiscoveryReason::Startup,
                true,
            )
            .await?;
        let base = runtime.share_base();
        let (system_prompt, _) =
            source.effective_system_context(&base.system_prompt, base.tools.clone());
        Ok(super::sessions::ShareSnapshot {
            session_id: source.session_id(),
            head: source.head(),
            context: super::sessions::ShareContext {
                system_prompt,
                tools: base.tools,
            },
        })
    })
    .await;
    let shutdown_failed = runtime
        .session_lifecycle_failed(
            args,
            &session,
            pi_rs::LifecycleEvent::SessionShutdown {
                reason: pi_rs::SessionShutdownReason::Quit,
                target_session_file: None,
            },
            |extension| started.contains(extension),
        )
        .await;
    let servers = runtime
        .catalog
        .servers
        .lock()
        .expect("MCP catalogs")
        .clone();
    futures_util::future::join_all(servers.iter().map(|server| server.close())).await;
    let closed = runtime.close().await;
    let snapshot = result?;
    if shutdown_failed {
        return Err(pi_rs::Error::Config(
            "Extension session shutdown failed while preparing Radius share".into(),
        ));
    }
    closed.map(|()| snapshot)
}

fn resolve_discovered_path(
    path: &std::path::Path,
    cwd: &std::path::Path,
) -> Result<std::path::PathBuf> {
    let Some(text) = path.to_str() else {
        return Ok(if path.is_absolute() {
            path.to_owned()
        } else {
            cwd.join(path)
        });
    };
    let text = text.trim();
    if text.starts_with("file://") {
        let url = reqwest::Url::parse(text)
            .map_err(|error| pi_rs::Error::Config(format!("invalid resource URL: {error}")))?;
        if url
            .host_str()
            .is_some_and(|host| !host.is_empty() && host != "localhost")
        {
            return Err(pi_rs::Error::Config("invalid local resource URL".into()));
        }
        return url
            .to_file_path()
            .map_err(|_| pi_rs::Error::Config("invalid local resource URL".into()));
    }
    if (text == "~" || text.starts_with("~/") || (cfg!(windows) && text.starts_with("~\\")))
        && let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))
    {
        let mut result = std::path::PathBuf::from(home);
        if text.len() > 1 {
            result.push(&text[2..]);
        }
        return Ok(result);
    }
    let path = std::path::Path::new(text);
    Ok(if path.is_absolute() {
        path.to_owned()
    } else {
        cwd.join(path)
    })
}

struct LiveMcpCatalog {
    args: Cli,
    resources: std::sync::Mutex<resources::Resources>,
    servers: std::sync::Mutex<Vec<Arc<pi_rs_workbench::mcp::McpExtension>>>,
}
#[async_trait::async_trait]
impl pi_rs::RegistryRefresher for LiveMcpCatalog {
    async fn refresh(&self, current: &Registry) -> Result<Option<pi_rs::RegistryUpdate>> {
        let previous = self.servers.lock().expect("MCP catalogs").clone();
        if previous.is_empty() {
            return Ok(None);
        }
        let mut servers = Vec::with_capacity(previous.len());
        let mut registry = current.clone();
        for server in previous {
            let refreshed = server.refresh_if_changed().await?.map_or(server, Arc::new);
            // A prior in-flight refresh may have updated the shared catalog
            // while this caller still holds the previous registry snapshot.
            registry.replace(refreshed.clone())?;
            servers.push(refreshed);
        }
        let (selection, documents, hidden) = tool_discovery(&self.args, &servers)?;
        registry.replace(Arc::new(pi_rs_extensions::tool_search::ToolSearch::new(
            documents,
        )))?;
        let resources = self.resources.lock().expect("resource catalog").clone();
        let system_prompt = select_tools(
            &mut registry,
            &selection,
            &hidden,
            &resources,
            &self.args,
            &servers,
        )?;
        *self.servers.lock().expect("MCP catalogs") = servers;
        Ok(Some(pi_rs::RegistryUpdate {
            registry,
            system_prompt: Some(system_prompt),
        }))
    }
}

pub(super) fn shell_config(args: &Cli) -> ShellConfig {
    ShellConfig::new(
        args.settings.string("shellPath"),
        args.settings.string("shellCommandPrefix"),
    )
}

pub async fn configure(args: &Cli, interactive_mcp: bool) -> Result<Runtime> {
    configure_after(args, interactive_mcp, std::future::ready(()), None).await
}

async fn load_configured_extensions(
    args: &Cli,
    resources: &resources::Resources,
) -> Result<Vec<Arc<dyn pi_rs::Extension>>> {
    let mut extensions: Vec<Arc<dyn pi_rs::Extension>> = Vec::new();
    let mut extension_names: Vec<_> = args
        .extensions
        .iter()
        .filter(|entry| super::resource_paths::named_extension(entry))
        .cloned()
        .collect();
    if !args.no_extensions {
        extension_names.extend(
            args.settings
                .strings("extensions")
                .into_iter()
                .filter(|entry| super::resource_paths::named_extension(entry)),
        );
    }
    extension_names.extend(
        resources
            .extensions
            .iter()
            .map(|path| path.to_string_lossy().into_owned()),
    );
    let mut seen = std::collections::BTreeSet::new();
    let mut flags = args.extension_flags.clone();
    let mut flag_names = super::cli_compat::reserved_flag_names();
    for name in extension_names
        .iter()
        .filter(|name| seen.insert((*name).clone()))
    {
        match name.as_str() {
            "builtin:mcp" | "builtin:codemode" | "builtin:tool-search" => {}
            "notes" => extensions.push(Arc::new(pi_rs_extensions::Notes)),
            "todo" => extensions.push(Arc::new(pi_rs_extensions::TodoList)),
            name if name.starts_with("builtin:") => {
                return Err(pi_rs::Error::Config(format!(
                    "unsupported native built-in extension {name}"
                )));
            }
            _ => {
                let mut extension = pi_rs_extensions::ExecutableExtension::load_async(name).await?;
                if !args.disable_extension.iter().any(|id| id == extension.id()) {
                    for flag in extension.cli_flags() {
                        if !flag_names.insert(flag.name.clone()) {
                            return Err(pi_rs::Error::Config(format!(
                                "extension flag --{} is already registered",
                                flag.name
                            )));
                        }
                    }
                    extension.apply_flags(&mut flags)?;
                }
                extensions.push(Arc::new(extension));
            }
        }
    }
    if let Some(name) = flags.keys().next() {
        return Err(pi_rs::Error::Config(format!("unknown option --{name}")));
    }
    Ok(extensions)
}

async fn configure_after(
    args: &Cli,
    interactive_mcp: bool,
    before_preparation: impl std::future::Future<Output = ()>,
    share_services: Option<&ShareServices<'_>>,
) -> Result<Runtime> {
    before_preparation.await;
    super::packages::prepare_with_cleanup(args, share_services.map(|services| &services.processes))
        .await?;
    let local_resources = resources::Resources::load(args)?;
    for warning in &local_resources.warnings {
        eprintln!("{warning}");
    }
    let configured_extensions = load_configured_extensions(args, &local_resources).await?;
    let mcp_enabled = mcp_config::mcp_enabled(args)?;
    let mut discovered = mcp_config::discover(args, false, interactive_mcp, share_services).await?;
    let mut mcp_connections = std::mem::take(&mut discovered.connected);
    let result = async {
        let only_code = match args.settings.value()["codemode"]["mode"]
            .as_str()
            .unwrap_or("on")
        {
            "only" => true,
            "on" => false,
            _ => {
                return Err(pi_rs::Error::Config(
                    "codemode.mode must be on or only".into(),
                ));
            }
        };
        let enabled = |name: &str| super::resource_paths::builtin_enabled(args, name);
        let mut extensions: Vec<Arc<dyn pi_rs::Extension>> = Vec::new();
        let builtin_tool_search = enabled("builtin:tool-search")?;
        if builtin_tool_search {
            extensions.push(Arc::new(pi_rs_extensions::tool_search::ToolSearch::new(
                Vec::new(),
            )));
        }
        if enabled("builtin:codemode")? {
            extensions.push(Arc::new(super::operation_models::Operations::new(args)?));
            extensions.push(Arc::new(pi_rs_extensions::codemode::Codemode {
                only: only_code,
                inline_budget: args.settings.value()["codemode"]["inlineBudget"]
                    .as_u64()
                    .unwrap_or(3000)
                    .min(100_000) as usize,
                timeout: Duration::MAX,
            }));
        }
        if let Some(project) = &args.project
            && !args.no_context_files
        {
            extensions.push(Arc::new(ProjectInstructions {
                resources: ProjectResources::open(&args.workspace, args.workspace.join(project))?,
                skills: args
                    .skill
                    .iter()
                    .filter(|value| super::resource_paths::skill_path(args, value).is_none())
                    .cloned()
                    .collect(),
            }));
        }
        if !args.no_extensions {
            extensions.push(Arc::new(WorkbenchContext {
                instruction: args.instruction.clone(),
            }));
            extensions.push(Arc::new(pi_rs_plan_mode::PlanMode::new(
                ["read_text", "lookup_order", "read", "grep", "find", "ls"].map(str::to_owned),
            )));
            extensions.push(Arc::new(pi_rs_review::ReviewContext));
        }
        extensions.extend(configured_extensions);
        #[cfg(feature = "evaluation-fixtures")]
        if let Some(endpoint) = args.fixtures.as_deref() {
            extensions.push(Arc::new(pi_rs_fixture_tools::Fixtures::new(endpoint)?));
        }
        for connection in &mcp_connections {
            extensions.push(connection.clone());
        }

        let extension_sources = extensions
            .iter()
            .filter_map(|extension| {
                extension
                    .command_source()
                    .map(|path| (extension.id().to_owned(), path.to_owned()))
            })
            .collect();
        let mut disabled_servers = discovered.skipped_disabled.clone();
        if mcp_enabled {
            for extension in extensions
                .iter()
                .filter(|extension| !args.disable_extension.iter().any(|id| id == extension.id()))
            {
                for server in extension.mcp_server_declarations() {
                    disabled_servers.insert(format!("mcp-{}", server.name.replace('-', "_")));
                }
            }
        }
        let disabled = args
            .disable_extension
            .iter()
            .filter(|id| {
                !disabled_servers.contains(*id)
                    || extensions
                        .iter()
                        .any(|extension| extension.id() == id.as_str())
            })
            .cloned()
            .collect();
        let mut registry =
            Registry::compose_with_mcp_declarations(extensions, &disabled, mcp_enabled)?;
        registry.omit_default_tool_timeout(args.tool_timeout_secs.is_none());
        registry.register(Arc::new(LocalCommandPolicy(CommandPolicy {
            allow_commands: args.allow_commands,
        })))?;
        let order_config = args
            .orders
            .as_deref()
            .map(|url| (url, std::env::var("PI_RS_ORDERS_TOKEN").ok()));
        registry.register(Arc::new(Tools::new(&args.workspace, order_config)?))?;
        let shell = shell_config(args);
        registry.register(Arc::new(
            CodingTools::local(&args.workspace)?
                .with_shell_config(shell.clone())
                .with_image_resize(args.settings.resize_model_images())
                .with_image_profile(args.model_image_resize())
                .with_model_image_support(
                    args.model_profiles
                        .model(&args.provider, &args.model)
                        .and_then(|model| model.supports_images()),
                ),
        ))?;
        let extra = mcp_config::discover_registered(
            args,
            registry.registered_mcp_servers(),
            &discovered.configured_namespaces,
            interactive_mcp,
            share_services,
        )
        .await?;
        let configured_count = mcp_connections.len();
        mcp_connections.extend(extra.connected);
        discovered.sign_in.extend(extra.sign_in);
        discovered.inspection.extend(extra.inspection);
        for connection in &mcp_connections[configured_count..] {
            registry.register(connection.clone())?;
        }
        let (selection, documents, hidden) = tool_discovery(args, &mcp_connections)?;
        if builtin_tool_search {
            registry.replace(Arc::new(pi_rs_extensions::tool_search::ToolSearch::new(
                documents,
            )))?;
        }
        let system_prompt = select_tools(
            &mut registry,
            &selection,
            &hidden,
            &local_resources,
            args,
            &mcp_connections,
        )?;
        let catalog = Arc::new(LiveMcpCatalog {
            args: args.clone(),
            resources: std::sync::Mutex::new(local_resources.clone()),
            servers: std::sync::Mutex::new(mcp_connections.clone()),
        });
        registry.set_refresher(catalog.clone());
        let provider = model_client(args, super::thinking::reported(args).is_some(), true)?;

        // Subprocesses must inherit the frontend's live controls rather than
        // re-resolving them from project settings when a worker is replaced.
        let live_controls = if args.stdin_control {
            std::env::var(super::process::LIVE_CONTROLS_ENV)
                .ok()
                .map(|value| serde_json::from_str::<(Option<bool>, Option<bool>)>(&value))
                .transpose()
                .map_err(|error| {
                    pi_rs::Error::Config(format!("invalid worker live controls: {error}"))
                })?
        } else {
            None
        };
        let mut retry_settings = args.settings.retry_settings();
        if let Some((_, Some(enabled))) = live_controls {
            retry_settings.enabled = enabled;
        }
        Ok(Runtime {
            retry: pi_rs::RetryControl::new(retry_settings),
            auto_compaction: pi_rs::AutoCompactionControl::new(
                live_controls
                    .and_then(|(enabled, _)| enabled)
                    .unwrap_or_else(|| args.settings.auto_compaction_enabled()),
            ),
            provider,
            registry,
            extension_sources,
            shell,
            system_prompt,
            resources: local_resources,
            resource_origins: ResourceOrigins::default(),
            catalog,
            mcp_sign_in: discovered.sign_in.into(),
            mcp_inspection: discovered.inspection,
        })
    }
    .await;
    if result.is_err() {
        futures_util::future::join_all(mcp_connections.iter().map(|connection| connection.close()))
            .await;
    }
    result
}

fn effective_limits(args: &Cli) -> Limits {
    Limits {
        model_requests: args.max_requests,
        output_tokens_per_request: args.effective_max_output_tokens(),
        output_tokens_per_run: args.output_budget,
        model_timeout: Duration::from_secs(args.model_timeout_secs),
        tool_timeout: args.tool_timeout(),
        context_window: args.effective_context_window(),
        compaction_tokens: None,
    }
}

pub(super) fn model_client(
    args: &Cli,
    reported: bool,
    observe: bool,
) -> Result<super::image_policy::WorkbenchModel> {
    let client = args
        .connect_provider(&args.provider, Some(&args.model))?
        .with_context_probes(args.allow_model_probes())
        .with_model_options(args, reported)?;
    let strict_policy = client.uses_final_strict_policy().then(|| {
        Arc::new(super::image_policy::FinalStrictPolicy {
            profiles: args.model_profiles.clone(),
            provider: args.provider.clone(),
            selected_model: args.model.clone(),
            explicit_endpoint: args.explicit_endpoint_for(&args.provider).is_some(),
            bound_support: None,
        })
    });
    Ok(super::image_policy::WorkbenchModel {
        strict_policy,
        client: if observe { client.observe() } else { client },
        block_images: args.settings.block_model_images(),
        supports_images: args
            .model_profiles
            .model(&args.provider, &args.model)
            .and_then(|model| model.supports_images()),
        price: args
            .model_profiles
            .model(&args.provider, &args.model)
            .and_then(|model| model.cost),
        session_affinity: args.model_profiles.session_affinity(
            &args.provider,
            &args.model,
            args.model_endpoint_for(&args.provider, Some(&args.model)),
        ),
    })
}

fn tool_discovery(
    args: &Cli,
    mcp_connections: &[Arc<pi_rs_workbench::mcp::McpExtension>],
) -> Result<(
    super::tool_selection::Selection,
    Vec<pi_rs_extensions::tool_search::SearchDocument>,
    std::collections::BTreeSet<String>,
)> {
    let mut selection = super::tool_selection::Selection::new(args)?;
    let documents: Vec<_> = mcp_connections
        .iter()
        .flat_map(|connection| {
            let tools = if selection.declares_mcp() {
                connection.deferred_tools()
            } else {
                connection
                    .tools()
                    .into_iter()
                    .map(|tool| tool.spec())
                    .collect()
            };
            tools
                .into_iter()
                .filter(|tool| selection.includes(&tool.name))
                .map(|tool| pi_rs_extensions::tool_search::SearchDocument {
                    tool,
                    namespace: format!("{} {}", connection.server_name, connection.description),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let hidden: std::collections::BTreeSet<_> = documents
        .iter()
        .map(|document| document.tool.name.clone())
        .collect();
    if mcp_connections
        .iter()
        .any(|connection| connection.has_deferred())
    {
        selection.enable_automatic("tool_search");
    }
    if mcp_connections
        .iter()
        .any(|connection| connection.has_codemode())
        && mcp_config::auto_enable_codemode(args)?
    {
        selection.enable_automatic("codemode");
    }
    Ok((selection, documents, hidden))
}

fn select_tools(
    registry: &mut Registry,
    selection: &super::tool_selection::Selection,
    hidden: &std::collections::BTreeSet<String>,
    resources: &resources::Resources,
    args: &Cli,
    mcp_connections: &[Arc<pi_rs_workbench::mcp::McpExtension>],
) -> Result<String> {
    let only_code = args.settings.value()["codemode"]["mode"].as_str() == Some("only");
    registry.retain_tools(|tool| selection.callable(&tool.name));
    let active: Vec<_> = registry
        .tool_specs()
        .into_iter()
        .filter(|tool| tool.name != super::operation_models::TOOL && selection.includes(&tool.name))
        .map(|tool| tool.name)
        .collect();
    registry.set_active_tools(active)?;
    let visible_tools: Vec<_> = registry
        .active_tool_specs()
        .into_iter()
        .filter(|tool| {
            !hidden.contains(&tool.name)
                && (!only_code || matches!(tool.name.as_str(), "codemode" | "tool_search"))
        })
        .collect();
    let mut system_prompt =
        resources.system_prompt(DEFAULT_SYSTEM_PROMPT, &visible_tools, &args.workspace);
    if !hidden.is_empty() {
        system_prompt.push_str("\n\n<mcp_servers>\nUse codemode searchTools() or tool_search to discover tools from these servers.\n");
        for connection in mcp_connections {
            system_prompt.push_str(&format!(
                "- {}: {}\n",
                connection.server_name, connection.description
            ));
        }
        system_prompt.push_str("</mcp_servers>");
    }
    Ok(system_prompt)
}

struct LocalCommandPolicy(CommandPolicy);
#[async_trait::async_trait]
impl pi_rs::Extension for LocalCommandPolicy {
    fn id(&self) -> &str {
        self.0.id()
    }
    async fn before_tool(
        &self,
        name: &str,
        arguments: &serde_json::Value,
        context: &pi_rs::ToolContext,
        state: Option<&pi_rs::ExtensionState>,
    ) -> pi_rs::ExtensionResult<pi_rs::Decision> {
        if matches!(name, "bash" | "powershell") {
            Ok(pi_rs::Decision::Allow)
        } else {
            self.0.before_tool(name, arguments, context, state).await
        }
    }
}

#[cfg(test)]
mod standalone_share_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn share_session_copy_preserves_imported_provenance_and_compaction_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.jsonl");
        std::fs::write(&path, concat!(
            "{\"type\":\"session\",\"version\":3}\n",
            "{\"type\":\"message\",\"id\":\"u\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"question\"}}\n",
            "{\"type\":\"compaction\",\"id\":\"c\",\"parentId\":\"u\",\"firstKeptEntryId\":\"u\",\"summary\":\"summary\",\"tokensBefore\":42,\"fromHook\":true,\"details\":{\"source\":\"extension\"}}\n",
        )).unwrap();
        let source = crate::session_import::read(&path).unwrap();
        let expected_counts = source.validated_pi_conversion_counts().unwrap();
        let copy = standalone_share_session_copy(&source).unwrap();
        assert_eq!(copy.session_id(), source.session_id());
        assert_eq!(copy.head(), source.head());
        assert_eq!(
            copy.validated_pi_conversion_counts().unwrap(),
            expected_counts
        );
        let compacted = copy
            .records()
            .iter()
            .find(|record| matches!(record.event, pi_rs::Event::Compacted { .. }))
            .unwrap();
        assert_eq!(
            copy.record_metadata(compacted.id),
            Some(&serde_json::json!({"fromExtension":true,"details":{"source":"extension"}}))
        );
    }

    struct LifecycleRecorder {
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
        invalid_prompt: Option<std::path::PathBuf>,
    }

    #[async_trait::async_trait]
    impl pi_rs::Extension for LifecycleRecorder {
        fn id(&self) -> &str {
            "share-lifecycle-recorder"
        }
        fn handles_lifecycle(&self) -> bool {
            true
        }
        fn handles_session_lifecycle(&self) -> bool {
            true
        }
        fn handles_resource_discovery(&self) -> bool {
            true
        }
        async fn lifecycle(
            &self,
            event: &pi_rs::LifecycleEvent,
            _: Option<&pi_rs::ExtensionState>,
        ) -> pi_rs::ExtensionResult<()> {
            let event = match event {
                pi_rs::LifecycleEvent::SessionStart {
                    reason: pi_rs::SessionStartReason::Startup,
                    previous_session_file: None,
                } => "start",
                pi_rs::LifecycleEvent::SessionShutdown {
                    reason: pi_rs::SessionShutdownReason::Quit,
                    target_session_file: None,
                } => "shutdown",
                _ => "unexpected",
            };
            self.events.lock().unwrap().push(event);
            Ok(())
        }
        async fn discover_resources(
            &self,
            _: &pi_rs::ResourceDiscoveryEvent,
            _: Option<&pi_rs::ExtensionState>,
        ) -> pi_rs::ExtensionResult<pi_rs::DiscoveredResources> {
            self.events.lock().unwrap().push("discover");
            if let Some(path) = &self.invalid_prompt {
                std::fs::write(path, vec![b'x'; 64 * 1024 + 1]).unwrap();
            }
            Ok(pi_rs::DiscoveredResources::default())
        }
        async fn shutdown(&self) -> pi_rs::ExtensionResult<()> {
            self.events.lock().unwrap().push("close");
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn standalone_share_pairs_session_start_and_shutdown_on_success_and_failure() {
        for fail in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.jsonl");
            let prompt = directory.path().join("prompt.md");
            std::fs::write(&prompt, "test prompt").unwrap();
            let source = Session::open(&path).unwrap();
            let original = std::fs::read(&path).unwrap();
            let mut args = Cli::try_parse_from([
                "pi",
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--endpoint",
                "http://127.0.0.1:1/v1/chat/completions",
            ])
            .unwrap();
            args.workspace = directory.path().to_owned();
            args.session = path.clone();
            args.system_prompt = Some(prompt.clone());
            let mut runtime = configure(&args, false).await.unwrap();
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            runtime
                .registry
                .register(Arc::new(LifecycleRecorder {
                    events: events.clone(),
                    invalid_prompt: fail.then_some(prompt),
                }))
                .unwrap();
            let session = Session::from_records(source.records().to_vec()).unwrap();
            let result = standalone_share_snapshot_with_runtime(
                &args,
                &source,
                session,
                runtime,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &super::super::share_refresh::Broker::new(),
            )
            .await;
            assert_eq!(result.is_err(), fail);
            assert_eq!(
                *events.lock().unwrap(),
                ["start", "discover", "shutdown", "close"]
            );
            assert_eq!(std::fs::read(path).unwrap(), original);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn standalone_share_keeps_active_tools_after_imported_system_checkpoint() {
        for checkpoint in [
            serde_json::json!({"type":"message","id":"s","parentId":null,"message":{"role":"system","content":[{"type":"text","text":"checkpoint prompt"}]}}),
            serde_json::json!({"type":"compaction","id":"c","parentId":null,"firstKeptEntryId":"c","summary":"prior","systemMessage":{"role":"system","content":[{"type":"text","text":"checkpoint prompt"}]}}),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("source.jsonl");
            let entries = [
                serde_json::json!({"type":"session","version":3}),
                checkpoint,
            ];
            std::fs::write(
                &path,
                entries
                    .iter()
                    .map(|entry| entry.to_string() + "\n")
                    .collect::<String>(),
            )
            .unwrap();
            let original = std::fs::read(&path).unwrap();
            let source = super::super::session_import::read(&path).unwrap();
            let mut args = Cli::try_parse_from([
                "pi",
                "--offline",
                "--no-trust",
                "--no-mcp",
                "--no-extensions",
                "--no-skills",
                "--no-context-files",
                "--endpoint",
                "http://127.0.0.1:1/v1/chat/completions",
            ])
            .unwrap();
            args.workspace = directory.path().to_owned();
            args.session = path.clone();
            let runtime = configure(&args, false).await.unwrap();
            let expected = runtime.share_base().tools;
            assert!(!expected.is_empty());
            let session = Session::from_records(source.records().to_vec()).unwrap();
            let snapshot = standalone_share_snapshot_with_runtime(
                &args,
                &source,
                session,
                runtime,
                tokio::time::Instant::now() + Duration::from_secs(5),
                &super::super::share_refresh::Broker::new(),
            )
            .await
            .unwrap();
            assert_eq!(snapshot.context.system_prompt, "checkpoint prompt");
            assert_eq!(snapshot.context.tools, expected);
            assert_eq!(std::fs::read(path).unwrap(), original);
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn slow_preparation_waits_for_metadata_without_detaching() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let session = Session::open(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let mut args = Cli::try_parse_from([
            "pi",
            "--offline",
            "--no-trust",
            "--no-mcp",
            "--no-extensions",
            "--no-skills",
            "--no-context-files",
            "--endpoint",
            "http://127.0.0.1:1/v1/chat/completions",
        ])
        .unwrap();
        args.workspace = directory.path().to_owned();
        args.session = path.clone();
        let (release, gate) = tokio::sync::oneshot::channel();
        let refresh = super::super::share_refresh::Broker::new();
        let mut preparation = Box::pin(standalone_share_snapshot_after(
            &args,
            &session,
            &refresh,
            async {
                let _ = gate.await;
            },
        ));
        assert!(
            tokio::time::timeout(Duration::from_secs(11), &mut preparation)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        let snapshot = preparation.await.unwrap();
        assert_eq!(snapshot.session_id, session.session_id());
        assert_eq!(snapshot.head, session.head());
        assert_eq!(std::fs::read(path).unwrap(), original);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn standalone_share_deadline_stops_preparation_before_configure() {
        let args = Cli::try_parse_from(["pi", "--offline", "--no-trust"]).unwrap();
        let source = Session::with_store(pi_rs::MemoryStore::default()).unwrap();
        let refresh = super::super::share_refresh::Broker::new();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let Err(error) = standalone_share_snapshot_after(&args, &source, &refresh, async {
            let _ = gate.await;
        })
        .await
        else {
            panic!("preparation should time out");
        };
        assert!(
            error
                .to_string()
                .contains("Radius share preparation timed out")
        );
        assert!(release.send(()).is_err());
        refresh.finish().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_preparation_drops_it_without_starting_configure() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let session = Session::open(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        let mut args = Cli::try_parse_from(["pi", "--offline", "--no-trust"]).unwrap();
        args.session = path.clone();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let refresh = super::super::share_refresh::Broker::new();
        let mut preparation = Box::pin(standalone_share_snapshot_after(
            &args,
            &session,
            &refresh,
            async {
                let _ = gate.await;
            },
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut preparation)
                .await
                .is_err()
        );
        drop(preparation);
        assert!(release.send(()).is_err());
        assert_eq!(std::fs::read(path).unwrap(), original);
    }
}

#[cfg(test)]
mod resource_origin_tests {
    use super::*;

    #[test]
    fn runtime_final_strict_policy_is_limited_to_openai_adapters() {
        use clap::Parser;
        for (api, path, expected) in [
            ("openai-responses", "v1", true),
            ("openai-completions", "v1", true),
            ("anthropic-messages", "v1/messages", false),
            ("google-generative-ai", "v1", false),
        ] {
            let endpoint = format!("http://127.0.0.1:1234/{path}");
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles =
                super::super::model_profiles::Profiles::parse(&json!({"providers":{"fixture":{
                    "api":api, "baseUrl":endpoint, "models":[{"id":"a"}]
                }}}))
                .unwrap();
            args.provider = args.model_profiles.provider("fixture").unwrap();
            args.model = "a".into();
            args.endpoint = endpoint;
            args.api_key = Some("fixture".into());
            let model = model_client(&args, false, false).unwrap();
            assert_eq!(model.strict_policy.is_some(), expected, "{api}");
        }
    }

    struct InlineCommand;
    impl pi_rs::Extension for InlineCommand {
        fn id(&self) -> &str {
            "custom"
        }
        fn commands(&self) -> &[&str] {
            &["set"]
        }
    }

    #[test]
    fn command_provenance_distinguishes_inline_from_builtin() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(pi_rs_extensions::Notes))
            .unwrap();
        registry.register(Arc::new(InlineCommand)).unwrap();
        let commands = registry.registered_commands();
        let builtin = commands
            .iter()
            .find(|command| command.extension_id == "notes" && command.name == "set")
            .unwrap();
        let inline = commands
            .iter()
            .find(|command| command.extension_id == "custom")
            .unwrap();
        assert_eq!(builtin.invocation_name, "set:1");
        assert_eq!(inline.invocation_name, "set:2");
        assert_eq!(
            synthetic_command_source(builtin),
            json!({"path":"builtin:notes", "source":"builtin", "scope":"user", "origin":"top-level"})
        );
        assert_eq!(
            synthetic_command_source(inline),
            json!({"path":"<inline:custom>", "source":"inline", "scope":"temporary", "origin":"top-level"})
        );
    }

    #[test]
    fn discovered_resource_sources_distinguish_inline_builtin_and_files() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(pi_rs_extensions::Notes))
            .unwrap();
        registry.register(Arc::new(InlineCommand)).unwrap();
        let mut item = pi_rs::ExtensionResources {
            extension_id: "custom".into(),
            source: None,
            resources: Default::default(),
        };
        assert_eq!(
            discovered_resource_source(&item, &registry),
            "extension:inline:custom"
        );
        item.extension_id = "notes".into();
        assert_eq!(
            discovered_resource_source(&item, &registry),
            "extension:builtin:notes"
        );
        item.source = Some("/extensions/example.ts".into());
        assert_eq!(
            discovered_resource_source(&item, &registry),
            "extension:example"
        );
        item.source = Some("/extensions/example.js".into());
        assert_eq!(
            discovered_resource_source(&item, &registry),
            "extension:example"
        );
        item.source = Some("/extensions/native.exe".into());
        assert_eq!(
            discovered_resource_source(&item, &registry),
            "extension:native.exe"
        );
    }

    #[test]
    fn extension_paths_resolve_against_workspace_with_home_and_file_urls() {
        let directory = tempfile::tempdir().unwrap();
        let cwd = directory.path();
        assert_eq!(
            resolve_discovered_path(std::path::Path::new(" resources/skills "), cwd).unwrap(),
            cwd.join("resources/skills")
        );
        let target = cwd.join("a b.md");
        let url = reqwest::Url::from_file_path(&target).unwrap();
        assert_eq!(
            resolve_discovered_path(std::path::Path::new(url.as_str()), cwd).unwrap(),
            target
        );
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            assert_eq!(
                resolve_discovered_path(std::path::Path::new("~/example"), cwd).unwrap(),
                std::path::PathBuf::from(home).join("example")
            );
        }
        assert!(
            resolve_discovered_path(std::path::Path::new("file://invalid-host/x"), cwd).is_err()
        );
    }

    #[test]
    fn discovered_resource_origin_uses_real_path_and_extension_source() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let resource = base.join("skills/example/SKILL.md");
        std::fs::create_dir_all(resource.parent().unwrap()).unwrap();
        std::fs::write(&resource, "content").unwrap();
        let origins = vec![ResourceOrigin {
            root: base.join("skills"),
            source: "extension:fixture".into(),
            base_dir: Some(base.clone()),
        }];
        let source = ResourceOrigin::source_info(&resource, &origins).unwrap();
        assert_eq!(source["path"], json!(resource));
        assert_eq!(source["source"], "extension:fixture");
        assert_eq!(source["scope"], "temporary");
        assert_eq!(source["origin"], "top-level");
        assert_eq!(source["baseDir"], json!(base));
        assert!(ResourceOrigin::source_info(&base.join("skills-other/file"), &origins).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn discovered_resource_origin_matches_target_but_reports_selected_path() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let target = base.join("target/SKILL.md");
        let alias = base.join("alias/SKILL.md");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::create_dir_all(alias.parent().unwrap()).unwrap();
        std::fs::write(&target, "content").unwrap();
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        let origins = vec![ResourceOrigin {
            root: target,
            source: "extension:fixture".into(),
            base_dir: None,
        }];
        assert_eq!(
            ResourceOrigin::source_info(&alias, &origins).unwrap()["path"],
            json!(alias)
        );
    }
}
