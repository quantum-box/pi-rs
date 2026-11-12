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
    shell: ShellConfig,
    system_prompt: String,
    pub resources: resources::Resources,
    resource_origins: ResourceOrigins,
    catalog: Arc<LiveMcpCatalog>,
    mcp_sign_in: std::collections::VecDeque<mcp_config::PendingSignIn>,
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
        let path = path.canonicalize().unwrap_or_else(|_| path.to_owned());
        let origin = origins
            .iter()
            .find(|origin| path.starts_with(&origin.root))?;
        let mut result =
            json!({"path":path,"source":origin.source,"scope":"temporary","origin":"top-level"});
        if let Some(base) = &origin.base_dir {
            result["baseDir"] = json!(base);
        }
        Some(result)
    }
}

impl Runtime {
    pub async fn discover_resources(
        &mut self,
        args: &Cli,
        session: &Session,
        reason: pi_rs::ResourceDiscoveryReason,
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
        let results = self
            .registry
            .discover_resources(
                &event,
                &session.extension_states(),
                args.tool_timeout(),
                |extension, error| eprintln!("Extension {extension} (resources_discover): {error}"),
            )
            .await;
        let mut discovered = pi_rs::DiscoveredResources::default();
        let mut origins = ResourceOrigins::default();
        for item in results {
            let source = item
                .source
                .as_ref()
                .and_then(|path| path.file_name())
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| item.extension_id.clone());
            let source = format!("extension:{source}");
            let base_dir = item
                .source
                .as_deref()
                .and_then(std::path::Path::parent)
                .map(std::path::Path::to_path_buf);
            let resolve = |path: std::path::PathBuf| {
                let path = match resolve_discovered_path(&path, &event.cwd) {
                    Ok(path) => path,
                    Err(error) => {
                        eprintln!("Extension {}: {error}", item.extension_id);
                        return None;
                    }
                };
                if path.exists() {
                    Some(path)
                } else {
                    eprintln!(
                        "Extension {}: resource {} was not found",
                        item.extension_id,
                        path.display()
                    );
                    None
                }
            };
            let add = |paths: Vec<std::path::PathBuf>,
                       target: &mut Vec<std::path::PathBuf>,
                       records: &mut Vec<ResourceOrigin>| {
                for path in paths.into_iter().filter_map(&resolve) {
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
                None => json!({
                    "path": format!("builtin:{}", command.extension_id),
                    "source": "builtin",
                    "scope": "user",
                    "origin": "top-level",
                }),
            };
            let mut entry =
                json!({"name":command.invocation_name,"source":"extension","sourceInfo":source});
            if let Some(description) = command.description {
                entry["description"] = json!(description);
            }
            commands.push(entry);
        }
        for (name, prompt) in &self.resources.prompts {
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
        json!({"commands":commands})
    }

    pub fn agent<'a>(&'a self, args: &'a Cli, session_id: uuid::Uuid) -> Agent<'a> {
        self.shell.set_context(ShellContext {
            id: session_id,
            path: (!args.no_session && !args.ephemeral_worker_session)
                .then(|| args.session.clone()),
            provider: args.provider.id().into(),
            model: args.model.clone(),
            reasoning_level: Some(args.thinking.clone().unwrap_or_else(|| "off".into())),
        });
        self.auto_compaction.set_token_settings(
            args.settings
                .compaction_tokens(args.provider.id(), &args.model),
        );
        Agent {
            model: &self.provider,
            model_name: &args.model,
            registry: &self.registry,
            system_prompt: &self.system_prompt,
            limits: Limits {
                model_requests: args.max_requests,
                output_tokens_per_request: args.effective_max_output_tokens(),
                output_tokens_per_run: args.output_budget,
                model_timeout: Duration::from_secs(args.model_timeout_secs),
                tool_timeout: args.tool_timeout(),
                context_window: args.effective_context_window(),
            },
        }
    }
    pub fn mcp_status(&self, args: &Cli) -> Result<()> {
        let servers = self.catalog.servers.lock().expect("MCP catalogs");
        for (name, value) in mcp_config::configurations(args)? {
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
                mcp_config::configurations(args)?
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
                pi_rs_workbench::mcp::McpExtension::connect_with_http_options(
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
        if self.registry.has_session_lifecycle_hooks() && session.host_binding().is_none() {
            pi_rs::notify_extension_lifecycle(
                &self.registry,
                event,
                &session.extension_states(),
                args.tool_timeout(),
                &mut |notification| super::observe(notification, args.trace),
            )
            .await;
        }
    }
    pub async fn close(&self) -> Result<()> {
        self.registry.shutdown().await
    }
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
    super::packages::prepare(args).await?;
    let local_resources = resources::Resources::load(args)?;
    for warning in &local_resources.warnings {
        eprintln!("{warning}");
    }
    let discovered = mcp_config::discover(args, false, interactive_mcp).await?;
    let mcp_connections = discovered.connected;
    let result = async {
        let (selection, documents, hidden) = tool_discovery(args, &mcp_connections)?;
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
        if enabled("builtin:tool-search")? {
            extensions.push(Arc::new(pi_rs_extensions::tool_search::ToolSearch::new(
                documents,
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
                timeout: args.tool_timeout(),
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
            local_resources
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
                    let mut extension = pi_rs_extensions::ExecutableExtension::load(name)?;
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
        #[cfg(feature = "evaluation-fixtures")]
        if let Some(endpoint) = args.fixtures.as_deref() {
            extensions.push(Arc::new(pi_rs_fixture_tools::Fixtures::new(endpoint)?));
        }
        for connection in &mcp_connections {
            extensions.push(connection.clone());
        }

        let mut registry = Registry::compose(
            extensions,
            &args.disable_extension.iter().cloned().collect(),
        )?;
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

        Ok(Runtime {
            retry: pi_rs::RetryControl::new(args.settings.retry_settings()),
            auto_compaction: pi_rs::AutoCompactionControl::new(
                args.settings.auto_compaction_enabled(),
            ),
            provider,
            registry,
            shell,
            system_prompt,
            resources: local_resources,
            resource_origins: ResourceOrigins::default(),
            catalog,
            mcp_sign_in: discovered.sign_in.into(),
        })
    }
    .await;
    if result.is_err() {
        for connection in mcp_connections {
            connection.close().await;
        }
    }
    result
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
    Ok(super::image_policy::WorkbenchModel {
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
mod resource_origin_tests {
    use super::*;

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
}
