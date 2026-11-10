//! Shared local runtime construction for print, RPC and terminal sessions.
use super::providers::Client;
use super::{Cli, DEFAULT_SYSTEM_PROMPT, mcp_config, output, resources};
use pi_rs::{
    Agent, CancellationToken, Extension, ExtensionUi, Limits, ProjectResources, Registry, Result,
    UiRequest, UiResponse,
};
use pi_rs_workbench::{CommandPolicy, ProjectInstructions, Tools, WorkbenchContext};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub struct Runtime {
    pub auto_compaction: pi_rs::AutoCompactionControl,
    pub retry: pi_rs::RetryControl,
    provider: Client,
    pub registry: Registry,
    system_prompt: String,
    pub resources: resources::Resources,
    catalog: Arc<LiveMcpCatalog>,
    mcp_sign_in: std::collections::VecDeque<(String, pi_rs_workbench::mcp::ServerConfig)>,
}
impl Runtime {
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
                "sourceInfo": source_info(&prompt.path, &self.resources.packages.prompts),
            }));
        }
        for (name, skill) in &self.resources.skills {
            commands.push(json!({
                "name": format!("skill:{name}"),
                "description": skill.description,
                "source": "skill",
                "sourceInfo": source_info(&skill.path, &self.resources.packages.skills),
            }));
        }
        json!({"commands":commands})
    }

    pub fn agent<'a>(&'a self, args: &'a Cli) -> Agent<'a> {
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
                output_tokens_per_request: args.max_output_tokens,
                output_tokens_per_run: args.output_budget,
                model_timeout: Duration::from_secs(args.model_timeout_secs),
                tool_timeout: args.tool_timeout(),
                context_window: args.context_window,
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
        while let Some((name, config)) = self.mcp_sign_in.front().cloned() {
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
                pi_rs_workbench::mcp_auth::login(&name, url, None, &[], |_| {}).await?;
                if current()?.as_ref() != Some(&config) {
                    return Err(pi_rs::Error::Config(
                        "MCP configuration changed during sign-in".into(),
                    ));
                }
                pi_rs_workbench::mcp::McpExtension::connect(&name, &config, &args.workspace).await
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

    pub fn set_thinking(&mut self, thinking: Option<&str>, reported: bool) {
        self.provider = self.provider.clone().with_thinking(thinking, reported);
    }
    pub async fn close(&self) -> Result<()> {
        self.registry.shutdown().await
    }
}

struct LiveMcpCatalog {
    args: Cli,
    resources: resources::Resources,
    base_prompt: String,
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
        let system_prompt = select_tools(
            &mut registry,
            &selection,
            &hidden,
            (&self.resources, &self.base_prompt),
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

pub async fn configure(args: &Cli, interactive_mcp: bool) -> Result<Runtime> {
    super::packages::prepare(args).await?;
    let mut local_resources = resources::Resources::load(args)?;
    for argument in &args.append_system_prompt {
        local_resources.append_system.push_str("\n\n");
        local_resources
            .append_system
            .push_str(&resources::prompt_argument(&args.workspace, argument)?);
    }
    for warning in &local_resources.warnings {
        eprintln!("{warning}");
    }
    let base_prompt = args
        .system_prompt
        .as_ref()
        .map(|value| resources::prompt_argument(&args.workspace, &value.to_string_lossy()))
        .transpose()?
        .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.into());
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
                _ => extensions.push(Arc::new(pi_rs_extensions::ExecutableExtension::load(name)?)),
            }
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
        registry.register(Arc::new(pi_rs_workbench::coding::CodingTools::local(
            &args.workspace,
        )?))?;
        let system_prompt = select_tools(
            &mut registry,
            &selection,
            &hidden,
            (&local_resources, &base_prompt),
            args,
            &mcp_connections,
        )?;
        let catalog = Arc::new(LiveMcpCatalog {
            args: args.clone(),
            resources: local_resources.clone(),
            base_prompt: base_prompt.clone(),
            servers: std::sync::Mutex::new(mcp_connections.clone()),
        });
        registry.set_refresher(catalog.clone());
        let provider = args
            .provider
            .connect(&args.endpoint, args.model_token())?
            .with_context_probes(args.allow_model_probes())
            .with_thinking(
                args.thinking.as_deref(),
                super::thinking::reported(args).is_some(),
            )
            .observe(args.trace);

        Ok(Runtime {
            retry: pi_rs::RetryControl::new(args.settings.retry_settings()),
            auto_compaction: pi_rs::AutoCompactionControl::new(
                args.settings.auto_compaction_enabled(),
            ),
            provider,
            registry,
            system_prompt,
            resources: local_resources,
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
    prompt: (&resources::Resources, &str),
    args: &Cli,
    mcp_connections: &[Arc<pi_rs_workbench::mcp::McpExtension>],
) -> Result<String> {
    let (resources, base_prompt) = prompt;
    let only_code = args.settings.value()["codemode"]["mode"].as_str() == Some("only");
    registry.retain_tools(|tool| selection.callable(&tool.name));
    let active: Vec<_> = registry
        .tool_specs()
        .into_iter()
        .filter(|tool| selection.includes(&tool.name))
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
    let mut system_prompt = resources.system_prompt(
        base_prompt,
        args.system_prompt.is_some(),
        &visible_tools,
        &args.workspace,
    );
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
