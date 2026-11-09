//! Shared local runtime construction for print, RPC and terminal sessions.
use super::{Cli, DEFAULT_SYSTEM_PROMPT, mcp_config, output, resources};
use pi_rs::{Agent, Extension, Limits, ProjectResources, Registry, Result, ollama::Ollama};
use pi_rs_workbench::{CommandPolicy, ProjectInstructions, Tools, WorkbenchContext};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

pub struct Runtime {
    provider: Ollama,
    pub registry: Registry,
    system_prompt: String,
    pub resources: resources::Resources,
    servers: Vec<Arc<pi_rs_workbench::mcp::McpExtension>>,
}
impl Runtime {
    pub fn agent<'a>(&'a self, args: &'a Cli) -> Agent<'a> {
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
                tool_timeout: Duration::from_secs(args.tool_timeout_secs),
                context_window: args.context_window,
            },
        }
    }
    pub fn mcp_status(&self, args: &Cli) -> Result<()> {
        for (name, value) in mcp_config::configurations(args)? {
            if let Some(server) = self
                .servers
                .iter()
                .find(|server| server.server_name == name)
            {
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
    pub fn set_thinking(&mut self, thinking: Option<&str>) {
        self.provider = self.provider.clone().with_reasoning_effort(thinking);
    }
    pub async fn close(&self) -> Result<()> {
        self.registry.shutdown().await
    }
}

pub async fn configure(args: &Cli) -> Result<Runtime> {
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
    let mcp_connections = mcp_config::connect_all(args, false).await?;
    let result = async {
        let mut selection = super::tool_selection::Selection::new(args)?;
        let documents: Vec<_> = mcp_connections
            .iter()
            .flat_map(|connection| {
                let tools = if selection.declares_mcp() {
                    connection.deferred_tools()
                } else {
                    connection.tools().into_iter().map(|tool| tool.spec()).collect()
                };
                tools.into_iter()
                    .filter(|tool| selection.includes(&tool.name))
                    .map(|tool| pi_rs_extensions::tool_search::SearchDocument {
                        tool,
                        namespace: format!("{} {}", connection.server_name, connection.description),
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let hidden: std::collections::BTreeSet<_> = documents.iter()
            .map(|document| document.tool.name.clone())
            .collect();
        if mcp_connections.iter().any(|connection| connection.has_deferred()) {
            selection.enable_automatic("tool_search");
        }
        if mcp_connections.iter().any(|connection| connection.has_codemode()) && mcp_config::auto_enable_codemode(args)? {
            selection.enable_automatic("codemode");
        }
        let only_code = match args.settings.value()["codemode"]["mode"].as_str().unwrap_or("on") {
            "only" => true,
            "on" => false,
            _ => return Err(pi_rs::Error::Config("codemode.mode must be on or only".into())),
        };
        let enabled = |name: &str| !args.no_extensions || args.extensions.iter().any(|entry| entry == name);
        let mut extensions: Vec<Arc<dyn pi_rs::Extension>> = Vec::new();
        if enabled("builtin:tool-search") || !mcp_connections.is_empty() {
            extensions.push(Arc::new(pi_rs_extensions::tool_search::ToolSearch::new(documents)));
        }
        if enabled("builtin:codemode") || mcp_connections.iter().any(|connection| connection.has_codemode()) {
            extensions.push(Arc::new(pi_rs_extensions::codemode::Codemode {
                only: only_code,
                inline_budget: args.settings.value()["codemode"]["inlineBudget"].as_u64().unwrap_or(3000).min(100_000) as usize,
                timeout: Duration::from_secs(args.tool_timeout_secs),
            }));
        }
        if let Some(project) = &args.project
            && !args.no_context_files
        {
            extensions.push(Arc::new(ProjectInstructions {
                resources: ProjectResources::open(&args.workspace, args.workspace.join(project))?,
                skills: args.skill.clone(),
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
        let mut extension_names = args.extensions.clone();
        if !args.no_extensions {
            extension_names.extend(
                local_resources
                    .packages
                    .extensions
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned()),
            );
        }
        for name in extension_names
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
        {
            match name.as_str() {
                "builtin:mcp" | "builtin:codemode" | "builtin:tool-search" => {},
                "notes" => extensions.push(Arc::new(pi_rs_extensions::Notes)),
                "todo" => extensions.push(Arc::new(pi_rs_extensions::TodoList)),
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
        registry.retain_tools(|tool| selection.callable(&tool.name));
        let active: Vec<_> = registry.tool_specs().into_iter().filter(|tool| selection.includes(&tool.name)).map(|tool| tool.name).collect();
        registry.set_active_tools(active)?;
        let visible_tools: Vec<_> = registry.active_tool_specs().into_iter().filter(|tool| !hidden.contains(&tool.name) && (!only_code || matches!(tool.name.as_str(), "codemode" | "tool_search"))).collect();
        let mut system_prompt = local_resources.system_prompt(
            &base_prompt,
            args.system_prompt.is_some(),
            &visible_tools,
            &args.workspace,
        );
        if !hidden.is_empty() {
            system_prompt.push_str("\n\n<mcp_servers>\nUse codemode searchTools() or tool_search to discover tools from these servers.\n");
            for connection in &mcp_connections {
                system_prompt.push_str(&format!("- {}: {}\n", connection.server_name, connection.description));
            }
            system_prompt.push_str("</mcp_servers>");
        }
        let show_reasoning = args.trace;
        let provider = Ollama::new(&args.endpoint, std::env::var("PI_RS_MODEL_TOKEN").ok())?
            .with_reasoning_effort(args.thinking.as_deref())
            .with_progress(|stage| output(json!({"type":"model_progress","stage":stage})))
            .with_reasoning(move |delta| {
                if show_reasoning {
                    output(json!({"type":"reasoning_delta","delta":delta}));
                }
            });

        Ok(Runtime {
            provider,
            registry,
            system_prompt,
            resources: local_resources,
            servers: mcp_connections.clone(),
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
