//! Shared local runtime construction for print, RPC and terminal sessions.
use super::{Cli, DEFAULT_SYSTEM_PROMPT, mcp_config, output, resources};
use pi_rs::{Agent, Limits, ProjectResources, Registry, Result, ollama::Ollama};
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
    let local_resources = resources::Resources::load(args)?;
    for warning in &local_resources.warnings {
        eprintln!("{warning}");
    }
    let base_prompt = args
        .system_prompt
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?
        .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.into());
    let mcp_connections = mcp_config::connect_all(args, false).await?;
    let result = async {
        let mut extensions: Vec<Arc<dyn pi_rs::Extension>> = Vec::new();
        if let Some(project) = &args.project {
            extensions.push(Arc::new(ProjectInstructions {
                resources: ProjectResources::open(&args.workspace, args.workspace.join(project))?,
                skills: args.skill.clone(),
            }));
        }
        extensions.push(Arc::new(WorkbenchContext {
            instruction: args.instruction.clone(),
        }));
        extensions.push(Arc::new(pi_rs_plan_mode::PlanMode::new(
            ["read_text", "lookup_order", "read", "grep", "find", "ls"].map(str::to_owned),
        )));
        extensions.push(Arc::new(pi_rs_review::ReviewContext));
        let mut extension_names = args.extensions.clone();
        extension_names.extend(
            local_resources
                .packages
                .extensions
                .iter()
                .map(|path| path.to_string_lossy().into_owned()),
        );
        for name in extension_names
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
        {
            match name.as_str() {
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
        let selection = super::tool_selection::Selection::new(args)?;
        registry.retain_tools(|tool| selection.includes(&tool.name));
        let system_prompt = local_resources.system_prompt(
            &base_prompt,
            args.system_prompt.is_some(),
            &registry.tool_specs(),
            &args.workspace,
        );
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
