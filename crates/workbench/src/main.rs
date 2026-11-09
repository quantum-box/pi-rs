use std::io::{BufRead, IsTerminal, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use pi_rs::{
    Agent, CancellationToken, Compaction, Error, Event, InputKind, Limits, Notification,
    OperationState, Outcome, ProjectResources, QueuedInput, Registry, Result, Session,
    ollama::Ollama,
};
use pi_rs_workbench::{CommandPolicy, ProjectInstructions, Tools, WorkbenchContext};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

mod config;
mod interactive;
mod process;
mod resources;
mod rpc;
mod sessions;
mod ui;

static TEXT_OUTPUT: AtomicBool = AtomicBool::new(false);

const DEFAULT_SYSTEM_PROMPT: &str =
    "You are a workspace assistant. Use tools when needed and answer accurately.";

#[derive(Parser, Clone)]
#[command(name = "pi-rs", version, about)]
struct Cli {
    #[arg(skip)]
    settings: config::Settings,
    #[arg(skip)]
    trusted: bool,
    #[arg(skip)]
    trust_pending: bool,
    /// Load this project's executable configuration.
    #[arg(long, global = true, conflicts_with = "no_trust")]
    trust: bool,
    /// Ignore this project's executable configuration.
    #[arg(long, global = true)]
    no_trust: bool,
    /// Path to the private append-only session journal.
    #[arg(long, default_value_os_t = interactive::new_session_path())]
    session: PathBuf,
    /// Resume a session file, or open the session selector when omitted.
    #[arg(short = 'r', long = "resume", num_args = 0..=1, default_missing_value = "", conflicts_with_all = ["session", "continue_session", "no_session"])]
    resume_session: Option<PathBuf>,
    /// Name the current session.
    #[arg(long, global = true)]
    name: Option<String>,
    /// Override the directory in which sessions are stored.
    #[arg(long, global = true)]
    session_dir: Option<PathBuf>,
    /// Continue the latest session for this working directory.
    #[arg(short = 'c', long = "continue", conflicts_with = "session")]
    continue_session: bool,
    /// Keep the session only for this process.
    #[arg(long, conflicts_with_all = ["session", "continue_session"])]
    no_session: bool,
    /// Print the final answer and exit.
    #[arg(short = 'p', long)]
    print: bool,
    #[arg(long, value_enum)]
    mode: Option<OutputMode>,
    /// Include full requests, tool payloads and streamed text in stdout JSONL.
    #[arg(long, global = true)]
    trace: bool,
    /// Model served by the Ollama endpoint.
    #[arg(long, default_value = "qwen2.5:7b", global = true)]
    model: String,
    /// Full URL of the Ollama-compatible chat completions endpoint.
    #[arg(
        long,
        default_value = "http://127.0.0.1:11434/v1/chat/completions",
        global = true
    )]
    endpoint: String,
    /// Root directory exposed to file tools.
    #[arg(long, default_value = ".", global = true)]
    workspace: PathBuf,
    /// Load AGENTS.md from the workspace root through this workspace-relative directory.
    #[arg(long, global = true)]
    project: Option<PathBuf>,
    /// Load a named project skill. Requires --project.
    #[arg(long, global = true, requires = "project")]
    skill: Vec<String>,
    #[arg(long, global = true)]
    allow_commands: bool,
    /// Enable local read, write, edit, bash, grep, find, and ls tools.
    #[arg(long, global = true)]
    coding_tools: bool,
    /// Enable a bundled extension or a native extension executable. Repeatable.
    #[arg(long = "extension", global = true)]
    extensions: Vec<String>,
    /// Theme name, or a light/dark theme pair for automatic appearance.
    #[arg(long, global = true)]
    use_theme: Option<String>,
    /// Additional theme file or directory. Repeatable.
    #[arg(long = "theme", global = true)]
    themes: Vec<PathBuf>,
    /// Terminal presentation mode.
    /// Optional read-only order API collection URL.
    #[arg(long, global = true)]
    orders: Option<String>,
    /// Loopback fixture destination for evaluation-only mutating tools.
    #[cfg(feature = "evaluation-fixtures")]
    #[arg(long, global = true)]
    fixtures: Option<String>,
    /// File containing the system prompt.
    #[arg(long, global = true)]
    system_prompt: Option<PathBuf>,
    /// Additional trusted instruction applied by the context extension.
    #[arg(long, global = true)]
    instruction: Option<String>,
    #[arg(long, default_value_t = 12, global = true)]
    max_requests: usize,
    #[arg(long, default_value_t = 2048, global = true)]
    max_output_tokens: u32,
    #[arg(long, default_value_t = 8192, global = true)]
    output_budget: u32,
    #[arg(long, default_value_t = 120, global = true)]
    model_timeout_secs: u64,
    #[arg(long, default_value_t = 30, global = true)]
    tool_timeout_secs: u64,
    /// Estimated model context window. Enables bounded automatic compaction.
    #[arg(long, global = true)]
    context_window: Option<u32>,
    /// Read follow-up and cancellation messages as JSON lines on stdin.
    #[arg(long, global = true)]
    stdin_control: bool,
    /// Correlation key for retrying or explicitly resuming a host run.
    #[arg(long, global = true)]
    run_id: Option<Uuid>,
    /// Disable an optional compiled extension by ID.
    #[arg(long, global = true)]
    disable_extension: Vec<String>,
    #[command(subcommand)]
    command: Option<Command>,
    /// Initial prompt when no explicit subcommand is used.
    #[arg(value_name = "PROMPT", num_args = 0.., trailing_var_arg = true)]
    prompt: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputMode {
    Text,
    Json,
    Rpc,
}

#[derive(Subcommand, Clone)]
enum Command {
    /// Open the interactive terminal interface.
    Interactive,
    Run {
        prompt: String,
    },
    Resume,
    Compact {
        /// Supply a summary, or omit to request a tool-free model summary.
        #[arg(long)]
        summary: Option<String>,
        #[arg(long, default_value_t = 1)]
        keep_turns: usize,
    },
    Checkout {
        record_id: u64,
    },
    Inspect,
    Name {
        name: String,
    },
    Label {
        record_id: u64,
        label: Option<String>,
    },
    Export {
        path: PathBuf,
    },
    Import {
        path: PathBuf,
    },
    #[command(name = "clone")]
    CloneSession {
        destination: Option<PathBuf>,
    },
    Fork {
        record_id: u64,
        destination: Option<PathBuf>,
    },
    Copy,
    Share,
    Review {
        #[arg(long)]
        child: PathBuf,
        #[arg(long)]
        from_record: Option<u64>,
        #[arg(long)]
        resume: bool,
        task: String,
    },
    Extension {
        extension_id: String,
        command: String,
        #[arg(default_value = "")]
        arguments: String,
    },
    Reconcile {
        operation_id: Uuid,
        resolution: Resolution,
        evidence: String,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Resolution {
    Succeeded,
    Failed,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Control {
    Followup { content: String, id: Option<Uuid> },
    Steer { content: String, id: Option<Uuid> },
    Cancel,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let matches = Cli::command().get_matches();
    let result = async {
        let mut args =
            Cli::from_arg_matches(&matches).map_err(|error| Error::Config(error.to_string()))?;
        let decision = config::project_trust(
            &args.workspace,
            if args.no_trust {
                Some(false)
            } else if args.trust || args.project.is_some() {
                Some(true)
            } else {
                None
            },
        )?;
        args.trusted = decision.unwrap_or(false);
        args.trust_pending = decision.is_none();
        args.settings = config::Settings::load(&args.workspace, args.trusted)?;
        if matches.value_source("model") != Some(clap::parser::ValueSource::CommandLine)
            && let Some(model) = args.settings.string("defaultModel")
        {
            args.model = model.into();
        }
        args.themes.extend(
            args.settings
                .strings("themes")
                .into_iter()
                .map(PathBuf::from),
        );
        if args.use_theme.is_none() {
            args.use_theme = args.settings.string("theme").map(str::to_owned);
        }
        if args.session_dir.is_none() {
            args.session_dir = std::env::var_os("PI_CODING_AGENT_SESSION_DIR")
                .map(PathBuf::from)
                .or_else(|| args.settings.string("sessionDir").map(PathBuf::from));
        }
        if matches.value_source("session") != Some(clap::parser::ValueSource::CommandLine) {
            args.session = interactive::session_path(&args.workspace, args.session_dir.as_deref());
        }
        execute(args).await
    }
    .await;
    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(if matches!(err, Error::Cancelled) {
            130
        } else {
            1
        });
    }
}

async fn execute(mut args: Cli) -> Result<()> {
    if let Some(path) = args
        .resume_session
        .as_ref()
        .filter(|path| !path.as_os_str().is_empty())
    {
        if !path.is_file() {
            return Err(Error::Config("session file not found".into()));
        }
        args.session = path.clone();
    }
    let temporary = args.no_session.then(tempfile::tempdir).transpose()?;
    if let Some(directory) = &temporary {
        args.session = directory.path().join("session.jsonl");
    } else if args.continue_session {
        args.session = interactive::latest_session(&args.session)?;
    }
    if args.mode == Some(OutputMode::Rpc) {
        return rpc::run(args);
    }
    if args.command.is_none() {
        let mut prompt = args.prompt.join(" ");
        if !std::io::stdin().is_terminal() && !args.stdin_control {
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input)?;
            if !input.is_empty() {
                prompt = format!("{input}\n{prompt}");
            }
        }
        if !prompt.trim().is_empty() {
            args.command = Some(Command::Run { prompt });
            if args.mode.is_none() {
                args.mode = Some(OutputMode::Text);
            }
        } else if args.print {
            return Err(Error::Config(
                "print mode requires a prompt or piped input".into(),
            ));
        }
    }
    TEXT_OUTPUT.store(
        args.print || args.mode == Some(OutputMode::Text),
        Ordering::Relaxed,
    );
    let mut command = args.command.take().unwrap_or(Command::Interactive);
    if matches!(command, Command::Interactive) {
        return interactive::run(args);
    }
    if let Some(parent) = args
        .session
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    if let Command::Import { path } = &command {
        sessions::import(path, &args.session)?;
        output(json!({"type":"imported","path":args.session}));
        return Ok(());
    }
    let mut session = Session::open(&args.session)?;
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "a managed session must be opened by its host".into(),
        ));
    }
    let requested_workspace = args.workspace.clone();
    if let Some(cwd) = session.working_directory() {
        args.workspace = PathBuf::from(cwd);
        if !args.workspace.is_dir() {
            return Err(Error::Config(
                "session working directory no longer exists".into(),
            ));
        }
    } else if !matches!(
        command,
        Command::Inspect
            | Command::Export { .. }
            | Command::CloneSession { .. }
            | Command::Fork { .. }
            | Command::Copy
            | Command::Share
    ) {
        session.set_working_directory(&args.workspace)?;
    }
    if args.workspace != requested_workspace {
        let decision = config::project_trust(
            &args.workspace,
            if args.no_trust {
                Some(false)
            } else if args.trust || args.project.is_some() {
                Some(true)
            } else {
                None
            },
        )?;
        args.trusted = decision.unwrap_or(false);
        args.trust_pending = decision.is_none();
        args.settings = config::Settings::load(&args.workspace, args.trusted)?;
    }
    if let Some(name) = &args.name {
        session.set_name(name)?;
    }
    if matches!(command, Command::Fork { .. }) {
        args.workspace = requested_workspace;
    }
    let fork_target = match &command {
        Command::Fork { record_id, .. } => *record_id,
        _ => session.head(),
    };
    match command {
        Command::Name { name } => {
            session.set_name(&name)?;
            output(json!({"type":"session_name","name":session.name()}));
            return Ok(());
        }
        Command::Label { record_id, label } => {
            session.set_label(record_id, label)?;
            output(json!({"type":"labels","labels":session.labels()}));
            return Ok(());
        }
        Command::Export { path } => {
            let path = if path.is_absolute() {
                path
            } else {
                args.workspace.join(path)
            };
            sessions::export(&session, &path)?;
            output(json!({"type":"exported","path":path}));
            return Ok(());
        }
        Command::CloneSession { destination } | Command::Fork { destination, .. } => {
            let target = fork_target;
            let destination = destination.unwrap_or_else(|| {
                interactive::session_path(&args.workspace, args.session_dir.as_deref())
            });
            sessions::fork(&session, target, &destination, &args.workspace)?;
            output(json!({"type":"forked","path":destination}));
            return Ok(());
        }
        Command::Copy => {
            sessions::copy_last(&session)?;
            output(json!({"type":"copied"}));
            return Ok(());
        }
        Command::Share => {
            let url = sessions::share_gist(&session)?;
            output(json!({"type":"shared","url":url}));
            return Ok(());
        }
        Command::Inspect => {
            for record in session.records() {
                output(if args.trace {
                    json!({"type": "record", "record": record})
                } else {
                    metadata(record)
                });
            }
            let operations: Vec<_> = session.operations().iter().map(|(inv, state)| {
                let status = match state {
                    OperationState::Planned => "planned",
                    OperationState::Started => "unconfirmed_dispatch",
                    OperationState::Finished(outcome) => outcome_status(outcome),
                };
                json!({"operation_id": inv.operation_id, "name": inv.call.name, "status": status})
            }).collect();
            output(json!({"type": "session", "head": session.head(), "operations": operations}));
            return Ok(());
        }
        Command::Checkout { record_id } => {
            session.checkout(record_id)?;
            output(json!({"type": "checkout", "head": session.head()}));
            return Ok(());
        }
        Command::Reconcile {
            operation_id,
            resolution,
            evidence,
        } => {
            if evidence.trim().is_empty() {
                return Err(Error::Config(
                    "reconciliation requires authoritative evidence".into(),
                ));
            }
            let outcome = match resolution {
                Resolution::Succeeded => Outcome::Succeeded(evidence),
                Resolution::Failed => Outcome::Failed(evidence),
            };
            session.reconcile(operation_id, outcome)?;
            output(json!({"type": "reconciled", "operation_id": operation_id}));
            return Ok(());
        }
        _ => {}
    }
    let local_resources = resources::Resources::load(&args)?;
    for warning in &local_resources.warnings {
        eprintln!("{warning}");
    }
    if let Command::Run { prompt } = &mut command
        && let Some(expanded) = local_resources.expand(prompt)?
    {
        *prompt = expanded;
    }
    let mut system_prompt = args
        .system_prompt
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?
        .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.into());
    if args.system_prompt.is_some() {
        system_prompt.push_str(&local_resources.append_system);
        system_prompt.push_str(&local_resources.instructions);
    } else {
        system_prompt = local_resources.system_prompt(&system_prompt);
    }
    let mut extensions: Vec<Arc<dyn pi_rs::Extension>> = Vec::new();
    if let Some(project) = &args.project {
        extensions.push(Arc::new(ProjectInstructions {
            resources: ProjectResources::open(&args.workspace, args.workspace.join(project))?,
            skills: args.skill,
        }));
    }
    extensions.push(Arc::new(WorkbenchContext {
        instruction: args.instruction,
    }));
    extensions.push(Arc::new(pi_rs_plan_mode::PlanMode::new(
        ["read_text", "lookup_order", "read", "grep", "find", "ls"].map(str::to_owned),
    )));
    extensions.push(Arc::new(pi_rs_review::ReviewContext));
    for name in args
        .extensions
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
    let mut registry =
        Registry::compose(extensions, &args.disable_extension.into_iter().collect())?;
    registry.register(Arc::new(CommandPolicy {
        allow_commands: args.allow_commands,
    }))?;
    let order_config = args
        .orders
        .as_deref()
        .map(|url| (url, std::env::var("PI_RS_ORDERS_TOKEN").ok()));
    registry.register(Arc::new(Tools::new(&args.workspace, order_config)?))?;
    if args.coding_tools {
        registry.register(Arc::new(pi_rs_workbench::coding::CodingTools::local(
            &args.workspace,
        )?))?;
    }
    let show_reasoning = args.trace;
    let provider = Ollama::new(&args.endpoint, std::env::var("PI_RS_MODEL_TOKEN").ok())?
        .with_progress(|stage| output(json!({"type":"model_progress","stage":stage})))
        .with_reasoning(move |delta| {
            if show_reasoning {
                output(json!({"type":"reasoning_delta","delta":delta}));
            }
        });
    let agent = Agent {
        model: &provider,
        model_name: &args.model,
        registry: &registry,
        system_prompt: &system_prompt,
        limits: Limits {
            model_requests: args.max_requests,
            output_tokens_per_request: args.max_output_tokens,
            output_tokens_per_run: args.output_budget,
            model_timeout: Duration::from_secs(args.model_timeout_secs),
            tool_timeout: Duration::from_secs(args.tool_timeout_secs),
            context_window: args.context_window,
        },
    };
    let mut observer = |event: Notification<'_>| match event {
        Notification::Committed(record) if args.trace => {
            output(json!({"type": "record", "record": record}))
        }
        Notification::Committed(record) => output(metadata(record)),
        Notification::Request(request) if args.trace => {
            output(json!({"type": "request", "request": request}))
        }
        Notification::Text(delta) if args.trace => {
            output(json!({"type": "text_delta", "delta": delta}))
        }
        _ => {}
    };

    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let (sender, mut receiver) = mpsc::channel(32);
    if args.stdin_control {
        read_controls(sender, cancel.clone());
    }
    let input = match command {
        Command::Review {
            child,
            from_record,
            resume,
            task,
        } => {
            let mut child = Session::open(child)?;
            let review = pi_rs_review::Review {
                task: &task,
                from_record,
                resume,
                timeout: Duration::from_secs(args.model_timeout_secs),
                output_budget: args.output_budget,
                allowed_tools: ["read_text", "lookup_order", "read", "grep", "find", "ls"]
                    .into_iter()
                    .filter(|name| registry.has_tool(name))
                    .map(str::to_owned)
                    .collect(),
            };
            let parent_id = session.session_id();
            let child_id = child.session_id();
            let mut child_observer = |event: Notification<'_>| {
                if let Notification::Committed(record) = event {
                    let mut event = if args.trace {
                        json!({"type":"record","record":record})
                    } else {
                        metadata(record)
                    };
                    event["review_parent_session"] = json!(parent_id);
                    event["review_child_session"] = json!(child_id);
                    output(event);
                }
            };
            let answer = review
                .run(
                    &agent,
                    &mut session,
                    &mut child,
                    &cancel,
                    &mut child_observer,
                )
                .await?;
            output(
                json!({"type":"review_answer","content":answer,"child_session":child_id,"parent_session":parent_id}),
            );
            return Ok(());
        }
        Command::Extension {
            extension_id,
            command,
            arguments,
        } => {
            return agent.extension_command(
                &mut session,
                &extension_id,
                &command,
                &arguments,
                &mut observer,
            );
        }
        Command::Compact {
            summary: Some(summary),
            keep_turns,
        } => {
            return agent.compact(
                &mut session,
                Compaction {
                    summary,
                    keep_turns,
                },
                &mut observer,
            );
        }
        Command::Compact {
            summary: None,
            keep_turns,
        } => {
            return agent
                .summarize(&mut session, keep_turns, &cancel, &mut observer)
                .await;
        }
        Command::Run { prompt } => Some(prompt),
        Command::Resume => None,
        _ => unreachable!(),
    };
    let answer = agent
        .run_correlated(
            &mut session,
            args.run_id.unwrap_or_else(Uuid::new_v4),
            input,
            &mut receiver,
            &cancel,
            &mut observer,
        )
        .await?;
    output(json!({"type": "answer", "content": answer}));
    Ok(())
}

fn read_controls(sender: mpsc::Sender<QueuedInput>, cancel: CancellationToken) {
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            match serde_json::from_str(&line) {
                Ok(Control::Cancel) => {
                    cancel.cancel();
                    return;
                }
                Ok(control @ (Control::Followup { .. } | Control::Steer { .. })) => {
                    let (kind, content, id) = match control {
                        Control::Followup { content, id } => (InputKind::FollowUp, content, id),
                        Control::Steer { content, id } => (InputKind::Steer, content, id),
                        Control::Cancel => unreachable!(),
                    };
                    let input = QueuedInput {
                        id: id.unwrap_or_else(Uuid::new_v4),
                        kind,
                        content,
                    };
                    if sender.try_send(input).is_err() {
                        output(
                            json!({"type": "input_rejected", "reason": "follow-up queue unavailable"}),
                        );
                    }
                }
                Err(_) => {
                    output(json!({"type": "input_rejected", "reason": "invalid control message"}))
                }
            }
        }
    });
}

fn output(event: Value) {
    // The journal remains authoritative if the output consumer closes the pipe.
    if TEXT_OUTPUT.load(Ordering::Relaxed) {
        if let Some(answer) = event.get("content").and_then(Value::as_str) {
            let _ = writeln!(std::io::stdout().lock(), "{answer}");
        }
    } else {
        let _ = writeln!(std::io::stdout().lock(), "{event}");
    }
}

fn metadata(record: &pi_rs::Record) -> Value {
    let mut value = match &record.event {
        Event::Created { session_id } => json!({"event": "created", "session_id": session_id}),
        Event::SessionInfo { name, cwd } => json!({"event":"session_info","name":name,"cwd":cwd}),
        Event::Label { target, label } => json!({"event":"label","target":target,"label":label}),
        Event::HostBound { .. } => json!({"event": "host_bound"}),
        Event::HostRunBound { run_id, .. } => {
            json!({"event": "host_run_bound", "bound_run_id": run_id})
        }
        Event::Checkout { target } => json!({"event": "checkout", "target": target}),
        Event::Run { state, .. } => json!({"event": "run", "state": state}),
        Event::User { input_id, .. } => json!({"event": "user", "input_id": input_id}),
        Event::InputQueued { input } => {
            json!({"event": "input_queued", "input_id": input.id, "kind": input.kind})
        }
        Event::ModelRequested {
            model,
            messages,
            tools,
            max_output_tokens,
            request_id,
            context_head,
            context_sha256,
        } => {
            json!({"event": "model_requested", "request_id": request_id, "context_head": context_head, "context_sha256": context_sha256, "model": model, "messages": messages, "tools": tools, "max_output_tokens": max_output_tokens})
        }
        Event::ModelFinished {
            request_id,
            metadata,
            usage,
            failure,
            ..
        } => {
            let mut metadata = metadata.clone();
            metadata.reasoning = None;
            json!({"event": "model_finished", "request_id": request_id, "metadata": metadata, "usage": usage, "failure": failure})
        }
        Event::TurnFinished { assistant_record } => {
            json!({"event": "turn_finished", "assistant_record": assistant_record})
        }
        Event::Assistant {
            invocations, usage, ..
        } => {
            let tools: Vec<_> = invocations
                .iter()
                .map(|inv| json!({"operation_id": inv.operation_id, "name": inv.call.name}))
                .collect();
            json!({"event": "assistant", "tools": tools, "usage": usage})
        }
        Event::ToolStarted { operation_id } => {
            json!({"event": "tool_started", "operation_id": operation_id})
        }
        Event::ToolFinished {
            operation_id,
            outcome,
            ..
        } => {
            json!({"event": "tool_finished", "operation_id": operation_id, "status": outcome_status(outcome)})
        }
        Event::ToolReconciled {
            operation_id,
            outcome,
        } => {
            json!({"event": "tool_reconciled", "operation_id": operation_id, "status": outcome_status(outcome)})
        }
        Event::Compacted {
            retain_from, usage, ..
        } => {
            json!({"event": "compacted", "retain_from": retain_from, "usage": usage})
        }
        Event::ExtensionStates { states } => {
            let versions: Vec<_> = states
                .iter()
                .map(|(id, state)| json!({"id": id, "version": state.version}))
                .collect();
            json!({"event": "extension_states", "versions": versions})
        }
    };
    value["type"] = json!("record");
    value["id"] = json!(record.id);
    value["parent"] = json!(record.parent);
    value["run_id"] = json!(record.run_id);
    value
}

fn outcome_status(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Succeeded(_) => "succeeded",
        Outcome::Failed(_) => "failed",
        Outcome::Skipped(_) => "skipped",
        Outcome::Unknown(_) => "unknown",
        Outcome::Pending { .. } => "pending",
        Outcome::Truncated { .. } => "truncated",
        Outcome::Structured { .. } => "succeeded",
    }
}
