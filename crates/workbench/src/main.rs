use std::io::{BufRead, IsTerminal, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::{FromArgMatches, Parser, Subcommand, ValueEnum};
use pi_rs::{
    CancellationToken, Compaction, Error, Event, InputKind, Notification, OperationState, Outcome,
    QueuedInput, Result, Session,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

mod attachments;
mod backend;
mod cli_compat;
mod clipboard;
mod config;
mod extension_ui;
mod interactive;
mod mcp_config;
mod model_selection;
mod packages;
mod process;
mod resource_paths;
mod resources;
mod rpc;
mod runtime;
mod session_import;
mod sessions;
mod tool_selection;
mod ui;

static TEXT_OUTPUT: AtomicBool = AtomicBool::new(false);

const DEFAULT_SYSTEM_PROMPT: &str = "You are a coding assistant running inside pi, a coding agent harness. Help users inspect files, run commands, and modify code.";

#[derive(Parser, Clone)]
#[command(
    name = "pi-rs",
    version,
    about,
    after_help = "Pi aliases: -v (--version), -nt (--no-tools), -nbt (--no-builtin-tools), -xt (--exclude-tools), -ne (--no-extensions), -ns (--no-skills), -np (--no-prompt-templates), -nc (--no-context-files), -na (--no-approve)."
)]
struct Cli {
    #[arg(skip)]
    settings: config::Settings,
    #[arg(skip)]
    model_explicit: bool,
    #[arg(skip)]
    thinking_explicit: bool,
    #[arg(skip)]
    initial_images: Vec<pi_rs::ImageContent>,
    #[arg(skip)]
    initial_prompt: Option<String>,
    #[arg(skip)]
    theme_paths: Vec<PathBuf>,
    #[arg(skip)]
    model_scope: Vec<model_selection::Choice>,
    #[arg(skip)]
    trusted: bool,
    #[arg(skip)]
    trust_pending: bool,
    /// Load this project's executable configuration.
    #[arg(
        short = 'a',
        long,
        visible_alias = "approve",
        global = true,
        conflicts_with = "no_trust"
    )]
    trust: bool,
    /// Ignore this project's executable configuration.
    #[arg(long, visible_alias = "no-approve", global = true)]
    no_trust: bool,
    /// Path to the private append-only session journal.
    #[arg(long, default_value_os_t = interactive::new_session_path())]
    session: PathBuf,
    /// Open or create a named session in this workspace's session directory.
    #[arg(long, conflicts_with_all = ["session", "continue_session", "resume_session", "no_session"], value_parser = sessions::validate_id)]
    session_id: Option<String>,
    /// Fork a session file or ID into a new session for this workspace.
    #[arg(long = "fork", conflicts_with_all = ["session", "continue_session", "resume_session", "no_session"])]
    fork_session: Option<PathBuf>,
    /// Resume a session file, or open the session selector when omitted.
    #[arg(short = 'r', long = "resume", num_args = 0..=1, default_missing_value = "", conflicts_with_all = ["session", "continue_session", "no_session"])]
    resume_session: Option<PathBuf>,
    /// Name the current session.
    #[arg(short = 'n', long, global = true, value_parser = sessions::validate_name)]
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
    /// Choose fullscreen or normal terminal scrollback rendering.
    #[arg(long, global = true, value_enum)]
    tui_mode: Option<TuiMode>,
    /// Include full requests, tool payloads and streamed text in stdout JSONL.
    #[arg(long, global = true)]
    trace: bool,
    /// Model provider used by this invocation.
    #[arg(
        long,
        global = true,
        value_enum,
        default_value = "ollama",
        ignore_case = true
    )]
    provider: Provider,
    /// Model served by the Ollama endpoint.
    #[arg(long, default_value = "qwen2.5:7b", global = true)]
    model: String,
    /// Comma-separated model IDs or patterns used for startup and cycling.
    #[arg(long, global = true)]
    models: Option<String>,
    /// List available models, optionally filtered by fuzzy search, then exit.
    #[arg(long, num_args = 0..=1, default_missing_value = "")]
    list_models: Option<String>,
    /// Override the model API key for this process without saving it.
    #[arg(long, global = true)]
    api_key: Option<String>,
    /// Export a native or Pi session to HTML and exit. Output defaults beside input.
    #[arg(long = "export", num_args = 1..=2, value_names = ["INPUT", "OUTPUT"])]
    export_session: Option<Vec<PathBuf>>,
    /// Thinking effort for models that support reasoning.
    #[arg(long, global = true, value_parser = ["off", "minimal", "low", "medium", "high", "xhigh", "max"])]
    thinking: Option<String>,
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
    /// Load a skill file or directory, or a named skill with --project.
    #[arg(long, global = true)]
    skill: Vec<String>,
    #[arg(long, global = true)]
    allow_commands: bool,
    /// Disable automatic extensions, retaining explicit --extension entries.
    #[arg(long, global = true)]
    no_extensions: bool,
    /// Disable discovered themes, retaining explicit --theme paths.
    #[arg(long, global = true)]
    no_themes: bool,
    /// Disable discovered skills, retaining explicit --skill paths.
    #[arg(long, global = true)]
    no_skills: bool,
    /// Load a prompt-template file or directory. Repeatable.
    #[arg(long, global = true)]
    prompt_template: Vec<PathBuf>,
    /// Disable discovered prompt templates, retaining explicit paths.
    #[arg(long, global = true)]
    no_prompt_templates: bool,
    /// Disable AGENTS.md and CLAUDE.md discovery.
    #[arg(long, global = true)]
    no_context_files: bool,
    /// Disable MCP server connections for this run.
    #[arg(long, global = true)]
    no_mcp: bool,
    /// Enable local read, write, edit, bash, grep, find, and ls tools.
    #[arg(long, global = true)]
    coding_tools: bool,
    /// Disable all tools unless explicitly selected.
    #[arg(long, global = true)]
    no_tools: bool,
    /// Disable built-in tools while keeping extension tools.
    #[arg(long, global = true)]
    no_builtin_tools: bool,
    /// Comma-separated tool names/patterns, or +name/-name modifiers.
    #[arg(short = 't', long, global = true, allow_hyphen_values = true)]
    tools: Option<String>,
    /// Comma-separated tool names/patterns to remove from the selection.
    #[arg(long, global = true)]
    exclude_tools: Option<String>,
    /// Load a bundled extension, native executable or package for this invocation. Repeatable.
    #[arg(short = 'e', long = "extension", global = true)]
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
    /// System prompt text or a file containing it.
    #[arg(long, global = true)]
    system_prompt: Option<PathBuf>,
    /// Append system prompt text or an existing file. Repeatable.
    #[arg(long, global = true)]
    append_system_prompt: Vec<String>,
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
    /// Bound tool calls in seconds. Unset permits long-running local operations.
    #[arg(long, global = true, value_parser = clap::value_parser!(u64).range(0..=2_147_483))]
    tool_timeout_secs: Option<u64>,
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

impl Cli {
    fn tool_timeout(&self) -> Duration {
        Duration::from_secs(self.tool_timeout_secs.unwrap_or(2_147_483))
    }
    fn model_token(&self) -> Option<String> {
        self.api_key
            .clone()
            .or_else(|| std::env::var("PI_RS_MODEL_TOKEN").ok())
    }
    fn apply_model_thinking_defaults(&mut self) {
        self.thinking = self
            .model_scope
            .iter()
            .find(|choice| choice.model == self.model)
            .and_then(|choice| choice.thinking.clone())
            .or_else(|| {
                self.settings
                    .thinking_for_model(&self.model)
                    .map(str::to_owned)
            })
            .or_else(|| self.thinking.clone());
    }
    fn restore_model_selection(&mut self, model: String, thinking: Option<String>) {
        if !self.model_explicit
            && (self.model_scope.is_empty()
                || self.model_scope.iter().any(|choice| choice.model == model))
        {
            self.model = model.clone();
        }
        if !self.thinking_explicit {
            self.thinking = self
                .model_scope
                .iter()
                .find(|choice| choice.model == self.model)
                .and_then(|choice| choice.thinking.clone())
                .or_else(|| (self.model == model).then_some(thinking).flatten())
                .or_else(|| {
                    self.settings
                        .thinking_for_model(&self.model)
                        .map(str::to_owned)
                })
                .or_else(|| self.thinking.clone());
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Provider {
    Ollama,
}
impl Provider {
    fn id(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
        }
    }

    fn connect(self, endpoint: &str, token: Option<String>) -> Result<pi_rs::ollama::Ollama> {
        match self {
            Self::Ollama => pi_rs::ollama::Ollama::new(endpoint, token),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TuiMode {
    Fullscreen,
    Regular,
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
    #[command(hide = true)]
    Serve,
    #[command(flatten)]
    Package(packages::PackageCommand),
    Mcp {
        #[command(subcommand)]
        command: mcp_config::McpCommand,
    },
    Run {
        prompt: String,
    },
    Resume,
    Compact {
        /// Supply a summary, or omit to request a tool-free model summary.
        #[arg(long)]
        summary: Option<String>,
        /// Additional focus for the model-generated summary.
        #[arg(long, conflicts_with = "summary")]
        instructions: Option<String>,
        /// Override token-based retention with an exact number of recent turns.
        #[arg(long)]
        keep_turns: Option<usize>,
    },
    Checkout {
        record_id: u64,
    },
    /// Replace one message in future model context, keeping its original history.
    ContextEdit {
        record_id: u64,
        /// Replacement text. Omit with --delete to remove it from model context.
        content: Option<String>,
        #[arg(long, conflicts_with = "content")]
        delete: bool,
    },
    Inspect,
    Name {
        #[arg(value_parser = sessions::validate_name)]
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
    let matches = cli_compat::parse(std::env::args_os()).unwrap_or_else(|error| error.exit());
    let result = async {
        let mut args =
            Cli::from_arg_matches(&matches).map_err(|error| Error::Config(error.to_string()))?;
        args.model_explicit =
            matches.value_source("model") == Some(clap::parser::ValueSource::CommandLine);
        args.thinking_explicit =
            matches.value_source("thinking") == Some(clap::parser::ValueSource::CommandLine);
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
        if args.tui_mode.is_none() {
            args.tui_mode = Some(if args.settings.string("tuiMode") == Some("regular") {
                TuiMode::Regular
            } else {
                TuiMode::Fullscreen
            });
        }
        if args.thinking.is_none() {
            args.thinking = args
                .settings
                .string("defaultThinkingLevel")
                .map(str::to_owned);
        }
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
    if let Some(paths) = &args.export_session {
        let input = &paths[0];
        let output_path = paths
            .get(1)
            .cloned()
            .unwrap_or_else(|| input.with_extension("html"));
        let session = session_import::read(input)?;
        if session.host_binding().is_some() {
            return Err(Error::Session(
                "managed sessions must be exported by their host".into(),
            ));
        }
        sessions::export(&session, &output_path)?;
        output(json!({"type":"exported","path":output_path}));
        return Ok(());
    }

    let available = if args.list_models.is_some() || args.models.is_some() {
        args.provider
            .connect(&args.endpoint, args.model_token())?
            .models()
            .await?
    } else {
        Vec::new()
    };
    if let Some(search) = &args.list_models {
        let search = model_selection::unqualified(search);
        for model in available
            .iter()
            .filter(|model| model_selection::fuzzy(search, model))
        {
            if args.mode == Some(OutputMode::Json) {
                output(json!({"type":"model","model":model}));
            } else {
                println!("{model}");
            }
        }
        return Ok(());
    }
    // The hidden worker command receives an already-resolved, literal model ID.
    if !matches!(args.command, Some(Command::Serve)) {
        let selected = model_selection::split(&args.model, &available)?;
        args.model = selected.model;
        if !args.thinking_explicit && selected.thinking.is_some() {
            args.thinking = selected.thinking;
            args.thinking_explicit = true;
        }
    }
    if let Some(patterns) = &args.models {
        args.model_scope = model_selection::scope(patterns, &available)?;
        if !args.model_explicit {
            let choice = args
                .model_scope
                .iter()
                .find(|choice| choice.model == args.model)
                .unwrap_or(&args.model_scope[0]);
            args.model = choice.model.clone();
            if !args.thinking_explicit && choice.thinking.is_some() {
                args.thinking = choice.thinking.clone();
            }
        }
    }

    if !args.thinking_explicit {
        args.apply_model_thinking_defaults();
    }

    if let Some(Command::Package(command)) = args.command.as_ref() {
        return packages::command(&args, command.clone()).await;
    }
    if matches!(args.command, Some(Command::Serve)) {
        return backend::run(args).await;
    }
    if let Some(Command::Mcp { command }) = args.command.as_ref() {
        return mcp_config::command(&args, command.clone()).await;
    }

    let directory = interactive::session_path(&args.workspace, args.session_dir.as_deref())
        .parent()
        .expect("session directory")
        .to_path_buf();
    if let Some(id) = &args.session_id {
        args.session = directory.join(format!("{id}.jsonl"));
    } else if let Some(path) = args
        .resume_session
        .as_ref()
        .filter(|path| !path.as_os_str().is_empty())
    {
        args.session = sessions::resolve_reference(path, &directory)?;
        if !args.session.is_file() {
            return Err(Error::Config("session file not found".into()));
        }
    } else if args.fork_session.is_none() {
        args.session = sessions::resolve_reference(&args.session, &directory)?;
    }
    if let Some(source) = &args.fork_session {
        let source = sessions::resolve_reference(source, &directory)?;
        let source_session = session_import::read(&source)?;
        sessions::fork(
            &source_session,
            &source,
            source_session.head(),
            &args.session,
            &args.workspace,
        )?;
    }
    let temporary = args.no_session.then(tempfile::tempdir).transpose()?;
    if let Some(directory) = &temporary {
        args.session = directory.path().join("session.jsonl");
    } else if args.continue_session {
        args.session = interactive::latest_session(&args.session, &args.workspace)?;
    }
    if args.mode == Some(OutputMode::Rpc) {
        if args.prompt.iter().any(|argument| argument.starts_with('@')) {
            return Err(Error::Config(
                "RPC mode accepts images in prompt requests, not @file arguments".into(),
            ));
        }
        return rpc::run(args);
    }
    if args.command.is_none() {
        let (mut prompt, images) = attachments::expand(&args.prompt, &args.workspace)?;
        args.initial_images = images;
        if !std::io::stdin().is_terminal() && !args.stdin_control {
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input)?;
            if !input.is_empty() {
                prompt = format!("{input}\n{prompt}");
            }
        }
        if !prompt.trim().is_empty() || !args.initial_images.is_empty() {
            if !args.print
                && args.mode != Some(OutputMode::Json)
                && std::io::stdin().is_terminal()
                && std::io::stdout().is_terminal()
            {
                args.initial_prompt = Some(prompt);
                args.command = Some(Command::Interactive);
            } else {
                args.command = Some(Command::Run { prompt });
                if args.mode.is_none() {
                    args.mode = Some(OutputMode::Text);
                }
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
        return interactive::run(args).await;
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
            sessions::fork(
                &session,
                &args.session,
                target,
                &destination,
                &args.workspace,
            )?;
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
        Command::ContextEdit {
            record_id,
            content,
            delete,
        } => {
            let replacement = match (content, delete) {
                (Some(content), false) => Some(pi_rs::ContextContent::Text(content)),
                (None, true) => None,
                _ => return Err(Error::Config("provide replacement text or --delete".into())),
            };
            session.edit_context(record_id, replacement)?;
            output(json!({"type":"context_edited","target":record_id}));
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
    if let Some((model, thinking)) = session.model_selection() {
        args.restore_model_selection(model, thinking);
    }
    session.select_model(&args.model, args.thinking.as_deref())?;
    let runtime = runtime::configure(&args).await?;
    session.set_auto_compaction_control(runtime.auto_compaction.clone());
    session.set_retry_control(runtime.retry.clone());
    let result = async {
        let registry = &runtime.registry;
        let agent = runtime.agent(&args);
        let mut observer = |event: Notification<'_>| observe(event, args.trace);
        if let Command::Run { prompt } = &mut command {
            if args.initial_images.is_empty() && agent.try_extension_command(&mut session, prompt, &mut observer)? { return Ok(()); }
            if let Some(expanded) = runtime.resources.expand(prompt)? { *prompt = expanded; }
        }



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
    let mut input = match command {
        Command::Review {
            child,
            from_record,
            resume,
            task,
        } => {
            let mut child = Session::open(child)?;
            child.set_auto_compaction_control(runtime.auto_compaction.clone());
            child.set_retry_control(runtime.retry.clone());
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
            ..
        } => {
            return match keep_turns {
                Some(keep_turns) => agent.compact(
                    &mut session, Compaction { summary, keep_turns }, &mut observer,
                ),
                None => agent.compact_configured(&mut session, summary, &mut observer),
            };
        }
        Command::Compact {
            summary: None, keep_turns, instructions,
        } => {
            return match keep_turns {
                Some(keep_turns) => agent.summarize_with_instructions::<pi_rs::AgentInput>(
                    &mut session, keep_turns, &cancel, &mut observer, None, instructions.as_deref(),
                ).await,
                None => agent.summarize_configured::<pi_rs::AgentInput>(
                    &mut session, &cancel, &mut observer, None, instructions.as_deref(),
                ).await,
            };
        }
        Command::Run { prompt } => Some(prompt),
        Command::Resume => None,
        _ => unreachable!(),
    };
    let run_id = args.run_id.unwrap_or_else(Uuid::new_v4);
    if !args.initial_images.is_empty() {
        if session.records().iter().any(|record| record.run_id == Some(run_id) || matches!(record.event, Event::UserWithImages { input_id: Some(id), .. } if id == run_id)) {
            return Err(Error::DuplicateRun(run_id));
        }
        session.append_user_with_images(input.take().unwrap_or_default(), args.initial_images.clone(), Some(run_id))?;
        observer(Notification::Committed(session.records().last().expect("appended user input")));
    }
    let answer = agent
        .run_correlated(
            &mut session,
            run_id,
            input,
            &mut receiver,
            &cancel,
            &mut observer,
        )
        .await?;
    output(json!({"type": "answer", "content": answer}));
    Ok(())
    }.await;
    let cleanup = runtime.close().await;
    result.and(cleanup)
}

fn observe(event: Notification<'_>, trace: bool) {
    if TEXT_OUTPUT.load(Ordering::Relaxed)
        && let Notification::Committed(record) = &event
        && matches!(&record.event, Event::ModelFinished { metadata, .. } if metadata.finish_reason.as_deref() == Some("length"))
    {
        eprintln!("Response reached the output token limit.");
    }
    match event {
        Notification::Retry(event) => output(serde_json::to_value(event).expect("retry event")),
        Notification::Committed(record) if trace => {
            output(json!({"type": "record", "record": record}))
        }
        Notification::Committed(record) => output(metadata(record)),
        Notification::Request(request) if trace => {
            output(json!({"type": "request", "request": request}))
        }
        Notification::Text(delta) if trace => output(json!({"type": "text_delta", "delta": delta})),
        _ => {}
    }
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
        Event::InputQueueCleared { ids } => json!({"event":"input_queue_cleared","ids":ids}),
        Event::QueueModeSelected { kind, mode } => {
            json!({"event":"queue_mode_selected","kind":kind,"mode":mode})
        }
        Event::BashExecution { execution } => {
            json!({"event":"bash_execution","exitCode":execution.exit_code,"cancelled":execution.cancelled,"truncated":execution.truncated})
        }
        Event::Created { session_id } => json!({"event": "created", "session_id": session_id}),
        Event::ParentSession { .. } => json!({"event":"parent_session"}),
        Event::SessionInfo { name, cwd } => json!({"event":"session_info","name":name,"cwd":cwd}),
        Event::ModelSelected { model, thinking } => {
            json!({"event":"model_selected","model":model,"thinking":thinking})
        }
        Event::ContextEdited { target, .. } => json!({"event":"context_edited","target":target}),
        Event::Label { target, label } => json!({"event":"label","target":target,"label":label}),
        Event::HostBound { .. } => json!({"event": "host_bound"}),
        Event::HostRunBound { run_id, .. } => {
            json!({"event": "host_run_bound", "bound_run_id": run_id})
        }
        Event::Checkout { target } => json!({"event": "checkout", "target": target}),
        Event::Run { state, .. } => json!({"event": "run", "state": state}),
        Event::UserWithImages {
            input_id, images, ..
        } => json!({"event":"user","input_id":input_id,"images":images.len()}),
        Event::User { input_id, .. } => json!({"event": "user", "input_id": input_id}),
        Event::InputQueuedWithImages { input } => {
            json!({"event":"input_queued","input_id":input.input.id,"kind":input.input.kind,"images":input.images.len()})
        }
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
        Event::ToolPlanned {
            invocation,
            parent_operation,
        } => {
            json!({"event":"tool_planned","operation_id":invocation.operation_id,"name":invocation.call.name,"parent_operation":parent_operation})
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
        Event::UserImages { images } => json!({"event":"user_images","images":images.len()}),
        Event::SourceEntry { format, .. } => json!({"event":"source_entry","format":format}),
        Event::SystemCheckpoint {
            sections,
            tools_added,
            tools_removed,
            ..
        } => {
            json!({"event":"system_checkpoint","sections":sections.len(),"tools_added":tools_added.len(),"tools_removed":tools_removed.len()})
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
        Outcome::Succeeded(_) | Outcome::SucceededWithDetails { .. } => "succeeded",
        Outcome::Failed(_) => "failed",
        Outcome::Skipped(_) => "skipped",
        Outcome::Unknown(_) => "unknown",
        Outcome::Pending { .. } => "pending",
        Outcome::Truncated { .. } => "truncated",
        Outcome::Structured { .. } => "succeeded",
    }
}

#[cfg(test)]
mod cli_timeout_tests {
    use super::*;

    #[test]
    fn session_names_reject_empty_input_and_trim_surrounding_whitespace() {
        for name in ["", " ", "\t\r\n", "\u{3000}"] {
            assert!(Cli::try_parse_from(["pi", "--name", name]).is_err());
            assert!(Cli::try_parse_from(["pi", "name", name]).is_err());
        }
        let args = Cli::try_parse_from(["pi", "--name", "  設計メモ  "]).unwrap();
        assert_eq!(args.name.as_deref(), Some("設計メモ"));
        let args = Cli::try_parse_from(["pi", "name", "  notes  "]).unwrap();
        assert!(matches!(args.command, Some(Command::Name {name}) if name == "notes"));
    }

    #[test]
    fn local_tools_have_an_optional_explicit_deadline() {
        let args = Cli::try_parse_from(["pi"]).unwrap();
        assert!(args.tool_timeout_secs.is_none());
        assert!(args.tool_timeout() > Duration::from_secs(30));
        let args = Cli::try_parse_from(["pi", "--tool-timeout-secs", "1"]).unwrap();
        assert_eq!(args.tool_timeout(), Duration::from_secs(1));
        assert!(Cli::try_parse_from(["pi", "--tool-timeout-secs", "2147484"]).is_err());
    }
}
