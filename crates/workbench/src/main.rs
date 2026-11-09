use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use pi_rs::{
    Agent, CancellationToken, Compaction, Error, Event, Limits, Notification, OperationState,
    Outcome, Registry, Result, Session, ollama::Ollama, project_context,
};
use pi_rs_workbench::{CommandPolicy, Tools, WorkbenchContext};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

const DEFAULT_SYSTEM_PROMPT: &str =
    "You are a workspace assistant. Use tools when needed and answer accurately.";

#[derive(Parser)]
#[command(name = "pi-rs", version, about)]
struct Cli {
    /// Path to the private append-only session journal.
    #[arg(long)]
    session: PathBuf,
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
    /// Optional read-only order API collection URL.
    #[arg(long, global = true)]
    orders: Option<String>,
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
    /// Read follow-up and cancellation messages as JSON lines on stdin.
    #[arg(long, global = true)]
    stdin_control: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
    Followup { content: String },
    Cancel,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = Cli::parse();
    if let Err(err) = execute(args).await {
        eprintln!("{err}");
        std::process::exit(if matches!(err, Error::Cancelled) {
            130
        } else {
            1
        });
    }
}

async fn execute(args: Cli) -> Result<()> {
    let mut session = Session::open(&args.session)?;
    match args.command {
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
    let mut system_prompt = args
        .system_prompt
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?
        .unwrap_or_else(|| DEFAULT_SYSTEM_PROMPT.into());
    if let Some(project) = &args.project {
        system_prompt.push_str(&project_context(
            &args.workspace,
            args.workspace.join(project),
            &args.skill,
        )?);
    }
    let mut registry = Registry::default();
    registry.register(Arc::new(CommandPolicy {
        allow_commands: args.allow_commands,
    }))?;
    registry.register(Arc::new(WorkbenchContext {
        instruction: args.instruction,
    }))?;
    let order_config = args
        .orders
        .as_deref()
        .map(|url| (url, std::env::var("PI_RS_ORDERS_TOKEN").ok()));
    registry.register(Arc::new(Tools::new(&args.workspace, order_config)?))?;
    let provider = Ollama::new(&args.endpoint, std::env::var("PI_RS_MODEL_TOKEN").ok())?;
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
    let input = match args.command {
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
    let (sender, mut receiver) = mpsc::channel(32);
    if args.stdin_control {
        read_controls(sender, cancel.clone());
    }
    let answer = agent
        .run(&mut session, input, &mut receiver, &cancel, &mut observer)
        .await?;
    output(json!({"type": "answer", "content": answer}));
    Ok(())
}

fn read_controls(sender: mpsc::Sender<String>, cancel: CancellationToken) {
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            match serde_json::from_str(&line) {
                Ok(Control::Cancel) => {
                    cancel.cancel();
                    return;
                }
                Ok(Control::Followup { content }) => {
                    if sender.try_send(content).is_err() {
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
    let _ = writeln!(std::io::stdout().lock(), "{event}");
}

fn metadata(record: &pi_rs::Record) -> Value {
    let mut value = match &record.event {
        Event::Created { session_id } => json!({"event": "created", "session_id": session_id}),
        Event::Checkout { target } => json!({"event": "checkout", "target": target}),
        Event::Run { state } => json!({"event": "run", "state": state}),
        Event::User { .. } => json!({"event": "user"}),
        Event::ModelRequested {
            model,
            messages,
            tools,
            max_output_tokens,
        } => {
            json!({"event": "model_requested", "model": model, "messages": messages, "tools": tools, "max_output_tokens": max_output_tokens})
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
    value
}

fn outcome_status(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Succeeded(_) => "succeeded",
        Outcome::Failed(_) => "failed",
        Outcome::Skipped(_) => "skipped",
        Outcome::Unknown(_) => "unknown",
    }
}
