//! Persistent native frontend worker. Journal locks are held only during a run.
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use pi_rs::{CancellationToken, Compaction, Error, InputKind, QueuedInput, Result, Session};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{Cli, observe, output, runtime};

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Configure {
        model: String,
        thinking: Option<String>,
        session: PathBuf,
    },
    Prompt {
        content: Option<String>,
    },
    Extension {
        extension_id: String,
        command: String,
        arguments: String,
    },
    Compact {
        summary: Option<String>,
        keep_turns: usize,
    },
    Steer {
        content: String,
    },
    Followup {
        content: String,
    },
    Review {
        child: PathBuf,
        from_record: Option<u64>,
        resume: bool,
        task: String,
    },
    McpStatus,
    Cancel,
    Shutdown,
}
#[derive(Default)]
struct ControlState {
    token: Option<CancellationToken>,
    queued: usize,
    pending_cancel: bool,
}

pub async fn run(mut args: Cli) -> Result<()> {
    let mut runtime = runtime::configure(&args).await?;
    let active = Arc::new(Mutex::new(ControlState::default()));
    let stopping = Arc::new(AtomicBool::new(false));
    let (commands, mut requests) = mpsc::channel(32);
    let stop_notify = Arc::new(tokio::sync::Notify::new());
    let (inputs, mut queued) = mpsc::channel(32);
    read_requests(commands, inputs, active.clone(), stopping.clone());
    let signal_active = active.clone();
    let signal_stopping = stopping.clone();
    let signal_notify = stop_notify.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_stopping.store(true, Ordering::SeqCst);
            signal_notify.notify_one();
            if let Some(cancel) = signal_active.lock().expect("worker control").token.as_ref() {
                cancel.cancel();
            }
        }
    });
    output(json!({"type":"backend_ready"}));
    while !stopping.load(Ordering::SeqCst) {
        let (request, scheduled) = tokio::select! {
            _ = stop_notify.notified() => break,
            request = requests.recv() => match request { Some(request) => (request, true), None => break },
            input = queued.recv() => match input { Some(input) => (Request::Prompt { content: Some(input.content) }, false), None => break },
        };
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        if let Request::Configure {
            model,
            thinking,
            session,
        } = request
        {
            args.model = model;
            args.thinking = thinking;
            args.session = session;
            runtime.set_thinking(args.thinking.as_deref());
            continue;
        }
        let cancel = CancellationToken::new();
        {
            let mut state = active.lock().expect("worker control");
            state.token = Some(cancel.clone());
            if scheduled {
                state.queued = state.queued.saturating_sub(1);
            }
            if std::mem::take(&mut state.pending_cancel) {
                cancel.cancel();
            }
        }
        output(json!({"type":"agent_started"}));
        let result = async {
            let mut session = Session::open(&args.session)?;
            if session.host_binding().is_some() {
                return Err(Error::Session(
                    "a managed session must be opened by its host".into(),
                ));
            }
            if session.working_directory().is_none() {
                session.set_working_directory(&args.workspace)?;
            }
            session.select_model(&args.model, args.thinking.as_deref())?;
            let agent = runtime.agent(&args);
            let mut observer = |event: pi_rs::Notification<'_>| observe(event, args.trace);
            match request {
                Request::Prompt { content } => {
                    let content = content
                        .map(|input| {
                            runtime
                                .resources
                                .expand(&input)
                                .map(|expanded| expanded.unwrap_or(input))
                        })
                        .transpose()?;
                    let answer = agent
                        .run_correlated(
                            &mut session,
                            Uuid::new_v4(),
                            content,
                            &mut queued,
                            &cancel,
                            &mut observer,
                        )
                        .await?;
                    output(json!({"type":"answer","content":answer}));
                }
                Request::Extension {
                    extension_id,
                    command,
                    arguments,
                } => agent.extension_command(
                    &mut session,
                    &extension_id,
                    &command,
                    &arguments,
                    &mut observer,
                )?,
                Request::Compact {
                    summary: Some(summary),
                    keep_turns,
                } => agent.compact(
                    &mut session,
                    Compaction {
                        summary,
                        keep_turns,
                    },
                    &mut observer,
                )?,
                Request::Compact {
                    summary: None,
                    keep_turns,
                } => {
                    agent
                        .summarize(&mut session, keep_turns, &cancel, &mut observer)
                        .await?
                }
                Request::McpStatus => runtime.mcp_status(&args)?,
                Request::Review { child, from_record, resume, task } => {
                    let mut child = Session::open(child)?;
                    let review = pi_rs_review::Review {
                        task: &task, from_record, resume, timeout: std::time::Duration::from_secs(args.model_timeout_secs),
                        output_budget: args.output_budget,
                        allowed_tools: ["read_text", "lookup_order", "read", "grep", "find", "ls"].into_iter().filter(|name| runtime.registry.has_tool(name)).map(str::to_owned).collect(),
                    };
                    let parent_id = session.session_id();
                    let child_id = child.session_id();
                    let mut child_observer = |event: pi_rs::Notification<'_>| {
                        if let pi_rs::Notification::Committed(record) = event {
                            let mut event = if args.trace { json!({"type":"record","record":record}) } else { super::metadata(record) };
                            event["review_parent_session"] = json!(parent_id);
                            event["review_child_session"] = json!(child_id);
                            output(event);
                        }
                    };
                    let answer = review.run(&agent, &mut session, &mut child, &cancel, &mut child_observer).await?;
                    output(json!({"type":"review_answer","content":answer,"child_session":child.session_id(),"parent_session":session.session_id()}));
                }
                _ => return Err(Error::Config("invalid worker operation".into())),
            }
            Ok::<(), Error>(())
        }
        .await;
        active.lock().expect("worker control").token = None;
        if let Err(error) = &result {
            output(json!({"type":"error","message":error.to_string()}));
        }
        output(json!({"type":"agent_settled","success":result.is_ok()}));
    }
    runtime.close().await
}
fn read_requests(
    commands: mpsc::Sender<Request>,
    inputs: mpsc::Sender<QueuedInput>,
    active: Arc<Mutex<ControlState>>,
    stopping: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else {
                break;
            };
            let request = match serde_json::from_str::<Request>(&line) {
                Ok(request) => request,
                Err(_) => {
                    output(json!({"type":"input_rejected","reason":"invalid worker request"}));
                    continue;
                }
            };
            let steering = matches!(&request, Request::Steer { .. });
            match request {
                Request::Steer { content } | Request::Followup { content } => {
                    let kind = if steering {
                        InputKind::Steer
                    } else {
                        InputKind::FollowUp
                    };
                    if inputs
                        .try_send(QueuedInput {
                            id: Uuid::new_v4(),
                            kind,
                            content,
                        })
                        .is_err()
                    {
                        output(json!({"type":"input_rejected","reason":"follow-up queue is full"}));
                    }
                }
                Request::Cancel => {
                    let mut state = active.lock().expect("worker control");
                    if let Some(cancel) = &state.token {
                        cancel.cancel();
                    } else if state.queued > 0 {
                        state.pending_cancel = true;
                    }
                }
                Request::Shutdown => break,
                request => {
                    let counted = !matches!(request, Request::Configure { .. });
                    if counted {
                        active.lock().expect("worker control").queued += 1;
                    }
                    if commands.try_send(request).is_err() {
                        if counted {
                            let mut state = active.lock().expect("worker control");
                            state.queued = state.queued.saturating_sub(1);
                        }
                        output(
                            json!({"type":"input_rejected","reason":"operation queue is full or closed"}),
                        );
                    }
                }
            }
        }
        stopping.store(true, Ordering::SeqCst);
        if let Some(cancel) = active.lock().expect("worker control").token.as_ref() {
            cancel.cancel();
        }
    });
}
