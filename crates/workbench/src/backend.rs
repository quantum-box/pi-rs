//! Persistent native frontend worker. Journal locks are held only during a run.
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use pi_rs::{
    AgentInput, CancellationToken, Compaction, Error, InputKind, QueueMode, QueuedInput,
    QueuedMessage, Result, Session,
};
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
        #[serde(default)]
        queued_only: bool,
        #[serde(default)]
        images: Vec<pi_rs::ImageContent>,
    },
    Extension {
        extension_id: String,
        command: String,
        arguments: String,
    },
    Compact {
        summary: Option<String>,
        keep_turns: usize,
        instructions: Option<String>,
        #[serde(default)]
        use_token_settings: bool,
        #[serde(default)]
        rpc: bool,
        id: Option<serde_json::Value>,
    },
    Steer {
        content: String,
        #[serde(default)]
        images: Vec<pi_rs::ImageContent>,
    },
    Followup {
        content: String,
        #[serde(default)]
        images: Vec<pi_rs::ImageContent>,
    },
    Review {
        child: PathBuf,
        from_record: Option<u64>,
        resume: bool,
        task: String,
    },
    McpStatus,
    Bash {
        command: String,
        #[serde(default)]
        exclude_from_context: bool,
        request_id: Option<serde_json::Value>,
    },
    ExtensionUiResponse {
        id: Uuid,
        value: pi_rs::UiResponse,
    },
    ClearQueue {
        id: Option<serde_json::Value>,
    },
    SetQueueMode {
        kind: InputKind,
        mode: QueueMode,
        id: Option<serde_json::Value>,
    },
    SetAutoCompaction {
        enabled: bool,
        id: Option<serde_json::Value>,
    },
    SetAutoRetry {
        enabled: bool,
        id: Option<serde_json::Value>,
    },
    AbortRetry {
        id: Option<serde_json::Value>,
    },
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
    let ui = Arc::new(super::extension_ui::Broker::default());
    runtime.registry.set_ui(ui.clone());
    let active = Arc::new(Mutex::new(ControlState::default()));
    let stopping = Arc::new(AtomicBool::new(false));
    let (commands, mut requests) = mpsc::channel(32);
    let stop_notify = Arc::new(tokio::sync::Notify::new());
    let (inputs, mut queued) = mpsc::channel(32);
    read_requests(
        commands,
        inputs,
        active.clone(),
        stopping.clone(),
        ui,
        runtime.auto_compaction.clone(),
        runtime.retry.clone(),
    );
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
    let mut continue_queue = false;
    while !stopping.load(Ordering::SeqCst) {
        let (request, scheduled) = tokio::select! {
            _ = stop_notify.notified() => break,
            request = requests.recv() => match request { Some(request) => (request, true), None => break },
            input = queued.recv() => match input {
                Some(input) => {
                    let mut starts_run = matches!(input, AgentInput::Message(_));
                    let result = Session::open(&args.session).and_then(|mut session| {
                        if session.host_binding().is_some() { return Err(Error::Session("managed session queue controls require its host".into())); }
                        let mut observer = |event: pi_rs::Notification<'_>| observe(event, args.trace);
                        input.apply(&mut session, &mut observer)?;
                        for _ in 0..queued.max_capacity() {
                            let Ok(input) = queued.try_recv() else { break; };
                            starts_run |= matches!(input, AgentInput::Message(_));
                            input.apply(&mut session, &mut observer)?;
                        }
                        Ok(starts_run && !session.queued_inputs().is_empty())
                    });
                    match result {
                        Ok(true) => (Request::Prompt { content: None, images: vec![], queued_only: true }, false),
                        Ok(false) => continue,
                        Err(error) => { output(json!({"type":"error","message":error.to_string()})); continue; }
                    }
                }
                None => break,
            },
            _ = std::future::ready(()), if continue_queue => (Request::Prompt { content: None, images: vec![], queued_only: true }, false),
        };
        continue_queue = false;
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
        let resume_queued = matches!(
            &request,
            Request::Prompt { .. } | Request::Compact { .. } | Request::Review { .. }
        );
        let compact_response = match &request {
            Request::Compact { rpc: true, id, .. } => Some(id.clone()),
            _ => None,
        };
        let mut compact_result = serde_json::Value::Null;
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
            if matches!(&request, Request::Prompt { .. }) {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    result = runtime.refresh_mcp(&args) => result?,
                }
            }
            let mut session = Session::open(&args.session)?;
            session.set_auto_compaction_control(runtime.auto_compaction.clone());
            session.set_retry_control(runtime.retry.clone());
            if session.host_binding().is_some() {
                return Err(Error::Session(
                    "a managed session must be opened by its host".into(),
                ));
            }
            if session.working_directory().is_none() {
                session.set_working_directory(&args.workspace)?;
            }
            session.select_model(&args.model, args.thinking.as_deref())?;
            initialize_queue_modes(&mut session, &args)?;
            let agent = runtime.agent(&args);
            let mut observer = |event: pi_rs::Notification<'_>| observe(event, args.trace);
            match request {
                Request::Prompt { content, images, queued_only } => {
                    if queued_only && !agent.apply_queued_inputs(&mut session, &mut observer)? { return Ok(()); }
                    let mut content = content
                        .map(|input| {
                            runtime
                                .resources
                                .expand(&input)
                                .map(|expanded| expanded.unwrap_or(input))
                        })
                        .transpose()?;
                    let run_id = Uuid::new_v4();
                    if !images.is_empty() {
                        session.append_user_with_images(content.take().unwrap_or_default(), images, Some(run_id))?;
                        observer(pi_rs::Notification::Committed(session.records().last().expect("appended user input")));
                    }
                    let answer = agent
                        .run_correlated(
                            &mut session,
                            run_id,
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
                    ..
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
                    instructions,
                    use_token_settings,
                    ..
                } => {
                    let tokens_before = session.estimated_context_tokens();
                    if use_token_settings {
                        agent.summarize_configured(
                            &mut session, &cancel, &mut observer, Some(&mut queued), instructions.as_deref(),
                        ).await?;
                    } else {
                        agent.summarize_with_instructions(
                            &mut session, keep_turns, &cancel, &mut observer, Some(&mut queued), instructions.as_deref(),
                        ).await?;
                    }
                    if let Some(record) = session.records().last()
                        && let pi_rs::Event::Compacted { summary, retain_from, .. } = &record.event
                    {
                        compact_result = json!({
                            "summary": summary,
                            "firstKeptEntryId": retain_from.unwrap_or(record.id).to_string(),
                            "tokensBefore": tokens_before,
                        });
                    }
                }
                Request::McpStatus => runtime.mcp_status(&args)?,
                Request::Bash { command, exclude_from_context, request_id } => {
                    let execution_id = Uuid::new_v4();
                    output(json!({"type":"bash_execution_start","executionId":execution_id,"command":command,"excludeFromContext":exclude_from_context,"id":request_id}));
                    let result = async {
                        session.ensure_settled()?;
                        let tools = pi_rs_workbench::coding::CodingTools::local(&args.workspace)?;
                        let mut execution = tools.execute_shell(&command, args.tool_timeout_secs.map(std::time::Duration::from_secs), &cancel, &mut |chunk| {
                            output(json!({"type":"bash_execution_update","executionId":execution_id,"output":chunk,"id":request_id}));
                        }).await?;
                        execution.exclude_from_context = exclude_from_context;
                        session.append_bash_execution(execution.clone())?;
                        observer(pi_rs::Notification::Committed(session.records().last().expect("saved shell execution")));
                        Ok::<_, Error>(execution)
                    }.await;
                    match result {
                        Ok(execution) => output(json!({"type":"bash_execution_end","executionId":execution_id,"data":execution,"id":request_id})),
                        Err(error) => {
                            output(json!({"type":"bash_execution_end","executionId":execution_id,"error":error.to_string(),"id":request_id}));
                            return Err(error);
                        }
                    }
                }

                Request::Review { child, from_record, resume, task } => {
                    let mut child = Session::open(child)?;
                    child.set_auto_compaction_control(runtime.auto_compaction.clone());
            child.set_retry_control(runtime.retry.clone());
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
                    let answer = review.run_with_controls(&agent, &mut session, &mut child, &cancel, &mut child_observer, Some(pi_rs_review::ReviewControls { inputs: &mut queued, observer: &mut observer })).await?;
                    output(json!({"type":"review_answer","content":answer,"child_session":child.session_id(),"parent_session":session.session_id()}));
                }
                _ => return Err(Error::Config("invalid worker operation".into())),
            }
            Ok::<(), Error>(())
        }
        .await;
        active.lock().expect("worker control").token = None;
        if resume_queued && (result.is_ok() || matches!(&result, Err(Error::Cancelled))) {
            continue_queue = Session::open(&args.session)
                .is_ok_and(|session| !session.queued_inputs().is_empty());
        }
        if let Err(error) = &result {
            output(json!({"type":"error","message":error.to_string()}));
        }
        output(json!({"type":"agent_settled","success":result.is_ok()}));
        if let Some(id) = compact_response {
            control_response(
                "compact",
                id,
                result
                    .as_ref()
                    .map(|_| compact_result)
                    .map_err(|error| Error::Config(error.to_string())),
            );
        }
    }
    runtime.close().await
}
fn read_requests(
    commands: mpsc::Sender<Request>,
    inputs: mpsc::Sender<AgentInput>,
    active: Arc<Mutex<ControlState>>,
    stopping: Arc<AtomicBool>,
    ui: Arc<super::extension_ui::Broker>,
    auto_compaction: pi_rs::AutoCompactionControl,
    retry: pi_rs::RetryControl,
) {
    let runtime = tokio::runtime::Handle::current();
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
                Request::SetAutoRetry { enabled, id } => {
                    retry.set_enabled(enabled);
                    control_response("set_auto_retry", id, Ok(serde_json::Value::Null));
                }
                Request::AbortRetry { id } => {
                    retry.abort();
                    control_response("abort_retry", id, Ok(serde_json::Value::Null));
                }
                Request::SetAutoCompaction { enabled, id } => {
                    auto_compaction.set_enabled(enabled);
                    control_response("set_auto_compaction", id, Ok(serde_json::Value::Null));
                }
                Request::Steer { content, images } | Request::Followup { content, images } => {
                    let kind = if steering {
                        InputKind::Steer
                    } else {
                        InputKind::FollowUp
                    };
                    let message = QueuedMessage {
                        input: QueuedInput {
                            id: Uuid::new_v4(),
                            kind,
                            content,
                        },
                        images,
                    };
                    if let Err(error) = message.validate() {
                        output(json!({"type":"input_rejected","reason":error.to_string()}));
                    } else if inputs.try_send(message.into()).is_err() {
                        output(json!({"type":"input_rejected","reason":"follow-up queue is full"}));
                    }
                }
                Request::ExtensionUiResponse { id, value } => ui.resolve(id, value),
                Request::ClearQueue { id } => {
                    let (reply, receiver) = tokio::sync::oneshot::channel();
                    let _ = inputs.try_send(AgentInput::ClearQueue { reply });
                    runtime.spawn(async move {
                        let result = receiver.await.map_err(|_| Error::Config("queue control channel closed".into())).and_then(|result| result).map(|messages| {
                            let steering: Vec<_> = messages.iter().filter(|message| message.input.kind == InputKind::Steer).map(|message| message.input.content.clone()).collect();
                            let follow_up: Vec<_> = messages.iter().filter(|message| message.input.kind == InputKind::FollowUp).map(|message| message.input.content.clone()).collect();
                            json!({"steering":steering,"followUp":follow_up,"messages":messages})
                        });
                        control_response("clear_queue", id, result);
                    });
                }
                Request::SetQueueMode { kind, mode, id } => {
                    let (reply, receiver) = tokio::sync::oneshot::channel();
                    let _ = inputs.try_send(AgentInput::SetQueueMode { kind, mode, reply });
                    runtime.spawn(async move {
                        let result = async {
                            receiver.await.map_err(|_| {
                                Error::Config("queue control channel closed".into())
                            })??;
                            tokio::task::spawn_blocking(move || {
                                let key = if kind == InputKind::Steer {
                                    "steeringMode"
                                } else {
                                    "followUpMode"
                                };
                                super::config::update_object(
                                    &super::config::agent_dir().join("settings.json"),
                                    |settings| {
                                        settings.insert(key.into(), json!(mode));
                                        Ok(())
                                    },
                                )
                            })
                            .await
                            .map_err(|error| Error::Config(error.to_string()))??;
                            Ok(json!({"mode":mode}))
                        }
                        .await;
                        control_response(
                            if kind == InputKind::Steer {
                                "set_steering_mode"
                            } else {
                                "set_follow_up_mode"
                            },
                            id,
                            result,
                        );
                    });
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

fn control_response(
    command: &str,
    id: Option<serde_json::Value>,
    result: Result<serde_json::Value>,
) {
    let mut response = json!({"type":"response","command":command,"success":result.is_ok()});
    if let Some(id) = id {
        response["id"] = id;
    }
    match result {
        Ok(data) if !data.is_null() => response["data"] = data,
        Ok(_) => {}
        Err(error) => response["error"] = json!(error.to_string()),
    }
    output(response);
}

pub(super) fn configured_queue_mode(args: &Cli, key: &str) -> Result<QueueMode> {
    Ok(serde_json::from_value(
        args.settings
            .value()
            .get(key)
            .cloned()
            .unwrap_or_else(|| json!("one-at-a-time")),
    )?)
}
pub(super) fn initialize_queue_modes(session: &mut Session, args: &Cli) -> Result<()> {
    let settings = super::config::Settings::load(&args.workspace, args.trusted)?;
    for (kind, key) in [
        (InputKind::Steer, "steeringMode"),
        (InputKind::FollowUp, "followUpMode"),
    ] {
        if !session.branch().iter().any(|record| matches!(record.event, pi_rs::Event::QueueModeSelected { kind: selected, .. } if selected == kind)) {
            let mode = settings.value().get(key).cloned().unwrap_or_else(|| json!("one-at-a-time"));
            session.set_queue_mode(kind, serde_json::from_value(mode)?)?;
        }
    }
    Ok(())
}
