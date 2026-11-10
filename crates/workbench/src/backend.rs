//! Persistent native frontend worker. Journal locks are held only during a run.
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use pi_rs::{
    AgentInput, CancellationToken, Compaction, Error, InputKind, QueueMode, QueuedMessage, Result,
    Session,
};
use pi_rs_workbench::coding::CodingTools;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{Cli, observe, output, runtime};

mod queue;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Configure {
        model: String,
        thinking: Option<String>,
        #[serde(default)]
        reported_thinking: bool,
        session: PathBuf,
    },
    Prompt {
        content: Option<String>,
        #[serde(default)]
        queued_only: bool,
        #[serde(default)]
        require_command: bool,
        #[serde(default)]
        images: Vec<pi_rs::ImageContent>,
        #[serde(default)]
        rpc: bool,
        id: Option<serde_json::Value>,
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
    BranchSummary {
        selected: u64,
        instructions: Option<String>,
        reserve_tokens: u64,
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
    QueueInput {
        message: QueuedMessage,
        command: String,
        id: Option<serde_json::Value>,
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
    AbortBash,
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
    GetCommands {
        id: Option<serde_json::Value>,
    },
    ReadSession {
        request: serde_json::Value,
        path: PathBuf,
        context_window: Option<u32>,
        state: serde_json::Value,
    },
    Cancel,
    Shutdown,
}
struct ReaderControls {
    snapshot: Arc<Mutex<SessionSnapshot>>,
    auto_compaction: pi_rs::AutoCompactionControl,
    retry: pi_rs::RetryControl,
    command_catalog: serde_json::Value,
    resources: super::resources::Resources,
    registry: pi_rs::Registry,
    shell: ShellRunner,
}

#[derive(Default)]
struct ControlState {
    token: Option<CancellationToken>,
    queued: usize,
    pending_cancel: bool,
    shell: Option<CancellationToken>,
}

struct SessionSnapshot {
    records: Vec<pi_rs::Record>,
    entries: Option<super::rpc_entries::Cursor>,
}

impl SessionSnapshot {
    fn new(records: Vec<pi_rs::Record>, emit_entries: bool) -> Self {
        let mut entries = emit_entries.then(super::rpc_entries::Cursor::default);
        if let Some(cursor) = &mut entries {
            cursor.advance(&records);
        }
        Self { records, entries }
    }

    fn refresh(&mut self, session: &Session) {
        self.records = session.records().to_vec();
        if let Some(cursor) = &mut self.entries {
            cursor.advance(&self.records);
        }
    }

    fn record(&mut self, record: &pi_rs::Record) -> Vec<serde_json::Value> {
        if let Some(existing) = self.records.get_mut(record.id.saturating_sub(1) as usize) {
            *existing = record.clone();
            return Vec::new();
        }
        self.records.push(record.clone());
        self.entries
            .as_mut()
            .map_or_else(Vec::new, |cursor| cursor.appended(&self.records))
    }
}

struct ShellRunner {
    tools: Arc<CodingTools>,
    timeout: Option<std::time::Duration>,
    completed: mpsc::Sender<ShellCompletion>,
}

struct ShellCompletion {
    id: Uuid,
    request_id: Option<serde_json::Value>,
    result: Result<pi_rs::BashExecution>,
}

pub fn run(args: Cli) -> std::pin::Pin<Box<impl Future<Output = Result<()>>>> {
    // Keep the long-lived worker's future out of the frontend stack frame.
    Box::pin(run_worker(args))
}

async fn run_worker(mut args: Cli) -> Result<()> {
    let mut runtime = runtime::configure(&args, true).await?;
    let ui = Arc::new(super::extension_ui::Broker::default());
    runtime.registry.set_ui(ui.clone());
    let active = Arc::new(Mutex::new(ControlState::default()));
    let stopping = Arc::new(AtomicBool::new(false));
    let (commands, mut requests) = mpsc::channel(32);
    let (shell_completed, mut shell_results) = mpsc::channel(1);
    let stop_notify = Arc::new(tokio::sync::Notify::new());
    let (inputs, mut queued) = mpsc::channel(32);
    let initial_records = Session::open(&args.session)?.records().to_vec();
    let snapshot = Arc::new(Mutex::new(SessionSnapshot::new(
        initial_records,
        args.mode == Some(super::OutputMode::Rpc),
    )));
    read_requests(
        commands,
        inputs,
        active.clone(),
        stopping.clone(),
        ui.clone(),
        ReaderControls {
            snapshot: snapshot.clone(),
            auto_compaction: runtime.auto_compaction.clone(),
            retry: runtime.retry.clone(),
            command_catalog: runtime.command_catalog(&args),
            resources: runtime.resources.clone(),
            registry: runtime.registry.clone(),
            shell: ShellRunner {
                tools: Arc::new(
                    CodingTools::local(&args.workspace)?
                        .with_shell_config(runtime::shell_config(&args)),
                ),
                timeout: args.tool_timeout_secs.map(std::time::Duration::from_secs),
                completed: shell_completed,
            },
        },
    );
    let signal_active = active.clone();
    let signal_stopping = stopping.clone();
    let signal_notify = stop_notify.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_stopping.store(true, Ordering::SeqCst);
            signal_notify.notify_one();
            cancel_active(&signal_active);
        }
    });
    output(json!({"type":"backend_ready"}));
    let mut continue_queue = false;
    while !stopping.load(Ordering::SeqCst) {
        let (request, scheduled) = tokio::select! {
            _ = stop_notify.notified() => break,
            Some(shell) = shell_results.recv() => {
                save_shell(shell, &args, &snapshot, &active);
                continue;
            }
            request = requests.recv() => match request { Some(request) => (request, true), None => break },
            input = queued.recv() => match input {
                Some(input) => {
                    let mut replies = Vec::new();
                    let result = Session::open(&args.session).and_then(|mut session| {
                        if session.host_binding().is_some() { return Err(Error::Session("managed session queue controls require its host".into())); }
                        snapshot.lock().expect("session snapshot").refresh(&session);
                        let mut observer = |event: pi_rs::Notification<'_>| observe_snapshot(event, args.trace, &snapshot);
                        let capacity = queued.max_capacity();
                        let pending = std::iter::from_fn(|| queued.try_recv().ok()).take(capacity);
                        let mut starts_run = false;
                        for input in std::iter::once(input).chain(pending) {
                            match input {
                                AgentInput::Enqueue { message, reply } => {
                                    let result = AgentInput::from(message).apply(&mut session, &mut observer);
                                    replies.push((reply, result));
                                }
                                input => {
                                    starts_run |= matches!(input, AgentInput::Message(_));
                                    input.apply(&mut session, &mut observer)?;
                                }
                            }
                        }
                        Ok(starts_run && !session.queued_inputs().is_empty())
                    });
                    for (reply, result) in replies {
                        let _ = reply.send(result);
                    }
                    match result {
                        Ok(true) => (Request::Prompt { content: None, images: vec![], queued_only: true, require_command: false, rpc: false, id: None }, false),
                        Ok(false) => continue,
                        Err(error) => { output(json!({"type":"error","message":error.to_string()})); continue; }
                    }
                }
                None => break,
            },
            _ = std::future::ready(()), if continue_queue => (Request::Prompt { content: None, images: vec![], queued_only: true, require_command: false, rpc: false, id: None }, false),
        };
        if stopping.load(Ordering::SeqCst) {
            break;
        }
        if let Request::Configure {
            model,
            thinking,
            reported_thinking,
            session,
        } = request
        {
            args.model = model;
            args.thinking = thinking;
            args.session = session;
            runtime.configure_model_options(&args, reported_thinking)?;
            snapshot
                .lock()
                .expect("session snapshot")
                .refresh(&Session::open(&args.session)?);
            continue;
        }
        let request = match request {
            Request::QueueInput {
                message,
                command,
                id,
            } => {
                let result = apply_idle_queue(&args, &snapshot, AgentInput::from(message))
                    .map(|()| json!({"disposition":"queued"}));
                control_response(&command, id, result);
                continue;
            }
            Request::ClearQueue { id } => {
                let (reply, mut receiver) = tokio::sync::oneshot::channel();
                let result = apply_idle_queue(&args, &snapshot, AgentInput::ClearQueue { reply })
                    .and_then(|()| {
                        receiver
                            .try_recv()
                            .map_err(|_| Error::Config("queue control channel closed".into()))?
                    })
                    .map(queue::clear_payload);
                control_response("clear_queue", id, result);
                continue;
            }
            request => request,
        };
        continue_queue = false;
        let resume_queued = matches!(
            &request,
            Request::Prompt { .. } | Request::Compact { .. } | Request::Review { .. }
        );
        let compact_response = match &request {
            Request::Compact { rpc: true, id, .. } => Some(id.clone()),
            _ => None,
        };
        let mut prompt_response = match &request {
            Request::Prompt { rpc: true, id, .. } => Some(id.clone()),
            _ => None,
        };
        let mut started = prompt_response.is_none();
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
        if started {
            output(json!({"type":"agent_started"}));
        }
        let mut completed_shells = Vec::new();
        let mut run_session = None;
        let mut operation = Box::pin(async {
            if matches!(&request, Request::Prompt { .. } | Request::McpStatus) {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(Error::Cancelled),
                    result = async {
                        runtime.authorize_pending_mcp(&args, ui.as_ref(), &cancel).await?;
                        runtime.refresh_mcp(&args).await
                    } => result?,
                }
            }
            let session = run_session.insert(Session::open(&args.session)?);
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
            args.record_model_selection(session)?;
            initialize_queue_modes(session, &args)?;
            snapshot.lock().expect("session snapshot").refresh(session);
            let agent = runtime.agent(&args, session.session_id());
            let mut observer =
                |event: pi_rs::Notification<'_>| observe_snapshot(event, args.trace, &snapshot);
            match request {
                Request::Prompt {
                    mut content,
                    mut images,
                    queued_only,
                    require_command,
                    ..
                } => {
                    if queued_only && !agent.apply_queued_inputs(session, &mut observer)? {
                        return Ok(());
                    }
                    if images.is_empty()
                        && let Some(input) = content.as_deref()
                        && agent.try_extension_command(session, input, &mut observer)?
                    {
                        return Ok(());
                    }
                    if runtime.registry.has_input_hooks()
                        && (content.is_some() || !images.is_empty())
                    {
                        let event = pi_rs::InputEvent {
                            text: content.take().unwrap_or_default(),
                            images,
                            source: if args.mode == Some(super::OutputMode::Rpc) {
                                pi_rs::InputSource::Rpc
                            } else {
                                pi_rs::InputSource::Interactive
                            },
                            streaming_behavior: None,
                        };
                        let states = session.extension_states();
                        let prepared = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return Err(Error::Cancelled),
                            result = runtime.registry.prepare_input(event, &states) => result?,
                        };
                        let Some(prepared) = prepared else {
                            return Ok(());
                        };
                        content = Some(prepared.text);
                        images = prepared.images;
                    }
                    let mut content = content
                        .map(|input| {
                            let expanded = runtime.resources.expand(&input)?;
                            if require_command && expanded.is_none() {
                                return Err(Error::Config("Unknown command. Use /help.".into()));
                            }
                            Ok(expanded.unwrap_or(input))
                        })
                        .transpose()?;
                    if images.is_empty()
                        && content
                            .as_ref()
                            .is_some_and(|content| content.trim().is_empty())
                    {
                        return Err(Error::Config("empty user input".into()));
                    }
                    if cancel.is_cancelled() {
                        return Err(Error::Cancelled);
                    }
                    if let Some(id) = prompt_response.take() {
                        started = true;
                        control_response("prompt", id, Ok(json!({"disposition":"started"})));
                        output(json!({"type":"agent_started"}));
                    }
                    let run_id = Uuid::new_v4();
                    if !images.is_empty() {
                        session.append_user_with_images(
                            content.take().unwrap_or_default(),
                            images,
                            Some(run_id),
                        )?;
                        observer(pi_rs::Notification::Committed(
                            session.records().last().expect("appended user input"),
                        ));
                    }
                    let answer = Box::pin(agent.run_correlated(
                        session,
                        run_id,
                        content,
                        &mut queued,
                        &cancel,
                        &mut observer,
                    ))
                    .await?;
                    output(json!({"type":"answer","content":answer}));
                }
                Request::Extension {
                    extension_id,
                    command,
                    arguments,
                } => agent.extension_command(
                    session,
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
                    session,
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
                        agent
                            .summarize_configured(
                                session,
                                &cancel,
                                &mut observer,
                                Some(&mut queued),
                                instructions.as_deref(),
                            )
                            .await?;
                    } else {
                        agent
                            .summarize_with_instructions(
                                session,
                                keep_turns,
                                &cancel,
                                &mut observer,
                                Some(&mut queued),
                                instructions.as_deref(),
                            )
                            .await?;
                    }
                    if let Some(record) = session.records().last()
                        && let pi_rs::Event::Compacted {
                            summary,
                            retain_from,
                            ..
                        } = &record.event
                    {
                        compact_result = json!({
                            "summary": summary,
                            "firstKeptEntryId": retain_from.unwrap_or(record.id).to_string(),
                            "tokensBefore": tokens_before,
                        });
                    }
                }
                Request::BranchSummary {
                    selected,
                    instructions,
                    reserve_tokens,
                } => {
                    Box::pin(agent.summarize_branch(
                        session,
                        selected,
                        instructions.as_deref(),
                        reserve_tokens,
                        &cancel,
                        &mut observer,
                    ))
                    .await?;
                }
                Request::McpStatus => runtime.mcp_status(&args)?,
                Request::Review {
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
                        timeout: std::time::Duration::from_secs(args.model_timeout_secs),
                        output_budget: args.output_budget,
                        allowed_tools: ["read_text", "lookup_order", "read", "grep", "find", "ls"]
                            .into_iter()
                            .filter(|name| runtime.registry.has_tool(name))
                            .map(str::to_owned)
                            .collect(),
                    };
                    let parent_id = session.session_id();
                    let child_id = child.session_id();
                    let mut child_observer = |event: pi_rs::Notification<'_>| {
                        if let pi_rs::Notification::Committed(record) = event {
                            let mut event = if args.trace {
                                json!({"type":"record","record":record})
                            } else {
                                super::metadata(record)
                            };
                            event["review_parent_session"] = json!(parent_id);
                            event["review_child_session"] = json!(child_id);
                            output(event);
                        }
                    };
                    let answer = review
                        .run_with_controls(
                            &agent,
                            session,
                            &mut child,
                            &cancel,
                            &mut child_observer,
                            Some(pi_rs_review::ReviewControls {
                                inputs: &mut queued,
                                observer: &mut observer,
                            }),
                        )
                        .await?;
                    output(
                        json!({"type":"review_answer","content":answer,"child_session":child.session_id(),"parent_session":session.session_id()}),
                    );
                }
                _ => return Err(Error::Config("invalid worker operation".into())),
            }
            Ok::<(), Error>(())
        });
        let result = loop {
            tokio::select! {
                result = &mut operation => break result,
                Some(shell) = shell_results.recv(), if completed_shells.len() < 32 => {
                    finish_shell(&shell, &active);
                    if let Ok(execution) = shell.result {
                        completed_shells.push(execution);
                    }
                }
            }
        };
        drop(operation);
        active.lock().expect("worker control").token = None;
        for execution in completed_shells {
            let recorded = match run_session.as_mut() {
                Some(session) => record_shell(session, &execution, &args, &snapshot),
                None => Session::open(&args.session).and_then(|mut session| {
                    record_shell(&mut session, &execution, &args, &snapshot)
                }),
            };
            if let Err(error) = recorded {
                output(
                    json!({"type":"error","message":format!("Could not record shell output: {error}")}),
                );
            }
        }
        drop(run_session);
        if started && resume_queued && (result.is_ok() || matches!(&result, Err(Error::Cancelled)))
        {
            continue_queue = Session::open(&args.session)
                .is_ok_and(|session| !session.queued_inputs().is_empty());
        }
        if let Some(id) = prompt_response {
            control_response(
                "prompt",
                id,
                result
                    .as_ref()
                    .map(|_| json!({"disposition":"handled"}))
                    .map_err(|error| Error::Config(error.to_string())),
            );
        } else if let Err(error) = &result {
            output(json!({"type":"error","message":error.to_string()}));
        }
        output(
            json!({"type":"agent_settled","success":result.is_ok(),"aborted":matches!(result, Err(Error::Cancelled)),"started":started}),
        );
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
    cancel_active(&active);
    let shell_pending = active.lock().expect("worker control").shell.is_some();
    if shell_pending && let Some(shell) = shell_results.recv().await {
        save_shell(shell, &args, &snapshot, &active);
    }
    runtime.close().await
}

fn cancel_active(active: &Mutex<ControlState>) {
    let state = active.lock().expect("worker control");
    for token in state.token.iter().chain(&state.shell) {
        token.cancel();
    }
}

fn save_shell(
    mut shell: ShellCompletion,
    args: &Cli,
    snapshot: &Mutex<SessionSnapshot>,
    active: &Mutex<ControlState>,
) {
    if let Ok(execution) = &shell.result
        && let Err(error) = Session::open(&args.session)
            .and_then(|mut session| record_shell(&mut session, execution, args, snapshot))
    {
        shell.result = Err(error);
    }
    finish_shell(&shell, active);
}

fn record_shell(
    session: &mut Session,
    execution: &pi_rs::BashExecution,
    args: &Cli,
    snapshot: &Mutex<SessionSnapshot>,
) -> Result<()> {
    if session.working_directory().is_none() {
        session.set_working_directory(&args.workspace)?;
        snapshot.lock().expect("session snapshot").refresh(session);
    }
    session.append_local_bash_result(execution.clone())?;
    observe_snapshot(
        pi_rs::Notification::Committed(session.records().last().expect("saved shell execution")),
        args.trace,
        snapshot,
    );
    Ok(())
}

fn finish_shell(shell: &ShellCompletion, active: &Mutex<ControlState>) {
    let mut state = active.lock().expect("worker control");
    state.shell = None;
    report_shell_end(shell.id, shell.request_id.as_ref(), &shell.result);
}

fn report_shell_end(
    id: Uuid,
    request_id: Option<&serde_json::Value>,
    result: &Result<pi_rs::BashExecution>,
) {
    match result {
        Ok(execution) => output(
            json!({"type":"bash_execution_end","executionId":id,"data":execution,"id":request_id}),
        ),
        Err(error) => output(
            json!({"type":"bash_execution_end","executionId":id,"error":error.to_string(),"id":request_id}),
        ),
    }
}
fn apply_idle_queue(
    args: &Cli,
    snapshot: &Mutex<SessionSnapshot>,
    input: AgentInput,
) -> Result<()> {
    let mut session = Session::open(&args.session)?;
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "managed session queue controls require its host".into(),
        ));
    }
    snapshot.lock().expect("session snapshot").refresh(&session);
    input.apply(&mut session, &mut |event| {
        observe_snapshot(event, args.trace, snapshot)
    })
}

fn observe_snapshot(
    event: pi_rs::Notification<'_>,
    trace: bool,
    snapshot: &Mutex<SessionSnapshot>,
) {
    let entries = if let pi_rs::Notification::Committed(record) = &event {
        snapshot.lock().expect("session snapshot").record(record)
    } else {
        Vec::new()
    };
    observe(event, trace);
    for entry in entries {
        output(json!({"type":"entry_appended","entry":entry}));
    }
}

fn read_requests(
    commands: mpsc::Sender<Request>,
    inputs: mpsc::Sender<AgentInput>,
    active: Arc<Mutex<ControlState>>,
    stopping: Arc<AtomicBool>,
    ui: Arc<super::extension_ui::Broker>,
    controls: ReaderControls,
) {
    let ReaderControls {
        auto_compaction,
        retry,
        command_catalog,
        resources,
        registry,
        snapshot,
        shell,
    } = controls;
    let runtime = tokio::runtime::Handle::current();
    let prepare_in_background = registry.has_input_hooks();
    let processor = Arc::new(queue::Processor {
        commands: commands.clone(),
        inputs,
        active: active.clone(),
        snapshot: snapshot.clone(),
        resources,
        registry,
    });
    let (queue_requests, queue) = mpsc::channel(32);
    let queue = prepare_in_background.then(|| runtime.spawn(processor.clone().run(queue)));
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if stopping.load(Ordering::SeqCst) {
                break;
            }
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
            match request {
                Request::ReadSession {
                    request,
                    path,
                    context_window,
                    state,
                } => {
                    let records = snapshot.lock().expect("session snapshot").records.clone();
                    let result = Session::from_records(records).and_then(|session| {
                        super::rpc::snapshot_response(
                            &request,
                            &session,
                            &path,
                            context_window,
                            state,
                        )
                    });
                    control_response(
                        request["type"].as_str().unwrap_or_default(),
                        request.get("id").cloned(),
                        result,
                    );
                }
                Request::GetCommands { id } => {
                    control_response("get_commands", id, Ok(command_catalog.clone()));
                }
                Request::Bash {
                    command,
                    exclude_from_context,
                    request_id,
                } => {
                    let id = Uuid::new_v4();
                    let cancel = CancellationToken::new();
                    {
                        let mut state = active.lock().expect("worker control");
                        if stopping.load(Ordering::SeqCst) {
                            break;
                        }
                        if state.shell.is_some() {
                            report_shell_end(
                                id,
                                request_id.as_ref(),
                                &Err(Error::Config("a shell command is already running".into())),
                            );
                            continue;
                        }
                        state.shell = Some(cancel.clone());
                    }
                    let allowed = {
                        let records = snapshot.lock().expect("session snapshot");
                        !records
                            .records
                            .iter()
                            .any(|record| matches!(record.event, pi_rs::Event::HostBound { .. }))
                    };
                    let tools = shell.tools.clone();
                    let timeout = shell.timeout;
                    let completed = shell.completed.clone();
                    runtime.spawn(async move {
                        output(json!({"type":"bash_execution_start","executionId":id,"command":command,"excludeFromContext":exclude_from_context,"id":request_id}));
                        let result = async {
                            if !allowed {
                                return Err(Error::Session("a managed session must execute shell commands through its host".into()));
                            }
                            let mut execution = tools.execute_shell(&command, timeout, &cancel, &mut |chunk| {
                                output(json!({"type":"bash_execution_update","executionId":id,"output":chunk,"id":request_id}));
                            }).await?;
                            execution.exclude_from_context = exclude_from_context;
                            Ok(execution)
                        }.await;
                        let _ = completed.send(ShellCompletion { id, request_id, result }).await;
                    });
                }
                Request::AbortBash => {
                    if let Some(cancel) = active.lock().expect("worker control").shell.as_ref() {
                        cancel.cancel();
                    }
                }
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
                Request::ExtensionUiResponse { id, value } => ui.resolve(id, value),
                request @ (Request::Steer { .. }
                | Request::Followup { .. }
                | Request::QueueInput { .. }
                | Request::ClearQueue { .. }
                | Request::SetQueueMode { .. }) => {
                    if prepare_in_background {
                        if let Err(error) = queue_requests.try_send(request) {
                            queue::reject(error.into_inner());
                        }
                    } else {
                        runtime.block_on(processor.handle(request));
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
                    if let Err(error) = commands.try_send(request) {
                        if counted {
                            let mut state = active.lock().expect("worker control");
                            state.queued = state.queued.saturating_sub(1);
                        }
                        let message = "operation queue is full or closed";
                        if let Request::Prompt { rpc: true, id, .. } = error.into_inner() {
                            control_response("prompt", id, Err(Error::Config(message.into())));
                        } else {
                            output(json!({"type":"input_rejected","reason":message}));
                        }
                    }
                }
            }
        }
        if let Some(queue) = queue {
            queue.abort();
        }
        stopping.store(true, Ordering::SeqCst);
        cancel_active(&active);
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
