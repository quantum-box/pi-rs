//! JSONL control over the same subprocess and journal as the terminal UI.
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::time::Duration;

use pi_rs::{Error, Result, Session};
use serde_json::{Value, json};

use super::{Cli, interactive, process::Worker, sessions};

pub(super) fn run(mut args: Cli) -> Result<()> {
    if args.command.is_some() || !args.prompt.is_empty() {
        return Err(Error::Config("RPC accepts prompts through stdin".into()));
    }
    if let Some(parent) = args
        .session
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    // Validate and release the journal before starting any child process.
    let mut session = Session::open(&args.session)?;
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "a managed session must be opened by its host".into(),
        ));
    }
    if session.working_directory().is_none() {
        session.set_working_directory(&args.workspace)?;
    }
    super::backend::initialize_queue_modes(&mut session, &args)?;
    drop(session);
    interactive::restore_workspace(&mut args)?;
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let mut worker: Option<Worker> = None;
    let mut pending_abort: Option<Value> = None;
    let mut pending_bash: Option<Value> = None;
    let mut pending_compact: Option<Value> = None;
    let mut pending_catalogs: Vec<Value> = Vec::new();
    loop {
        if let Some(active) = &mut worker {
            for event in active.events.try_iter() {
                if event["type"] == "response" && event["command"] == "get_commands" {
                    pending_catalogs.retain(|request| request["id"] != event["id"]);
                }
                match event["type"].as_str() {
                    Some("auto_retry_start") => active.retrying = true,
                    Some("auto_retry_end" | "agent_settled" | "request") => active.retrying = false,
                    _ => {}
                }
                forward_event(
                    event,
                    &mut active.busy,
                    &mut pending_abort,
                    &mut pending_bash,
                    &mut pending_compact,
                )?;
            }
            if let Some(status) = active.child.try_wait()? {
                let mut finished = worker.take().expect("active worker");
                for reader in finished.readers.drain(..) {
                    let _ = reader.join();
                }
                for event in finished.events.try_iter() {
                    if event["type"] == "response" && event["command"] == "get_commands" {
                        pending_catalogs.retain(|request| request["id"] != event["id"]);
                    }
                    forward_event(
                        event,
                        &mut finished.busy,
                        &mut pending_abort,
                        &mut pending_bash,
                        &mut pending_compact,
                    )?;
                }
                for request in pending_catalogs.drain(..) {
                    response(
                        &request,
                        Err(Error::Config(
                            "command discovery worker exited before returning a result".into(),
                        )),
                    )?;
                }
                if finished.busy {
                    emit(json!({"type":"agent_end","success":status.success()}))?;
                    emit(json!({"type":"agent_settled"}))?;
                }
                if let Some(request) = pending_abort.take() {
                    response(&request, Ok(None))?;
                }
                if let Some(request) = pending_compact.take() {
                    response(
                        &request,
                        Err(Error::Config(
                            "compaction worker exited before returning a result".into(),
                        )),
                    )?;
                }
                if let Some(request) = pending_bash.take() {
                    response(
                        &request,
                        Err(Error::Config(
                            "shell worker exited before returning a result".into(),
                        )),
                    )?;
                }
            }
        }
        // Keep a following session change from replacing the discovery worker.
        if !pending_catalogs.is_empty() {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        let line = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(line) => line?,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let request: Value = match serde_json::from_str(&line) {
            Ok(Value::Object(value)) => Value::Object(value),
            _ => {
                response(
                    &json!({"type":"parse"}),
                    Err(Error::Config("expected a JSON object".into())),
                )?;
                continue;
            }
        };
        if request["type"] == "extension_ui_response" {
            if let Some(active) = &mut worker {
                let value = if request["cancelled"] == true {
                    Value::Null
                } else {
                    request
                        .get("confirmed")
                        .unwrap_or(&request["value"])
                        .clone()
                };
                active.send(
                    json!({"type":"extension_ui_response","id":request["id"],"value":value}),
                )?;
            }
            continue;
        }
        if matches!(
            request["type"].as_str(),
            Some("clear_queue" | "set_steering_mode" | "set_follow_up_mode")
        ) {
            let result = (|| {
                let command = request["type"].as_str().expect("queue command");
                let control = if command == "clear_queue" {
                    json!({"type":"clear_queue","id":request.get("id")})
                } else {
                    let mode: pi_rs::QueueMode = serde_json::from_value(request["mode"].clone())?;
                    let kind = if command == "set_steering_mode" {
                        pi_rs::InputKind::Steer
                    } else {
                        pi_rs::InputKind::FollowUp
                    };
                    json!({"type":"set_queue_mode","kind":kind,"mode":mode,"id":request.get("id")})
                };
                Worker::control(&mut worker, &args, control)?;
                if command != "clear_queue" {
                    let mode: pi_rs::QueueMode = serde_json::from_value(request["mode"].clone())?;
                    let active = worker.as_mut().expect("control worker");
                    if command == "set_steering_mode" {
                        active.steering_mode = mode;
                    } else {
                        active.follow_up_mode = mode;
                    }
                }
                Ok::<_, Error>(())
            })();
            if let Err(error) = result {
                response(&request, Err(error))?;
            }
            continue;
        }
        if request["type"] == "get_commands" {
            match Worker::control(&mut worker, &args, request.clone()) {
                Ok(()) => pending_catalogs.push(request),
                Err(error) => response(&request, Err(error))?,
            }
            continue;
        }
        if request["type"] == "set_auto_retry" || request["type"] == "abort_retry" {
            let result = (|| {
                let mut control = request.clone();
                if request["type"] == "set_auto_retry" {
                    let enabled = request["enabled"]
                        .as_bool()
                        .ok_or_else(|| Error::Config("enabled must be a boolean".into()))?;
                    args.settings.set_auto_retry_enabled(enabled)?;
                    control =
                        json!({"type":"set_auto_retry","enabled":enabled,"id":request.get("id")});
                }
                Worker::control(&mut worker, &args, control)
            })();
            if let Err(error) = result {
                response(&request, Err(error))?;
            }
            continue;
        }
        if request["type"] == "set_auto_compaction" {
            let result = (|| {
                let enabled = request["enabled"]
                    .as_bool()
                    .ok_or_else(|| Error::Config("enabled must be a boolean".into()))?;
                args.settings.set_auto_compaction_enabled(enabled)?;
                Worker::control(
                    &mut worker,
                    &args,
                    json!({
                        "type":"set_auto_compaction", "enabled":enabled, "id":request.get("id"),
                    }),
                )
            })();
            if let Err(error) = result {
                response(&request, Err(error))?;
            }
            continue;
        }
        if request["type"] == "compact" {
            let result = (|| {
                if busy(&worker) {
                    return Err(Error::Config(
                        "wait for the current operation before compacting".into(),
                    ));
                }
                let instructions = request
                    .get("customInstructions")
                    .map(|value| {
                        value.as_str().ok_or_else(|| {
                            Error::Config("customInstructions must be a string".into())
                        })
                    })
                    .transpose()?;
                Worker::submit(
                    &mut worker,
                    &args,
                    json!({"type":"compact","summary":null,"keep_turns":1,"use_token_settings":true,"instructions":instructions,"rpc":true,"id":request.get("id")}),
                )
            })();
            match result {
                Ok(()) => pending_compact = Some(request),
                Err(error) => response(&request, Err(error))?,
            }
            continue;
        }
        if request["type"] == "bash" {
            let result = (|| {
                if busy(&worker) {
                    return Err(Error::Config(
                        "wait for the current operation before executing a shell command".into(),
                    ));
                }
                let command = request["command"]
                    .as_str()
                    .ok_or_else(|| Error::Config("command must be a string".into()))?;
                if command.trim().is_empty() || command.len() > 1024 * 1024 {
                    return Err(Error::Config(
                        "shell command must contain 1–1048576 bytes".into(),
                    ));
                }
                Worker::submit(
                    &mut worker,
                    &args,
                    json!({"type":"bash","command":command,"exclude_from_context":request["excludeFromContext"] == true,"request_id":request.get("id")}),
                )
            })();
            match result {
                Ok(()) => pending_bash = Some(request),
                Err(error) => response(&request, Err(error))?,
            }
            continue;
        }
        if (request["type"] == "abort"
            || (request["type"] == "abort_bash" && pending_bash.is_some()))
            && busy(&worker)
        {
            if pending_abort.is_some() {
                response(&request, Err(Error::Config("abort already pending".into())))?;
            } else if let Some(active) = &mut worker {
                match active.send(json!({"type":"cancel"})) {
                    Ok(()) => pending_abort = Some(request),
                    Err(error) => response(&request, Err(error))?,
                }
            }
            continue;
        }
        if request["type"] == "shutdown" {
            drop(worker.take());
            response(&request, Ok(None))?;
            break;
        }
        let result = command(&request, &mut args, &mut worker);
        response(&request, result)?;
    }
    drop(worker);
    Ok(())
}

fn command(request: &Value, args: &mut Cli, worker: &mut Option<Worker>) -> Result<Option<Value>> {
    let name = request["type"].as_str().unwrap_or_default();
    match name {
        "prompt" | "steer" | "follow_up" => {
            let images: Vec<pi_rs::ImageContent> = request
                .get("images")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()?
                .unwrap_or_default();
            for image in &images {
                image.validate()?;
            }
            let message = request["message"].as_str().unwrap_or_default();
            if message.is_empty() && images.is_empty() {
                return Err(Error::Config("message or images are required".into()));
            }
            if let Some(active) = worker.as_mut().filter(|worker| worker.busy) {
                let kind = match name {
                    "steer" => "steer",
                    "follow_up" => "followup",
                    _ => match request["streamingBehavior"].as_str() {
                        Some("steer") => "steer",
                        Some("followUp") => "followup",
                        _ => {
                            return Err(Error::Config(
                                "streamingBehavior is required while running".into(),
                            ));
                        }
                    },
                };
                active.send(json!({"type":kind,"content":message,"images":images}))?;
                Ok(Some(json!({"disposition":"queued"})))
            } else {
                Worker::submit(
                    worker,
                    args,
                    json!({"type":"prompt","content":message,"images":images}),
                )?;
                Ok(Some(json!({"disposition":"started"})))
            }
        }
        "abort" | "abort_bash" => Ok(None),
        "get_state" => {
            let mut state = json!({"sessionFile":args.session,"isStreaming":worker.as_ref().is_some_and(|worker| worker.busy && !worker.bashing && !worker.compacting && !worker.retrying),"isCompacting":worker.as_ref().is_some_and(|worker| worker.busy && worker.compacting),"isBashRunning":worker.as_ref().is_some_and(|worker| worker.busy && worker.bashing),"model":model_value(&args.model),"thinkingLevel":args.thinking.as_deref().unwrap_or("off")});
            state["autoCompactionEnabled"] = json!(args.settings.auto_compaction_enabled());
            state["steeringMode"] = json!(
                worker
                    .as_ref()
                    .map(|worker| worker.steering_mode)
                    .unwrap_or(super::backend::configured_queue_mode(args, "steeringMode")?)
            );
            state["followUpMode"] = json!(
                worker
                    .as_ref()
                    .map(|worker| worker.follow_up_mode)
                    .unwrap_or(super::backend::configured_queue_mode(args, "followUpMode")?)
            );
            if !busy(worker) {
                let session = Session::open(&args.session)?;
                state["steeringMode"] = json!(session.queue_mode(pi_rs::InputKind::Steer));
                state["followUpMode"] = json!(session.queue_mode(pi_rs::InputKind::FollowUp));
                state["sessionId"] = json!(session.session_id());
                state["sessionName"] = json!(session.name());
                state["messageCount"] = json!(session.conversation_at(session.head())?.len());
            }
            Ok(Some(state))
        }
        "get_available_models" => Ok(Some(
            json!({"models":available_models(args)?.iter().map(|model| model_value(model)).collect::<Vec<_>>()}),
        )),
        "cycle_model" if !busy(worker) => {
            let available = available_models(args)?;
            if available.is_empty() {
                return Ok(None);
            }
            let next = available
                .iter()
                .position(|model| *model == args.model)
                .map_or(0, |index| (index + 1) % available.len());
            args.model = available[next].clone();
            args.apply_model_thinking_defaults();
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
            Ok(Some(
                json!({"model":model_value(&args.model),"thinkingLevel":args.thinking.as_deref().unwrap_or("off"),"isScoped":!args.model_scope.is_empty()}),
            ))
        }
        "get_available_thinking_levels" => Ok(Some(json!({"levels":THINKING_LEVELS}))),
        "set_thinking_level" | "cycle_thinking_level" if !busy(worker) => {
            let level = if name == "set_thinking_level" {
                request["level"]
                    .as_str()
                    .filter(|level| THINKING_LEVELS.contains(level))
                    .ok_or_else(|| Error::Config("invalid thinking level".into()))?
            } else {
                let index = THINKING_LEVELS
                    .iter()
                    .position(|level| Some(*level) == args.thinking.as_deref())
                    .unwrap_or(0);
                THINKING_LEVELS[(index + 1) % THINKING_LEVELS.len()]
            };
            args.thinking = Some(level.into());
            Session::open(&args.session)?.select_model(&args.model, args.thinking.as_deref())?;
            Ok((name == "cycle_thinking_level").then(|| json!({"level":level})))
        }
        "get_entries" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            let since = request.get("since").map(record_id).transpose()?;
            if since.is_some_and(|id| !session.records().iter().any(|record| record.id == id)) {
                return Err(Error::Config("unknown since entry".into()));
            }
            let entries: Vec<_> = session
                .records()
                .iter()
                .filter(|record| since.is_none_or(|id| record.id > id))
                .collect();
            Ok(Some(
                json!({"entries":entries,"leafId":session.head().to_string()}),
            ))
        }
        "get_tree" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            Ok(Some(
                json!({"tree":session_tree(&session),"leafId":session.head().to_string()}),
            ))
        }
        "get_fork_messages" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            let messages: Vec<_> = session
                .branch()
                .iter()
                .filter_map(|record| match &record.event {
                    pi_rs::Event::User { content, .. }
                    | pi_rs::Event::UserWithImages { content, .. } => {
                        Some(json!({"entryId":record.id.to_string(),"text":content}))
                    }
                    _ => None,
                })
                .collect();
            Ok(Some(json!({"messages":messages})))
        }
        "get_last_assistant_text" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            let text = session
                .conversation_at(session.head())?
                .into_iter()
                .rev()
                .find_map(|message| match message {
                    pi_rs::Message::Assistant { content, .. } => Some(content),
                    _ => None,
                });
            Ok(Some(json!({"text":text})))
        }
        "switch_session" if !busy(worker) => {
            let path = request["sessionPath"]
                .as_str()
                .ok_or_else(|| Error::Config("sessionPath is required".into()))?;
            let path = args.workspace.join(path);
            switch_session(args, &path)?;
            Ok(Some(json!({"cancelled":false})))
        }
        "fork" | "clone" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            let (target, text) = if name == "fork" {
                let id = record_id(&request["entryId"])?;
                let record = session
                    .records()
                    .iter()
                    .find(|record| record.id == id)
                    .ok_or_else(|| Error::Config("unknown fork entry".into()))?;
                match &record.event {
                    pi_rs::Event::User { content, .. }
                    | pi_rs::Event::UserWithImages { content, .. } => {
                        (record.parent.unwrap_or(1), Some(content.clone()))
                    }
                    _ => return Err(Error::Config("fork must select a user message".into())),
                }
            } else {
                (session.head(), None)
            };
            let destination = if args.no_session {
                args.session
                    .with_file_name(format!("{}.jsonl", uuid::Uuid::new_v4()))
            } else {
                interactive::session_path(&args.workspace, args.session_dir.as_deref())
            };
            sessions::fork(&session, target, &destination, &args.workspace)?;
            drop(session);
            switch_session(args, &destination)?;
            Ok(Some(
                json!({"cancelled":false,"text":text,"sessionFile":args.session}),
            ))
        }
        "get_messages" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            Ok(Some(
                json!({"messages":session.conversation_at(session.head())?}),
            ))
        }
        "new_session" if !busy(worker) => {
            args.session = if args.no_session {
                args.session
                    .with_file_name(format!("{}.jsonl", uuid::Uuid::new_v4()))
            } else {
                interactive::session_path(&args.workspace, args.session_dir.as_deref())
            };
            if let Some(parent) = args.session.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut session = Session::open(&args.session)?;
            session.set_working_directory(&args.workspace)?;
            super::backend::initialize_queue_modes(&mut session, args)?;
            Ok(Some(json!({"cancelled":false})))
        }
        "set_model" if !busy(worker) => {
            if request["provider"]
                .as_str()
                .is_some_and(|provider| provider != "ollama")
            {
                return Err(Error::Config(
                    "this endpoint uses the ollama provider".into(),
                ));
            }
            let model = request["modelId"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Error::Config("modelId is required".into()))?;
            args.model = model.to_owned();
            args.apply_model_thinking_defaults();
            Session::open(&args.session)?.select_model(model, args.thinking.as_deref())?;
            Ok(Some(model_value(model)))
        }
        "edit_context" if !busy(worker) => {
            let target = request["target"]
                .as_u64()
                .ok_or_else(|| Error::Config("target must be a record ID".into()))?;
            let replacement = request
                .get("replacement")
                .ok_or_else(|| Error::Config("replacement is required".into()))?;
            let replacement = if replacement.is_null() {
                None
            } else {
                Some(serde_json::from_value(
                    replacement.get("content").unwrap_or(replacement).clone(),
                )?)
            };
            Session::open(&args.session)?.edit_context(target, replacement)?;
            Ok(None)
        }
        "set_session_name" if !busy(worker) => {
            let name = request["name"]
                .as_str()
                .ok_or_else(|| Error::Config("name is required".into()))?;
            Session::open(&args.session)?.set_name(name)?;
            Ok(None)
        }
        "export_html" if !busy(worker) => {
            let path = request["outputPath"]
                .as_str()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| {
                    args.workspace
                        .join(format!("pi-session-{}.html", uuid::Uuid::new_v4()))
                });
            sessions::export(&Session::open(&args.session)?, &path)?;
            Ok(Some(json!({"path":path})))
        }
        "get_session_stats" if !busy(worker) => {
            let session = Session::open(&args.session)?;
            let mut stats = serde_json::to_value(sessions::statistics(&session))?;
            stats["sessionId"] = json!(session.session_id());
            stats["sessionFile"] = json!(args.session);
            stats["sessionName"] = json!(session.name());
            stats["cost"] = Value::Null;
            if let Some(window) = args.context_window.filter(|window| *window > 0) {
                let estimate = session.context_usage_estimate();
                let after_compaction = session
                    .branch()
                    .iter()
                    .any(|record| matches!(record.event, pi_rs::Event::Compacted { .. }));
                let tokens = if after_compaction && estimate.last_usage_record.is_none() {
                    None
                } else {
                    Some(estimate.tokens)
                };
                stats["contextUsage"] = json!({
                    "tokens":tokens,
                    "contextWindow":window,
                    "percent":tokens.map(|tokens| tokens as f64 / f64::from(window) * 100.0),
                });
            }
            Ok(Some(stats))
        }
        _ => Err(Error::Config(format!(
            "command {name:?} is unavailable in the current state"
        ))),
    }
}

const THINKING_LEVELS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];

fn model_value(model: &str) -> Value {
    json!({"id":model,"name":model,"provider":"ollama"})
}

fn available_models(args: &Cli) -> Result<Vec<String>> {
    if !args.model_scope.is_empty() {
        return Ok(args
            .model_scope
            .iter()
            .map(|choice| choice.model.clone())
            .collect());
    }
    let endpoint = args.endpoint.clone();
    let token = args.model_token();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(pi_rs::ollama::Ollama::new(&endpoint, token)?.models())
    })
    .join()
    .map_err(|_| Error::Config("model discovery worker stopped".into()))?
}

fn record_id(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .filter(|id| *id > 0)
        .ok_or_else(|| Error::Config("entry ID must be a positive integer".into()))
}

fn switch_session(args: &mut Cli, path: &std::path::Path) -> Result<()> {
    if !path.is_file() {
        return Err(Error::Config("session file not found".into()));
    }
    let session = Session::open(path)?;
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "a managed session must be opened by its host".into(),
        ));
    }
    drop(session);
    let mut next = args.clone();
    next.session = path.to_path_buf();
    interactive::restore_workspace(&mut next)?;
    *args = next;
    Ok(())
}

fn session_tree(session: &Session) -> Vec<Value> {
    let labels = session.labels();
    let mut children = std::collections::BTreeMap::<u64, Vec<Value>>::new();
    let mut roots = Vec::new();
    for record in session.records().iter().rev() {
        let mut descendants = children.remove(&record.id).unwrap_or_default();
        descendants.reverse();
        let node = json!({"entry":record,"children":descendants,"label":labels.get(&record.id)});
        match record.parent {
            Some(parent) => children.entry(parent).or_default().push(node),
            None => roots.push(node),
        }
    }
    roots.reverse();
    roots
}

fn busy(worker: &Option<Worker>) -> bool {
    worker.as_ref().is_some_and(|worker| worker.busy)
}

fn forward_event(
    event: Value,
    busy: &mut bool,
    pending_abort: &mut Option<Value>,
    pending_bash: &mut Option<Value>,
    pending_compact: &mut Option<Value>,
) -> Result<()> {
    match event["type"].as_str() {
        Some("response") if event["command"] == "compact" => {
            pending_compact.take();
            *busy = false;
            emit(event)?;
            if let Some(request) = pending_abort.take() {
                response(&request, Ok(None))?;
            }
            Ok(())
        }
        Some("backend_ready" | "bash_execution_start") => Ok(()),
        Some("bash_execution_end") => {
            if let Some(request) = pending_bash.take() {
                let result = if let Some(error) = event["error"].as_str() {
                    Err(Error::Config(error.into()))
                } else {
                    Ok(Some(event["data"].clone()))
                };
                response(&request, result)?;
            }
            Ok(())
        }
        Some("agent_started") => {
            *busy = true;
            emit(json!({"type":"agent_start"}))
        }
        Some("agent_settled") => {
            *busy = pending_compact.is_some();
            emit(json!({"type":"agent_end","success":event["success"]}))?;
            emit(json!({"type":"agent_settled"}))?;
            if let Some(request) = pending_abort.take() {
                response(&request, Ok(None))?;
            }
            Ok(())
        }
        _ => emit_event(event),
    }
}

fn response(request: &Value, result: Result<Option<Value>>) -> Result<()> {
    let mut value = json!({"type":"response","command":request["type"],"success":result.is_ok()});
    if let Some(id) = request.get("id") {
        value["id"] = id.clone();
    }
    match result {
        Ok(Some(data)) => value["data"] = data,
        Err(error) => value["error"] = json!(error.to_string()),
        _ => {}
    }
    emit(value)
}

fn emit_event(event: Value) -> Result<()> {
    if event["type"] == "text_delta" {
        emit(
            json!({"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":event["delta"]}}),
        )
    } else {
        emit(event)
    }
}

fn emit(value: Value) -> Result<()> {
    let mut output = std::io::stdout().lock();
    writeln!(output, "{value}")?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn compaction_stays_busy_until_its_completion_response() {
        let mut busy = true;
        let mut abort = None;
        let mut bash = None;
        let mut compact = Some(json!({"type":"compact","id":"focus"}));
        forward_event(
            json!({"type":"agent_settled","success":true}),
            &mut busy,
            &mut abort,
            &mut bash,
            &mut compact,
        )
        .unwrap();
        assert!(busy);
        assert!(compact.is_some());
        abort = Some(json!({"type":"abort","id":"late-abort"}));
        let response = json!({
            "type": "response", "command": "compact", "id": "focus",
            "success": true, "data": {"summary": "done"},
        });
        forward_event(response, &mut busy, &mut abort, &mut bash, &mut compact).unwrap();
        assert!(!busy);
        assert!(compact.is_none());
        assert!(abort.is_none());
    }

    #[test]
    fn native_session_commands_preserve_sources_and_restore_selection() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.jsonl");
        let mut session = Session::open(&source).unwrap();
        session.select_model("fixture:4b", Some("low")).unwrap();
        session
            .append_user_with_images("first".into(), vec![], None)
            .unwrap();
        let first = session.head();
        session
            .append_user_with_images("second".into(), vec![], None)
            .unwrap();
        let second = session.head();
        drop(session);
        let before = std::fs::read(&source).unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = root.path().to_path_buf();
        args.session = source.clone();
        args.session_dir = Some(root.path().join("sessions"));
        let mut worker = None;
        let entries = command(
            &json!({"type":"get_entries", "since":first.to_string()}),
            &mut args,
            &mut worker,
        )
        .unwrap()
        .unwrap();
        assert_eq!(entries["entries"][0]["id"], second);
        let forks = command(&json!({"type":"get_fork_messages"}), &mut args, &mut worker)
            .unwrap()
            .unwrap();
        assert_eq!(forks["messages"].as_array().unwrap().len(), 2);
        let tree = command(&json!({"type":"get_tree"}), &mut args, &mut worker)
            .unwrap()
            .unwrap();
        assert_eq!(tree["tree"].as_array().unwrap().len(), 1);
        command(
            &json!({"type":"fork","entryId":second.to_string()}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        assert_ne!(args.session, source);
        assert_eq!(args.model, "fixture:4b");
        assert_eq!(args.thinking.as_deref(), Some("low"));
        let fork = Session::open(&args.session).unwrap();
        assert_eq!(fork.conversation_at(fork.head()).unwrap().len(), 1);
        drop(fork);
        assert_eq!(std::fs::read(&source).unwrap(), before);
        command(
            &json!({"type":"switch_session","sessionPath":source}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        assert_eq!(args.session, source);
        assert!(
            command(
                &json!({"type":"get_entries","since":"999999"}),
                &mut args,
                &mut worker
            )
            .is_err()
        );
    }

    #[test]
    fn switching_workspaces_restores_directory_and_rejects_missing_directories_atomically() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("other");
        std::fs::create_dir(&workspace).unwrap();
        let path = root.path().join("other.jsonl");
        let mut session = Session::open(&path).unwrap();
        session.set_working_directory(&workspace).unwrap();
        session.select_model("other-model", None).unwrap();
        drop(session);
        let mut args = Cli::try_parse_from(["pi", "--no-trust"]).unwrap();
        switch_session(&mut args, &path).unwrap();
        assert_eq!(args.workspace, workspace.canonicalize().unwrap());
        assert_eq!(args.model, "other-model");
        assert!(!args.trusted);
        let previous = args.session.clone();
        let invalid = root.path().join("invalid.jsonl");
        let mut session = Session::open(&invalid).unwrap();
        let missing = root.path().join("missing");
        std::fs::create_dir(&missing).unwrap();
        session.set_working_directory(&missing).unwrap();
        std::fs::remove_dir(&missing).unwrap();
        session.select_model("invalid-model", None).unwrap();
        drop(session);
        assert!(switch_session(&mut args, &invalid).is_err());
        assert_eq!(args.workspace, workspace.canonicalize().unwrap());
        assert_eq!(args.session, previous);
        assert_eq!(args.model, "other-model");
    }

    #[test]
    fn thinking_commands_persist_and_reject_invalid_levels() {
        let root = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.session = root.path().join("thinking.jsonl");
        args.model_scope = vec![
            crate::model_selection::Choice {
                model: "a".into(),
                thinking: Some("low".into()),
            },
            crate::model_selection::Choice {
                model: "b".into(),
                thinking: Some("high".into()),
            },
        ];
        args.model = "a".into();
        let mut worker = None;
        command(
            &json!({"type":"set_thinking_level","level":"medium"}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        let result = command(
            &json!({"type":"cycle_thinking_level"}),
            &mut args,
            &mut worker,
        )
        .unwrap()
        .unwrap();
        assert_eq!(result["level"], "high");
        let result = command(&json!({"type":"cycle_model"}), &mut args, &mut worker)
            .unwrap()
            .unwrap();
        assert_eq!(result["model"]["id"], "b");
        assert_eq!(
            Session::open(&args.session).unwrap().model_selection(),
            Some(("b".into(), Some("high".into())))
        );
        assert!(
            command(
                &json!({"type":"set_thinking_level","level":"invalid"}),
                &mut args,
                &mut worker
            )
            .is_err()
        );
    }
}
