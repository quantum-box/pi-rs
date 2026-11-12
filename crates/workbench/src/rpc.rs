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
    if let Some(name) = &args.name {
        session.set_name(name)?;
    }
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
    let mut pending_queries: Vec<Value> = Vec::new();
    let mut pending_inputs: Vec<Value> = Vec::new();
    let mut model_queries: Vec<(Value, mpsc::Receiver<Result<Value>>)> = Vec::new();
    let mut stream = super::rpc_events::Stream::default();
    loop {
        let mut index = 0;
        while index < model_queries.len() {
            match model_queries[index].1.try_recv() {
                Ok(result) => {
                    let (request, _) = model_queries.swap_remove(index);
                    response(&request, result.map(Some))?;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    let (request, _) = model_queries.swap_remove(index);
                    response(
                        &request,
                        Err(Error::Config("model discovery worker stopped".into())),
                    )?;
                }
                Err(mpsc::TryRecvError::Empty) => index += 1,
            }
        }
        if let Some(active) = &mut worker {
            for event in active.drain_events() {
                acknowledge(&mut pending_queries, &event);
                acknowledge(&mut pending_inputs, &event);
                match event["type"].as_str() {
                    Some("auto_retry_start") => active.retrying = true,
                    Some("auto_retry_end" | "agent_settled" | "request") => active.retrying = false,
                    Some("bash_execution_end") => active.bashing = false,
                    _ => {}
                }
                forward_event(
                    event,
                    &mut active.busy,
                    &mut pending_abort,
                    &mut pending_bash,
                    &mut pending_compact,
                    &mut stream,
                    &args,
                )?;
            }
            if let Some(status) = active.child.try_wait()? {
                let mut finished = worker.take().expect("active worker");
                for reader in finished.readers.drain(..) {
                    let _ = reader.join();
                }
                for event in finished.drain_events() {
                    acknowledge(&mut pending_queries, &event);
                    acknowledge(&mut pending_inputs, &event);
                    forward_event(
                        event,
                        &mut finished.busy,
                        &mut pending_abort,
                        &mut pending_bash,
                        &mut pending_compact,
                        &mut stream,
                        &args,
                    )?;
                }
                for request in pending_queries.drain(..) {
                    response(
                        &request,
                        Err(Error::Config(
                            "session query worker exited before returning a result".into(),
                        )),
                    )?;
                }
                for request in pending_inputs.drain(..) {
                    response(
                        &request,
                        Err(Error::Config(
                            "input worker exited before acknowledging the request".into(),
                        )),
                    )?;
                }
                if finished.busy {
                    for event in stream.settle(false) {
                        emit(event)?;
                    }
                    emit(
                        json!({"type":"agent_end","success":status.success(),"messages":stream.messages,"willRetry":false}),
                    )?;
                    emit(json!({"type":"agent_settled","aborted":stream.aborted()}))?;
                    stream.reset();
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
        if !pending_queries.is_empty() {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        let line = match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(line) => line?,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) if model_queries.is_empty() => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Do not drop accepted model-list requests when a batch client
                // closes stdin before discovery finishes.
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }
        };
        let request = match parse_request(&line) {
            Ok(request) => request,
            Err(error) => {
                response(&json!({"type":"parse"}), Err(error))?;
                continue;
            }
        };
        if request["type"] == "get_available_models" {
            if model_queries.len() >= 16 {
                response(
                    &request,
                    Err(Error::Config("too many pending model queries".into())),
                )?;
            } else {
                let args = args.clone();
                let (sender, receiver) = mpsc::channel();
                std::thread::spawn(move || {
                    let result =
                        available_models(&args, true).map(|(_, models)| json!({"models":models}));
                    let _ = sender.send(result);
                });
                model_queries.push((request, receiver));
            }
            continue;
        }
        if matches!(
            request["type"].as_str(),
            Some("prompt" | "steer" | "follow_up")
        ) {
            match submit_prompt(&request, &args, &mut worker) {
                Ok(()) => pending_inputs.push(request),
                Err(error) => response(&request, Err(error))?,
            }
            continue;
        }
        if request["type"] == "set_session_name" && busy(&worker) {
            let result = session_name(&request).and_then(|name| {
                Worker::control(
                    &mut worker,
                    &args,
                    json!({
                        "type":"set_session_name","name":name,"id":request.get("id")
                    }),
                )
            });
            match result {
                Ok(()) => pending_inputs.push(request),
                Err(error) => response(&request, Err(error))?,
            }
            continue;
        }
        if !pending_inputs.is_empty() && requires_idle(request["type"].as_str().unwrap_or_default())
        {
            response(
                &request,
                Err(Error::Config(
                    "wait for queued input to be acknowledged".into(),
                )),
            )?;
            continue;
        }
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
                Ok(()) => pending_queries.push(request),
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
                let command = request["command"]
                    .as_str()
                    .ok_or_else(|| Error::Config("command must be a string".into()))?;
                Worker::start_bash(
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
        if request["type"] == "abort_bash" {
            let result = worker
                .as_mut()
                .filter(|worker| worker.bashing)
                .map_or(Ok(()), |active| active.send(json!({"type":"abort_bash"})));
            response(&request, result.map(|()| None))?;
            continue;
        }
        if request["type"] == "abort" && worker.as_ref().is_some_and(|worker| worker.busy) {
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
        if busy(&worker) && snapshot_command(request["type"].as_str().unwrap_or_default()) {
            let state = state_flags(&args, &worker)?;
            let control = json!({"type":"read_session","request":request,"path":args.session,"context_window":args.effective_context_window(),"state":state});
            match Worker::control(&mut worker, &args, control) {
                Ok(()) => pending_queries.push(request),
                Err(error) => response(&request, Err(error))?,
            }
            continue;
        }
        let previous_thinking = args.thinking.clone();
        let previous_records = if !busy(&worker)
            && matches!(
                request["type"].as_str(),
                Some(
                    "set_model"
                        | "cycle_model"
                        | "set_thinking_level"
                        | "cycle_thinking_level"
                        | "set_session_name"
                        | "edit_context"
                )
            ) {
            Session::snapshot(&args.session)
                .ok()
                .map(|session| session.records().len())
        } else {
            None
        };
        let result = command(&request, &mut args, &mut worker).and_then(|data| {
            if data.as_ref().is_some_and(|data| data["cancelled"] == true) {
                return Ok(data);
            }
            let reason = match request["type"].as_str() {
                Some("new_session") => pi_rs::SessionStartReason::New,
                Some("switch_session") => pi_rs::SessionStartReason::Resume,
                Some("fork" | "clone") => pi_rs::SessionStartReason::Fork,
                _ => return Ok(data),
            };
            Worker::transition(&mut worker, &args, reason)?;
            Ok(data)
        });
        if result.is_ok()
            && let Some(previous) = previous_records
            && let Ok(session) = Session::snapshot(&args.session)
        {
            for entry in super::rpc_entries::changes(&session, previous) {
                emit(json!({"type":"entry_appended","entry":entry}))?;
            }
        }
        if result.is_ok()
            && previous_thinking.as_deref().unwrap_or("off")
                != args.thinking.as_deref().unwrap_or("off")
        {
            emit(
                json!({"type":"thinking_level_changed","level":args.thinking.as_deref().unwrap_or("off")}),
            )?;
        }
        for event in std::mem::take(&mut args.selection_events) {
            if let Err(error) = Worker::notify_selection(&mut worker, &args, &event) {
                eprintln!("Extension selection notification: {error}");
            }
        }
        response(&request, result)?;
    }
    drop(worker);
    Ok(())
}

fn acknowledge(pending: &mut Vec<Value>, event: &Value) {
    if event["type"] == "response"
        && let Some(index) = pending
            .iter()
            .position(|request| request["id"] == event["id"] && request["type"] == event["command"])
    {
        pending.remove(index);
    }
}

fn submit_prompt(request: &Value, args: &Cli, worker: &mut Option<Worker>) -> Result<()> {
    use pi_rs::{InputKind, QueuedInput, QueuedMessage};
    let name = request["type"].as_str().expect("prompt command");
    let images: Vec<pi_rs::ImageContent> = request
        .get("images")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?
        .unwrap_or_default();
    for image in &images {
        image.validate()?;
    }
    let message = request["message"].as_str().unwrap_or_default();
    if message.trim().is_empty() && images.is_empty() {
        return Err(Error::Config("message or images are required".into()));
    }
    let queue_kind = match name {
        "steer" => Some(InputKind::Steer),
        "follow_up" => Some(InputKind::FollowUp),
        _ if worker.as_ref().is_some_and(|worker| worker.busy) => {
            match request["streamingBehavior"].as_str() {
                Some("steer") => Some(InputKind::Steer),
                Some("followUp") => Some(InputKind::FollowUp),
                _ => {
                    return Err(Error::Config(
                        "streamingBehavior is required while running".into(),
                    ));
                }
            }
        }
        _ => None,
    };
    if let Some(kind) = queue_kind {
        let queued = QueuedMessage {
            input: QueuedInput {
                id: uuid::Uuid::new_v4(),
                kind,
                content: message.into(),
            },
            images,
        };
        Worker::control(
            worker,
            args,
            json!({
                "type":"queue_input","message":queued,"command":name,"id":request.get("id")
            }),
        )
    } else {
        Worker::submit(
            worker,
            args,
            json!({"type":"prompt","content":message,"images":images,"rpc":true,"id":request.get("id")}),
        )
    }
}

fn parse_request(line: &str) -> Result<Value> {
    match serde_json::from_str(line) {
        Ok(Value::Object(request)) => Ok(Value::Object(request)),
        Ok(_) => Err(Error::Config(
            "Failed to parse command: expected a JSON object".into(),
        )),
        Err(error) => Err(Error::Config(format!("Failed to parse command: {error}"))),
    }
}

fn requires_idle(name: &str) -> bool {
    matches!(
        name,
        "new_session"
            | "switch_session"
            | "fork"
            | "clone"
            | "set_model"
            | "cycle_model"
            | "set_thinking_level"
            | "cycle_thinking_level"
            | "set_session_name"
            | "edit_context"
    )
}

fn command(request: &Value, args: &mut Cli, worker: &mut Option<Worker>) -> Result<Option<Value>> {
    let name = request["type"].as_str().unwrap_or_default();
    if requires_idle(name) && busy(worker) {
        return Err(Error::Config(format!(
            "command {name:?} is unavailable in the current state"
        )));
    }
    match name {
        "abort" | "abort_bash" => Ok(None),
        name if snapshot_command(name) => {
            let session = Session::snapshot(&args.session)?;
            Ok(Some(snapshot_response(
                request,
                &session,
                &args.session,
                args.effective_context_window(),
                state_flags(args, worker)?,
            )?))
        }

        "get_available_models" => {
            let (_, models) = available_models(args, true)?;
            Ok(Some(json!({"models":models})))
        }
        "cycle_model" => {
            let (available, _) = available_models(args, false)?;
            let Some(next) = super::model_selection::next_model(args, &available, false) else {
                return Ok(Some(Value::Null));
            };
            args.change_model(
                next.provider,
                next.model,
                next.thinking,
                pi_rs::ModelSelectSource::Cycle,
            )?;
            Ok(Some(
                json!({"model":model_value(args, &args.provider, &args.model),"thinkingLevel":args.thinking.as_deref().unwrap_or("off"),"isScoped":!args.model_scope.is_empty()}),
            ))
        }
        "get_available_thinking_levels" => {
            Ok(Some(json!({"levels":super::thinking::available(args)})))
        }
        "set_thinking_level" => {
            let level = request["level"]
                .as_str()
                .ok_or_else(|| Error::Config("invalid thinking level".into()))?;
            super::thinking::set(args, level)?;
            Ok(None)
        }
        "cycle_thinking_level" => Ok(Some(
            super::thinking::cycle(args)?.map_or(Value::Null, |level| json!({"level":level})),
        )),

        "switch_session" => {
            let path = request["sessionPath"]
                .as_str()
                .ok_or_else(|| Error::Config("sessionPath is required".into()))?;
            let path = args.workspace.join(path);
            if Worker::check_session_transition(
                Some(worker),
                args,
                pi_rs::BeforeSessionTransition::Switch {
                    reason: pi_rs::SessionSwitchReason::Resume,
                    target_session_file: Some(path.clone()),
                },
            )? {
                return Ok(Some(json!({"cancelled":true})));
            }
            switch_session(args, &path)?;
            Ok(Some(json!({"cancelled":false})))
        }
        "fork" | "clone" => {
            let session = Session::open(&args.session)?;
            let (entry_id, position, target, text) = if name == "fork" {
                let invalid = || Error::Config("Invalid entry ID for forking".into());
                let entry = record_id(&request["entryId"]).map_err(|_| invalid())?;
                if !super::rpc_entries::Entries::new(&session).is_user_message(entry) {
                    return Err(invalid());
                }
                let id = super::rpc_entries::editable_record(&session, entry);
                let record = session
                    .records()
                    .iter()
                    .find(|record| record.id == id)
                    .ok_or_else(invalid)?;
                match &record.event {
                    pi_rs::Event::User { content, .. }
                    | pi_rs::Event::UserWithImages { content, .. } => {
                        let source = &session.records()[entry as usize - 1];
                        (
                            entry,
                            pi_rs::ForkPosition::Before,
                            source.parent.unwrap_or(1),
                            Some(content.clone()),
                        )
                    }
                    _ => return Err(invalid()),
                }
            } else {
                if !super::rpc_entries::Entries::new(&session).has_leaf() {
                    return Err(Error::Config(
                        "Cannot clone session: no current entry selected".into(),
                    ));
                }
                (
                    session.head(),
                    pi_rs::ForkPosition::At,
                    session.head(),
                    None,
                )
            };
            if Worker::check_session_transition(
                Some(worker),
                args,
                pi_rs::BeforeSessionTransition::Fork { entry_id, position },
            )? {
                return Ok(Some(json!({"cancelled":true})));
            }
            let destination = if args.no_session {
                args.session
                    .with_file_name(format!("{}.jsonl", uuid::Uuid::new_v4()))
            } else {
                interactive::session_path(&args.workspace, args.session_dir.as_deref())
            };
            sessions::fork(
                &session,
                &args.session,
                target,
                &destination,
                &args.workspace,
            )?;
            drop(session);
            switch_session(args, &destination)?;
            let mut data = json!({"cancelled":false});
            if let Some(text) = text {
                data["text"] = json!(text);
            }
            if request["includeNative"] == true {
                data["sessionFile"] = json!(args.session);
            }
            Ok(Some(data))
        }

        "new_session" => {
            let parent = match request.get("parentSession") {
                None | Some(Value::Null) => None,
                Some(Value::String(path)) => Some(path.as_str()).filter(|path| !path.is_empty()),
                Some(_) => return Err(Error::Config("parentSession must be a string".into())),
            };
            if Worker::check_session_transition(
                Some(worker),
                args,
                pi_rs::BeforeSessionTransition::Switch {
                    reason: pi_rs::SessionSwitchReason::New,
                    target_session_file: None,
                },
            )? {
                return Ok(Some(json!({"cancelled":true})));
            }
            let destination = if args.no_session {
                args.session
                    .with_file_name(format!("{}.jsonl", uuid::Uuid::new_v4()))
            } else {
                interactive::session_path(&args.workspace, args.session_dir.as_deref())
            };
            if let Some(directory) = destination.parent() {
                std::fs::create_dir_all(directory)?;
            }
            let mut next = args.clone();
            next.refresh_workspace_selection();
            next.session = destination;
            let mut session = Session::open(&next.session)?;
            session.set_working_directory(&next.workspace)?;
            super::backend::initialize_queue_modes(&mut session, &next)?;
            if let Some(parent) = parent {
                session.set_parent_session(parent)?;
            }
            next.restore_session_model(&session);
            super::thinking::adjust(&mut next);
            next.record_automatic_model_selection(&mut session)?;
            drop(session);
            *args = next;
            Ok(Some(json!({"cancelled":false})))
        }
        "set_model" => {
            let provider_id = request.get("provider").map_or(args.provider.id(), |value| {
                value.as_str().unwrap_or("undefined")
            });
            let model = request["modelId"].as_str().unwrap_or("undefined");
            let not_found = || Error::Config(format!("Model not found: {provider_id}/{model}"));
            let provider = args
                .model_profiles
                .provider(provider_id)
                .filter(|provider| provider.id() == provider_id)
                .ok_or_else(not_found)?;
            if request["modelId"]
                .as_str()
                .is_none_or(|model| model.is_empty())
            {
                return Err(not_found());
            }
            let value = if args
                .model_profiles
                .model_ids(&provider)
                .iter()
                .any(|id| id == model)
            {
                model_value(args, &provider, model)
            } else {
                let current = args.clone();
                let selected = provider.clone();
                let candidate = model.to_owned();
                discover_models(move || async move {
                    let probe = current.allow_endpoint_probes(current.endpoint_for(&selected));
                    if !probe {
                        return Ok(None);
                    }
                    let catalog = super::model_selection::catalog(&current, selected, true).await?;
                    if !catalog.models.contains(&candidate) {
                        return Ok(None);
                    }
                    let details = catalog
                        .details(std::slice::from_ref(&candidate), &current.model_profiles)
                        .await;
                    Ok(Some(super::model_listing::with_details(
                        model_value(&current, &catalog.provider, &candidate),
                        details.get(&candidate),
                    )))
                })?
                .ok_or_else(not_found)?
            };
            args.change_model(
                provider,
                model.to_owned(),
                None,
                pi_rs::ModelSelectSource::Set,
            )?;
            Ok(Some(value))
        }
        "edit_context" => {
            let session = &mut Session::open(&args.session)?;
            let target = super::rpc_entries::editable_record(
                session,
                record_id(request.get("targetId").unwrap_or(&request["target"]))?,
            );
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
            session.edit_context(target, replacement)?;
            Ok(None)
        }
        "set_session_name" => {
            let name = session_name(request)?;
            let mut session = Session::open(&args.session)?;
            session.set_name(&name)?;
            drop(session);
            emit(json!({"type":"session_info_changed","name":name}))?;
            Worker::notify_session_name(worker, args, Some(name))?;
            Ok(None)
        }
        "export_html" => {
            if args.no_session {
                return Err(Error::Config(
                    "Cannot export in-memory session to HTML".into(),
                ));
            }
            let session = Session::snapshot(&args.session)?;
            if !session.records().iter().any(|record| {
                matches!(
                    record.event,
                    pi_rs::Event::User { .. }
                        | pi_rs::Event::UserWithImages { .. }
                        | pi_rs::Event::Assistant { .. }
                )
            }) {
                return Err(Error::Config(
                    "Nothing to export yet - start a conversation first".into(),
                ));
            }
            let path = request["outputPath"]
                .as_str()
                .filter(|path| !path.is_empty())
                .map(pi_rs_workbench::normalize_path)
                .transpose()?
                .unwrap_or_else(|| sessions::default_html_destination(&args.session));
            sessions::export_html(&session, &args.session, &path)?;
            Ok(Some(json!({"path":path})))
        }

        _ => {
            let command = match request.get("type") {
                Some(Value::String(name)) => name.clone(),
                Some(value) => value.to_string(),
                None => "undefined".into(),
            };
            Err(Error::Config(format!("Unknown command: {command}")))
        }
    }
}

pub(super) fn snapshot_command(name: &str) -> bool {
    matches!(
        name,
        "get_state"
            | "get_entries"
            | "get_tree"
            | "get_fork_messages"
            | "get_last_assistant_text"
            | "get_messages"
            | "get_session_stats"
    )
}

fn state_flags(args: &Cli, worker: &Option<Worker>) -> Result<Value> {
    let mut state = json!({"isStreaming":worker.as_ref().is_some_and(|worker| worker.busy && !worker.compacting && !worker.retrying),"isCompacting":worker.as_ref().is_some_and(|worker| worker.busy && worker.compacting),"isBashRunning":worker.as_ref().is_some_and(|worker| worker.bashing),"model":model_value(args, &args.provider, &args.model),"thinkingLevel":args.thinking.as_deref().unwrap_or("off")});
    if !args.no_session {
        state["sessionFile"] = json!(args.session);
    }
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
    Ok(state)
}

pub(super) fn snapshot_response(
    request: &Value,
    session: &Session,
    path: &std::path::Path,
    context_window: Option<u32>,
    mut state: Value,
) -> Result<Value> {
    let pending_request = || {
        (state["isStreaming"] == true)
            .then(|| super::rpc_messages::pending_request(session))
            .flatten()
    };
    match request["type"].as_str().unwrap_or_default() {
        "get_state" => {
            let pending = pending_request();
            state["steeringMode"] = json!(session.queue_mode(pi_rs::InputKind::Steer));
            state["followUpMode"] = json!(session.queue_mode(pi_rs::InputKind::FollowUp));
            state["sessionId"] = json!(session.session_id());
            if let Some(name) = session.name() {
                state["sessionName"] = json!(name);
            }
            if request["includeNative"] == true && state.get("sessionFile").is_none() {
                state["sessionFile"] = json!(path);
            }
            state["messageCount"] = json!(super::rpc_messages::count(session));
            state["pendingMessageCount"] = json!(session.queued_inputs().len());
            if let Some(pending) = pending {
                state["_streamRequestId"] = json!(pending);
            }
            Ok(state)
        }
        "get_entries" => {
            let mut result =
                super::rpc_entries::Entries::new(session).after(request.get("since"))?;
            if request["includeNative"] == true {
                result["nativeEntries"] = json!(session.records());
                result["nativeLeafId"] = json!(session.head().to_string());
            }
            Ok(result)
        }
        "get_tree" => {
            let mut result = super::rpc_entries::Entries::new(session).tree();
            if request["includeNative"] == true {
                result["nativeTree"] = json!(session_tree(session));
                result["nativeLeafId"] = json!(session.head().to_string());
            }
            Ok(result)
        }
        "get_fork_messages" => Ok(super::rpc_entries::Entries::new(session).fork_messages()),
        "get_last_assistant_text" => Ok(super::rpc_messages::last_assistant_text(session)
            .map_or_else(|| json!({}), |text| json!({"text":text}))),
        "get_messages" => {
            let (messages, native) = super::rpc_messages::snapshot(session);
            let mut data = json!({"messages":messages});
            if request["includeNative"] == true {
                data["nativeMessages"] = json!(native);
            }
            if let Some(pending) = pending_request() {
                data["_streamRequestId"] = json!(pending);
            }
            Ok(data)
        }
        "get_session_stats" => {
            let mut native = session.statistics();
            super::rpc_entries::Entries::new(session).apply_message_counts(&mut native);
            let operations = super::sessions::model_operation_usage(session.records());
            native.tokens.input = native.tokens.input.saturating_add(operations.input);
            native.tokens.output = native.tokens.output.saturating_add(operations.output);
            native.tokens.cache_read = native
                .tokens
                .cache_read
                .saturating_add(operations.cache_read);
            native.tokens.cache_write = native
                .tokens
                .cache_write
                .saturating_add(operations.cache_write);
            native.tokens.total = native.tokens.total.saturating_add(operations.total_tokens);
            native.cost = native.cost.map(|cost| cost + operations.cost.total);
            let mut stats = serde_json::to_value(native)?;
            if operations.measured() {
                stats["operationUsage"] = serde_json::to_value(operations)?;
            }
            if state.get("sessionFile").is_some() || request["includeNative"] == true {
                stats["sessionFile"] = json!(path);
            }
            if let Some(usage) = context_window.and_then(|window| session.context_usage(window)) {
                stats["contextUsage"] = serde_json::to_value(usage)?;
            }
            Ok(stats)
        }
        _ => Err(Error::Config("unsupported snapshot command".into())),
    }
}

pub(super) fn model_value(args: &Cli, provider: &super::Provider, model: &str) -> Value {
    let configured = args.model_profiles.model(provider, model);
    let name = configured
        .as_ref()
        .and_then(|metadata| metadata.name.as_deref())
        .unwrap_or(model);
    let adapter = args.model_profiles.model_adapter(provider, model);
    let mut value = json!({"id":model,"name":name,"provider":provider.id(),"api":adapter.api()});
    if let Some(configured) = configured {
        configured.apply_catalog_metadata(&mut value);
        let mut details = pi_rs::ollama::ModelDetails::default();
        configured.apply(adapter, &mut details);
        super::model_listing::with_details(value, Some(&details))
    } else {
        value
    }
}

fn available_models(
    args: &Cli,
    include_details: bool,
) -> Result<(Vec<super::model_selection::ScopedChoice>, Vec<Value>)> {
    use super::model_selection::ScopedChoice;
    if !include_details && !args.model_scope.is_empty() {
        return Ok((args.model_scope.clone(), Vec::new()));
    }
    let args = args.clone();
    discover_models(move || async move {
        let mut catalogs = super::model_selection::catalogs(&args, false).await?;
        if !include_details {
            catalogs.sort_by_key(|catalog| {
                args.model_profiles
                    .providers()
                    .position(|provider| provider == catalog.provider)
                    .unwrap_or(usize::MAX)
            });
        }
        let mut choices = Vec::new();
        let mut rows = Vec::new();
        for catalog in catalogs {
            let details = if include_details {
                catalog.details(&catalog.models, &args.model_profiles).await
            } else {
                Default::default()
            };
            for model in catalog.models {
                if include_details {
                    rows.push(super::model_listing::with_details(
                        model_value(&args, &catalog.provider, &model),
                        details.get(&model),
                    ));
                }
                choices.push(ScopedChoice {
                    provider: catalog.provider.clone(),
                    model,
                    thinking: None,
                });
            }
        }
        Ok((choices, rows))
    })
}

fn discover_models<T: Send + 'static, F: std::future::Future<Output = Result<T>>>(
    query: impl FnOnce() -> F + Send + 'static,
) -> Result<T> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(query())
    })
    .join()
    .map_err(|_| Error::Config("model discovery worker stopped".into()))?
}

fn session_name(request: &Value) -> Result<String> {
    let name = request["name"]
        .as_str()
        .ok_or_else(|| Error::Config("name is required".into()))?;
    sessions::validate_name(name).map_err(Error::Config)
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
    let label_timestamps = session.label_timestamps();
    let mut children = std::collections::BTreeMap::<u64, Vec<Value>>::new();
    let mut roots = Vec::new();
    for record in session.records().iter().rev() {
        let mut descendants = children.remove(&record.id).unwrap_or_default();
        descendants.reverse();
        let mut node =
            json!({"entry":record,"children":descendants,"label":labels.get(&record.id)});
        if let Some(timestamp) = label_timestamps.get(&record.id) {
            node["labelTimestamp"] = json!(timestamp);
        }
        match record.parent {
            Some(parent) => children.entry(parent).or_default().push(node),
            None => roots.push(node),
        }
    }
    roots.reverse();
    roots
}

fn busy(worker: &Option<Worker>) -> bool {
    worker
        .as_ref()
        .is_some_and(|worker| worker.busy || worker.bashing)
}

fn forward_event(
    event: Value,
    busy: &mut bool,
    pending_abort: &mut Option<Value>,
    pending_bash: &mut Option<Value>,
    pending_compact: &mut Option<Value>,
    stream: &mut super::rpc_events::Stream,
    args: &Cli,
) -> Result<()> {
    match event["type"].as_str() {
        Some("response")
            if matches!(
                event["command"].as_str(),
                Some("get_messages" | "get_state")
            ) =>
        {
            let mut event = event;
            let pending = event["data"]
                .as_object_mut()
                .and_then(|data| data.remove("_streamRequestId"));
            if let Some(partial) = pending.as_ref().and_then(|id| stream.partial(id)) {
                if event["command"] == "get_messages" {
                    if let Some(messages) = event["data"]["messages"].as_array_mut() {
                        messages.push(partial.clone());
                    }
                } else if let Some(count) = event["data"]["messageCount"].as_u64() {
                    event["data"]["messageCount"] = json!(count.saturating_add(1));
                }
            }
            emit(event)
        }
        Some("response") if event["command"] == "compact" => {
            pending_compact.take();
            *busy = false;
            let mut completed = json!({
                "type":"compaction_end","reason":"manual",
                "aborted":stream.aborted(),"willRetry":false,
            });
            if event["success"] == true {
                completed["result"] = event["data"].clone();
            } else {
                completed["errorMessage"] = event["error"].clone();
            }
            emit(completed)?;
            emit(event)?;
            if let Some(request) = pending_abort.take() {
                response(&request, Ok(None))?;
            }
            Ok(())
        }
        Some("backend_ready" | "bash_execution_start" | "resources_discovered") => Ok(()),
        Some("bash_execution_update") => {
            let mut event = event;
            event["delta"] = event["output"].clone();
            emit(event)
        }
        Some("bash_execution_end") => {
            if let Some(request) = pending_bash.take() {
                let result = if let Some(error) = event["error"].as_str() {
                    Err(Error::Config(error.into()))
                } else {
                    let execution = &event["data"];
                    let mut data = json!({
                        "output": execution["output"],
                        "cancelled": execution["cancelled"],
                        "truncated": execution["truncated"]
                    });
                    if execution["cancelled"] != true
                        && let Some(code) =
                            execution.get("exitCode").filter(|value| !value.is_null())
                    {
                        data["exitCode"] = code.clone();
                    }
                    if let Some(path) = execution
                        .get("fullOutputPath")
                        .filter(|value| !value.is_null())
                    {
                        data["fullOutputPath"] = path.clone();
                    }
                    Ok(Some(data))
                };
                response(&request, result)?;
            }
            Ok(())
        }
        Some("agent_started") => {
            *busy = true;
            stream.reset();
            if pending_compact.is_some() {
                emit(json!({"type":"compaction_start","reason":"manual"}))?;
            }
            emit(json!({"type":"agent_start"}))
        }
        Some("agent_settled") => {
            *busy = pending_compact.is_some();
            if event["started"] != false {
                for mapped in stream.settle(event["aborted"] == true) {
                    emit(mapped)?;
                }
                emit(
                    json!({"type":"agent_end","success":event["success"],"messages":stream.messages,"willRetry":false}),
                )?;
            }
            emit(
                json!({"type":"agent_settled","aborted":event["aborted"] == true || (event["started"] != false && stream.aborted())}),
            )?;
            // Manual compaction can emit agent_settled before its command
            // response. An abort must wait until the whole operation is idle.
            if pending_compact.is_none()
                && let Some(request) = pending_abort.take()
            {
                response(&request, Ok(None))?;
            }
            Ok(())
        }
        _ => {
            for mapped in stream.observe(&event, args) {
                emit(mapped)?;
            }
            if matches!(
                event["type"].as_str(),
                Some("text_delta" | "tool_call_delta" | "summary_request_start")
            ) {
                return Ok(());
            }
            emit(event)
        }
    }
}

fn response(request: &Value, result: Result<Option<Value>>) -> Result<()> {
    let mut value = json!({"type":"response","success":result.is_ok()});
    if let Some(command) = request.get("type") {
        value["command"] = command.clone();
    }
    if let Some(id) = request.get("id") {
        value["id"] = id.clone();
    }
    match result {
        Ok(Some(data)) => value["data"] = data,
        Err(Error::Config(message)) => value["error"] = json!(message),
        Err(error) => value["error"] = json!(error.to_string()),
        _ => {}
    }
    emit(value)
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
    fn rpc_html_export_uses_the_requested_format_and_protects_the_session() {
        let directory = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.session = directory.path().join("source.jsonl");
        let mut session = Session::open(&args.session).unwrap();
        let output = directory.path().join("transcript.jsonl");
        let request = json!({"type":"export_html","outputPath":output});
        assert!(command(&request, &mut args, &mut None).is_err());
        session
            .append_user_with_images("fixture <text>".into(), vec![], None)
            .unwrap();
        drop(session);
        std::fs::write(&output, "old").unwrap();
        let url = reqwest::Url::from_file_path(&output).unwrap();
        let request = json!({"type":"export_html","outputPath":url.as_str()});
        let result = command(&request, &mut args, &mut None).unwrap();
        assert_eq!(result, Some(json!({"path":output})));
        let exported = std::fs::read_to_string(&output).unwrap();
        assert!(exported.starts_with("<!doctype html>"));
        assert!(exported.contains("fixture &lt;text&gt;"));
        let original = std::fs::read(&args.session).unwrap();
        let request = json!({"type":"export_html","outputPath":args.session});
        assert!(command(&request, &mut args, &mut None).is_err());
        assert_eq!(std::fs::read(&args.session).unwrap(), original);
        args.no_session = true;
        let error = command(&request, &mut args, &mut None).unwrap_err();
        assert!(matches!(error, Error::Config(message)
            if message == "Cannot export in-memory session to HTML"));
    }

    #[test]
    fn last_assistant_text_matches_pi_transcript_rules() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("upstream.jsonl");
        let answer = json!({"type":"message","id":"a","parentId":null,"message":{
            "role":"assistant","stopReason":"stop","content":[
                {"type":"text","text":" \u{feff}Hello"},
                {"type":"thinking","thinking":"private"},
                {"type":"text","text":" world\n"}
            ]
        }});
        let aborted = json!({"type":"message","id":"b","parentId":"a","message":{
            "role":"assistant","stopReason":"aborted","content":[]
        }});
        let thinking_only = json!({"type":"message","id":"c","parentId":"b","message":{
            "role":"assistant","stopReason":"aborted",
            "content":[{"type":"thinking","thinking":"partial reasoning"}]
        }});
        let entries = [
            json!({"type":"session","version":3}),
            answer,
            aborted,
            thinking_only,
        ];
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        for (length, expected) in [
            (1, json!({})),
            (2, json!({"text":"Hello world"})),
            (3, json!({"text":"Hello world"})),
            (4, json!({})),
        ] {
            let body = entries[..length]
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            std::fs::write(&source, body).unwrap();
            args.session = root.path().join(format!("native-{length}.jsonl"));
            super::super::sessions::import(&source, &args.session).unwrap();
            assert_eq!(
                command(
                    &json!({"type":"get_last_assistant_text"}),
                    &mut args,
                    &mut None
                )
                .unwrap(),
                Some(expected)
            );
        }
    }

    #[test]
    fn compaction_stays_busy_until_its_completion_response() {
        let mut busy = true;
        let mut abort = Some(json!({"type":"abort","id":"during-compaction"}));
        let mut bash = None;
        let mut compact = Some(json!({"type":"compact","id":"focus"}));
        let mut stream = super::super::rpc_events::Stream::default();
        let args = Cli::try_parse_from(["pi"]).unwrap();
        forward_event(
            json!({"type":"agent_settled","success":true}),
            &mut busy,
            &mut abort,
            &mut bash,
            &mut compact,
            &mut stream,
            &args,
        )
        .unwrap();
        assert!(busy);
        assert!(compact.is_some());
        assert_eq!(abort.as_ref().unwrap()["id"], "during-compaction");
        let response = json!({
            "type": "response", "command": "compact", "id": "focus",
            "success": true, "data": {"summary": "done"},
        });
        forward_event(
            response,
            &mut busy,
            &mut abort,
            &mut bash,
            &mut compact,
            &mut stream,
            &args,
        )
        .unwrap();
        assert!(!busy);
        assert!(compact.is_none());
        assert!(abort.is_none());
    }

    #[test]
    fn invalid_names_leave_the_existing_session_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("session.jsonl");
        let mut session = Session::open(&path).unwrap();
        session.set_name("original").unwrap();
        drop(session);
        let before = std::fs::read(&path).unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.session = path.clone();
        let mut worker = None;
        for name in [
            json!(""),
            json!(" \t\n"),
            json!("\u{3000}"),
            json!(null),
            json!(42),
        ] {
            assert!(
                command(
                    &json!({"type":"set_session_name", "name":name}),
                    &mut args,
                    &mut worker
                )
                .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
        command(
            &json!({"type":"set_session_name", "name":"  設計  "}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        assert_eq!(Session::snapshot(&path).unwrap().name(), Some("設計"));
        let before_repeat = Session::snapshot(&path).unwrap().records().len();
        command(
            &json!({"type":"set_session_name", "name":"設計"}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        let snapshot = Session::snapshot(&path).unwrap();
        assert_eq!(snapshot.records().len(), before_repeat + 1);
        assert_eq!(snapshot.name(), Some("設計"));
    }

    #[test]
    fn new_session_retains_parent_and_rejects_invalid_parent_before_switching() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.jsonl");
        drop(Session::open(&source).unwrap());
        let original = std::fs::read(&source).unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = root.path().to_path_buf();
        args.session = source.clone();
        args.session_dir = Some(root.path().join("sessions"));
        let mut worker = None;
        assert!(
            command(
                &json!({"type":"new_session","parentSession":42}),
                &mut args,
                &mut worker
            )
            .is_err()
        );
        assert_eq!(args.session, source);
        let parent = source.to_string_lossy().into_owned();
        command(
            &json!({"type":"new_session","parentSession":parent}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        let child = Session::snapshot(&args.session).unwrap();
        assert_eq!(child.parent_session(), Some(parent.as_str()));
        assert!(child.current_messages().is_empty());
        assert_eq!(
            super::sessions::summary(&args.session, &child)
                .parent_session
                .as_deref(),
            Some(parent.as_str())
        );
        let export = root.path().join("export.jsonl");
        child.export_jsonl(&export).unwrap();
        assert_eq!(
            Session::snapshot(export).unwrap().parent_session(),
            Some(parent.as_str())
        );
        assert_eq!(std::fs::read(&source).unwrap(), original);
        command(&json!({"type":"new_session"}), &mut args, &mut worker).unwrap();
        assert!(
            Session::snapshot(&args.session)
                .unwrap()
                .parent_session()
                .is_none()
        );
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
        assert_eq!(entries["entries"][0]["id"], second.to_string());
        assert_eq!(entries["entries"][0]["type"], "message");
        assert_eq!(entries["entries"][0]["parentId"], first.to_string());
        let forks = command(&json!({"type":"get_fork_messages"}), &mut args, &mut worker)
            .unwrap()
            .unwrap();
        assert_eq!(forks["messages"].as_array().unwrap().len(), 2);
        let tree = command(&json!({"type":"get_tree"}), &mut args, &mut worker)
            .unwrap()
            .unwrap();
        assert_eq!(tree["tree"].as_array().unwrap().len(), 1);
        let forked = command(
            &json!({"type":"fork","entryId":second.to_string()}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        assert_eq!(forked, Some(json!({"text":"second","cancelled":false})));
        assert_ne!(args.session, source);
        assert_eq!(args.model, "fixture:4b");
        assert_eq!(args.thinking.as_deref(), Some("low"));
        let fork = Session::open(&args.session).unwrap();
        assert_eq!(fork.conversation_at(fork.head()).unwrap().len(), 1);
        assert_eq!(
            fork.parent_session(),
            Some(source.to_string_lossy().as_ref())
        );
        drop(fork);
        assert_eq!(std::fs::read(&source).unwrap(), before);
        command(
            &json!({"type":"switch_session","sessionPath":source}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        assert_eq!(args.session, source);
        let cloned = command(&json!({"type":"clone"}), &mut args, &mut worker).unwrap();
        assert_eq!(cloned, Some(json!({"cancelled":false})));
        let clone = Session::snapshot(&args.session).unwrap();
        assert_eq!(clone.current_messages().len(), 2);
        let cloned = command(
            &json!({"type":"clone","includeNative":true}),
            &mut args,
            &mut worker,
        )
        .unwrap();
        assert_eq!(
            cloned,
            Some(json!({"cancelled":false,"sessionFile":args.session}))
        );
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
    fn clone_rejects_sessions_without_a_visible_leaf() {
        let root = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.workspace = root.path().to_path_buf();
        args.session = root.path().join("source.jsonl");
        let mut session = Session::open(&args.session).unwrap();
        session.set_working_directory(root.path()).unwrap();
        drop(session);
        let original = args.session.clone();
        let error = command(&json!({"type":"clone"}), &mut args, &mut None).unwrap_err();
        assert!(matches!(error, Error::Config(message)
            if message == "Cannot clone session: no current entry selected"));
        assert_eq!(args.session, original);
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
        session
            .append_user_with_images("existing conversation".into(), Vec::new(), None)
            .unwrap();
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
    fn switching_to_empty_saved_session_uses_initial_model() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("empty.jsonl");
        let mut session = Session::open(&path).unwrap();
        session.set_working_directory(root.path()).unwrap();
        session.select_model("old-model", Some("low")).unwrap();
        drop(session);
        let mut args = Cli::try_parse_from(["pi", "--no-trust"]).unwrap();
        args.workspace = root.path().into();
        args.model = "initial-model".into();
        args.remember_initial_selection();
        args.model = "current-model".into();
        switch_session(&mut args, &path).unwrap();
        assert_eq!(args.model, "initial-model");
    }

    #[test]
    fn new_session_restores_original_explicit_cli_selection() {
        let root = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.workspace = root.path().into();
        args.session = root.path().join("current.jsonl");
        args.session_dir = Some(root.path().into());
        args.endpoint = "https://example.invalid/v1/chat/completions".into();
        args.endpoint_explicit = true;
        args.initial_connection = Some((super::super::Provider::Ollama, args.endpoint.clone()));
        args.model = "alpha".into();
        args.thinking = Some("low".into());
        args.model_explicit = true;
        args.thinking_explicit = true;
        args.remember_initial_selection();
        args.change_model(
            super::super::Provider::Ollama,
            "beta".into(),
            Some("high".into()),
            pi_rs::ModelSelectSource::Set,
        )
        .unwrap();
        command(&json!({"type":"new_session"}), &mut args, &mut None).unwrap();
        assert_eq!(
            (args.model.as_str(), args.thinking.as_deref()),
            ("alpha", Some("low"))
        );
        assert_eq!(
            Session::open(&args.session).unwrap().model_selection(),
            Some(("alpha".into(), Some("low".into())))
        );
    }

    #[test]
    fn switching_workspace_rebuilds_defaults_and_scope_for_new_session() {
        let root = tempfile::tempdir().unwrap();
        let launch = root.path().join("launch");
        let target = root.path().join("target");
        for (workspace, model, thinking) in [(&launch, "alpha", "low"), (&target, "beta", "high")] {
            std::fs::create_dir_all(workspace.join(".pi")).unwrap();
            std::fs::write(workspace.join(".pi/settings.json"), json!({"defaultProvider":"ollama", "defaultModel":model, "defaultThinkingLevel":thinking, "enabledModels":[format!("ollama/{model}:{thinking}")]}).to_string()).unwrap();
        }
        let target_session = root.path().join("target.jsonl");
        let mut session = Session::open(&target_session).unwrap();
        session.set_working_directory(&target).unwrap();
        drop(session);
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.workspace = launch.clone();
        args.session = root.path().join("current.jsonl");
        args.session_dir = Some(root.path().into());
        args.trust = true;
        args.model_profiles = super::super::model_profiles::Profiles::parse(&json!({"providers":{"ollama":{"baseUrl":"https://example.invalid/v1", "models":[{"id":"alpha","reasoning":true},{"id":"beta","reasoning":true}]}}})).unwrap();
        args.endpoint = args
            .model_profiles
            .endpoint(&super::super::Provider::Ollama)
            .into();
        args.settings = super::super::config::Settings::load(&launch, true).unwrap();
        args.model = "alpha".into();
        args.refresh_workspace_selection();
        args.thinking = Some("low".into());
        args.remember_initial_selection();
        switch_session(&mut args, &target_session).unwrap();
        assert_eq!(
            (args.model.as_str(), args.thinking.as_deref()),
            ("beta", Some("high"))
        );
        assert_eq!(
            args.model_scope
                .iter()
                .map(|choice| choice.model.as_str())
                .collect::<Vec<_>>(),
            ["beta"]
        );
        args.change_model(
            super::super::Provider::Ollama,
            "alpha".into(),
            Some("low".into()),
            pi_rs::ModelSelectSource::Set,
        )
        .unwrap();
        command(&json!({"type":"new_session"}), &mut args, &mut None).unwrap();
        assert_eq!(
            (args.model.as_str(), args.thinking.as_deref()),
            ("beta", Some("high"))
        );
        assert_eq!(
            Session::open(&args.session).unwrap().model_selection(),
            Some(("beta".into(), Some("high".into())))
        );
    }

    #[test]
    fn thinking_rpc_does_not_emit_model_change_after_virtual_response() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("pi.jsonl");
        let entries = [
            json!({"type":"session","version":3}),
            json!({"type":"model_change","id":"change","parentId":null,"provider":"router","modelId":"virtual"}),
            json!({"type":"message","id":"assistant","parentId":"change","message":{"role":"assistant","provider":"ollama","model":"physical","content":[]}}),
        ];
        std::fs::write(
            &source,
            entries
                .iter()
                .map(|entry| format!("{entry}\n"))
                .collect::<String>(),
        )
        .unwrap();
        let native = root.path().join("native.jsonl");
        super::super::session_import::read(&source)
            .unwrap()
            .export_jsonl(&native)
            .unwrap();
        let mut args = Cli::try_parse_from(["pi", "--offline"]).unwrap();
        args.session = native.clone();
        args.model = "physical".into();
        args.thinking = Some("low".into());
        command(
            &json!({"type":"set_thinking_level","level":"high"}),
            &mut args,
            &mut None,
        )
        .unwrap();
        let session = Session::open(&native).unwrap();
        let bytes = super::super::rpc_entries::Entries::new(&session)
            .write_share_branch(&session, root.path())
            .unwrap();
        let entries: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry["type"] == "model_change")
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .rev()
                .find(|entry| entry["type"] == "thinking_level_change")
                .unwrap()["thinkingLevel"],
            "high"
        );
    }

    #[test]
    fn thinking_commands_persist_and_reject_invalid_levels() {
        let root = tempfile::tempdir().unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.session = root.path().join("thinking.jsonl");
        args.endpoint = "http://127.0.0.1:9/chat/completions".into();
        args.model_scope = vec![
            crate::model_selection::ScopedChoice {
                provider: super::super::Provider::Ollama,
                model: "a".into(),
                thinking: Some("low".into()),
            },
            crate::model_selection::ScopedChoice {
                provider: super::super::Provider::Ollama,
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
        assert!(
            matches!(args.selection_events.last(), Some(pi_rs::LifecycleEvent::ModelSelect {
            source: pi_rs::ModelSelectSource::Cycle, model, previous_model: Some(previous)
        }) if model["id"] == "b" && previous["id"] == "a")
        );
        args.model_scope.truncate(1);
        assert_eq!(
            command(&json!({"type":"cycle_model"}), &mut args, &mut worker).unwrap(),
            Some(Value::Null)
        );
        assert_eq!(args.model, "b");
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
