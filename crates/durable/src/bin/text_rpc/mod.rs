//! Companion implementation of the line-delimited Pi RPC protocol.
use std::collections::HashSet;
use std::io::{self, BufRead, Read};
use std::sync::Arc;
use std::time::Duration;

use pi_rs_core::{CancellationToken, Message};
use pi_rs_durable::images::{BuiltinImageProcessor, UserImageAdmission};
use pi_rs_durable::progress;
use pi_rs_durable::runtime::{
    AdmissionError, AdmissionOptions, Conversation, Error, InputMode, OnAbort, QueueMode,
    Scheduler, Status, Submission, WithdrawnInput,
};
use pi_rs_durable::worker_activity::WorkerKind;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{RoutedModel, emit};

const MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;

struct Turn {
    submission: Uuid,
    users: Vec<Value>,
    partial: String,
    attempt: Option<Uuid>,
    assistant_open: bool,
    emitted_failures: HashSet<Uuid>,
}

impl Turn {
    fn start(input: &Submission) -> io::Result<Self> {
        emit(&json!({"type":"agent_start"}))?;
        let mut turn = Self {
            submission: input.id,
            users: Vec::new(),
            partial: String::new(),
            attempt: None,
            assistant_open: false,
            emitted_failures: HashSet::new(),
        };
        turn.emit_user(input)?;
        Ok(turn)
    }

    fn emit_user(&mut self, input: &Submission) -> io::Result<()> {
        let user = submission_message(input);
        emit(&json!({"type":"message_start","message":user}))?;
        emit(&json!({"type":"message_end","message":user}))?;
        self.users.push(user);
        Ok(())
    }

    fn emit_joined_inputs(
        &mut self,
        state: &Conversation,
        seen: &mut HashSet<Uuid>,
        status: &Status,
    ) -> io::Result<()> {
        let answer = state.answer_id(self.submission);
        for input in &state.submissions {
            let same_run = match status {
                Status::Running => input.status == Status::Running,
                Status::Done { .. } => answer.is_some() && state.answer_id(input.id) == answer,
                Status::Unanswered { .. } => state.run_failure.as_ref().is_some_and(|failed| {
                    failed.inputs.contains(&self.submission) && failed.inputs.contains(&input.id)
                }),
                Status::Queued => false,
            };
            if same_run && seen.insert(input.id) {
                self.emit_user(input)?;
            }
        }
        Ok(())
    }

    fn begin_attempt(&mut self, attempt: Option<Uuid>) -> io::Result<()> {
        if self.assistant_open && attempt != self.attempt {
            self.end_attempt(&self.partial.clone(), Some("model attempt was interrupted"))?;
        }
        if !self.assistant_open {
            emit(&json!({"type":"turn_start"}))?;
            emit(&json!({"type":"message_start","message":text_message("assistant", "")}))?;
            self.assistant_open = true;
            self.attempt = attempt;
        }
        Ok(())
    }

    fn update(&mut self, text: &str) -> io::Result<()> {
        if self.partial == text {
            return Ok(());
        }
        if self.partial.is_empty() && !text.is_empty() {
            message_update(
                text_message("assistant", ""),
                json!({
                    "type":"text_start", "contentIndex":0
                }),
            )?;
        }
        let delta = text.strip_prefix(&self.partial).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "model partial changed within an attempt",
            )
        })?;
        message_update(
            text_message("assistant", text),
            json!({
                "type":"text_delta", "contentIndex":0, "delta":delta
            }),
        )?;
        self.partial = text.to_owned();
        Ok(())
    }

    fn end_attempt(&mut self, text: &str, failure: Option<&str>) -> io::Result<Value> {
        self.update(text)?;
        let mut message = text_message("assistant", text);
        if !text.is_empty() {
            message_update(
                message.clone(),
                json!({
                    "type":"text_end", "contentIndex":0, "content":text
                }),
            )?;
        }
        if let Some(reason) = failure {
            message["stopReason"] = json!(if reason == "aborted" {
                "aborted"
            } else {
                "error"
            });
            message["errorMessage"] = json!(reason);
        }
        emit(&json!({"type":"message_end","message":message}))?;
        emit(&json!({"type":"turn_end","message":message,"toolResults":[]}))?;
        self.partial.clear();
        self.assistant_open = false;
        self.attempt = None;
        Ok(message)
    }

    fn emit_retried_attempts(&mut self, state: &Conversation) -> io::Result<()> {
        for partial in &state.aborted_generations {
            if partial.submission == Some(self.submission)
                && !partial.terminal
                && self.emitted_failures.insert(partial.request_id)
            {
                self.begin_attempt(Some(partial.request_id))?;
                self.end_attempt(&partial.text, Some("model attempt was interrupted"))?;
            }
        }
        Ok(())
    }

    fn end(&mut self, text: &str, failure: Option<&str>) -> io::Result<()> {
        if !self.assistant_open || !text.starts_with(&self.partial) {
            self.begin_attempt(None)?;
        }
        let message = self.end_attempt(text, failure)?;
        self.users.push(message);
        emit(&json!({
            "type":"agent_end",
            "success":failure.is_none(),
            "messages":self.users,
            "willRetry":false
        }))
    }

    fn settle(&mut self, state: &Conversation, failure: Option<String>) -> io::Result<()> {
        self.emit_retried_attempts(state)?;
        let status = state
            .submissions
            .iter()
            .find(|input| input.id == self.submission)
            .map(|input| &input.status);
        match status {
            Some(Status::Done { answer }) => self.end(answer, None),
            Some(Status::Unanswered { reason }) => {
                if let Some(partial) =
                    state.aborted_generations.iter().rev().find(|partial| {
                        partial.submission == Some(self.submission) && partial.terminal
                    })
                {
                    self.begin_attempt(Some(partial.request_id))?;
                    self.end(&partial.text, Some(reason))
                } else {
                    self.end(&self.partial.clone(), Some(reason))
                }
            }
            _ => self.end(
                &self.partial.clone(),
                Some(failure.as_deref().unwrap_or("submission is still pending")),
            ),
        }
    }
}

pub(super) async fn run(
    scheduler: &mut Scheduler,
    id: Uuid,
    model: &RoutedModel,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = prepare_input_adapter(scheduler, id, model.has_image_adapter)?;
    let provider_model = model.provider_model(&state.config.model)?.to_owned();
    let mut seen = state
        .submissions
        .iter()
        .filter(|input| !matches!(input.status, Status::Queued | Status::Running))
        .map(|input| input.id)
        .collect();
    let mut watching = state
        .submissions
        .iter()
        .any(|input| matches!(input.status, Status::Queued | Status::Running));
    if watching {
        scheduler.schedule(id);
    }
    let mut active = None;
    let mut input = input_reader();
    let mut input_open = true;
    let mut poll = tokio::time::interval(Duration::from_millis(50));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    while input_open || watching {
        tokio::select! {
            result = &mut interrupt => {
                result?;
                return Err("interrupted".into());
            }
            received = input.recv(), if input_open => {
                match received {
                    Some(request) => {
                        watching |= respond(scheduler, id, &provider_model, model.has_image_adapter, &mut active, &mut seen, request).await?;
                    }
                    None => input_open = false,
                }
            }
            _ = poll.tick(), if watching => {
                watching = advance(scheduler, id, &mut active, &mut seen).await?;
            }
        }
    }
    Ok(())
}

fn input_reader() -> tokio::sync::mpsc::Receiver<Result<Value, String>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(16);
    std::thread::spawn(move || {
        let mut stdin = io::stdin().lock();
        loop {
            let mut line = Vec::new();
            match stdin
                .by_ref()
                .take(MAX_REQUEST_BYTES + 1)
                .read_until(b'\n', &mut line)
            {
                Ok(0) => break,
                Ok(n) if n as u64 > MAX_REQUEST_BYTES => {
                    let _ = sender.blocking_send(Err("RPC request exceeds the size limit".into()));
                    break;
                }
                Ok(_) => {
                    let request = serde_json::from_slice(&line).map_err(|error| error.to_string());
                    if sender.blocking_send(request).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.blocking_send(Err(error.to_string()));
                    break;
                }
            }
        }
    });
    receiver
}

async fn respond(
    scheduler: &mut Scheduler,
    id: Uuid,
    model: &str,
    has_image_adapter: bool,
    active: &mut Option<Turn>,
    seen: &mut HashSet<Uuid>,
    request: Result<Value, String>,
) -> io::Result<bool> {
    let request = match request {
        Ok(request) if request.is_object() => request,
        Ok(_) => {
            return reply(&Value::Null, Err("RPC request must be an object".into())).map(|_| false);
        }
        Err(error) => return reply(&Value::Null, Err(error)).map(|_| false),
    };
    let command = request["type"].as_str().unwrap_or_default();
    let mut admitted = false;
    let result = match command {
        "prompt" | "steer" | "follow_up" => {
            let state = prepare_input_adapter(scheduler, id, has_image_adapter)
                .map_err(io::Error::other)?;
            publish_turns(&state, active, seen, None, true, None)?;
            let mut result = submit_input(scheduler, &state, command, &request, false).await;
            if matches!(result, Err(Error::Admission(AdmissionError::Idle)))
                && state.input_run_active()
                && request["images"].as_array().is_some_and(|images| !images.is_empty())
            {
                // The previous run settled during fitting. Publish its result
                // before admission can clear its persisted failure record.
                let settled = prepare_input_adapter(scheduler, id, has_image_adapter)
                    .map_err(io::Error::other)?;
                publish_turns(&settled, active, seen, None, true, None)?;
                result = submit_input(scheduler, &settled, command, &request, true)
                    .await.map(|_| false);
            }
            result.map_err(|error| error.to_string()).map(|started| {
                admitted = true;
                Some(json!({"disposition": if started { "started" } else { "queued" }}))
            })
        },
        "clear_queue" => scheduler.clear_input_queue(id)
            .map(|withdrawn| Some(queue_response(withdrawn, seen, request["includeContent"] == true)))
            .map_err(|error| error.to_string()),
        "set_steering_mode" | "set_follow_up_mode" => {
            set_queue_mode(scheduler, id, command, &request)
        }
        "abort" => match scheduler.abort(id) {
            Ok(()) => {
                let settled = scheduler.wait(id).await;
                let state = scheduler.conversation(id).map_err(io::Error::other)?;
                publish_turns(
                    &state, active, seen, None, false,
                    settled.as_ref().err().map(ToString::to_string),
                )?;
                settled.and_then(|_| {
                    if request["restoreQueue"] == true {
                        // RPC inputs survive abort. Once the scheduler is stopped,
                        // the same RPC handler can withdraw them before another admission.
                        let withdrawn = scheduler.clear_input_queue(id)?;
                        Ok(Some(queue_response(withdrawn, seen, true)))
                    } else {
                        Ok(None)
                    }
                }).map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        },
        "get_state" => scheduler.conversation(id).map(|state| {
            Some(json!({
                "sessionId": id,
                "model": {"id": model, "provider":"openai", "api":"openai-responses"},
                "isStreaming": state.input_run_active(),
                "isCompacting": false,
                "steeringMode": state.queue_modes.steering_mode,
                "followUpMode": state.queue_modes.follow_up_mode,
                "messageCount": messages(&state).len(),
                "pendingMessageCount": state.submissions.iter().filter(|s| matches!(s.status, Status::Queued)).count()
            }))
        }).map_err(|error| error.to_string()),
        "get_messages" => scheduler.conversation(id).map(|state| {
            let mut data = json!({"messages":messages(&state)});
            if request["includeNative"] == true {
                data["nativeMessages"] = json!(state.messages);
            }
            Some(data)
        }).map_err(|error| error.to_string()),
        _ => Err("unsupported durable RPC command".into()),
    };
    reply(&request, result)?;
    Ok(admitted)
}

fn queue_response(
    withdrawn: Vec<WithdrawnInput>,
    seen: &mut HashSet<Uuid>,
    include_content: bool,
) -> Value {
    let (mut steering, mut follow_up) = (Vec::new(), Vec::new());
    let mut inputs = Vec::new();
    for input in withdrawn {
        seen.insert(input.submission.id);
        let (queue, mode) = match input.mode {
            InputMode::Steer => (&mut steering, "steer"),
            InputMode::FollowUp => (&mut follow_up, "followUp"),
        };
        if include_content {
            let mut message = submission_message(&input.submission);
            message["inputMode"] = json!(mode);
            inputs.push(message);
        }
        let text = input
            .submission
            .input_blocks
            .as_ref()
            .and_then(|value| value.blocks.first())
            .and_then(|block| block["text"].as_str())
            .map(str::to_owned)
            .unwrap_or(input.submission.content);
        queue.push(text);
    }
    let mut result = json!({"steering": steering, "followUp": follow_up});
    if include_content {
        result["inputs"] = json!(inputs);
    }
    result
}

fn prepare_input_adapter(
    scheduler: &mut Scheduler,
    id: Uuid,
    has_image_adapter: bool,
) -> Result<Conversation, Error> {
    let state = scheduler.conversation(id)?;
    if has_image_adapter && state.provider_adapter.is_none() && !state.input_run_active() {
        scheduler.select_provider_adapter(id, true)?;
        return scheduler.conversation(id);
    }
    Ok(state)
}

async fn submit_input(
    scheduler: &mut Scheduler,
    state: &Conversation,
    command: &str,
    request: &Value,
    continue_after_settlement: bool,
) -> Result<bool, Error> {
    let attachments = match request.get("images") {
        Some(value) => value
            .as_array()
            .ok_or_else(|| Error::Invalid("images must be an array".into()))?
            .as_slice(),
        None => &[],
    };
    let message = request["message"]
        .as_str()
        .ok_or_else(|| Error::Invalid("input message must be text".into()))?;
    let behavior = match (command, request.get("streamingBehavior")) {
        ("steer", _) => Some(InputMode::Steer),
        ("follow_up", _) => Some(InputMode::FollowUp),
        (_, None) => None,
        (_, Some(value)) if value == "steer" => Some(InputMode::Steer),
        (_, Some(value)) if value == "followUp" => Some(InputMode::FollowUp),
        _ => {
            return Err(Error::Invalid(
                "streamingBehavior must be steer or followUp".into(),
            ));
        }
    };
    let running = state.input_run_active();
    let queued = command != "prompt" || running;
    if queued && behavior.is_none() {
        return Err(Error::Invalid(
            "a running prompt needs streamingBehavior".into(),
        ));
    }
    let options = AdmissionOptions {
        mode: behavior.unwrap_or(InputMode::FollowUp),
        reject_when_busy: !queued,
        reject_when_idle: queued && !continue_after_settlement,
        on_abort: OnAbort::Keep,
    };
    let submission = if attachments.is_empty() {
        scheduler.submit_with_options(state.id, None, message.to_owned(), options)
    } else {
        if attachments.iter().any(|image| {
            image["type"] != "image" || !image["data"].is_string() || !image["mimeType"].is_string()
        }) {
            return Err(Error::Invalid(
                "images must contain image blocks with data and mimeType".into(),
            ));
        }
        let mut blocks = vec![json!({"type": "text", "text": message})];
        blocks.extend_from_slice(attachments);
        let admission = UserImageAdmission::new(Arc::new(BuiltinImageProcessor));
        scheduler
            .submit_fitted_user_blocks_with_options(
                state.id,
                None,
                blocks,
                &admission,
                options,
                &CancellationToken::default(),
            )
            .await
            .map(|input| input.submission_id)
    };
    submission.map(|_| !queued)
}

fn set_queue_mode(
    scheduler: &mut Scheduler,
    id: Uuid,
    command: &str,
    request: &Value,
) -> Result<Option<Value>, String> {
    let mode = match request["mode"].as_str() {
        Some("all") => QueueMode::All,
        Some("one-at-a-time") => QueueMode::OneAtATime,
        _ => return Err("mode must be all or one-at-a-time".into()),
    };
    let mut modes = scheduler
        .conversation(id)
        .map_err(|e| e.to_string())?
        .queue_modes;
    if command == "set_steering_mode" {
        modes.steering_mode = mode;
    } else {
        modes.follow_up_mode = mode;
    }
    scheduler
        .set_queue_modes(id, modes)
        .map_err(|e| e.to_string())?;
    Ok(None)
}

fn reply(request: &Value, result: Result<Option<Value>, String>) -> io::Result<()> {
    let mut reply = json!({"type":"response","success":result.is_ok()});
    if let Some(command) = request.get("type") {
        reply["command"] = command.clone();
    }
    if let Some(id) = request.get("id") {
        reply["id"] = id.clone();
    }
    match result {
        Ok(Some(data)) => reply["data"] = data,
        Err(error) => reply["error"] = json!(error),
        Ok(None) => {}
    }
    emit(&reply)
}

async fn advance(
    scheduler: &mut Scheduler,
    id: Uuid,
    active: &mut Option<Turn>,
    seen: &mut HashSet<Uuid>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let running = scheduler
        .worker_activity()
        .snapshot()
        .iter()
        .any(|worker| worker.kind == WorkerKind::Conversation && worker.id == id);
    let failure = if running {
        None
    } else {
        scheduler
            .wait(id)
            .await
            .err()
            .map(|error| error.to_string())
    };
    let view = scheduler.view_status(id)?;
    publish_turns(
        &view.state,
        active,
        seen,
        view.live.as_ref(),
        running,
        failure,
    )?;
    Ok(running)
}

fn publish_turns(
    state: &Conversation,
    active: &mut Option<Turn>,
    seen: &mut HashSet<Uuid>,
    live: Option<&progress::State>,
    running: bool,
    failure: Option<String>,
) -> io::Result<()> {
    loop {
        if active.is_none() {
            let Some(input) = state.submissions.iter().find(|input| {
                !seen.contains(&input.id)
                    && match input.status {
                        Status::Running | Status::Done { .. } => true,
                        Status::Unanswered { .. } => state
                            .run_failure
                            .as_ref()
                            .is_some_and(|failed| failed.inputs.contains(&input.id)),
                        Status::Queued => false,
                    }
            }) else {
                return Ok(());
            };
            seen.insert(input.id);
            *active = Some(Turn::start(input)?);
        }
        let turn = active.as_mut().unwrap();
        let status = &state
            .submissions
            .iter()
            .find(|input| input.id == turn.submission)
            .ok_or_else(|| io::Error::other("active RPC input disappeared"))?
            .status;
        turn.emit_joined_inputs(state, seen, status)?;
        if running && !matches!(status, Status::Done { .. } | Status::Unanswered { .. }) {
            turn.emit_retried_attempts(state)?;
            if let Some(request_id) = state.model_request_id(turn.submission)
                && let Some(progress::State::Running(partial)) = live
            {
                turn.begin_attempt(Some(request_id))?;
                turn.update(&partial.text)?;
            }
            return Ok(());
        }
        turn.settle(state, failure.clone())?;
        *active = None;
    }
}

fn message_update(message: Value, mut event: Value) -> io::Result<()> {
    event["partial"] = message.clone();
    emit(&json!({
        "type":"message_update",
        "message":message,
        "assistantMessageEvent":event
    }))
}

fn messages(state: &Conversation) -> Vec<Value> {
    state
        .messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| match message {
            Message::User { content } => Some(state.content_blocks.get(&index).map_or_else(
                || text_message("user", content),
                |value| user_message(&value.blocks),
            )),
            Message::Assistant { content, .. } => Some(text_message("assistant", content)),
            _ => None,
        })
        .collect()
}

fn submission_message(input: &Submission) -> Value {
    input.input_blocks.as_ref().map_or_else(
        || text_message("user", &input.content),
        |value| user_message(&value.blocks),
    )
}

fn user_message(blocks: &[Value]) -> Value {
    json!({"role": "user", "content": blocks, "timestamp": 0})
}

fn text_message(role: &str, text: &str) -> Value {
    let mut message = json!({"role":role,"content":[{"type":"text","text":text}],"timestamp":0});
    if role == "assistant" {
        message["stopReason"] = json!("stop");
    }
    message
}
