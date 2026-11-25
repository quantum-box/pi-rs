//! Text-only subset of the line-delimited Pi RPC protocol.
use std::collections::HashSet;
use std::io::{self, BufRead, Read};
use std::num::NonZeroUsize;
use std::time::Duration;

use pi_rs_core::Message;
use pi_rs_durable::progress;
use pi_rs_durable::runtime::{
    AdmissionOptions, Conversation, InputMode, OnAbort, QueueMode, Scheduler, Status,
};
use pi_rs_durable::worker_activity::WorkerKind;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{RoutedModel, emit};

const MAX_REQUEST_BYTES: u64 = 1024 * 1024;

struct Turn {
    submission: Uuid,
    users: Vec<Value>,
    partial: String,
    attempt: Option<Uuid>,
    assistant_open: bool,
    emitted_failures: HashSet<Uuid>,
}

impl Turn {
    fn start(submission: Uuid, content: &str) -> io::Result<Self> {
        emit(&json!({"type":"agent_start"}))?;
        let mut turn = Self {
            submission,
            users: Vec::new(),
            partial: String::new(),
            attempt: None,
            assistant_open: false,
            emitted_failures: HashSet::new(),
        };
        turn.emit_user(content)?;
        Ok(turn)
    }

    fn emit_user(&mut self, content: &str) -> io::Result<()> {
        let user = text_message("user", content);
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
                _ => false,
            };
            if same_run && seen.insert(input.id) {
                self.emit_user(&input.content)?;
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
    let state = scheduler.conversation(id)?;
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
                        watching |= respond(scheduler, id, &provider_model, &mut active, &mut seen, request).await?;
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
    let mut admitted = None;
    let result = match command {
        "prompt" | "steer" | "follow_up" => submit_input(scheduler, id, command, &request)
            .map(|(submission, running)| {
                admitted = Some((submission, running));
                Some(json!({"disposition": if running { "started" } else { "queued" }}))
            }),
        "clear_queue" => scheduler.clear_input_queue(id).map(|withdrawn| {
            let (mut steering, mut follow_up) = (Vec::new(), Vec::new());
            for input in withdrawn {
                seen.insert(input.submission.id);
                let queue = match input.mode {
                    InputMode::Steer => &mut steering,
                    InputMode::FollowUp => &mut follow_up,
                };
                queue.push(input.submission.content);
            }
            Some(json!({"steering": steering, "followUp": follow_up}))
        }).map_err(|error| error.to_string()),
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
                settled.map(|_| None).map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        },
        "get_state" => scheduler.conversation(id).map(|state| {
            Some(json!({
                "sessionId": id,
                "model": {"id": model, "provider":"openai", "api":"openai-responses"},
                "isStreaming": state.submissions.iter().any(|s| s.status == Status::Running),
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
    if let Some((submission, true)) = admitted
        && active.is_none()
    {
        seen.insert(submission);
        *active = Some(Turn::start(
            submission,
            request["message"].as_str().unwrap(),
        )?);
    }
    Ok(admitted.is_some())
}

fn submit_input(
    scheduler: &mut Scheduler,
    id: Uuid,
    command: &str,
    request: &Value,
) -> Result<(Uuid, bool), String> {
    if request
        .get("images")
        .is_some_and(|value| value.as_array().is_none_or(|images| !images.is_empty()))
    {
        return Err("durable text RPC accepts text input".into());
    }
    let message = request["message"]
        .as_str()
        .ok_or("input message must be text")?;
    let behavior = match (command, request.get("streamingBehavior")) {
        ("steer", _) => Some(InputMode::Steer),
        ("follow_up", _) => Some(InputMode::FollowUp),
        (_, None) => None,
        (_, Some(value)) if value == "steer" => Some(InputMode::Steer),
        (_, Some(value)) if value == "followUp" => Some(InputMode::FollowUp),
        _ => return Err("streamingBehavior must be steer or followUp".into()),
    };
    let state = scheduler
        .conversation(id)
        .map_err(|error| error.to_string())?;
    let running = state
        .submissions
        .iter()
        .any(|s| matches!(s.status, Status::Running | Status::Queued));
    let queued = command != "prompt" || running;
    if queued && behavior.is_none() {
        return Err("a running prompt needs streamingBehavior".into());
    }
    let options = AdmissionOptions {
        mode: behavior.unwrap_or(InputMode::FollowUp),
        reject_when_busy: !queued,
        reject_when_idle: command != "prompt",
        on_abort: OnAbort::Keep,
    };
    scheduler
        .submit_with_options(id, None, message.to_owned(), options)
        .map(|submission| (submission, !queued))
        .map_err(|error| error.to_string())
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
    let view = scheduler.view(id, NonZeroUsize::MIN)?;
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
                matches!(input.status, Status::Running | Status::Done { .. })
                    && !seen.contains(&input.id)
            }) else {
                return Ok(());
            };
            seen.insert(input.id);
            *active = Some(Turn::start(input.id, &input.content)?);
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
        .filter_map(|message| match message {
            Message::User { content } => Some(text_message("user", content)),
            Message::Assistant { content, .. } => Some(text_message("assistant", content)),
            _ => None,
        })
        .collect()
}

fn text_message(role: &str, text: &str) -> Value {
    let mut message = json!({"role":role,"content":[{"type":"text","text":text}],"timestamp":0});
    if role == "assistant" {
        message["stopReason"] = json!("stop");
    }
    message
}
