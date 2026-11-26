//! Text-only subset of the line-delimited Pi RPC protocol.
use std::collections::HashSet;
use std::io::{self, BufRead, Read};
use std::num::NonZeroUsize;
use std::time::Duration;

use pi_rs_core::Message;
use pi_rs_durable::progress;
use pi_rs_durable::runtime::{Conversation, Scheduler, Status};
use pi_rs_durable::worker_activity::WorkerKind;
use serde_json::{Value, json};
use uuid::Uuid;

use super::{RoutedModel, emit};

const MAX_REQUEST_BYTES: u64 = 1024 * 1024;

struct Turn {
    submission: Uuid,
    user: Value,
    partial: String,
    attempt: Option<Uuid>,
    assistant_open: bool,
    emitted_failures: HashSet<Uuid>,
    foreground_subagents: Vec<Uuid>,
}

impl Turn {
    fn start(submission: Uuid, content: &str) -> io::Result<Self> {
        let user = text_message("user", content);
        emit(&json!({"type":"agent_start"}))?;
        emit(&json!({"type":"message_start","message":user}))?;
        emit(&json!({"type":"message_end","message":user}))?;
        Ok(Self {
            submission,
            user,
            partial: String::new(),
            attempt: None,
            assistant_open: false,
            emitted_failures: HashSet::new(),
            foreground_subagents: Vec::new(),
        })
    }

    fn update_foreground_subagents(&mut self, conversations: Vec<Uuid>) -> io::Result<()> {
        if self.foreground_subagents != conversations {
            emit(&json!({
                "type": "durable_foreground_subagents",
                "conversationIds": conversations,
            }))?;
            self.foreground_subagents = conversations;
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
        emit(&json!({
            "type":"agent_end",
            "success":failure.is_none(),
            "messages":[self.user, message],
            "willRetry":false
        }))
    }

    fn settle(&mut self, state: &Conversation, failure: Option<String>) -> io::Result<()> {
        self.update_foreground_subagents(Vec::new())?;
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
    let mut active = if let Some(input) = state
        .submissions
        .iter()
        .find(|input| matches!(input.status, Status::Queued | Status::Running))
    {
        scheduler.schedule(id);
        Some(Turn::start(input.id, &input.content)?)
    } else {
        None
    };
    let mut input = input_reader();
    let mut input_open = true;
    let mut poll = tokio::time::interval(Duration::from_millis(50));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    while input_open || active.is_some() {
        tokio::select! {
            result = &mut interrupt => {
                result?;
                return Err("interrupted".into());
            }
            received = input.recv(), if input_open => {
                match received {
                    Some(request) => {
                        respond(scheduler, id, &provider_model, &mut active, request).await?;
                    }
                    None => input_open = false,
                }
            }
            _ = poll.tick(), if active.is_some() => {
                if advance(scheduler, id, active.as_mut().unwrap()).await? {
                    active = None;
                }
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
    request: Result<Value, String>,
) -> io::Result<()> {
    let request = match request {
        Ok(request) if request.is_object() => request,
        Ok(_) => {
            return reply(&Value::Null, Err("RPC request must be an object".into()));
        }
        Err(error) => return reply(&Value::Null, Err(error)),
    };
    let command = request["type"].as_str().unwrap_or_default();
    let mut admitted = None;
    let result = match command {
        "prompt" => {
            if active.is_some() {
                Err("a prompt is already running".into())
            } else if request.get("images").is_some_and(|value| value.as_array().is_none_or(|images| !images.is_empty()))
                || request.get("streamingBehavior").is_some()
            {
                Err("durable text RPC accepts an unqueued text prompt".into())
            } else if let Some(message) = request["message"].as_str() {
                scheduler.submit(id, None, message.to_owned()).map(|submission| {
                    admitted = Some((submission, message));
                    Some(json!({"disposition":"started"}))
                }).map_err(|error| error.to_string())
            } else {
                Err("prompt message must be text".into())
            }
        }
        "abort" => match scheduler.abort(id) {
            Ok(()) => {
                let settled = scheduler.wait(id).await;
                if let Some(mut turn) = active.take() {
                    let state = scheduler.conversation(id).map_err(io::Error::other)?;
                    turn.settle(&state, settled.as_ref().err().map(ToString::to_string))?;
                }
                settled.map(|_| None).map_err(|error| error.to_string())
            }
            Err(error) => Err(error.to_string()),
        },
        "get_state" => scheduler.conversation(id).map(|state| {
            Some(json!({
                "sessionId": id,
                "model": {"id": model, "provider":"openai", "api":"openai-responses"},
                "isStreaming": active.is_some(),
                "isCompacting": false,
                "messageCount": messages(&state).len(),
                "pendingMessageCount": state.submissions.iter().filter(|s| matches!(s.status, Status::Queued)).count()
            }))
        }).map_err(|error| error.to_string()),
        "get_foreground_subagent" => foreground_subagent_view(scheduler, id, &request).map(Some),
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
    if let Some((submission, message)) = admitted {
        *active = Some(Turn::start(submission, message)?);
    }
    Ok(())
}

fn foreground_subagent_view(
    scheduler: &Scheduler,
    parent: Uuid,
    request: &Value,
) -> Result<Value, String> {
    let child = request["conversationId"]
        .as_str()
        .and_then(|id| Uuid::parse_str(id).ok())
        .ok_or("conversationId must be a UUID")?;
    let limit = NonZeroUsize::new(32).expect("nonzero transcript read limit");
    let child_view = scheduler
        .foreground_subagent_view(parent, child, limit)
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "conversationId": child,
        "messages": child_view.entries.into_iter().rev().collect::<Vec<_>>(),
        "live": child_view.live,
        "toolProgress": child_view.tool_progress,
    }))
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
    turn: &mut Turn,
) -> Result<bool, Box<dyn std::error::Error>> {
    if scheduler
        .worker_activity()
        .snapshot()
        .iter()
        .any(|worker| worker.kind == WorkerKind::Conversation && worker.id == id)
    {
        let view = scheduler.view(id, NonZeroUsize::MIN)?;
        turn.update_foreground_subagents(view.foreground_subagents)?;
        turn.emit_retried_attempts(&view.state)?;
        if let Some(request_id) = view.state.model_request_id(turn.submission)
            && let Some(progress::State::Running(partial)) = view.live
        {
            turn.begin_attempt(Some(request_id))?;
            turn.update(&partial.text)?;
        }
        return Ok(false);
    }

    // Join the finished worker to release the handle and collect provider errors.
    let failure = scheduler
        .wait(id)
        .await
        .err()
        .map(|error| error.to_string());
    turn.settle(&scheduler.conversation(id)?, failure)?;
    Ok(true)
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
