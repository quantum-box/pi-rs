//! Text-only subset of the line-delimited Pi RPC protocol.
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
}

impl Turn {
    fn start(submission: Uuid, content: &str) -> io::Result<Self> {
        let user = text_message("user", content);
        emit(&json!({"type":"agent_start"}))?;
        emit(&json!({"type":"message_start","message":user}))?;
        emit(&json!({"type":"message_end","message":user}))?;
        emit(&json!({"type":"turn_start"}))?;
        emit(&json!({"type":"message_start","message":text_message("assistant", "")}))?;
        Ok(Self {
            submission,
            user,
            partial: String::new(),
        })
    }

    fn update(&mut self, text: String) -> io::Result<()> {
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
        let delta = text.strip_prefix(&self.partial).unwrap_or_default();
        message_update(
            text_message("assistant", &text),
            json!({
                "type":"text_delta", "contentIndex":0, "delta":delta
            }),
        )?;
        self.partial = text;
        Ok(())
    }

    fn end(&mut self, text: &str, failure: Option<&str>) -> io::Result<()> {
        self.update(text.to_owned())?;
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
        emit(&json!({
            "type":"agent_end",
            "success":failure.is_none(),
            "messages":[self.user, message],
            "willRetry":false
        }))
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
                        respond(scheduler, id, &provider_model, &mut active, request)?;
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

fn respond(
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
        "abort" => scheduler.abort(id).map(|_| None).map_err(|error| error.to_string()),
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
        if view
            .state
            .submissions
            .iter()
            .any(|input| input.id == turn.submission && matches!(input.status, Status::Running))
            && let Some(progress::State::Running(partial)) = view.live
        {
            turn.update(partial.text)?;
        }
        return Ok(false);
    }

    // Join the finished worker to release the handle and collect provider errors.
    let failure = scheduler
        .wait(id)
        .await
        .err()
        .map(|error| error.to_string());
    let state = scheduler.conversation(id)?;
    match state
        .submissions
        .iter()
        .find(|input| input.id == turn.submission)
        .map(|input| &input.status)
    {
        Some(Status::Done { answer }) => turn.end(answer, None)?,
        Some(Status::Unanswered { reason }) => turn.end(&turn.partial.clone(), Some(reason))?,
        _ => turn.end(
            &turn.partial.clone(),
            Some(failure.as_deref().unwrap_or("submission is still pending")),
        )?,
    }
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
