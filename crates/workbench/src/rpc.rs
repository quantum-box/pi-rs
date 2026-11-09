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
    let session = Session::open(&args.session)?;
    if session.host_binding().is_some() {
        return Err(Error::Session(
            "a managed session must be opened by its host".into(),
        ));
    }
    drop(session);
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
    loop {
        if let Some(active) = &mut worker {
            for event in active.events.try_iter() {
                forward_event(event, &mut active.busy, &mut pending_abort)?;
            }
            if let Some(status) = active.child.try_wait()? {
                let mut finished = worker.take().expect("active worker");
                for reader in finished.readers.drain(..) {
                    let _ = reader.join();
                }
                for event in finished.events.try_iter() {
                    forward_event(event, &mut finished.busy, &mut pending_abort)?;
                }
                if finished.busy {
                    emit(json!({"type":"agent_end","success":status.success()}))?;
                    emit(json!({"type":"agent_settled"}))?;
                }
                if let Some(request) = pending_abort.take() {
                    response(&request, Ok(None))?;
                }
            }
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
        if request["type"] == "abort" && busy(&worker) {
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
        "abort" => Ok(None),
        "get_state" => Ok(Some(
            json!({"sessionFile":args.session,"isStreaming":busy(worker),"isCompacting":worker.as_ref().is_some_and(|worker| worker.busy && worker.compacting),"model":{"id":args.model,"provider":"ollama"}}),
        )),
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
            Ok(Some(json!({"cancelled":false})))
        }
        "set_model" if !busy(worker) => {
            let model = request["modelId"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Error::Config("modelId is required".into()))?;
            args.model = model.to_owned();
            Session::open(&args.session)?.select_model(model, args.thinking.as_deref())?;
            Ok(Some(json!({"id":model,"provider":"ollama"})))
        }
        "compact" if !busy(worker) => {
            Worker::submit(
                worker,
                args,
                json!({"type":"compact","summary":null,"keep_turns":1}),
            )?;
            Ok(Some(json!({"disposition":"started"})))
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
            let stats = sessions::summary(&args.session, &session);
            Ok(Some(
                json!({"sessionId":session.session_id(),"sessionFile":args.session,"sessionName":session.name(),"totalMessages":stats.messages,"tokens":{"input":stats.input_tokens,"output":stats.output_tokens,"total":stats.input_tokens.saturating_add(stats.output_tokens)}}),
            ))
        }
        _ => Err(Error::Config(format!(
            "command {name:?} is unavailable in the current state"
        ))),
    }
}

fn busy(worker: &Option<Worker>) -> bool {
    worker.as_ref().is_some_and(|worker| worker.busy)
}

fn forward_event(event: Value, busy: &mut bool, pending_abort: &mut Option<Value>) -> Result<()> {
    match event["type"].as_str() {
        Some("backend_ready") => Ok(()),
        Some("agent_started") => {
            *busy = true;
            emit(json!({"type":"agent_start"}))
        }
        Some("agent_settled") => {
            *busy = false;
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
