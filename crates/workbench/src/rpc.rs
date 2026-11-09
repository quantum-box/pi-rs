//! JSONL control over the same subprocess and journal as the terminal UI.
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::time::Duration;

use pi_rs::{Error, Result, Session};
use serde_json::{Value, json};

use super::{Cli, interactive, process::Worker};

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
                emit_event(event)?;
            }
            if let Some(status) = active.child.try_wait()? {
                let mut finished = worker.take().expect("active worker");
                for reader in finished.readers.drain(..) {
                    let _ = reader.join();
                }
                for event in finished.events.try_iter() {
                    emit_event(event)?;
                }
                emit(json!({"type":"agent_end","success":status.success()}))?;
                emit(json!({"type":"agent_settled"}))?;
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
        if request["type"] == "abort" && worker.is_some() {
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
            if request
                .get("images")
                .is_some_and(|value| value.as_array().is_none_or(|images| !images.is_empty()))
            {
                return Err(Error::Config(
                    "image input is not supported by this model adapter".into(),
                ));
            }
            let message = request["message"]
                .as_str()
                .filter(|text| !text.is_empty())
                .ok_or_else(|| Error::Config("message must be a nonempty string".into()))?;
            if let Some(active) = worker {
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
                active.send(json!({"type":kind,"content":message}))?;
                Ok(Some(json!({"disposition":"queued"})))
            } else {
                *worker = Some(Worker::start(args, &["run", message])?);
                Ok(Some(json!({"disposition":"started"})))
            }
        }
        "abort" => Ok(None),
        "get_state" => Ok(Some(
            json!({"sessionFile":args.session,"isStreaming":worker.is_some(),"isCompacting":worker.as_ref().is_some_and(|worker| worker.compacting),"model":{"id":args.model,"provider":"ollama"}}),
        )),
        "get_messages" if worker.is_none() => {
            let session = Session::open(&args.session)?;
            Ok(Some(
                json!({"messages":session.conversation_at(session.head())?}),
            ))
        }
        "new_session" if worker.is_none() => {
            args.session = if args.no_session {
                args.session
                    .with_file_name(format!("{}.jsonl", uuid::Uuid::new_v4()))
            } else {
                interactive::new_session_path()
            };
            if let Some(parent) = args.session.parent() {
                std::fs::create_dir_all(parent)?;
            }
            drop(Session::open(&args.session)?);
            Ok(Some(json!({"cancelled":false})))
        }
        "set_model" if worker.is_none() => {
            let model = request["modelId"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Error::Config("modelId is required".into()))?;
            args.model = model.to_owned();
            Ok(Some(json!({"id":model,"provider":"ollama"})))
        }
        "compact" if worker.is_none() => {
            *worker = Some(Worker::start(args, &["compact"])?);
            Ok(Some(json!({"disposition":"started"})))
        }
        "get_session_stats" if worker.is_none() => {
            let session = Session::open(&args.session)?;
            Ok(Some(
                json!({"sessionId":session.session_id(),"sessionFile":args.session,"totalMessages":session.conversation_at(session.head())?.len()}),
            ))
        }
        _ => Err(Error::Config(format!(
            "command {name:?} is unavailable in the current state"
        ))),
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
