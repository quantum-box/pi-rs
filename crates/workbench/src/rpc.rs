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
    if let Some((model, thinking)) = session.model_selection() {
        args.restore_model_selection(model, thinking);
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
        "get_state" => {
            let mut state = json!({"sessionFile":args.session,"isStreaming":busy(worker),"isCompacting":worker.as_ref().is_some_and(|worker| worker.busy && worker.compacting),"model":model_value(&args.model),"thinkingLevel":args.thinking.as_deref().unwrap_or("off")});
            if !busy(worker) {
                let session = Session::open(&args.session)?;
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
            if let Some(choice) = args
                .model_scope
                .iter()
                .find(|choice| choice.model == args.model)
                && choice.thinking.is_some()
            {
                args.thinking = choice.thinking.clone();
            }
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
            Session::open(&args.session)?.select_model(model, args.thinking.as_deref())?;
            Ok(Some(model_value(model)))
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
    if let Some((model, thinking)) = session.model_selection() {
        args.restore_model_selection(model, thinking);
    }
    args.session = path.to_path_buf();
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
