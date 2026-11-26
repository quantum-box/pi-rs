//! The terminal view over the Durable companion's line-delimited RPC.
use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{BufRead, BufReader, Read},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, TryRecvError},
};

use clap::Parser;
use uuid::Uuid;

use super::*;
use crate::ui::input::{InputEvent, Reader};

// The companion counts the whole UTF-8 RPC line, including its newline.
const MAX_RPC_REQUEST_BYTES: usize = 4 * 1024 * 1024;

struct Companion {
    child: Child,
    input: Option<ChildStdin>,
    events: Receiver<Result<Value>>,
}

impl Companion {
    fn spawn(executable: &Path, arguments: &[OsString]) -> Result<Self> {
        let mut child = Command::new(executable)
            .args(arguments)
            .arg("--rpc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let input = child.stdin.take().expect("piped Durable stdin");
        let stdout = child.stdout.take().expect("piped Durable stdout");
        let stderr = child.stderr.take().expect("piped Durable stderr");
        let (sender, events) = mpsc::sync_channel(32);
        let errors = sender.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let event = line
                    .map_err(Error::from)
                    .and_then(|line| serde_json::from_str(&line).map_err(Error::from));
                let stop = event.is_err();
                if sender.send(event).is_err() || stop {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            let mut stream = BufReader::new(stderr);
            let mut captured = Vec::new();
            if stream
                .by_ref()
                .take(8192)
                .read_to_end(&mut captured)
                .is_ok()
            {
                let _ = std::io::copy(&mut stream, &mut std::io::sink());
            }
            if !captured.is_empty() {
                let message = String::from_utf8_lossy(&captured);
                let _ = errors.send(Err(Error::Config(message.trim().to_owned())));
            }
        });
        Ok(Self {
            child,
            input: Some(input),
            events,
        })
    }

    fn send(&mut self, request: Value) -> Result<()> {
        self.send_json(&serde_json::to_vec(&request)?)
    }

    fn send_json(&mut self, request: &[u8]) -> Result<()> {
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| Error::Config("Durable companion input is closed".into()))?;
        input.write_all(request)?;
        writeln!(input)?;
        input.flush()?;
        Ok(())
    }
}

impl Drop for Companion {
    fn drop(&mut self) {
        drop(self.input.take());
        let deadline = Instant::now() + Duration::from_millis(500);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                _ => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Default)]
struct Conversation {
    active: bool,
    submissions: BTreeMap<Uuid, pi_rs::QueuedMessage>,
}

impl Conversation {
    fn busy(&self) -> bool {
        self.active || !self.submissions.is_empty()
    }

    fn event(&mut self, view: &mut View, args: &mut Cli, event: Value) {
        match event["type"].as_str() {
            Some("response") => {
                let pending = event["id"]
                    .as_str()
                    .and_then(|id| Uuid::parse_str(id).ok())
                    .and_then(|id| self.submissions.remove(&id));
                if event["success"] != true {
                    if let Some(pending) = pending {
                        restore_draft(view, pending);
                    }
                    view.append(event["error"].as_str().unwrap_or("Durable request failed"));
                    return;
                }
                match event["command"].as_str() {
                    Some("get_state") => {
                        self.active = event["data"]["isStreaming"] == true;
                        if let Some(model) = event["data"]["model"]["id"].as_str() {
                            args.model = model.to_owned();
                        }
                        if let Some(session) = event["data"]["sessionId"].as_str()
                            && view.session_name.as_deref() != Some(session)
                        {
                            view.session_name = Some(session.to_owned());
                            append_session_reference(view);
                        }
                    }
                    Some("get_messages") => {
                        if let Some(messages) = event["data"]["messages"].as_array() {
                            view.lines.clear();
                            append_session_reference(view);
                            for message in messages {
                                append_message(view, message);
                            }
                        }
                    }
                    Some("prompt") if event["data"]["disposition"] == "started" => {
                        self.active = true;
                    }
                    Some("clear_queue") => restore_queue(view, &event["data"]),
                    _ => {}
                }
            }
            Some("agent_start") => self.active = true,
            Some("agent_end") => self.active = false,
            Some("message_start") if event["message"]["role"] == "user" => {
                append_message(view, &event["message"]);
                view.viewport.follow();
            }
            Some("message_update") if event["message"]["role"] == "assistant" => {
                view.streaming = message_text(&event["message"]);
            }
            Some("message_end") if event["message"]["role"] == "assistant" => {
                view.streaming.clear();
                append_message(view, &event["message"]);
                if let Some(error) = event["message"]["errorMessage"].as_str() {
                    view.append(error);
                }
            }
            _ => {}
        }
    }

    fn submit(
        &mut self,
        view: &mut View,
        follow_up: bool,
        send: impl FnOnce(&[u8]) -> Result<()>,
    ) -> Result<()> {
        if view.clipboard.is_some() {
            return Ok(());
        }
        let text = &view.editor.text;
        if text.trim().is_empty() && view.draft_images.is_empty() {
            return Ok(());
        }
        let busy = self.busy();
        let id = Uuid::new_v4();
        let images: Vec<_> = view
            .draft_images
            .iter()
            .map(|image| json!({"type":"image", "mimeType":image.mime_type, "data":image.data}))
            .collect();
        let mut request = json!({"type":"prompt", "id":id, "message":text, "images":images});
        if busy {
            request["streamingBehavior"] = json!(if follow_up { "followUp" } else { "steer" });
        }
        let encoded = serde_json::to_vec(&request)?;
        if encoded.len() >= MAX_RPC_REQUEST_BYTES {
            view.append("Durable request exceeds 4 MiB. Shorten the draft or remove an image.");
            return Ok(());
        }
        send(&encoded)?;
        let input = view.draft_message(text);
        view.editor.take();
        view.history_index = None;
        view.draft_images.clear();
        self.submissions.insert(id, input);
        Ok(())
    }
}

fn append_session_reference(view: &mut View) {
    if let Some(session) = &view.session_name {
        view.append(&format!("Durable session ID {session}"));
    }
}

fn message_text(message: &Value) -> String {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "text")
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn append_message(view: &mut View, message: &Value) {
    let text = message_text(message);
    match message["role"].as_str() {
        Some("user") => {
            view.remember_prompt(&text);
            view.append_entry(Entry::User(text.into()));
            let attachments = message_images(message);
            if !attachments.is_empty() {
                view.append(&format!("{} attached image(s)", attachments.len()));
                view.add_images(&attachments);
            }
        }
        Some("assistant") if !text.is_empty() => view.append_entry(Entry::Assistant(text.into())),
        _ => {}
    }
}

fn message_images(message: &Value) -> Vec<pi_rs::ImageContent> {
    message["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "image")
        .filter_map(|block| serde_json::from_value(block.clone()).ok())
        .collect()
}

fn restore_queue(view: &mut View, data: &Value) {
    let mut draft = view.draft_message("");
    draft.images.clear();
    let text = if let Some(inputs) = data["inputs"].as_array() {
        draft.images = inputs.iter().flat_map(message_images).collect();
        inputs.iter().map(message_text).collect::<Vec<_>>()
    } else {
        [&data["steering"], &data["followUp"]]
            .into_iter()
            .filter_map(Value::as_array)
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    };
    if !text.is_empty() {
        draft.input.content = text.join("\n\n");
        restore_draft(view, draft);
    }
}

fn restore_draft(view: &mut View, mut input: pi_rs::QueuedMessage) {
    let existing = view.editor.take();
    let content = [input.input.content, existing]
        .into_iter()
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    view.editor.replace(content);
    input.images.append(&mut view.draft_images);
    view.draft_images = input.images;
}

pub(crate) fn run(executable: &Path, options: &[OsString]) -> Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return Err(Error::Config(
            "the Durable terminal interface requires a terminal".into(),
        ));
    }
    let mut args = Cli::try_parse_from(["pi", "--no-session"])
        .map_err(|error| Error::Config(error.to_string()))?;
    args.workspace = std::env::current_dir()?;
    args.provider = Provider::Custom {
        id: "openai".into(),
        adapter: crate::providers::Adapter::OpenAiResponses,
    };
    args.settings = crate::config::Settings::load(&args.workspace, false)?;
    args.tui_mode = if args.settings.string("tuiMode") == Some("regular") {
        Some(TuiMode::Regular)
    } else {
        Some(TuiMode::Fullscreen)
    };
    let mut worker = Companion::spawn(executable, options)?;
    worker.send(json!({"type":"get_state"}))?;
    worker.send(json!({"type":"get_messages"}))?;
    let mut view = View::default();
    let mut conversation = Conversation::default();
    #[cfg(any(unix, windows))]
    let _pending_colors = crate::ui::theme::PendingTerminalColors::new();
    let mut theme = load_resolved_theme(
        args.theme_setting().unwrap_or("system"),
        &args.theme_paths,
        &args.workspace,
        false,
    )
    .unwrap_or_else(|_| Theme::system());
    let keys = Keys::load()?;
    let mut watcher = ThemeWatcher::new(&theme);
    let mut terminal = Terminal::enter(
        args.tui_mode,
        view.graphics.cleanup(),
        #[cfg(any(unix, windows))]
        None,
        #[cfg(any(unix, windows))]
        follows_terminal_appearance(args.theme_setting()),
    )?;
    let mut input = Reader::new()?;
    if view.graphics.enabled() {
        images::query_cell_size()?;
    }
    let mut dirty = true;
    let mut last_theme_check = Instant::now();
    loop {
        loop {
            match worker.events.try_recv() {
                Ok(event) => conversation.event(&mut view, &mut args, event?),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    let status = worker.child.wait()?;
                    return Err(Error::Config(format!(
                        "Durable companion exited ({status})"
                    )));
                }
            }
            dirty = true;
        }
        dirty |= view.poll_clipboard();
        if last_theme_check.elapsed() >= Duration::from_millis(500) {
            dirty |= watcher.poll(&mut theme)?;
            last_theme_check = Instant::now();
        }
        terminal.update_progress(args.settings.terminal_progress() && conversation.busy())?;
        if dirty {
            view.draw(&args, &theme, conversation.busy(), &keys)?;
        }
        dirty = true;
        match input.next(Duration::from_millis(32))? {
            Some(InputEvent::Event(Event::Key(key))) if key.kind != KeyEventKind::Release => {
                if let Some(action) = keys.viewport_action(key) {
                    view.viewport.navigate(action);
                    view.scrollbar_activity = Some(Instant::now());
                    continue;
                }
                let action = keys
                    .action(
                        key,
                        view.editor.text.is_empty() && view.draft_images.is_empty(),
                    )
                    .unwrap_or("");
                if view.editor.jump_key(action, key)
                    || view.editor.action(action)
                    || view.editor_vertical_action(action)
                {
                    continue;
                }
                match action {
                    "app.exit" => return Ok(()),
                    "app.clear" => {
                        view.editor.take();
                        view.draft_images.clear();
                    }
                    "app.clipboard.pasteImage" => view.paste_clipboard(&args),
                    "app.message.dequeue" => {
                        worker.send(json!({"type":"clear_queue","includeContent":true}))?
                    }
                    "app.interrupt" if conversation.busy() => {
                        worker.send(json!({"type":"clear_queue","includeContent":true}))?;
                        worker.send(json!({"type":"abort"}))?;
                    }
                    "tui.input.submit" if view.editor.continued_line() => {}
                    "tui.input.submit" | "app.message.followUp" => {
                        conversation.submit(
                            &mut view,
                            action == "app.message.followUp",
                            |request| worker.send_json(request),
                        )?;
                    }
                    _ => {
                        if let KeyCode::Char(character) = key.code
                            && !key.modifiers.intersects(
                                KeyModifiers::CONTROL | KeyModifiers::ALT | KeyModifiers::SUPER,
                            )
                        {
                            view.editor.type_character(character);
                        }
                    }
                }
            }
            Some(InputEvent::Event(Event::Paste(text))) => view.paste_editor_text(&text),
            Some(InputEvent::Event(Event::Resize(_, _))) => (),
            #[cfg(any(unix, windows))]
            Some(InputEvent::CellSize { width, height }) => {
                images::set_cell_dimensions(width, height);
            }
            #[cfg(unix)]
            Some(InputEvent::KeyboardEnhancementFlags(_)) => {
                terminal.keyboard_reply(true)?;
            }
            #[cfg(any(unix, windows))]
            Some(InputEvent::PrimaryDeviceAttributes) => {
                finish_initial_terminal_colors(&mut theme);
                #[cfg(unix)]
                terminal.keyboard_reply(false)?;
            }
            #[cfg(any(unix, windows))]
            Some(InputEvent::Appearance { dark }) if terminal.appearance_notifications => {
                let (changed, _) = handle_terminal_appearance(
                    &mut terminal.colors,
                    &mut theme,
                    dark,
                    &mut std::io::stdout(),
                )?;
                if changed {
                    refresh_terminal_theme_selection(&args, &mut view, &mut theme);
                }
            }
            #[cfg(any(unix, windows))]
            Some(InputEvent::TerminalColor {
                foreground,
                red,
                green,
                blue,
            }) => {
                if let Some(snapshot) = terminal.colors.record_color(
                    if foreground { 16 } else { 17 },
                    [red, green, blue],
                    &mut std::io::stdout(),
                )? {
                    let (changed, _) = apply_terminal_color_snapshot(snapshot, &mut theme);
                    if changed {
                        refresh_terminal_theme_selection(&args, &mut view, &mut theme);
                    }
                }
                settle_complete_colors(&terminal.colors, &mut theme);
            }
            _ => dirty = false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_prompt_preserves_the_entire_draft_and_never_writes_to_rpc() {
        let mut view = View::default();
        let mut conversation = Conversation::default();
        // JSON escaping doubles these bytes even though the editor text fits.
        let text = "\\".repeat(MAX_RPC_REQUEST_BYTES / 2);
        let image = pi_rs::ImageContent {
            data: "YQ==".into(),
            mime_type: "image/png".into(),
        };
        view.editor.replace(text.clone());
        view.editor.cursor = 2;
        view.draft_images.push(image.clone());
        conversation
            .submit(&mut view, false, |_| panic!("oversized prompt was sent"))
            .unwrap();
        assert_eq!(view.editor.text, text);
        assert_eq!(view.editor.cursor, 2);
        assert_eq!(view.draft_images, [image]);
        assert!(conversation.submissions.is_empty());
        assert!(matches!(&view.lines[..], [Entry::Notice(text)] if text.contains("4 MiB")));

        view.editor.replace("shortened".into());
        conversation
            .submit(&mut view, false, |request| {
                let request: Value = serde_json::from_slice(request)?;
                assert_eq!(request["message"], "shortened");
                assert_eq!(request["images"][0]["data"], "YQ==");
                Ok(())
            })
            .unwrap();
        assert!(view.editor.text.is_empty());
        assert!(view.draft_images.is_empty());
        assert_eq!(conversation.submissions.len(), 1);
    }

    #[test]
    fn prompt_limit_counts_the_wire_newline() {
        let mut view = View::default();
        let prefix_bytes = serde_json::to_vec(
            &json!({"type":"prompt", "id":Uuid::nil(), "message":"", "images":[]}),
        )
        .unwrap()
        .len();
        let mut conversation = Conversation::default();
        view.editor
            .replace("a".repeat(MAX_RPC_REQUEST_BYTES - prefix_bytes));
        conversation
            .submit(&mut view, false, |_| panic!("oversized prompt was sent"))
            .unwrap();
        assert!(conversation.submissions.is_empty());

        view.editor
            .replace("a".repeat(MAX_RPC_REQUEST_BYTES - prefix_bytes - 1));
        conversation
            .submit(&mut view, false, |request| {
                assert_eq!(request.len() + 1, MAX_RPC_REQUEST_BYTES);
                Ok(())
            })
            .unwrap();
        assert_eq!(conversation.submissions.len(), 1);
    }

    #[test]
    fn durable_session_id_survives_history_loading_with_the_header_hidden() {
        let session = Uuid::new_v4().to_string();
        let settings_dir = tempfile::tempdir().unwrap();
        let path = settings_dir.path().join("settings.json");
        std::fs::write(&path, r#"{"quietStartup":true}"#).unwrap();
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        args.tui_mode = Some(TuiMode::Regular);
        args.settings =
            crate::config::Settings::load_from_paths(settings_dir.path(), false, &path).unwrap();
        assert!(!show_startup_header(&args));

        let mut conversation = Conversation::default();
        let mut view = View::default();
        let state = json!({
            "type":"response","command":"get_state","success":true,
            "data":{"sessionId":session}
        });
        conversation.event(&mut view, &mut args, state.clone());
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"response","command":"get_messages","success":true,"data":{"messages":[
                    {"role":"user","content":[{"type":"text","text":"saved"}]}
                ]}
            }),
        );
        conversation.event(&mut view, &mut args, state);
        assert!(matches!(
            &view.lines[..],
            [Entry::Notice(text), Entry::User(_)] if text == &format!("Durable session ID {session}")
        ));
    }

    #[test]
    fn durable_messages_share_the_native_transcript_and_restore_rejected_input() {
        let mut args = Cli::try_parse_from(["pi"]).unwrap();
        let mut view = View::default();
        let mut conversation = Conversation::default();
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_start",
                "message":{"role":"user","content":[{"type":"text","text":"first"}]}
            }),
        );
        assert!(matches!(&view.lines[..], [Entry::User(_)]));
        view.editor_width = 80;
        assert!(view.editor_vertical_action("tui.editor.cursorUp"));
        assert_eq!(view.editor.text, "first");
        assert!(view.editor_vertical_action("tui.editor.cursorDown"));
        assert!(view.editor.text.is_empty());

        view.editor.replace("first line\nsecond line".into());
        let last_row_cursor = view.editor.cursor;
        assert!(view.editor_vertical_action("tui.editor.cursorUp"));
        assert!(view.editor.cursor < "first line\n".len());
        assert!(view.editor_vertical_action("tui.editor.cursorDown"));
        assert_eq!(view.editor.cursor, last_row_cursor);
        view.editor.take();
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_update",
                "message":{"role":"assistant","content":[{"type":"text","text":"live"}]}
            }),
        );
        assert_eq!(view.streaming, "live");
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"message_end",
                "message":{"role":"assistant","content":[{"type":"text","text":"final"}]}
            }),
        );
        assert!(view.streaming.is_empty());
        assert!(matches!(
            &view.lines[..],
            [Entry::User(_), Entry::Assistant(_)]
        ));
        let id = Uuid::new_v4();
        conversation
            .submissions
            .insert(id, view.draft_message("rejected"));
        view.editor.replace("later".into());
        conversation.event(
            &mut view,
            &mut args,
            json!({
                "type":"response","command":"prompt","id":id,"success":false,"error":"unavailable"
            }),
        );
        assert_eq!(view.editor.take(), "rejected\n\nlater");
        assert!(conversation.submissions.is_empty());
        assert!(matches!(view.lines.last(), Some(Entry::Notice(text)) if text == "unavailable"));
    }
}
